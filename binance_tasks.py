#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import queue
import random
import threading
import time
from decimal import Decimal
from tkinter import messagebox

from api_clients import SubmissionUncertainError
from core_models import AccountEntry, WithdrawRuntimeParams
from shared_utils import dispatch_ui_callback, set_ui_batch_size

SUBMITTED_TIMEOUT_SECONDS = 10.0


def run_withdraw(app, accounts: list[AccountEntry], params: WithdrawRuntimeParams, dry_run: bool, one_to_many: bool) -> None:
    dispatch_ui = getattr(app, "_dispatch_ui", lambda callback: dispatch_ui_callback(app, callback))
    try:
        set_ui_batch_size(app, params.threads)
        def job_prefix(index: int, account: AccountEntry) -> str:
            if one_to_many:
                identity = app._mask(account.address, head=8, tail=6)
            else:
                identity = app._mask(account.api_key)
            return f"[{index}/{len(accounts)}][{identity}]"

        amount_view = params.amount
        progress_keys = [acc.address if one_to_many else acc.api_key for acc in accounts]
        track_amount_total = not (dry_run and app._is_amount_all(params.amount))
        source_context = accounts[0].api_key.strip() if one_to_many and accounts else ""
        if one_to_many and not hasattr(app, "account_withdraw_status_context"):
            app.account_withdraw_status_context = {}
        if params.random_enabled and params.random_min is not None and params.random_max is not None:
            amount_view = f"random({params.random_min:.2f}~{params.random_max:.2f})"
        withdraw_fee: Decimal | None = None
        if (not dry_run) and accounts:
            try:
                withdraw_fee = app.client.get_withdraw_fee(accounts[0], params.coin, params.network)
            except Exception as exc:
                dispatch_ui(lambda m=f"{params.coin}/{params.network} 手续费读取失败：{exc}": app.log(m))
        dispatch_ui(lambda keys=progress_keys: app._begin_progress("withdraw", keys))
        dispatch_ui(
            lambda a=app._coin_amount_text(params.coin, Decimal("0")), g=(
                app._coin_amount_text(params.coin, Decimal("0")) if withdraw_fee is not None else "-"
            ), amt_known=track_amount_total: app._set_progress_metrics(amount_text=(a if amt_known else "-"), gas_text=g),
        )
        dispatch_ui(
            lambda: app.log(
                f"开始批量提现：mode={'1对多' if one_to_many else '多对多'}，数量={len(accounts)}，"
                f"coin={params.coin}, amount={amount_view}, network={params.network}, "
                f"delay={params.delay}, threads={params.threads}, dry_run={dry_run}"
            ),
        )
        fallback_prefixes: dict[str, str] = {}
        for acc in accounts:
            status_key = acc.address if one_to_many else acc.api_key
            fallback_prefixes[status_key] = job_prefix(len(fallback_prefixes) + 1, acc)
            if one_to_many:
                app.account_withdraw_status_context[status_key] = source_context
            dispatch_ui(lambda k=status_key: app._set_account_status(k, "waiting"))

        success = 0
        failed = 0
        resolved = 0
        total_amount = Decimal("0")
        total_fee = Decimal("0")
        lock = threading.Lock()
        resolved_event = threading.Event()
        resolved_keys: set[str] = set()
        jobs: queue.Queue[tuple[int, AccountEntry, str]] = queue.Queue()
        for i, acc in enumerate(accounts, start=1):
            status_key = acc.address if one_to_many else acc.api_key
            jobs.put((i, acc, status_key))

        def finalize_result(status_key: str, result_status: str, msg: str, *, withdraw_amount_text: str = "") -> None:
            nonlocal success, failed, resolved, total_amount, total_fee
            with lock:
                if status_key in resolved_keys:
                    return
                resolved_keys.add(status_key)
                if result_status == "success":
                    success += 1
                    if track_amount_total:
                        try:
                            total_amount += Decimal(withdraw_amount_text)
                        except Exception:
                            pass
                    if withdraw_fee is not None:
                        total_fee += withdraw_fee
                else:
                    failed += 1
                amount_text = app._coin_amount_text(params.coin, total_amount) if track_amount_total else "-"
                gas_text = app._coin_amount_text(params.coin, total_fee) if withdraw_fee is not None else "-"
                resolved += 1
                done = resolved >= len(accounts)
            if done:
                resolved_event.set()
            dispatch_ui(lambda m=msg: app.log(m))
            if one_to_many:
                app.account_withdraw_status_context[status_key] = source_context
            dispatch_ui(lambda k=status_key, s=result_status: app._set_account_status(k, s))
            dispatch_ui(lambda a=amount_text, g=gas_text: app._set_progress_metrics(amount_text=a, gas_text=g))

        def schedule_submitted_timeout(status_key: str, prefix: str, submitted_timeout_seconds: float) -> None:
            timeout_msg = f"{prefix} 确认中超过 {submitted_timeout_seconds:g} 秒，自动判定失败"

            def timeout_worker():
                if submitted_timeout_seconds > 0:
                    time.sleep(submitted_timeout_seconds)
                finalize_result(status_key, "failed", timeout_msg)

            if submitted_timeout_seconds > 0:
                threading.Thread(target=timeout_worker, daemon=True).start()
            else:
                timeout_worker()

        def worker():
            while True:
                try:
                    i, acc, status_key = jobs.get_nowait()
                except queue.Empty:
                    return

                prefix = job_prefix(i, acc)
                result_status = "failed"
                msg = ""
                withdraw_amount_text = ""
                withdraw_id = ""
                if one_to_many:
                    app.account_withdraw_status_context[status_key] = source_context
                dispatch_ui(lambda k=status_key: app._set_account_status(k, "running"))
                submitted_timeout_seconds = max(0.0, float(getattr(app, "submitted_timeout_seconds", SUBMITTED_TIMEOUT_SECONDS)))
                try:
                    withdraw_amount = app._resolve_withdraw_amount(acc, params, dry_run=dry_run)
                    withdraw_amount_text = str(withdraw_amount)

                    if dry_run:
                        msg = f"{prefix} 模拟成功 -> {params.coin} {withdraw_amount} 到 {app._mask(acc.address, head=8, tail=6)}"
                        result_status = "success"
                    else:
                        withdraw_order_id_fn = getattr(app.client, "new_withdraw_order_id", None)
                        withdraw_order_id = (
                            withdraw_order_id_fn()
                            if callable(withdraw_order_id_fn)
                            else f"codex_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
                        )
                        try:
                            resp = app.client.withdraw(
                                acc,
                                coin=params.coin,
                                amount=withdraw_amount,
                                network=params.network,
                                withdraw_order_id=withdraw_order_id,
                            )
                        except SubmissionUncertainError as exc:
                            msg = f"{prefix} 提现确认中：未拿到 withdrawId，{exc}"
                            result_status = "submitted"
                        else:
                            withdraw_id = str(resp.get("id", "") or "")
                            if withdraw_id:
                                msg = (
                                    f"{prefix} 提现成功 -> {params.coin} {withdraw_amount} 到 "
                                    f"{app._mask(acc.address, head=8, tail=6)}，withdrawId={withdraw_id}"
                                )
                                result_status = "success"
                            else:
                                msg = f"{prefix} 提现确认中：接口未返回 withdrawId"
                                result_status = "submitted"
                except Exception as exc:
                    msg = f"{prefix} 提现失败：{exc}"
                finally:
                    jobs.task_done()

                if result_status == "submitted":
                    dispatch_ui(lambda m=msg: app.log(m))
                    if one_to_many:
                        app.account_withdraw_status_context[status_key] = source_context
                    dispatch_ui(lambda k=status_key: app._set_account_status(k, "submitted"))
                    schedule_submitted_timeout(status_key, prefix, submitted_timeout_seconds)
                else:
                    finalize_result(status_key, result_status, msg, withdraw_amount_text=withdraw_amount_text)

                if params.delay > 0:
                    time.sleep(params.delay)

        workers: list[threading.Thread] = []
        worker_count = max(1, min(params.threads, len(accounts)))
        for _ in range(worker_count):
            t = threading.Thread(target=worker, daemon=True)
            t.start()
            workers.append(t)
        for t in workers:
            t.join()
        batch_finalize_timeout_seconds = max(
            0.2,
            max(0.0, float(getattr(app, "submitted_timeout_seconds", SUBMITTED_TIMEOUT_SECONDS))) + 1.0,
        )
        if len(accounts) == 0:
            resolved_event.set()
        if not resolved_event.wait(batch_finalize_timeout_seconds):
            with lock:
                pending_keys = [key for key in progress_keys if key not in resolved_keys]
            for status_key in pending_keys:
                prefix = fallback_prefixes.get(status_key, "")
                timeout_msg = f"{prefix} 任务收尾超时，自动判定失败" if prefix else "任务收尾超时，自动判定失败"
                finalize_result(status_key, "failed", timeout_msg)

        amount_total_text = app._coin_amount_text(params.coin, total_amount) if track_amount_total else "-"
        gas_total_text = app._coin_amount_text(params.coin, total_fee) if withdraw_fee is not None else "-"
        summary = (
            f"提现任务结束：成功 {success}，失败 {failed}，"
            f"提现总额={amount_total_text}，gas合计={gas_total_text}"
        )
        dispatch_ui(lambda: app.log(summary))
        dispatch_ui(lambda: messagebox.showinfo("执行完成", summary))
        dispatch_ui(lambda s=success, f=failed: app._finish_progress("withdraw", s, f))
    except Exception as exc:
        err_text = str(exc)
        dispatch_ui(lambda m=f"提现任务异常终止：{err_text}": app.log(m))
        dispatch_ui(lambda e=err_text: messagebox.showerror("执行异常", e))
        dispatch_ui(
            lambda s=success if "success" in locals() else 0, f=failed if "failed" in locals() else 0: app._finish_progress("withdraw", s, f),
        )
    finally:
        app.is_running = False


def run_query_balance(app, accounts: list[AccountEntry], coin: str) -> None:
    dispatch_ui = getattr(app, "_dispatch_ui", lambda callback: dispatch_ui_callback(app, callback))
    try:
        set_ui_batch_size(app, app._runtime_worker_threads())
        progress_keys = [acc.api_key for acc in accounts]
        dispatch_ui(lambda keys=progress_keys: app._begin_progress("query", keys))
        dispatch_ui(lambda a=app._coin_amount_text(coin, Decimal("0")): app._set_progress_metrics(balance_text=a))
        dispatch_ui(lambda: app.log(f"开始查询余额（全币种）：{len(accounts)} 账号，统计币种={coin}"))
        dispatch_ui(lambda keys=progress_keys: app._set_query_statuses(keys, "waiting"))

        total = Decimal("0")
        totals_all: dict[str, Decimal] = {}
        ok = 0
        failed = 0
        lock = threading.Lock()
        jobs: queue.Queue[tuple[int, AccountEntry]] = queue.Queue()
        for i, acc in enumerate(accounts, start=1):
            jobs.put((i, acc))

        def worker():
            nonlocal total, ok, failed
            while True:
                try:
                    i, acc = jobs.get_nowait()
                except queue.Empty:
                    return
                prefix = f"[{i}/{len(accounts)}][{app._mask(acc.api_key)}]"
                dispatch_ui(lambda k=acc.api_key: app._set_account_query_status(k, "running"))
                try:
                    balances = app.client.get_all_spot_balances(acc, include_zero=False)
                    per_account_totals: dict[str, Decimal] = {}
                    for asset, (free, locked) in balances.items():
                        subtotal = free + locked
                        if subtotal <= 0:
                            continue
                        per_account_totals[asset] = subtotal
                    with lock:
                        for asset, subtotal in per_account_totals.items():
                            totals_all[asset] = totals_all.get(asset, Decimal("0")) + subtotal
                        total += per_account_totals.get(coin, Decimal("0"))
                        ok += 1
                        balance_text = app._coin_amount_text(coin, total)
                    dispatch_ui(lambda k=acc.api_key, d=per_account_totals: app._replace_account_coin_totals(k, d))
                    dispatch_ui(lambda a=balance_text: app._set_progress_metrics(balance_text=a))
                    msg = f"{prefix} 余额 -> {app._totals_to_text(per_account_totals)}"
                    dispatch_ui(lambda m=msg: app.log(m))
                    dispatch_ui(lambda k=acc.api_key: app._set_account_query_status(k, "success"))
                except Exception as exc:
                    with lock:
                        failed += 1
                    dispatch_ui(lambda m=f"{prefix} 查询失败：{exc}": app.log(m))
                    dispatch_ui(lambda k=acc.api_key: app._set_account_query_status(k, "failed"))
                finally:
                    jobs.task_done()

        workers: list[threading.Thread] = []
        worker_count = max(1, min(app._runtime_worker_threads(), len(accounts)))
        for _ in range(worker_count):
            t = threading.Thread(target=worker, daemon=True)
            t.start()
            workers.append(t)
        for t in workers:
            t.join()

        dispatch_ui(lambda totals=dict(totals_all): app._update_coin_options_from_totals(totals))
        top = app._totals_to_text(dict(sorted(totals_all.items(), key=lambda x: (-x[1], x[0]))[:10]))
        summary = f"余额查询结束：成功 {ok}，失败 {failed}，{coin} 合计={app._decimal_to_text(total)}，全币种Top={top}"
        dispatch_ui(lambda: app.log(summary))
        dispatch_ui(lambda s=ok, f=failed: app._finish_progress("query", s, f))
    except Exception as exc:
        err_text = str(exc)
        dispatch_ui(lambda m=f"余额查询任务异常终止：{err_text}": app.log(m))
        dispatch_ui(lambda e=err_text: messagebox.showerror("执行异常", e))
        dispatch_ui(lambda s=ok if "ok" in locals() else 0, f=failed if "failed" in locals() else 0: app._finish_progress("query", s, f))
    finally:
        app.is_running = False
