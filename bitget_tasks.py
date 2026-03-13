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
from core_models import WithdrawRuntimeParams
from shared_utils import dispatch_ui_callback, set_ui_batch_size

SUBMITTED_TIMEOUT_SECONDS = 10.0


def run_query_balance(app, cred: tuple[str, str, str], selected_coin: str) -> None:
    dispatch_ui = getattr(app, "_dispatch_ui", lambda callback: dispatch_ui_callback(app, callback))
    try:
        set_ui_batch_size(app, app._runtime_worker_threads())
        k, s, p = cred
        query_key = f"source:{k}"
        context_sig = k.strip()
        dispatch_ui(lambda: app._begin_progress("query", [query_key]))
        dispatch_ui(lambda a=app._coin_amount_text(selected_coin, Decimal("0")): app._set_progress_metrics(balance_text=a))
        dispatch_ui(lambda q=query_key: app._set_query_status(q, "waiting"))
        dispatch_ui(lambda: app.log("开始查询余额（全币种）"))
        dispatch_ui(lambda q=query_key: app._set_query_status(q, "running"))
        totals = app.client.get_account_assets(k, s, p)
        dispatch_ui(lambda c=context_sig, d=dict(totals): app._apply_source_assets(c, d, update_coin_options=True))
        balance_total = totals.get(selected_coin, Decimal("0"))
        dispatch_ui(lambda c=context_sig, a=app._coin_amount_text(selected_coin, balance_total): app._apply_query_balance_metrics(c, a))
        summary = f"余额查询结束：{app._totals_to_text(totals)}"
        dispatch_ui(lambda: app.log(summary))
        dispatch_ui(lambda q=query_key: app._set_query_status(q, "success"))
        dispatch_ui(lambda: app._finish_progress("query", 1, 0))
    except Exception as exc:
        err_text = str(exc)
        dispatch_ui(lambda m=f"余额查询失败：{err_text}": app.log(m))
        dispatch_ui(lambda e=err_text: messagebox.showerror("执行异常", e))
        if "k" in locals():
            dispatch_ui(lambda q=f"source:{k}": app._set_query_status(q, "failed"))
        dispatch_ui(lambda: app._finish_progress("query", 0, 1))
    finally:
        app.is_running = False


def run_withdraw(
    app,
    addrs: list[str],
    params: WithdrawRuntimeParams,
    cred: tuple[str, str, str],
    dry_run: bool,
) -> None:
    dispatch_ui = getattr(app, "_dispatch_ui", lambda callback: dispatch_ui_callback(app, callback))
    try:
        set_ui_batch_size(app, params.threads)

        def job_prefix(index: int, addr: str) -> str:
            return f"[{index}/{len(addrs)}][{app._mask(addr, head=8, tail=6)}]"

        amount_view = params.amount
        source_context = cred[0].strip()
        track_amount_total = not (dry_run and params.amount == app.AMOUNT_ALL_LABEL)
        if not hasattr(app, "address_status_context"):
            app.address_status_context = {}
        if params.random_enabled and params.random_min is not None and params.random_max is not None:
            amount_view = f"random({params.random_min:.2f}~{params.random_max:.2f})"
        withdraw_fee: Decimal | None = None
        if not dry_run:
            try:
                withdraw_fee = app.client.get_withdraw_fee(params.coin, params.network)
            except Exception as exc:
                dispatch_ui(lambda m=f"{params.coin}/{params.network} 手续费读取失败：{exc}": app.log(m))
        dispatch_ui(lambda keys=addrs: app._begin_progress("withdraw", keys))
        dispatch_ui(
            lambda a=app._coin_amount_text(params.coin, Decimal("0")), g=(
                app._coin_amount_text(params.coin, Decimal("0")) if withdraw_fee is not None else "-"
            ), amt_known=track_amount_total: app._set_progress_metrics(amount_text=(a if amt_known else "-"), gas_text=g),
        )
        dispatch_ui(
            lambda: app.log(
                f"开始批量提现：地址数={len(addrs)}, coin={params.coin}, amount={amount_view}, "
                f"network={params.network}, delay={params.delay}, threads={params.threads}, dry_run={dry_run}"
            ),
        )
        fallback_prefixes: dict[str, str] = {}
        for addr in addrs:
            fallback_prefixes[addr] = job_prefix(len(fallback_prefixes) + 1, addr)
            app.address_status_context[addr] = source_context
            dispatch_ui(lambda a=addr: app._set_status(a, "waiting"))

        success = 0
        failed = 0
        resolved = 0
        total_amount = Decimal("0")
        total_fee = Decimal("0")
        lock = threading.Lock()
        resolved_event = threading.Event()
        resolved_addrs: set[str] = set()
        jobs: queue.Queue[tuple[int, str]] = queue.Queue()
        for i, addr in enumerate(addrs, start=1):
            jobs.put((i, addr))

        def finalize_result(addr: str, result_status: str, msg: str, *, amount_text: str = "") -> None:
            nonlocal success, failed, resolved, total_amount, total_fee
            with lock:
                if addr in resolved_addrs:
                    return
                resolved_addrs.add(addr)
                if result_status == "success":
                    success += 1
                    if track_amount_total:
                        try:
                            total_amount += Decimal(amount_text)
                        except Exception:
                            pass
                    if withdraw_fee is not None:
                        total_fee += withdraw_fee
                else:
                    failed += 1
                amount_total_text = app._coin_amount_text(params.coin, total_amount) if track_amount_total else "-"
                gas_total_text = app._coin_amount_text(params.coin, total_fee) if withdraw_fee is not None else "-"
                resolved += 1
                done = resolved >= len(addrs)
            if done:
                resolved_event.set()
            dispatch_ui(lambda m=msg: app.log(m))
            app.address_status_context[addr] = source_context
            dispatch_ui(lambda a=addr, s=result_status: app._set_status(a, s))
            dispatch_ui(lambda a=amount_total_text, g=gas_total_text: app._set_progress_metrics(amount_text=a, gas_text=g))

        def schedule_submitted_timeout(addr: str, prefix: str, submitted_timeout_seconds: float) -> None:
            timeout_msg = f"{prefix} 确认中超过 {submitted_timeout_seconds:g} 秒，自动判定失败"

            def timeout_worker():
                if submitted_timeout_seconds > 0:
                    time.sleep(submitted_timeout_seconds)
                finalize_result(addr, "failed", timeout_msg)

            if submitted_timeout_seconds > 0:
                threading.Thread(target=timeout_worker, daemon=True).start()
            else:
                timeout_worker()

        def worker():
            while True:
                try:
                    i, addr = jobs.get_nowait()
                except queue.Empty:
                    return
                prefix = job_prefix(i, addr)
                result_status = "failed"
                msg = ""
                amount_text = ""
                app.address_status_context[addr] = source_context
                dispatch_ui(lambda a=addr: app._set_status(a, "running"))
                submitted_timeout_seconds = max(0.0, float(getattr(app, "submitted_timeout_seconds", SUBMITTED_TIMEOUT_SECONDS)))
                try:
                    amount = app._resolve_amount(params, cred, dry_run=dry_run)
                    amount_text = str(amount)
                    if dry_run:
                        msg = f"{prefix} 模拟成功 -> {params.coin} {amount}"
                        result_status = "success"
                    else:
                        new_client_oid = getattr(app.client, "new_client_oid", None)
                        client_oid = (
                            new_client_oid()
                            if callable(new_client_oid)
                            else f"codex_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
                        )
                        try:
                            resp = app.client.withdraw(
                                cred[0],
                                cred[1],
                                cred[2],
                                coin=params.coin,
                                address=addr,
                                amount=amount,
                                chain=params.network,
                                client_oid=client_oid,
                            )
                        except SubmissionUncertainError as exc:
                            msg = f"{prefix} 提现确认中：未拿到 orderId，{exc}"
                            result_status = "submitted"
                        else:
                            order_id = str(resp.get("orderId", "") or "")
                            if order_id:
                                msg = f"{prefix} 提现成功 -> {params.coin} {amount}，orderId={order_id}"
                                result_status = "success"
                            else:
                                msg = f"{prefix} 提现确认中：接口未返回 orderId"
                                result_status = "submitted"
                except Exception as exc:
                    msg = f"{prefix} 提现失败：{exc}"
                finally:
                    jobs.task_done()

                if result_status == "submitted":
                    dispatch_ui(lambda m=msg: app.log(m))
                    app.address_status_context[addr] = source_context
                    dispatch_ui(lambda a=addr: app._set_status(a, "submitted"))
                    schedule_submitted_timeout(addr, prefix, submitted_timeout_seconds)
                else:
                    finalize_result(addr, result_status, msg, amount_text=amount_text)
                if params.delay > 0:
                    time.sleep(params.delay)

        workers: list[threading.Thread] = []
        worker_count = max(1, min(params.threads, len(addrs)))
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
        if len(addrs) == 0:
            resolved_event.set()
        if not resolved_event.wait(batch_finalize_timeout_seconds):
            with lock:
                pending_addrs = [addr for addr in addrs if addr not in resolved_addrs]
            for addr in pending_addrs:
                prefix = fallback_prefixes.get(addr, "")
                timeout_msg = f"{prefix} 任务收尾超时，自动判定失败" if prefix else "任务收尾超时，自动判定失败"
                finalize_result(addr, "failed", timeout_msg)

        amount_total_text = app._coin_amount_text(params.coin, total_amount) if track_amount_total else "-"
        gas_total_text = app._coin_amount_text(params.coin, total_fee) if withdraw_fee is not None else "-"
        summary = (
            f"Bitget 提现任务结束：成功 {success}，失败 {failed}，"
            f"提现总额={amount_total_text}，gas合计={gas_total_text}"
        )
        dispatch_ui(lambda: app.log(summary))
        dispatch_ui(lambda: messagebox.showinfo("执行完成", summary))
        dispatch_ui(lambda s=success, f=failed: app._finish_progress("withdraw", s, f))
    except Exception as exc:
        err_text = str(exc)
        dispatch_ui(lambda m=f"Bitget 提现任务异常终止：{err_text}": app.log(m))
        dispatch_ui(lambda e=err_text: messagebox.showerror("执行异常", e))
        dispatch_ui(
            lambda s=success if "success" in locals() else 0, f=failed if "failed" in locals() else 0: app._finish_progress("withdraw", s, f),
        )
    finally:
        app.is_running = False
