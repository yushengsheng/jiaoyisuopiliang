#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import queue
import threading
import time
from decimal import Decimal
from tkinter import messagebox

from core_models import AccountEntry, WithdrawRuntimeParams


def run_withdraw(app, accounts: list[AccountEntry], params: WithdrawRuntimeParams, dry_run: bool, one_to_many: bool) -> None:
    try:
        amount_view = params.amount
        if params.random_enabled and params.random_min is not None and params.random_max is not None:
            amount_view = f"random({params.random_min:.2f}~{params.random_max:.2f})"
        app.root.after(
            0,
            lambda: app.log(
                f"开始批量提现：mode={'1对多' if one_to_many else '多对多'}，数量={len(accounts)}，"
                f"coin={params.coin}, amount={amount_view}, network={params.network}, "
                f"delay={params.delay}, threads={params.threads}, dry_run={dry_run}"
            ),
        )
        for acc in accounts:
            status_key = acc.address if one_to_many else acc.api_key
            app.root.after(0, lambda k=status_key: app._set_account_status(k, "waiting"))

        success = 0
        failed = 0
        lock = threading.Lock()
        jobs: queue.Queue[tuple[int, AccountEntry, str]] = queue.Queue()
        for i, acc in enumerate(accounts, start=1):
            status_key = acc.address if one_to_many else acc.api_key
            jobs.put((i, acc, status_key))

        def worker():
            nonlocal success, failed
            while True:
                try:
                    i, acc, status_key = jobs.get_nowait()
                except queue.Empty:
                    return

                if one_to_many:
                    identity = app._mask(acc.address, head=8, tail=6)
                else:
                    identity = app._mask(acc.api_key)
                prefix = f"[{i}/{len(accounts)}][{identity}]"
                ok = False
                msg = ""
                app.root.after(0, lambda k=status_key: app._set_account_status(k, "running"))
                try:
                    withdraw_amount = app._resolve_withdraw_amount(acc, params, dry_run=dry_run)

                    if dry_run:
                        msg = f"{prefix} 模拟成功 -> {params.coin} {withdraw_amount} 到 {app._mask(acc.address, head=8, tail=6)}"
                    else:
                        resp = app.client.withdraw(acc, coin=params.coin, amount=withdraw_amount, network=params.network)
                        withdraw_id = resp.get("id", "N/A")
                        msg = (
                            f"{prefix} 提现成功 -> {params.coin} {withdraw_amount} 到 "
                            f"{app._mask(acc.address, head=8, tail=6)}，withdrawId={withdraw_id}"
                        )
                    ok = True
                except Exception as exc:
                    msg = f"{prefix} 提现失败：{exc}"
                finally:
                    jobs.task_done()

                app.root.after(0, lambda m=msg: app.log(m))
                if ok:
                    app.root.after(0, lambda k=status_key: app._set_account_status(k, "success"))
                else:
                    app.root.after(0, lambda k=status_key: app._set_account_status(k, "failed"))
                with lock:
                    if ok:
                        success += 1
                    else:
                        failed += 1

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

        summary = f"提现任务结束：成功 {success}，失败 {failed}"
        app.root.after(0, lambda: app.log(summary))
        app.root.after(0, lambda: messagebox.showinfo("执行完成", summary))
    except Exception as exc:
        err_text = str(exc)
        app.root.after(0, lambda m=f"提现任务异常终止：{err_text}": app.log(m))
        app.root.after(0, lambda e=err_text: messagebox.showerror("执行异常", e))
    finally:
        app.is_running = False


def run_refresh_coin_options(app, accounts: list[AccountEntry]) -> None:
    try:
        app.root.after(0, lambda: app.log(f"开始按余额刷新币种：{len(accounts)} 账号"))
        totals: dict[str, Decimal] = {}
        ok = 0
        failed = 0
        lock = threading.Lock()
        jobs: queue.Queue[tuple[int, AccountEntry]] = queue.Queue()
        for i, acc in enumerate(accounts, start=1):
            jobs.put((i, acc))

        def worker():
            nonlocal ok, failed
            while True:
                try:
                    i, acc = jobs.get_nowait()
                except queue.Empty:
                    return
                prefix = f"[{i}/{len(accounts)}][{app._mask(acc.api_key)}]"
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
                            totals[asset] = totals.get(asset, Decimal("0")) + subtotal
                        ok += 1
                    app.root.after(0, lambda k=acc.api_key, d=per_account_totals: app._replace_account_coin_totals(k, d))
                    app.root.after(0, lambda m=f"{prefix} 币种扫描完成": app.log(m))
                except Exception as exc:
                    with lock:
                        failed += 1
                    app.root.after(0, lambda m=f"{prefix} 扫描失败：{exc}": app.log(m))
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

        app.root.after(0, lambda: app._update_coin_options_from_totals(totals))
        ranked = sorted(totals.items(), key=lambda x: (-x[1], x[0]))
        if ranked:
            top = ", ".join([f"{coin}={app._decimal_to_text(total)}" for coin, total in ranked[:8]])
            summary = f"币种刷新结束：成功 {ok}，失败 {failed}，可用币种 {len(ranked)}；Top: {top}"
        else:
            summary = f"币种刷新结束：成功 {ok}，失败 {failed}，未发现可用余额币种"
        app.root.after(0, lambda: app.log(summary))
        app.root.after(0, lambda: messagebox.showinfo("币种刷新完成", summary))
    except Exception as exc:
        err_text = str(exc)
        app.root.after(0, lambda m=f"币种刷新任务异常终止：{err_text}": app.log(m))
        app.root.after(0, lambda e=err_text: messagebox.showerror("执行异常", e))
    finally:
        app.is_running = False


def run_query_balance(app, accounts: list[AccountEntry], coin: str) -> None:
    try:
        app.root.after(0, lambda: app.log(f"开始查询余额（全币种）：{len(accounts)} 账号，统计币种={coin}"))

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
                    app.root.after(0, lambda k=acc.api_key, d=per_account_totals: app._replace_account_coin_totals(k, d))
                    msg = f"{prefix} 余额 -> {app._totals_to_text(per_account_totals)}"
                    app.root.after(0, lambda m=msg: app.log(m))
                except Exception as exc:
                    with lock:
                        failed += 1
                    app.root.after(0, lambda m=f"{prefix} 查询失败：{exc}": app.log(m))
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

        top = app._totals_to_text(dict(sorted(totals_all.items(), key=lambda x: (-x[1], x[0]))[:10]))
        summary = f"余额查询结束：成功 {ok}，失败 {failed}，{coin} 合计={app._decimal_to_text(total)}，全币种Top={top}"
        app.root.after(0, lambda: app.log(summary))
    except Exception as exc:
        err_text = str(exc)
        app.root.after(0, lambda m=f"余额查询任务异常终止：{err_text}": app.log(m))
        app.root.after(0, lambda e=err_text: messagebox.showerror("执行异常", e))
    finally:
        app.is_running = False
