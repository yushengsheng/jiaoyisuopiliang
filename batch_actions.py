#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import threading
from tkinter import messagebox

from core_models import AccountEntry


def _query_balance_stat_coin(app) -> str:
    current = ""
    if hasattr(app, "coin_var"):
        try:
            current = app.coin_var.get().strip().upper()
        except Exception:
            current = ""
    if current:
        return current
    settings = getattr(getattr(app, "store", None), "settings", None)
    saved = str(getattr(settings, "coin", "") or "").strip().upper()
    if saved:
        return saved
    defaults = list(getattr(app, "DEFAULT_COIN_OPTIONS", ["USDT"]))
    return str(defaults[0] if defaults else "USDT").strip().upper()


def _has_submitted_records(app) -> bool:
    if app._is_one_to_many_mode():
        return any(app._display_account_status(addr) == "submitted" for addr in app.store.one_to_many_addresses)
    return any(app.account_withdraw_status.get(acc.api_key, "") == "submitted" for acc in app.store.accounts)


def start_batch_withdraw(app) -> None:
    if app.is_running:
        messagebox.showwarning("提示", "已有任务在运行")
        return

    params = app._validate_withdraw_params()
    if not params:
        return
    dry_run = bool(app.dry_run_var.get())
    one_to_many = app._is_one_to_many_mode()

    if one_to_many:
        source = app._build_source_account(with_message=True)
        if not source:
            return
        addresses = app._checked_one_to_many_addresses()
        if not addresses:
            messagebox.showwarning("提示", "请先勾选至少一个提现地址")
            return
        if app._is_amount_all(params.amount) and len(addresses) > 1:
            messagebox.showerror("参数错误", "1对多模式下，数量为“全部”时只能勾选 1 个地址")
            return
        accounts = [AccountEntry(api_key=source.api_key, api_secret=source.api_secret, address=addr) for addr in addresses]
        count_label = f"地址数：{len(addresses)}（同一提现账号）"
        mode_label = "1对多"
    else:
        accounts = app._checked_accounts()
        if not accounts:
            messagebox.showwarning("提示", "请先勾选至少一个账号")
            return
        count_label = f"账号数：{len(accounts)}"
        mode_label = "多对多"

    if not dry_run:
        amount_text = params.amount if not app._is_amount_all(params.amount) else f"{app.AMOUNT_ALL_LABEL}(按账号可用余额)"
        if params.random_enabled and params.random_min is not None and params.random_max is not None:
            amount_text = f"随机 {params.random_min:.2f} ~ {params.random_max:.2f}"
        text = (
            f"即将执行真实提现：\n"
            f"模式：{mode_label}\n"
            f"{count_label}\n"
            f"币种：{params.coin}\n"
            f"数量：{amount_text}\n"
            f"执行间隔：{params.delay} 秒\n"
            f"执行线程数：{params.threads}\n"
            f"网络：{params.network}\n\n"
            "确认继续？"
        )
        if not messagebox.askyesno("高风险确认", text):
            app.log("用户取消了真实提现")
            return

    app.is_running = True
    t = threading.Thread(
        target=app._run_withdraw,
        args=(accounts, params, dry_run, one_to_many),
        daemon=True,
    )
    t.start()


def start_retry_failed(app) -> None:
    if app.is_running:
        messagebox.showwarning("提示", "已有任务在运行")
        return

    params = app._validate_withdraw_params()
    if not params:
        return
    dry_run = bool(app.dry_run_var.get())
    one_to_many = app._is_one_to_many_mode()
    accounts = app._failed_accounts_for_retry()
    if not accounts:
        tip = "当前没有失败账号可重试"
        if _has_submitted_records(app):
            tip = "当前没有失败账号可重试，存在确认中的记录，请等待自动确认完成"
        messagebox.showwarning("提示", tip)
        return
    if one_to_many and app._is_amount_all(params.amount) and len(accounts) > 1:
        messagebox.showerror("参数错误", "重试模式下，数量为“全部”时只能处理 1 个失败地址")
        return

    mode_label = "1对多" if one_to_many else "多对多"
    count_label = f"地址数：{len(accounts)}（同一提现账号）" if one_to_many else f"账号数：{len(accounts)}"
    if not dry_run:
        amount_text = params.amount if not app._is_amount_all(params.amount) else f"{app.AMOUNT_ALL_LABEL}(按账号可用余额)"
        if params.random_enabled and params.random_min is not None and params.random_max is not None:
            amount_text = f"随机 {params.random_min:.2f} ~ {params.random_max:.2f}"
        text = (
            f"即将重试失败提现：\n"
            f"模式：{mode_label}\n"
            f"{count_label}\n"
            f"币种：{params.coin}\n"
            f"数量：{amount_text}\n"
            f"执行间隔：{params.delay} 秒\n"
            f"执行线程数：{params.threads}\n"
            f"网络：{params.network}\n\n"
            "确认继续？"
        )
        if not messagebox.askyesno("重试确认", text):
            app.log("用户取消了失败重试")
            return

    app.log(f"开始重试失败账号：mode={mode_label}，数量={len(accounts)}")
    app.is_running = True
    t = threading.Thread(
        target=app._run_withdraw,
        args=(accounts, params, dry_run, one_to_many),
        daemon=True,
    )
    t.start()


def start_query_balance(app) -> None:
    if app.is_running:
        messagebox.showwarning("提示", "已有任务在运行")
        return

    coin = _query_balance_stat_coin(app)

    if app._is_one_to_many_mode():
        source = app._build_source_account(with_message=False)
        if not source:
            messagebox.showwarning("提示", "无数据可查，至少需要 1 条查询数据")
            return
        accounts = [source]
    else:
        accounts = app._checked_accounts()
        if not accounts:
            messagebox.showwarning("提示", "请先勾选至少一个账号")
            return

    app.is_running = True
    t = threading.Thread(target=app._run_query_balance, args=(accounts, coin), daemon=True)
    t.start()
