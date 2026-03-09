#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from decimal import Decimal
from tkinter import messagebox

from core_models import AccountEntry


def on_mode_var_changed(app, *_args) -> None:
    mode = app.mode_var.get().strip()
    if mode not in {app.MODE_M2M, app.MODE_1M}:
        app.mode_var.set(app.MODE_M2M)
        mode = app.MODE_M2M
    app.start_refresh_network_options()
    if hasattr(app, "import_hint_label"):
        if mode == app.MODE_1M:
            text = "提示：点击账号列表后可按 Cmd+V / Ctrl+V 粘贴提现地址（每行一个地址）"
        else:
            text = "提示：点击账号列表后可直接按 Cmd+V / Ctrl+V 粘贴导入"
        app.import_hint_label.configure(text=text)
    app._update_source_balance_display()
    app._apply_setting_layout(compact=bool(app._compact_mode))
    app._refresh_tree()
    app._resize_tree_columns()


def on_source_api_changed(app, *_args) -> None:
    app._update_source_balance_display()


def update_source_balance_display(app) -> None:
    if not app._is_one_to_many_mode():
        app.source_balance_var.set("-")
        return
    app.source_balance_var.set(app._one_to_many_balance_text())


def on_coin_var_changed(app, *_args) -> None:
    if hasattr(app, "network_box"):
        app.start_refresh_network_options()


def update_coin_options_from_totals(app, totals: dict[str, Decimal]) -> None:
    app.coin_balance_totals = dict(totals)
    ranked = sorted(totals.items(), key=lambda x: (-x[1], x[0]))
    options = [coin for coin, _ in ranked]
    current = app.coin_var.get().strip().upper()
    fallback = current or (app.store.settings.coin or "USDT")
    if fallback and fallback not in options:
        options.insert(0, fallback)
    if not options:
        options = ["USDT"]
    app.coin_box.configure(values=options)
    if current and current in options:
        app.coin_var.set(current)
    else:
        app.coin_var.set(options[0])


def current_coin(app) -> str:
    return app.coin_var.get().strip().upper()


def build_source_account(app, with_message: bool = False) -> AccountEntry | None:
    api_key = app.source_api_key_var.get().strip()
    api_secret = app.source_api_secret_var.get().strip()
    if not api_key or not api_secret:
        if with_message:
            messagebox.showerror("参数错误", "1对多模式请填写提现账号 API Key 与 API Secret")
        return None
    return AccountEntry(api_key=api_key, api_secret=api_secret, address="N/A")


def pick_meta_account(app) -> AccountEntry | None:
    if app._is_one_to_many_mode():
        return app._build_source_account(with_message=False)
    checked = app._checked_accounts()
    if checked:
        return checked[0]
    if app.store.accounts:
        return app.store.accounts[0]
    return None


def all_balances_text(app, api_key: str) -> str:
    totals = app.account_coin_balance_cache.get(api_key, {})
    return app._totals_to_text(totals)


def one_to_many_balance_text(app) -> str:
    source_key = app.source_api_key_var.get().strip()
    if not source_key:
        return "-"
    return app._all_balances_text(source_key)


def withdraw_status_text(app, api_key: str) -> str:
    status = app.account_withdraw_status.get(api_key, "")
    if status == "waiting":
        return "等待中"
    if status == "running":
        return "进行中"
    if status == "success":
        return "完成"
    if status == "failed":
        return "失败"
    return "-"


def withdraw_status_tag(status: str) -> str:
    if status == "waiting":
        return "st_waiting"
    if status == "running":
        return "st_running"
    if status == "success":
        return "st_success"
    if status == "failed":
        return "st_failed"
    return ""


def set_account_status(app, api_key: str, status: str) -> None:
    app.account_withdraw_status[api_key] = status
    row_id = app.row_id_by_api_key.get(api_key)
    if row_id and row_id in app.row_index_map:
        values = list(app.tree.item(row_id, "values"))
        if len(values) >= 7:
            values[5] = app._withdraw_status_text(api_key)
            app.tree.item(row_id, values=values)
        tag = app._withdraw_status_tag(status)
        app.tree.item(row_id, tags=(tag,) if tag else ())


def totals_to_text(app, totals: dict[str, Decimal]) -> str:
    if not totals:
        return "-"
    arr = [(coin, amount) for coin, amount in totals.items() if amount > 0]
    if not arr:
        return "-"
    arr.sort(key=lambda x: (-x[1], x[0]))
    return " | ".join([f"{coin}:{app._decimal_to_text(total)}" for coin, total in arr])


def replace_account_coin_totals(app, api_key: str, totals: dict[str, Decimal]) -> None:
    app.account_coin_balance_cache[api_key] = dict(totals)
    if app._is_one_to_many_mode() and api_key == app.source_api_key_var.get().strip():
        app._update_source_balance_display()
        app._refresh_tree()
        return
    row_id = app.row_id_by_api_key.get(api_key)
    if row_id and row_id in app.row_index_map:
        values = list(app.tree.item(row_id, "values"))
        if len(values) >= 7:
            values[6] = app._all_balances_text(api_key)
            app.tree.item(row_id, values=values)
