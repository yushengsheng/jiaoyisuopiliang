#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from tkinter import messagebox

from core_models import GlobalSettings


def load_data(app) -> None:
    try:
        app.store.load()
        loaded_mode = str(app.store.settings.mode or "").strip().upper()
        if loaded_mode in {"1M", "1TO MANY", "1TO_MANY", app.MODE_1M.upper()}:
            app.mode_var.set(app.MODE_1M)
        else:
            app.mode_var.set(app.MODE_M2M)
        app.coin_var.set(app.store.settings.coin or "USDT")
        app.network_var.set(app.store.settings.network or "")
        saved_amount = app.store.settings.amount or ""
        app.amount_var.set("" if app._is_amount_all(saved_amount) else saved_amount)
        app.random_min_var.set(app.store.settings.random_amount_min or "")
        app.random_max_var.set(app.store.settings.random_amount_max or "")
        if bool(app.store.settings.random_amount_enabled):
            app.amount_mode_var.set(app.AMOUNT_MODE_RANDOM)
        elif app._is_amount_all(saved_amount):
            app.amount_mode_var.set(app.AMOUNT_MODE_ALL)
        else:
            app.amount_mode_var.set(app.AMOUNT_MODE_FIXED)
        app.source_api_key_var.set(app.store.one_to_many_source_api_key or "")
        app.source_api_secret_var.set(app.store.one_to_many_source_api_secret or "")
        app.delay_var.set(app.store.settings.delay_seconds)
        app.threads_var.set(str(max(1, int(app.store.settings.worker_threads or 1))))
        app.dry_run_var.set(app.store.settings.dry_run)
        app._update_source_balance_display()
        app._update_coin_options_from_totals({})
        app._on_mode_var_changed()
        app.log("配置加载完成")
    except Exception as exc:
        messagebox.showerror("加载失败", str(exc))


def apply_settings_to_store(app) -> bool:
    mode = app.mode_var.get().strip()
    if mode not in {app.MODE_M2M, app.MODE_1M}:
        mode = app.MODE_M2M
        app.mode_var.set(mode)
    coin = app.coin_var.get().strip().upper()
    network = app.network_var.get().strip().upper()
    amount_mode = app._amount_mode()
    amount = app.amount_var.get().strip()
    random_enabled = amount_mode == app.AMOUNT_MODE_RANDOM
    if amount_mode == app.AMOUNT_MODE_ALL:
        amount = app.AMOUNT_ALL_LABEL

    if not coin:
        messagebox.showerror("参数错误", "提现币种不能为空")
        return False

    try:
        delay = max(0.0, float(app.delay_var.get()))
    except Exception:
        messagebox.showerror("参数错误", "账号间隔秒数格式无效")
        return False

    try:
        threads = max(1, int(str(app.threads_var.get()).strip()))
    except Exception:
        messagebox.showerror("参数错误", "执行线程数必须是大于 0 的整数")
        return False

    random_min = app.random_min_var.get().strip()
    random_max = app.random_max_var.get().strip()

    app.store.settings = GlobalSettings(
        coin=coin,
        network=network,
        amount=amount,
        delay_seconds=delay,
        worker_threads=threads,
        mode=mode,
        random_amount_enabled=random_enabled,
        random_amount_min=random_min,
        random_amount_max=random_max,
        dry_run=bool(app.dry_run_var.get()),
    )
    app.store.one_to_many_source_api_key = app.source_api_key_var.get().strip()
    app.store.one_to_many_source_api_secret = app.source_api_secret_var.get().strip()
    return True
