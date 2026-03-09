#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from tkinter import messagebox

from core_models import AccountEntry, WithdrawRuntimeParams
from shared_utils import random_decimal_between


def is_amount_all(value: str, label: str, aliases: set[str]) -> bool:
    s = (value or "").strip()
    return s == label or s.upper() in aliases


def random_withdraw_decimal(low: Decimal, high: Decimal) -> Decimal:
    return random_decimal_between(low, high)


def resolve_withdraw_amount(app, account: AccountEntry, params: WithdrawRuntimeParams, dry_run: bool = False) -> str:
    if params.random_enabled and params.random_min is not None and params.random_max is not None:
        val = random_withdraw_decimal(params.random_min, params.random_max)
        if val <= 0:
            raise RuntimeError("随机金额生成失败：结果必须大于 0")
        return f"{val:.2f}"

    if is_amount_all(params.amount, app.AMOUNT_ALL_LABEL, app.AMOUNT_ALL_ALIASES):
        if dry_run:
            return f"{app.AMOUNT_ALL_LABEL}(模拟)"
        free, _locked = app.client.get_spot_balance(account, coin=params.coin)
        if free <= 0:
            raise RuntimeError(f"{params.coin} 可用余额为 0")
        return app._decimal_to_text(free)

    return params.amount


def validate_withdraw_params(app) -> WithdrawRuntimeParams | None:
    coin = app.coin_var.get().strip().upper()
    amount_raw = app.amount_var.get().strip()
    network = app.network_var.get().strip().upper()
    amount_mode = app._amount_mode()
    random_enabled = amount_mode == app.AMOUNT_MODE_RANDOM

    if not coin:
        messagebox.showerror("参数错误", "提现币种不能为空")
        return None
    if not network:
        messagebox.showerror("参数错误", "未设置提现网络，无法提现")
        return None

    if amount_mode == app.AMOUNT_MODE_ALL:
        amount = app.AMOUNT_ALL_LABEL
    else:
        if not amount_raw and amount_mode == app.AMOUNT_MODE_FIXED:
            messagebox.showerror("参数错误", "提现数量不能为空")
            return None
        try:
            if amount_mode == app.AMOUNT_MODE_FIXED:
                v = Decimal(amount_raw)
                if v <= 0:
                    raise InvalidOperation
                amount = app._decimal_to_text(v)
            else:
                amount = amount_raw or "0"
        except Exception:
            messagebox.showerror("参数错误", "固定数量必须是大于 0 的数字")
            return None

    random_min: Decimal | None = None
    random_max: Decimal | None = None
    if random_enabled:
        min_raw = app.random_min_var.get().strip()
        max_raw = app.random_max_var.get().strip()
        try:
            random_min = Decimal(min_raw)
            random_max = Decimal(max_raw)
        except Exception:
            messagebox.showerror("参数错误", "随机金额最小值/最大值格式错误")
            return None
        if random_min <= 0 or random_max <= 0:
            messagebox.showerror("参数错误", "随机金额最小值和最大值必须大于 0")
            return None
        if random_max < random_min:
            messagebox.showerror("参数错误", "随机金额最大值必须大于或等于最小值")
            return None

    try:
        delay = max(0.0, float(app.delay_var.get()))
    except Exception:
        messagebox.showerror("参数错误", "账号间隔秒数格式错误")
        return None

    try:
        threads = max(1, int(str(app.threads_var.get()).strip()))
    except Exception:
        messagebox.showerror("参数错误", "执行线程数格式错误")
        return None

    return WithdrawRuntimeParams(
        coin=coin,
        amount=amount,
        network=network,
        delay=delay,
        threads=threads,
        random_enabled=random_enabled,
        random_min=random_min,
        random_max=random_max,
    )
