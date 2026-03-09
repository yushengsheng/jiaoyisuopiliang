#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import random
from datetime import datetime
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from tkinter import END, Scrollbar


LOG_MAX_ROWS = 500


def parse_worker_threads(value, default: int = 2) -> int:
    try:
        return max(1, int(str(value).strip()))
    except Exception:
        return max(1, int(default))


def append_log_row(log_tree, text: str, max_rows: int = LOG_MAX_ROWS) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_tree.insert("", END, values=(now, text))
    rows = list(log_tree.get_children())
    overflow = len(rows) - max_rows
    if overflow > 0:
        log_tree.delete(*rows[:overflow])
        rows = rows[overflow:]
    if rows:
        log_tree.see(rows[-1])


def make_scrollbar(parent, orient, command):
    bar = Scrollbar(parent, orient=orient, command=command, width=14)
    for k, v in {
        "bg": "#bcbcbc",
        "activebackground": "#8f8f8f",
        "troughcolor": "#ececec",
        "relief": "raised",
        "bd": 1,
    }.items():
        try:
            bar.configure(**{k: v})
        except Exception:
            pass
    return bar


def random_decimal_between(low: Decimal, high: Decimal, unit: Decimal = Decimal("0.01")) -> Decimal:
    low_i = int((low / unit).to_integral_value(rounding=ROUND_CEILING))
    high_i = int((high / unit).to_integral_value(rounding=ROUND_FLOOR))
    if low_i > high_i:
        raise RuntimeError("随机金额范围至少要包含 0.01")
    return (unit * Decimal(random.randint(low_i, high_i))).quantize(unit)


def decimal_to_text(value: Decimal) -> str:
    text = format(value.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def mask_text(value: str, head: int = 6, tail: int = 4) -> str:
    if len(value) <= head + tail:
        return "*" * len(value)
    return f"{value[:head]}...{value[-tail:]}"
