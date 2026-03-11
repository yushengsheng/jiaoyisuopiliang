#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import threading
from tkinter import Tk, messagebox, ttk

from api_clients import BinanceClient, BitgetClient, EvmClient
from app_paths import DATA_DIR
from core_models import (
    AccountEntry,
    BgOneToManySettings,
    GlobalSettings,
    OnchainPairEntry,
    OnchainSettings,
    WithdrawRuntimeParams,
)
from page_bitget import BitgetOneToManyPage
from page_onchain import OnchainTransferPage
from shared_utils import LOG_MAX_ROWS, append_log_row, make_scrollbar, parse_worker_threads, random_decimal_between
from stores import AccountStore, BgOneToManyStore, OnchainStore
from ui_dialogs import PasteImportDialog
from withdraw_app import WithdrawApp


def _safe_style_configure(style: ttk.Style, style_name: str, **kwargs) -> None:
    try:
        style.configure(style_name, **kwargs)
    except Exception:
        pass


def _safe_style_map(style: ttk.Style, style_name: str, **kwargs) -> None:
    try:
        style.map(style_name, **kwargs)
    except Exception:
        pass


def _apply_apple_theme(root: Tk) -> None:
    style = ttk.Style(root)
    if "aqua" in style.theme_names():
        try:
            style.theme_use("aqua")
        except Exception:
            pass

    try:
        root.configure(bg="#f5f5f7")
    except Exception:
        pass

    _safe_style_configure(style, "Hero.TLabel", foreground="#1d1d1f", font=("PingFang SC", 20, "bold"))
    _safe_style_configure(style, "Subtle.TLabel", foreground="#6e6e73")
    _safe_style_configure(style, "Value.TLabel", foreground="#0b62c4")
    _safe_style_configure(style, "Danger.TLabel", foreground="#a94442")
    _safe_style_configure(style, "Action.TButton", font=("PingFang SC", 12, "bold"), padding=(10, 4))
    _safe_style_configure(style, "Menu.TNotebook.Tab", font=("PingFang SC", 15, "bold"), padding=(14, 8))
    _safe_style_map(
        style,
        "Menu.TNotebook.Tab",
        foreground=[("selected", "#c1272d"), ("active", "#aa141b"), ("!disabled", "#c1272d")],
    )


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    root = Tk()
    try:
        _apply_apple_theme(root)
    except Exception:
        pass
    WithdrawApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
