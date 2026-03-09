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


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    root = Tk()
    style = ttk.Style(root)
    if "aqua" in style.theme_names():
        style.theme_use("aqua")
    style.configure("Menu.TNotebook.Tab", font=("PingFang SC", 15, "bold"), foreground="#c1272d", padding=(14, 8))
    style.map(
        "Menu.TNotebook.Tab",
        foreground=[("selected", "#c1272d"), ("active", "#aa141b"), ("!disabled", "#c1272d")],
    )
    style.configure("Action.TButton", font=("PingFang SC", 12, "bold"), foreground="#6c2dc7", padding=(10, 4))
    style.map(
        "Action.TButton",
        foreground=[("pressed", "#5923a6"), ("active", "#5923a6"), ("!disabled", "#6c2dc7")],
    )
    WithdrawApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
