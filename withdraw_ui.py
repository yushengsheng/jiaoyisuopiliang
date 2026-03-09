#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from tkinter import BOTH, LEFT, RIGHT, VERTICAL, W, Menu
from tkinter import ttk

from page_bitget import BitgetOneToManyPage
from page_onchain import OnchainTransferPage


def build_ui(app) -> None:
    shell = ttk.Frame(app.root, padding=8)
    shell.pack(fill=BOTH, expand=True)

    topbar = ttk.Frame(shell)
    topbar.pack(fill="x", pady=(0, 6))
    ip_box = ttk.Frame(topbar)
    ip_box.pack(side=RIGHT)
    ttk.Label(ip_box, text="当前公网IP").pack(side=LEFT)
    ttk.Label(ip_box, textvariable=app.ip_var, foreground="#0b62c4").pack(side=LEFT, padx=(8, 0))
    app.btn_detect_ip = ttk.Button(ip_box, text="检测IP", command=app.start_detect_ip)
    app.btn_detect_ip.pack(side=LEFT, padx=(10, 0))

    app.menu_tabs = ttk.Notebook(shell, style="Menu.TNotebook")
    app.menu_tabs.pack(fill=BOTH, expand=True)

    main = ttk.Frame(app.menu_tabs, padding=10)
    app.menu_tabs.add(main, text=app.MENU_ITEMS[0])
    for title in app.MENU_ITEMS[1:]:
        page = ttk.Frame(app.menu_tabs, padding=16)
        app.menu_tabs.add(page, text=title)
        if title == "bg交易所1对多":
            app.bg_page = BitgetOneToManyPage(page)
        elif title == "链上":
            app.onchain_page = OnchainTransferPage(page)
        else:
            build_waiting_page(app, page, title)

    warning = "⚠️ 提现属于真实资金操作：默认模拟执行，确认参数无误后再关闭模拟。"
    ttk.Label(main, text=warning, foreground="#a94442").pack(anchor=W, pady=(0, 8))

    setting = ttk.LabelFrame(main, text="统一配置（所有选中账号共用）", padding=10)
    setting.pack(fill="x", pady=(0, 10))

    app.setting_frame = setting

    app.lbl_mode = ttk.Label(setting, text="提现模式*")
    app.mode_box = ttk.Combobox(setting, textvariable=app.mode_var, values=[app.MODE_M2M, app.MODE_1M], width=10, state="readonly")

    app.lbl_coin = ttk.Label(setting, text="提现币种*")
    app.coin_box = ttk.Combobox(setting, textvariable=app.coin_var, width=12)
    app.coin_box.configure(values=["USDT"])

    app.lbl_network = ttk.Label(setting, text="提现网络")
    app.network_box = ttk.Combobox(setting, textvariable=app.network_var, values=app.NETWORK_OPTIONS, width=14)

    app.lbl_amount = ttk.Label(setting, text="提现数量*")
    app.amount_ctrl = ttk.Frame(setting)
    app.amount_mode_box = ttk.Combobox(
        app.amount_ctrl,
        textvariable=app.amount_mode_var,
        values=[app.AMOUNT_MODE_FIXED, app.AMOUNT_MODE_RANDOM, app.AMOUNT_MODE_ALL],
        width=8,
        state="readonly",
    )
    app.ent_amount = ttk.Entry(app.amount_ctrl, textvariable=app.amount_var, width=10)
    app.ent_random_min = ttk.Entry(app.amount_ctrl, textvariable=app.random_min_var, width=8)
    app.lbl_random_sep = ttk.Label(app.amount_ctrl, text="~")
    app.ent_random_max = ttk.Entry(app.amount_ctrl, textvariable=app.random_max_var, width=8)
    app.lbl_amount_all_hint = ttk.Label(app.amount_ctrl, text="按可用余额", foreground="#666")
    app._apply_amount_input_layout()

    app.chk_dry_run = ttk.Checkbutton(setting, text="模拟执行", variable=app.dry_run_var)

    app.lbl_delay = ttk.Label(setting, text="执行间隔(秒)")
    app.ent_delay = ttk.Entry(setting, textvariable=app.delay_var, width=12)
    app.lbl_threads = ttk.Label(setting, text="执行线程数")
    app.spin_threads = ttk.Spinbox(setting, from_=1, to=64, textvariable=app.threads_var, width=12)

    app.lbl_source_api_key = ttk.Label(setting, text="提现账号API Key*")
    app.ent_source_api_key = ttk.Entry(setting, textvariable=app.source_api_key_var, width=36)
    app.lbl_source_api_secret = ttk.Label(setting, text="提现账号API Secret*")
    app.ent_source_api_secret = ttk.Entry(setting, textvariable=app.source_api_secret_var, width=36, show="*")
    app.lbl_source_balance_title = ttk.Label(setting, text="提现账号余额")
    app.lbl_source_balance_val = ttk.Label(setting, textvariable=app.source_balance_var, foreground="#0b62c4")

    app.lbl_batch_hint = ttk.Label(setting, text="批量操作按勾选账号执行", foreground="#666")
    app._apply_setting_layout(compact=False)

    app.table_wrap = ttk.Frame(main)
    app.table_wrap.pack(fill=BOTH, expand=True)
    app.table_wrap.columnconfigure(0, weight=1)
    app.table_wrap.rowconfigure(0, weight=1)

    columns = ("checked", "idx", "api_key", "api_secret", "address", "status", "balance")
    app.tree = ttk.Treeview(app.table_wrap, columns=columns, show="headings", selectmode="extended", height=18)
    app.tree.heading("checked", text="勾选")
    app.tree.heading("idx", text="编号")
    app.tree.heading("api_key", text="API Key(掩码)")
    app.tree.heading("api_secret", text="API Secret(掩码)")
    app.tree.heading("address", text="提现地址")
    app.tree.heading("status", text="执行状态")
    app.tree.heading("balance", text="账号余额(全币种)")

    app.tree.column("checked", width=42, anchor="center")
    app.tree.column("idx", width=42, anchor="center")
    app.tree.column("api_key", width=170, anchor="w")
    app.tree.column("api_secret", width=170, anchor="w")
    app.tree.column("address", width=300, anchor="w")
    app.tree.column("status", width=100, anchor="center")
    app.tree.column("balance", width=220, anchor="w")
    app.tree.tag_configure("st_waiting", foreground="#8a6d3b", background="#fff7e0")
    app.tree.tag_configure("st_running", foreground="#1d5fbf", background="#eaf2ff")
    app.tree.tag_configure("st_success", foreground="#1b7f3b", background="#eaf8ef")
    app.tree.tag_configure("st_failed", foreground="#b02a37", background="#fdecef")

    app.tree_ybar = app._make_scrollbar(app.table_wrap, orient=VERTICAL, command=app.tree.yview)
    app.tree_xbar = app._make_scrollbar(app.table_wrap, orient="horizontal", command=app.tree.xview)
    app.tree.configure(yscrollcommand=app.tree_ybar.set, xscrollcommand=app.tree_xbar.set)
    app.tree.grid(row=0, column=0, sticky="nsew")
    app.tree_ybar.grid(row=0, column=1, sticky="ns")
    app.tree_xbar.grid(row=1, column=0, sticky="ew")

    app.tree.bind("<Double-Button-1>", app._on_tree_click, add="+")
    app.tree.bind("<Button-2>", app._on_tree_right_click, add="+")
    app.tree.bind("<Button-3>", app._on_tree_right_click, add="+")
    app.tree.bind("<Control-Button-1>", app._on_tree_right_click, add="+")
    app.tree.bind("<Command-v>", app._on_tree_paste)
    app.tree.bind("<Control-v>", app._on_tree_paste)
    app.row_menu = Menu(app.root, tearoff=0)
    app.row_menu.add_command(label="查询余额（当前行）", command=app.start_query_balance_current_row)
    app.row_menu.add_command(label="提现（当前行）", command=app.start_withdraw_current_row)
    app.row_menu.add_separator()
    app.row_menu.add_command(label="删除（当前行）", command=app.delete_current_row)
    app.mode_var.trace_add("write", app._on_mode_var_changed)
    app.amount_mode_var.trace_add("write", app._on_amount_mode_changed)
    app.coin_var.trace_add("write", app._on_coin_var_changed)
    app.source_api_key_var.trace_add("write", app._on_source_api_changed)
    app.import_hint_label = ttk.Label(main, text="", foreground="#666")
    app.import_hint_label.pack(anchor=W, pady=(4, 0))

    action1 = ttk.Frame(main)
    action1.pack(fill="x", pady=10)
    ttk.Button(action1, text="粘贴导入", command=app.import_from_paste).pack(side=LEFT)
    ttk.Button(action1, text="导入 TXT", command=app.import_txt).pack(side=LEFT, padx=(8, 0))
    ttk.Button(action1, text="导出 TXT", command=app.export_txt).pack(side=LEFT, padx=(8, 0))
    ttk.Button(action1, text="全选/取消全选", command=app.toggle_check_all).pack(side=LEFT, padx=(8, 0))
    ttk.Button(action1, text="删除选中", command=app.delete_selected).pack(side=LEFT, padx=(8, 0))
    ttk.Button(action1, text="保存配置", command=app.save_all).pack(side=LEFT, padx=(8, 0))

    action2 = ttk.Frame(main)
    action2.pack(fill="x", pady=(0, 10))
    ttk.Button(action2, text="按余额刷新币种", command=app.start_refresh_coin_options).pack(side=LEFT)
    ttk.Button(action2, text="查询余额（全币种）", command=app.start_query_balance).pack(side=LEFT, padx=(8, 0))
    ttk.Button(action2, text="执行批量提现", style="Action.TButton", command=app.start_batch_withdraw).pack(side=RIGHT)
    ttk.Button(action2, text="失败重试", style="Action.TButton", command=app.start_retry_failed).pack(side=RIGHT, padx=(8, 0))

    app.log_box = ttk.LabelFrame(main, text="执行日志", padding=8)
    app.log_box.pack(fill=BOTH, expand=False)
    app.log_box.columnconfigure(0, weight=1)
    app.log_box.rowconfigure(0, weight=1)
    app.log_tree = ttk.Treeview(app.log_box, columns=("time", "msg"), show="headings", height=10)
    app.log_tree.heading("time", text="时间")
    app.log_tree.heading("msg", text="日志")
    app.log_tree.column("time", width=170, anchor="center")
    app.log_tree.column("msg", width=980, anchor="w")

    app.log_ybar = app._make_scrollbar(app.log_box, orient=VERTICAL, command=app.log_tree.yview)
    app.log_xbar = app._make_scrollbar(app.log_box, orient="horizontal", command=app.log_tree.xview)
    app.log_tree.configure(yscrollcommand=app.log_ybar.set, xscrollcommand=app.log_xbar.set)
    app.log_tree.grid(row=0, column=0, sticky="nsew")
    app.log_ybar.grid(row=0, column=1, sticky="ns")
    app.log_xbar.grid(row=1, column=0, sticky="ew")

    app.table_wrap.bind("<Configure>", app._on_table_resize)
    app.log_box.bind("<Configure>", app._on_log_resize)
    app.root.bind("<Configure>", app._on_root_resize, add="+")
    app.root.after_idle(app._resize_tree_columns)
    app.root.after_idle(app._on_log_resize)
    app.root.after_idle(app._on_root_resize)


def build_waiting_page(_app, page: ttk.Frame, title: str) -> None:
    box = ttk.LabelFrame(page, text=title, padding=16)
    box.pack(fill=BOTH, expand=True)
    ttk.Label(box, text=f"{title}：等待开发", foreground="#666").pack(anchor=W)
    ttk.Label(box, text="当前已实现功能：币安提现、bg交易所1对多、链上(EVM)批量转账。").pack(anchor=W, pady=(8, 0))


def on_root_resize(app, _event=None) -> None:
    width = app.root.winfo_width()
    compact = width < 1220
    if app._compact_mode == compact:
        return
    app._apply_setting_layout(compact=compact)
    app._resize_tree_columns()
    app._on_log_resize()


def apply_amount_input_layout(app) -> None:
    if not hasattr(app, "amount_mode_box"):
        return
    for w in (app.amount_mode_box, app.ent_amount, app.ent_random_min, app.lbl_random_sep, app.ent_random_max, app.lbl_amount_all_hint):
        w.pack_forget()

    app.amount_mode_box.pack(side=LEFT)
    mode = app._amount_mode()
    if mode == app.AMOUNT_MODE_FIXED:
        app.ent_amount.pack(side=LEFT, padx=(4, 0))
    elif mode == app.AMOUNT_MODE_RANDOM:
        app.ent_random_min.pack(side=LEFT, padx=(4, 0))
        app.lbl_random_sep.pack(side=LEFT, padx=(2, 2))
        app.ent_random_max.pack(side=LEFT)
    else:
        app.lbl_amount_all_hint.pack(side=LEFT, padx=(4, 0))


def on_amount_mode_changed(app, *_args) -> None:
    app._apply_amount_input_layout()


def apply_setting_layout(app, compact: bool) -> None:
    app._compact_mode = compact
    is_one = app._is_one_to_many_mode()
    widgets = [
        app.lbl_mode,
        app.mode_box,
        app.lbl_coin,
        app.coin_box,
        app.lbl_network,
        app.network_box,
        app.lbl_amount,
        app.amount_ctrl,
        app.chk_dry_run,
        app.lbl_delay,
        app.ent_delay,
        app.lbl_threads,
        app.spin_threads,
        app.lbl_source_api_key,
        app.ent_source_api_key,
        app.lbl_source_api_secret,
        app.ent_source_api_secret,
        app.lbl_source_balance_title,
        app.lbl_source_balance_val,
        app.lbl_batch_hint,
    ]
    for w in widgets:
        w.grid_forget()

    for c in range(12):
        app.setting_frame.columnconfigure(c, weight=0)

    if not compact:
        app.lbl_mode.grid(row=0, column=0, sticky=W)
        app.mode_box.grid(row=0, column=1, sticky=W, padx=(4, 10))
        app.lbl_coin.grid(row=0, column=2, sticky=W)
        app.coin_box.grid(row=0, column=3, sticky=W, padx=(4, 10))
        app.lbl_network.grid(row=0, column=4, sticky=W)
        app.network_box.grid(row=0, column=5, sticky=W, padx=(4, 10))
        app.lbl_amount.grid(row=0, column=6, sticky=W)
        app.amount_ctrl.grid(row=0, column=7, sticky=W, padx=(4, 10))
        app.chk_dry_run.grid(row=0, column=8, sticky=W, padx=(4, 0))

        app.lbl_delay.grid(row=1, column=0, sticky=W, pady=(8, 0))
        app.ent_delay.grid(row=1, column=1, sticky=W, padx=(4, 10), pady=(8, 0))
        app.lbl_threads.grid(row=1, column=2, sticky=W, pady=(8, 0))
        app.spin_threads.grid(row=1, column=3, sticky=W, padx=(4, 10), pady=(8, 0))

        hint_row = 2
        if is_one:
            app.lbl_source_api_key.grid(row=2, column=0, sticky=W, pady=(8, 0))
            app.ent_source_api_key.grid(row=2, column=1, columnspan=5, sticky="ew", padx=(4, 10), pady=(8, 0))
            app.lbl_source_api_secret.grid(row=2, column=6, sticky=W, pady=(8, 0))
            app.ent_source_api_secret.grid(row=2, column=7, columnspan=4, sticky="ew", padx=(4, 0), pady=(8, 0))
            app.lbl_source_balance_title.grid(row=3, column=0, sticky=W, pady=(8, 0))
            app.lbl_source_balance_val.grid(row=3, column=1, columnspan=10, sticky=W, padx=(4, 0), pady=(8, 0))
            app.setting_frame.columnconfigure(1, weight=1)
            app.setting_frame.columnconfigure(7, weight=1)
            hint_row = 4

        app.lbl_batch_hint.grid(row=hint_row, column=0, columnspan=11, sticky=W, pady=(8, 0))
        return

    app.lbl_mode.grid(row=0, column=0, sticky=W)
    app.mode_box.grid(row=0, column=1, sticky="ew", padx=(4, 8))
    app.lbl_coin.grid(row=0, column=2, sticky=W)
    app.coin_box.grid(row=0, column=3, sticky="ew", padx=(4, 0))

    app.lbl_network.grid(row=1, column=0, sticky=W, pady=(8, 0))
    app.network_box.grid(row=1, column=1, sticky="ew", padx=(4, 8), pady=(8, 0))
    app.lbl_amount.grid(row=1, column=2, sticky=W, pady=(8, 0))
    app.amount_ctrl.grid(row=1, column=3, sticky="w", padx=(4, 0), pady=(8, 0))

    app.lbl_delay.grid(row=2, column=0, sticky=W, pady=(8, 0))
    app.ent_delay.grid(row=2, column=1, sticky="ew", padx=(4, 8), pady=(8, 0))
    app.lbl_threads.grid(row=2, column=2, sticky=W, pady=(8, 0))
    app.spin_threads.grid(row=2, column=3, sticky="ew", padx=(4, 0), pady=(8, 0))

    app.chk_dry_run.grid(row=3, column=0, sticky=W, pady=(8, 0))

    next_row = 4
    if is_one:
        app.lbl_source_api_key.grid(row=4, column=0, sticky=W, pady=(8, 0))
        app.ent_source_api_key.grid(row=4, column=1, columnspan=3, sticky="ew", padx=(4, 0), pady=(8, 0))
        app.lbl_source_api_secret.grid(row=5, column=0, sticky=W, pady=(8, 0))
        app.ent_source_api_secret.grid(row=5, column=1, columnspan=3, sticky="ew", padx=(4, 0), pady=(8, 0))
        app.lbl_source_balance_title.grid(row=6, column=0, sticky=W, pady=(8, 0))
        app.lbl_source_balance_val.grid(row=6, column=1, columnspan=3, sticky=W, padx=(4, 0), pady=(8, 0))
        next_row = 7

    app.lbl_batch_hint.grid(row=next_row, column=0, columnspan=4, sticky=W, pady=(8, 0))

    app.setting_frame.columnconfigure(1, weight=1)
    app.setting_frame.columnconfigure(3, weight=1)


def on_table_resize(app, _event=None) -> None:
    app._resize_tree_columns()


def resize_tree_columns(app) -> None:
    if not hasattr(app, "tree"):
        return
    cols = ("checked", "idx", "api_key", "api_secret", "address", "status", "balance")
    min_widths = app.TREE_COL_MIN_WIDTHS
    total_min = sum(min_widths[c] for c in cols)

    width = app.tree.winfo_width()
    if width <= 1 and hasattr(app, "table_wrap"):
        width = app.table_wrap.winfo_width() - 20
    if width <= 0:
        return

    if width <= total_min:
        for c in cols:
            app.tree.column(c, width=min_widths[c], stretch=False)
        return

    extra = width - total_min
    weights = app.TREE_COL_WEIGHTS
    total_weight = sum(weights[c] for c in cols)
    used = 0
    for i, c in enumerate(cols):
        add = extra - used if i == len(cols) - 1 else int(extra * weights[c] / total_weight)
        used += add
        app.tree.column(c, width=min_widths[c] + max(0, add), stretch=True)


def on_log_resize(app, _event=None) -> None:
    if not hasattr(app, "log_tree"):
        return
    width = app.log_tree.winfo_width()
    if width <= 1 and hasattr(app, "log_box"):
        width = app.log_box.winfo_width() - 20
    if width <= 0:
        return
    msg_width = max(300, width - 190)
    app.log_tree.column("time", width=170, stretch=False)
    app.log_tree.column("msg", width=msg_width, stretch=True)
