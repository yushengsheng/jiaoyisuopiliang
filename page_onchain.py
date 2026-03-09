#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import queue
import random
import re
import threading
import time
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal, InvalidOperation
from pathlib import Path
from tkinter import BOTH, END, LEFT, RIGHT, VERTICAL, W, Y, BooleanVar, DoubleVar, Menu, Scrollbar, StringVar
from tkinter import messagebox, ttk

from api_clients import EvmClient
from app_paths import ONCHAIN_DATA_FILE
from core_models import EvmToken, OnchainPairEntry, WithdrawRuntimeParams
from shared_utils import LOG_MAX_ROWS, append_log_row, decimal_to_text, make_scrollbar, mask_text, parse_worker_threads, random_decimal_between
from stores import OnchainStore
from ui_dialogs import PasteImportDialog

class OnchainTransferPage:
    MODE_M2M = "多对多"
    MODE_1M = "1对多"
    MODE_M1 = "多对1"
    AMOUNT_MODE_FIXED = "固定数量"
    AMOUNT_MODE_RANDOM = "随机数"
    AMOUNT_MODE_ALL = "全部"
    AMOUNT_ALL_LABEL = "全部"
    NETWORK_OPTIONS = ["", "ETH", "BSC"]
    MAX_TOKEN_DECIMALS = 36
    TREE_COL_MIN_WIDTHS = {
        "checked": 42,
        "idx": 42,
        "source": 230,
        "source_addr": 170,
        "target": 220,
        "status": 96,
        "balance": 160,
    }
    TREE_COL_WEIGHTS = {
        "checked": 1,
        "idx": 1,
        "source": 4,
        "source_addr": 3,
        "target": 4,
        "status": 2,
        "balance": 3,
    }

    def __init__(self, parent):
        self.parent = parent
        self.root = parent.winfo_toplevel()
        self.store = OnchainStore(ONCHAIN_DATA_FILE)
        self.client = EvmClient()
        self.is_running = False
        self._compact_mode: bool | None = None

        self.row_index_map: dict[str, int] = {}
        self.row_key_by_row_id: dict[str, str] = {}
        self.row_id_by_key: dict[str, str] = {}
        self.checked_row_keys: set[str] = set()
        self.row_status: dict[str, str] = {}
        self.source_balance_cache: dict[str, Decimal] = {}
        self.target_balance_cache: dict[str, Decimal] = {}
        self.source_address_cache: dict[str, str] = {}
        self.source_private_key_cache: dict[str, str] = {}
        self.current_tokens: dict[str, EvmToken] = {}
        self.custom_tokens_by_network: dict[str, dict[str, EvmToken]] = {"ETH": {}, "BSC": {}}
        self.wallet_cache_lock = threading.Lock()

        self.mode_var = StringVar(value=self.MODE_M2M)
        self.network_var = StringVar(value="")
        self.coin_var = StringVar(value="")
        self.symbol_var = StringVar(value="-")
        self.contract_search_var = StringVar(value="")
        self.amount_mode_var = StringVar(value=self.AMOUNT_MODE_FIXED)
        self.amount_var = StringVar(value="")
        self.random_min_var = StringVar(value="")
        self.random_max_var = StringVar(value="")
        self.delay_var = DoubleVar(value=1.0)
        self.threads_var = StringVar(value="2")
        self.dry_run_var = BooleanVar(value=True)
        self.source_credential_var = StringVar(value="")
        self.target_address_var = StringVar(value="")
        self.source_balance_var = StringVar(value="-")
        self.target_balance_var = StringVar(value="-")

        self._build_ui()
        self._load_data()

    def _build_ui(self):
        main = ttk.Frame(self.parent, padding=10)
        main.pack(fill=BOTH, expand=True)

        warning = "⚠️ 链上转账属于真实资金操作：默认模拟执行，确认参数无误后再关闭模拟。"
        ttk.Label(main, text=warning, foreground="#a94442").pack(anchor=W, pady=(0, 8))

        setting = ttk.LabelFrame(main, text="链上批量转账配置（EVM）", padding=10)
        setting.pack(fill="x", pady=(0, 10))
        self.setting_frame = setting

        self.lbl_mode = ttk.Label(setting, text="转账模式*")
        self.mode_box = ttk.Combobox(
            setting,
            textvariable=self.mode_var,
            values=[self.MODE_M2M, self.MODE_1M, self.MODE_M1],
            width=10,
            state="readonly",
        )
        self.lbl_network = ttk.Label(setting, text="网络*")
        self.network_box = ttk.Combobox(
            setting,
            textvariable=self.network_var,
            values=self.NETWORK_OPTIONS,
            width=10,
            state="readonly",
        )
        self.lbl_coin = ttk.Label(setting, text="币种*")
        self.coin_box = ttk.Combobox(setting, textvariable=self.coin_var, width=16, state="readonly")
        self.lbl_contract_search = ttk.Label(setting, text="合约搜索")
        self.ent_contract_search = ttk.Entry(setting, textvariable=self.contract_search_var, width=40)
        self.btn_contract_search = ttk.Button(setting, text="搜索并选择", command=self.search_contract_token)

        self.lbl_amount = ttk.Label(setting, text="转账数量*")
        self.amount_ctrl = ttk.Frame(setting)
        self.amount_mode_box = ttk.Combobox(
            self.amount_ctrl,
            textvariable=self.amount_mode_var,
            values=[self.AMOUNT_MODE_FIXED, self.AMOUNT_MODE_RANDOM, self.AMOUNT_MODE_ALL],
            width=8,
            state="readonly",
        )
        self.ent_amount = ttk.Entry(self.amount_ctrl, textvariable=self.amount_var, width=10)
        self.ent_random_min = ttk.Entry(self.amount_ctrl, textvariable=self.random_min_var, width=8)
        self.lbl_random_sep = ttk.Label(self.amount_ctrl, text="~")
        self.ent_random_max = ttk.Entry(self.amount_ctrl, textvariable=self.random_max_var, width=8)
        self.lbl_amount_all_hint = ttk.Label(self.amount_ctrl, text="按钱包可用余额", foreground="#666")
        self._apply_amount_layout()

        self.chk_dry_run = ttk.Checkbutton(setting, text="模拟执行", variable=self.dry_run_var)
        self.lbl_delay = ttk.Label(setting, text="执行间隔(秒)")
        self.ent_delay = ttk.Entry(setting, textvariable=self.delay_var, width=10)
        self.lbl_threads = ttk.Label(setting, text="执行线程数")
        self.spin_threads = ttk.Spinbox(setting, from_=1, to=64, textvariable=self.threads_var, width=10)

        self.lbl_source_credential = ttk.Label(setting, text="转出钱包私钥/助记词*")
        self.ent_source_credential = ttk.Entry(setting, textvariable=self.source_credential_var)
        self.lbl_source_balance_title = ttk.Label(setting, text="转出钱包余额")
        self.lbl_source_balance_val = ttk.Label(setting, textvariable=self.source_balance_var, foreground="#0b62c4")
        self.lbl_target_address = ttk.Label(setting, text="收款地址*")
        self.ent_target_address = ttk.Entry(setting, textvariable=self.target_address_var)
        self.lbl_target_balance_title = ttk.Label(setting, text="收款地址余额")
        self.lbl_target_balance_val = ttk.Label(setting, textvariable=self.target_balance_var, foreground="#0b62c4")

        self.lbl_batch_hint = ttk.Label(setting, text="批量操作按勾选行执行", foreground="#666")
        self._apply_setting_layout(compact=False)

        self.table_wrap = ttk.Frame(main)
        self.table_wrap.pack(fill=BOTH, expand=True)
        self.table_wrap.columnconfigure(0, weight=1)
        self.table_wrap.rowconfigure(0, weight=1)

        cols = ("checked", "idx", "source", "source_addr", "target", "status", "balance")
        self.tree = ttk.Treeview(self.table_wrap, columns=cols, show="headings", selectmode="extended", height=16)
        self.tree.heading("checked", text="勾选")
        self.tree.heading("idx", text="编号")
        self.tree.heading("source", text="转出凭证(掩码)")
        self.tree.heading("source_addr", text="转出地址")
        self.tree.heading("target", text="接收地址")
        self.tree.heading("status", text="执行状态")
        self.tree.heading("balance", text="余额")

        self.tree.column("checked", width=42, anchor="center")
        self.tree.column("idx", width=42, anchor="center")
        self.tree.column("source", width=260, anchor="w")
        self.tree.column("source_addr", width=180, anchor="w")
        self.tree.column("target", width=260, anchor="w")
        self.tree.column("status", width=110, anchor="center")
        self.tree.column("balance", width=200, anchor="w")
        self.tree.tag_configure("st_waiting", foreground="#8a6d3b", background="#fff7e0")
        self.tree.tag_configure("st_running", foreground="#1d5fbf", background="#eaf2ff")
        self.tree.tag_configure("st_success", foreground="#1b7f3b", background="#eaf8ef")
        self.tree.tag_configure("st_failed", foreground="#b02a37", background="#fdecef")

        self.tree_ybar = self._make_scrollbar(self.table_wrap, orient=VERTICAL, command=self.tree.yview)
        self.tree_xbar = self._make_scrollbar(self.table_wrap, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=self.tree_ybar.set, xscrollcommand=self.tree_xbar.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        self.tree_ybar.grid(row=0, column=1, sticky="ns")
        self.tree_xbar.grid(row=1, column=0, sticky="ew")

        self.tree.bind("<Double-Button-1>", self._on_tree_click, add="+")
        self.tree.bind("<Button-2>", self._on_tree_right_click, add="+")
        self.tree.bind("<Button-3>", self._on_tree_right_click, add="+")
        self.tree.bind("<Control-Button-1>", self._on_tree_right_click, add="+")
        self.tree.bind("<Command-v>", self._on_tree_paste)
        self.tree.bind("<Control-v>", self._on_tree_paste)
        self.row_menu = Menu(self.root, tearoff=0)
        self.row_menu.add_command(label="查询余额（当前行）", command=self.start_query_balance_current_row)
        self.row_menu.add_command(label="执行转账（当前行）", command=self.start_transfer_current_row)
        self.row_menu.add_separator()
        self.row_menu.add_command(label="删除（当前行）", command=self.delete_current_row)

        self.import_hint_label = ttk.Label(main, text="", foreground="#666")
        self.import_hint_label.pack(anchor=W, pady=(4, 0))

        action1 = ttk.Frame(main)
        action1.pack(fill="x", pady=10)
        ttk.Button(action1, text="粘贴导入", command=self.import_from_paste).pack(side=LEFT)
        ttk.Button(action1, text="导入 TXT", command=self.import_txt).pack(side=LEFT, padx=(8, 0))
        ttk.Button(action1, text="导出 TXT", command=self.export_txt).pack(side=LEFT, padx=(8, 0))
        ttk.Button(action1, text="全选/取消全选", command=self.toggle_check_all).pack(side=LEFT, padx=(8, 0))
        ttk.Button(action1, text="删除选中", command=self.delete_selected).pack(side=LEFT, padx=(8, 0))
        ttk.Button(action1, text="保存配置", command=self.save_all).pack(side=LEFT, padx=(8, 0))

        action2 = ttk.Frame(main)
        action2.pack(fill="x", pady=(0, 10))
        ttk.Button(action2, text="查询余额", command=self.start_query_balance).pack(side=LEFT)
        ttk.Button(action2, text="执行批量转账", style="Action.TButton", command=self.start_batch_transfer).pack(side=RIGHT)
        ttk.Button(action2, text="失败重试", style="Action.TButton", command=self.start_retry_failed).pack(side=RIGHT, padx=(8, 0))

        self.log_box = ttk.LabelFrame(main, text="执行日志", padding=8)
        self.log_box.pack(fill=BOTH, expand=False)
        self.log_box.columnconfigure(0, weight=1)
        self.log_box.rowconfigure(0, weight=1)
        self.log_tree = ttk.Treeview(self.log_box, columns=("time", "msg"), show="headings", height=9)
        self.log_tree.heading("time", text="时间")
        self.log_tree.heading("msg", text="日志")
        self.log_tree.column("time", width=170, anchor="center")
        self.log_tree.column("msg", width=950, anchor="w")

        self.log_ybar = self._make_scrollbar(self.log_box, orient=VERTICAL, command=self.log_tree.yview)
        self.log_xbar = self._make_scrollbar(self.log_box, orient="horizontal", command=self.log_tree.xview)
        self.log_tree.configure(yscrollcommand=self.log_ybar.set, xscrollcommand=self.log_xbar.set)
        self.log_tree.grid(row=0, column=0, sticky="nsew")
        self.log_ybar.grid(row=0, column=1, sticky="ns")
        self.log_xbar.grid(row=1, column=0, sticky="ew")

        self.mode_var.trace_add("write", self._on_mode_changed)
        self.network_var.trace_add("write", self._on_network_changed)
        self.coin_var.trace_add("write", self._on_coin_changed)
        self.amount_mode_var.trace_add("write", self._on_amount_mode_changed)
        self.source_credential_var.trace_add("write", self._on_source_or_target_changed)
        self.target_address_var.trace_add("write", self._on_source_or_target_changed)

        self.table_wrap.bind("<Configure>", self._on_table_resize)
        self.log_box.bind("<Configure>", self._on_log_resize)
        self.root.bind("<Configure>", self._on_root_resize, add="+")
        self.root.after_idle(self._on_mode_changed)
        self.root.after_idle(self._resize_tree_columns)
        self.root.after_idle(self._on_log_resize)
        self.root.after_idle(self._on_root_resize)

    @staticmethod
    def _make_scrollbar(parent, orient, command):
        return make_scrollbar(parent, orient, command)

    def _mode(self) -> str:
        m = self.mode_var.get().strip()
        if m not in {self.MODE_M2M, self.MODE_1M, self.MODE_M1}:
            m = self.MODE_M2M
            self.mode_var.set(m)
        return m

    def _is_mode_m2m(self) -> bool:
        return self._mode() == self.MODE_M2M

    def _is_mode_1m(self) -> bool:
        return self._mode() == self.MODE_1M

    def _is_mode_m1(self) -> bool:
        return self._mode() == self.MODE_M1

    @staticmethod
    def _mask(value: str, head: int = 6, tail: int = 4) -> str:
        return mask_text(value, head=head, tail=tail)

    @classmethod
    def _decimal_to_text(cls, v: Decimal) -> str:
        return decimal_to_text(v)

    @staticmethod
    def _factor_by_decimals(decimals: int) -> Decimal:
        if decimals < 0 or decimals > OnchainTransferPage.MAX_TOKEN_DECIMALS:
            raise RuntimeError(f"代币精度超出范围：{decimals}")
        return Decimal(10) ** decimals

    @classmethod
    def _units_to_amount(cls, units: int, decimals: int) -> Decimal:
        return Decimal(units) / cls._factor_by_decimals(decimals)

    @classmethod
    def _amount_to_units(cls, amount: Decimal, decimals: int) -> int:
        val = (amount * cls._factor_by_decimals(decimals)).to_integral_value(rounding=ROUND_FLOOR)
        return int(val)

    def _runtime_worker_threads(self) -> int:
        raw = self.threads_var.get() if hasattr(self, "threads_var") else 2
        return parse_worker_threads(raw, default=2)

    def _mask_credential(self, credential: str) -> str:
        s = credential.strip()
        if re.fullmatch(r"(0x)?[a-fA-F0-9]{64}", s):
            return self._mask(s)
        words = [x for x in s.split() if x]
        if len(words) >= 2:
            return f"{words[0]} {words[1]} ... ({len(words)}词)"
        return self._mask(s, head=4, tail=4)

    @staticmethod
    def _status_text(status: str) -> str:
        if status == "waiting":
            return "等待中"
        if status == "running":
            return "进行中"
        if status == "success":
            return "完成"
        if status == "failed":
            return "失败"
        return "-"

    @staticmethod
    def _status_tag(status: str) -> str:
        if status == "waiting":
            return "st_waiting"
        if status == "running":
            return "st_running"
        if status == "success":
            return "st_success"
        if status == "failed":
            return "st_failed"
        return ""

    @staticmethod
    def _m2m_key(item: OnchainPairEntry) -> str:
        return f"m2m:{item.source}|{item.target}"

    @staticmethod
    def _one_to_many_key(address: str) -> str:
        return f"1m:{address}"

    @staticmethod
    def _many_to_one_key(source: str) -> str:
        return f"m1:{source}"

    @staticmethod
    def _token_identity(token: EvmToken) -> tuple[str, str]:
        return token.symbol.strip().upper(), token.contract.strip().lower()

    def _token_display(self, token: EvmToken) -> str:
        if token.is_native:
            return f"{token.symbol}(原生)"
        return token.symbol

    def _balance_text_for_source(self, source: str) -> str:
        v = self.source_balance_cache.get(source)
        if v is None:
            return "-"
        symbol = self.symbol_var.get().strip() or "-"
        return f"{symbol}:{self._decimal_to_text(v)}"

    def _balance_text_for_target(self, target: str) -> str:
        v = self.target_balance_cache.get(target)
        if v is None:
            return "-"
        symbol = self.symbol_var.get().strip() or "-"
        return f"{symbol}:{self._decimal_to_text(v)}"

    def _source_addr_text(self, source: str) -> str:
        addr = self.source_address_cache.get(source, "")
        return addr if addr else "-"

    def _set_status(self, row_key: str, status: str):
        self.row_status[row_key] = status
        row_id = self.row_id_by_key.get(row_key)
        if not row_id or row_id not in self.row_index_map:
            return
        values = list(self.tree.item(row_id, "values"))
        if len(values) >= 7:
            values[5] = self._status_text(status)
            self.tree.item(row_id, values=values)
        tag = self._status_tag(status)
        self.tree.item(row_id, tags=(tag,) if tag else ())

    def _active_row_keys(self) -> list[str]:
        if self._is_mode_m2m():
            return [self._m2m_key(x) for x in self.store.multi_to_multi_pairs]
        if self._is_mode_1m():
            return [self._one_to_many_key(x) for x in self.store.one_to_many_addresses]
        return [self._many_to_one_key(x) for x in self.store.many_to_one_sources]

    def _refresh_tree(self):
        self.tree.delete(*self.tree.get_children())
        self.row_index_map = {}
        self.row_key_by_row_id = {}
        self.row_id_by_key = {}

        active = set(self._active_row_keys())
        self.checked_row_keys.intersection_update(active)
        self.row_status = {k: v for k, v in self.row_status.items() if k in active}

        mode = self._mode()
        if mode == self.MODE_M2M:
            for i, item in enumerate(self.store.multi_to_multi_pairs, start=1):
                key = self._m2m_key(item)
                row_id = f"onchain_row_{i}"
                self.row_index_map[row_id] = i - 1
                self.row_key_by_row_id[row_id] = key
                self.row_id_by_key[key] = row_id
                checked = "✓" if key in self.checked_row_keys else ""
                st = self.row_status.get(key, "")
                tag = self._status_tag(st)
                self.tree.insert(
                    "",
                    END,
                    iid=row_id,
                    values=(
                        checked,
                        i,
                        self._mask_credential(item.source),
                        self._source_addr_text(item.source),
                        item.target,
                        self._status_text(st),
                        self._balance_text_for_source(item.source),
                    ),
                    tags=(tag,) if tag else (),
                )
            return

        if mode == self.MODE_1M:
            source = self.source_credential_var.get().strip()
            for i, target in enumerate(self.store.one_to_many_addresses, start=1):
                key = self._one_to_many_key(target)
                row_id = f"onchain_row_{i}"
                self.row_index_map[row_id] = i - 1
                self.row_key_by_row_id[row_id] = key
                self.row_id_by_key[key] = row_id
                checked = "✓" if key in self.checked_row_keys else ""
                st = self.row_status.get(key, "")
                tag = self._status_tag(st)
                self.tree.insert(
                    "",
                    END,
                    iid=row_id,
                    values=(
                        checked,
                        i,
                        self._mask_credential(source) if source else "-",
                        self._source_addr_text(source) if source else "-",
                        target,
                        self._status_text(st),
                        self._balance_text_for_target(target),
                    ),
                    tags=(tag,) if tag else (),
                )
            return

        target = self.target_address_var.get().strip()
        for i, source in enumerate(self.store.many_to_one_sources, start=1):
            key = self._many_to_one_key(source)
            row_id = f"onchain_row_{i}"
            self.row_index_map[row_id] = i - 1
            self.row_key_by_row_id[row_id] = key
            self.row_id_by_key[key] = row_id
            checked = "✓" if key in self.checked_row_keys else ""
            st = self.row_status.get(key, "")
            tag = self._status_tag(st)
            self.tree.insert(
                "",
                END,
                iid=row_id,
                values=(
                    checked,
                    i,
                    self._mask_credential(source),
                    self._source_addr_text(source),
                    target if target else "-",
                    self._status_text(st),
                    self._balance_text_for_source(source),
                ),
                tags=(tag,) if tag else (),
            )

    def _selected_indices(self) -> list[int]:
        idxs: list[int] = []
        for row_id in self.tree.selection():
            if row_id in self.row_index_map:
                idxs.append(self.row_index_map[row_id])
        return sorted(set(idxs))

    def _on_tree_click(self, event):
        if self.tree.identify("region", event.x, event.y) != "cell":
            return None
        row_id = self.tree.identify_row(event.y)
        if not row_id:
            return None
        key = self.row_key_by_row_id.get(row_id, "")
        if not key:
            return "break"
        checked = key not in self.checked_row_keys
        if checked:
            self.checked_row_keys.add(key)
        else:
            self.checked_row_keys.discard(key)
        values = list(self.tree.item(row_id, "values"))
        if values:
            values[0] = "✓" if checked else ""
            self.tree.item(row_id, values=values)
        return None

    def _on_tree_right_click(self, event):
        if self.tree.identify("region", event.x, event.y) != "cell":
            return None
        row_id = self.tree.identify_row(event.y)
        if not row_id:
            return None
        self.tree.selection_set(row_id)
        self.tree.focus(row_id)
        try:
            self.row_menu.tk_popup(event.x_root, event.y_root)
        finally:
            try:
                self.row_menu.grab_release()
            except Exception:
                pass
        return "break"

    def _single_selected_index(self) -> int | None:
        idxs = self._selected_indices()
        if not idxs:
            return None
        return idxs[0]

    def _single_row_job(self) -> tuple[str, str, str] | None:
        idx = self._single_selected_index()
        if idx is None:
            return None
        mode = self._mode()
        if mode == self.MODE_M2M:
            if not (0 <= idx < len(self.store.multi_to_multi_pairs)):
                return None
            item = self.store.multi_to_multi_pairs[idx]
            return self._m2m_key(item), item.source, item.target
        if mode == self.MODE_1M:
            if not (0 <= idx < len(self.store.one_to_many_addresses)):
                return None
            target = self.store.one_to_many_addresses[idx]
            source = self.source_credential_var.get().strip()
            return self._one_to_many_key(target), source, target
        if not (0 <= idx < len(self.store.many_to_one_sources)):
            return None
        source = self.store.many_to_one_sources[idx]
        target = self.target_address_var.get().strip()
        return self._many_to_one_key(source), source, target

    def _on_tree_paste(self, _event=None):
        self.import_from_clipboard()
        return "break"

    def _amount_mode(self) -> str:
        m = self.amount_mode_var.get().strip()
        if m not in {self.AMOUNT_MODE_FIXED, self.AMOUNT_MODE_RANDOM, self.AMOUNT_MODE_ALL}:
            m = self.AMOUNT_MODE_FIXED
            self.amount_mode_var.set(m)
        return m

    def _apply_amount_layout(self):
        for w in (self.amount_mode_box, self.ent_amount, self.ent_random_min, self.lbl_random_sep, self.ent_random_max, self.lbl_amount_all_hint):
            w.pack_forget()
        self.amount_mode_box.pack(side=LEFT)
        m = self._amount_mode()
        if m == self.AMOUNT_MODE_FIXED:
            self.ent_amount.pack(side=LEFT, padx=(4, 0))
        elif m == self.AMOUNT_MODE_RANDOM:
            self.ent_random_min.pack(side=LEFT, padx=(4, 0))
            self.lbl_random_sep.pack(side=LEFT, padx=(2, 2))
            self.ent_random_max.pack(side=LEFT)
        else:
            self.lbl_amount_all_hint.pack(side=LEFT, padx=(4, 0))

    def _on_amount_mode_changed(self, *_args):
        self._apply_amount_layout()

    def _apply_setting_layout(self, compact: bool):
        self._compact_mode = compact
        widgets = [
            self.lbl_mode,
            self.mode_box,
            self.lbl_network,
            self.network_box,
            self.lbl_coin,
            self.coin_box,
            self.lbl_contract_search,
            self.ent_contract_search,
            self.btn_contract_search,
            self.lbl_amount,
            self.amount_ctrl,
            self.chk_dry_run,
            self.lbl_delay,
            self.ent_delay,
            self.lbl_threads,
            self.spin_threads,
            self.lbl_source_credential,
            self.ent_source_credential,
            self.lbl_source_balance_title,
            self.lbl_source_balance_val,
            self.lbl_target_address,
            self.ent_target_address,
            self.lbl_target_balance_title,
            self.lbl_target_balance_val,
            self.lbl_batch_hint,
        ]
        for w in widgets:
            w.grid_forget()

        for c in range(12):
            self.setting_frame.columnconfigure(c, weight=0)

        show_source = self._is_mode_1m()
        show_target = self._is_mode_m1()

        if not compact:
            self.lbl_mode.grid(row=0, column=0, sticky=W)
            self.mode_box.grid(row=0, column=1, sticky=W, padx=(4, 10))
            self.lbl_network.grid(row=0, column=2, sticky=W)
            self.network_box.grid(row=0, column=3, sticky=W, padx=(4, 10))
            self.lbl_coin.grid(row=0, column=4, sticky=W)
            self.coin_box.grid(row=0, column=5, sticky=W, padx=(4, 10))
            self.lbl_amount.grid(row=0, column=6, sticky=W)
            self.amount_ctrl.grid(row=0, column=7, sticky=W, padx=(4, 10))
            self.chk_dry_run.grid(row=0, column=8, sticky=W, padx=(4, 0))

            self.lbl_contract_search.grid(row=1, column=0, sticky=W, pady=(8, 0))
            self.ent_contract_search.grid(row=1, column=1, columnspan=5, sticky="ew", padx=(4, 10), pady=(8, 0))
            self.btn_contract_search.grid(row=1, column=6, sticky=W, pady=(8, 0))
            self.lbl_delay.grid(row=1, column=7, sticky=W, pady=(8, 0))
            self.ent_delay.grid(row=1, column=8, sticky=W, padx=(4, 10), pady=(8, 0))
            self.lbl_threads.grid(row=1, column=9, sticky=W, pady=(8, 0))
            self.spin_threads.grid(row=1, column=10, sticky=W, padx=(4, 0), pady=(8, 0))
            self.setting_frame.columnconfigure(1, weight=1)

            row = 2
            if show_source:
                self.lbl_source_credential.grid(row=row, column=0, sticky=W, pady=(8, 0))
                self.ent_source_credential.grid(row=row, column=1, columnspan=7, sticky="ew", padx=(4, 10), pady=(8, 0))
                self.lbl_source_balance_title.grid(row=row, column=8, sticky=W, pady=(8, 0))
                self.lbl_source_balance_val.grid(row=row, column=9, columnspan=2, sticky=W, padx=(4, 0), pady=(8, 0))
                self.setting_frame.columnconfigure(1, weight=1)
                row += 1

            if show_target:
                self.lbl_target_address.grid(row=row, column=0, sticky=W, pady=(8, 0))
                self.ent_target_address.grid(row=row, column=1, columnspan=7, sticky="ew", padx=(4, 10), pady=(8, 0))
                self.lbl_target_balance_title.grid(row=row, column=8, sticky=W, pady=(8, 0))
                self.lbl_target_balance_val.grid(row=row, column=9, columnspan=2, sticky=W, padx=(4, 0), pady=(8, 0))
                self.setting_frame.columnconfigure(1, weight=1)
                row += 1

            self.lbl_batch_hint.grid(row=row, column=0, columnspan=11, sticky=W, pady=(8, 0))
            return

        self.lbl_mode.grid(row=0, column=0, sticky=W)
        self.mode_box.grid(row=0, column=1, sticky="ew", padx=(4, 8))
        self.lbl_network.grid(row=0, column=2, sticky=W)
        self.network_box.grid(row=0, column=3, sticky="ew", padx=(4, 0))

        self.lbl_coin.grid(row=1, column=0, sticky=W, pady=(8, 0))
        self.coin_box.grid(row=1, column=1, sticky="ew", padx=(4, 8), pady=(8, 0))
        self.lbl_amount.grid(row=1, column=2, sticky=W, pady=(8, 0))
        self.amount_ctrl.grid(row=1, column=3, sticky=W, padx=(4, 0), pady=(8, 0))

        self.lbl_contract_search.grid(row=2, column=0, sticky=W, pady=(8, 0))
        self.ent_contract_search.grid(row=2, column=1, columnspan=2, sticky="ew", padx=(4, 8), pady=(8, 0))
        self.btn_contract_search.grid(row=2, column=3, sticky=W, pady=(8, 0))

        self.lbl_delay.grid(row=3, column=0, sticky=W, pady=(8, 0))
        self.ent_delay.grid(row=3, column=1, sticky="ew", padx=(4, 8), pady=(8, 0))
        self.lbl_threads.grid(row=3, column=2, sticky=W, pady=(8, 0))
        self.spin_threads.grid(row=3, column=3, sticky="ew", padx=(4, 0), pady=(8, 0))

        self.chk_dry_run.grid(row=4, column=0, sticky=W, pady=(8, 0))
        row = 5

        if show_source:
            self.lbl_source_credential.grid(row=row, column=0, sticky=W, pady=(8, 0))
            self.ent_source_credential.grid(row=row, column=1, columnspan=2, sticky="ew", padx=(4, 8), pady=(8, 0))
            self.lbl_source_balance_title.grid(row=row, column=3, sticky=W, pady=(8, 0))
            self.lbl_source_balance_val.grid(row=row + 1, column=1, columnspan=3, sticky=W, padx=(4, 0))
            row += 2

        if show_target:
            self.lbl_target_address.grid(row=row, column=0, sticky=W, pady=(8, 0))
            self.ent_target_address.grid(row=row, column=1, columnspan=2, sticky="ew", padx=(4, 8), pady=(8, 0))
            self.lbl_target_balance_title.grid(row=row, column=3, sticky=W, pady=(8, 0))
            self.lbl_target_balance_val.grid(row=row + 1, column=1, columnspan=3, sticky=W, padx=(4, 0))
            row += 2

        self.lbl_batch_hint.grid(row=row, column=0, columnspan=4, sticky=W, pady=(8, 0))
        self.setting_frame.columnconfigure(1, weight=1)
        self.setting_frame.columnconfigure(2, weight=1)
        self.setting_frame.columnconfigure(3, weight=1)

    def _on_root_resize(self, _event=None):
        width = self.root.winfo_width()
        compact = width < 1220
        if compact != self._compact_mode:
            self._apply_setting_layout(compact=compact)
        self._resize_tree_columns()
        self._on_log_resize()

    def _on_table_resize(self, _event=None):
        self._resize_tree_columns()

    def _resize_tree_columns(self):
        if not hasattr(self, "tree"):
            return
        cols = ("checked", "idx", "source", "source_addr", "target", "status", "balance")
        min_widths = self.TREE_COL_MIN_WIDTHS
        total_min = sum(min_widths[c] for c in cols)
        width = self.tree.winfo_width()
        if width <= 1 and hasattr(self, "table_wrap"):
            width = self.table_wrap.winfo_width() - 20
        if width <= 0:
            return

        if width <= total_min:
            for c in cols:
                self.tree.column(c, width=min_widths[c], stretch=False)
            return

        extra = width - total_min
        weights = self.TREE_COL_WEIGHTS
        total_weight = sum(weights[c] for c in cols)
        used = 0
        for i, c in enumerate(cols):
            add = extra - used if i == len(cols) - 1 else int(extra * weights[c] / total_weight)
            used += add
            self.tree.column(c, width=min_widths[c] + max(0, add), stretch=True)

    def _on_log_resize(self, _event=None):
        if not hasattr(self, "log_tree"):
            return
        width = self.log_tree.winfo_width()
        if width <= 1 and hasattr(self, "log_box"):
            width = self.log_box.winfo_width() - 20
        if width <= 0:
            return
        msg_width = max(300, width - 190)
        self.log_tree.column("time", width=170, stretch=False)
        self.log_tree.column("msg", width=msg_width, stretch=True)

    def _on_mode_changed(self, *_args):
        mode = self._mode()
        if mode == self.MODE_M2M:
            hint = "提示：每行格式：私钥或助记词 + 接收地址（地址放在最后一列）"
            self.source_balance_var.set("-")
            self.target_balance_var.set("-")
        elif mode == self.MODE_1M:
            hint = "提示：1对多只需导入接收地址；转出钱包私钥/助记词在上方填写"
            source = self.source_credential_var.get().strip()
            self.source_balance_var.set(self._balance_text_for_source(source) if source else "-")
            self.target_balance_var.set("-")
        else:
            hint = "提示：多对1请先填写收款地址，再导入多个私钥/助记词"
            self.source_balance_var.set("-")
            target = self.target_address_var.get().strip()
            self.target_balance_var.set(self._balance_text_for_target(target) if target else "-")
        self.import_hint_label.configure(text=hint)
        self._apply_setting_layout(compact=bool(self._compact_mode))
        self._refresh_tree()
        self._resize_tree_columns()

    def _on_network_changed(self, *_args):
        network = self.network_var.get().strip().upper()
        if network in {"ETH", "BSC"}:
            self._build_token_options(network=network, prefer_symbol=self.symbol_var.get().strip(), prefer_contract="")
        else:
            self.current_tokens = {}
            self.coin_box.configure(values=[])
            self.coin_var.set("")
            self.symbol_var.set("-")
        self.source_balance_cache.clear()
        self.target_balance_cache.clear()
        self.source_balance_var.set("-")
        self.target_balance_var.set("-")
        self._update_balance_heading()
        self._refresh_tree()

    def _on_coin_changed(self, *_args):
        token = self._selected_token(with_message=False)
        if token:
            self.symbol_var.set(token.symbol)
        else:
            self.symbol_var.set("-")
        self.source_balance_cache.clear()
        self.target_balance_cache.clear()
        self.source_balance_var.set("-")
        self.target_balance_var.set("-")
        self._update_balance_heading()
        self._refresh_tree()

    def _on_source_or_target_changed(self, *_args):
        if self._is_mode_1m():
            source = self.source_credential_var.get().strip()
            bal = self._balance_text_for_source(source) if source else "-"
            self.source_balance_var.set(bal)
            self.target_balance_var.set("-")
            self._refresh_tree()
            return
        self.source_balance_var.set("-")
        if self._is_mode_m1():
            target = self.target_address_var.get().strip()
            self.target_balance_var.set(self._balance_text_for_target(target) if target else "-")
            self._refresh_tree()
            return
        self.target_balance_var.set("-")

    def _update_balance_heading(self):
        symbol = self.symbol_var.get().strip() or "-"
        self.tree.heading("balance", text=f"余额({symbol})")

    @staticmethod
    def _short_contract(contract: str) -> str:
        s = contract.strip()
        if len(s) <= 12:
            return s
        return f"{s[:8]}...{s[-6:]}"

    def _build_token_options(self, network: str, prefer_symbol: str = "", prefer_contract: str = ""):
        net = network.strip().upper()
        defaults = self.client.get_default_tokens(net)
        custom = list((self.custom_tokens_by_network.get(net) or {}).values())
        all_tokens = defaults + custom

        mapping: dict[str, EvmToken] = {}
        used: set[str] = set()
        for token in all_tokens:
            label = self._token_display(token)
            if label in used:
                if token.contract:
                    label = f"{token.symbol}({self._short_contract(token.contract)})"
                else:
                    label = f"{token.symbol}(原生)"
            idx = 2
            while label in used:
                label = f"{label}#{idx}"
                idx += 1
            used.add(label)
            mapping[label] = token

        self.current_tokens = mapping
        values = list(mapping.keys())
        self.coin_box.configure(values=values)
        if not values:
            self.coin_var.set("")
            self.symbol_var.set("-")
            return

        current_label = self.coin_var.get().strip()
        selected = ""
        prefer_contract_l = prefer_contract.strip().lower()
        prefer_symbol_u = prefer_symbol.strip().upper()

        if prefer_contract_l:
            for label, token in mapping.items():
                if token.contract.strip().lower() == prefer_contract_l:
                    selected = label
                    break
        if not selected and prefer_symbol_u:
            for label, token in mapping.items():
                if token.symbol.strip().upper() == prefer_symbol_u:
                    selected = label
                    break
        if not selected and current_label in mapping:
            selected = current_label
        if not selected:
            selected = values[0]
        self.coin_var.set(selected)

    def _selected_token(self, with_message: bool = False) -> EvmToken | None:
        label = self.coin_var.get().strip() if hasattr(self, "coin_var") else ""
        token_map = self.current_tokens if hasattr(self, "current_tokens") else {}
        token = token_map.get(label)
        if token is not None:
            return token

        if token_map:
            first_label = next(iter(token_map))
            if hasattr(self, "coin_var"):
                self.coin_var.set(first_label)
            return token_map[first_label]

        network = self.network_var.get().strip().upper()
        if network in {"ETH", "BSC"}:
            symbol = self.client.get_symbol(network)
            if symbol:
                return EvmToken(symbol=symbol, contract="", decimals=18, is_native=True)

        if with_message:
            messagebox.showerror("参数错误", "币种未设置，无法执行")
        return None

    def _token_desc(self, token: EvmToken) -> str:
        if token.is_native:
            return token.symbol
        return f"{token.symbol}({self._short_contract(token.contract)})"

    def _token_desc_from_params(self, params: WithdrawRuntimeParams) -> str:
        if params.token_is_native:
            return params.coin
        return f"{params.coin}({self._short_contract(params.token_contract)})"

    def search_contract_token(self):
        if self.is_running:
            messagebox.showwarning("提示", "已有任务在运行")
            return
        network = self.network_var.get().strip().upper()
        if network not in {"ETH", "BSC"}:
            messagebox.showerror("参数错误", "请先设置网络后再搜索合约")
            return
        contract = self.contract_search_var.get().strip()
        if not self.client.is_address(contract):
            messagebox.showerror("参数错误", "合约地址格式错误")
            return
        self.log(f"开始搜索代币合约：network={network}, contract={contract}")
        threading.Thread(target=self._run_search_contract_token, args=(network, contract), daemon=True).start()

    def _run_search_contract_token(self, network: str, contract: str):
        try:
            token = self.client.get_erc20_token_info(network, contract)
            self.custom_tokens_by_network.setdefault(network, {})[token.contract.lower()] = token
            self.root.after(0, lambda n=network, t=token: self._apply_found_token(n, t))
        except Exception as exc:
            err_text = str(exc)
            self.root.after(0, lambda m=f"合约搜索失败：{err_text}": self.log(m))
            self.root.after(0, lambda e=err_text: messagebox.showerror("合约搜索失败", e))

    def _apply_found_token(self, network: str, token: EvmToken):
        self.log(
            f"合约识别成功：network={network}, symbol={token.symbol}, decimals={token.decimals}, contract={token.contract}"
        )
        if self.network_var.get().strip().upper() == network:
            self._build_token_options(network=network, prefer_symbol=token.symbol, prefer_contract=token.contract)

    def log(self, text: str):
        append_log_row(self.log_tree, text, max_rows=LOG_MAX_ROWS)

    def _parse_m2m_lines(self, lines: list[str]) -> list[OnchainPairEntry]:
        result: list[OnchainPairEntry] = []
        seen: set[tuple[str, str]] = set()
        for i, line in enumerate(lines, start=1):
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if any(x in s for x in ("\t", ",", ";")):
                arr = [x.strip() for x in re.split(r"[\t,;]+", s) if x.strip()]
                if len(arr) < 2:
                    raise RuntimeError(f"第 {i} 行格式错误：至少需要 2 列（私钥/助记词 接收地址）")
                source = " ".join(arr[:-1]).strip()
                target = arr[-1].strip()
            else:
                arr = [x for x in s.split() if x]
                if len(arr) < 2:
                    raise RuntimeError(f"第 {i} 行格式错误：至少需要 2 列（私钥/助记词 接收地址）")
                source = " ".join(arr[:-1]).strip()
                target = arr[-1].strip()
            if not source:
                raise RuntimeError(f"第 {i} 行格式错误：私钥/助记词不能为空")
            if not self.client.is_address(target):
                raise RuntimeError(f"第 {i} 行地址格式错误：{target}")
            key = (source, target)
            if key in seen:
                continue
            seen.add(key)
            result.append(OnchainPairEntry(source=source, target=target))
        return result

    def _parse_address_lines(self, lines: list[str]) -> list[str]:
        result: list[str] = []
        seen: set[str] = set()
        for i, line in enumerate(lines, start=1):
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if not self.client.is_address(s):
                raise RuntimeError(f"第 {i} 行地址格式错误：{s}")
            if s in seen:
                continue
            seen.add(s)
            result.append(s)
        return result

    def _parse_source_lines(self, lines: list[str]) -> list[str]:
        result: list[str] = []
        seen: set[str] = set()
        for i, line in enumerate(lines, start=1):
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if s in seen:
                continue
            seen.add(s)
            result.append(s)
        return result

    def _import_rows(self, rows: list[OnchainPairEntry] | list[str], source_name: str):
        if not rows:
            messagebox.showwarning("提示", "没有可导入的数据")
            return
        mode = self._mode()
        if mode == self.MODE_M2M:
            created = self.store.upsert_multi_to_multi(rows)  # type: ignore[arg-type]
            self.checked_row_keys = set(self._active_row_keys())
            self._refresh_tree()
            self.log(f"{source_name}导入完成：新增 {created} 条，已自动全选 {len(self.store.multi_to_multi_pairs)} 条")
            return
        if mode == self.MODE_1M:
            created = self.store.upsert_one_to_many_addresses(rows)  # type: ignore[arg-type]
            self.checked_row_keys = set(self._active_row_keys())
            self._refresh_tree()
            self.log(f"{source_name}导入完成：新增 {created} 条地址，已自动全选 {len(self.store.one_to_many_addresses)} 条")
            return
        created = self.store.upsert_many_to_one_sources(rows)  # type: ignore[arg-type]
        self.checked_row_keys = set(self._active_row_keys())
        self._refresh_tree()
        self.log(f"{source_name}导入完成：新增 {created} 条钱包，已自动全选 {len(self.store.many_to_one_sources)} 条")

    def import_from_paste(self):
        self.import_from_clipboard()

    def import_from_clipboard(self):
        try:
            text = self.root.clipboard_get()
        except Exception:
            messagebox.showwarning("提示", "剪贴板为空或不可读取")
            return
        try:
            lines = text.splitlines()
            mode = self._mode()
            if mode == self.MODE_M2M:
                rows = self._parse_m2m_lines(lines)
            elif mode == self.MODE_1M:
                rows = self._parse_address_lines(lines)
            else:
                rows = self._parse_source_lines(lines)
            self._import_rows(rows, "剪贴板")
        except Exception as exc:
            messagebox.showerror("导入失败", str(exc))

    def import_txt(self):
        from tkinter import filedialog

        path = filedialog.askopenfilename(title="选择 TXT", filetypes=[("Text", "*.txt"), ("All Files", "*.*")])
        if not path:
            return
        try:
            content = Path(path).read_text(encoding="utf-8")
        except UnicodeDecodeError:
            content = Path(path).read_text(encoding="utf-8-sig")
        try:
            lines = content.splitlines()
            mode = self._mode()
            if mode == self.MODE_M2M:
                rows = self._parse_m2m_lines(lines)
            elif mode == self.MODE_1M:
                rows = self._parse_address_lines(lines)
            else:
                rows = self._parse_source_lines(lines)
            self._import_rows(rows, "TXT")
        except Exception as exc:
            messagebox.showerror("导入失败", str(exc))

    def export_txt(self):
        from tkinter import filedialog

        path = filedialog.asksaveasfilename(title="导出 TXT", defaultextension=".txt", filetypes=[("Text", "*.txt")])
        if not path:
            return
        try:
            mode = self._mode()
            if mode == self.MODE_M2M:
                lines = [f"{x.source} {x.target}" for x in self.store.multi_to_multi_pairs]
            elif mode == self.MODE_1M:
                lines = list(self.store.one_to_many_addresses)
            else:
                lines = list(self.store.many_to_one_sources)
            Path(path).write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
            self.log(f"TXT 导出完成：{path}")
            messagebox.showinfo("导出完成", f"已导出 {len(lines)} 条")
        except Exception as exc:
            messagebox.showerror("导出失败", str(exc))

    def toggle_check_all(self):
        keys = set(self._active_row_keys())
        if not keys:
            messagebox.showwarning("提示", "当前模式暂无可操作数据")
            return
        if self.checked_row_keys == keys:
            self.checked_row_keys.clear()
            self.log("已取消全选")
        else:
            self.checked_row_keys = set(keys)
            self.log(f"已全选 {len(keys)} 条")
        self._refresh_tree()

    def delete_selected(self):
        mode = self._mode()
        if mode == self.MODE_M2M:
            idxs = [i for i, item in enumerate(self.store.multi_to_multi_pairs) if self._m2m_key(item) in self.checked_row_keys]
        elif mode == self.MODE_1M:
            idxs = [i for i, target in enumerate(self.store.one_to_many_addresses) if self._one_to_many_key(target) in self.checked_row_keys]
        else:
            idxs = [i for i, source in enumerate(self.store.many_to_one_sources) if self._many_to_one_key(source) in self.checked_row_keys]
        if not idxs:
            messagebox.showwarning("提示", "请先勾选要删除的数据")
            return
        if not messagebox.askyesno("确认删除", f"确认删除 {len(idxs)} 条数据吗？"):
            return

        if mode == self.MODE_M2M:
            self.store.delete_multi_to_multi_by_indices(idxs)
        elif mode == self.MODE_1M:
            self.store.delete_one_to_many_by_indices(idxs)
        else:
            self.store.delete_many_to_one_by_indices(idxs)
        self._refresh_tree()
        self.log(f"已删除 {len(idxs)} 条")

    def delete_current_row(self):
        idx = self._single_selected_index()
        if idx is None:
            messagebox.showwarning("提示", "请先右键选中一条数据")
            return
        mode = self._mode()
        if mode == self.MODE_M2M and not (0 <= idx < len(self.store.multi_to_multi_pairs)):
            messagebox.showwarning("提示", "请先右键选中一条数据")
            return
        if mode == self.MODE_1M and not (0 <= idx < len(self.store.one_to_many_addresses)):
            messagebox.showwarning("提示", "请先右键选中一条数据")
            return
        if mode == self.MODE_M1 and not (0 <= idx < len(self.store.many_to_one_sources)):
            messagebox.showwarning("提示", "请先右键选中一条数据")
            return
        if not messagebox.askyesno("确认删除", "确认删除当前数据吗？"):
            return
        if mode == self.MODE_M2M:
            self.store.delete_multi_to_multi_by_indices([idx])
        elif mode == self.MODE_1M:
            self.store.delete_one_to_many_by_indices([idx])
        else:
            self.store.delete_many_to_one_by_indices([idx])
        self._refresh_tree()
        self.log("已删除当前数据")

    def _apply_settings_to_store(self) -> bool:
        mode = self._mode()
        network = self.network_var.get().strip().upper()
        token = self._selected_token(with_message=False)
        amount_mode = self._amount_mode()
        amount = self.amount_var.get().strip()
        if amount_mode == self.AMOUNT_MODE_ALL:
            amount = self.AMOUNT_ALL_LABEL
        try:
            delay = max(0.0, float(self.delay_var.get()))
        except Exception:
            messagebox.showerror("参数错误", "执行间隔格式错误")
            return False
        try:
            threads = max(1, int(str(self.threads_var.get()).strip()))
        except Exception:
            messagebox.showerror("参数错误", "执行线程数格式错误")
            return False

        self.store.settings = OnchainSettings(
            mode=mode,
            network=network,
            token_symbol=(token.symbol if token else ""),
            token_contract=(token.contract if token else ""),
            amount_mode=amount_mode,
            amount=amount,
            random_min=self.random_min_var.get().strip(),
            random_max=self.random_max_var.get().strip(),
            delay_seconds=delay,
            worker_threads=threads,
            dry_run=bool(self.dry_run_var.get()),
            one_to_many_source=self.source_credential_var.get().strip(),
            many_to_one_target=self.target_address_var.get().strip(),
        )
        return True

    def save_all(self):
        if not self._apply_settings_to_store():
            return
        try:
            self.store.save()
            self.log("链上配置已保存")
            messagebox.showinfo("成功", f"配置已保存到：{ONCHAIN_DATA_FILE}")
        except Exception as exc:
            messagebox.showerror("保存失败", str(exc))

    def _load_data(self):
        try:
            self.store.load()
            st = self.store.settings
            loaded_mode = st.mode if st.mode in {self.MODE_M2M, self.MODE_1M, self.MODE_M1} else self.MODE_M2M
            self.mode_var.set(loaded_mode)
            net = st.network if st.network in {"ETH", "BSC"} else ""
            self.network_var.set(net)
            if net:
                self._build_token_options(network=net, prefer_symbol=st.token_symbol, prefer_contract=st.token_contract)
            else:
                self.current_tokens = {}
                self.coin_box.configure(values=[])
                self.coin_var.set("")
            self.amount_mode_var.set(st.amount_mode if st.amount_mode in {self.AMOUNT_MODE_FIXED, self.AMOUNT_MODE_RANDOM, self.AMOUNT_MODE_ALL} else self.AMOUNT_MODE_FIXED)
            self.amount_var.set("" if self.amount_mode_var.get() == self.AMOUNT_MODE_ALL else (st.amount or ""))
            self.random_min_var.set(st.random_min or "")
            self.random_max_var.set(st.random_max or "")
            self.delay_var.set(st.delay_seconds)
            self.threads_var.set(str(max(1, int(st.worker_threads or 1))))
            self.dry_run_var.set(st.dry_run)
            self.source_credential_var.set(st.one_to_many_source or "")
            self.target_address_var.set(st.many_to_one_target or "")
            self.contract_search_var.set(st.token_contract or "")
            self.source_balance_var.set("-")
            self.target_balance_var.set("-")
            self.checked_row_keys = set(self._active_row_keys())
            self._on_coin_changed()
            self._on_mode_changed()
            self.log("链上配置加载完成")
        except Exception as exc:
            messagebox.showerror("加载失败", str(exc))

    def _collect_jobs(self, with_message: bool = False) -> list[tuple[str, str, str]]:
        mode = self._mode()
        jobs: list[tuple[str, str, str]] = []

        if mode == self.MODE_M2M:
            for item in self.store.multi_to_multi_pairs:
                key = self._m2m_key(item)
                if key in self.checked_row_keys:
                    jobs.append((key, item.source, item.target))
        elif mode == self.MODE_1M:
            source = self.source_credential_var.get().strip()
            if not source:
                if with_message:
                    messagebox.showerror("参数错误", "1对多模式请填写转出钱包私钥/助记词")
                return []
            for target in self.store.one_to_many_addresses:
                key = self._one_to_many_key(target)
                if key in self.checked_row_keys:
                    jobs.append((key, source, target))
        else:
            target = self.target_address_var.get().strip()
            if not target:
                if with_message:
                    messagebox.showerror("参数错误", "多对1模式请填写收款地址")
                return []
            if not self.client.is_address(target):
                if with_message:
                    messagebox.showerror("参数错误", "收款地址格式错误")
                return []
            for source in self.store.many_to_one_sources:
                key = self._many_to_one_key(source)
                if key in self.checked_row_keys:
                    jobs.append((key, source, target))

        if with_message and not jobs:
            messagebox.showwarning("提示", "请先勾选至少一条数据")
        return jobs

    def _collect_failed_jobs(self, with_message: bool = False) -> list[tuple[str, str, str]]:
        mode = self._mode()
        jobs: list[tuple[str, str, str]] = []

        if mode == self.MODE_M2M:
            for item in self.store.multi_to_multi_pairs:
                key = self._m2m_key(item)
                if self.row_status.get(key, "") == "failed":
                    jobs.append((key, item.source, item.target))
        elif mode == self.MODE_1M:
            source = self.source_credential_var.get().strip()
            if not source:
                if with_message:
                    messagebox.showerror("参数错误", "1对多模式请填写转出钱包私钥/助记词")
                return []
            for target in self.store.one_to_many_addresses:
                key = self._one_to_many_key(target)
                if self.row_status.get(key, "") == "failed":
                    jobs.append((key, source, target))
        else:
            target = self.target_address_var.get().strip()
            if not target:
                if with_message:
                    messagebox.showerror("参数错误", "多对1模式请填写收款地址")
                return []
            if not self.client.is_address(target):
                if with_message:
                    messagebox.showerror("参数错误", "收款地址格式错误")
                return []
            for source in self.store.many_to_one_sources:
                key = self._many_to_one_key(source)
                if self.row_status.get(key, "") == "failed":
                    jobs.append((key, source, target))

        if with_message and not jobs:
            messagebox.showwarning("提示", "当前没有失败记录可重试")
        return jobs

    @classmethod
    def _random_decimal_between(cls, low: Decimal, high: Decimal) -> Decimal:
        return random_decimal_between(low, high)

    def _validate_transfer_params(self) -> WithdrawRuntimeParams | None:
        network = self.network_var.get().strip().upper()
        amount_mode = self._amount_mode()
        amount_raw = self.amount_var.get().strip()
        random_enabled = amount_mode == self.AMOUNT_MODE_RANDOM

        if network not in {"ETH", "BSC"}:
            messagebox.showerror("参数错误", "未设置网络，无法转账")
            return None

        token = self._selected_token(with_message=True)
        if not token:
            return None
        if (not token.is_native) and (not self.client.is_address(token.contract)):
            messagebox.showerror("参数错误", "代币合约地址格式错误")
            return None
        decimals = int(token.decimals)
        if decimals < 0 or decimals > self.MAX_TOKEN_DECIMALS:
            messagebox.showerror("参数错误", f"代币精度无效：{decimals}")
            return None
        coin = token.symbol.strip().upper()
        if not coin:
            messagebox.showerror("参数错误", "币种无效")
            return None

        if amount_mode == self.AMOUNT_MODE_ALL:
            amount = self.AMOUNT_ALL_LABEL
        else:
            if amount_mode == self.AMOUNT_MODE_FIXED and not amount_raw:
                messagebox.showerror("参数错误", "转账数量不能为空")
                return None
            try:
                if amount_mode == self.AMOUNT_MODE_FIXED:
                    v = Decimal(amount_raw)
                    if v <= 0:
                        raise InvalidOperation
                    amount = self._decimal_to_text(v)
                else:
                    amount = amount_raw or "0"
            except Exception:
                messagebox.showerror("参数错误", "固定数量必须是大于 0 的数字")
                return None

        random_min: Decimal | None = None
        random_max: Decimal | None = None
        if random_enabled:
            min_raw = self.random_min_var.get().strip()
            max_raw = self.random_max_var.get().strip()
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
            delay = max(0.0, float(self.delay_var.get()))
        except Exception:
            messagebox.showerror("参数错误", "执行间隔格式错误")
            return None
        try:
            threads = max(1, int(str(self.threads_var.get()).strip()))
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
            token_contract=token.contract if not token.is_native else "",
            token_decimals=decimals,
            token_is_native=bool(token.is_native),
        )

    def _resolve_wallet(self, source: str) -> tuple[str, str]:
        source_key = source.strip()
        if not source_key:
            raise RuntimeError("转出钱包私钥/助记词不能为空")
        with self.wallet_cache_lock:
            cached_pk = self.source_private_key_cache.get(source_key)
            cached_addr = self.source_address_cache.get(source_key)
            if cached_pk and cached_addr:
                return cached_pk, cached_addr
        private_key = self.client.credential_to_private_key(source_key)
        address = self.client.address_from_private_key(private_key)
        with self.wallet_cache_lock:
            self.source_private_key_cache[source_key] = private_key
            self.source_address_cache[source_key] = address
        return private_key, address

    def start_query_balance(self):
        try:
            if self.is_running:
                messagebox.showwarning("提示", "已有任务在运行")
                return
            try:
                self.client._ensure_eth_account()
            except Exception as exc:
                messagebox.showerror("依赖缺失", str(exc))
                return
            network = self.network_var.get().strip().upper()
            if network not in {"ETH", "BSC"}:
                messagebox.showerror("参数错误", "未设置网络，无法查询余额")
                return
            token = self._selected_token(with_message=True)
            if not token:
                return
            mode = self._mode()

            if mode == self.MODE_1M:
                source = self.source_credential_var.get().strip()
                selected_targets = [t for t in self.store.one_to_many_addresses if self._one_to_many_key(t) in self.checked_row_keys]
                targets: list[str] = []
                seen_t: set[str] = set()
                for target in selected_targets:
                    t = target.strip()
                    if not t or t in seen_t:
                        continue
                    seen_t.add(t)
                    targets.append(t)
                if not targets:
                    messagebox.showwarning("提示", "请先勾选至少一条查询数据")
                    return
                self.is_running = True
                threading.Thread(
                    target=self._run_query_balance_one_to_many,
                    args=(network, token, source, targets),
                    daemon=True,
                ).start()
                return

            if mode == self.MODE_M1:
                target = self.target_address_var.get().strip()
                if target and not self.client.is_address(target):
                    self.log("收款地址格式错误，已跳过收款地址余额查询")
                    target = ""
                selected_sources = [s for s in self.store.many_to_one_sources if self._many_to_one_key(s) in self.checked_row_keys]
                sources: list[str] = []
                seen_s: set[str] = set()
                for source in selected_sources:
                    s = source.strip()
                    if not s or s in seen_s:
                        continue
                    seen_s.add(s)
                    sources.append(s)
                if not sources:
                    messagebox.showwarning("提示", "请先勾选至少一条查询数据")
                    return
                self.is_running = True
                threading.Thread(
                    target=self._run_query_balance_many_to_one,
                    args=(network, token, target, sources),
                    daemon=True,
                ).start()
                return

            selected_pairs = [x for x in self.store.multi_to_multi_pairs if self._m2m_key(x) in self.checked_row_keys]
            sources: list[str] = []
            seen: set[str] = set()
            for item in selected_pairs:
                s = item.source.strip()
                if not s or s in seen:
                    continue
                seen.add(s)
                sources.append(s)
            if not sources:
                messagebox.showwarning("提示", "请先勾选至少一条查询数据")
                return

            self.is_running = True
            threading.Thread(target=self._run_query_balance_for_sources, args=(network, token, sources), daemon=True).start()
        except Exception as exc:
            self.log(f"余额查询启动失败：{exc}")
            messagebox.showerror("执行异常", str(exc))

    def start_query_balance_current_row(self):
        try:
            if self.is_running:
                messagebox.showwarning("提示", "已有任务在运行")
                return
            try:
                self.client._ensure_eth_account()
            except Exception as exc:
                messagebox.showerror("依赖缺失", str(exc))
                return
            network = self.network_var.get().strip().upper()
            if network not in {"ETH", "BSC"}:
                messagebox.showerror("参数错误", "未设置网络，无法查询余额")
                return
            token = self._selected_token(with_message=True)
            if not token:
                return
            job = self._single_row_job()
            if not job:
                messagebox.showwarning("提示", "请先右键选中一条数据")
                return
            _row_key, source, target = job
            mode = self._mode()
            self.is_running = True
            if mode == self.MODE_1M:
                threading.Thread(
                    target=self._run_query_balance_one_to_many,
                    args=(network, token, source, [target]),
                    daemon=True,
                ).start()
                return
            if mode == self.MODE_M1:
                target_addr = target
                if target_addr and not self.client.is_address(target_addr):
                    self.log("收款地址格式错误，已跳过收款地址余额查询")
                    target_addr = ""
                threading.Thread(
                    target=self._run_query_balance_many_to_one,
                    args=(network, token, target_addr, [source]),
                    daemon=True,
                ).start()
                return
            threading.Thread(
                target=self._run_query_balance_for_sources,
                args=(network, token, [source]),
                daemon=True,
            ).start()
        except Exception as exc:
            self.log(f"当前行余额查询启动失败：{exc}")
            messagebox.showerror("执行异常", str(exc))

    def start_transfer_current_row(self):
        try:
            if self.is_running:
                messagebox.showwarning("提示", "已有任务在运行")
                return
            params = self._validate_transfer_params()
            if not params:
                return
            job = self._single_row_job()
            if not job:
                messagebox.showwarning("提示", "请先右键选中一条数据")
                return
            row_key, source, target = job
            mode = self._mode()
            if mode == self.MODE_1M and not source:
                messagebox.showerror("参数错误", "1对多模式请填写转出钱包私钥/助记词")
                return
            if mode == self.MODE_M1:
                if not target:
                    messagebox.showerror("参数错误", "多对1模式请填写收款地址")
                    return
                if not self.client.is_address(target):
                    messagebox.showerror("参数错误", "收款地址格式错误")
                    return

            dry_run = bool(self.dry_run_var.get())
            if not dry_run:
                try:
                    self.client._ensure_eth_account()
                except Exception as exc:
                    messagebox.showerror("依赖缺失", str(exc))
                    return
            token_desc = self._token_desc_from_params(params)
            if not dry_run:
                amount_text = params.amount
                if params.random_enabled and params.random_min is not None and params.random_max is not None:
                    amount_text = f"随机 {params.random_min:.2f} ~ {params.random_max:.2f}"
                text = (
                    f"即将执行链上当前行真实转账：\n"
                    f"模式：{self._mode()}\n"
                    f"任务数：1\n"
                    f"网络：{params.network}\n"
                    f"币种：{token_desc}\n"
                    f"数量：{amount_text}\n"
                    f"执行间隔：{params.delay} 秒\n"
                    f"执行线程数：{params.threads}\n\n"
                    "确认继续？"
                )
                if not messagebox.askyesno("高风险确认", text):
                    self.log("用户取消了当前行真实链上转账")
                    return

            self.log(f"已启动当前行转账任务：mode={self._mode()}，币种={token_desc}")
            self.is_running = True
            threading.Thread(target=self._run_batch_transfer, args=([(row_key, source, target)], params, dry_run), daemon=True).start()
        except Exception as exc:
            self.log(f"当前行转账启动失败：{exc}")
            messagebox.showerror("执行异常", str(exc))

    def _run_query_balance_for_sources(self, network: str, token: EvmToken, sources: list[str]):
        try:
            symbol = token.symbol
            token_desc = symbol if token.is_native else f"{symbol}({self._short_contract(token.contract)})"
            self.root.after(0, lambda: self.log(f"开始查询余额：网络={network}，币种={token_desc}，钱包数={len(sources)}"))
            ok = 0
            failed = 0
            total = Decimal("0")
            lock = threading.Lock()
            jobs: queue.Queue[tuple[int, str]] = queue.Queue()
            for i, source in enumerate(sources, start=1):
                jobs.put((i, source))

            def worker():
                nonlocal ok, failed, total
                while True:
                    try:
                        i, source = jobs.get_nowait()
                    except queue.Empty:
                        return
                    prefix = f"[{i}/{len(sources)}]"
                    try:
                        _pk, addr = self._resolve_wallet(source)
                        if token.is_native:
                            units = self.client.get_balance_wei(network, addr)
                        else:
                            units = self.client.get_erc20_balance(network, token.contract, addr)
                        amount = self._units_to_amount(units, token.decimals)
                        with lock:
                            self.source_balance_cache[source] = amount
                            total += amount
                            ok += 1
                        msg = f"{prefix}[{self._mask(addr, head=8, tail=6)}] 余额={symbol} {self._decimal_to_text(amount)}"
                        self.root.after(0, lambda m=msg: self.log(m))
                    except Exception as exc:
                        with lock:
                            failed += 1
                        self.root.after(0, lambda m=f"{prefix} 查询失败：{exc}": self.log(m))
                    finally:
                        jobs.task_done()

            workers: list[threading.Thread] = []
            worker_count = max(1, min(self._runtime_worker_threads(), len(sources)))
            for _ in range(worker_count):
                t = threading.Thread(target=worker, daemon=True)
                t.start()
                workers.append(t)
            for t in workers:
                t.join()
            summary = f"余额查询结束：成功 {ok}，失败 {failed}，{symbol} 合计={self._decimal_to_text(total)}"
            self.root.after(0, lambda: self._refresh_tree())
            self.root.after(0, lambda: self.log(summary))
        except Exception as exc:
            err_text = str(exc)
            self.root.after(0, lambda m=f"余额查询任务异常终止：{err_text}": self.log(m))
            self.root.after(0, lambda e=err_text: messagebox.showerror("执行异常", e))
        finally:
            self.is_running = False

    def _run_query_balance_one_to_many(self, network: str, token: EvmToken, source: str, targets: list[str]):
        try:
            symbol = token.symbol
            token_desc = self._token_desc(token)
            src_count = 1 if source else 0
            self.root.after(
                0,
                lambda: self.log(f"开始查询余额：模式=1对多，网络={network}，币种={token_desc}，源钱包数={src_count}，目标地址数={len(targets)}"),
            )

            # 先查询转出钱包余额，展示在私钥输入框旁
            source_addr = ""
            source_query_state = "skip"
            if source:
                try:
                    _pk, source_addr = self._resolve_wallet(source)
                    if token.is_native:
                        src_units = self.client.get_balance_wei(network, source_addr)
                    else:
                        src_units = self.client.get_erc20_balance(network, token.contract, source_addr)
                    src_amount = self._units_to_amount(src_units, token.decimals)
                    self.source_balance_cache[source] = src_amount
                    src_text = f"{symbol}:{self._decimal_to_text(src_amount)}"
                    self.root.after(0, lambda t=src_text: self.source_balance_var.set(t))
                    self.root.after(
                        0,
                        lambda m=f"[SRC][{self._mask(source_addr, head=8, tail=6)}] 余额={symbol} {self._decimal_to_text(src_amount)}": self.log(m),
                    )
                    source_query_state = "ok"
                except Exception as exc:
                    self.root.after(0, lambda: self.source_balance_var.set("-"))
                    self.root.after(0, lambda m=f"[SRC] 查询失败：{exc}": self.log(m))
                    source_query_state = "failed"
            else:
                self.root.after(0, lambda: self.source_balance_var.set("-"))
                self.root.after(0, lambda: self.log("[SRC] 未提供转出钱包，已跳过源钱包余额查询"))

            ok = 0
            failed = 0
            total = Decimal("0")
            if not targets:
                self.root.after(0, lambda: self.log("[DST] 未提供目标地址，已跳过目标地址余额查询"))
            for i, addr in enumerate(targets, start=1):
                prefix = f"[{i}/{len(targets)}]"
                try:
                    if token.is_native:
                        units = self.client.get_balance_wei(network, addr)
                    else:
                        units = self.client.get_erc20_balance(network, token.contract, addr)
                    amount = self._units_to_amount(units, token.decimals)
                    self.target_balance_cache[addr] = amount
                    total += amount
                    msg = f"{prefix}[{self._mask(addr, head=8, tail=6)}] 余额={symbol} {self._decimal_to_text(amount)}"
                    self.root.after(0, lambda m=msg: self.log(m))
                    ok += 1
                except Exception as exc:
                    self.root.after(0, lambda m=f"{prefix} 查询失败：{exc}": self.log(m))
                    failed += 1

            src_status_text = {"ok": "成功", "failed": "失败", "skip": "跳过"}.get(source_query_state, source_query_state)
            summary = (
                f"余额查询结束：源钱包查询={src_status_text}，目标地址成功 {ok}，失败 {failed}，"
                f"目标地址{symbol}合计={self._decimal_to_text(total)}"
            )
            self.root.after(0, lambda: self._refresh_tree())
            self.root.after(0, lambda: self.log(summary))
        except Exception as exc:
            err_text = str(exc)
            self.root.after(0, lambda m=f"余额查询任务异常终止：{err_text}": self.log(m))
            self.root.after(0, lambda e=err_text: messagebox.showerror("执行异常", e))
        finally:
            self.is_running = False

    def _run_query_balance_many_to_one(self, network: str, token: EvmToken, target: str, sources: list[str]):
        try:
            symbol = token.symbol
            token_desc = self._token_desc(token)
            target_count = 1 if target else 0
            self.root.after(
                0,
                lambda: self.log(
                    f"开始查询余额：模式=多对1，网络={network}，币种={token_desc}，收款地址数={target_count}，源钱包数={len(sources)}"
                ),
            )

            target_query_state = "skip"
            if target:
                try:
                    if token.is_native:
                        target_units = self.client.get_balance_wei(network, target)
                    else:
                        target_units = self.client.get_erc20_balance(network, token.contract, target)
                    target_amount = self._units_to_amount(target_units, token.decimals)
                    self.target_balance_cache[target] = target_amount
                    target_text = f"{symbol}:{self._decimal_to_text(target_amount)}"
                    self.root.after(0, lambda t=target_text: self.target_balance_var.set(t))
                    self.root.after(
                        0,
                        lambda m=f"[DST][{self._mask(target, head=8, tail=6)}] 余额={symbol} {self._decimal_to_text(target_amount)}": self.log(m),
                    )
                    target_query_state = "ok"
                except Exception as exc:
                    self.root.after(0, lambda: self.target_balance_var.set("-"))
                    self.root.after(0, lambda m=f"[DST] 查询失败：{exc}": self.log(m))
                    target_query_state = "failed"
            else:
                self.root.after(0, lambda: self.target_balance_var.set("-"))
                self.root.after(0, lambda: self.log("[DST] 未提供收款地址，已跳过收款地址余额查询"))

            ok = 0
            failed = 0
            total = Decimal("0")
            if not sources:
                self.root.after(0, lambda: self.log("[SRC] 未提供源钱包，已跳过源钱包余额查询"))
            for i, source in enumerate(sources, start=1):
                prefix = f"[{i}/{len(sources)}]"
                try:
                    _pk, addr = self._resolve_wallet(source)
                    if token.is_native:
                        units = self.client.get_balance_wei(network, addr)
                    else:
                        units = self.client.get_erc20_balance(network, token.contract, addr)
                    amount = self._units_to_amount(units, token.decimals)
                    self.source_balance_cache[source] = amount
                    total += amount
                    msg = f"{prefix}[{self._mask(addr, head=8, tail=6)}] 余额={symbol} {self._decimal_to_text(amount)}"
                    self.root.after(0, lambda m=msg: self.log(m))
                    ok += 1
                except Exception as exc:
                    self.root.after(0, lambda m=f"{prefix} 查询失败：{exc}": self.log(m))
                    failed += 1

            dst_status_text = {"ok": "成功", "failed": "失败", "skip": "跳过"}.get(target_query_state, target_query_state)
            summary = (
                f"余额查询结束：收款地址查询={dst_status_text}，源钱包成功 {ok}，失败 {failed}，"
                f"源钱包{symbol}合计={self._decimal_to_text(total)}"
            )
            self.root.after(0, lambda: self._refresh_tree())
            self.root.after(0, lambda: self.log(summary))
        except Exception as exc:
            err_text = str(exc)
            self.root.after(0, lambda m=f"余额查询任务异常终止：{err_text}": self.log(m))
            self.root.after(0, lambda e=err_text: messagebox.showerror("执行异常", e))
        finally:
            self.is_running = False

    def _resolve_amount_and_gas(self, params: WithdrawRuntimeParams, source_addr: str, target_addr: str) -> tuple[int, int, int, str]:
        gas_price = self.client.get_gas_price_wei(params.network)

        if params.token_is_native:
            gas_limit = self.client.NATIVE_GAS_LIMIT
            gas_cost = gas_price * gas_limit

            if params.random_enabled and params.random_min is not None and params.random_max is not None:
                amount_dec = self._random_decimal_between(params.random_min, params.random_max)
                if amount_dec <= 0:
                    raise RuntimeError("随机金额生成失败：结果必须大于 0")
                value_units = self._amount_to_units(amount_dec, params.token_decimals)
                amount_text = f"{amount_dec:.2f}"
                balance_units = self.client.get_balance_wei(params.network, source_addr)
                if balance_units < value_units + gas_cost:
                    raise RuntimeError("余额不足（随机金额 + gas）")
                return value_units, gas_price, gas_limit, amount_text

            if params.amount == self.AMOUNT_ALL_LABEL:
                balance_units = self.client.get_balance_wei(params.network, source_addr)
                value_units = balance_units - gas_cost
                if value_units <= 0:
                    raise RuntimeError("余额不足以覆盖 gas")
                amount_text = self._decimal_to_text(self._units_to_amount(value_units, params.token_decimals))
                return value_units, gas_price, gas_limit, amount_text

            amount_dec = Decimal(params.amount)
            if amount_dec <= 0:
                raise RuntimeError("转账数量必须大于 0")
            value_units = self._amount_to_units(amount_dec, params.token_decimals)
            if value_units <= 0:
                raise RuntimeError("转账数量过小")
            balance_units = self.client.get_balance_wei(params.network, source_addr)
            if balance_units < value_units + gas_cost:
                raise RuntimeError("余额不足（固定金额 + gas）")
            return value_units, gas_price, gas_limit, self._decimal_to_text(amount_dec)

        token_contract = params.token_contract.strip()
        if not token_contract:
            raise RuntimeError("代币合约未设置")
        token_balance_units = self.client.get_erc20_balance(params.network, token_contract, source_addr)
        if params.random_enabled and params.random_min is not None and params.random_max is not None:
            amount_dec = self._random_decimal_between(params.random_min, params.random_max)
            if amount_dec <= 0:
                raise RuntimeError("随机金额生成失败：结果必须大于 0")
            value_units = self._amount_to_units(amount_dec, params.token_decimals)
            amount_text = f"{amount_dec:.2f}"
        elif params.amount == self.AMOUNT_ALL_LABEL:
            value_units = token_balance_units
            amount_text = self._decimal_to_text(self._units_to_amount(value_units, params.token_decimals))
        else:
            amount_dec = Decimal(params.amount)
            if amount_dec <= 0:
                raise RuntimeError("转账数量必须大于 0")
            value_units = self._amount_to_units(amount_dec, params.token_decimals)
            amount_text = self._decimal_to_text(amount_dec)
        if value_units <= 0:
            raise RuntimeError("代币余额为 0 或转账数量过小")
        if token_balance_units < value_units:
            raise RuntimeError("代币余额不足")

        gas_limit = self.client.estimate_erc20_transfer_gas(
            params.network,
            source_addr,
            token_contract,
            target_addr,
            value_units,
        )
        gas_cost = gas_price * gas_limit
        native_balance_units = self.client.get_balance_wei(params.network, source_addr)
        if native_balance_units < gas_cost:
            raise RuntimeError("原生币余额不足，无法支付 gas")
        return value_units, gas_price, gas_limit, amount_text

    def start_batch_transfer(self):
        try:
            if self.is_running:
                messagebox.showwarning("提示", "已有任务在运行")
                return
            params = self._validate_transfer_params()
            if not params:
                return
            jobs = self._collect_jobs(with_message=True)
            if not jobs:
                return
            dry_run = bool(self.dry_run_var.get())
            if not dry_run:
                try:
                    self.client._ensure_eth_account()
                except Exception as exc:
                    messagebox.showerror("依赖缺失", str(exc))
                    return
            if params.amount == self.AMOUNT_ALL_LABEL:
                source_counter: dict[str, int] = {}
                for _k, source, _target in jobs:
                    source_counter[source] = source_counter.get(source, 0) + 1
                duplicated = [s for s, c in source_counter.items() if c > 1]
                if duplicated:
                    messagebox.showerror("参数错误", "数量为“全部”时，同一个转出钱包只能执行 1 条任务")
                    return

            token_desc = self._token_desc_from_params(params)
            if not dry_run:
                amount_text = params.amount
                if params.random_enabled and params.random_min is not None and params.random_max is not None:
                    amount_text = f"随机 {params.random_min:.2f} ~ {params.random_max:.2f}"
                text = (
                    f"即将执行链上真实转账：\n"
                    f"模式：{self._mode()}\n"
                    f"任务数：{len(jobs)}\n"
                    f"网络：{params.network}\n"
                    f"币种：{token_desc}\n"
                    f"数量：{amount_text}\n"
                    f"执行间隔：{params.delay} 秒\n"
                    f"执行线程数：{params.threads}\n\n"
                    "确认继续？"
                )
                if not messagebox.askyesno("高风险确认", text):
                    self.log("用户取消了真实链上转账")
                    return

            self.log(f"已启动批量转账任务准备：任务={len(jobs)}，币种={token_desc}")
            self.is_running = True
            threading.Thread(target=self._run_batch_transfer, args=(jobs, params, dry_run), daemon=True).start()
        except Exception as exc:
            self.log(f"批量转账启动失败：{exc}")
            messagebox.showerror("执行异常", str(exc))

    def start_retry_failed(self):
        try:
            if self.is_running:
                messagebox.showwarning("提示", "已有任务在运行")
                return
            params = self._validate_transfer_params()
            if not params:
                return
            jobs = self._collect_failed_jobs(with_message=True)
            if not jobs:
                return
            dry_run = bool(self.dry_run_var.get())
            if not dry_run:
                try:
                    self.client._ensure_eth_account()
                except Exception as exc:
                    messagebox.showerror("依赖缺失", str(exc))
                    return
            if params.amount == self.AMOUNT_ALL_LABEL:
                source_counter: dict[str, int] = {}
                for _k, source, _target in jobs:
                    source_counter[source] = source_counter.get(source, 0) + 1
                duplicated = [s for s, c in source_counter.items() if c > 1]
                if duplicated:
                    messagebox.showerror("参数错误", "重试模式下，数量为“全部”时同一个转出钱包只能处理 1 条失败任务")
                    return

            token_desc = self._token_desc_from_params(params)
            if not dry_run:
                amount_text = params.amount
                if params.random_enabled and params.random_min is not None and params.random_max is not None:
                    amount_text = f"随机 {params.random_min:.2f} ~ {params.random_max:.2f}"
                text = (
                    f"即将重试链上失败转账：\n"
                    f"模式：{self._mode()}\n"
                    f"失败任务数：{len(jobs)}\n"
                    f"网络：{params.network}\n"
                    f"币种：{token_desc}\n"
                    f"数量：{amount_text}\n"
                    f"执行间隔：{params.delay} 秒\n"
                    f"执行线程数：{params.threads}\n\n"
                    "确认继续？"
                )
                if not messagebox.askyesno("重试确认", text):
                    self.log("用户取消了失败重试")
                    return

            self.log(f"开始重试失败任务：{len(jobs)}")
            self.is_running = True
            threading.Thread(target=self._run_batch_transfer, args=(jobs, params, dry_run), daemon=True).start()
        except Exception as exc:
            self.log(f"失败重试启动异常：{exc}")
            messagebox.showerror("执行异常", str(exc))

    def _run_batch_transfer(self, jobs_data: list[tuple[str, str, str]], params: WithdrawRuntimeParams, dry_run: bool):
        try:
            amount_view = params.amount
            token_desc = self._token_desc_from_params(params)
            if params.random_enabled and params.random_min is not None and params.random_max is not None:
                amount_view = f"random({params.random_min:.2f}~{params.random_max:.2f})"
            self.root.after(
                0,
                lambda: self.log(
                    f"开始批量链上转账：mode={self._mode()}，任务={len(jobs_data)}，network={params.network}，"
                    f"coin={token_desc}，amount={amount_view}，delay={params.delay}，threads={params.threads}，dry_run={dry_run}"
                ),
            )
            for row_key, _source, _target in jobs_data:
                self.root.after(0, lambda k=row_key: self._set_status(k, "waiting"))

            success = 0
            failed = 0
            lock = threading.Lock()
            nonce_lock_map: dict[str, threading.Lock] = {}
            nonce_next_map: dict[str, int] = {}
            nonce_guard = threading.Lock()

            jobs_q: queue.Queue[tuple[int, str, str, str]] = queue.Queue()
            for i, item in enumerate(jobs_data, start=1):
                row_key, source, target = item
                jobs_q.put((i, row_key, source, target))

            def alloc_nonce(source_addr: str) -> int:
                with nonce_guard:
                    source_lock = nonce_lock_map.setdefault(source_addr, threading.Lock())
                with source_lock:
                    nonce = nonce_next_map.get(source_addr)
                    if nonce is None:
                        nonce = self.client.get_nonce(params.network, source_addr)
                    nonce_next_map[source_addr] = nonce + 1
                    return nonce

            def rollback_nonce(source_addr: str, used_nonce: int):
                with nonce_guard:
                    source_lock = nonce_lock_map.setdefault(source_addr, threading.Lock())
                with source_lock:
                    cached_next = nonce_next_map.get(source_addr)
                    # 仅在当前缓存恰好对应本次分配的 nonce 时回滚，避免覆盖其他线程进度。
                    if cached_next == used_nonce + 1:
                        nonce_next_map.pop(source_addr, None)

            def worker():
                nonlocal success, failed
                total = len(jobs_data)
                while True:
                    try:
                        i, row_key, source, target = jobs_q.get_nowait()
                    except queue.Empty:
                        return

                    ok = False
                    msg = ""
                    used_nonce: int | None = None
                    source_addr = ""
                    self.root.after(0, lambda k=row_key: self._set_status(k, "running"))
                    try:
                        if dry_run:
                            if params.random_enabled and params.random_min is not None and params.random_max is not None:
                                amount_text = f"{self._random_decimal_between(params.random_min, params.random_max):.2f}"
                            elif params.amount == self.AMOUNT_ALL_LABEL:
                                amount_text = f"{self.AMOUNT_ALL_LABEL}(模拟)"
                            else:
                                amount_text = params.amount
                            prefix = f"[{i}/{total}][{self._mask_credential(source)}]"
                            msg = (
                                f"{prefix} 模拟成功 -> {params.coin} {amount_text} "
                                f"到 {self._mask(target, head=8, tail=6)}"
                            )
                        else:
                            private_key, source_addr = self._resolve_wallet(source)
                            value_units, gas_price_wei, gas_limit, amount_text = self._resolve_amount_and_gas(
                                params, source_addr, target
                            )
                            prefix = f"[{i}/{total}][{self._mask(source_addr, head=8, tail=6)}]"
                            nonce = alloc_nonce(source_addr)
                            used_nonce = nonce
                            if params.token_is_native:
                                txid = self.client.send_native_transfer(
                                    network=params.network,
                                    private_key=private_key,
                                    to_address=target,
                                    value_wei=value_units,
                                    nonce=nonce,
                                    gas_price_wei=gas_price_wei,
                                    gas_limit=gas_limit,
                                )
                            else:
                                txid = self.client.send_erc20_transfer(
                                    network=params.network,
                                    private_key=private_key,
                                    token_contract=params.token_contract,
                                    to_address=target,
                                    amount_units=value_units,
                                    nonce=nonce,
                                    gas_price_wei=gas_price_wei,
                                    gas_limit=gas_limit,
                                )
                            msg = (
                                f"{prefix} 转账成功 -> {params.coin} {amount_text} 到 {self._mask(target, head=8, tail=6)}，"
                                f"txid={txid}"
                            )
                        ok = True
                    except Exception as exc:
                        if used_nonce is not None and source_addr:
                            rollback_nonce(source_addr, used_nonce)
                        msg = f"[{i}/{total}] 转账失败：{exc}"
                    finally:
                        jobs_q.task_done()

                    self.root.after(0, lambda m=msg: self.log(m))
                    if ok:
                        self.root.after(0, lambda k=row_key: self._set_status(k, "success"))
                    else:
                        self.root.after(0, lambda k=row_key: self._set_status(k, "failed"))

                    with lock:
                        if ok:
                            success += 1
                        else:
                            failed += 1
                    if params.delay > 0:
                        time.sleep(params.delay)

            workers: list[threading.Thread] = []
            worker_count = max(1, min(params.threads, len(jobs_data)))
            for _ in range(worker_count):
                t = threading.Thread(target=worker, daemon=True)
                t.start()
                workers.append(t)
            for t in workers:
                t.join()

            summary = f"链上转账任务结束：成功 {success}，失败 {failed}"
            self.root.after(0, lambda: self.log(summary))
            self.root.after(0, lambda: messagebox.showinfo("执行完成", summary))
        except Exception as exc:
            err_text = str(exc)
            self.root.after(0, lambda m=f"链上转账任务异常终止：{err_text}": self.log(m))
            self.root.after(0, lambda e=err_text: messagebox.showerror("执行异常", e))
        finally:
            self.is_running = False
