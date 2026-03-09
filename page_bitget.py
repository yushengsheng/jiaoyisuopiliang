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

from api_clients import BitgetClient
from app_paths import BG_DATA_FILE
from core_models import WithdrawRuntimeParams
from shared_utils import LOG_MAX_ROWS, append_log_row, decimal_to_text, make_scrollbar, mask_text, random_decimal_between
from stores import BgOneToManyStore
from ui_dialogs import PasteImportDialog

class BitgetOneToManyPage:
    AMOUNT_MODE_FIXED = "固定数量"
    AMOUNT_MODE_RANDOM = "随机数"
    AMOUNT_MODE_ALL = "全部"
    AMOUNT_ALL_LABEL = "全部"

    def __init__(self, parent):
        self.parent = parent
        self.root = parent.winfo_toplevel()
        self.store = BgOneToManyStore(BG_DATA_FILE)
        self.client = BitgetClient()
        self.is_running = False
        self.row_index_map: dict[str, int] = {}
        self.row_id_by_addr: dict[str, str] = {}
        self.checked_addresses: set[str] = set()
        self.address_status: dict[str, str] = {}
        self.coin_network_cache: dict[str, list[str]] = {}
        self.source_asset_cache: dict[str, Decimal] = {}

        self.coin_var = StringVar(value="USDT")
        self.network_var = StringVar(value="")
        self.amount_mode_var = StringVar(value=self.AMOUNT_MODE_FIXED)
        self.amount_var = StringVar(value="")
        self.random_min_var = StringVar(value="")
        self.random_max_var = StringVar(value="")
        self.delay_var = DoubleVar(value=1.0)
        self.threads_var = StringVar(value="2")
        self.dry_run_var = BooleanVar(value=True)
        self.api_key_var = StringVar(value="")
        self.api_secret_var = StringVar(value="")
        self.passphrase_var = StringVar(value="")

        self._build_ui()
        self._load_data()

    def _build_ui(self):
        main = ttk.Frame(self.parent, padding=10)
        main.pack(fill=BOTH, expand=True)

        warning = "⚠️ Bitget 提现属于真实资金操作：默认模拟执行，确认参数无误后再关闭模拟。"
        ttk.Label(main, text=warning, foreground="#a94442").pack(anchor=W, pady=(0, 8))

        setting = ttk.LabelFrame(main, text="Bitget 1对多配置", padding=10)
        setting.pack(fill="x", pady=(0, 10))
        self.setting_frame = setting

        self.lbl_coin = ttk.Label(setting, text="提现币种*")
        self.coin_box = ttk.Combobox(setting, textvariable=self.coin_var, width=14)
        self.coin_box.configure(values=["USDT"])
        self.lbl_network = ttk.Label(setting, text="提现网络*")
        self.network_box = ttk.Combobox(setting, textvariable=self.network_var, width=16)

        self.lbl_amount = ttk.Label(setting, text="提现数量*")
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
        self.lbl_all_hint = ttk.Label(self.amount_ctrl, text="按可用余额", foreground="#666")

        self.chk_dry_run = ttk.Checkbutton(setting, text="模拟执行", variable=self.dry_run_var)
        self.lbl_delay = ttk.Label(setting, text="执行间隔(秒)")
        self.ent_delay = ttk.Entry(setting, textvariable=self.delay_var, width=10)
        self.lbl_threads = ttk.Label(setting, text="执行线程数")
        self.spin_threads = ttk.Spinbox(setting, from_=1, to=64, textvariable=self.threads_var, width=10)

        self.lbl_api_key = ttk.Label(setting, text="提现账号API Key*")
        self.ent_api_key = ttk.Entry(setting, textvariable=self.api_key_var)
        self.lbl_api_secret = ttk.Label(setting, text="提现账号API Secret*")
        self.ent_api_secret = ttk.Entry(setting, textvariable=self.api_secret_var, show="*")
        self.lbl_passphrase = ttk.Label(setting, text="Passphrase*")
        self.ent_passphrase = ttk.Entry(setting, textvariable=self.passphrase_var, show="*")
        self.lbl_hint = ttk.Label(setting, text="批量操作按勾选地址执行", foreground="#666")

        self._apply_amount_layout()
        self._layout_setting()

        self.table_wrap = ttk.Frame(main)
        self.table_wrap.pack(fill=BOTH, expand=True)
        self.table_wrap.columnconfigure(0, weight=1)
        self.table_wrap.rowconfigure(0, weight=1)

        cols = ("checked", "idx", "address", "status", "balance")
        self.tree = ttk.Treeview(self.table_wrap, columns=cols, show="headings", selectmode="extended", height=16)
        self.tree.heading("checked", text="勾选")
        self.tree.heading("idx", text="编号")
        self.tree.heading("address", text="提现地址")
        self.tree.heading("status", text="执行状态")
        self.tree.heading("balance", text="提现账号余额(全币种)")
        self.tree.column("checked", width=42, anchor="center")
        self.tree.column("idx", width=42, anchor="center")
        self.tree.column("address", width=430, anchor="w")
        self.tree.column("status", width=110, anchor="center")
        self.tree.column("balance", width=260, anchor="w")
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
        self.row_menu.add_command(label="提现（当前行）", command=self.start_withdraw_current_row)
        self.row_menu.add_separator()
        self.row_menu.add_command(label="删除（当前行）", command=self.delete_current_row)

        self.import_hint_label = ttk.Label(main, text="提示：点击地址列表后可按 Cmd+V / Ctrl+V 粘贴提现地址（每行一个地址）", foreground="#666")
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
        ttk.Button(action2, text="按余额刷新币种", command=self.start_refresh_coin_options).pack(side=LEFT)
        ttk.Button(action2, text="查询余额（全币种）", command=self.start_query_balance).pack(side=LEFT, padx=(8, 0))
        ttk.Button(action2, text="执行批量提现", style="Action.TButton", command=self.start_batch_withdraw).pack(side=RIGHT)
        ttk.Button(action2, text="失败重试", style="Action.TButton", command=self.start_retry_failed).pack(side=RIGHT, padx=(8, 0))

        self.log_box = ttk.LabelFrame(main, text="执行日志", padding=8)
        self.log_box.pack(fill=BOTH, expand=False)
        self.log_box.columnconfigure(0, weight=1)
        self.log_box.rowconfigure(0, weight=1)
        self.log_tree = ttk.Treeview(self.log_box, columns=("time", "msg"), show="headings", height=9)
        self.log_tree.heading("time", text="时间")
        self.log_tree.heading("msg", text="日志")
        self.log_tree.column("time", width=170, anchor="center")
        self.log_tree.column("msg", width=960, anchor="w")

        self.log_ybar = self._make_scrollbar(self.log_box, orient=VERTICAL, command=self.log_tree.yview)
        self.log_xbar = self._make_scrollbar(self.log_box, orient="horizontal", command=self.log_tree.xview)
        self.log_tree.configure(yscrollcommand=self.log_ybar.set, xscrollcommand=self.log_xbar.set)
        self.log_tree.grid(row=0, column=0, sticky="nsew")
        self.log_ybar.grid(row=0, column=1, sticky="ns")
        self.log_xbar.grid(row=1, column=0, sticky="ew")

        self.amount_mode_var.trace_add("write", self._on_amount_mode_changed)
        self.coin_var.trace_add("write", self._on_coin_var_changed)

    def _layout_setting(self):
        self.lbl_coin.grid(row=0, column=0, sticky=W)
        self.coin_box.grid(row=0, column=1, sticky="ew", padx=(4, 10))
        self.lbl_network.grid(row=0, column=2, sticky=W)
        self.network_box.grid(row=0, column=3, sticky="ew", padx=(4, 10))
        self.lbl_amount.grid(row=0, column=4, sticky=W)
        self.amount_ctrl.grid(row=0, column=5, sticky=W, padx=(4, 10))
        self.chk_dry_run.grid(row=0, column=6, sticky=W, padx=(4, 0))

        self.lbl_delay.grid(row=1, column=0, sticky=W, pady=(8, 0))
        self.ent_delay.grid(row=1, column=1, sticky=W, padx=(4, 10), pady=(8, 0))
        self.lbl_threads.grid(row=1, column=2, sticky=W, pady=(8, 0))
        self.spin_threads.grid(row=1, column=3, sticky=W, padx=(4, 10), pady=(8, 0))

        self.lbl_api_key.grid(row=2, column=0, sticky=W, pady=(8, 0))
        self.ent_api_key.grid(row=2, column=1, columnspan=6, sticky="ew", padx=(4, 0), pady=(8, 0))
        self.lbl_api_secret.grid(row=3, column=0, sticky=W, pady=(8, 0))
        self.ent_api_secret.grid(row=3, column=1, columnspan=3, sticky="ew", padx=(4, 10), pady=(8, 0))
        self.lbl_passphrase.grid(row=3, column=4, sticky=W, pady=(8, 0))
        self.ent_passphrase.grid(row=3, column=5, columnspan=2, sticky="ew", padx=(4, 0), pady=(8, 0))
        self.lbl_hint.grid(row=4, column=0, columnspan=7, sticky=W, pady=(8, 0))

        self.setting_frame.columnconfigure(1, weight=1)
        self.setting_frame.columnconfigure(3, weight=1)
        self.setting_frame.columnconfigure(5, weight=1)

    @staticmethod
    def _make_scrollbar(parent, orient, command):
        return make_scrollbar(parent, orient, command)

    @staticmethod
    def _decimal_to_text(v: Decimal) -> str:
        return decimal_to_text(v)

    @staticmethod
    def _mask(value: str, head: int = 6, tail: int = 4) -> str:
        return mask_text(value, head=head, tail=tail)

    def _totals_to_text(self, totals: dict[str, Decimal]) -> str:
        if not totals:
            return "-"
        arr = [(k, v) for k, v in totals.items() if v > 0]
        if not arr:
            return "-"
        arr.sort(key=lambda x: (-x[1], x[0]))
        return " | ".join([f"{c}:{self._decimal_to_text(v)}" for c, v in arr])

    def _amount_mode(self) -> str:
        m = self.amount_mode_var.get().strip()
        if m not in {self.AMOUNT_MODE_FIXED, self.AMOUNT_MODE_RANDOM, self.AMOUNT_MODE_ALL}:
            m = self.AMOUNT_MODE_FIXED
            self.amount_mode_var.set(m)
        return m

    def _apply_amount_layout(self):
        for w in (self.amount_mode_box, self.ent_amount, self.ent_random_min, self.lbl_random_sep, self.ent_random_max, self.lbl_all_hint):
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
            self.lbl_all_hint.pack(side=LEFT, padx=(4, 0))

    def _on_amount_mode_changed(self, *_args):
        self._apply_amount_layout()

    def _on_coin_var_changed(self, *_args):
        self.start_refresh_network_options()

    def _status_text(self, key: str) -> str:
        st = self.address_status.get(key, "")
        if st == "waiting":
            return "等待中"
        if st == "running":
            return "进行中"
        if st == "success":
            return "完成"
        if st == "failed":
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

    def _set_status(self, addr: str, status: str):
        self.address_status[addr] = status
        row_id = self.row_id_by_addr.get(addr)
        if not row_id or row_id not in self.row_index_map:
            return
        values = list(self.tree.item(row_id, "values"))
        if len(values) >= 5:
            values[3] = self._status_text(addr)
            self.tree.item(row_id, values=values)
        tag = self._status_tag(status)
        self.tree.item(row_id, tags=(tag,) if tag else ())

    def _balance_text(self) -> str:
        return self._totals_to_text(self.source_asset_cache)

    def _refresh_tree(self):
        self.tree.delete(*self.tree.get_children())
        self.row_index_map = {}
        self.row_id_by_addr = {}
        active = set(self.store.addresses)
        self.checked_addresses.intersection_update(active)
        self.address_status = {k: v for k, v in self.address_status.items() if k in active}
        bal_text = self._balance_text()

        for i, addr in enumerate(self.store.addresses, start=1):
            row_id = f"bg_row_{i}"
            self.row_index_map[row_id] = i - 1
            self.row_id_by_addr[addr] = row_id
            checked = "✓" if addr in self.checked_addresses else ""
            status_text = self._status_text(addr)
            tag = self._status_tag(self.address_status.get(addr, ""))
            self.tree.insert(
                "",
                END,
                iid=row_id,
                values=(checked, i, addr, status_text, bal_text),
                tags=(tag,) if tag else (),
            )

    def _selected_indices(self) -> list[int]:
        idxs: list[int] = []
        for row_id in self.tree.selection():
            if row_id in self.row_index_map:
                idxs.append(self.row_index_map[row_id])
        return sorted(set(idxs))

    def _checked_addr_list(self) -> list[str]:
        arr: list[str] = []
        for addr in self.store.addresses:
            if addr in self.checked_addresses:
                arr.append(addr)
        return arr

    def _failed_addr_list(self) -> list[str]:
        arr: list[str] = []
        for addr in self.store.addresses:
            if self.address_status.get(addr, "") == "failed":
                arr.append(addr)
        return arr

    def _on_tree_click(self, event):
        if self.tree.identify("region", event.x, event.y) != "cell":
            return None
        row_id = self.tree.identify_row(event.y)
        if not row_id:
            return None
        idx = self.row_index_map.get(row_id)
        if idx is None or not (0 <= idx < len(self.store.addresses)):
            return "break"
        key = self.store.addresses[idx]
        checked = key not in self.checked_addresses
        if checked:
            self.checked_addresses.add(key)
        else:
            self.checked_addresses.discard(key)
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

    def log(self, text: str):
        append_log_row(self.log_tree, text, max_rows=LOG_MAX_ROWS)

    def _parse_address_lines(self, lines: list[str]) -> list[str]:
        arr: list[str] = []
        seen: set[str] = set()
        for i, line in enumerate(lines, start=1):
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if any(x.isspace() for x in s):
                raise RuntimeError(f"第 {i} 行地址格式错误：地址中不能包含空白字符")
            if s in seen:
                continue
            seen.add(s)
            arr.append(s)
        return arr

    def _import_addresses(self, rows: list[str], source: str):
        if not rows:
            messagebox.showwarning("提示", "没有可导入的数据")
            return
        old = set(self.store.addresses)
        created = 0
        for x in rows:
            s = str(x or "").strip()
            if not s or s in old:
                continue
            self.store.addresses.append(s)
            old.add(s)
            created += 1
        self.checked_addresses = set(self.store.addresses)
        self._refresh_tree()
        self.log(f"{source}导入完成：新增地址 {created}，已自动全选 {len(self.store.addresses)} 个地址")

    def import_from_paste(self):
        dialog = PasteImportDialog(self.root, one_to_many=True)
        self.root.wait_window(dialog.top)
        if not dialog.result:
            return
        self._import_addresses([str(x) for x in dialog.result], "粘贴")

    def _on_tree_paste(self, _event=None):
        self.import_from_clipboard()
        return "break"

    def import_from_clipboard(self):
        try:
            text = self.root.clipboard_get()
        except Exception:
            messagebox.showwarning("提示", "剪贴板为空或不可读取")
            return
        try:
            rows = self._parse_address_lines(text.splitlines())
            self._import_addresses(rows, "剪贴板")
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
            rows = self._parse_address_lines(content.splitlines())
            self._import_addresses(rows, "TXT")
        except Exception as exc:
            messagebox.showerror("导入失败", str(exc))

    def export_txt(self):
        from tkinter import filedialog

        path = filedialog.asksaveasfilename(title="导出 TXT", defaultextension=".txt", filetypes=[("Text", "*.txt")])
        if not path:
            return
        try:
            lines = list(self.store.addresses)
            Path(path).write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
            self.log(f"TXT 导出完成：{path}")
            messagebox.showinfo("导出完成", f"已导出 {len(lines)} 条")
        except Exception as exc:
            messagebox.showerror("导出失败", str(exc))

    def toggle_check_all(self):
        if not self.store.addresses:
            messagebox.showwarning("提示", "暂无提现地址")
            return
        all_keys = set(self.store.addresses)
        if self.checked_addresses == all_keys:
            self.checked_addresses.clear()
            self.log("已取消全选")
        else:
            self.checked_addresses = set(all_keys)
            self.log(f"已全选 {len(all_keys)} 个地址")
        self._refresh_tree()

    def delete_selected(self):
        checked = set(self._checked_addr_list())
        idxs = [i for i, addr in enumerate(self.store.addresses) if addr in checked]
        if not idxs:
            messagebox.showwarning("提示", "请先勾选要删除的地址")
            return
        if not messagebox.askyesno("确认删除", f"确认删除 {len(idxs)} 条地址吗？"):
            return
        self.store.delete_by_indices(idxs)
        self._refresh_tree()
        self.log(f"已删除 {len(idxs)} 条地址")

    def delete_current_row(self):
        idx = self._single_selected_index()
        if idx is None or not (0 <= idx < len(self.store.addresses)):
            messagebox.showwarning("提示", "请先右键选中一条地址")
            return
        if not messagebox.askyesno("确认删除", "确认删除当前地址吗？"):
            return
        self.store.delete_by_indices([idx])
        self._refresh_tree()
        self.log("已删除当前地址")

    def start_query_balance_current_row(self):
        idx = self._single_selected_index()
        if idx is None or not (0 <= idx < len(self.store.addresses)):
            messagebox.showwarning("提示", "请先右键选中一条地址")
            return
        if self.is_running:
            messagebox.showwarning("提示", "已有任务在运行")
            return
        addr = self.store.addresses[idx]
        self.log(f"当前行余额查询：{self._mask(addr, head=8, tail=6)}")
        cred = self._source_cred(with_message=True)
        if not cred:
            return
        self.is_running = True
        threading.Thread(target=self._run_query_balance, args=(cred,), daemon=True).start()

    def start_withdraw_current_row(self):
        idx = self._single_selected_index()
        if idx is None or not (0 <= idx < len(self.store.addresses)):
            messagebox.showwarning("提示", "请先右键选中一条地址")
            return
        if self.is_running:
            messagebox.showwarning("提示", "已有任务在运行")
            return
        params = self._validate_withdraw_params()
        if not params:
            return
        cred = self._source_cred(with_message=True)
        if not cred:
            return
        addr = self.store.addresses[idx]
        addrs = [addr]
        dry_run = bool(self.dry_run_var.get())
        if not dry_run:
            amount_text = params.amount
            if params.random_enabled and params.random_min is not None and params.random_max is not None:
                amount_text = f"随机 {params.random_min:.2f} ~ {params.random_max:.2f}"
            text = (
                f"即将执行 Bitget 当前行真实提现：\n"
                f"地址数：1\n"
                f"币种：{params.coin}\n"
                f"数量：{amount_text}\n"
                f"网络：{params.network}\n"
                f"执行间隔：{params.delay} 秒\n"
                f"执行线程数：{params.threads}\n\n"
                "确认继续？"
            )
            if not messagebox.askyesno("高风险确认", text):
                self.log("用户取消了当前行真实提现")
                return
        self.is_running = True
        threading.Thread(target=self._run_withdraw, args=(addrs, params, cred, dry_run), daemon=True).start()

    def _apply_settings_to_store(self) -> bool:
        coin = self.coin_var.get().strip().upper()
        network = self.network_var.get().strip()
        mode = self._amount_mode()
        amount = self.amount_var.get().strip()
        if mode == self.AMOUNT_MODE_ALL:
            amount = self.AMOUNT_ALL_LABEL
        if not coin:
            messagebox.showerror("参数错误", "提现币种不能为空")
            return False
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
        self.store.settings = BgOneToManySettings(
            coin=coin,
            network=network,
            amount_mode=mode,
            amount=amount,
            random_min=self.random_min_var.get().strip(),
            random_max=self.random_max_var.get().strip(),
            delay_seconds=delay,
            worker_threads=threads,
            dry_run=bool(self.dry_run_var.get()),
            api_key=self.api_key_var.get().strip(),
            api_secret=self.api_secret_var.get().strip(),
            passphrase=self.passphrase_var.get().strip(),
        )
        return True

    def save_all(self):
        if not self._apply_settings_to_store():
            return
        try:
            self.store.save()
            self.log("配置已保存")
            messagebox.showinfo("成功", f"配置已保存到：{BG_DATA_FILE}")
        except Exception as exc:
            messagebox.showerror("保存失败", str(exc))

    def _load_data(self):
        try:
            self.store.load()
            st = self.store.settings
            self.coin_var.set(st.coin or "USDT")
            self.network_var.set(st.network or "")
            self.amount_var.set("" if st.amount_mode == self.AMOUNT_MODE_ALL else (st.amount or ""))
            self.amount_mode_var.set(st.amount_mode or self.AMOUNT_MODE_FIXED)
            self.random_min_var.set(st.random_min or "")
            self.random_max_var.set(st.random_max or "")
            self.delay_var.set(st.delay_seconds)
            self.threads_var.set(str(max(1, int(st.worker_threads or 1))))
            self.dry_run_var.set(st.dry_run)
            self.api_key_var.set(st.api_key or "")
            self.api_secret_var.set(st.api_secret or "")
            self.passphrase_var.set(st.passphrase or "")
            self._refresh_tree()
            self.start_refresh_network_options()
            self.log("Bitget 配置加载完成")
        except Exception as exc:
            messagebox.showerror("加载失败", str(exc))

    def _source_cred(self, with_message: bool = False) -> tuple[str, str, str] | None:
        k = self.api_key_var.get().strip()
        s = self.api_secret_var.get().strip()
        p = self.passphrase_var.get().strip()
        if not k or not s or not p:
            if with_message:
                messagebox.showerror("参数错误", "请填写 Bitget API Key / API Secret / Passphrase")
            return None
        return k, s, p

    def start_refresh_network_options(self):
        coin = self.coin_var.get().strip().upper()
        if not coin:
            self.network_box.configure(values=[""])
            self.network_var.set("")
            return
        cached = self.coin_network_cache.get(coin)
        if cached is not None:
            self._apply_network_options(coin, cached, cache_result=True)
            return
        self.network_box.configure(values=[""])
        self.network_var.set("")
        threading.Thread(target=self._run_refresh_network_options, args=(coin,), daemon=True).start()

    def _run_refresh_network_options(self, coin: str):
        try:
            arr = self.client.get_coin_networks(coin)
            self.root.after(0, lambda c=coin, x=arr: self._apply_network_options(c, x, cache_result=True))
        except Exception as exc:
            self.root.after(0, lambda m=f"{coin} 网络读取失败：{exc}": self.log(m))
            self.root.after(0, lambda c=coin: self._apply_network_options(c, [], cache_result=False))

    def _apply_network_options(self, coin: str, networks: list[str], cache_result: bool):
        arr = [x.strip() for x in networks if x and x.strip()]
        if cache_result:
            self.coin_network_cache[coin] = arr
        if self.coin_var.get().strip().upper() != coin:
            return
        values = [""] + arr
        self.network_box.configure(values=values)
        current = self.network_var.get().strip()
        if current and current in values:
            self.network_var.set(current)
        elif current and current not in values:
            self.network_var.set("")

    def _update_coin_options(self, totals: dict[str, Decimal]):
        ranked = sorted(totals.items(), key=lambda x: (-x[1], x[0]))
        options = [coin for coin, _ in ranked]
        current = self.coin_var.get().strip().upper()
        fallback = current or "USDT"
        if fallback and fallback not in options:
            options.insert(0, fallback)
        if not options:
            options = ["USDT"]
        self.coin_box.configure(values=options)
        if current in options:
            self.coin_var.set(current)
        else:
            self.coin_var.set(options[0])

    def start_refresh_coin_options(self):
        if self.is_running:
            messagebox.showwarning("提示", "已有任务在运行")
            return
        cred = self._source_cred(with_message=True)
        if not cred:
            return
        self.is_running = True
        threading.Thread(target=self._run_refresh_coin_options, args=(cred,), daemon=True).start()

    def _run_refresh_coin_options(self, cred: tuple[str, str, str]):
        try:
            k, s, p = cred
            self.root.after(0, lambda: self.log("开始按余额刷新币种"))
            totals = self.client.get_account_assets(k, s, p)
            self.source_asset_cache = dict(totals)
            self.root.after(0, lambda: self._update_coin_options(totals))
            self.root.after(0, lambda: self._refresh_tree())
            ranked = sorted(totals.items(), key=lambda x: (-x[1], x[0]))
            if ranked:
                top = ", ".join([f"{c}={self._decimal_to_text(v)}" for c, v in ranked[:8]])
                summary = f"币种刷新结束：可用币种 {len(ranked)}；Top: {top}"
            else:
                summary = "币种刷新结束：未发现可用余额币种"
            self.root.after(0, lambda: self.log(summary))
        except Exception as exc:
            err_text = str(exc)
            self.root.after(0, lambda m=f"币种刷新失败：{err_text}": self.log(m))
            self.root.after(0, lambda e=err_text: messagebox.showerror("执行异常", e))
        finally:
            self.is_running = False

    def start_query_balance(self):
        if self.is_running:
            messagebox.showwarning("提示", "已有任务在运行")
            return
        cred = self._source_cred(with_message=True)
        if not cred:
            return
        self.is_running = True
        threading.Thread(target=self._run_query_balance, args=(cred,), daemon=True).start()

    def _run_query_balance(self, cred: tuple[str, str, str]):
        try:
            k, s, p = cred
            self.root.after(0, lambda: self.log("开始查询余额（全币种）"))
            totals = self.client.get_account_assets(k, s, p)
            self.source_asset_cache = dict(totals)
            self.root.after(0, lambda: self._refresh_tree())
            summary = f"余额查询结束：{self._totals_to_text(totals)}"
            self.root.after(0, lambda: self.log(summary))
        except Exception as exc:
            err_text = str(exc)
            self.root.after(0, lambda m=f"余额查询失败：{err_text}": self.log(m))
            self.root.after(0, lambda e=err_text: messagebox.showerror("执行异常", e))
        finally:
            self.is_running = False

    @classmethod
    def _random_decimal_between(cls, low: Decimal, high: Decimal) -> Decimal:
        return random_decimal_between(low, high)

    def _resolve_amount(self, params: WithdrawRuntimeParams, cred: tuple[str, str, str], dry_run: bool = False) -> str:
        if params.random_enabled and params.random_min is not None and params.random_max is not None:
            val = self._random_decimal_between(params.random_min, params.random_max)
            if val <= 0:
                raise RuntimeError("随机金额生成失败：结果必须大于 0")
            return f"{val:.2f}"
        if params.amount == self.AMOUNT_ALL_LABEL:
            if dry_run:
                return f"{self.AMOUNT_ALL_LABEL}(模拟)"
            k, s, p = cred
            available = self.client.get_available_balance(k, s, p, params.coin)
            if available <= 0:
                raise RuntimeError(f"{params.coin} 可用余额为 0")
            return self._decimal_to_text(available)
        return params.amount

    def _validate_withdraw_params(self) -> WithdrawRuntimeParams | None:
        coin = self.coin_var.get().strip().upper()
        network = self.network_var.get().strip()
        mode = self._amount_mode()
        amount_raw = self.amount_var.get().strip()
        random_enabled = mode == self.AMOUNT_MODE_RANDOM
        if not coin:
            messagebox.showerror("参数错误", "提现币种不能为空")
            return None
        if not network:
            messagebox.showerror("参数错误", "未设置提现网络，无法提现")
            return None

        if mode == self.AMOUNT_MODE_ALL:
            amount = self.AMOUNT_ALL_LABEL
        else:
            if mode == self.AMOUNT_MODE_FIXED and not amount_raw:
                messagebox.showerror("参数错误", "提现数量不能为空")
                return None
            try:
                if mode == self.AMOUNT_MODE_FIXED:
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
        )

    def start_batch_withdraw(self):
        if self.is_running:
            messagebox.showwarning("提示", "已有任务在运行")
            return
        params = self._validate_withdraw_params()
        if not params:
            return
        cred = self._source_cred(with_message=True)
        if not cred:
            return
        addrs = self._checked_addr_list()
        if not addrs:
            messagebox.showwarning("提示", "请先勾选至少一个提现地址")
            return
        if params.amount == self.AMOUNT_ALL_LABEL and len(addrs) > 1:
            messagebox.showerror("参数错误", "1对多模式下，数量为“全部”时只能勾选 1 个地址")
            return

        dry_run = bool(self.dry_run_var.get())
        if not dry_run:
            amount_text = params.amount
            if params.random_enabled and params.random_min is not None and params.random_max is not None:
                amount_text = f"随机 {params.random_min:.2f} ~ {params.random_max:.2f}"
            text = (
                f"即将执行 Bitget 真实提现：\n"
                f"地址数：{len(addrs)}\n"
                f"币种：{params.coin}\n"
                f"数量：{amount_text}\n"
                f"网络：{params.network}\n"
                f"执行间隔：{params.delay} 秒\n"
                f"执行线程数：{params.threads}\n\n"
                "确认继续？"
            )
            if not messagebox.askyesno("高风险确认", text):
                self.log("用户取消了真实提现")
                return

        self.is_running = True
        threading.Thread(target=self._run_withdraw, args=(addrs, params, cred, dry_run), daemon=True).start()

    def start_retry_failed(self):
        if self.is_running:
            messagebox.showwarning("提示", "已有任务在运行")
            return
        params = self._validate_withdraw_params()
        if not params:
            return
        cred = self._source_cred(with_message=True)
        if not cred:
            return

        addrs = self._failed_addr_list()
        if not addrs:
            messagebox.showwarning("提示", "当前没有失败地址可重试")
            return
        if params.amount == self.AMOUNT_ALL_LABEL and len(addrs) > 1:
            messagebox.showerror("参数错误", "重试模式下，数量为“全部”时只能处理 1 个失败地址")
            return

        dry_run = bool(self.dry_run_var.get())
        if not dry_run:
            amount_text = params.amount
            if params.random_enabled and params.random_min is not None and params.random_max is not None:
                amount_text = f"随机 {params.random_min:.2f} ~ {params.random_max:.2f}"
            text = (
                f"即将重试 Bitget 失败提现：\n"
                f"失败地址数：{len(addrs)}\n"
                f"币种：{params.coin}\n"
                f"数量：{amount_text}\n"
                f"网络：{params.network}\n"
                f"执行间隔：{params.delay} 秒\n"
                f"执行线程数：{params.threads}\n\n"
                "确认继续？"
            )
            if not messagebox.askyesno("重试确认", text):
                self.log("用户取消了失败重试")
                return

        self.log(f"开始重试失败地址：{len(addrs)}")
        self.is_running = True
        threading.Thread(target=self._run_withdraw, args=(addrs, params, cred, dry_run), daemon=True).start()

    def _run_withdraw(self, addrs: list[str], params: WithdrawRuntimeParams, cred: tuple[str, str, str], dry_run: bool):
        try:
            amount_view = params.amount
            if params.random_enabled and params.random_min is not None and params.random_max is not None:
                amount_view = f"random({params.random_min:.2f}~{params.random_max:.2f})"
            self.root.after(
                0,
                lambda: self.log(
                    f"开始批量提现：地址数={len(addrs)}, coin={params.coin}, amount={amount_view}, "
                    f"network={params.network}, delay={params.delay}, threads={params.threads}, dry_run={dry_run}"
                ),
            )
            for addr in addrs:
                self.root.after(0, lambda a=addr: self._set_status(a, "waiting"))

            success = 0
            failed = 0
            lock = threading.Lock()
            jobs: queue.Queue[tuple[int, str]] = queue.Queue()
            for i, addr in enumerate(addrs, start=1):
                jobs.put((i, addr))

            def worker():
                nonlocal success, failed
                while True:
                    try:
                        i, addr = jobs.get_nowait()
                    except queue.Empty:
                        return
                    prefix = f"[{i}/{len(addrs)}][{self._mask(addr, head=8, tail=6)}]"
                    ok = False
                    msg = ""
                    self.root.after(0, lambda a=addr: self._set_status(a, "running"))
                    try:
                        amount = self._resolve_amount(params, cred, dry_run=dry_run)
                        if dry_run:
                            msg = f"{prefix} 模拟成功 -> {params.coin} {amount}"
                        else:
                            k, s, p = cred
                            resp = self.client.withdraw(k, s, p, coin=params.coin, address=addr, amount=amount, chain=params.network)
                            order_id = resp.get("orderId", "N/A")
                            msg = f"{prefix} 提现成功 -> {params.coin} {amount}，orderId={order_id}"
                        ok = True
                    except Exception as exc:
                        msg = f"{prefix} 提现失败：{exc}"
                    finally:
                        jobs.task_done()

                    self.root.after(0, lambda m=msg: self.log(m))
                    if ok:
                        self.root.after(0, lambda a=addr: self._set_status(a, "success"))
                    else:
                        self.root.after(0, lambda a=addr: self._set_status(a, "failed"))

                    with lock:
                        if ok:
                            success += 1
                        else:
                            failed += 1
                    if params.delay > 0:
                        time.sleep(params.delay)

            workers: list[threading.Thread] = []
            worker_count = max(1, min(params.threads, len(addrs)))
            for _ in range(worker_count):
                t = threading.Thread(target=worker, daemon=True)
                t.start()
                workers.append(t)
            for t in workers:
                t.join()

            summary = f"Bitget 提现任务结束：成功 {success}，失败 {failed}"
            self.root.after(0, lambda: self.log(summary))
            self.root.after(0, lambda: messagebox.showinfo("执行完成", summary))
        except Exception as exc:
            err_text = str(exc)
            self.root.after(0, lambda m=f"Bitget 提现任务异常终止：{err_text}": self.log(m))
            self.root.after(0, lambda e=err_text: messagebox.showerror("执行异常", e))
        finally:
            self.is_running = False
