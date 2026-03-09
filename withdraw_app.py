#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import threading
from decimal import Decimal
from tkinter import BooleanVar, DoubleVar, StringVar, Tk
from tkinter import ttk

from api_clients import BinanceClient
from app_paths import DATA_FILE
import batch_actions
from binance_tasks import run_query_balance, run_refresh_coin_options, run_withdraw
from core_models import (
    AccountEntry,
    WithdrawRuntimeParams,
)
import ip_detection
import network_options
import withdraw_config_state
import withdraw_params
import withdraw_ui
import withdraw_view_state
from shared_utils import (
    LOG_MAX_ROWS,
    append_log_row,
    decimal_to_text,
    make_scrollbar,
    mask_text,
    parse_worker_threads,
)
from stores import AccountStore
import withdraw_account_ops as account_ops

class WithdrawApp:
    MODE_M2M = "多对多"
    MODE_1M = "1对多"
    AMOUNT_MODE_FIXED = "固定数量"
    AMOUNT_MODE_RANDOM = "随机数"
    AMOUNT_MODE_ALL = "全部"
    NETWORK_OPTIONS = [
        "",
        "BSC",
        "ETH",
        "TRX",
        "ARBITRUM",
        "MATIC",
        "OPTIMISM",
        "SOL",
        "AVAXC",
    ]
    AMOUNT_ALL_LABEL = "全部"
    AMOUNT_ALL_ALIASES = {"ALL", "MAX", "100%"}
    MENU_ITEMS = [
        "币安提现",
        "bg交易所1对多",
        "链上",
    ]
    TREE_COL_MIN_WIDTHS = {
        "checked": 42,
        "idx": 42,
        "api_key": 140,
        "api_secret": 140,
        "address": 200,
        "status": 96,
        "balance": 160,
    }
    TREE_COL_WEIGHTS = {
        "checked": 1,
        "idx": 1,
        "api_key": 3,
        "api_secret": 3,
        "address": 4,
        "status": 2,
        "balance": 3,
    }

    def __init__(self, root: Tk):
        self.root = root
        self.root.title("交易所批量工具")
        self.root.geometry("1180x760")
        self.root.minsize(860, 560)

        self.store = AccountStore(DATA_FILE)
        self.client = BinanceClient()
        self.is_running = False
        self.row_index_map: dict[str, int] = {}
        self.row_id_by_api_key: dict[str, str] = {}
        self.checked_api_keys: set[str] = set()
        self.checked_one_to_many_addresses: set[str] = set()
        self.coin_balance_totals: dict[str, Decimal] = {}
        self.account_coin_balance_cache: dict[str, dict[str, Decimal]] = {}
        self.coin_network_cache: dict[str, list[str]] = {}
        self.account_withdraw_status: dict[str, str] = {}
        self._compact_mode: bool | None = None

        self.mode_var = StringVar(value=self.MODE_M2M)
        self.coin_var = StringVar(value="USDT")
        self.network_var = StringVar(value="")
        self.amount_mode_var = StringVar(value=self.AMOUNT_MODE_FIXED)
        self.amount_var = StringVar(value="")
        self.random_min_var = StringVar(value="")
        self.random_max_var = StringVar(value="")
        self.source_api_key_var = StringVar(value="")
        self.source_api_secret_var = StringVar(value="")
        self.source_balance_var = StringVar(value="-")
        self.delay_var = DoubleVar(value=1.0)
        self.threads_var = StringVar(value="2")
        self.dry_run_var = BooleanVar(value=True)
        self.ip_var = StringVar(value="未检测")
        self.ip_detect_inflight = False
        self.ip_detect_lock = threading.Lock()

        self._build_ui()
        self._load_data()
        self.root.after(300, self.start_detect_ip)

    def _build_ui(self):
        withdraw_ui.build_ui(self)

    @staticmethod
    def _make_scrollbar(parent, orient, command):
        return make_scrollbar(parent, orient, command)

    def _build_waiting_page(self, page: ttk.Frame, title: str):
        withdraw_ui.build_waiting_page(self, page, title)

    def _on_root_resize(self, _event=None):
        withdraw_ui.on_root_resize(self, _event)

    def _is_one_to_many_mode(self) -> bool:
        return self.mode_var.get().strip() == self.MODE_1M

    def _amount_mode(self) -> str:
        mode = self.amount_mode_var.get().strip()
        if mode not in {self.AMOUNT_MODE_FIXED, self.AMOUNT_MODE_RANDOM, self.AMOUNT_MODE_ALL}:
            mode = self.AMOUNT_MODE_FIXED
            self.amount_mode_var.set(mode)
        return mode

    def _runtime_worker_threads(self) -> int:
        raw = self.threads_var.get() if hasattr(self, "threads_var") else 2
        return parse_worker_threads(raw, default=2)

    def _apply_amount_input_layout(self):
        withdraw_ui.apply_amount_input_layout(self)

    def _on_amount_mode_changed(self, *_args):
        withdraw_ui.on_amount_mode_changed(self, *_args)

    def _apply_setting_layout(self, compact: bool):
        withdraw_ui.apply_setting_layout(self, compact)

    def _on_table_resize(self, _event=None):
        withdraw_ui.on_table_resize(self, _event)

    def _resize_tree_columns(self):
        withdraw_ui.resize_tree_columns(self)

    def _on_log_resize(self, _event=None):
        withdraw_ui.on_log_resize(self, _event)

    def _load_data(self):
        withdraw_config_state.load_data(self)

    def _apply_settings_to_store(self) -> bool:
        return withdraw_config_state.apply_settings_to_store(self)

    @staticmethod
    def _decimal_to_text(v: Decimal) -> str:
        return decimal_to_text(v)

    @classmethod
    def _is_amount_all(cls, value: str) -> bool:
        return withdraw_params.is_amount_all(value, cls.AMOUNT_ALL_LABEL, cls.AMOUNT_ALL_ALIASES)

    def _on_mode_var_changed(self, *_args):
        withdraw_view_state.on_mode_var_changed(self, *_args)

    def _on_source_api_changed(self, *_args):
        withdraw_view_state.on_source_api_changed(self, *_args)

    def _update_source_balance_display(self):
        withdraw_view_state.update_source_balance_display(self)

    def _on_coin_var_changed(self, *_args):
        withdraw_view_state.on_coin_var_changed(self, *_args)

    def _update_coin_options_from_totals(self, totals: dict[str, Decimal]):
        withdraw_view_state.update_coin_options_from_totals(self, totals)

    def _current_coin(self) -> str:
        return withdraw_view_state.current_coin(self)

    def _build_source_account(self, with_message: bool = False) -> AccountEntry | None:
        return withdraw_view_state.build_source_account(self, with_message=with_message)

    def _pick_meta_account(self) -> AccountEntry | None:
        return withdraw_view_state.pick_meta_account(self)

    def start_refresh_network_options(self):
        network_options.start_refresh_network_options(self)

    def _run_refresh_network_options(self, account: AccountEntry, account_key: str, coin: str, cache_key: str):
        network_options.run_refresh_network_options(self, account, account_key, coin, cache_key)

    def _apply_network_options(self, cache_key: str, account_key: str, coin: str, networks: list[str], cache_result: bool):
        network_options.apply_network_options(self, cache_key, account_key, coin, networks, cache_result)

    def _all_balances_text(self, api_key: str) -> str:
        return withdraw_view_state.all_balances_text(self, api_key)

    def _one_to_many_balance_text(self) -> str:
        return withdraw_view_state.one_to_many_balance_text(self)

    def _withdraw_status_text(self, api_key: str) -> str:
        return withdraw_view_state.withdraw_status_text(self, api_key)

    @staticmethod
    def _withdraw_status_tag(status: str) -> str:
        return withdraw_view_state.withdraw_status_tag(status)

    def _set_account_status(self, api_key: str, status: str):
        withdraw_view_state.set_account_status(self, api_key, status)

    def _totals_to_text(self, totals: dict[str, Decimal]) -> str:
        return withdraw_view_state.totals_to_text(self, totals)

    def _replace_account_coin_totals(self, api_key: str, totals: dict[str, Decimal]):
        withdraw_view_state.replace_account_coin_totals(self, api_key, totals)

    @staticmethod
    def _mask(value: str, head: int = 6, tail: int = 4) -> str:
        return mask_text(value, head=head, tail=tail)

    def _refresh_tree(self):
        account_ops.refresh_tree(self)

    def _selected_indices(self) -> list[int]:
        return account_ops.selected_indices(self)

    def _selected_accounts(self) -> list[AccountEntry]:
        return account_ops.selected_accounts(self)

    def _checked_indices(self) -> list[int]:
        return account_ops.checked_indices(self)

    def _checked_accounts(self) -> list[AccountEntry]:
        return account_ops.checked_accounts(self)

    def _checked_one_to_many_addresses(self) -> list[str]:
        return account_ops.checked_one_to_many_addresses(self)

    def _failed_accounts_for_retry(self) -> list[AccountEntry]:
        return account_ops.failed_accounts_for_retry(self)

    def _on_tree_click(self, event):
        return account_ops.on_tree_click(self, event)

    def _on_tree_right_click(self, event):
        return account_ops.on_tree_right_click(self, event)

    def toggle_check_all(self):
        account_ops.toggle_check_all(self)

    def _parse_account_lines(self, lines: list[str]) -> list[AccountEntry]:
        return account_ops.parse_account_lines(lines)

    def _parse_address_lines(self, lines: list[str]) -> list[str]:
        return account_ops.parse_address_lines(lines)

    def _import_rows(self, rows: list[AccountEntry] | list[str], source: str):
        account_ops.import_rows(self, rows, source)

    def import_from_paste(self):
        account_ops.import_from_paste(self)

    def _on_tree_paste(self, _event=None):
        return account_ops.on_tree_paste(self, _event)

    def import_from_clipboard(self):
        account_ops.import_from_clipboard(self)

    def import_txt(self):
        account_ops.import_txt(self)

    def export_txt(self):
        account_ops.export_txt(self)

    def delete_selected(self):
        account_ops.delete_selected(self)

    def delete_current_row(self):
        account_ops.delete_current_row(self)

    def start_query_balance_current_row(self):
        account_ops.start_query_balance_current_row(self)

    def start_withdraw_current_row(self):
        account_ops.start_withdraw_current_row(self)

    def save_all(self):
        account_ops.save_all(self)

    def log(self, text: str):
        append_log_row(self.log_tree, text, max_rows=LOG_MAX_ROWS)

    def start_detect_ip(self):
        ip_detection.start_detect_ip(self)

    def _finish_detect_ip(self):
        ip_detection.finish_detect_ip(self)

    @staticmethod
    def _fetch_public_ip() -> str:
        return ip_detection.fetch_public_ip()

    def _run_detect_ip(self):
        ip_detection.run_detect_ip(self)

    @classmethod
    def _random_decimal_between(cls, low: Decimal, high: Decimal) -> Decimal:
        return withdraw_params.random_withdraw_decimal(low, high)

    def _resolve_withdraw_amount(self, account: AccountEntry, params: WithdrawRuntimeParams, dry_run: bool = False) -> str:
        return withdraw_params.resolve_withdraw_amount(self, account, params, dry_run=dry_run)

    def _validate_withdraw_params(self) -> WithdrawRuntimeParams | None:
        return withdraw_params.validate_withdraw_params(self)

    def start_batch_withdraw(self):
        batch_actions.start_batch_withdraw(self)

    def start_retry_failed(self):
        batch_actions.start_retry_failed(self)

    def _run_withdraw(self, accounts: list[AccountEntry], params: WithdrawRuntimeParams, dry_run: bool, one_to_many: bool):
        run_withdraw(self, accounts, params, dry_run, one_to_many)

    def start_refresh_coin_options(self):
        batch_actions.start_refresh_coin_options(self)

    def _run_refresh_coin_options(self, accounts: list[AccountEntry]):
        run_refresh_coin_options(self, accounts)

    def start_query_balance(self):
        batch_actions.start_query_balance(self)

    def _run_query_balance(self, accounts: list[AccountEntry], coin: str):
        run_query_balance(self, accounts, coin)
