#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import ast
import json
import re
import sys
import tempfile
from decimal import Decimal
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import main as app
import batch_actions
import network_options
import page_bitget
import page_onchain
import withdraw_account_ops


class DummyVar:
    def __init__(self, value):
        self.value = value

    def get(self):
        return self.value

    def set(self, value):
        self.value = value


class DummyRoot:
    def after(self, _ms, callback=None):
        if callback is not None:
            callback()


class DummyButton:
    def __init__(self):
        self.state = "normal"

    def configure(self, **kwargs):
        if "state" in kwargs:
            self.state = kwargs["state"]


class FakeLogTree:
    def __init__(self):
        self.rows: list[tuple[str, tuple[str, str]]] = []
        self._seq = 0
        self.last_seen = ""

    def insert(self, _parent, _index, values=()):
        self._seq += 1
        row_id = f"row_{self._seq}"
        self.rows.append((row_id, values))
        return row_id

    def get_children(self):
        return [row_id for row_id, _ in self.rows]

    def delete(self, *row_ids):
        remove = set(row_ids)
        self.rows = [item for item in self.rows if item[0] not in remove]

    def see(self, row_id):
        self.last_seen = row_id


class FakeToggleTree:
    def __init__(self, *, row_id: str, column: str, values: list[str | int], region: str = "cell"):
        self._row_id = row_id
        self._column = column
        self._region = region
        self._values_by_row = {row_id: list(values)}
        self._selection: tuple[str, ...] = ()
        self._focus = ""

    def identify(self, kind, _x, _y):
        if kind == "region":
            return self._region
        return ""

    def identify_row(self, _y):
        return self._row_id

    def identify_column(self, _x):
        return self._column

    def item(self, row_id, option=None, **kwargs):
        if "values" in kwargs:
            self._values_by_row[row_id] = list(kwargs["values"])
            return None
        if option == "values":
            return list(self._values_by_row.get(row_id, []))
        return {"values": list(self._values_by_row.get(row_id, []))}

    def selection_set(self, row_id):
        self._selection = (row_id,)

    def selection(self):
        return self._selection

    def focus(self, row_id):
        self._focus = row_id


class FakePopupMenu:
    def __init__(self):
        self.popped = False
        self.released = False
        self.last_x = None
        self.last_y = None

    def tk_popup(self, x, y):
        self.popped = True
        self.last_x = x
        self.last_y = y

    def grab_release(self):
        self.released = True


class FakeComboBox:
    def __init__(self):
        self.values = []

    def configure(self, **kwargs):
        if "values" in kwargs:
            self.values = list(kwargs["values"])


class DummyClickEvent:
    def __init__(self, x: int = 0, y: int = 0, x_root: int = 0, y_root: int = 0):
        self.x = x
        self.y = y
        self.x_root = x_root
        self.y_root = y_root


class MessagePatch:
    def __init__(self):
        self.errors: list[tuple[str, str]] = []
        self.warnings: list[tuple[str, str]] = []
        self.infos: list[tuple[str, str]] = []
        self._orig = {}

    def __enter__(self):
        self._orig["showerror"] = app.messagebox.showerror
        self._orig["showwarning"] = app.messagebox.showwarning
        self._orig["showinfo"] = app.messagebox.showinfo
        self._orig["askyesno"] = app.messagebox.askyesno
        app.messagebox.showerror = lambda title, msg: self.errors.append((str(title), str(msg)))
        app.messagebox.showwarning = lambda title, msg: self.warnings.append((str(title), str(msg)))
        app.messagebox.showinfo = lambda title, msg: self.infos.append((str(title), str(msg)))
        app.messagebox.askyesno = lambda *_args, **_kwargs: True
        return self

    def __exit__(self, exc_type, exc, tb):
        app.messagebox.showerror = self._orig["showerror"]
        app.messagebox.showwarning = self._orig["showwarning"]
        app.messagebox.showinfo = self._orig["showinfo"]
        app.messagebox.askyesno = self._orig["askyesno"]
        return False


class ThreadPatch:
    def __init__(self):
        self._orig_main = None
        self._orig_batch = None
        self._orig_ops = None
        self._orig_bitget = None
        self._orig_onchain = None

    def __enter__(self):
        self._orig_main = app.threading.Thread
        self._orig_batch = batch_actions.threading.Thread
        self._orig_ops = withdraw_account_ops.threading.Thread
        self._orig_bitget = page_bitget.threading.Thread
        self._orig_onchain = page_onchain.threading.Thread

        class ImmediateThread:
            def __init__(self, target=None, args=(), kwargs=None, daemon=None):
                self._target = target
                self._args = args
                self._kwargs = kwargs or {}

            def start(self):
                if self._target is not None:
                    self._target(*self._args, **self._kwargs)

            def join(self):
                return None

        app.threading.Thread = ImmediateThread
        batch_actions.threading.Thread = ImmediateThread
        withdraw_account_ops.threading.Thread = ImmediateThread
        page_bitget.threading.Thread = ImmediateThread
        page_onchain.threading.Thread = ImmediateThread
        return self

    def __exit__(self, exc_type, exc, tb):
        app.threading.Thread = self._orig_main
        batch_actions.threading.Thread = self._orig_batch
        withdraw_account_ops.threading.Thread = self._orig_ops
        page_bitget.threading.Thread = self._orig_bitget
        page_onchain.threading.Thread = self._orig_onchain
        return False


class FakeClient:
    def withdraw(self, account: app.AccountEntry, coin: str, amount: str, network: str = ""):
        if "bad" in account.address:
            raise RuntimeError("mock failed")
        return {"id": f"wd_{account.address[-4:]}"}

    def get_spot_balance(self, _account: app.AccountEntry, _coin: str):
        return Decimal("12.34"), Decimal("0")


def make_import_stub(one_to_many: bool):
    w = app.WithdrawApp.__new__(app.WithdrawApp)
    w.store = app.AccountStore(Path(tempfile.gettempdir()) / "noop_accounts.json")
    w.checked_api_keys = set()
    w.checked_one_to_many_addresses = set()
    w.log_messages: list[str] = []
    w.log = lambda m: w.log_messages.append(m)
    w._refresh_tree = lambda: None
    w.start_refresh_network_options = lambda: None
    w._is_one_to_many_mode = lambda: one_to_many
    return w


def make_validate_stub(
    *,
    amount_mode: str,
    coin: str = "USDT",
    network: str = "BSC",
    amount: str = "1",
    random_min: str = "0.01",
    random_max: str = "0.09",
    delay: float = 1.0,
    threads: str = "2",
):
    w = app.WithdrawApp.__new__(app.WithdrawApp)
    w.coin_var = DummyVar(coin)
    w.network_var = DummyVar(network)
    w.amount_mode_var = DummyVar(amount_mode)
    w.amount_var = DummyVar(amount)
    w.random_min_var = DummyVar(random_min)
    w.random_max_var = DummyVar(random_max)
    w.delay_var = DummyVar(delay)
    w.threads_var = DummyVar(threads)
    return w


def test_store_roundtrip():
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "accounts.json"
        store = app.AccountStore(p)
        store.accounts = [app.AccountEntry("k1", "s1", "a1")]
        store.one_to_many_addresses = ["addr1", "addr2"]
        store.one_to_many_source_api_key = "src_k"
        store.one_to_many_source_api_secret = "src_s"
        store.settings = app.GlobalSettings(
            coin="USDT",
            network="BSC",
            amount="1.23",
            delay_seconds=1.0,
            worker_threads=2,
            mode=app.WithdrawApp.MODE_1M,
            random_amount_enabled=True,
            random_amount_min="0.01",
            random_amount_max="0.09",
            dry_run=False,
        )
        store.save()

        loaded = app.AccountStore(p)
        loaded.load()
        assert len(loaded.accounts) == 1
        assert loaded.accounts[0].address == "a1"
        assert loaded.one_to_many_addresses == ["addr1", "addr2"]
        assert loaded.settings.network == "BSC"
        assert loaded.settings.random_amount_enabled is True


def test_store_encrypts_sensitive_fields():
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "accounts.json"
        store = app.AccountStore(p)
        store.accounts = [app.AccountEntry("plain_api_key_123", "plain_api_secret_456", "addr1")]
        store.one_to_many_source_api_key = "src_key_plain_abc"
        store.one_to_many_source_api_secret = "src_secret_plain_def"
        store.save()

        raw = p.read_text(encoding="utf-8")
        assert "enc::v1::" in raw
        assert "plain_api_key_123" not in raw
        assert "plain_api_secret_456" not in raw
        assert "src_key_plain_abc" not in raw
        assert "src_secret_plain_def" not in raw

        loaded = app.AccountStore(p)
        loaded.load()
        assert loaded.accounts[0].api_key == "plain_api_key_123"
        assert loaded.accounts[0].api_secret == "plain_api_secret_456"
        assert loaded.one_to_many_source_api_key == "src_key_plain_abc"
        assert loaded.one_to_many_source_api_secret == "src_secret_plain_def"


def test_append_log_row_keeps_500_rows():
    tree = FakeLogTree()
    for i in range(505):
        app.append_log_row(tree, f"log-{i}", max_rows=500)
    assert len(tree.get_children()) == 500
    assert tree.rows[0][1][1] == "log-5"
    assert tree.rows[-1][1][1] == "log-504"


def test_store_old_list_compat():
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "accounts.json"
        p.write_text(
            json.dumps([{"api_key": "ak", "api_secret": "as", "address": "addr"}], ensure_ascii=False),
            encoding="utf-8",
        )
        loaded = app.AccountStore(p)
        loaded.load()
        assert len(loaded.accounts) == 1
        assert loaded.accounts[0].api_key == "ak"


def test_bg_store_roundtrip():
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "bg.json"
        store = app.BgOneToManyStore(p)
        store.addresses = ["a1", "a2", "a1"]
        store.settings = app.BgOneToManySettings(
            coin="USDT",
            network="trc20",
            amount_mode="随机数",
            amount="",
            random_min="0.01",
            random_max="0.05",
            delay_seconds=1.0,
            worker_threads=2,
            dry_run=True,
            api_key="k",
            api_secret="s",
            passphrase="p",
        )
        store.save()
        loaded = app.BgOneToManyStore(p)
        loaded.load()
        assert loaded.addresses == ["a1", "a2"]
        assert loaded.settings.network == "trc20"
        assert loaded.settings.passphrase == "p"


def test_bg_store_encrypts_sensitive_fields():
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "bg.json"
        store = app.BgOneToManyStore(p)
        store.addresses = ["a1"]
        store.settings = app.BgOneToManySettings(
            api_key="bitget_key_plain",
            api_secret="bitget_secret_plain",
            passphrase="bitget_passphrase_plain",
        )
        store.save()

        raw = p.read_text(encoding="utf-8")
        assert "enc::v1::" in raw
        assert "bitget_key_plain" not in raw
        assert "bitget_secret_plain" not in raw
        assert "bitget_passphrase_plain" not in raw

        loaded = app.BgOneToManyStore(p)
        loaded.load()
        assert loaded.settings.api_key == "bitget_key_plain"
        assert loaded.settings.api_secret == "bitget_secret_plain"
        assert loaded.settings.passphrase == "bitget_passphrase_plain"


def test_import_auto_select_accounts():
    w = make_import_stub(one_to_many=False)
    rows = [
        app.AccountEntry("k1", "s1", "addr1"),
        app.AccountEntry("k2", "s2", "addr2"),
    ]
    w._import_rows(rows, "测试")
    assert len(w.store.accounts) == 2
    assert w.checked_api_keys == {"k1", "k2"}
    assert any("已自动全选 2 个账号" in x for x in w.log_messages)


def test_import_auto_select_addresses():
    w = make_import_stub(one_to_many=True)
    rows = ["a1", "a2", "a1"]
    w._import_rows(rows, "测试")
    assert w.store.one_to_many_addresses == ["a1", "a2"]
    assert w.checked_one_to_many_addresses == {"a1", "a2"}
    assert any("已自动全选 2 个地址" in x for x in w.log_messages)


def test_delete_selected_falls_back_to_checked_accounts():
    w = app.WithdrawApp.__new__(app.WithdrawApp)
    w.store = app.AccountStore(Path(tempfile.gettempdir()) / "noop_delete_accounts.json")
    w.store.accounts = [
        app.AccountEntry("k1", "s1", "a1"),
        app.AccountEntry("k2", "s2", "a2"),
    ]
    w.checked_api_keys = {"k2"}
    w._selected_indices = lambda: []
    w._checked_indices = lambda: [i for i, acc in enumerate(w.store.accounts) if acc.api_key in w.checked_api_keys]
    w._is_one_to_many_mode = lambda: False
    w.refreshed = False
    w.network_refreshed = False
    w._refresh_tree = lambda: setattr(w, "refreshed", True)
    w.start_refresh_network_options = lambda: setattr(w, "network_refreshed", True)
    w.log_messages = []
    w.log = lambda m: w.log_messages.append(m)

    with MessagePatch() as mp:
        w.delete_selected()

    assert not mp.warnings
    assert len(w.store.accounts) == 1
    assert w.store.accounts[0].api_key == "k1"
    assert w.refreshed is True
    assert w.network_refreshed is True


def test_delete_selected_falls_back_to_checked_addresses():
    w = app.WithdrawApp.__new__(app.WithdrawApp)
    w.store = app.AccountStore(Path(tempfile.gettempdir()) / "noop_delete_addresses.json")
    w.store.one_to_many_addresses = ["a1", "a2", "a3"]
    w.checked_one_to_many_addresses = {"a2", "a3"}
    w._selected_indices = lambda: []
    w._checked_one_to_many_addresses = lambda: [
        addr for addr in w.store.one_to_many_addresses if addr in w.checked_one_to_many_addresses
    ]
    w._is_one_to_many_mode = lambda: True
    w.refreshed = False
    w._refresh_tree = lambda: setattr(w, "refreshed", True)
    w.log_messages = []
    w.log = lambda m: w.log_messages.append(m)

    with MessagePatch() as mp:
        w.delete_selected()

    assert not mp.warnings
    assert w.store.one_to_many_addresses == ["a1"]
    assert w.refreshed is True


def test_delete_selected_ignores_mouse_selection_when_not_checked():
    w = app.WithdrawApp.__new__(app.WithdrawApp)
    w.store = app.AccountStore(Path(tempfile.gettempdir()) / "noop_delete_selected_only.json")
    w.store.accounts = [
        app.AccountEntry("k1", "s1", "a1"),
        app.AccountEntry("k2", "s2", "a2"),
    ]
    w.checked_api_keys = set()
    w._selected_indices = lambda: [1]
    w._checked_indices = lambda: []
    w._is_one_to_many_mode = lambda: False
    w.refreshed = False
    w.network_refreshed = False
    w._refresh_tree = lambda: setattr(w, "refreshed", True)
    w.start_refresh_network_options = lambda: setattr(w, "network_refreshed", True)
    w.log_messages = []
    w.log = lambda m: w.log_messages.append(m)

    with MessagePatch() as mp:
        w.delete_selected()

    assert mp.warnings and "请先勾选要删除的账号" in mp.warnings[-1][1]
    assert len(w.store.accounts) == 2
    assert w.refreshed is False
    assert w.network_refreshed is False


def test_tree_right_click_selects_row_and_shows_menu():
    w = app.WithdrawApp.__new__(app.WithdrawApp)
    w.tree = FakeToggleTree(row_id="row_1", column="#3", values=["", 1, "k1", "s1", "a1", "-", "-"])
    w.row_menu = FakePopupMenu()
    event = DummyClickEvent(x=5, y=5, x_root=120, y_root=88)

    ret = w._on_tree_right_click(event)

    assert ret == "break"
    assert w.tree.selection() == ("row_1",)
    assert w.row_menu.popped is True
    assert w.row_menu.released is True
    assert (w.row_menu.last_x, w.row_menu.last_y) == (120, 88)


def test_query_balance_current_row_uses_selected_row_only():
    w = app.WithdrawApp.__new__(app.WithdrawApp)
    w.is_running = False
    w.coin_var = DummyVar("USDT")
    w._is_one_to_many_mode = lambda: False
    w.store = type(
        "Store",
        (),
        {
            "accounts": [
                app.AccountEntry("k1", "s1", "a1"),
                app.AccountEntry("k2", "s2", "a2"),
            ],
            "one_to_many_addresses": [],
        },
    )()
    w._selected_indices = lambda: [1]
    seen = {}
    w._run_query_balance = lambda accounts, coin: seen.update({"accounts": accounts, "coin": coin})

    with MessagePatch() as mp, ThreadPatch():
        w.start_query_balance_current_row()

    assert not mp.warnings
    assert seen["coin"] == "USDT"
    assert len(seen["accounts"]) == 1
    assert seen["accounts"][0].api_key == "k2"


def test_withdraw_current_row_uses_selected_row_only():
    w = app.WithdrawApp.__new__(app.WithdrawApp)
    w.is_running = False
    w.dry_run_var = DummyVar(True)
    w._is_one_to_many_mode = lambda: False
    w._is_amount_all = lambda _v: False
    w._validate_withdraw_params = lambda: app.WithdrawRuntimeParams(
        coin="USDT",
        amount="1",
        network="BSC",
        delay=0.0,
        threads=1,
    )
    w.store = type(
        "Store",
        (),
        {
            "accounts": [
                app.AccountEntry("k1", "s1", "a1"),
                app.AccountEntry("k2", "s2", "a2"),
            ],
            "one_to_many_addresses": [],
        },
    )()
    w._selected_indices = lambda: [0]
    seen = {}
    w._run_withdraw = lambda accounts, params, dry_run, one_to_many: seen.update(
        {"accounts": accounts, "dry_run": dry_run, "one_to_many": one_to_many}
    )

    with MessagePatch() as mp, ThreadPatch():
        w.start_withdraw_current_row()

    assert not mp.warnings
    assert len(seen["accounts"]) == 1
    assert seen["accounts"][0].api_key == "k1"
    assert seen["dry_run"] is True
    assert seen["one_to_many"] is False


def test_delete_current_row_uses_selected_row_only():
    w = app.WithdrawApp.__new__(app.WithdrawApp)
    w.store = app.AccountStore(Path(tempfile.gettempdir()) / "noop_delete_current_row.json")
    w.store.accounts = [
        app.AccountEntry("k1", "s1", "a1"),
        app.AccountEntry("k2", "s2", "a2"),
    ]
    w._selected_indices = lambda: [1]
    w._is_one_to_many_mode = lambda: False
    w.refreshed = False
    w.network_refreshed = False
    w._refresh_tree = lambda: setattr(w, "refreshed", True)
    w.start_refresh_network_options = lambda: setattr(w, "network_refreshed", True)
    w.log_messages = []
    w.log = lambda m: w.log_messages.append(m)

    with MessagePatch() as mp:
        w.delete_current_row()

    assert not mp.warnings
    assert len(w.store.accounts) == 1
    assert w.store.accounts[0].api_key == "k1"
    assert w.refreshed is True
    assert w.network_refreshed is True


def test_bitget_tree_right_click_selects_row_and_shows_menu():
    w = app.BitgetOneToManyPage.__new__(app.BitgetOneToManyPage)
    w.tree = FakeToggleTree(row_id="bg_row_1", column="#3", values=["", 1, "addr_1", "-", "-"])
    w.row_menu = FakePopupMenu()
    event = DummyClickEvent(x=5, y=5, x_root=200, y_root=66)

    ret = w._on_tree_right_click(event)

    assert ret == "break"
    assert w.tree.selection() == ("bg_row_1",)
    assert w.row_menu.popped is True
    assert w.row_menu.released is True
    assert (w.row_menu.last_x, w.row_menu.last_y) == (200, 66)


def test_bitget_current_row_ops_use_selected_row_only():
    w = app.BitgetOneToManyPage.__new__(app.BitgetOneToManyPage)
    w.is_running = False
    w.store = type("Store", (), {"addresses": ["addr_1", "addr_2"]})()
    w._selected_indices = lambda: [1]
    w.dry_run_var = DummyVar(True)
    w._validate_withdraw_params = lambda: app.WithdrawRuntimeParams(
        coin="USDT",
        amount="1",
        network="trc20",
        delay=0.0,
        threads=1,
    )
    w._source_cred = lambda with_message=False: ("k", "s", "p")
    w.log_messages = []
    w.log = lambda m: w.log_messages.append(m)
    seen = {}
    w._run_query_balance = lambda cred: (seen.update({"query_cred": cred}), setattr(w, "is_running", False))
    w._run_withdraw = lambda addrs, params, cred, dry_run: seen.update(
        {"addrs": addrs, "params": params, "cred": cred, "dry_run": dry_run}
    )

    with MessagePatch() as mp, ThreadPatch():
        w.start_query_balance_current_row()
        assert not mp.warnings
        assert seen["query_cred"] == ("k", "s", "p")
        w.start_withdraw_current_row()

    assert not mp.warnings
    assert seen["addrs"] == ["addr_2"]
    assert seen["dry_run"] is True


def test_bitget_delete_current_row_uses_selected_row_only():
    w = app.BitgetOneToManyPage.__new__(app.BitgetOneToManyPage)
    w.store = type("Store", (), {})()
    w.store.addresses = ["addr_1", "addr_2", "addr_3"]
    w.store.delete_by_indices = lambda idxs: setattr(
        w.store, "addresses", [a for i, a in enumerate(w.store.addresses) if i not in set(idxs)]
    )
    w._selected_indices = lambda: [1]
    w.refreshed = False
    w._refresh_tree = lambda: setattr(w, "refreshed", True)
    w.log_messages = []
    w.log = lambda m: w.log_messages.append(m)

    with MessagePatch() as mp:
        w.delete_current_row()

    assert not mp.warnings
    assert w.store.addresses == ["addr_1", "addr_3"]
    assert w.refreshed is True


def test_onchain_tree_right_click_selects_row_and_shows_menu():
    w = app.OnchainTransferPage.__new__(app.OnchainTransferPage)
    w.tree = FakeToggleTree(row_id="oc_row_1", column="#4", values=["", 1, "src", "src_addr", "dst", "-", "-"])
    w.row_menu = FakePopupMenu()
    event = DummyClickEvent(x=2, y=2, x_root=333, y_root=121)

    ret = w._on_tree_right_click(event)

    assert ret == "break"
    assert w.tree.selection() == ("oc_row_1",)
    assert w.row_menu.popped is True
    assert w.row_menu.released is True
    assert (w.row_menu.last_x, w.row_menu.last_y) == (333, 121)


def test_onchain_current_row_ops_use_selected_row_only():
    class DummyClient:
        @staticmethod
        def _ensure_eth_account():
            return None

        @staticmethod
        def is_address(_v):
            return True

    w = app.OnchainTransferPage.__new__(app.OnchainTransferPage)
    w.is_running = False
    w.client = DummyClient()
    w.network_var = DummyVar("ETH")
    w.dry_run_var = DummyVar(True)
    w._selected_token = lambda with_message=False: page_onchain.EvmToken(symbol="ETH", contract="", decimals=18, is_native=True)
    w._single_row_job = lambda: ("m2m:key", "source_1", "target_1")
    w._mode = lambda: app.OnchainTransferPage.MODE_M2M
    w._validate_transfer_params = lambda: app.WithdrawRuntimeParams(
        coin="ETH",
        amount="0.01",
        network="ETH",
        delay=0.0,
        threads=1,
        token_contract="",
        token_decimals=18,
        token_is_native=True,
    )
    w._token_desc_from_params = lambda _p: "ETH"
    w.log_messages = []
    w.log = lambda m: w.log_messages.append(m)
    seen = {}
    w._run_query_balance_for_sources = lambda network, token, sources: (
        seen.update({"q_network": network, "q_sources": sources}),
        setattr(w, "is_running", False),
    )
    w._run_batch_transfer = lambda jobs, params, dry_run: seen.update(
        {"jobs": jobs, "params": params, "dry_run": dry_run}
    )

    with MessagePatch() as mp, ThreadPatch():
        w.start_query_balance_current_row()
        assert not mp.warnings
        assert seen["q_network"] == "ETH"
        assert seen["q_sources"] == ["source_1"]
        w.start_transfer_current_row()

    assert not mp.warnings
    assert seen["jobs"] == [("m2m:key", "source_1", "target_1")]
    assert seen["dry_run"] is True


def test_onchain_delete_current_row_uses_selected_row_only():
    w = app.OnchainTransferPage.__new__(app.OnchainTransferPage)
    w.store = type("Store", (), {})()
    w.store.multi_to_multi_pairs = [app.OnchainPairEntry(source="s1", target="t1")]
    w.store.one_to_many_addresses = []
    w.store.many_to_one_sources = []
    w.store.delete_multi_to_multi_by_indices = lambda idxs: setattr(
        w.store,
        "multi_to_multi_pairs",
        [x for i, x in enumerate(w.store.multi_to_multi_pairs) if i not in set(idxs)],
    )
    w._selected_indices = lambda: [0]
    w._mode = lambda: app.OnchainTransferPage.MODE_M2M
    w.refreshed = False
    w._refresh_tree = lambda: setattr(w, "refreshed", True)
    w.log_messages = []
    w.log = lambda m: w.log_messages.append(m)

    with MessagePatch() as mp:
        w.delete_current_row()

    assert not mp.warnings
    assert w.store.multi_to_multi_pairs == []
    assert w.refreshed is True


def test_bitget_delete_selected_uses_checked_rows_only():
    w = app.BitgetOneToManyPage.__new__(app.BitgetOneToManyPage)
    w.store = type("Store", (), {})()
    w.store.addresses = ["addr_1", "addr_2", "addr_3"]
    w.store.delete_by_indices = lambda idxs: setattr(
        w.store, "addresses", [a for i, a in enumerate(w.store.addresses) if i not in set(idxs)]
    )
    w.checked_addresses = {"addr_2"}
    w._selected_indices = lambda: [0]
    w.refreshed = False
    w._refresh_tree = lambda: setattr(w, "refreshed", True)
    w.log_messages = []
    w.log = lambda m: w.log_messages.append(m)

    with MessagePatch() as mp:
        w.delete_selected()

    assert not mp.warnings
    assert w.store.addresses == ["addr_1", "addr_3"]
    assert w.refreshed is True


def test_onchain_delete_selected_uses_checked_rows_only():
    w = app.OnchainTransferPage.__new__(app.OnchainTransferPage)
    w.store = type("Store", (), {})()
    w.store.multi_to_multi_pairs = [
        app.OnchainPairEntry(source="s1", target="t1"),
        app.OnchainPairEntry(source="s2", target="t2"),
    ]
    w.store.one_to_many_addresses = []
    w.store.many_to_one_sources = []
    w.store.delete_multi_to_multi_by_indices = lambda idxs: setattr(
        w.store,
        "multi_to_multi_pairs",
        [x for i, x in enumerate(w.store.multi_to_multi_pairs) if i not in set(idxs)],
    )
    w.store.delete_one_to_many_by_indices = lambda idxs: setattr(
        w.store, "one_to_many_addresses", [x for i, x in enumerate(w.store.one_to_many_addresses) if i not in set(idxs)]
    )
    w.store.delete_many_to_one_by_indices = lambda idxs: setattr(
        w.store, "many_to_one_sources", [x for i, x in enumerate(w.store.many_to_one_sources) if i not in set(idxs)]
    )
    w.checked_row_keys = {app.OnchainTransferPage._m2m_key(w.store.multi_to_multi_pairs[1])}
    w._selected_indices = lambda: [0]
    w._mode = lambda: app.OnchainTransferPage.MODE_M2M
    w.refreshed = False
    w._refresh_tree = lambda: setattr(w, "refreshed", True)
    w.log_messages = []
    w.log = lambda m: w.log_messages.append(m)

    with MessagePatch() as mp:
        w.delete_selected()

    assert not mp.warnings
    assert len(w.store.multi_to_multi_pairs) == 1
    assert w.store.multi_to_multi_pairs[0].source == "s1"
    assert w.refreshed is True


def make_onchain_query_start_stub(mode: str):
    class DummyClient:
        @staticmethod
        def _ensure_eth_account():
            return None

        @staticmethod
        def is_address(_v):
            return True

    w = app.OnchainTransferPage.__new__(app.OnchainTransferPage)
    w.is_running = False
    w.client = DummyClient()
    w.network_var = DummyVar("ETH")
    w.source_credential_var = DummyVar("source_seed")
    w.target_address_var = DummyVar("0x1111111111111111111111111111111111111111")
    w.checked_row_keys = set()
    w._mode = lambda: mode
    w._selected_token = lambda with_message=False: page_onchain.EvmToken(symbol="ETH", contract="", decimals=18, is_native=True)
    w.store = type(
        "Store",
        (),
        {
            "multi_to_multi_pairs": [],
            "one_to_many_addresses": [],
            "many_to_one_sources": [],
        },
    )()
    w.log_messages = []
    w.log = lambda m: w.log_messages.append(m)
    return w


def test_onchain_query_balance_requires_checked_rows_in_m2m():
    w = make_onchain_query_start_stub(app.OnchainTransferPage.MODE_M2M)
    w.store.multi_to_multi_pairs = [app.OnchainPairEntry(source="s1", target="t1")]
    seen = {"called": False}
    w._run_query_balance_for_sources = lambda network, token, sources: seen.update({"called": True})

    with MessagePatch() as mp, ThreadPatch():
        w.start_query_balance()

    assert mp.warnings and "请先勾选至少一条查询数据" in mp.warnings[-1][1]
    assert seen["called"] is False


def test_onchain_query_balance_uses_checked_rows_in_m2m():
    w = make_onchain_query_start_stub(app.OnchainTransferPage.MODE_M2M)
    w.store.multi_to_multi_pairs = [
        app.OnchainPairEntry(source="s1", target="t1"),
        app.OnchainPairEntry(source="s2", target="t2"),
    ]
    w.checked_row_keys = {app.OnchainTransferPage._m2m_key(w.store.multi_to_multi_pairs[1])}
    seen = {}
    w._run_query_balance_for_sources = lambda network, token, sources: seen.update(
        {"network": network, "token": token.symbol, "sources": sources}
    )

    with MessagePatch() as mp, ThreadPatch():
        w.start_query_balance()

    assert not mp.warnings
    assert seen["network"] == "ETH"
    assert seen["token"] == "ETH"
    assert seen["sources"] == ["s2"]


def test_onchain_query_balance_requires_checked_rows_in_1m():
    w = make_onchain_query_start_stub(app.OnchainTransferPage.MODE_1M)
    w.store.one_to_many_addresses = [
        "0x1111111111111111111111111111111111111111",
        "0x2222222222222222222222222222222222222222",
    ]
    seen = {"called": False}
    w._run_query_balance_one_to_many = lambda network, token, source, targets: seen.update({"called": True})

    with MessagePatch() as mp, ThreadPatch():
        w.start_query_balance()

    assert mp.warnings and "请先勾选至少一条查询数据" in mp.warnings[-1][1]
    assert seen["called"] is False


def test_onchain_query_balance_requires_checked_rows_in_m1():
    w = make_onchain_query_start_stub(app.OnchainTransferPage.MODE_M1)
    w.store.many_to_one_sources = ["seed_1", "seed_2"]
    seen = {"called": False}
    w._run_query_balance_many_to_one = lambda network, token, target, sources: seen.update({"called": True})

    with MessagePatch() as mp, ThreadPatch():
        w.start_query_balance()

    assert mp.warnings and "请先勾选至少一条查询数据" in mp.warnings[-1][1]
    assert seen["called"] is False


def test_main_tree_click_toggles_checked_on_any_column():
    w = app.WithdrawApp.__new__(app.WithdrawApp)
    w.store = type("Store", (), {"accounts": [app.AccountEntry("k1", "s1", "a1")], "one_to_many_addresses": []})()
    w.row_index_map = {"row_1": 0}
    w.checked_api_keys = set()
    w.checked_one_to_many_addresses = set()
    w._is_one_to_many_mode = lambda: False
    w.tree = FakeToggleTree(row_id="row_1", column="#4", values=["", 1, "k1", "s1", "a1", "-", "-"])
    event = DummyClickEvent()

    ret1 = w._on_tree_click(event)
    assert ret1 is None
    assert w.checked_api_keys == {"k1"}
    assert w.tree.item("row_1", "values")[0] == "✓"

    ret2 = w._on_tree_click(event)
    assert ret2 is None
    assert w.checked_api_keys == set()
    assert w.tree.item("row_1", "values")[0] == ""


def test_main_tree_click_toggles_one_to_many_checked_on_any_column():
    w = app.WithdrawApp.__new__(app.WithdrawApp)
    w.store = type("Store", (), {"accounts": [], "one_to_many_addresses": ["addr_1"]})()
    w.row_index_map = {"row_1": 0}
    w.checked_api_keys = set()
    w.checked_one_to_many_addresses = set()
    w._is_one_to_many_mode = lambda: True
    w.tree = FakeToggleTree(row_id="row_1", column="#5", values=["", 1, "-", "-", "addr_1", "-", "-"])
    event = DummyClickEvent()

    ret1 = w._on_tree_click(event)
    assert ret1 is None
    assert w.checked_one_to_many_addresses == {"addr_1"}
    assert w.tree.item("row_1", "values")[0] == "✓"

    ret2 = w._on_tree_click(event)
    assert ret2 is None
    assert w.checked_one_to_many_addresses == set()
    assert w.tree.item("row_1", "values")[0] == ""


def test_bitget_tree_click_toggles_checked_on_any_column():
    w = app.BitgetOneToManyPage.__new__(app.BitgetOneToManyPage)
    w.store = type("Store", (), {"addresses": ["addr_1"]})()
    w.row_index_map = {"bg_row_1": 0}
    w.checked_addresses = set()
    w.tree = FakeToggleTree(row_id="bg_row_1", column="#3", values=["", 1, "addr_1", "-", "-"])
    event = DummyClickEvent()

    ret1 = w._on_tree_click(event)
    assert ret1 is None
    assert w.checked_addresses == {"addr_1"}
    assert w.tree.item("bg_row_1", "values")[0] == "✓"

    ret2 = w._on_tree_click(event)
    assert ret2 is None
    assert w.checked_addresses == set()
    assert w.tree.item("bg_row_1", "values")[0] == ""


def test_onchain_tree_click_toggles_checked_on_any_column():
    w = app.OnchainTransferPage.__new__(app.OnchainTransferPage)
    w.row_key_by_row_id = {"oc_row_1": "pair_1"}
    w.checked_row_keys = set()
    w.tree = FakeToggleTree(row_id="oc_row_1", column="#6", values=["", 1, "src", "src_addr", "target", "-", "-"])
    event = DummyClickEvent()

    ret1 = w._on_tree_click(event)
    assert ret1 is None
    assert w.checked_row_keys == {"pair_1"}
    assert w.tree.item("oc_row_1", "values")[0] == "✓"

    ret2 = w._on_tree_click(event)
    assert ret2 is None
    assert w.checked_row_keys == set()
    assert w.tree.item("oc_row_1", "values")[0] == ""


def test_validate_requires_network():
    w = make_validate_stub(amount_mode=app.WithdrawApp.AMOUNT_MODE_FIXED, network="")
    with MessagePatch() as mp:
        params = w._validate_withdraw_params()
    assert params is None
    assert mp.errors and "未设置提现网络，无法提现" in mp.errors[-1][1]


def test_validate_random_mode():
    w = make_validate_stub(
        amount_mode=app.WithdrawApp.AMOUNT_MODE_RANDOM,
        network="BSC",
        amount="",
        random_min="0.01",
        random_max="0.05",
    )
    with MessagePatch():
        params = w._validate_withdraw_params()
    assert params is not None
    assert params.random_enabled is True
    assert params.random_min == Decimal("0.01")
    assert params.random_max == Decimal("0.05")


def test_validate_all_mode():
    w = make_validate_stub(amount_mode=app.WithdrawApp.AMOUNT_MODE_ALL, network="ETH", amount="")
    with MessagePatch():
        params = w._validate_withdraw_params()
    assert params is not None
    assert params.amount == app.WithdrawApp.AMOUNT_ALL_LABEL
    assert params.random_enabled is False


def make_bg_validate_stub(
    *,
    amount_mode: str,
    coin: str = "USDT",
    network: str = "trc20",
    amount: str = "1",
    random_min: str = "0.01",
    random_max: str = "0.09",
    delay: float = 1.0,
    threads: str = "2",
):
    w = app.BitgetOneToManyPage.__new__(app.BitgetOneToManyPage)
    w.coin_var = DummyVar(coin)
    w.network_var = DummyVar(network)
    w.amount_mode_var = DummyVar(amount_mode)
    w.amount_var = DummyVar(amount)
    w.random_min_var = DummyVar(random_min)
    w.random_max_var = DummyVar(random_max)
    w.delay_var = DummyVar(delay)
    w.threads_var = DummyVar(threads)
    return w


class DummyEvmClient:
    def get_symbol(self, network: str) -> str:
        return {"ETH": "ETH", "BSC": "BNB"}.get(network, "")


def make_onchain_validate_stub(
    *,
    amount_mode: str,
    network: str = "ETH",
    amount: str = "0.5",
    random_min: str = "0.01",
    random_max: str = "0.09",
    delay: float = 1.0,
    threads: str = "2",
):
    w = app.OnchainTransferPage.__new__(app.OnchainTransferPage)
    w.client = DummyEvmClient()
    w.network_var = DummyVar(network)
    w.amount_mode_var = DummyVar(amount_mode)
    w.amount_var = DummyVar(amount)
    w.random_min_var = DummyVar(random_min)
    w.random_max_var = DummyVar(random_max)
    w.delay_var = DummyVar(delay)
    w.threads_var = DummyVar(threads)
    return w


def test_onchain_store_roundtrip():
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "onchain.json"
        store = app.OnchainStore(p)
        store.multi_to_multi_pairs = [
            app.OnchainPairEntry(source="pk_a", target="0x1111111111111111111111111111111111111111"),
            app.OnchainPairEntry(source="pk_b", target="0x2222222222222222222222222222222222222222"),
        ]
        store.one_to_many_addresses = ["0x3333333333333333333333333333333333333333"]
        store.many_to_one_sources = ["pk_x", "pk_y"]
        store.settings = app.OnchainSettings(
            mode=app.OnchainTransferPage.MODE_M1,
            network="BSC",
            token_symbol="USDT",
            token_contract="0x55d398326f99059ff775485246999027b3197955",
            amount_mode=app.OnchainTransferPage.AMOUNT_MODE_RANDOM,
            amount="",
            random_min="0.01",
            random_max="0.05",
            delay_seconds=1.0,
            worker_threads=2,
            dry_run=True,
            one_to_many_source="pk_src",
            many_to_one_target="0x9999999999999999999999999999999999999999",
        )
        store.save()

        loaded = app.OnchainStore(p)
        loaded.load()
        assert len(loaded.multi_to_multi_pairs) == 2
        assert loaded.one_to_many_addresses == ["0x3333333333333333333333333333333333333333"]
        assert loaded.many_to_one_sources == ["pk_x", "pk_y"]
        assert loaded.settings.mode == app.OnchainTransferPage.MODE_M1
        assert loaded.settings.network == "BSC"
        assert loaded.settings.token_symbol == "USDT"
        assert loaded.settings.token_contract.lower() == "0x55d398326f99059ff775485246999027b3197955"


def test_onchain_store_encrypts_sensitive_fields():
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "onchain.json"
        store = app.OnchainStore(p)
        store.multi_to_multi_pairs = [app.OnchainPairEntry(source="source_secret_1", target="0x1111111111111111111111111111111111111111")]
        store.many_to_one_sources = ["source_secret_2"]
        store.settings.one_to_many_source = "source_secret_3"
        store.settings.many_to_one_target = "0x9999999999999999999999999999999999999999"
        store.save()

        raw = p.read_text(encoding="utf-8")
        assert "enc::v1::" in raw
        assert "source_secret_1" not in raw
        assert "source_secret_2" not in raw
        assert "source_secret_3" not in raw

        loaded = app.OnchainStore(p)
        loaded.load()
        assert loaded.multi_to_multi_pairs[0].source == "source_secret_1"
        assert loaded.many_to_one_sources == ["source_secret_2"]
        assert loaded.settings.one_to_many_source == "source_secret_3"


def test_bg_validate_requires_network():
    w = make_bg_validate_stub(amount_mode=app.BitgetOneToManyPage.AMOUNT_MODE_FIXED, network="")
    with MessagePatch() as mp:
        params = w._validate_withdraw_params()
    assert params is None
    assert mp.errors and "未设置提现网络，无法提现" in mp.errors[-1][1]


def test_onchain_validate_requires_network():
    w = make_onchain_validate_stub(amount_mode=app.OnchainTransferPage.AMOUNT_MODE_FIXED, network="")
    with MessagePatch() as mp:
        params = w._validate_transfer_params()
    assert params is None
    assert mp.errors and "未设置网络，无法转账" in mp.errors[-1][1]


def test_onchain_validate_random_mode():
    w = make_onchain_validate_stub(
        amount_mode=app.OnchainTransferPage.AMOUNT_MODE_RANDOM,
        network="ETH",
        amount="",
        random_min="0.01",
        random_max="0.05",
    )
    with MessagePatch():
        params = w._validate_transfer_params()
    assert params is not None
    assert params.coin == "ETH"
    assert params.random_enabled is True
    assert params.random_min == Decimal("0.01")
    assert params.random_max == Decimal("0.05")


def test_random_amount_two_decimals():
    w = app.WithdrawApp.__new__(app.WithdrawApp)
    params = app.WithdrawRuntimeParams(
        coin="USDT",
        amount="0",
        network="BSC",
        delay=0.0,
        threads=1,
        random_enabled=True,
        random_min=Decimal("0.01"),
        random_max=Decimal("0.05"),
    )
    dummy_account = app.AccountEntry("k", "s", "addr")
    for _ in range(40):
        amount_text = w._resolve_withdraw_amount(dummy_account, params)
        assert re.fullmatch(r"\d+\.\d{2}", amount_text), amount_text
        val = Decimal(amount_text)
        assert Decimal("0.01") <= val <= Decimal("0.05")


def test_bg_random_amount_two_decimals():
    w = app.BitgetOneToManyPage.__new__(app.BitgetOneToManyPage)
    params = app.WithdrawRuntimeParams(
        coin="USDT",
        amount="0",
        network="trc20",
        delay=0.0,
        threads=1,
        random_enabled=True,
        random_min=Decimal("0.01"),
        random_max=Decimal("0.05"),
    )
    for _ in range(40):
        amount_text = w._resolve_amount(params, ("k", "s", "p"))
        assert re.fullmatch(r"\d+\.\d{2}", amount_text), amount_text
        val = Decimal(amount_text)
        assert Decimal("0.01") <= val <= Decimal("0.05")


def make_withdraw_stub():
    w = app.WithdrawApp.__new__(app.WithdrawApp)
    w.root = DummyRoot()
    w.client = FakeClient()
    w.row_id_by_api_key = {}
    w.row_index_map = {}
    w.account_withdraw_status = {}
    w.is_running = True
    w.log_messages: list[str] = []
    w.log = lambda m: w.log_messages.append(m)
    return w


def make_network_options_stub(*, current_coin: str = "USDT", current_api_key: str = "k_active", current_network: str = "BSC"):
    w = app.WithdrawApp.__new__(app.WithdrawApp)
    w.coin_network_cache = {}
    w.network_var = DummyVar(current_network)
    w.network_box = FakeComboBox()
    w._current_coin = lambda: current_coin
    w._pick_meta_account = lambda: app.AccountEntry(current_api_key, "s", "addr")
    return w


def test_network_options_ignores_stale_account_context():
    w = make_network_options_stub(current_coin="USDT", current_api_key="k_current", current_network="BSC")

    network_options.apply_network_options(
        w,
        cache_key="k_old|USDT",
        account_key="k_old",
        coin="USDT",
        networks=["ETH"],
        cache_result=True,
    )

    assert w.coin_network_cache["k_old|USDT"] == ["ETH"]
    assert w.network_box.values == []
    assert w.network_var.get() == "BSC"


def test_network_options_applies_when_account_and_coin_match():
    w = make_network_options_stub(current_coin="USDT", current_api_key="k_current", current_network="BSC")

    network_options.apply_network_options(
        w,
        cache_key="k_current|USDT",
        account_key="k_current",
        coin="USDT",
        networks=["eth", "BSC", "eth", ""],
        cache_result=True,
    )

    assert w.coin_network_cache["k_current|USDT"] == ["ETH", "BSC"]
    assert w.network_box.values == ["", "ETH", "BSC"]
    assert w.network_var.get() == "BSC"


def make_binance_balance_stub():
    w = app.WithdrawApp.__new__(app.WithdrawApp)
    w.is_running = False
    w.coin_var = DummyVar("USDT")
    w._is_one_to_many_mode = lambda: False
    w._checked_accounts = lambda: []
    w._selected_accounts = lambda: []
    w._build_source_account = lambda with_message=False: None
    w.store = type("Store", (), {"accounts": []})()
    return w


def make_binance_withdraw_start_stub():
    w = app.WithdrawApp.__new__(app.WithdrawApp)
    w.is_running = False
    w.dry_run_var = DummyVar(True)
    w._is_one_to_many_mode = lambda: False
    w._checked_accounts = lambda: []
    w._selected_accounts = lambda: []
    w._is_amount_all = lambda _v: False
    w._validate_withdraw_params = lambda: app.WithdrawRuntimeParams(
        coin="USDT",
        amount="1",
        network="BSC",
        delay=0.0,
        threads=1,
    )
    w.log_messages = []
    w.log = lambda m: w.log_messages.append(m)
    return w


def test_binance_query_balance_checked_only_accounts():
    w = make_binance_balance_stub()
    checked_accounts = [
        app.AccountEntry("k_checked_1", "s_checked_1", "a_checked_1"),
        app.AccountEntry("k_checked_2", "s_checked_2", "a_checked_2"),
    ]
    w._checked_accounts = lambda: checked_accounts
    w.store.accounts = [
        app.AccountEntry("k_store_1", "s_store_1", "a_store_1"),
        app.AccountEntry("k_store_2", "s_store_2", "a_store_2"),
    ]
    seen = {}
    w._run_query_balance = lambda accounts, coin: seen.update({"accounts": accounts, "coin": coin})

    with MessagePatch() as mp, ThreadPatch():
        w.start_query_balance()

    assert not mp.warnings
    assert seen["coin"] == "USDT"
    assert seen["accounts"] == checked_accounts


def test_binance_refresh_coin_checked_only_accounts():
    w = make_binance_balance_stub()
    checked_accounts = [
        app.AccountEntry("k_checked_1", "s_checked_1", "a_checked_1"),
        app.AccountEntry("k_checked_2", "s_checked_2", "a_checked_2"),
    ]
    w._checked_accounts = lambda: checked_accounts
    w.store.accounts = [app.AccountEntry("k1", "s1", "a1"), app.AccountEntry("k2", "s2", "a2")]
    seen = {}
    w._run_refresh_coin_options = lambda accounts: seen.update({"accounts": accounts})

    with MessagePatch() as mp, ThreadPatch():
        w.start_refresh_coin_options()

    assert not mp.warnings
    assert seen["accounts"] == checked_accounts


def test_binance_refresh_coin_requires_checked_accounts():
    w = make_binance_balance_stub()
    w.store.accounts = [app.AccountEntry("k1", "s1", "a1")]
    seen = {"called": False}
    w._run_refresh_coin_options = lambda _accounts: seen.update({"called": True})

    with MessagePatch() as mp, ThreadPatch():
        w.start_refresh_coin_options()

    assert mp.warnings and "请先勾选至少一个账号" in mp.warnings[-1][1]
    assert seen["called"] is False


def test_binance_query_balance_requires_checked_accounts():
    w = make_binance_balance_stub()
    seen = {"called": False}
    w._run_query_balance = lambda accounts, coin: seen.update({"called": True})

    with MessagePatch() as mp, ThreadPatch():
        w.start_query_balance()

    assert mp.warnings and "请先勾选至少一个账号" in mp.warnings[-1][1]
    assert seen["called"] is False


def test_binance_batch_withdraw_checked_only_accounts():
    w = make_binance_withdraw_start_stub()
    checked_accounts = [
        app.AccountEntry("k_chk_1", "s_chk_1", "addr_chk_1"),
        app.AccountEntry("k_chk_2", "s_chk_2", "addr_chk_2"),
    ]
    w._checked_accounts = lambda: checked_accounts
    seen = {}
    w._run_withdraw = lambda accounts, params, dry_run, one_to_many: seen.update(
        {"accounts": accounts, "params": params, "dry_run": dry_run, "one_to_many": one_to_many}
    )

    with MessagePatch() as mp, ThreadPatch():
        w.start_batch_withdraw()

    assert not mp.warnings
    assert seen["accounts"] == checked_accounts
    assert seen["dry_run"] is True
    assert seen["one_to_many"] is False


def test_binance_batch_withdraw_requires_checked_accounts():
    w = make_binance_withdraw_start_stub()
    seen = {"called": False}
    w._run_withdraw = lambda *_args, **_kwargs: seen.update(called=True)

    with MessagePatch() as mp, ThreadPatch():
        w.start_batch_withdraw()

    assert mp.warnings and "请先勾选至少一个账号" in mp.warnings[-1][1]
    assert seen["called"] is False


def test_run_withdraw_one_to_many_status():
    w = make_withdraw_stub()
    accounts = [
        app.AccountEntry("src", "sec", "good_addr_1"),
        app.AccountEntry("src", "sec", "bad_addr_2"),
        app.AccountEntry("src", "sec", "good_addr_3"),
    ]
    params = app.WithdrawRuntimeParams(
        coin="USDT",
        amount="1.00",
        network="BSC",
        delay=0.0,
        threads=2,
    )
    with MessagePatch() as mp:
        w._run_withdraw(accounts, params, dry_run=False, one_to_many=True)
    assert w.account_withdraw_status["good_addr_1"] == "success"
    assert w.account_withdraw_status["good_addr_3"] == "success"
    assert w.account_withdraw_status["bad_addr_2"] == "failed"
    assert any("提现任务结束：成功 2，失败 1" in x for x in w.log_messages)
    assert mp.infos


def test_run_withdraw_multi_to_multi_status():
    w = make_withdraw_stub()
    accounts = [
        app.AccountEntry("k1", "s1", "good_x"),
        app.AccountEntry("k2", "s2", "bad_y"),
    ]
    params = app.WithdrawRuntimeParams(
        coin="USDT",
        amount="2",
        network="ETH",
        delay=0.0,
        threads=2,
    )
    with MessagePatch() as mp:
        w._run_withdraw(accounts, params, dry_run=False, one_to_many=False)
    assert w.account_withdraw_status["k1"] == "success"
    assert w.account_withdraw_status["k2"] == "failed"
    assert any("提现任务结束：成功 1，失败 1" in x for x in w.log_messages)
    assert mp.infos


def test_binance_dry_run_all_does_not_call_balance_api():
    class FailClient:
        def withdraw(self, *_args, **_kwargs):
            raise AssertionError("dry_run 不应调用 withdraw")

        def get_spot_balance(self, *_args, **_kwargs):
            raise AssertionError("dry_run + 全部 不应查询真实余额")

    w = make_withdraw_stub()
    w.client = FailClient()
    accounts = [app.AccountEntry("k1", "s1", "good_x")]
    params = app.WithdrawRuntimeParams(
        coin="USDT",
        amount=app.WithdrawApp.AMOUNT_ALL_LABEL,
        network="BSC",
        delay=0.0,
        threads=1,
    )
    with MessagePatch():
        w._run_withdraw(accounts, params, dry_run=True, one_to_many=False)
    assert w.account_withdraw_status["k1"] == "success"
    assert any("模拟成功" in x for x in w.log_messages)


def test_bitget_dry_run_all_does_not_call_balance_api():
    class FailClient:
        def withdraw(self, *_args, **_kwargs):
            raise AssertionError("dry_run 不应调用 withdraw")

        def get_available_balance(self, *_args, **_kwargs):
            raise AssertionError("dry_run + 全部 不应查询真实余额")

    w = app.BitgetOneToManyPage.__new__(app.BitgetOneToManyPage)
    w.root = DummyRoot()
    w.client = FailClient()
    w.row_id_by_addr = {}
    w.row_index_map = {}
    w.address_status = {}
    w.is_running = True
    w.log_messages = []
    w.log = lambda m: w.log_messages.append(m)
    w._set_status = lambda addr, status: w.address_status.__setitem__(addr, status)

    params = app.WithdrawRuntimeParams(
        coin="USDT",
        amount=app.BitgetOneToManyPage.AMOUNT_ALL_LABEL,
        network="trc20",
        delay=0.0,
        threads=1,
    )
    with MessagePatch():
        w._run_withdraw(["addr_ok"], params, ("k", "s", "p"), dry_run=True)
    assert w.address_status["addr_ok"] == "success"
    assert any("模拟成功" in x for x in w.log_messages)


def test_onchain_dry_run_does_not_call_rpc():
    class FailClient:
        NATIVE_GAS_LIMIT = 21000

        def __getattr__(self, _name):
            raise AssertionError("dry_run 不应触发链上 RPC 或签名发送")

    w = app.OnchainTransferPage.__new__(app.OnchainTransferPage)
    w.root = DummyRoot()
    w.client = FailClient()
    w.row_status = {}
    w.row_id_by_key = {}
    w.row_index_map = {}
    w.is_running = True
    w.log_messages = []
    w.log = lambda m: w.log_messages.append(m)
    w._mode = lambda: app.OnchainTransferPage.MODE_M2M
    w._set_status = lambda row_key, status: w.row_status.__setitem__(row_key, status)

    jobs = [("row_1", "seed words secret", "0x1111111111111111111111111111111111111111")]
    params = app.WithdrawRuntimeParams(
        coin="ETH",
        amount=app.OnchainTransferPage.AMOUNT_ALL_LABEL,
        network="ETH",
        delay=0.0,
        threads=1,
        token_is_native=True,
    )
    with MessagePatch():
        w._run_batch_transfer(jobs, params, dry_run=True)
    assert w.row_status["row_1"] == "success"
    assert any("模拟成功" in x for x in w.log_messages)


def test_start_detect_ip_single_inflight():
    w = app.WithdrawApp.__new__(app.WithdrawApp)
    w.is_running = False
    w.ip_var = DummyVar("未检测")
    w.ip_detect_inflight = False
    w.ip_detect_lock = app.threading.Lock()
    w.btn_detect_ip = DummyButton()

    orig_thread = app.threading.Thread
    created: list[object] = []

    class HoldThread:
        def __init__(self, target=None, args=(), kwargs=None, daemon=None):
            self._target = target
            self._args = args
            self._kwargs = kwargs or {}

        def start(self):
            created.append(self)

    app.threading.Thread = HoldThread
    try:
        w.start_detect_ip()
        w.start_detect_ip()
        assert len(created) == 1
        assert w.ip_detect_inflight is True
        assert w.btn_detect_ip.state == "disabled"
        w._finish_detect_ip()
        assert w.ip_detect_inflight is False
        assert w.btn_detect_ip.state == "normal"
    finally:
        app.threading.Thread = orig_thread


def test_no_except_lambda_exc_capture():
    offenders: list[tuple[str, int, str]] = []
    py_files = [p for p in ROOT_DIR.glob("**/*.py") if "__pycache__" not in p.parts]

    for file_path in py_files:
        source = file_path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(file_path))

        class Visitor(ast.NodeVisitor):
            def __init__(self):
                self.exc_stack: list[str | None] = []

            def visit_ExceptHandler(self, node):
                self.exc_stack.append(node.name if isinstance(node.name, str) else None)
                for stmt in node.body:
                    self.visit(stmt)
                self.exc_stack.pop()

            def visit_Lambda(self, node):
                names = {n.id for n in ast.walk(node.body) if isinstance(n, ast.Name)}
                for exc_name in [x for x in self.exc_stack if x]:
                    if exc_name in names:
                        offenders.append((str(file_path.relative_to(ROOT_DIR)), node.lineno, exc_name))
                self.generic_visit(node)

        Visitor().visit(tree)

    assert not offenders, f"发现 except 变量被 lambda 延迟引用: {offenders[:5]}"


def run_case(name: str, fn):
    try:
        fn()
        print(f"[PASS] {name}")
        return True
    except Exception as exc:
        print(f"[FAIL] {name}: {exc}")
        return False


def main():
    cases = [
        ("AccountStore roundtrip", test_store_roundtrip),
        ("AccountStore encrypts sensitive", test_store_encrypts_sensitive_fields),
        ("Append log keeps 500 rows", test_append_log_row_keeps_500_rows),
        ("AccountStore old format", test_store_old_list_compat),
        ("Bitget store roundtrip", test_bg_store_roundtrip),
        ("Bitget store encrypts sensitive", test_bg_store_encrypts_sensitive_fields),
        ("Onchain store roundtrip", test_onchain_store_roundtrip),
        ("Onchain store encrypts sensitive", test_onchain_store_encrypts_sensitive_fields),
        ("Import auto-select accounts", test_import_auto_select_accounts),
        ("Import auto-select addresses", test_import_auto_select_addresses),
        ("Delete falls back to checked accounts", test_delete_selected_falls_back_to_checked_accounts),
        ("Delete falls back to checked addresses", test_delete_selected_falls_back_to_checked_addresses),
        ("Delete ignores selected without checked", test_delete_selected_ignores_mouse_selection_when_not_checked),
        ("Right click selects row and shows menu", test_tree_right_click_selects_row_and_shows_menu),
        ("Query balance current row only", test_query_balance_current_row_uses_selected_row_only),
        ("Withdraw current row only", test_withdraw_current_row_uses_selected_row_only),
        ("Delete current row only", test_delete_current_row_uses_selected_row_only),
        ("Bitget right click menu", test_bitget_tree_right_click_selects_row_and_shows_menu),
        ("Bitget current row ops", test_bitget_current_row_ops_use_selected_row_only),
        ("Bitget delete current row", test_bitget_delete_current_row_uses_selected_row_only),
        ("Bitget delete checked rows only", test_bitget_delete_selected_uses_checked_rows_only),
        ("Onchain right click menu", test_onchain_tree_right_click_selects_row_and_shows_menu),
        ("Onchain current row ops", test_onchain_current_row_ops_use_selected_row_only),
        ("Onchain delete current row", test_onchain_delete_current_row_uses_selected_row_only),
        ("Onchain delete checked rows only", test_onchain_delete_selected_uses_checked_rows_only),
        ("Onchain query requires checked m2m", test_onchain_query_balance_requires_checked_rows_in_m2m),
        ("Onchain query uses checked m2m", test_onchain_query_balance_uses_checked_rows_in_m2m),
        ("Onchain query requires checked 1m", test_onchain_query_balance_requires_checked_rows_in_1m),
        ("Onchain query requires checked m1", test_onchain_query_balance_requires_checked_rows_in_m1),
        ("Main click toggle checked any column", test_main_tree_click_toggles_checked_on_any_column),
        ("Main click toggle 1m checked any column", test_main_tree_click_toggles_one_to_many_checked_on_any_column),
        ("Bitget click toggle checked any column", test_bitget_tree_click_toggles_checked_on_any_column),
        ("Onchain click toggle checked any column", test_onchain_tree_click_toggles_checked_on_any_column),
        ("Validate requires network", test_validate_requires_network),
        ("Validate random mode", test_validate_random_mode),
        ("Validate all mode", test_validate_all_mode),
        ("Bitget validate requires network", test_bg_validate_requires_network),
        ("Onchain validate requires network", test_onchain_validate_requires_network),
        ("Onchain validate random mode", test_onchain_validate_random_mode),
        ("Random amount two decimals", test_random_amount_two_decimals),
        ("Bitget random amount two decimals", test_bg_random_amount_two_decimals),
        ("Network options ignore stale account", test_network_options_ignores_stale_account_context),
        ("Network options apply on match", test_network_options_applies_when_account_and_coin_match),
        ("Binance query checked only", test_binance_query_balance_checked_only_accounts),
        ("Binance refresh checked only", test_binance_refresh_coin_checked_only_accounts),
        ("Binance refresh requires checked", test_binance_refresh_coin_requires_checked_accounts),
        ("Binance query requires checked", test_binance_query_balance_requires_checked_accounts),
        ("Binance withdraw checked only", test_binance_batch_withdraw_checked_only_accounts),
        ("Binance withdraw requires checked", test_binance_batch_withdraw_requires_checked_accounts),
        ("Withdraw status one-to-many", test_run_withdraw_one_to_many_status),
        ("Withdraw status multi-to-multi", test_run_withdraw_multi_to_multi_status),
        ("Binance dry run all no balance API", test_binance_dry_run_all_does_not_call_balance_api),
        ("Bitget dry run all no balance API", test_bitget_dry_run_all_does_not_call_balance_api),
        ("Onchain dry run no rpc", test_onchain_dry_run_does_not_call_rpc),
        ("IP detect single inflight", test_start_detect_ip_single_inflight),
        ("No except-lambda exc capture", test_no_except_lambda_exc_capture),
    ]

    ok = 0
    for name, fn in cases:
        if run_case(name, fn):
            ok += 1

    total = len(cases)
    print(f"\nSummary: {ok}/{total} passed")
    return 0 if ok == total else 1


if __name__ == "__main__":
    raise SystemExit(main())
