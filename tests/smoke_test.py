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
import api_clients
import batch_actions
import ip_detection
import network_options
import page_bitget
import page_onchain
import table_import_utils
import withdraw_ui
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


class RaisingAfterRoot:
    def after(self, _ms, callback=None):
        raise RuntimeError("root destroyed")


class DummyMainloopRoot:
    def __init__(self):
        self.mainloop_called = False

    def mainloop(self):
        self.mainloop_called = True


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

    def focus_set(self):
        return None

    def heading(self, *_args, **_kwargs):
        return None


class ClipboardRoot:
    def __init__(self, text: str = ""):
        self.text = text

    def clipboard_get(self):
        return self.text


class FakeColumnTree:
    def __init__(self, widths: dict[str, int], viewport_width: int, xview_start: float = 0.0):
        self._widths = dict(widths)
        self._viewport_width = viewport_width
        self._xview_start = xview_start

    def column(self, column, option=None):
        width = self._widths[column]
        if option == "width":
            return width
        return {"width": width}

    def winfo_width(self):
        return self._viewport_width

    def xview(self):
        return (self._xview_start, 1.0)


class FakeStatusTree:
    def __init__(self, rows: dict[str, list[str | int]]):
        self._rows = {row_id: {"values": list(values), "tags": ()} for row_id, values in rows.items()}

    def item(self, row_id, option=None, **kwargs):
        row = self._rows[row_id]
        if "values" in kwargs:
            row["values"] = list(kwargs["values"])
        if "tags" in kwargs:
            row["tags"] = tuple(kwargs["tags"])
        if option == "values":
            return list(row["values"])
        if option == "tags":
            return tuple(row["tags"])
        return {"values": list(row["values"]), "tags": tuple(row["tags"])}

    def get_children(self):
        return list(self._rows)

    def delete(self, *row_ids):
        for row_id in row_ids:
            self._rows.pop(row_id, None)


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


class FakeSizedWidget:
    def __init__(self, width: int = 760):
        self._width = width

    def winfo_width(self):
        return self._width


class FakeHintLabel:
    def __init__(self):
        self.text = ""
        self.wraplength = 0
        self.placed = False

    def configure(self, **kwargs):
        if "text" in kwargs:
            self.text = kwargs["text"]
        if "wraplength" in kwargs:
            self.wraplength = kwargs["wraplength"]

    def place(self, **_kwargs):
        self.placed = True

    def place_forget(self):
        self.placed = False

    def lift(self):
        return None


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


class CountingThreadPatch:
    def __init__(self, module):
        self.module = module
        self.created = 0
        self._orig = None

    def __enter__(self):
        self._orig = self.module.threading.Thread
        patch = self

        class CountingThread:
            def __init__(self, target=None, args=(), kwargs=None, daemon=None):
                self._target = target
                self._args = args
                self._kwargs = kwargs or {}
                patch.created += 1

            def start(self):
                if self._target is not None:
                    self._target(*self._args, **self._kwargs)

            def join(self):
                return None

        self.module.threading.Thread = CountingThread
        return self

    def __exit__(self, exc_type, exc, tb):
        self.module.threading.Thread = self._orig
        return False


class FakeClient:
    @staticmethod
    def new_withdraw_order_id() -> str:
        return "fake_wd_oid"

    def withdraw(self, account: app.AccountEntry, coin: str, amount: str, network: str = "", *, withdraw_order_id: str = ""):
        if "bad" in account.address:
            raise RuntimeError("mock failed")
        return {"id": f"wd_{account.address[-4:]}"}

    def get_spot_balance(self, _account: app.AccountEntry, _coin: str):
        return Decimal("12.34"), Decimal("0")

    def get_all_spot_balances(self, account: app.AccountEntry, include_zero: bool = True):
        if "bad" in account.api_key:
            raise RuntimeError("mock balance failed")
        return {
            "USDT": (Decimal("1"), Decimal("0")),
            "BTC": (Decimal("0.5"), Decimal("0")),
        }

    def get_withdraw_fee(self, _account: app.AccountEntry, _coin: str, _network: str = ""):
        return Decimal("0.2")


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


def test_store_load_missing_file_clears_one_to_many_state():
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "accounts.json"
        store = app.AccountStore(p)
        store.one_to_many_addresses = ["addr1"]
        store.one_to_many_source_api_key = "src_k"
        store.one_to_many_source_api_secret = "src_s"
        store.load()
        assert store.accounts == []
        assert store.one_to_many_addresses == []
        assert store.one_to_many_source_api_key == ""
        assert store.one_to_many_source_api_secret == ""


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


def test_evm_validate_address_rejects_bad_checksum():
    client = app.EvmClient()
    ok = client.validate_evm_address("0x52908400098527886e0f7030069857d2e4169ee7", "接收地址")
    assert ok.startswith("0x")
    assert len(ok) == 42
    try:
        client.validate_evm_address("0x52908400098527886E0F7030069857D2e4169ee7", "接收地址")
    except RuntimeError as exc:
        assert "校验失败" in str(exc)
    else:
        raise AssertionError("expected checksum validation failure")


def test_table_import_utils_merge_column_values():
    drafts = [{"api_key": "k1", "api_secret": "", "address": ""}]
    added = table_import_utils.merge_column_values(drafts, ("api_key", "api_secret", "address"), "api_secret", ["s1", "s2"])
    assert added == 1
    assert drafts == [
        {"api_key": "k1", "api_secret": "s1", "address": ""},
        {"api_key": "", "api_secret": "s2", "address": ""},
    ]


def test_table_import_utils_compute_column_highlight_geometry():
    tree = FakeColumnTree(
        widths={"checked": 42, "idx": 42, "api_key": 180, "api_secret": 180},
        viewport_width=260,
        xview_start=0.14,
    )
    rect = table_import_utils.compute_column_highlight_geometry(
        tree,
        ("checked", "idx", "api_key", "api_secret"),
        "api_key",
    )
    assert rect == (22, 180)
    assert table_import_utils.compute_column_highlight_geometry(tree, ("checked", "idx"), "full") is None


def test_main_column_clipboard_import_promotes_complete_accounts():
    w = make_import_stub(one_to_many=False)
    w.account_import_drafts = []
    w._import_target = "api_key"
    w.root = ClipboardRoot(
        "\n".join(
            [
                "key_1",
                "key_2",
            ]
        )
    )

    w.import_from_clipboard()
    assert w.account_import_drafts == [
        {"api_key": "key_1", "api_secret": "", "address": ""},
        {"api_key": "key_2", "api_secret": "", "address": ""},
    ]

    w._import_target = "api_secret"
    w.root.text = "sec_1\nsec_2"
    w.import_from_clipboard()
    assert w.account_import_drafts == [
        {"api_key": "key_1", "api_secret": "sec_1", "address": ""},
        {"api_key": "key_2", "api_secret": "sec_2", "address": ""},
    ]

    w._import_target = "address"
    w.root.text = "addr_1\naddr_2"
    w.import_from_clipboard()

    assert [(x.api_key, x.api_secret, x.address) for x in w.store.accounts] == [
        ("key_1", "sec_1", "addr_1"),
        ("key_2", "sec_2", "addr_2"),
    ]
    assert w.account_import_drafts == []
    assert w.checked_api_keys == {"key_1", "key_2"}
    assert any("待补齐 2 行" in x for x in w.log_messages)
    assert any("补齐 2 个账号" in x for x in w.log_messages)


def test_onchain_column_clipboard_import_promotes_complete_pairs():
    w = app.OnchainTransferPage.__new__(app.OnchainTransferPage)
    w.store = app.OnchainStore(Path(tempfile.gettempdir()) / "noop_onchain_column_import.json")
    w.client = type(
        "Client",
        (),
        {
            "is_address": staticmethod(lambda addr: str(addr).startswith("0x") and len(str(addr)) == 42),
        },
    )()
    w.mode_var = DummyVar(app.OnchainTransferPage.MODE_M2M)
    w.checked_row_keys = set()
    w.row_status = {}
    w.query_row_status = {}
    w.source_balance_cache = {}
    w.target_balance_cache = {}
    w.row_index_map = {}
    w.row_key_by_row_id = {}
    w.row_id_by_key = {}
    w.m2m_import_drafts = []
    w._import_target = "source"
    w.log_messages = []
    w.log = lambda msg: w.log_messages.append(msg)
    w._refresh_tree = lambda: None
    w.root = ClipboardRoot("source_1\nsource_2")

    w.import_from_clipboard()
    assert w.m2m_import_drafts == [
        {"source": "source_1", "target": ""},
        {"source": "source_2", "target": ""},
    ]

    w._import_target = "target"
    w.root.text = "\n".join(
        [
            "0x1111111111111111111111111111111111111111",
            "0x2222222222222222222222222222222222222222",
        ]
    )
    w.import_from_clipboard()

    assert [(x.source, x.target) for x in w.store.multi_to_multi_pairs] == [
        ("source_1", "0x1111111111111111111111111111111111111111"),
        ("source_2", "0x2222222222222222222222222222222222222222"),
    ]
    assert w.m2m_import_drafts == []
    assert w.checked_row_keys == {
        "m2m:source_1|0x1111111111111111111111111111111111111111",
        "m2m:source_2|0x2222222222222222222222222222222222222222",
    }
    assert any("待补齐 2 行" in x for x in w.log_messages)
    assert any("补齐 2 条" in x for x in w.log_messages)


def test_main_empty_hint_updates_by_mode_and_data():
    w = app.WithdrawApp.__new__(app.WithdrawApp)
    w.store = app.AccountStore(Path(tempfile.gettempdir()) / "noop_hint_accounts.json")
    w.empty_hint_label = FakeHintLabel()
    w.table_wrap = FakeSizedWidget()
    w._is_one_to_many_mode = lambda: False

    withdraw_ui.update_empty_import_hint(w)
    assert w.empty_hint_label.placed is True
    assert "API Key API Secret 提现地址" in w.empty_hint_label.text
    assert "点击中间空白处后按 Cmd+V / Ctrl+V" in w.empty_hint_label.text

    w._is_one_to_many_mode = lambda: True
    withdraw_ui.update_empty_import_hint(w)
    assert w.empty_hint_label.placed is True
    assert "每行一个提现地址" in w.empty_hint_label.text
    assert "点击“提现地址”表头后可按列粘贴" in w.empty_hint_label.text

    w.store.one_to_many_addresses = ["addr_1"]
    withdraw_ui.update_empty_import_hint(w)
    assert w.empty_hint_label.placed is False


def test_bitget_empty_hint_hides_after_rows_exist():
    w = app.BitgetOneToManyPage.__new__(app.BitgetOneToManyPage)
    w.store = app.BgOneToManyStore(Path(tempfile.gettempdir()) / "noop_bg_hint.json")
    w.empty_hint_label = FakeHintLabel()
    w.table_wrap = FakeSizedWidget()

    w._update_empty_hint()
    assert w.empty_hint_label.placed is True
    assert "每行一个提现地址" in w.empty_hint_label.text
    assert "点击“提现地址”表头后可按列粘贴" in w.empty_hint_label.text

    w.store.addresses = ["addr_1"]
    w._update_empty_hint()
    assert w.empty_hint_label.placed is False


def test_onchain_empty_hint_switches_with_mode():
    w = app.OnchainTransferPage.__new__(app.OnchainTransferPage)
    w.store = app.OnchainStore(Path(tempfile.gettempdir()) / "noop_onchain_hint.json")
    w.empty_hint_label = FakeHintLabel()
    w.table_wrap = FakeSizedWidget()
    w.mode_var = DummyVar(app.OnchainTransferPage.MODE_M2M)

    w._update_empty_hint()
    assert w.empty_hint_label.placed is True
    assert "转出钱包私钥或助记词 接收地址" in w.empty_hint_label.text
    assert "点击“转出凭证”或“接收地址”表头后可按列粘贴" in w.empty_hint_label.text

    w.mode_var.set(app.OnchainTransferPage.MODE_M1)
    w._update_empty_hint()
    assert "每行一个转出钱包私钥或助记词" in w.empty_hint_label.text

    w.store.many_to_one_sources = ["seed words"]
    w._update_empty_hint()
    assert w.empty_hint_label.placed is False


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


def test_delete_selected_removes_checked_account_drafts():
    w = app.WithdrawApp.__new__(app.WithdrawApp)
    w.store = app.AccountStore(Path(tempfile.gettempdir()) / "noop_delete_account_drafts.json")
    w.store.accounts = [app.AccountEntry("k1", "s1", "a1")]
    w.account_import_drafts = [{"api_key": "k2", "api_secret": "", "address": ""}]
    w.checked_api_keys = {"k1"}
    w.checked_account_draft_rows = {0}
    w._checked_indices = lambda: [0]
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
    assert w.store.accounts == []
    assert w.account_import_drafts == []
    assert w.checked_account_draft_rows == set()
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
    w.tree = FakeToggleTree(row_id="oc_row_1", column="#4", values=["", 1, "src", "dst", "-", "-"])
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


def test_onchain_delete_selected_removes_checked_drafts():
    w = app.OnchainTransferPage.__new__(app.OnchainTransferPage)
    w.store = type("Store", (), {})()
    w.store.multi_to_multi_pairs = [app.OnchainPairEntry(source="s1", target="0x1111111111111111111111111111111111111111")]
    w.store.one_to_many_addresses = []
    w.store.many_to_one_sources = []
    w.store.delete_multi_to_multi_by_indices = lambda idxs: setattr(
        w.store,
        "multi_to_multi_pairs",
        [x for i, x in enumerate(w.store.multi_to_multi_pairs) if i not in set(idxs)],
    )
    w.checked_row_keys = {app.OnchainTransferPage._m2m_key(w.store.multi_to_multi_pairs[0])}
    w.m2m_import_drafts = [{"source": "s2", "target": ""}]
    w.checked_m2m_draft_rows = {0}
    w._mode = lambda: app.OnchainTransferPage.MODE_M2M
    w.refreshed = False
    w._refresh_tree = lambda: setattr(w, "refreshed", True)
    w.log_messages = []
    w.log = lambda m: w.log_messages.append(m)

    with MessagePatch() as mp:
        w.delete_selected()

    assert not mp.warnings
    assert w.store.multi_to_multi_pairs == []
    assert w.m2m_import_drafts == []
    assert w.checked_m2m_draft_rows == set()
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


def test_onchain_query_balance_one_to_many_uses_runtime_threads():
    w = make_onchain_balance_runtime_stub(threads="2")
    token = page_onchain.EvmToken(symbol="ETH", contract="", decimals=18, is_native=True)

    with CountingThreadPatch(page_onchain) as tp:
        w._run_query_balance_one_to_many("ETH", token, "", ["addr_1", "addr_2", "addr_3"])

    assert tp.created == 2
    assert len(w.target_balance_cache) == 3
    assert w.refreshed is True
    assert w.is_running is False


def test_onchain_query_balance_many_to_one_uses_runtime_threads():
    w = make_onchain_balance_runtime_stub(threads="3")
    token = page_onchain.EvmToken(symbol="ETH", contract="", decimals=18, is_native=True)

    with CountingThreadPatch(page_onchain) as tp:
        w._run_query_balance_many_to_one("ETH", token, "", ["seed_1", "seed_2"])

    assert tp.created == 2
    assert len(w.source_balance_cache) == 2
    assert w.refreshed is True
    assert w.is_running is False


def test_onchain_query_balance_updates_row_realtime_in_m2m():
    w = make_onchain_query_row_runtime_stub(mode=app.OnchainTransferPage.MODE_M2M)
    item = app.OnchainPairEntry(source="seed_1", target="0x1111111111111111111111111111111111111111")
    row_key = app.OnchainTransferPage._m2m_key(item)
    w.store = type(
        "Store",
        (),
        {"multi_to_multi_pairs": [item], "one_to_many_addresses": [], "many_to_one_sources": []},
    )()
    w.checked_row_keys = {row_key}
    w.row_index_map = {"row_1": 0}
    w.row_key_by_row_id = {"row_1": row_key}
    w.row_id_by_key = {row_key: "row_1"}
    w.tree = FakeStatusTree({"row_1": ["✓", 1, "seed_1", item.target, "-", "-"]})
    w.source_address_cache["seed_1"] = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    w.source_balance_cache["seed_1"] = Decimal("1")
    w._set_query_status(row_key, "running")
    assert w.tree.item("row_1", "values")[4] == "进行中"
    w._set_query_status(row_key, "success")

    values = w.tree.item("row_1", "values")
    assert values[2] == "seed_1"
    assert values[4] == "完成"
    assert values[5] == "ETH:1"
    assert w.tree.item("row_1", "tags") == ("st_success",)


def test_onchain_query_balance_updates_row_realtime_in_1m():
    w = make_onchain_query_row_runtime_stub(mode=app.OnchainTransferPage.MODE_1M)
    target = "0x2222222222222222222222222222222222222222"
    row_key = app.OnchainTransferPage._one_to_many_key(target)
    w.store = type(
        "Store",
        (),
        {"multi_to_multi_pairs": [], "one_to_many_addresses": [target], "many_to_one_sources": []},
    )()
    w.checked_row_keys = {row_key}
    w.row_index_map = {"row_1": 0}
    w.row_key_by_row_id = {"row_1": row_key}
    w.row_id_by_key = {row_key: "row_1"}
    w.tree = FakeStatusTree({"row_1": ["✓", 1, "seed_src", target, "-", "-"]})
    w.source_address_cache["seed_src"] = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    w.target_balance_cache[target] = Decimal("1")
    w._set_query_status(row_key, "running")
    assert w.tree.item("row_1", "values")[4] == "进行中"
    w._set_query_status(row_key, "success")

    values = w.tree.item("row_1", "values")
    assert values[2] == "seed_src"
    assert values[4] == "完成"
    assert values[5] == "ETH:1"
    assert w.tree.item("row_1", "tags") == ("st_success",)


def test_onchain_collect_failed_jobs_ignores_query_failed_status():
    item = app.OnchainPairEntry(source="seed_1", target="0x1111111111111111111111111111111111111111")
    row_key = app.OnchainTransferPage._m2m_key(item)
    w = app.OnchainTransferPage.__new__(app.OnchainTransferPage)
    w.store = type(
        "Store",
        (),
        {"multi_to_multi_pairs": [item], "one_to_many_addresses": [], "many_to_one_sources": []},
    )()
    w.row_status = {}
    w.query_row_status = {row_key: "failed"}
    w._mode = lambda: app.OnchainTransferPage.MODE_M2M

    jobs = w._collect_failed_jobs()
    assert jobs == []


def test_onchain_query_progress_label_updates():
    w = make_onchain_query_row_runtime_stub(mode=app.OnchainTransferPage.MODE_M2M)
    row_key = "m2m:seed_1|addr_1"
    suffix = " | 余额总额=- | 转账总额=- | gas总额=-"

    w._begin_progress("query", [row_key])
    assert w.progress_var.get() == f"查询进度：0/1 | 等待0 | 进行中0 | 成功0 | 失败0{suffix}"

    w._set_query_status(row_key, "waiting")
    assert w.progress_var.get() == f"查询进度：0/1 | 等待1 | 进行中0 | 成功0 | 失败0{suffix}"

    w._set_query_status(row_key, "running")
    assert w.progress_var.get() == f"查询进度：0/1 | 等待0 | 进行中1 | 成功0 | 失败0{suffix}"

    w._set_query_status(row_key, "success")
    assert w.progress_var.get() == f"查询进度：1/1 | 等待0 | 进行中0 | 成功1 | 失败0{suffix}"

    w._finish_progress("query", 1, 0)
    assert w.progress_var.get() == f"查询完成：总1 | 成功1 | 失败0{suffix}"


def test_onchain_query_finish_progress_counts_rows_for_shared_source():
    w = make_onchain_query_row_runtime_stub(mode=app.OnchainTransferPage.MODE_M2M)
    row_keys = ["m2m:seed_1|addr_1", "m2m:seed_1|addr_2"]

    w._begin_progress("query", row_keys)
    w._set_query_statuses(row_keys, "success")
    w._finish_progress("query", 1, 0)

    assert w.progress_var.get() == "查询完成：总2 | 成功2 | 失败0 | 余额总额=- | 转账总额=- | gas总额=-"


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
    w.tree = FakeToggleTree(row_id="oc_row_1", column="#5", values=["", 1, "src", "target", "-", "-"])
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


def make_onchain_balance_runtime_stub(*, threads: str = "2"):
    class BalanceClient:
        @staticmethod
        def get_balance_wei(_network: str, _address: str):
            return 1

        @staticmethod
        def get_erc20_balance(_network: str, _contract: str, _address: str):
            return 1

    w = app.OnchainTransferPage.__new__(app.OnchainTransferPage)
    w.root = DummyRoot()
    w.client = BalanceClient()
    w.threads_var = DummyVar(threads)
    w.source_balance_cache = {}
    w.target_balance_cache = {}
    w.source_address_cache = {}
    w.source_private_key_cache = {}
    w.source_balance_var = DummyVar("-")
    w.target_balance_var = DummyVar("-")
    w.symbol_var = DummyVar("ETH")
    w.progress_var = DummyVar("进度：空闲")
    w._active_progress_kind = ""
    w._active_progress_keys = []
    w.is_running = True
    w.refreshed = False
    w._refresh_tree = lambda: setattr(w, "refreshed", True)
    w.log_messages = []
    w.log = lambda m: w.log_messages.append(m)
    w._token_desc = lambda token: token.symbol
    w._mask = lambda value, head=8, tail=6: value
    w._units_to_amount = lambda units, _decimals: Decimal(str(units))
    w._decimal_to_text = lambda amount: str(amount)
    w._resolve_wallet = lambda source: (f"pk_{source}", f"addr_{source}")
    return w


def make_onchain_query_row_runtime_stub(*, mode: str, threads: str = "1"):
    w = make_onchain_balance_runtime_stub(threads=threads)
    w.row_status = {}
    w.query_row_status = {}
    w.checked_row_keys = set()
    w._mode = lambda: mode
    w._refresh_tree = lambda: None
    w.source_credential_var = DummyVar("seed_src")
    w.target_address_var = DummyVar("0x9999999999999999999999999999999999999999")
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


def test_main_save_rejects_invalid_random_amount_range():
    w = app.WithdrawApp.__new__(app.WithdrawApp)
    w.store = app.AccountStore(Path(tempfile.gettempdir()) / "noop_main_save_validate.json")
    w.mode_var = DummyVar(app.WithdrawApp.MODE_M2M)
    w.coin_var = DummyVar("USDT")
    w.network_var = DummyVar("BSC")
    w.amount_mode_var = DummyVar(app.WithdrawApp.AMOUNT_MODE_RANDOM)
    w.amount_var = DummyVar("")
    w.random_min_var = DummyVar("1.0")
    w.random_max_var = DummyVar("0.5")
    w.delay_var = DummyVar(1.0)
    w.threads_var = DummyVar("2")
    w.dry_run_var = DummyVar(True)
    w.source_api_key_var = DummyVar("")
    w.source_api_secret_var = DummyVar("")
    w._amount_mode = lambda: w.amount_mode_var.get()
    with MessagePatch() as mp:
        ok = w._apply_settings_to_store()
    assert ok is False
    assert mp.errors and "随机金额最大值必须大于或等于最小值" in mp.errors[-1][1]


def test_bitget_save_rejects_invalid_fixed_amount():
    w = app.BitgetOneToManyPage.__new__(app.BitgetOneToManyPage)
    w.coin_var = DummyVar("USDT")
    w.network_var = DummyVar("trc20")
    w.amount_mode_var = DummyVar(app.BitgetOneToManyPage.AMOUNT_MODE_FIXED)
    w.amount_var = DummyVar("abc")
    w.random_min_var = DummyVar("")
    w.random_max_var = DummyVar("")
    w.delay_var = DummyVar(1.0)
    w.threads_var = DummyVar("2")
    w.dry_run_var = DummyVar(True)
    w.api_key_var = DummyVar("")
    w.api_secret_var = DummyVar("")
    w.passphrase_var = DummyVar("")
    w._amount_mode = lambda: w.amount_mode_var.get()
    with MessagePatch() as mp:
        ok = w._apply_settings_to_store()
    assert ok is False
    assert mp.errors and "固定数量必须是大于 0 的数字" in mp.errors[-1][1]


def test_onchain_save_rejects_invalid_random_amount_range():
    w = app.OnchainTransferPage.__new__(app.OnchainTransferPage)
    w.store = app.OnchainStore(Path(tempfile.gettempdir()) / "noop_onchain_save_validate.json")
    w.mode_var = DummyVar(app.OnchainTransferPage.MODE_M2M)
    w.network_var = DummyVar("ETH")
    w.amount_mode_var = DummyVar(app.OnchainTransferPage.AMOUNT_MODE_RANDOM)
    w.amount_var = DummyVar("")
    w.random_min_var = DummyVar("2")
    w.random_max_var = DummyVar("1")
    w.delay_var = DummyVar(1.0)
    w.threads_var = DummyVar("2")
    w.dry_run_var = DummyVar(True)
    w.source_credential_var = DummyVar("")
    w.target_address_var = DummyVar("")
    w._mode = lambda: w.mode_var.get()
    w._amount_mode = lambda: w.amount_mode_var.get()
    w._selected_token = lambda with_message=False: page_onchain.EvmToken(symbol="ETH")
    with MessagePatch() as mp:
        ok = w._apply_settings_to_store()
    assert ok is False
    assert mp.errors and "随机金额最大值必须大于或等于最小值" in mp.errors[-1][1]


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
    w.submitted_timeout_seconds = 0.0
    w.row_id_by_api_key = {}
    w.row_index_map = {}
    w.account_withdraw_status = {}
    w.account_withdraw_status_context = {}
    w.account_query_status = {}
    w.account_coin_balance_cache = {}
    w.progress_var = DummyVar("进度：空闲")
    w._active_progress_kind = ""
    w._active_progress_keys = []
    w._progress_amount_label = "提现总额"
    w._summary_balance_text = "-"
    w._summary_amount_text = "-"
    w._summary_gas_text = "-"
    w.is_running = True
    w.log_messages: list[str] = []
    w.log = lambda m: w.log_messages.append(m)
    w._replace_account_coin_totals = lambda api_key, totals: w.account_coin_balance_cache.__setitem__(api_key, dict(totals))
    w._totals_to_text = lambda totals: " | ".join(f"{k}:{v}" for k, v in sorted(totals.items())) if totals else "-"
    w._coin_amount_text = lambda coin, amount: f"{amount} {coin}"
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
    w.mode_var = DummyVar(app.WithdrawApp.MODE_1M)
    w.source_api_key_var = DummyVar("src")
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
    assert "提现完成：总3 | 成功2 | 失败1" in w.progress_var.get()
    assert "提现总额=2.00 USDT" in w.progress_var.get()
    assert "gas总额=0.4 USDT" in w.progress_var.get()
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
    assert "提现完成：总2 | 成功1 | 失败1" in w.progress_var.get()
    assert "提现总额=2 USDT" in w.progress_var.get()
    assert "gas总额=0.2 USDT" in w.progress_var.get()
    assert mp.infos


def test_binance_withdraw_without_withdraw_id_auto_fails():
    class UncertainClient:
        @staticmethod
        def new_withdraw_order_id() -> str:
            return "wd_oid_pending"

        def withdraw(self, *_args, **_kwargs):
            return {}

        def get_withdraw_fee(self, _account: app.AccountEntry, _coin: str, _network: str = ""):
            return Decimal("0.2")

    w = make_withdraw_stub()
    w.client = UncertainClient()
    accounts = [app.AccountEntry("k1", "s1", "good_x")]
    params = app.WithdrawRuntimeParams(
        coin="USDT",
        amount="2",
        network="ETH",
        delay=0.0,
        threads=1,
    )
    with MessagePatch():
        w._run_withdraw(accounts, params, dry_run=False, one_to_many=False)
    assert w.account_withdraw_status["k1"] == "failed"
    assert any("未返回 withdrawId" in x for x in w.log_messages)
    assert any("自动判定失败" in x for x in w.log_messages)
    assert w.progress_var.get() == "提现完成：总1 | 成功0 | 失败1 | 余额总额=- | 提现总额=0 USDT | gas总额=0 USDT"


def test_binance_submitted_timeout_does_not_block_next_job():
    class UncertainClient:
        @staticmethod
        def new_withdraw_order_id() -> str:
            return "wd_oid_pending"

        def withdraw(self, *_args, **_kwargs):
            return {}

        def get_withdraw_fee(self, _account: app.AccountEntry, _coin: str, _network: str = ""):
            return Decimal("0.2")

    w = make_withdraw_stub()
    w.client = UncertainClient()
    w.submitted_timeout_seconds = 0.05
    accounts = [
        app.AccountEntry("k1", "s1", "good_x"),
        app.AccountEntry("k2", "s2", "good_y"),
    ]
    params = app.WithdrawRuntimeParams(
        coin="USDT",
        amount="2",
        network="ETH",
        delay=0.0,
        threads=1,
    )
    with MessagePatch():
        w._run_withdraw(accounts, params, dry_run=False, one_to_many=False)
    pending_logs = [i for i, text in enumerate(w.log_messages) if "未返回 withdrawId" in text]
    timeout_logs = [i for i, text in enumerate(w.log_messages) if "自动判定失败" in text]
    assert len(pending_logs) == 2
    assert len(timeout_logs) == 2
    assert max(pending_logs) < min(timeout_logs)


def test_binance_withdraw_finishes_when_root_after_fails():
    w = make_withdraw_stub()
    w.root = RaisingAfterRoot()
    accounts = [app.AccountEntry("k1", "s1", "good_x")]
    params = app.WithdrawRuntimeParams(
        coin="USDT",
        amount="2",
        network="ETH",
        delay=0.0,
        threads=1,
    )
    with MessagePatch():
        w._run_withdraw(accounts, params, dry_run=False, one_to_many=False)
    assert w.account_withdraw_status["k1"] == "success"
    assert w.is_running is False


def test_binance_query_progress_shows_balance_total():
    w = make_withdraw_stub()
    accounts = [
        app.AccountEntry("good_api_1", "s1", "addr_1"),
        app.AccountEntry("bad_api_2", "s2", "addr_2"),
    ]

    with MessagePatch():
        w._run_query_balance(accounts, "USDT")

    assert any("余额查询结束：成功 1，失败 1" in x for x in w.log_messages)
    assert w.progress_var.get() == "查询完成：总2 | 成功1 | 失败1 | 余额总额=1 USDT | 提现总额=- | gas总额=-"


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
    assert "提现总额=-" in w.progress_var.get()


def test_binance_retry_ignores_stale_one_to_many_status_after_source_change():
    w = app.WithdrawApp.__new__(app.WithdrawApp)
    w.store = app.AccountStore(Path(tempfile.gettempdir()) / "noop_binance_retry_context.json")
    w.store.one_to_many_addresses = ["addr_1", "addr_2"]
    w.mode_var = DummyVar(app.WithdrawApp.MODE_1M)
    w.source_api_key_var = DummyVar("src_old")
    w.source_api_secret_var = DummyVar("sec_old")
    w.source_balance_var = DummyVar("-")
    w.account_withdraw_status = {"addr_1": "failed"}
    w.account_withdraw_status_context = {"addr_1": "src_old"}
    w.account_query_status = {}
    w.account_coin_balance_cache = {}
    w.progress_var = DummyVar("进度：空闲")
    w._active_progress_kind = ""
    w._active_progress_keys = []
    w._progress_amount_label = "提现总额"
    w._summary_balance_text = "-"
    w._summary_amount_text = "-"
    w._summary_gas_text = "-"
    w.is_running = False
    w._refresh_tree = lambda: None
    w._update_source_balance_display = lambda: None
    w._build_source_account = lambda with_message=False: app.AccountEntry(
        w.source_api_key_var.get(), w.source_api_secret_var.get(), "N/A"
    )

    w.source_api_key_var.set("src_new")
    w._on_source_api_changed()

    assert w._failed_accounts_for_retry() == []


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
    w.address_status_context = {}
    w.is_running = True
    w.progress_var = DummyVar("进度：空闲")
    w._active_progress_kind = ""
    w._active_progress_keys = []
    w._progress_amount_label = "提现总额"
    w._summary_balance_text = "-"
    w._summary_amount_text = "-"
    w._summary_gas_text = "-"
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
    assert "提现总额=-" in w.progress_var.get()


def test_bitget_source_change_clears_balance_cache_and_hides_old_failed_status():
    w = app.BitgetOneToManyPage.__new__(app.BitgetOneToManyPage)
    w.store = app.BgOneToManyStore(Path(tempfile.gettempdir()) / "noop_bitget_context.json")
    w.store.addresses = ["addr_1", "addr_2"]
    w.api_key_var = DummyVar("bg_old")
    w.api_secret_var = DummyVar("sec_old")
    w.passphrase_var = DummyVar("pass_old")
    w.source_asset_cache = {"USDT": Decimal("9")}
    w.address_status = {"addr_1": "failed"}
    w.address_status_context = {"addr_1": "bg_old"}
    w.query_status = {"source:bg_old": "success"}
    w.progress_var = DummyVar("进度：空闲")
    w._active_progress_kind = ""
    w._active_progress_keys = []
    w._progress_amount_label = "提现总额"
    w._summary_balance_text = "-"
    w._summary_amount_text = "-"
    w._summary_gas_text = "-"
    w.is_running = False
    w._refresh_tree = lambda: None

    w.api_key_var.set("bg_new")
    w._on_source_cred_changed()

    assert w.source_asset_cache == {}
    assert w.query_status == {}
    assert w._failed_addr_list() == []


def test_bitget_query_progress_shows_balance_total():
    class QueryClient:
        def get_account_assets(self, _k, _s, _p):
            return {"USDT": Decimal("3"), "BTC": Decimal("1")}

    w = app.BitgetOneToManyPage.__new__(app.BitgetOneToManyPage)
    w.root = DummyRoot()
    w.client = QueryClient()
    w.query_status = {}
    w.address_status = {}
    w.row_id_by_addr = {}
    w.row_index_map = {}
    w.is_running = True
    w.coin_var = DummyVar("USDT")
    w.source_asset_cache = {}
    w.progress_var = DummyVar("进度：空闲")
    w._active_progress_kind = ""
    w._active_progress_keys = []
    w.log_messages = []
    w.log = lambda m: w.log_messages.append(m)
    w._refresh_tree = lambda: None

    with MessagePatch():
        w._run_query_balance(("k", "s", "p"))

    assert any("余额查询结束" in x for x in w.log_messages)
    assert w.progress_var.get() == "查询完成：总1 | 成功1 | 失败0 | 余额总额=3 USDT | 提现总额=- | gas总额=-"


def test_bitget_withdraw_progress_shows_amount_and_fee():
    class WithdrawClient:
        @staticmethod
        def new_client_oid() -> str:
            return "bg_client_oid"

        def get_withdraw_fee(self, _coin: str, _chain: str):
            return Decimal("0.3")

        def withdraw(self, _k, _s, _p, coin: str, address: str, amount: str, chain: str, *, client_oid: str = ""):
            if "bad" in address:
                raise RuntimeError("mock failed")
            return {"orderId": f"bg_{address[-3:]}"}

    w = app.BitgetOneToManyPage.__new__(app.BitgetOneToManyPage)
    w.root = DummyRoot()
    w.client = WithdrawClient()
    w.query_status = {}
    w.address_status = {}
    w.row_id_by_addr = {}
    w.row_index_map = {}
    w.is_running = True
    w.progress_var = DummyVar("进度：空闲")
    w._active_progress_kind = ""
    w._active_progress_keys = []
    w.log_messages = []
    w.log = lambda m: w.log_messages.append(m)

    params = app.WithdrawRuntimeParams(
        coin="USDT",
        amount="1.50",
        network="trc20",
        delay=0.0,
        threads=2,
    )
    with MessagePatch():
        w._run_withdraw(["addr_ok", "addr_bad"], params, ("k", "s", "p"), dry_run=False)

    assert any("Bitget 提现任务结束：成功 1，失败 1" in x for x in w.log_messages)
    assert w.progress_var.get() == "提现完成：总2 | 成功1 | 失败1 | 余额总额=- | 提现总额=1.5 USDT | gas总额=0.3 USDT"


def test_bitget_withdraw_without_order_id_auto_fails():
    class UncertainClient:
        @staticmethod
        def new_client_oid() -> str:
            return "bg_client_oid_pending"

        def get_withdraw_fee(self, _coin: str, _chain: str):
            return Decimal("0.3")

        def withdraw(self, *_args, **_kwargs):
            return {}

    w = app.BitgetOneToManyPage.__new__(app.BitgetOneToManyPage)
    w.root = DummyRoot()
    w.client = UncertainClient()
    w.submitted_timeout_seconds = 0.0
    w.query_status = {}
    w.address_status = {}
    w.row_id_by_addr = {}
    w.row_index_map = {}
    w.is_running = True
    w.progress_var = DummyVar("进度：空闲")
    w._active_progress_kind = ""
    w._active_progress_keys = []
    w.log_messages = []
    w.log = lambda m: w.log_messages.append(m)

    params = app.WithdrawRuntimeParams(
        coin="USDT",
        amount="1.50",
        network="trc20",
        delay=0.0,
        threads=1,
    )
    with MessagePatch():
        w._run_withdraw(["addr_ok"], params, ("k", "s", "p"), dry_run=False)

    assert w.address_status["addr_ok"] == "failed"
    assert any("未返回 orderId" in x for x in w.log_messages)
    assert any("自动判定失败" in x for x in w.log_messages)
    assert w.progress_var.get() == "提现完成：总1 | 成功0 | 失败1 | 余额总额=- | 提现总额=0 USDT | gas总额=0 USDT"


def test_bitget_submitted_timeout_does_not_block_next_job():
    class UncertainClient:
        @staticmethod
        def new_client_oid() -> str:
            return "bg_client_oid_pending"

        def get_withdraw_fee(self, _coin: str, _chain: str):
            return Decimal("0.3")

        def withdraw(self, *_args, **_kwargs):
            return {}

    w = app.BitgetOneToManyPage.__new__(app.BitgetOneToManyPage)
    w.root = DummyRoot()
    w.client = UncertainClient()
    w.submitted_timeout_seconds = 0.05
    w.query_status = {}
    w.address_status = {}
    w.row_id_by_addr = {}
    w.row_index_map = {}
    w.is_running = True
    w.progress_var = DummyVar("进度：空闲")
    w._active_progress_kind = ""
    w._active_progress_keys = []
    w.log_messages = []
    w.log = lambda m: w.log_messages.append(m)

    params = app.WithdrawRuntimeParams(
        coin="USDT",
        amount="1.50",
        network="trc20",
        delay=0.0,
        threads=1,
    )
    with MessagePatch():
        w._run_withdraw(["addr_1", "addr_2"], params, ("k", "s", "p"), dry_run=False)
    pending_logs = [i for i, text in enumerate(w.log_messages) if "未返回 orderId" in text]
    timeout_logs = [i for i, text in enumerate(w.log_messages) if "自动判定失败" in text]
    assert len(pending_logs) == 2
    assert len(timeout_logs) == 2
    assert max(pending_logs) < min(timeout_logs)


def test_bitget_withdraw_finishes_when_root_after_fails():
    class WithdrawClient:
        @staticmethod
        def new_client_oid() -> str:
            return "bg_client_oid"

        def get_withdraw_fee(self, _coin: str, _chain: str):
            return Decimal("0.3")

        def withdraw(self, *_args, **_kwargs):
            return {"orderId": "bg_ok"}

    w = app.BitgetOneToManyPage.__new__(app.BitgetOneToManyPage)
    w.root = RaisingAfterRoot()
    w.client = WithdrawClient()
    w.query_status = {}
    w.address_status = {}
    w.row_id_by_addr = {}
    w.row_index_map = {}
    w.is_running = True
    w.progress_var = DummyVar("进度：空闲")
    w._active_progress_kind = ""
    w._active_progress_keys = []
    w.log_messages = []
    w.log = lambda m: w.log_messages.append(m)

    params = app.WithdrawRuntimeParams(
        coin="USDT",
        amount="1.50",
        network="trc20",
        delay=0.0,
        threads=1,
    )
    with MessagePatch():
        w._run_withdraw(["addr_ok"], params, ("k", "s", "p"), dry_run=False)
    assert w.address_status["addr_ok"] == "success"
    assert w.is_running is False


def test_onchain_dry_run_does_not_call_rpc():
    class FailClient:
        NATIVE_GAS_LIMIT = 21000

        def __getattr__(self, _name):
            raise AssertionError("dry_run 不应触发链上 RPC 或签名发送")

    w = app.OnchainTransferPage.__new__(app.OnchainTransferPage)
    w.root = DummyRoot()
    w.client = FailClient()
    w.row_status = {}
    w.row_status_context = {}
    w.row_id_by_key = {}
    w.row_index_map = {}
    w.is_running = True
    w.progress_var = DummyVar("进度：空闲")
    w._active_progress_kind = ""
    w._active_progress_keys = []
    w._progress_amount_label = "转账总额"
    w._summary_balance_text = "-"
    w._summary_amount_text = "-"
    w._summary_gas_text = "-"
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
    assert "转账总额=-" in w.progress_var.get()


def test_onchain_retry_ignores_stale_one_to_many_status_after_source_change():
    w = app.OnchainTransferPage.__new__(app.OnchainTransferPage)
    w.store = app.OnchainStore(Path(tempfile.gettempdir()) / "noop_onchain_retry_context.json")
    w.store.one_to_many_addresses = ["0x1111111111111111111111111111111111111111"]
    w.mode_var = DummyVar(app.OnchainTransferPage.MODE_1M)
    w.source_credential_var = DummyVar("seed_old")
    w.target_address_var = DummyVar("")
    w.row_status = {"1m:0x1111111111111111111111111111111111111111": "failed"}
    w.row_status_context = {"1m:0x1111111111111111111111111111111111111111": "seed_old"}
    w.query_row_status = {}
    w.query_row_status_context = {}
    w.progress_var = DummyVar("进度：空闲")
    w._active_progress_kind = ""
    w._active_progress_keys = []
    w._progress_amount_label = "转账总额"
    w._summary_balance_text = "-"
    w._summary_amount_text = "-"
    w._summary_gas_text = "-"
    w.source_balance_var = DummyVar("-")
    w.target_balance_var = DummyVar("-")
    w.source_balance_cache = {}
    w.target_balance_cache = {}
    w.is_running = False
    w._refresh_tree = lambda: None
    w.client = type(
        "Client",
        (),
        {
            "validate_evm_address": staticmethod(lambda value, _label="收款地址": str(value).strip()),
            "is_address": staticmethod(lambda value: str(value).startswith("0x") and len(str(value)) == 42),
        },
    )()

    w.source_credential_var.set("seed_new")
    w._on_source_or_target_changed()

    assert w._collect_failed_jobs() == []


def test_erc20_send_supports_legacy_raw_transaction_attr():
    client = app.EvmClient()
    captured = {}

    class Signed:
        rawTransaction = bytes.fromhex("aabb")

    class FakeAccount:
        @staticmethod
        def sign_transaction(tx, private_key=None):
            captured["tx"] = dict(tx)
            captured["private_key"] = private_key
            return Signed()

    client._ensure_eth_account = lambda: FakeAccount
    client.get_chain_id = lambda _network: 56
    client._rpc_call = lambda network, method, params: (
        captured.update({"network": network, "method": method, "params": list(params)}),
        "0xtxhash",
    )[1]

    tx_hash = client.send_erc20_transfer(
        network="BSC",
        private_key="0x" + "1" * 64,
        token_contract="0x55d398326f99059ff775485246999027b3197955",
        to_address="0x1111111111111111111111111111111111111111",
        amount_units=123,
        nonce=5,
        gas_price_wei=100,
        gas_limit=70000,
    )

    assert tx_hash == "0xtxhash"
    assert captured["method"] == "eth_sendRawTransaction"
    assert captured["params"] == ["0xaabb"]
    assert captured["tx"]["data"].startswith("0xa9059cbb")
    assert captured["private_key"] == "0x" + "1" * 64


def test_onchain_transfer_logs_gas_and_total():
    class GasClient:
        def get_symbol(self, network: str) -> str:
            return {"ETH": "ETH", "BSC": "BNB"}.get(network, network)

        def get_nonce(self, _network: str, _address: str) -> int:
            return 0

        def send_native_transfer(self, **_kwargs):
            return "0xtxhash"

    w = app.OnchainTransferPage.__new__(app.OnchainTransferPage)
    w.root = DummyRoot()
    w.client = GasClient()
    w.row_status = {}
    w.query_row_status = {}
    w.row_id_by_key = {}
    w.row_index_map = {}
    w.is_running = True
    w.progress_var = DummyVar("进度：空闲")
    w._active_progress_kind = ""
    w._active_progress_keys = []
    w.log_messages = []
    w.log = lambda m: w.log_messages.append(m)
    w._mode = lambda: app.OnchainTransferPage.MODE_M2M
    w._token_desc_from_params = lambda _params: "ETH"
    w._mask = lambda value, head=8, tail=6: value
    w._mask_credential = lambda credential: credential
    w._resolve_wallet = lambda source: ("0x" + "1" * 64, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    w._resolve_amount_and_gas = lambda _params, _source_addr, _target: (123, 10_000_000_000, 21000, "0.01")

    jobs = [("row_1", "seed_1", "0x1111111111111111111111111111111111111111")]
    params = app.WithdrawRuntimeParams(
        coin="ETH",
        amount="0.01",
        network="ETH",
        delay=0.0,
        threads=1,
        token_is_native=True,
    )
    with MessagePatch():
        w._run_batch_transfer(jobs, params, dry_run=False)

    assert any(
        "金额=ETH 0.01" in x
        and "预估gas=0.00021 ETH" in x
        and "from=0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" in x
        and "to=0x1111111111111111111111111111111111111111" in x
        and "txid=0xtxhash" in x
        for x in w.log_messages
    )
    assert any("预估gas合计=0.00021 ETH" in x for x in w.log_messages)
    assert w.progress_var.get() == "转账完成：总1 | 成功1 | 失败0 | 余额总额=- | 转账总额=0.01 ETH | gas总额=预估 0.00021 ETH"


def test_onchain_transfer_without_txid_auto_fails():
    pair = app.OnchainPairEntry(source="seed_1", target="0x1111111111111111111111111111111111111111")
    row_key = app.OnchainTransferPage._m2m_key(pair)

    class PendingClient:
        def get_symbol(self, network: str) -> str:
            return {"ETH": "ETH", "BSC": "BNB"}.get(network, network)

        def is_address(self, value: str) -> bool:
            return str(value).startswith("0x") and len(str(value)) == 42

        def get_nonce(self, _network: str, _address: str) -> int:
            return 0

        def send_native_transfer(self, **_kwargs):
            return ""

    w = app.OnchainTransferPage.__new__(app.OnchainTransferPage)
    w.root = DummyRoot()
    w.client = PendingClient()
    w.submitted_timeout_seconds = 0.0
    w.store = app.OnchainStore(Path(tempfile.gettempdir()) / "noop_onchain_pending.json")
    w.store.multi_to_multi_pairs = [pair]
    w.row_status = {}
    w.row_status_context = {}
    w.query_row_status = {}
    w.query_row_status_context = {}
    w.row_id_by_key = {}
    w.row_index_map = {}
    w.is_running = True
    w.progress_var = DummyVar("进度：空闲")
    w._active_progress_kind = ""
    w._active_progress_keys = []
    w.log_messages = []
    w.log = lambda m: w.log_messages.append(m)
    w._mode = lambda: app.OnchainTransferPage.MODE_M2M
    w._token_desc_from_params = lambda _params: "ETH"
    w._mask = lambda value, head=8, tail=6: value
    w._mask_credential = lambda credential: credential
    w._resolve_wallet = lambda source: ("0x" + "1" * 64, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    w._resolve_amount_and_gas = lambda _params, _source_addr, _target: (123, 10_000_000_000, 21000, "0.01")

    jobs = [(row_key, pair.source, pair.target)]
    params = app.WithdrawRuntimeParams(
        coin="ETH",
        amount="0.01",
        network="ETH",
        delay=0.0,
        threads=1,
        token_is_native=True,
    )
    with MessagePatch():
        w._run_batch_transfer(jobs, params, dry_run=False)

    assert w.row_status[row_key] == "failed"
    assert any("未拿到 txid" in x for x in w.log_messages)
    assert any("自动判定失败" in x for x in w.log_messages)
    assert w._collect_failed_jobs() == jobs
    assert w.progress_var.get() == "转账完成：总1 | 成功0 | 失败1 | 余额总额=- | 转账总额=0 ETH | gas总额=预估 0 ETH"


def test_onchain_submitted_timeout_does_not_block_next_job():
    class PendingClient:
        def get_symbol(self, network: str) -> str:
            return {"ETH": "ETH", "BSC": "BNB"}.get(network, network)

        def is_address(self, value: str) -> bool:
            return str(value).startswith("0x") and len(str(value)) == 42

        def get_nonce(self, _network: str, _address: str) -> int:
            return 0

        def send_native_transfer(self, **_kwargs):
            return ""

    w = app.OnchainTransferPage.__new__(app.OnchainTransferPage)
    w.root = DummyRoot()
    w.client = PendingClient()
    w.submitted_timeout_seconds = 0.05
    w.store = app.OnchainStore(Path(tempfile.gettempdir()) / "noop_onchain_pending_queue.json")
    pairs = [
        app.OnchainPairEntry(source="seed_1", target="0x1111111111111111111111111111111111111111"),
        app.OnchainPairEntry(source="seed_2", target="0x2222222222222222222222222222222222222222"),
    ]
    w.store.multi_to_multi_pairs = list(pairs)
    w.row_status = {}
    w.row_status_context = {}
    w.query_row_status = {}
    w.query_row_status_context = {}
    w.row_id_by_key = {}
    w.row_index_map = {}
    w.is_running = True
    w.progress_var = DummyVar("进度：空闲")
    w._active_progress_kind = ""
    w._active_progress_keys = []
    w.log_messages = []
    w.log = lambda m: w.log_messages.append(m)
    w._mode = lambda: app.OnchainTransferPage.MODE_M2M
    w._token_desc_from_params = lambda _params: "ETH"
    w._mask = lambda value, head=8, tail=6: value
    w._mask_credential = lambda credential: credential
    w._resolve_wallet = lambda source: ("0x" + "1" * 64, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    w._resolve_amount_and_gas = lambda _params, _source_addr, _target: (123, 10_000_000_000, 21000, "0.01")

    jobs = [(app.OnchainTransferPage._m2m_key(pair), pair.source, pair.target) for pair in pairs]
    params = app.WithdrawRuntimeParams(
        coin="ETH",
        amount="0.01",
        network="ETH",
        delay=0.0,
        threads=1,
        token_is_native=True,
    )
    with MessagePatch():
        w._run_batch_transfer(jobs, params, dry_run=False)
    pending_logs = [i for i, text in enumerate(w.log_messages) if "未拿到 txid" in text]
    timeout_logs = [i for i, text in enumerate(w.log_messages) if "自动判定失败" in text]
    assert len(pending_logs) == 2
    assert len(timeout_logs) == 2
    assert max(pending_logs) < min(timeout_logs)


def test_onchain_transfer_finishes_when_root_after_fails():
    class GasClient:
        def get_symbol(self, network: str) -> str:
            return {"ETH": "ETH", "BSC": "BNB"}.get(network, network)

        def get_nonce(self, _network: str, _address: str) -> int:
            return 0

        def send_native_transfer(self, **_kwargs):
            return "0xtxhash"

    w = app.OnchainTransferPage.__new__(app.OnchainTransferPage)
    w.root = RaisingAfterRoot()
    w.client = GasClient()
    w.row_status = {}
    w.query_row_status = {}
    w.row_id_by_key = {}
    w.row_index_map = {}
    w.is_running = True
    w.progress_var = DummyVar("进度：空闲")
    w._active_progress_kind = ""
    w._active_progress_keys = []
    w.log_messages = []
    w.log = lambda m: w.log_messages.append(m)
    w._mode = lambda: app.OnchainTransferPage.MODE_M2M
    w._token_desc_from_params = lambda _params: "ETH"
    w._mask = lambda value, head=8, tail=6: value
    w._mask_credential = lambda credential: credential
    w._resolve_wallet = lambda source: ("0x" + "1" * 64, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    w._resolve_amount_and_gas = lambda _params, _source_addr, _target: (123, 10_000_000_000, 21000, "0.01")

    jobs = [("row_1", "seed_1", "0x1111111111111111111111111111111111111111")]
    params = app.WithdrawRuntimeParams(
        coin="ETH",
        amount="0.01",
        network="ETH",
        delay=0.0,
        threads=1,
        token_is_native=True,
    )
    with MessagePatch():
        w._run_batch_transfer(jobs, params, dry_run=False)
    assert w.row_status["row_1"] == "success"
    assert w.is_running is False


def test_onchain_batch_transfer_uses_params_threads():
    w = app.OnchainTransferPage.__new__(app.OnchainTransferPage)
    w.root = DummyRoot()
    w.row_status = {}
    w.row_id_by_key = {}
    w.row_index_map = {}
    w.is_running = True
    w.log_messages = []
    w.log = lambda m: w.log_messages.append(m)
    w._mode = lambda: app.OnchainTransferPage.MODE_M2M
    w._set_status = lambda row_key, status: w.row_status.__setitem__(row_key, status)
    w._token_desc_from_params = lambda _params: "ETH"
    w._mask = lambda value, head=8, tail=6: value
    w._mask_credential = lambda credential: credential

    jobs = [
        ("row_1", "seed_1", "addr_1"),
        ("row_2", "seed_2", "addr_2"),
        ("row_3", "seed_3", "addr_3"),
    ]
    params = app.WithdrawRuntimeParams(
        coin="ETH",
        amount="0.01",
        network="ETH",
        delay=0.0,
        threads=2,
        token_is_native=True,
    )
    with MessagePatch(), CountingThreadPatch(page_onchain) as tp:
        w._run_batch_transfer(jobs, params, dry_run=True)

    assert tp.created == 2
    assert w.row_status == {"row_1": "success", "row_2": "success", "row_3": "success"}
    assert w.is_running is False


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


def test_fetch_public_ip_skips_invalid_payload():
    class FakeResp:
        def __init__(self, text: str):
            self.text = text

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return self.text.encode("utf-8")

    responses = iter(["not-an-ip", "1.2.3.4"])
    orig_urlopen = ip_detection.urllib.request.urlopen
    ip_detection.urllib.request.urlopen = lambda _url, timeout=8: FakeResp(next(responses))
    try:
        assert ip_detection.fetch_public_ip() == "1.2.3.4"
    finally:
        ip_detection.urllib.request.urlopen = orig_urlopen


def test_apply_apple_theme_tolerates_style_failures():
    class BrokenStyle:
        def __init__(self, _root):
            pass

        def theme_names(self):
            return ["aqua"]

        def theme_use(self, _name):
            raise RuntimeError("theme failed")

        def configure(self, _style_name, **_kwargs):
            raise RuntimeError("configure failed")

        def map(self, _style_name, **_kwargs):
            raise RuntimeError("map failed")

    class BrokenRoot:
        def configure(self, **_kwargs):
            raise RuntimeError("root configure failed")

    orig_style = app.ttk.Style
    app.ttk.Style = BrokenStyle
    try:
        app._apply_apple_theme(BrokenRoot())
    finally:
        app.ttk.Style = orig_style


def test_main_startup_ignores_theme_failure():
    orig_tk = app.Tk
    orig_theme = app._apply_apple_theme
    orig_withdraw_app = app.WithdrawApp
    orig_data_dir = app.DATA_DIR

    started: list[object] = []
    fake_root = DummyMainloopRoot()
    temp_dir = Path(tempfile.mkdtemp()) / "data"

    app.Tk = lambda: fake_root
    app._apply_apple_theme = lambda _root: (_ for _ in ()).throw(RuntimeError("theme boom"))
    app.WithdrawApp = lambda root: started.append(root)
    app.DATA_DIR = temp_dir
    try:
        app.main()
    finally:
        app.Tk = orig_tk
        app._apply_apple_theme = orig_theme
        app.WithdrawApp = orig_withdraw_app
        app.DATA_DIR = orig_data_dir

    assert started == [fake_root]
    assert fake_root.mainloop_called is True
    assert temp_dir.exists()


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
        ("AccountStore missing file clears one-to-many", test_store_load_missing_file_clears_one_to_many_state),
        ("Bitget store roundtrip", test_bg_store_roundtrip),
        ("Bitget store encrypts sensitive", test_bg_store_encrypts_sensitive_fields),
        ("Onchain store roundtrip", test_onchain_store_roundtrip),
        ("Onchain store encrypts sensitive", test_onchain_store_encrypts_sensitive_fields),
        ("Import auto-select accounts", test_import_auto_select_accounts),
        ("Import auto-select addresses", test_import_auto_select_addresses),
        ("EVM address checksum validation", test_evm_validate_address_rejects_bad_checksum),
        ("Main empty hint updates by mode", test_main_empty_hint_updates_by_mode_and_data),
        ("Bitget empty hint hides after rows", test_bitget_empty_hint_hides_after_rows_exist),
        ("Onchain empty hint switches with mode", test_onchain_empty_hint_switches_with_mode),
        ("Delete falls back to checked accounts", test_delete_selected_falls_back_to_checked_accounts),
        ("Delete checked account drafts", test_delete_selected_removes_checked_account_drafts),
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
        ("Onchain delete checked drafts", test_onchain_delete_selected_removes_checked_drafts),
        ("Onchain query requires checked m2m", test_onchain_query_balance_requires_checked_rows_in_m2m),
        ("Onchain query uses checked m2m", test_onchain_query_balance_uses_checked_rows_in_m2m),
        ("Onchain query requires checked 1m", test_onchain_query_balance_requires_checked_rows_in_1m),
        ("Onchain query requires checked m1", test_onchain_query_balance_requires_checked_rows_in_m1),
        ("Onchain query 1m uses runtime threads", test_onchain_query_balance_one_to_many_uses_runtime_threads),
        ("Onchain query m1 uses runtime threads", test_onchain_query_balance_many_to_one_uses_runtime_threads),
        ("Onchain query m2m realtime row update", test_onchain_query_balance_updates_row_realtime_in_m2m),
        ("Onchain query 1m realtime row update", test_onchain_query_balance_updates_row_realtime_in_1m),
        ("Onchain retry ignores query failed status", test_onchain_collect_failed_jobs_ignores_query_failed_status),
        ("Onchain query progress label updates", test_onchain_query_progress_label_updates),
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
        ("Main save rejects invalid random", test_main_save_rejects_invalid_random_amount_range),
        ("Bitget save rejects invalid fixed amount", test_bitget_save_rejects_invalid_fixed_amount),
        ("Onchain save rejects invalid random", test_onchain_save_rejects_invalid_random_amount_range),
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
        ("Binance withdraw without withdrawId auto fails", test_binance_withdraw_without_withdraw_id_auto_fails),
        ("Binance submitted timeout does not block queue", test_binance_submitted_timeout_does_not_block_next_job),
        ("Binance withdraw survives after failure", test_binance_withdraw_finishes_when_root_after_fails),
        ("Binance query progress total", test_binance_query_progress_shows_balance_total),
        ("Binance dry run all no balance API", test_binance_dry_run_all_does_not_call_balance_api),
        ("Bitget dry run all no balance API", test_bitget_dry_run_all_does_not_call_balance_api),
        ("Bitget query progress total", test_bitget_query_progress_shows_balance_total),
        ("Bitget withdraw progress total", test_bitget_withdraw_progress_shows_amount_and_fee),
        ("Bitget withdraw without orderId auto fails", test_bitget_withdraw_without_order_id_auto_fails),
        ("Bitget submitted timeout does not block queue", test_bitget_submitted_timeout_does_not_block_next_job),
        ("Bitget withdraw survives after failure", test_bitget_withdraw_finishes_when_root_after_fails),
        ("Onchain dry run no rpc", test_onchain_dry_run_does_not_call_rpc),
        ("ERC20 send supports legacy raw attr", test_erc20_send_supports_legacy_raw_transaction_attr),
        ("Onchain transfer logs gas and total", test_onchain_transfer_logs_gas_and_total),
        ("Onchain transfer without txid auto fails", test_onchain_transfer_without_txid_auto_fails),
        ("Onchain submitted timeout does not block queue", test_onchain_submitted_timeout_does_not_block_next_job),
        ("Onchain transfer survives after failure", test_onchain_transfer_finishes_when_root_after_fails),
        ("Onchain transfer uses params threads", test_onchain_batch_transfer_uses_params_threads),
        ("IP detect single inflight", test_start_detect_ip_single_inflight),
        ("Fetch public IP skips invalid payload", test_fetch_public_ip_skips_invalid_payload),
        ("Apple theme tolerates style failures", test_apply_apple_theme_tolerates_style_failures),
        ("Main startup ignores theme failure", test_main_startup_ignores_theme_failure),
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
