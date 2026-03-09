#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import re
import threading
from pathlib import Path
from tkinter import END, messagebox

from app_paths import DATA_FILE
from core_models import AccountEntry
from ui_dialogs import PasteImportDialog


def refresh_tree(app) -> None:
    app.tree.delete(*app.tree.get_children())
    app.row_index_map = {}
    app.row_id_by_api_key = {}

    if app._is_one_to_many_mode():
        active_keys = set(app.store.one_to_many_addresses)
        app.checked_one_to_many_addresses.intersection_update(active_keys)
        app.account_withdraw_status = {k: v for k, v in app.account_withdraw_status.items() if k in active_keys}
        app.tree.heading("balance", text="地址余额")
        app._update_source_balance_display()
        for i, addr in enumerate(app.store.one_to_many_addresses, start=1):
            row_id = f"row_{i}"
            app.row_index_map[row_id] = i - 1
            app.row_id_by_api_key[addr] = row_id
            checked = "✓" if addr in app.checked_one_to_many_addresses else ""
            status_text = app._withdraw_status_text(addr)
            status_tag = app._withdraw_status_tag(app.account_withdraw_status.get(addr, ""))
            app.tree.insert(
                "",
                END,
                iid=row_id,
                values=(checked, i, "-", "-", addr, status_text, "-"),
                tags=(status_tag,) if status_tag else (),
            )
        return

    active_keys = {a.api_key for a in app.store.accounts}
    app.checked_api_keys.intersection_update(active_keys)
    app.account_coin_balance_cache = {k: v for k, v in app.account_coin_balance_cache.items() if k in active_keys}
    app.account_withdraw_status = {k: v for k, v in app.account_withdraw_status.items() if k in active_keys}
    app.tree.heading("balance", text="账号余额(全币种)")

    for i, acc in enumerate(app.store.accounts, start=1):
        row_id = f"row_{i}"
        app.row_index_map[row_id] = i - 1
        app.row_id_by_api_key[acc.api_key] = row_id
        checked = "✓" if acc.api_key in app.checked_api_keys else ""
        status_text = app._withdraw_status_text(acc.api_key)
        bal_text = app._all_balances_text(acc.api_key)
        status_tag = app._withdraw_status_tag(app.account_withdraw_status.get(acc.api_key, ""))
        app.tree.insert(
            "",
            END,
            iid=row_id,
            values=(checked, i, app._mask(acc.api_key), app._mask(acc.api_secret), acc.address, status_text, bal_text),
            tags=(status_tag,) if status_tag else (),
        )


def selected_indices(app) -> list[int]:
    idxs: list[int] = []
    for row_id in app.tree.selection():
        if row_id in app.row_index_map:
            idxs.append(app.row_index_map[row_id])
    return sorted(set(idxs))


def selected_accounts(app) -> list[AccountEntry]:
    idxs = app._selected_indices()
    return [app.store.accounts[i] for i in idxs if 0 <= i < len(app.store.accounts)]


def checked_indices(app) -> list[int]:
    idxs: list[int] = []
    for i, acc in enumerate(app.store.accounts):
        if acc.api_key in app.checked_api_keys:
            idxs.append(i)
    return idxs


def checked_accounts(app) -> list[AccountEntry]:
    idxs = app._checked_indices()
    return [app.store.accounts[i] for i in idxs if 0 <= i < len(app.store.accounts)]


def checked_one_to_many_addresses(app) -> list[str]:
    arr: list[str] = []
    for addr in app.store.one_to_many_addresses:
        if addr in app.checked_one_to_many_addresses:
            arr.append(addr)
    return arr


def failed_accounts_for_retry(app) -> list[AccountEntry]:
    if app._is_one_to_many_mode():
        source = app._build_source_account(with_message=True)
        if not source:
            return []
        failed_addrs: list[str] = []
        for addr in app.store.one_to_many_addresses:
            if app.account_withdraw_status.get(addr, "") == "failed":
                failed_addrs.append(addr)
        return [AccountEntry(api_key=source.api_key, api_secret=source.api_secret, address=addr) for addr in failed_addrs]

    arr: list[AccountEntry] = []
    for acc in app.store.accounts:
        if app.account_withdraw_status.get(acc.api_key, "") == "failed":
            arr.append(acc)
    return arr


def on_tree_click(app, event):
    if app.tree.identify("region", event.x, event.y) != "cell":
        return None
    row_id = app.tree.identify_row(event.y)
    if not row_id:
        return None
    idx = app.row_index_map.get(row_id)
    if idx is None or not (0 <= idx < len(app.store.accounts)):
        if not app._is_one_to_many_mode():
            return "break"

    if app._is_one_to_many_mode():
        if not (0 <= idx < len(app.store.one_to_many_addresses)):
            return "break"
        key = app.store.one_to_many_addresses[idx]
        checked = key not in app.checked_one_to_many_addresses
        if checked:
            app.checked_one_to_many_addresses.add(key)
        else:
            app.checked_one_to_many_addresses.discard(key)
    else:
        if not (0 <= idx < len(app.store.accounts)):
            return "break"
        key = app.store.accounts[idx].api_key
        checked = key not in app.checked_api_keys
        if checked:
            app.checked_api_keys.add(key)
        else:
            app.checked_api_keys.discard(key)

    values = list(app.tree.item(row_id, "values"))
    if values:
        values[0] = "✓" if checked else ""
        app.tree.item(row_id, values=values)
    return None


def on_tree_right_click(app, event):
    if app.tree.identify("region", event.x, event.y) != "cell":
        return None
    row_id = app.tree.identify_row(event.y)
    if not row_id:
        return None
    app.tree.selection_set(row_id)
    app.tree.focus(row_id)
    menu = getattr(app, "row_menu", None)
    if menu is not None:
        try:
            menu.tk_popup(event.x_root, event.y_root)
        finally:
            try:
                menu.grab_release()
            except Exception:
                pass
    return "break"


def toggle_check_all(app) -> None:
    if app._is_one_to_many_mode():
        if not app.store.one_to_many_addresses:
            messagebox.showwarning("提示", "暂无提现地址")
            return
        all_keys = set(app.store.one_to_many_addresses)
        if app.checked_one_to_many_addresses == all_keys:
            app.checked_one_to_many_addresses.clear()
            app.log("已取消全选")
        else:
            app.checked_one_to_many_addresses = set(all_keys)
            app.log(f"已全选 {len(all_keys)} 个地址")
        app._refresh_tree()
        return

    if not app.store.accounts:
        messagebox.showwarning("提示", "暂无账号")
        return
    all_keys = {a.api_key for a in app.store.accounts}
    if app.checked_api_keys == all_keys:
        app.checked_api_keys.clear()
        app.log("已取消全选")
    else:
        app.checked_api_keys = set(all_keys)
        app.log(f"已全选 {len(all_keys)} 个账号")
    app._refresh_tree()


def parse_account_lines(lines: list[str]) -> list[AccountEntry]:
    rows: list[AccountEntry] = []
    for i, line in enumerate(lines, start=1):
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        arr = [x for x in re.split(r"[\s,;]+", s) if x]
        if len(arr) < 3:
            raise RuntimeError(f"第 {i} 行格式错误：至少需要 3 列（api_key api_secret 提现地址）")
        rows.append(AccountEntry(api_key=arr[0], api_secret=arr[1], address=arr[2]))
    return rows


def parse_address_lines(lines: list[str]) -> list[str]:
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


def import_rows(app, rows: list[AccountEntry] | list[str], source: str) -> None:
    if not rows:
        messagebox.showwarning("提示", "没有可导入的数据")
        return
    if app._is_one_to_many_mode():
        old = set(app.store.one_to_many_addresses)
        created = 0
        for x in rows:
            addr = str(x).strip()
            if not addr or addr in old:
                continue
            app.store.one_to_many_addresses.append(addr)
            old.add(addr)
            created += 1
        app.checked_one_to_many_addresses = set(app.store.one_to_many_addresses)
        app._refresh_tree()
        app.log(f"{source}导入完成：新增地址 {created}，已自动全选 {len(app.store.one_to_many_addresses)} 个地址")
        return

    created, updated = app.store.upsert_many(rows)  # type: ignore[arg-type]
    app.checked_api_keys = {a.api_key for a in app.store.accounts}
    app._refresh_tree()
    app.start_refresh_network_options()
    app.log(f"{source}导入完成：新增 {created}，更新 {updated}，已自动全选 {len(app.store.accounts)} 个账号")


def import_from_paste(app) -> None:
    dialog = PasteImportDialog(app.root, one_to_many=app._is_one_to_many_mode())
    app.root.wait_window(dialog.top)
    if not dialog.result:
        return
    app._import_rows(dialog.result, "粘贴")


def on_tree_paste(app, _event=None):
    app.import_from_clipboard()
    return "break"


def import_from_clipboard(app) -> None:
    try:
        text = app.root.clipboard_get()
    except Exception:
        messagebox.showwarning("提示", "剪贴板为空或不可读取")
        return

    try:
        if app._is_one_to_many_mode():
            rows = app._parse_address_lines(text.splitlines())
        else:
            rows = app._parse_account_lines(text.splitlines())
        app._import_rows(rows, "剪贴板")
    except Exception as exc:
        messagebox.showerror("粘贴失败", str(exc))


def import_txt(app) -> None:
    from tkinter import filedialog

    path = filedialog.askopenfilename(title="选择 TXT", filetypes=[("Text", "*.txt"), ("All Files", "*.*")])
    if not path:
        return

    try:
        content = Path(path).read_text(encoding="utf-8")
    except UnicodeDecodeError:
        content = Path(path).read_text(encoding="utf-8-sig")

    try:
        if app._is_one_to_many_mode():
            rows = app._parse_address_lines(content.splitlines())
        else:
            rows = app._parse_account_lines(content.splitlines())
        app._import_rows(rows, "TXT")
    except Exception as exc:
        messagebox.showerror("导入失败", str(exc))


def export_txt(app) -> None:
    from tkinter import filedialog

    path = filedialog.asksaveasfilename(title="导出 TXT", defaultextension=".txt", filetypes=[("Text", "*.txt")])
    if not path:
        return

    try:
        if app._is_one_to_many_mode():
            lines = list(app.store.one_to_many_addresses)
            count = len(lines)
        else:
            lines = [f"{a.api_key} {a.api_secret} {a.address}" for a in app.store.accounts]
            count = len(lines)
        Path(path).write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
        app.log(f"TXT 导出完成：{path}")
        messagebox.showinfo("导出完成", f"已导出 {count} 条")
    except Exception as exc:
        messagebox.showerror("导出失败", str(exc))


def delete_selected(app) -> None:
    if app._is_one_to_many_mode():
        checked_addresses = set(app._checked_one_to_many_addresses())
        idxs = [i for i, addr in enumerate(app.store.one_to_many_addresses) if addr in checked_addresses]
    else:
        idxs = app._checked_indices()
    if not idxs:
        tip = "请先勾选要删除的地址" if app._is_one_to_many_mode() else "请先勾选要删除的账号"
        messagebox.showwarning("提示", tip)
        return

    if app._is_one_to_many_mode():
        if not messagebox.askyesno("确认删除", f"确认删除 {len(idxs)} 条地址吗？"):
            return
        app.store.delete_addresses_by_indices(idxs)
        app._refresh_tree()
        app.log(f"已删除 {len(idxs)} 条地址")
        return

    if not messagebox.askyesno("确认删除", f"确认删除 {len(idxs)} 条账号吗？"):
        return
    app.store.delete_by_indices(idxs)
    app._refresh_tree()
    app.start_refresh_network_options()
    app.log(f"已删除 {len(idxs)} 条账号")


def _single_selected_index(app) -> int | None:
    idxs = app._selected_indices()
    if not idxs:
        return None
    return idxs[0]


def _selected_row_accounts(app) -> list[AccountEntry] | None:
    idx = _single_selected_index(app)
    if idx is None:
        return None
    if app._is_one_to_many_mode():
        if not (0 <= idx < len(app.store.one_to_many_addresses)):
            return None
        source = app._build_source_account(with_message=True)
        if not source:
            return None
        addr = app.store.one_to_many_addresses[idx]
        return [AccountEntry(api_key=source.api_key, api_secret=source.api_secret, address=addr)]
    if not (0 <= idx < len(app.store.accounts)):
        return None
    return [app.store.accounts[idx]]


def start_query_balance_current_row(app) -> None:
    if app.is_running:
        messagebox.showwarning("提示", "已有任务在运行")
        return
    coin = app.coin_var.get().strip().upper()
    if not coin:
        messagebox.showerror("参数错误", "请先填写币种（用于查询余额）")
        return
    accounts = _selected_row_accounts(app)
    if not accounts:
        tip = "请先右键选中一条地址" if app._is_one_to_many_mode() else "请先右键选中一条账号"
        messagebox.showwarning("提示", tip)
        return
    app.is_running = True
    threading.Thread(target=app._run_query_balance, args=(accounts, coin), daemon=True).start()


def start_withdraw_current_row(app) -> None:
    if app.is_running:
        messagebox.showwarning("提示", "已有任务在运行")
        return
    params = app._validate_withdraw_params()
    if not params:
        return
    dry_run = bool(app.dry_run_var.get())
    one_to_many = app._is_one_to_many_mode()
    accounts = _selected_row_accounts(app)
    if not accounts:
        tip = "请先右键选中一条地址" if one_to_many else "请先右键选中一条账号"
        messagebox.showwarning("提示", tip)
        return

    mode_label = "1对多" if one_to_many else "多对多"
    count_label = "地址数：1（右键当前行）" if one_to_many else "账号数：1（右键当前行）"
    if not dry_run:
        amount_text = params.amount if not app._is_amount_all(params.amount) else f"{app.AMOUNT_ALL_LABEL}(按账号可用余额)"
        if params.random_enabled and params.random_min is not None and params.random_max is not None:
            amount_text = f"随机 {params.random_min:.2f} ~ {params.random_max:.2f}"
        text = (
            f"即将执行真实提现：\n"
            f"模式：{mode_label}\n"
            f"{count_label}\n"
            f"币种：{params.coin}\n"
            f"数量：{amount_text}\n"
            f"执行间隔：{params.delay} 秒\n"
            f"执行线程数：{params.threads}\n"
            f"网络：{params.network}\n\n"
            "确认继续？"
        )
        if not messagebox.askyesno("高风险确认", text):
            app.log("用户取消了当前行真实提现")
            return

    app.is_running = True
    threading.Thread(
        target=app._run_withdraw,
        args=(accounts, params, dry_run, one_to_many),
        daemon=True,
    ).start()


def delete_current_row(app) -> None:
    idx = _single_selected_index(app)
    if idx is None:
        tip = "请先右键选中一条地址" if app._is_one_to_many_mode() else "请先右键选中一条账号"
        messagebox.showwarning("提示", tip)
        return

    if app._is_one_to_many_mode():
        if not (0 <= idx < len(app.store.one_to_many_addresses)):
            messagebox.showwarning("提示", "请先右键选中一条地址")
            return
        if not messagebox.askyesno("确认删除", "确认删除当前地址吗？"):
            return
        app.store.delete_addresses_by_indices([idx])
        app._refresh_tree()
        app.log("已删除当前地址")
        return

    if not (0 <= idx < len(app.store.accounts)):
        messagebox.showwarning("提示", "请先右键选中一条账号")
        return
    if not messagebox.askyesno("确认删除", "确认删除当前账号吗？"):
        return
    app.store.delete_by_indices([idx])
    app._refresh_tree()
    app.start_refresh_network_options()
    app.log("已删除当前账号")


def save_all(app) -> None:
    if not app._apply_settings_to_store():
        return
    try:
        app.store.save()
        app.log("配置已保存")
        messagebox.showinfo("成功", f"配置已保存到：{DATA_FILE}")
    except Exception as exc:
        messagebox.showerror("保存失败", str(exc))
