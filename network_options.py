#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import threading

from core_models import AccountEntry
from shared_utils import dispatch_ui_callback


def _clear_network_box(app) -> None:
    app.network_box.configure(values=[""])
    app.network_var.set("")


def _normalize_networks(networks: list[str], *, uppercase: bool, dedupe: bool) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in networks:
        text = str(item or "").strip()
        if uppercase:
            text = text.upper()
        if not text:
            continue
        if dedupe:
            marker = text
            if marker in seen:
                continue
            seen.add(marker)
        result.append(text)
    return result


def _apply_network_values(app, networks: list[str], *, uppercase_current: bool) -> None:
    values = [""] + list(networks)
    app.network_box.configure(values=values)
    current_network = app.network_var.get().strip()
    if uppercase_current:
        current_network = current_network.upper()
    if current_network and current_network in values:
        app.network_var.set(current_network)
    elif current_network and current_network not in values:
        app.network_var.set("")


def start_refresh_network_options(app) -> None:
    coin = app._current_coin()
    if not coin:
        _clear_network_box(app)
        return

    account = app._pick_meta_account()
    if not account:
        _clear_network_box(app)
        return

    account_key = account.api_key
    cache_key = f"{account_key}|{coin}"
    cached = app.coin_network_cache.get(cache_key)
    if cached is not None:
        app._apply_network_options(cache_key, account_key, coin, cached, cache_result=True)
        return

    # 币种切换后先清空旧网络，避免短暂展示错币种网络。
    _clear_network_box(app)
    threading.Thread(
        target=app._run_refresh_network_options,
        args=(account, account_key, coin, cache_key),
        daemon=True,
    ).start()


def run_refresh_network_options(app, account: AccountEntry, account_key: str, coin: str, cache_key: str) -> None:
    try:
        networks = app.client.get_coin_networks(account, coin=coin)
        dispatch_ui_callback(app, lambda k=cache_key, a=account_key, c=coin, n=networks: app._apply_network_options(k, a, c, n, cache_result=True))
    except Exception as exc:
        dispatch_ui_callback(app, lambda m=f"{coin} 网络读取失败：{exc}": app.log(m))
        fallback = [n for n in app.NETWORK_OPTIONS if n]
        dispatch_ui_callback(app, lambda k=cache_key, a=account_key, c=coin, n=fallback: app._apply_network_options(k, a, c, n, cache_result=False))


def apply_network_options(app, cache_key: str, account_key: str, coin: str, networks: list[str], cache_result: bool) -> None:
    arr = _normalize_networks(networks, uppercase=True, dedupe=True)
    if cache_result:
        app.coin_network_cache[cache_key] = arr
    if app._current_coin() != coin:
        return
    current = app._pick_meta_account()
    if not current or current.api_key != account_key:
        return
    _apply_network_values(app, arr, uppercase_current=True)


def start_coin_network_refresh(app) -> None:
    coin = app.coin_var.get().strip().upper() if hasattr(app, "coin_var") else ""
    if not coin:
        _clear_network_box(app)
        return
    cached = app.coin_network_cache.get(coin)
    if cached is not None:
        app._apply_network_options(coin, cached, cache_result=True)
        return
    _clear_network_box(app)
    threading.Thread(target=app._run_refresh_network_options, args=(coin,), daemon=True).start()


def run_coin_network_refresh(app, coin: str) -> None:
    try:
        networks = app.client.get_coin_networks(coin)
        dispatch_ui_callback(app, lambda c=coin, n=networks: app._apply_network_options(c, n, cache_result=True))
    except Exception as exc:
        dispatch_ui_callback(app, lambda m=f"{coin} 网络读取失败：{exc}": app.log(m))
        dispatch_ui_callback(app, lambda c=coin: app._apply_network_options(c, [], cache_result=False))


def apply_coin_network_options(app, coin: str, networks: list[str], cache_result: bool) -> None:
    arr = _normalize_networks(networks, uppercase=False, dedupe=False)
    if cache_result:
        app.coin_network_cache[coin] = arr
    current_coin = app.coin_var.get().strip().upper() if hasattr(app, "coin_var") else ""
    if current_coin != coin:
        return
    _apply_network_values(app, arr, uppercase_current=False)
