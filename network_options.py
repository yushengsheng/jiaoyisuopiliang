#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import threading

from core_models import AccountEntry


def start_refresh_network_options(app) -> None:
    coin = app._current_coin()
    if not coin:
        app.network_box.configure(values=[""])
        app.network_var.set("")
        return

    account = app._pick_meta_account()
    if not account:
        app.network_box.configure(values=[""])
        app.network_var.set("")
        return

    account_key = account.api_key
    cache_key = f"{account_key}|{coin}"
    cached = app.coin_network_cache.get(cache_key)
    if cached is not None:
        app._apply_network_options(cache_key, account_key, coin, cached, cache_result=True)
        return

    # 币种切换后先清空旧网络，避免短暂展示错币种网络。
    app.network_box.configure(values=[""])
    app.network_var.set("")
    threading.Thread(
        target=app._run_refresh_network_options,
        args=(account, account_key, coin, cache_key),
        daemon=True,
    ).start()


def run_refresh_network_options(app, account: AccountEntry, account_key: str, coin: str, cache_key: str) -> None:
    try:
        networks = app.client.get_coin_networks(account, coin=coin)
        app.root.after(
            0,
            lambda k=cache_key, a=account_key, c=coin, n=networks: app._apply_network_options(k, a, c, n, cache_result=True),
        )
    except Exception as exc:
        app.root.after(0, lambda m=f"{coin} 网络读取失败：{exc}": app.log(m))
        fallback = [n for n in app.NETWORK_OPTIONS if n]
        app.root.after(
            0,
            lambda k=cache_key, a=account_key, c=coin, n=fallback: app._apply_network_options(k, a, c, n, cache_result=False),
        )


def apply_network_options(app, cache_key: str, account_key: str, coin: str, networks: list[str], cache_result: bool) -> None:
    arr: list[str] = []
    seen: set[str] = set()
    for item in networks:
        s = str(item or "").strip().upper()
        if not s or s in seen:
            continue
        seen.add(s)
        arr.append(s)
    if cache_result:
        app.coin_network_cache[cache_key] = arr
    if app._current_coin() != coin:
        return
    current = app._pick_meta_account()
    if not current or current.api_key != account_key:
        return

    values = [""] + arr
    app.network_box.configure(values=values)
    current_network = app.network_var.get().strip().upper()
    if current_network and current_network in values:
        app.network_var.set(current_network)
    elif current_network and current_network not in values:
        app.network_var.set("")
