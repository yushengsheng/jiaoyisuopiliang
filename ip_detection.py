#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import ipaddress
import threading
import urllib.request

from shared_utils import dispatch_ui_callback


def start_detect_ip(app) -> None:
    if not hasattr(app, "ip_detect_lock"):
        app.ip_detect_lock = threading.Lock()
    if not hasattr(app, "ip_detect_inflight"):
        app.ip_detect_inflight = False
    with app.ip_detect_lock:
        if app.ip_detect_inflight:
            return
        app.ip_detect_inflight = True
    app.ip_var.set("检测中...")
    if hasattr(app, "btn_detect_ip"):
        try:
            app.btn_detect_ip.configure(state="disabled")
        except Exception:
            pass
    threading.Thread(target=app._run_detect_ip, daemon=True).start()


def finish_detect_ip(app) -> None:
    if not hasattr(app, "ip_detect_lock"):
        app.ip_detect_lock = threading.Lock()
    with app.ip_detect_lock:
        app.ip_detect_inflight = False
    if hasattr(app, "btn_detect_ip"):
        try:
            app.btn_detect_ip.configure(state="normal")
        except Exception:
            pass


def fetch_public_ip() -> str:
    urls = [
        "https://api.ipify.org",
        "https://ifconfig.me/ip",
        "https://ipinfo.io/ip",
    ]
    for url in urls:
        try:
            with urllib.request.urlopen(url, timeout=8) as resp:
                ip = resp.read().decode("utf-8", errors="ignore").strip()
                if ip and len(ip) <= 64:
                    try:
                        ipaddress.ip_address(ip)
                    except ValueError:
                        continue
                    return ip
        except Exception:
            continue
    raise RuntimeError("无法获取公网IP")


def run_detect_ip(app) -> None:
    try:
        ip = fetch_public_ip()
        dispatch_ui_callback(app, lambda: app.ip_var.set(ip))
        dispatch_ui_callback(app, lambda: app.log(f"公网IP：{ip}"))
    except Exception as exc:
        err_text = str(exc)
        dispatch_ui_callback(app, lambda: app.ip_var.set("检测失败"))
        dispatch_ui_callback(app, lambda m=f"IP 检测失败：{err_text}": app.log(m))
    finally:
        dispatch_ui_callback(app, app._finish_detect_ip)
