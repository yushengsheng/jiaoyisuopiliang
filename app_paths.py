#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from pathlib import Path


APP_DIR = Path(__file__).resolve().parent
DATA_DIR = APP_DIR / "data"
DATA_FILE = DATA_DIR / "accounts.json"
BG_DATA_FILE = DATA_DIR / "bg_one_to_many.json"
ONCHAIN_DATA_FILE = DATA_DIR / "onchain.json"
SECRET_KEY_FILE = DATA_DIR / ".secret.key"
