#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [ -x "$ROOT_DIR/.venv/bin/python" ]; then
  PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python3)"
else
  echo "[ERROR] 未找到可用 Python 解释器（.venv/bin/python 或 python3）" >&2
  exit 1
fi

echo "[1/4] 语法检查"
if command -v rg >/dev/null 2>&1; then
  rg --files -g '*.py' -0 | xargs -0 "$PYTHON_BIN" -m py_compile
else
  find . -type f -name '*.py' -print0 | xargs -0 "$PYTHON_BIN" -m py_compile
fi

echo "[2/4] 运行冒烟测试"
"$PYTHON_BIN" tests/smoke_test.py

echo "[3/4] 运行 pytest"
"$PYTHON_BIN" -m pytest -q

echo "[4/4] 完成"
echo "自检通过：语法、冒烟、pytest 全部通过。"
