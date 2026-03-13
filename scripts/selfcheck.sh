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

echo "[1/5] 语法检查"
if command -v rg >/dev/null 2>&1; then
  rg --files -g '*.py' -0 | xargs -0 "$PYTHON_BIN" -m py_compile
else
  find . -type f -name '*.py' -print0 | xargs -0 "$PYTHON_BIN" -m py_compile
fi

echo "[2/5] 运行环境检查"
"$PYTHON_BIN" - <<'PY'
import importlib.util
missing = [name for name in ("eth_account", "eth_utils") if importlib.util.find_spec(name) is None]
if missing:
    print(f"[WARN] 当前解释器缺少链上依赖：{', '.join(missing)}")
    print("[WARN] 可执行：python -m pip install -r requirements-runtime.txt")
else:
    print("[OK] 链上运行依赖已就绪")
PY

echo "[3/5] 运行冒烟测试"
"$PYTHON_BIN" tests/smoke_test.py

echo "[4/5] 运行 pytest"
"$PYTHON_BIN" -m pytest -q

echo "[5/5] 完成"
echo "自检通过：语法、冒烟、pytest 全部通过。"
