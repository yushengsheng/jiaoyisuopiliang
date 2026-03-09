#!/bin/bash
set -u

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

# 可选：从 .env 读取自定义 EVM RPC（示例）
# EVM_RPC_BSC=https://your-bsc-rpc
# EVM_RPC_ETH=https://your-eth-rpc
if [ -f "$SCRIPT_DIR/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  source "$SCRIPT_DIR/.env"
  set +a
fi

# 按优先级选择 Python（优先 Homebrew 版本，避免系统 python3 的 Tk 崩溃问题）
CANDIDATES=(
  "/opt/homebrew/bin/python3.11"
  "/opt/homebrew/bin/python3.12"
  "/opt/homebrew/bin/python3.13"
  "/opt/homebrew/bin/python3"
  "$SCRIPT_DIR/.venv/bin/python"
  "python3"
)

probe_python() {
  local py="$1"
  "$py" - <<'PY' >/dev/null 2>&1
import tkinter
import main
print("ok")
PY
}

is_system_python3() {
  local py="$1"
  local exec_path
  local real_path
  exec_path="$("$py" - <<'PY' 2>/dev/null
import sys
print(sys.executable)
PY
)" || return 1
  real_path="$("$py" - <<'PY' 2>/dev/null
import os, sys
print(os.path.realpath(sys.executable))
PY
)" || return 1
  case "$exec_path:$real_path" in
    *"/usr/bin/python3"*|*"/Library/Developer/CommandLineTools/"*)
      return 0
      ;;
  esac
  return 1
}

LOG_DIR="$SCRIPT_DIR/data"
mkdir -p "$LOG_DIR"
PY_CACHE_FILE="$LOG_DIR/python_launcher_path.txt"

ALL_CANDIDATES=()
if [ -f "$PY_CACHE_FILE" ]; then
  cached_py="$(cat "$PY_CACHE_FILE" 2>/dev/null || true)"
  if [ -n "$cached_py" ]; then
    ALL_CANDIDATES+=("$cached_py")
  fi
fi
ALL_CANDIDATES+=("${CANDIDATES[@]}")

VALID_BINS=()
seen_bins=""
for p in "${ALL_CANDIDATES[@]}"; do
  if ! command -v "$p" >/dev/null 2>&1; then
    continue
  fi
  py_bin="$(command -v "$p")"
  if echo "|$seen_bins|" | grep -q "|$py_bin|"; then
    continue
  fi
  seen_bins="${seen_bins}|${py_bin}"
  if is_system_python3 "$py_bin"; then
    continue
  fi
  if probe_python "$py_bin"; then
    VALID_BINS+=("$py_bin")
  fi
done

if [ ${#VALID_BINS[@]} -eq 0 ]; then
  echo "[错误] 未找到可用 Python 运行环境（需可导入 tkinter 和 main.py）。"
  echo "说明：已自动跳过 /usr/bin/python3（该解释器在此机型会崩溃）"
  read -r -n 1 -s -p "按任意键退出..."
  echo
  exit 1
fi

echo "正在启动 Binance 批量提现工具..."
for PYTHON_BIN in "${VALID_BINS[@]}"; do
  echo "使用解释器：$PYTHON_BIN"
  "$PYTHON_BIN" "$SCRIPT_DIR/main.py"
  EXIT_CODE=$?
  if [ "$EXIT_CODE" -eq 0 ]; then
    echo "$PYTHON_BIN" >"$PY_CACHE_FILE"
    exit 0
  fi
  echo "解释器退出码：${EXIT_CODE}，尝试下一个解释器..."
done

echo
echo "程序异常退出：所有可用解释器均启动失败。"
read -r -n 1 -s -p "按任意键关闭窗口..."
echo
