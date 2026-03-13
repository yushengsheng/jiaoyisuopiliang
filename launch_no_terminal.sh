#!/bin/bash
set -u

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

# 可选：读取 .env
if [ -f "$SCRIPT_DIR/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  source "$SCRIPT_DIR/.env"
  set +a
fi

CANDIDATES=(
  "$SCRIPT_DIR/.venv/bin/python"
  "/opt/homebrew/bin/python3.11"
  "/opt/homebrew/bin/python3.12"
  "/opt/homebrew/bin/python3.13"
  "/opt/homebrew/bin/python3"
  "python3"
)

show_error() {
  local msg="$1"
  if command -v osascript >/dev/null 2>&1; then
    osascript -e "display alert \"启动失败\" message \"$msg\" as critical" >/dev/null 2>&1 || true
  fi
  echo "[ERROR] $msg" >&2
}

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
LOG_FILE="$LOG_DIR/launch.log"
PY_CACHE_FILE="$LOG_DIR/python_launcher_path.txt"
: >"$LOG_FILE"
{
  echo "=== launch $(date '+%Y-%m-%d %H:%M:%S') ==="
} >>"$LOG_FILE"

if pgrep -f "$SCRIPT_DIR/main.py" >/dev/null 2>&1; then
  show_error "程序已在运行，无需重复启动。"
  exit 0
fi

ALL_CANDIDATES=("${CANDIDATES[@]}")
if [ -f "$PY_CACHE_FILE" ]; then
  cached_py="$(cat "$PY_CACHE_FILE" 2>/dev/null || true)"
  if [ -n "$cached_py" ]; then
    ALL_CANDIDATES+=("$cached_py")
  fi
fi

seen_bins=""
for p in "${ALL_CANDIDATES[@]}"; do
  if ! command -v "$p" >/dev/null 2>&1; then
    continue
  fi
  PYTHON_BIN="$(command -v "$p")"
  if echo "|$seen_bins|" | grep -q "|$PYTHON_BIN|"; then
    continue
  fi
  seen_bins="${seen_bins}|${PYTHON_BIN}"

  if is_system_python3 "$PYTHON_BIN"; then
    echo "[skip] $PYTHON_BIN (system python3 is not supported)" >>"$LOG_FILE"
    continue
  fi
  if ! probe_python "$PYTHON_BIN"; then
    echo "[skip] $PYTHON_BIN (probe failed)" >>"$LOG_FILE"
    continue
  fi

  echo "[try ] $PYTHON_BIN" >>"$LOG_FILE"
  nohup "$PYTHON_BIN" "$SCRIPT_DIR/main.py" >>"$LOG_FILE" 2>&1 &
  APP_PID=$!
  sleep 0.4
  if kill -0 "$APP_PID" >/dev/null 2>&1; then
    echo "[ok  ] pid=$APP_PID" >>"$LOG_FILE"
    echo "$PYTHON_BIN" >"$PY_CACHE_FILE"
    exit 0
  fi
  echo "[fail] $PYTHON_BIN (process exited quickly)" >>"$LOG_FILE"
done

show_error "程序启动失败，请查看日志：$LOG_FILE"
exit 1
