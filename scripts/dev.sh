#!/usr/bin/env bash
# Harness for starting and stopping the apiserver + kubelet-lite locally.
#
# Subcommands:
#   up   — start both processes in the background, write PIDs to ./run/
#   down — stop background processes by PID, ignoring missing files

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RUN_DIR="$ROOT_DIR/run"
BIN_DIR="$ROOT_DIR/bin"
DATA_DIR="$ROOT_DIR/data"

APISERVER_BIN="$BIN_DIR/apiserver"
KUBELET_BIN="$BIN_DIR/kubelet-lite"

APISERVER_PID="$RUN_DIR/apiserver.pid"
KUBELET_PID="$RUN_DIR/kubelet-lite.pid"

APISERVER_LOG="$RUN_DIR/apiserver.log"
KUBELET_LOG="$RUN_DIR/kubelet-lite.log"

start_one() {
  local name="$1" bin="$2" pid_file="$3" log_file="$4"
  if [[ -f "$pid_file" ]] && kill -0 "$(<"$pid_file")" 2>/dev/null; then
    echo "$name already running (pid $(<"$pid_file"))"
    return 0
  fi
  echo "starting $name → $log_file"
  ( "$bin" >"$log_file" 2>&1 & echo $! >"$pid_file" )
}

stop_one() {
  local name="$1" pid_file="$2"
  [[ -f "$pid_file" ]] || { echo "$name not running"; return 0; }
  local pid; pid="$(<"$pid_file")"
  if kill -0 "$pid" 2>/dev/null; then
    echo "stopping $name (pid $pid)"
    kill "$pid" || true
    for _ in {1..20}; do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
    kill -0 "$pid" 2>/dev/null && kill -9 "$pid" || true
  fi
  rm -f "$pid_file"
}

case "${1:-}" in
  up)
    mkdir -p "$RUN_DIR" "$DATA_DIR"
    # TODO(harness): generate self-signed PKI + kubeconfig in $RUN_DIR if missing.
    # Will be filled in once the apiserver phase produces a working binary.
    start_one apiserver     "$APISERVER_BIN" "$APISERVER_PID" "$APISERVER_LOG"
    start_one kubelet-lite  "$KUBELET_BIN"   "$KUBELET_PID"   "$KUBELET_LOG"
    echo "use \`make logs\` to follow output, \`make down\` to stop"
    ;;
  down)
    stop_one kubelet-lite "$KUBELET_PID"
    stop_one apiserver    "$APISERVER_PID"
    ;;
  *)
    echo "usage: $0 {up|down}" >&2
    exit 2
    ;;
esac
