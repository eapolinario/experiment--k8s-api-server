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

PKI_DIR="$RUN_DIR/pki"
KUBECONFIG_PATH="$RUN_DIR/kubeconfig"

APISERVER_PID="$RUN_DIR/apiserver.pid"
KUBELET_PID="$RUN_DIR/kubelet-lite.pid"

APISERVER_LOG="$RUN_DIR/apiserver.log"
KUBELET_LOG="$RUN_DIR/kubelet-lite.log"

start_one() {
  local name="$1" pid_file="$2" log_file="$3"; shift 3
  if [[ -f "$pid_file" ]] && kill -0 "$(<"$pid_file")" 2>/dev/null; then
    echo "$name already running (pid $(<"$pid_file"))"
    return 0
  fi
  echo "starting $name → $log_file"
  ( "$@" >"$log_file" 2>&1 & echo $! >"$pid_file" )
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
    mkdir -p "$RUN_DIR" "$DATA_DIR" "$PKI_DIR"
    # The apiserver binary is responsible for creating the self-signed PKI
    # and kubeconfig on first run; we just pass explicit paths so the
    # Makefile-side tooling and the binary agree.
    start_one apiserver "$APISERVER_PID" "$APISERVER_LOG" \
      "$APISERVER_BIN" \
      --bind-address=127.0.0.1 \
      --secure-port=6443 \
      --cert-dir="$PKI_DIR" \
      --data-dir="$DATA_DIR" \
      --kubeconfig-out="$KUBECONFIG_PATH"
    start_one kubelet-lite "$KUBELET_PID" "$KUBELET_LOG" \
      "$KUBELET_BIN" \
      --kubeconfig="$KUBECONFIG_PATH" \
      --resync=30s \
      --node-name=kubelet-lite \
      -v=2
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
