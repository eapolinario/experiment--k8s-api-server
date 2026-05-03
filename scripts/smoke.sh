#!/usr/bin/env bash
# Smoke test: start apiserver, exercise kubectl, shut down cleanly.
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

RUN_DIR="$ROOT_DIR/run"
DATA_DIR="$ROOT_DIR/data"
PKI_DIR="$RUN_DIR/pki"
KUBECONFIG_PATH="$RUN_DIR/kubeconfig"
LOG_FILE="$RUN_DIR/apiserver.log"
BIN="$ROOT_DIR/bin/apiserver"
KCTL=(kubectl --kubeconfig "$KUBECONFIG_PATH")

fail=0

cleanup() {
  if [[ -n "${API_PID:-}" ]] && kill -0 "$API_PID" 2>/dev/null; then
    kill "$API_PID" 2>/dev/null || true
    for _ in {1..30}; do
      kill -0 "$API_PID" 2>/dev/null || break
      sleep 0.2
    done
    kill -0 "$API_PID" 2>/dev/null && kill -9 "$API_PID" || true
  fi
}
trap cleanup EXIT

# Clean up prior state.
rm -rf "$RUN_DIR" "$DATA_DIR"
mkdir -p "$RUN_DIR"

if [[ ! -x "$BIN" ]]; then
  echo "smoke: building apiserver"
  go build -o "$BIN" ./cmd/apiserver
fi

echo "smoke: starting apiserver"
"$BIN" --bind-address 127.0.0.1 --secure-port 6443 \
       --cert-dir "$PKI_DIR" --data-dir "$DATA_DIR" \
       --kubeconfig-out "$KUBECONFIG_PATH" \
       >"$LOG_FILE" 2>&1 &
API_PID=$!

# Wait for readyz.
ready=0
for i in {1..50}; do
  if curl -sk --cacert "$PKI_DIR/ca.crt" --max-time 2 \
       https://127.0.0.1:6443/readyz >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 0.2
done
if [[ "$ready" -ne 1 ]]; then
  echo "smoke: apiserver did not become ready"
  tail -50 "$LOG_FILE"
  exit 1
fi
echo "smoke: apiserver ready"

run_step() {
  local desc="$1"; shift
  echo "smoke: $desc"
  if ! "$@"; then
    echo "smoke: FAILED: $desc"
    fail=1
  fi
}

run_step "get namespaces (empty)" "${KCTL[@]}" get namespaces
run_step "create namespace demo" "${KCTL[@]}" create ns demo
run_step "apply nginx pod" "${KCTL[@]}" apply --validate=false -f examples/nginx.yaml
run_step "get pods -A"   "${KCTL[@]}" get pods -A

NGINX_FILE="$DATA_DIR/pods/default/nginx.json"
if [[ -f "$NGINX_FILE" ]]; then
  echo "smoke: on-disk pod found at $NGINX_FILE"
else
  echo "smoke: FAILED: missing pod file at $NGINX_FILE"
  fail=1
fi

if [[ "$fail" -ne 0 ]]; then
  echo "smoke: log tail:"
  tail -50 "$LOG_FILE"
  exit 1
fi

echo "smoke: all checks passed"
