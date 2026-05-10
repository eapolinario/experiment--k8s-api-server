#!/usr/bin/env bash
# Smoke test: start apiserver + kubelet-lite, exercise kubectl, verify a
# real Docker container is created and torn down, shut down cleanly.
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

RUN_DIR="$ROOT_DIR/run"
DATA_DIR="$ROOT_DIR/data"
PKI_DIR="$RUN_DIR/pki"
KUBECONFIG_PATH="$RUN_DIR/kubeconfig"
API_LOG="$RUN_DIR/apiserver.log"
KUBELET_LOG="$RUN_DIR/kubelet-lite.log"
APISERVER_BIN="$ROOT_DIR/bin/apiserver"
KUBELET_BIN="$ROOT_DIR/bin/kubelet-lite"
KCTL=(kubectl --kubeconfig "$KUBECONFIG_PATH")

fail=0

cleanup() {
  if [[ -n "${KUBELET_PID:-}" ]] && kill -0 "$KUBELET_PID" 2>/dev/null; then
    kill "$KUBELET_PID" 2>/dev/null || true
    for _ in {1..30}; do kill -0 "$KUBELET_PID" 2>/dev/null || break; sleep 0.2; done
    kill -0 "$KUBELET_PID" 2>/dev/null && kill -9 "$KUBELET_PID" || true
  fi
  if [[ -n "${API_PID:-}" ]] && kill -0 "$API_PID" 2>/dev/null; then
    kill "$API_PID" 2>/dev/null || true
    for _ in {1..30}; do kill -0 "$API_PID" 2>/dev/null || break; sleep 0.2; done
    kill -0 "$API_PID" 2>/dev/null && kill -9 "$API_PID" || true
  fi
  # Best-effort: leave no klite_* containers lingering even on failure.
  docker ps -aq --filter label=io.k8s.managed-by=kubelet-lite 2>/dev/null \
    | xargs -r docker rm -f >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Clean up prior state.
rm -rf "$RUN_DIR" "$DATA_DIR"
mkdir -p "$RUN_DIR"

# Reap any stale klite containers from a previous failed run.
docker ps -aq --filter label=io.k8s.managed-by=kubelet-lite 2>/dev/null \
  | xargs -r docker rm -f >/dev/null 2>&1 || true

if [[ ! -x "$APISERVER_BIN" || ! -x "$KUBELET_BIN" ]]; then
  echo "smoke: building binaries"
  go build -o "$APISERVER_BIN" ./cmd/apiserver
  go build -o "$KUBELET_BIN"   ./cmd/kubelet-lite
fi

echo "smoke: starting apiserver"
"$APISERVER_BIN" --bind-address 127.0.0.1 --secure-port 6443 \
       --cert-dir "$PKI_DIR" --data-dir "$DATA_DIR" \
       --kubeconfig-out "$KUBECONFIG_PATH" \
       >"$API_LOG" 2>&1 &
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
  tail -50 "$API_LOG"
  exit 1
fi
echo "smoke: apiserver ready"

echo "smoke: starting kubelet-lite"
"$KUBELET_BIN" --kubeconfig "$KUBECONFIG_PATH" --resync 30s \
       --node-name kubelet-lite -v=2 \
       >"$KUBELET_LOG" 2>&1 &
KUBELET_PID=$!

# Wait briefly for the kubelet to init and the informer to sync.
synced=0
for i in {1..50}; do
  if grep -q "informer synced" "$KUBELET_LOG" 2>/dev/null; then
    synced=1
    break
  fi
  if ! kill -0 "$KUBELET_PID" 2>/dev/null; then
    echo "smoke: kubelet-lite died early"
    tail -50 "$KUBELET_LOG"
    exit 1
  fi
  sleep 0.2
done
if [[ "$synced" -ne 1 ]]; then
  echo "smoke: kubelet-lite did not sync within timeout"
  tail -50 "$KUBELET_LOG"
  exit 1
fi
echo "smoke: kubelet-lite synced"

run_step() {
  local desc="$1"; shift
  echo "smoke: $desc"
  if ! "$@"; then
    echo "smoke: FAILED: $desc"
    fail=1
  fi
}

run_step "get namespaces (empty)" "${KCTL[@]}" get namespaces
run_step "create namespace demo" "${KCTL[@]}" create namespace demo
run_step "apply nginx pod"       "${KCTL[@]}" apply --validate=false -f examples/nginx.yaml
run_step "get pods -A"           "${KCTL[@]}" get pods -A

# Custom TableConvertor: kubectl get pods must render READY/STATUS columns,
# and the `po` short name must resolve.
if ! "${KCTL[@]}" get po -A 2>/dev/null | head -1 | grep -q 'READY'; then
  echo "smoke: FAILED: kubectl get po -A header missing READY column"
  fail=1
else
  echo "smoke: kubectl get po short name + columns OK"
fi
if ! "${KCTL[@]}" get ns 2>/dev/null | head -1 | grep -q 'STATUS'; then
  echo "smoke: FAILED: kubectl get ns header missing STATUS column"
  fail=1
fi

NGINX_FILE="$DATA_DIR/pods/default/nginx.json"
if [[ -f "$NGINX_FILE" ]]; then
  echo "smoke: on-disk pod found at $NGINX_FILE"
else
  echo "smoke: FAILED: missing pod file at $NGINX_FILE"
  fail=1
fi

# Wait up to 60s for status.phase=Running.
echo "smoke: waiting for pod nginx to reach Running"
phase=""
for i in {1..60}; do
  phase="$("${KCTL[@]}" get pod nginx -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  if [[ "$phase" == "Running" ]]; then break; fi
  sleep 1
done
if [[ "$phase" != "Running" ]]; then
  echo "smoke: FAILED: pod phase = '$phase' (expected Running)"
  echo "--- kubelet log tail ---"; tail -80 "$KUBELET_LOG"
  echo "--- apiserver log tail ---"; tail -40 "$API_LOG"
  fail=1
fi

POD_UID="$("${KCTL[@]}" get pod nginx -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
echo "smoke: pod UID=$POD_UID"

# Verify Docker container is running with the matching label.
container_count="$(docker ps --filter "label=io.k8s.pod.uid=$POD_UID" --format '{{.ID}}' | wc -l | tr -d ' ')"
if [[ "$container_count" == "1" ]]; then
  echo "smoke: docker container present (1 match for uid=$POD_UID)"
else
  echo "smoke: FAILED: expected 1 running container with uid=$POD_UID, got $container_count"
  docker ps --filter "label=io.k8s.pod.uid=$POD_UID"
  fail=1
fi

# Verify kubectl logs streams output from the container via the pods/log
# subresource (apiserver -> kubelet-lite log server -> docker logs).
echo "smoke: kubectl logs nginx"
logs_ok=0
for i in {1..15}; do
  if "${KCTL[@]}" logs nginx --tail=20 2>"$RUN_DIR/kubectl-logs.err" | grep -q .; then
    logs_ok=1
    break
  fi
  sleep 1
done
if [[ "$logs_ok" -ne 1 ]]; then
  echo "smoke: FAILED: kubectl logs nginx returned no output"
  cat "$RUN_DIR/kubectl-logs.err" || true
  fail=1
else
  echo "smoke: kubectl logs returned output"
fi

# Events: the kubelet should have posted at least Pulled/Created/Started
# during the pod-bringup phase. Assert they're visible via the apiserver's
# events resource (via both `kubectl get events` and the typed list path).
echo "smoke: kubectl get events"
ev_table="$("${KCTL[@]}" get events -n default --field-selector involvedObject.name=nginx 2>&1)"
echo "$ev_table"
for want_reason in Pulling Pulled Created Started; do
  if ! echo "$ev_table" | grep -q "$want_reason"; then
    echo "smoke: FAIL expected event reason '$want_reason' in events list"
    fail=1
  fi
done
if [[ $fail -eq 0 ]]; then
  echo "smoke: events Pulling/Pulled/Created/Started all present"
fi

# `kubectl describe pod` should surface the Events section.
if "${KCTL[@]}" describe pod nginx 2>/dev/null | grep -A20 '^Events:' | grep -qE 'Pulled|Started|Pulling'; then
  echo "smoke: kubectl describe pod nginx Events section populated"
else
  echo "smoke: FAIL kubectl describe pod nginx missing Events section"
  fail=1
fi

# Now delete the pod and assert teardown within 30s.
echo "smoke: deleting pod nginx (graceful, default 30s grace; expect Terminating phase first)"
"${KCTL[@]}" delete pod nginx --wait=false 2>&1 | head -1 || true

# Allow the apiserver a moment to set deletionTimestamp + emit the
# Modified watch event before we observe.
sleep 1
phase_status="$("${KCTL[@]}" get pod nginx --no-headers 2>/dev/null || true)"
if echo "$phase_status" | grep -q 'Terminating'; then
  echo "smoke: pod observed as Terminating during graceful delete"
else
  # Tight race — kubelet may have force-deleted already. Don't fail; just log.
  echo "smoke: (info) Terminating phase not observed: '$phase_status'"
fi

gone=0
for i in {1..30}; do
  remaining="$(docker ps -a --filter "label=io.k8s.pod.uid=$POD_UID" --format '{{.ID}}' | wc -l | tr -d ' ')"
  if [[ "$remaining" == "0" ]]; then
    gone=1
    break
  fi
  sleep 1
done
if [[ "$gone" -ne 1 ]]; then
  echo "smoke: FAILED: container with uid=$POD_UID still present after 30s"
  docker ps -a --filter "label=io.k8s.pod.uid=$POD_UID"
  fail=1
else
  echo "smoke: container torn down after pod delete"
fi

# Best-effort namespace cleanup.
"${KCTL[@]}" delete namespace demo >/dev/null 2>&1 || true

if [[ "$fail" -ne 0 ]]; then
  echo "--- apiserver log tail ---"; tail -50 "$API_LOG"
  echo "--- kubelet log tail ---";   tail -80 "$KUBELET_LOG"
  exit 1
fi

echo "smoke: all checks passed"
