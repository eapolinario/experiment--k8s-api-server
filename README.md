# experiment--k8s-api-server

A minimal Kubernetes API server backed by the local filesystem (no etcd, no
[kine][kine]) plus a tiny kubelet that runs Pods as real Docker containers.

This is a learning experiment. The whole stack — apiserver, storage,
"kubelet" — is small enough to read in one sitting and lets you do this:

```bash
$ make up
$ kubectl --kubeconfig run/kubeconfig apply -f examples/nginx.yaml
pod/nginx created

$ kubectl --kubeconfig run/kubeconfig get pods -A
NAMESPACE   NAME    READY   STATUS    RESTARTS   AGE
default     nginx   1/1     Running   0          3s

$ docker ps --filter label=io.k8s.managed-by=kubelet-lite
CONTAINER ID   IMAGE                COMMAND                  STATUS
ab12cd34ef56   nginx:1.27-alpine    "/docker-entrypoint.…"   Up 3 seconds

$ make data
data
├── namespaces
│   └── default.json
├── pods
│   └── default
│       └── nginx.json
└── .rv

$ kubectl --kubeconfig run/kubeconfig delete pod nginx
$ docker ps --filter label=io.k8s.managed-by=kubelet-lite
CONTAINER ID   IMAGE   COMMAND   STATUS    # ← gone
```

…with **a real `nginx` container running on your host** the entire time.

## Prerequisites

- [Nix](https://nixos.org/) (with flakes) — provides Go, kubectl, docker
  CLI, and everything else.
- A running Docker daemon. The user running the experiment must be in the
  `docker` group with a fresh login. Verify with `docker info`.

That's it — no Kubernetes installation, no kind/k3s/minikube.

## Quick start

```bash
nix develop          # enter dev shell (Go 1.25, kubectl 1.35, docker, …)
make build           # compile cmd/apiserver and cmd/kubelet-lite
make up              # start both in background (./run/*.{log,pid})
make demo            # kubectl apply -f examples/nginx.yaml
make data            # tree ./data — the on-disk "etcd"
make logs            # tail -F both processes' logs
make down            # stop everything
make clean           # also wipe ./bin ./run ./data
```

The `make up` step generates a self-signed CA + serving cert into
`./run/pki/` and a kubeconfig at `./run/kubeconfig` on first run. The
kubeconfig already points at `https://127.0.0.1:6443` and is preconfigured
with `Username: anonymous` so kubectl never prompts.

## Make targets

| Target       | What it does                                           |
| ------------ | ------------------------------------------------------ |
| `make build` | Build `bin/apiserver` and `bin/kubelet-lite`           |
| `make up`    | Start both binaries in the background                  |
| `make down`  | Stop them by saved PID                                 |
| `make logs`  | `tail -F` both log files                               |
| `make data`  | `tree ./data` — every object on disk as JSON           |
| `make demo`  | `kubectl apply` an nginx Pod and `get pods -A`         |
| `make smoke` | Full E2E: start, apply, assert container, delete, stop |
| `make test`  | `go test ./...` (unit tests only — no Docker required) |
| `make tidy`  | `go mod tidy`                                          |
| `make lint`  | `golangci-lint run`                                    |
| `make clean` | `make down` + remove `bin/`, `run/`, `data/`           |

## Architecture

```
                 kubectl
                   │ HTTPS to 127.0.0.1:6443
                   ▼
    ┌─────────────────────────────┐         ┌────────────────────┐
    │           apiserver         │◄────────│    kubelet-lite    │
    │     (k8s.io/apiserver)      │  watch  │    (client-go)     │
    │   ├─ /api/v1/namespaces     │  pods   │         │          │
    │   ├─ /api/v1/pods           │────────►│         ▼          │
    │   ├─ /api/v1/pods/.../status│  PATCH  │     Docker SDK     │
    │   └─ fsstorage              │  status │         │          │
    └────────────┬────────────────┘         └─────────┼──────────┘
                 ▼                                    ▼
          ./data/*.json                       Docker containers
```

### `internal/fsstorage`

Implements `k8s.io/apiserver/pkg/storage.Interface` against plain JSON
files on disk. Layout is `<root>/<resource>/<namespace>/<name>.json` for
namespaced kinds, `<root>/<resource>/<name>.json` for cluster-scoped.

- **resourceVersion**: a single monotonic `uint64` shared across all
  resources, persisted to `<root>/.rv`. The shared `Counter` lives in
  `counter.go` and is injected into every `Store` instance — without
  this, two stores rooted at the same dir would clobber each other's RVs.
- **Watch**: served from an in-memory ring buffer of recent events
  (256 per resource). Clients with `resourceVersion=0` get a synthetic
  list snapshot then live updates; clients asking for a specific RV
  beyond the buffer get `Gone`.
- **Atomic writes**: every mutation is a write-to-`*.tmp` + `rename`,
  guarded by a per-key `sync.Mutex`.

### `cmd/apiserver` + `internal/apiserver`

Builds a generic apiserver with `genericserver.RecommendedConfig`:

- Serving HTTPS via `SecureServingOptions.WithLoopback`
- Authn = `AnonymousAuthenticator`
- Authz = `AlwaysAllowAuthorizer`
- Registered REST: `Pod`, `Pod/status` subresource, `Pod/log` subresource
  (a `rest.Connecter` that proxies streaming GETs to kubelet-lite),
  `Namespace`, `Event`, `ConfigMap`, `Secret` — all under `/api/v1`, all
  backed by `fsstorage`. Custom `rest.TableConvertor` impls render the
  columns kubectl users expect (`READY`/`STATUS`/`RESTARTS`/`AGE` for
  pods; `STATUS`/`AGE` for ns; `LAST SEEN`/`TYPE`/`REASON`/`OBJECT`/
  `MESSAGE` for events; `NAME`/`DATA`/`AGE` for cm; `NAME`/`TYPE`/`DATA`/
  `AGE` for secrets). Short names: `po`, `ns`, `ev`, `cm`. The Secret
  strategy folds `stringData` into `data` on create + update, matching
  upstream behaviour. Per-resource field-selector allowlists (e.g.
  `involvedObject.name` for Events, `type` for Secrets) are wired via
  `scheme.AddFieldLabelConversionFunc` so `kubectl describe pod` works.
- OpenAPI v2 disabled (vendoring `pkg/generated/openapi` would more than
  double the binary), v3 stubbed in `openapi.go` with empty schemas. This
  is why `kubectl apply` requires `--validate=false`.

### `cmd/kubelet-lite` + `internal/kubelet`

A SharedIndexInformer-driven reconciler with one worker:

- For every Pod, ensure exactly one Docker container exists named
  `klite_<pod-uid>` and labeled with `io.k8s.pod.{uid,namespace,name}` and
  `io.k8s.managed-by=kubelet-lite`.
- Pure-function spec→Docker config translation in
  `internal/kubelet/translate.go` (independent of the SDK, fully unit
  tested).
- Status is PATCHed back through the apiserver via the `/status`
  subresource so it survives the `objectStrategy` strip.
- An orphan reaper sweeps containers whose Pod no longer exists.
- A small HTTP server on `127.0.0.1:10350` exposes
  `GET /containerLogs/{ns}/{name}` so the apiserver's `pods/log`
  subresource can stream `docker logs` output back to `kubectl logs`.
- A minimal event recorder posts `corev1.Event` objects (one per
  occurrence — we don't aggregate into series) for the upstream-kubelet
  reasons `Pulling`/`Pulled`/`Failed`/`Created`/`Started`/`Killing`,
  visible to `kubectl get events` and `kubectl describe pod`.
- Resolves Pod `env` / `envFrom` and `configMap` / `secret` volume
  sources by fetching the referenced objects from the apiserver. Volume
  contents are materialised under `--volume-root` (one dir per
  `(podUID, volumeName)`) and bind-mounted read-only into the container.
  Stale projection dirs are removed when the pod is reaped. Other volume
  types (emptyDir, hostPath, projected, ...) are skipped with a warning.

### Examples

- `examples/nginx.yaml` — long-running container, lands in `Running`.
- `examples/sleep.yaml` — `busybox sleep 30`, watch it transition to
  `Succeeded` and the container be reaped.

## What real etcd / kube-apiserver do that we *don't*

This list is the actual point of the experiment — it's the difference
between "real Kubernetes" and "the smallest thing that still says
`Running`":

- **MVCC.** etcd keeps the entire revision history; you can watch from
  any past revision. We keep the last 256 events per resource and answer
  `Gone` for older RVs.
- **Compaction.** Real etcd periodically compacts old revisions. We
  never accumulate them in the first place.
- **Leases & TTL.** Used by Endpoints/Lease objects; we don't implement
  Lease and have no TTL machinery.
- **Watch bookmarks.** We emit the *initial-events-end* `Bookmark` after
  the snapshot phase of a watch (so client-go's `WatchListClient`
  streaming-list path terminates), but we don't emit the periodic
  progress-notify Bookmarks real etcd-backed apiservers send during
  quiet periods.
- **Strategic merge / apply.** We accept JSON merge patches (good enough
  for our `/status` updates) and rely on `kubectl apply --validate=false`
  rather than serving a full OpenAPI schema for client-side three-way
  merges.
- **Admission.** No mutating or validating admission, no webhook hooks,
  no defaulting beyond what registry strategies bake in.
- **RBAC.** Everyone is `system:anonymous` with `AlwaysAllow`.
- **Scheduler.** There isn't one. `spec.nodeName` is ignored — every Pod
  belongs to our single kubelet.
- **Garbage collection.** No GC controller; no owner-ref cascading. We
  leave `metadata.ownerReferences` untouched but never act on them.
- **Finalizers.** We don't run finalizer arrays. We do implement the
  two-phase graceful delete dance: a DELETE on a Pod sets
  `metadata.deletionTimestamp` + `metadata.deletionGracePeriodSeconds`
  (defaulted from `spec.terminationGracePeriodSeconds`, fallback 30s)
  and returns 200 without removing the object. `kubelet-lite` observes
  the resulting Modified event, stops the container honoring the grace
  period, then issues a force-DELETE (`gracePeriodSeconds=0`) so the
  apiserver removes the object and a watch DELETE fires. `kubectl get
  pod` shows `STATUS=Terminating` while termination is in flight.
- **Eventual consistency between resources.** Real apiserver fans an event
  out across many watch caches, indexers, and admission paths. We do one
  write to disk and one broadcast.

## v1 limits (and why)

- **Single container per Pod.** Multi-container support means caring about
  pod sandbox/pause containers, shared network namespaces, init order,
  and probes. Pods with `len(spec.containers) != 1` are accepted but the
  kubelet marks them `Failed` with a clear message.
- **No volumes, probes, init containers, security context, or ports.**
- **No network plumbing.** Containers run on Docker's default bridge.
  `status.podIP` is hardcoded to a placeholder.
- **No Service / Endpoints / DNS / kube-proxy / CNI.**
- **No CRDs, aggregation layer, or webhook admission.**
- **`kubectl logs` works** — the apiserver registers a `pods/log`
  subresource that proxies to a tiny HTTP server in `kubelet-lite`
  (`/containerLogs/{ns}/{name}`) which in turn shells out to
  `docker logs`. Streaming + `--follow` + `--tail` + `--timestamps`
  + `--since` are all forwarded.
- **HA is not a goal.** A single apiserver process owns its `data/`
  directory exclusively.

## Testing

```bash
make test                                    # unit tests, no Docker required
nix develop --command go test ./... -race    # same, with the race detector
bash scripts/smoke.sh                        # full E2E with Docker; ~10s
```

The smoke test starts both processes, applies `nginx.yaml`, polls until
the Pod reaches `Running`, asserts a Docker container with the right
label is up, deletes the Pod, and asserts the container is reaped within
30s. CI-friendly (sets `fail=1` and dumps log tails on any failure).

## Repo layout

```
.
├── Makefile
├── flake.nix              # Go 1.25, kubectl, docker CLI, gh, ...
├── go.mod
├── cmd/
│   ├── apiserver/         # generic apiserver entrypoint
│   └── kubelet-lite/      # informer + Docker reconciler entrypoint
├── internal/
│   ├── apiserver/         # scheme, REST storage, server config
│   ├── fsstorage/         # storage.Interface backed by ./data/
│   ├── kubelet/           # reconciler + Docker adapter (pure-fn translate)
│   └── pki/               # self-signed CA / serving cert / kubeconfig
├── scripts/
│   ├── dev.sh             # process supervisor for `make up`/`make down`
│   └── smoke.sh           # full E2E test
├── examples/
│   ├── nginx.yaml         # long-running pod
│   └── sleep.yaml         # short-lived pod (Succeeded transition)
├── data/                  # gitignored — the "etcd"
└── run/                   # gitignored — pki, kubeconfig, pids, logs
```

## License

Experimental code. Use at your own risk; not intended for production
anything.

[kine]: https://github.com/k3s-io/kine
