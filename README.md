# experiment--k8s-api-server

A minimal Kubernetes API server backed by the local filesystem (no etcd, no
kine), plus a tiny kubelet that runs Pods as real Docker containers.

**Status:** scaffolding phase. See `plan.md` (in the session workspace) for
the design and roadmap.

## Quick start

Requires the [Nix](https://nixos.org/) package manager and a running Docker
daemon on the host.

```bash
nix develop          # enter dev shell (Go 1.25, kubectl, docker CLI, …)
make build           # compile cmd/apiserver and cmd/kubelet-lite
make up              # start both in the background (run/*.log, run/*.pid)
make demo            # kubectl apply -f examples/nginx.yaml
make data            # inspect the on-disk "etcd" under ./data/
make down            # stop background processes
make clean           # also wipe ./data and ./run
```

## Layout

```
cmd/
  apiserver/        # generic apiserver binary
  kubelet-lite/     # mini kubelet that drives Docker
internal/
  fsstorage/        # storage.Interface backed by ./data/<resource>/<ns>/<name>.json
  apiserver/        # wiring: scheme, REST storage, server config
  kubelet/          # reconciler + Docker adapter
scripts/dev.sh      # process supervisor for `make up`/`make down`
examples/           # sample Pod manifests
```
