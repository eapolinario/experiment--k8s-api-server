// Package kubelet — logs.go
//
// Tiny HTTP server that mimics a real kubelet's /containerLogs/ endpoint
// just well enough for our apiserver's pods/log subresource to proxy to it.
//
// Route: GET /containerLogs/{namespace}/{name}
// Query params (subset of corev1.PodLogOptions):
//   follow=true|false
//   tailLines=<int>
//   sinceSeconds=<int>
//   timestamps=true|false
//   limitBytes=<int>
//
// We ignore `container` (only one container per Pod in v1) and `previous`
// (we never restart containers).
package kubelet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"
)

// PodResolver returns the Pod UID for a given namespace+name. Production
// uses a SharedInformer-backed lister; tests use an in-memory map.
type PodResolver interface {
	UIDFor(namespace, name string) (uid string, err error)
}

// listerResolver adapts a corelisters.PodLister to PodResolver.
type listerResolver struct{ l corelisters.PodLister }

func (r listerResolver) UIDFor(ns, name string) (string, error) {
	p, err := r.l.Pods(ns).Get(name)
	if err != nil {
		return "", err
	}
	if p.UID == "" {
		return "", fmt.Errorf("pod %s/%s has no UID", ns, name)
	}
	return string(p.UID), nil
}

// LogServer is an HTTP server exposing `docker logs` for kubelet-lite
// containers, addressed by Pod namespace+name.
type LogServer struct {
	docker   DockerRuntime
	resolver PodResolver
}

// NewLogServer wires a LogServer against the reconciler's lister + docker.
func NewLogServer(r *Reconciler) *LogServer {
	return &LogServer{
		docker:   r.docker,
		resolver: listerResolver{l: r.lister},
	}
}

// newLogServerForTest is used by tests with a stub resolver/runtime.
func newLogServerForTest(d DockerRuntime, res PodResolver) *LogServer {
	return &LogServer{docker: d, resolver: res}
}

// Handler returns the http.Handler implementing the routes above. Mounted
// at root by Run.
func (s *LogServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/containerLogs/", s.handleLogs)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}

// Run serves until ctx is canceled. addr is "host:port".
func (s *LogServer) Run(ctx context.Context, addr string) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("log listen %s: %w", addr, err)
	}
	klog.Infof("kubelet-lite log server on http://%s", ln.Addr())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()
	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func (s *LogServer) handleLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, "/containerLogs/")
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		http.Error(w, "expected /containerLogs/{namespace}/{name}", http.StatusBadRequest)
		return
	}
	ns, name := parts[0], parts[1]

	uid, err := s.resolver.UIDFor(ns, name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			http.Error(w, fmt.Sprintf("pod %s/%s not found", ns, name), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	opts, err := parseLogOptions(r.URL.Query())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	rc, err := s.docker.Logs(r.Context(), ContainerNameForUID(uid), opts)
	if err != nil {
		if IsContainerNotFound(err) {
			http.Error(w, fmt.Sprintf("no container for pod %s/%s yet", ns, name), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rc.Close()

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)

	flusher, _ := w.(http.Flusher)
	buf := make([]byte, 32*1024)
	for {
		n, rerr := rc.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if rerr != nil {
			if !errors.Is(rerr, io.EOF) {
				klog.V(2).Infof("logs %s/%s: read: %v", ns, name, rerr)
			}
			return
		}
	}
}

func parseLogOptions(q map[string][]string) (LogOptions, error) {
	out := LogOptions{}
	get := func(k string) string {
		if v := q[k]; len(v) > 0 {
			return v[0]
		}
		return ""
	}
	if v := get("follow"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return out, fmt.Errorf("follow: %w", err)
		}
		out.Follow = b
	}
	if v := get("timestamps"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return out, fmt.Errorf("timestamps: %w", err)
		}
		out.Timestamps = b
	}
	if v := get("tailLines"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return out, fmt.Errorf("tailLines: %w", err)
		}
		out.TailLines = &n
	}
	if v := get("sinceSeconds"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return out, fmt.Errorf("sinceSeconds: %w", err)
		}
		out.SinceSeconds = &n
	}
	if v := get("limitBytes"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return out, fmt.Errorf("limitBytes: %w", err)
		}
		out.LimitBytes = &n
	}
	return out, nil
}

// parseLogOptions terminates the file.
