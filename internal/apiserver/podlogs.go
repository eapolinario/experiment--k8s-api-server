// Package apiserver — podlogs.go
//
// Pod /log subresource implemented as a rest.Connecter that proxies HTTP
// streaming GETs to kubelet-lite's /containerLogs/{ns}/{name} endpoint.
//
// Real kubernetes resolves the kubelet address per-Pod via the Node object;
// we have a single kubelet so the upstream URL is configured once via
// Options.KubeletLogURL (default http://127.0.0.1:10350).
package apiserver

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
)

// podLogREST implements rest.Connecter for /api/v1/namespaces/{ns}/pods/{name}/log.
type podLogREST struct {
	upstream   *url.URL    // base URL of the kubelet log server, e.g. http://127.0.0.1:10350
	httpClient *http.Client
}

// newPodLogREST validates the upstream URL and constructs the subresource.
func newPodLogREST(upstream string, client *http.Client) (*podLogREST, error) {
	u, err := url.Parse(upstream)
	if err != nil {
		return nil, fmt.Errorf("parse kubelet log url %q: %w", upstream, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("kubelet log url must have scheme + host, got %q", upstream)
	}
	if client == nil {
		client = http.DefaultClient
	}
	return &podLogREST{upstream: u, httpClient: client}, nil
}

// rest.Storage
func (r *podLogREST) New() runtime.Object { return &corev1.Pod{} }
func (r *podLogREST) Destroy()            {}

// rest.Scoper — pods/log is namespaced.
func (r *podLogREST) NamespaceScoped() bool { return true }

// rest.Connecter
func (r *podLogREST) NewConnectOptions() (runtime.Object, bool, string) {
	return &corev1.PodLogOptions{}, false, ""
}
func (r *podLogREST) ConnectMethods() []string { return []string{http.MethodGet} }

func (r *podLogREST) Connect(ctx context.Context, name string, options runtime.Object, responder rest.Responder) (http.Handler, error) {
	opts, ok := options.(*corev1.PodLogOptions)
	if !ok {
		return nil, fmt.Errorf("invalid options type %T", options)
	}
	ns, ok := request.NamespaceFrom(ctx)
	if !ok || ns == "" {
		return nil, apierrors.NewBadRequest("namespace required")
	}

	target := *r.upstream
	target.Path = strings.TrimRight(target.Path, "/") + "/containerLogs/" + ns + "/" + name
	target.RawQuery = encodeLogQuery(opts)

	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		upReq, err := http.NewRequestWithContext(req.Context(), http.MethodGet, target.String(), nil)
		if err != nil {
			responder.Error(apierrors.NewInternalError(err))
			return
		}
		resp, err := r.httpClient.Do(upReq)
		if err != nil {
			responder.Error(apierrors.NewServiceUnavailable(fmt.Sprintf("kubelet log backend: %v", err)))
			return
		}
		defer resp.Body.Close()

		// Forward selected headers + status.
		if ct := resp.Header.Get("Content-Type"); ct != "" {
			w.Header().Set("Content-Type", ct)
		} else {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		}
		w.WriteHeader(resp.StatusCode)

		flusher, _ := w.(http.Flusher)
		buf := make([]byte, 32*1024)
		for {
			n, rerr := resp.Body.Read(buf)
			if n > 0 {
				if _, werr := w.Write(buf[:n]); werr != nil {
					return
				}
				if flusher != nil {
					flusher.Flush()
				}
			}
			if rerr != nil {
				if rerr != io.EOF {
					// Connection broken mid-stream; nothing to do — bytes already written.
				}
				return
			}
		}
	}), nil
}

// encodeLogQuery serializes the subset of PodLogOptions our kubelet log
// server understands. Keep in sync with internal/kubelet.parseLogOptions.
func encodeLogQuery(o *corev1.PodLogOptions) string {
	if o == nil {
		return ""
	}
	v := url.Values{}
	if o.Follow {
		v.Set("follow", "true")
	}
	if o.Timestamps {
		v.Set("timestamps", "true")
	}
	if o.TailLines != nil {
		v.Set("tailLines", strconv.FormatInt(*o.TailLines, 10))
	}
	if o.SinceSeconds != nil {
		v.Set("sinceSeconds", strconv.FormatInt(*o.SinceSeconds, 10))
	}
	if o.LimitBytes != nil {
		v.Set("limitBytes", strconv.FormatInt(*o.LimitBytes, 10))
	}
	return v.Encode()
}
