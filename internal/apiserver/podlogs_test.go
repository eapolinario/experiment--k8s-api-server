package apiserver

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
)

type fakeResponder struct{ err error }

func (f *fakeResponder) Object(int, runtime.Object) {}
func (f *fakeResponder) Error(err error)            { f.err = err }

func newPodLogRESTForTest(t *testing.T, upstream string) *podLogREST {
	t.Helper()
	r, err := newPodLogREST(upstream, http.DefaultClient)
	if err != nil {
		t.Fatalf("newPodLogREST: %v", err)
	}
	return r
}

func TestPodLogREST_ProxiesToKubelet(t *testing.T) {
	var gotPath, gotQuery string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		gotPath = req.URL.Path
		gotQuery = req.URL.RawQuery
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, "log-line-1\nlog-line-2\n")
	}))
	defer upstream.Close()

	r := newPodLogRESTForTest(t, upstream.URL)

	tail := int64(7)
	opts := &corev1.PodLogOptions{Follow: true, TailLines: &tail, Timestamps: true}
	ctx := request.WithNamespace(context.Background(), "default")

	h, err := r.Connect(ctx, "nginx", opts, &fakeResponder{})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/namespaces/default/pods/nginx/log", nil)
	h.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status=%d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "log-line-1") {
		t.Errorf("body=%q", rec.Body.String())
	}
	if gotPath != "/containerLogs/default/nginx" {
		t.Errorf("upstream path=%q", gotPath)
	}
	q, _ := url.ParseQuery(gotQuery)
	if q.Get("follow") != "true" || q.Get("tailLines") != "7" || q.Get("timestamps") != "true" {
		t.Errorf("query=%q", gotQuery)
	}
	if rec.Header().Get("Content-Type") != "text/plain; charset=utf-8" {
		t.Errorf("content-type=%q", rec.Header().Get("Content-Type"))
	}
}

func TestPodLogREST_PropagatesUpstreamStatus(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "no container yet", http.StatusNotFound)
	}))
	defer upstream.Close()

	r := newPodLogRESTForTest(t, upstream.URL)
	ctx := request.WithNamespace(context.Background(), "default")
	h, err := r.Connect(ctx, "nginx", &corev1.PodLogOptions{}, &fakeResponder{})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d want 404", rec.Code)
	}
}

func TestPodLogREST_BadUpstreamReportsServiceUnavailable(t *testing.T) {
	r := newPodLogRESTForTest(t, "http://127.0.0.1:1") // unlikely to be listening
	ctx := request.WithNamespace(context.Background(), "default")
	resp := &fakeResponder{}
	h, err := r.Connect(ctx, "nginx", &corev1.PodLogOptions{}, resp)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
	if resp.err == nil {
		t.Fatalf("expected responder.Error to be invoked on dial failure")
	}
}

func TestPodLogREST_RequiresNamespace(t *testing.T) {
	r := newPodLogRESTForTest(t, "http://127.0.0.1:1")
	_, err := r.Connect(context.Background(), "nginx", &corev1.PodLogOptions{}, &fakeResponder{})
	if err == nil {
		t.Fatalf("expected error without namespace in ctx")
	}
}

func TestPodLogREST_ConnectMethodsAndOptions(t *testing.T) {
	r := newPodLogRESTForTest(t, "http://127.0.0.1:1")
	if got := r.ConnectMethods(); len(got) != 1 || got[0] != http.MethodGet {
		t.Errorf("ConnectMethods=%v", got)
	}
	obj, takesPath, name := r.NewConnectOptions()
	if _, ok := obj.(*corev1.PodLogOptions); !ok {
		t.Errorf("options type=%T", obj)
	}
	if takesPath || name != "" {
		t.Errorf("takesPath=%v name=%q", takesPath, name)
	}
	if !r.NamespaceScoped() {
		t.Errorf("expected namespaced")
	}
	// Storage interface compliance.
	var _ rest.Connecter = r
	var _ rest.Storage = r
	var _ rest.Scoper = r
}

func TestEncodeLogQueryEmpty(t *testing.T) {
	if q := encodeLogQuery(&corev1.PodLogOptions{}); q != "" {
		t.Errorf("empty options -> %q want empty", q)
	}
	if q := encodeLogQuery(nil); q != "" {
		t.Errorf("nil options -> %q want empty", q)
	}
}

func TestNewPodLogREST_RejectsBadURL(t *testing.T) {
	if _, err := newPodLogREST("not-a-url", nil); err == nil {
		t.Errorf("expected error for missing scheme")
	}
	if _, err := newPodLogREST("://broken", nil); err == nil {
		t.Errorf("expected error for malformed url")
	}
}
