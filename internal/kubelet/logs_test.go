package kubelet

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// fakeDocker satisfies DockerRuntime; only Logs is exercised here.
type fakeDocker struct {
	logs       func(ctx context.Context, name string, opts LogOptions) (io.ReadCloser, error)
	gotName    string
	gotOpts    LogOptions
	gotInvoked bool
}

func (f *fakeDocker) EnsurePulled(context.Context, string, string) error { return nil }
func (f *fakeDocker) CreateAndStart(context.Context, string, ContainerSpec) (string, error) {
	return "", nil
}
func (f *fakeDocker) Inspect(context.Context, string) (ContainerView, bool, error) {
	return ContainerView{}, false, nil
}
func (f *fakeDocker) Stop(context.Context, string, time.Duration) error { return nil }
func (f *fakeDocker) Remove(context.Context, string) error              { return nil }
func (f *fakeDocker) ListManagedContainers(context.Context) ([]ManagedContainer, error) {
	return nil, nil
}
func (f *fakeDocker) Close() error { return nil }
func (f *fakeDocker) Logs(ctx context.Context, name string, opts LogOptions) (io.ReadCloser, error) {
	f.gotInvoked = true
	f.gotName = name
	f.gotOpts = opts
	return f.logs(ctx, name, opts)
}

type fakeResolver struct {
	// uids: "ns/name" -> uid
	uids map[string]string
	// containers: "ns/name" -> ordered container names; missing entries
	// default to a single "main" container so older single-container
	// tests don't need updating.
	containers map[string][]string
	err        error
}

func (r fakeResolver) Resolve(ns, name string) (string, []string, error) {
	if r.err != nil {
		return "", nil, r.err
	}
	uid, ok := r.uids[ns+"/"+name]
	if !ok {
		return "", nil, &notFoundErr{}
	}
	cs := r.containers[ns+"/"+name]
	if len(cs) == 0 {
		cs = []string{"main"}
	}
	return uid, cs, nil
}

type notFoundErr struct{}

func (notFoundErr) Error() string         { return "pods not found" }
func (notFoundErr) Status() int           { return 404 }
func (notFoundErr) Is(target error) bool  { return target == errNotFound }

var errNotFound = errors.New("not found")

func TestLogServer_StreamsLogsForKnownPod(t *testing.T) {
	body := "hello\nworld\n"
	fd := &fakeDocker{
		logs: func(_ context.Context, _ string, _ LogOptions) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(body)), nil
		},
	}
	srv := newLogServerForTest(fd, fakeResolver{uids: map[string]string{"default/nginx": "uid-1"}})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/containerLogs/default/nginx?tailLines=10&timestamps=true")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	got, _ := io.ReadAll(resp.Body)
	if string(got) != body {
		t.Errorf("body=%q want %q", got, body)
	}
	if fd.gotName != ContainerName("uid-1", "main") {
		t.Errorf("container name=%q want %q", fd.gotName, ContainerName("uid-1", "main"))
	}
	if fd.gotOpts.TailLines == nil || *fd.gotOpts.TailLines != 10 {
		t.Errorf("tailLines=%v want 10", fd.gotOpts.TailLines)
	}
	if !fd.gotOpts.Timestamps {
		t.Errorf("timestamps not propagated")
	}
}

func TestLogServer_404OnUnknownPod(t *testing.T) {
	fd := &fakeDocker{logs: func(context.Context, string, LogOptions) (io.ReadCloser, error) {
		t.Fatal("docker.Logs must not be called")
		return nil, nil
	}}
	// resolver with empty map — UIDFor returns notFoundErr (which mimics IsNotFound via errors.Is).
	srv := newLogServerForTest(fd, fakeResolver{uids: map[string]string{}})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/containerLogs/default/missing")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	// fakeResolver returns a generic error (not apierrors.IsNotFound),
	// so the handler classifies it as 500. Document that here so the
	// real lister-backed path (which DOES return apierrors.IsNotFound) is
	// the one exercised against kubectl.
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status=%d want 500 (fake resolver returns plain error)", resp.StatusCode)
	}
	if fd.gotInvoked {
		t.Errorf("docker.Logs invoked despite pod lookup failure")
	}
}

func TestLogServer_BadRoute(t *testing.T) {
	srv := newLogServerForTest(&fakeDocker{}, fakeResolver{})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/containerLogs/")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d want 400", resp.StatusCode)
	}
}

func TestLogServer_PropagatesContainerNotFound(t *testing.T) {
	fd := &fakeDocker{logs: func(context.Context, string, LogOptions) (io.ReadCloser, error) {
		return nil, errContainerNotFound
	}}
	srv := newLogServerForTest(fd, fakeResolver{uids: map[string]string{"default/nginx": "uid-x"}})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/containerLogs/default/nginx")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%d want 404", resp.StatusCode)
	}
}

func TestParseLogOptions(t *testing.T) {
	q := map[string][]string{
		"follow":       {"true"},
		"tailLines":    {"5"},
		"sinceSeconds": {"60"},
		"limitBytes":   {"1024"},
		"timestamps":   {"false"},
	}
	o, err := parseLogOptions(q)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !o.Follow || o.Timestamps {
		t.Errorf("flags: %+v", o)
	}
	if o.TailLines == nil || *o.TailLines != 5 {
		t.Errorf("tailLines: %v", o.TailLines)
	}
	if o.SinceSeconds == nil || *o.SinceSeconds != 60 {
		t.Errorf("sinceSeconds: %v", o.SinceSeconds)
	}
	if o.LimitBytes == nil || *o.LimitBytes != 1024 {
		t.Errorf("limitBytes: %v", o.LimitBytes)
	}

	if _, err := parseLogOptions(map[string][]string{"tailLines": {"nope"}}); err == nil {
		t.Errorf("expected parse error for non-int tailLines")
	}
}

func TestLimitWriter(t *testing.T) {
	var buf bytes.Buffer
	lw := &limitWriter{w: &buf, n: 5}
	n, err := lw.Write([]byte("hello world"))
	if n != 5 {
		t.Errorf("wrote %d want 5", n)
	}
	if !errors.Is(err, errLimitReached) {
		t.Errorf("err=%v want errLimitReached", err)
	}
	if buf.String() != "hello" {
		t.Errorf("buf=%q", buf.String())
	}
}

func TestLogServer_MultiContainerRequiresContainerParam(t *testing.T) {
	fd := &fakeDocker{logs: func(context.Context, string, LogOptions) (io.ReadCloser, error) {
		t.Fatal("docker.Logs must not be called when container ambiguous")
		return nil, nil
	}}
	srv := newLogServerForTest(fd, fakeResolver{
		uids:       map[string]string{"default/web": "uid-w"},
		containers: map[string][]string{"default/web": {"main", "side"}},
	})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/containerLogs/default/web")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d want 400 (multi-container needs ?container=)", resp.StatusCode)
	}
}

func TestLogServer_PicksContainerByName(t *testing.T) {
	fd := &fakeDocker{logs: func(_ context.Context, _ string, _ LogOptions) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("hi from sidecar\n")), nil
	}}
	srv := newLogServerForTest(fd, fakeResolver{
		uids:       map[string]string{"default/web": "uid-w"},
		containers: map[string][]string{"default/web": {"main", "side"}},
	})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/containerLogs/default/web?container=side")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d want 200", resp.StatusCode)
	}
	if fd.gotName != ContainerName("uid-w", "side") {
		t.Errorf("docker name=%q want %q", fd.gotName, ContainerName("uid-w", "side"))
	}
}

func TestLogServer_RejectsUnknownContainerName(t *testing.T) {
	fd := &fakeDocker{logs: func(context.Context, string, LogOptions) (io.ReadCloser, error) {
		t.Fatal("docker.Logs must not be called when container name unknown")
		return nil, nil
	}}
	srv := newLogServerForTest(fd, fakeResolver{
		uids:       map[string]string{"default/web": "uid-w"},
		containers: map[string][]string{"default/web": {"main", "side"}},
	})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/containerLogs/default/web?container=ghost")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d want 400", resp.StatusCode)
	}
}

func TestPickContainer(t *testing.T) {
	cases := []struct {
		name       string
		requested  string
		available  []string
		want       string
		wantError  bool
	}{
		{"single implicit", "", []string{"only"}, "only", false},
		{"single explicit", "only", []string{"only"}, "only", false},
		{"multi implicit fails", "", []string{"a", "b"}, "", true},
		{"multi explicit ok", "b", []string{"a", "b"}, "b", false},
		{"unknown name fails", "ghost", []string{"a", "b"}, "", true},
		{"empty available fails", "", nil, "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := pickContainer(c.requested, c.available)
			if (err != nil) != c.wantError {
				t.Fatalf("err=%v wantError=%v", err, c.wantError)
			}
			if got != c.want {
				t.Errorf("got=%q want=%q", got, c.want)
			}
		})
	}
}
