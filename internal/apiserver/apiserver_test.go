package apiserver

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/eapolinario/experiment-k8s-api-server/internal/pki"
)

// freePort reserves an ephemeral TCP port on 127.0.0.1 and returns it.
// SecureServingOptions.ApplyTo will attempt to listen on it shortly after.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

// TestBuildRegistersExpectedREST validates that Build wires the legacy
// /api/v1 group with Pod, Pod /status and Namespace REST handlers.
func TestBuildRegistersExpectedREST(t *testing.T) {
	dir := t.TempDir()
	if err := pki.EnsureServerCert(dir+"/pki", []string{"127.0.0.1"}, time.Hour); err != nil {
		t.Fatalf("ensure cert: %v", err)
	}
	srv, err := Build(Options{
		BindAddress: "127.0.0.1",
		BindPort:    freePort(t),
		CertDir:     dir + "/pki",
		DataDir:     dir + "/data",
	})

	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if srv == nil {
		t.Fatalf("Build returned nil server")
	}

	want := []string{
		"/api/v1/namespaces",
		"/api/v1/namespaces/{name}",
		"/api/v1/pods",
		"/api/v1/namespaces/{namespace}/pods",
		"/api/v1/namespaces/{namespace}/pods/{name}",
		"/api/v1/namespaces/{namespace}/pods/{name}/status",
		"/api/v1/namespaces/{namespace}/pods/{name}/log",
		"/api/v1/events",
		"/api/v1/namespaces/{namespace}/events",
		"/api/v1/namespaces/{namespace}/events/{name}",
	}
	listed := srv.Handler.GoRestfulContainer.RegisteredWebServices()
	all := []string{}
	for _, ws := range listed {
		for _, r := range ws.Routes() {
			all = append(all, r.Path)
		}
	}
	joined := strings.Join(all, "\n")
	for _, p := range want {
		if !strings.Contains(joined, p) {
			t.Errorf("missing route %q\nregistered:\n%s", p, joined)
		}
	}
}

func TestBuildOptionsDefaults(t *testing.T) {
	o := Options{}
	o.Defaults()
	if o.BindAddress == "" || o.CertDir == "" || o.DataDir == "" {
		t.Fatalf("Defaults left fields empty: %+v", o)
	}
}
