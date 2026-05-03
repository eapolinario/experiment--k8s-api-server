package pki

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"k8s.io/client-go/tools/clientcmd"
)

func TestEnsureServerCert_CreatesMaterial(t *testing.T) {
	dir := t.TempDir()
	if err := EnsureServerCert(dir, []string{"example.test"}, time.Hour); err != nil {
		t.Fatalf("EnsureServerCert: %v", err)
	}

	for _, name := range []string{caCertFile, caKeyFile, serverCertFile, serverKeyFile} {
		st, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("missing %s: %v", name, err)
		}
		mode := st.Mode().Perm()
		if name == caKeyFile || name == serverKeyFile {
			if mode != 0o600 {
				t.Errorf("%s mode = %o, want 0600", name, mode)
			}
		} else {
			if mode&0o077 != 0 && mode != 0o644 {
				t.Errorf("%s mode = %o, want 0644-ish", name, mode)
			}
		}
	}

	caCert := mustLoadCert(t, filepath.Join(dir, caCertFile))
	srvCert := mustLoadCert(t, filepath.Join(dir, serverCertFile))

	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	if _, err := srvCert.Verify(x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		t.Fatalf("server cert does not verify against CA: %v", err)
	}

	if err := srvCert.VerifyHostname("localhost"); err != nil {
		t.Errorf("missing SAN localhost: %v", err)
	}
	if err := srvCert.VerifyHostname("127.0.0.1"); err != nil {
		t.Errorf("missing SAN 127.0.0.1: %v", err)
	}
	if err := srvCert.VerifyHostname("example.test"); err != nil {
		t.Errorf("missing SAN example.test: %v", err)
	}

	// Validity ~ 1h (allow generous slack for the -1m skew applied at issuance).
	dur := srvCert.NotAfter.Sub(srvCert.NotBefore)
	if dur < 59*time.Minute || dur > 61*time.Minute {
		t.Errorf("unexpected validity duration: %s", dur)
	}
}

func TestEnsureServerCert_Idempotent(t *testing.T) {
	dir := t.TempDir()
	if err := EnsureServerCert(dir, []string{"example.test"}, time.Hour); err != nil {
		t.Fatalf("first: %v", err)
	}
	first := readAll(t, dir)

	// Sleep a tick so any rewrite would change mtime perceptibly.
	time.Sleep(20 * time.Millisecond)

	if err := EnsureServerCert(dir, []string{"example.test"}, time.Hour); err != nil {
		t.Fatalf("second: %v", err)
	}
	second := readAll(t, dir)

	for name, b1 := range first {
		b2, ok := second[name]
		if !ok {
			t.Fatalf("file disappeared: %s", name)
		}
		if string(b1) != string(b2) {
			t.Errorf("file %s changed between idempotent calls", name)
		}
	}
}

func TestEnsureServerCert_RegeneratesExpired(t *testing.T) {
	dir := t.TempDir()
	// Issue with very short validity so it's expired by the time we re-call.
	if err := EnsureServerCert(dir, []string{"example.test"}, 10*time.Millisecond); err != nil {
		t.Fatalf("first: %v", err)
	}
	first := readAll(t, dir)
	time.Sleep(80 * time.Millisecond) // wait past NotAfter (note: NotBefore is -1m)

	// Re-issue with a normal validity; expired material should be replaced.
	if err := EnsureServerCert(dir, []string{"example.test"}, time.Hour); err != nil {
		t.Fatalf("second: %v", err)
	}
	second := readAll(t, dir)

	if string(first[caCertFile]) == string(second[caCertFile]) {
		t.Error("expected ca.crt to be regenerated")
	}
	if string(first[serverCertFile]) == string(second[serverCertFile]) {
		t.Error("expected server.crt to be regenerated")
	}
	srv := mustLoadCert(t, filepath.Join(dir, serverCertFile))
	if time.Now().After(srv.NotAfter) {
		t.Error("regenerated server cert is already expired")
	}
}

func TestEnsureServerCert_RegeneratesOnMissingHost(t *testing.T) {
	dir := t.TempDir()
	if err := EnsureServerCert(dir, []string{"a.test"}, time.Hour); err != nil {
		t.Fatalf("first: %v", err)
	}
	first := readAll(t, dir)

	// New host that wasn't in the previous SAN list -> must regenerate.
	if err := EnsureServerCert(dir, []string{"a.test", "b.test"}, time.Hour); err != nil {
		t.Fatalf("second: %v", err)
	}
	second := readAll(t, dir)
	if string(first[serverCertFile]) == string(second[serverCertFile]) {
		t.Error("server cert should have been regenerated for new SAN")
	}
	srv := mustLoadCert(t, filepath.Join(dir, serverCertFile))
	if err := srv.VerifyHostname("b.test"); err != nil {
		t.Errorf("regenerated cert missing b.test SAN: %v", err)
	}
}

func TestWriteKubeconfig(t *testing.T) {
	dir := t.TempDir()
	if err := EnsureServerCert(dir, nil, time.Hour); err != nil {
		t.Fatalf("EnsureServerCert: %v", err)
	}

	kcPath := filepath.Join(dir, "nested", "kubeconfig")
	const server = "https://127.0.0.1:6443"
	if err := WriteKubeconfig(kcPath, server, filepath.Join(dir, caCertFile)); err != nil {
		t.Fatalf("WriteKubeconfig: %v", err)
	}

	cfg, err := clientcmd.LoadFromFile(kcPath)
	if err != nil {
		t.Fatalf("load kubeconfig: %v", err)
	}
	if cfg.CurrentContext == "" {
		t.Fatal("current-context not set")
	}
	rest, err := clientcmd.NewDefaultClientConfig(*cfg, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		t.Fatalf("ClientConfig: %v", err)
	}
	if rest.Host != server {
		t.Errorf("Host = %q, want %q", rest.Host, server)
	}
	if len(rest.CAData) == 0 && rest.CAFile == "" {
		t.Error("kubeconfig has neither CAData nor CAFile")
	}
	if rest.BearerToken != "" || rest.Username != "" || len(rest.CertData) != 0 {
		t.Errorf("expected anonymous auth, got token=%q user=%q certData=%d",
			rest.BearerToken, rest.Username, len(rest.CertData))
	}
}

func TestEndToEndTLSHandshake(t *testing.T) {
	dir := t.TempDir()
	if err := EnsureServerCert(dir, nil, time.Hour); err != nil {
		t.Fatalf("EnsureServerCert: %v", err)
	}
	cert, err := tls.LoadX509KeyPair(filepath.Join(dir, serverCertFile), filepath.Join(dir, serverKeyFile))
	if err != nil {
		t.Fatalf("LoadX509KeyPair: %v", err)
	}

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	// Bind to loopback so SAN matches.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv.Listener = l
	srv.StartTLS()
	defer srv.Close()

	caPEM, err := os.ReadFile(filepath.Join(dir, caCertFile))
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatal("could not append CA")
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}}

	// Use 127.0.0.1 hostname (in SANs); URL from httptest already does this.
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "ok" {
		t.Errorf("body = %q, want ok", body)
	}
}

func mustLoadCert(t *testing.T, path string) *x509.Certificate {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(b)
	if block == nil {
		t.Fatalf("no PEM in %s", path)
	}
	c, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func readAll(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	for _, n := range []string{caCertFile, caKeyFile, serverCertFile, serverKeyFile} {
		b, err := os.ReadFile(filepath.Join(dir, n))
		if err != nil {
			t.Fatalf("read %s: %v", n, err)
		}
		out[n] = b
	}
	return out
}
