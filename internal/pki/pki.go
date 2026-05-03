// Package pki materializes a self-signed CA + serving cert pair on disk for
// the experiment's apiserver, and writes a matching anonymous-auth
// kubeconfig for kubectl.
package pki

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"

	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	clientcmdlatest "k8s.io/client-go/tools/clientcmd/api/latest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	caCertFile     = "ca.crt"
	caKeyFile      = "ca.key"
	serverCertFile = "server.crt"
	serverKeyFile  = "server.key"

	rsaBits = 2048
)

// EnsureServerCert generates a self-signed CA and a serving cert signed by
// it, written to dir as ca.crt, ca.key, server.crt, server.key. If valid
// material is already present it is left untouched. hosts is the list of
// SAN entries (DNS names and/or IP literals); 127.0.0.1 and localhost are
// auto-added if absent.
func EnsureServerCert(dir string, hosts []string, validity time.Duration) error {
	if validity <= 0 {
		return fmt.Errorf("pki: validity must be positive, got %s", validity)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("pki: mkdir %s: %w", dir, err)
	}

	hosts = ensureLoopback(hosts)

	caCertPath := filepath.Join(dir, caCertFile)
	caKeyPath := filepath.Join(dir, caKeyFile)
	srvCertPath := filepath.Join(dir, serverCertFile)
	srvKeyPath := filepath.Join(dir, serverKeyFile)

	if existingValid(caCertPath, srvCertPath, hosts) {
		// Sanity-check that the matching keys are present too.
		if fileExists(caKeyPath) && fileExists(srvKeyPath) {
			return nil
		}
	}

	caKey, err := rsa.GenerateKey(rand.Reader, rsaBits)
	if err != nil {
		return fmt.Errorf("pki: generate ca key: %w", err)
	}
	now := time.Now().Add(-1 * time.Minute)
	caTmpl := &x509.Certificate{
		SerialNumber: serial(),
		Subject:      pkix.Name{CommonName: "experiment-k8s-apiserver-ca"},
		NotBefore:    now,
		NotAfter:     now.Add(validity),
		KeyUsage:     x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		IsCA:         true,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("pki: create ca cert: %w", err)
	}

	srvKey, err := rsa.GenerateKey(rand.Reader, rsaBits)
	if err != nil {
		return fmt.Errorf("pki: generate server key: %w", err)
	}
	srvTmpl := &x509.Certificate{
		SerialNumber: serial(),
		Subject:      pkix.Name{CommonName: "experiment-k8s-apiserver"},
		NotBefore:    now,
		NotAfter:     now.Add(validity),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			srvTmpl.IPAddresses = append(srvTmpl.IPAddresses, ip)
		} else {
			srvTmpl.DNSNames = append(srvTmpl.DNSNames, h)
		}
	}

	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return fmt.Errorf("pki: parse ca: %w", err)
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTmpl, caCert, &srvKey.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("pki: create server cert: %w", err)
	}

	if err := writePEM(caCertPath, "CERTIFICATE", caDER, 0o644); err != nil {
		return err
	}
	if err := writePEM(caKeyPath, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(caKey), 0o600); err != nil {
		return err
	}
	if err := writePEM(srvCertPath, "CERTIFICATE", srvDER, 0o644); err != nil {
		return err
	}
	if err := writePEM(srvKeyPath, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(srvKey), 0o600); err != nil {
		return err
	}
	return nil
}

// WriteKubeconfig writes a kubeconfig at path that points at the server URL,
// trusts the CA at caPath (embedded as certificate-authority-data), and uses
// anonymous auth — appropriate for an apiserver running AlwaysAllow +
// AnonymousAuthn.
func WriteKubeconfig(path, server, caPath string) error {
	caData, err := os.ReadFile(caPath)
	if err != nil {
		return fmt.Errorf("pki: read ca: %w", err)
	}
	cfg := clientcmdapi.NewConfig()
	cfg.Clusters["experiment"] = &clientcmdapi.Cluster{
		Server:                   server,
		CertificateAuthorityData: caData,
	}
	cfg.Contexts["experiment"] = &clientcmdapi.Context{
		Cluster:  "experiment",
		AuthInfo: "anonymous",
	}
	cfg.AuthInfos["anonymous"] = &clientcmdapi.AuthInfo{}
	cfg.CurrentContext = "experiment"

	if err := clientcmdapi.MinifyConfig(cfg); err != nil {
		return fmt.Errorf("pki: minify kubeconfig: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("pki: mkdir kubeconfig parent: %w", err)
	}
	// Touch latest so we use the on-disk v1 schema.
	_ = clientcmdlatest.Codec
	if err := clientcmd.WriteToFile(*cfg, path); err != nil {
		return fmt.Errorf("pki: write kubeconfig: %w", err)
	}
	return nil
}

func ensureLoopback(hosts []string) []string {
	hasLocalhost, hasLoopbackIP := false, false
	for _, h := range hosts {
		switch {
		case h == "localhost":
			hasLocalhost = true
		case net.ParseIP(h) != nil && net.ParseIP(h).IsLoopback():
			hasLoopbackIP = true
		}
	}
	out := append([]string(nil), hosts...)
	if !hasLocalhost {
		out = append(out, "localhost")
	}
	if !hasLoopbackIP {
		out = append(out, "127.0.0.1")
	}
	return out
}

func existingValid(caPath, srvPath string, hosts []string) bool {
	caCert, err := loadCert(caPath)
	if err != nil {
		return false
	}
	srvCert, err := loadCert(srvPath)
	if err != nil {
		return false
	}
	now := time.Now()
	if now.Before(caCert.NotBefore) || now.After(caCert.NotAfter) {
		return false
	}
	if now.Before(srvCert.NotBefore) || now.After(srvCert.NotAfter) {
		return false
	}
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	if _, err := srvCert.Verify(x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		return false
	}
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			if srvCert.VerifyHostname(ip.String()) != nil {
				return false
			}
		} else if srvCert.VerifyHostname(h) != nil {
			return false
		}
	}
	return true
}

func loadCert(path string) (*x509.Certificate, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(b)
	if block == nil {
		return nil, fmt.Errorf("pki: no PEM block in %s", path)
	}
	return x509.ParseCertificate(block.Bytes)
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

func writePEM(path, typ string, der []byte, mode os.FileMode) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return fmt.Errorf("pki: open %s: %w", path, err)
	}
	defer f.Close()
	if err := pem.Encode(f, &pem.Block{Type: typ, Bytes: der}); err != nil {
		return fmt.Errorf("pki: encode %s: %w", path, err)
	}
	// Ensure mode is correct even if file pre-existed.
	return os.Chmod(path, mode)
}

func serial() *big.Int {
	max := new(big.Int).Lsh(big.NewInt(1), 128)
	n, err := rand.Int(rand.Reader, max)
	if err != nil {
		// Fallback to time-based serial; cryptographic strength is not
		// critical for an experiment self-signed cert.
		return big.NewInt(time.Now().UnixNano())
	}
	return n
}
