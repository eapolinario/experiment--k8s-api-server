// Package main is the apiserver binary entrypoint.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/eapolinario/experiment-k8s-api-server/internal/apiserver"
	"github.com/eapolinario/experiment-k8s-api-server/internal/pki"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	var (
		bindAddr      = flag.String("bind-address", "127.0.0.1", "host/IP to bind")
		bindPort      = flag.Int("secure-port", 6443, "https port")
		certDir       = flag.String("cert-dir", "run/pki", "directory for serving cert + CA")
		dataDir       = flag.String("data-dir", "data", "filesystem storage root")
		kubeletLogURL = flag.String("kubelet-log-url", "http://127.0.0.1:10350", "base URL of kubelet-lite's log HTTP server")
		kubeconfigOut = flag.String("kubeconfig-out", "run/kubeconfig", "where to write the anonymous-auth kubeconfig")
	)
	flag.Parse()

	if err := run(*bindAddr, *bindPort, *certDir, *dataDir, *kubeconfigOut, *kubeletLogURL); err != nil {
		log.Fatalf("apiserver: %v", err)
	}
}

func run(bindAddr string, bindPort int, certDir, dataDir, kubeconfigOut, kubeletLogURL string) error {
	if err := os.MkdirAll(certDir, 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}
	hosts := []string{bindAddr, "127.0.0.1", "localhost"}
	if err := pki.EnsureServerCert(certDir, hosts, 365*24*time.Hour); err != nil {
		return fmt.Errorf("pki: %w", err)
	}
	server := fmt.Sprintf("https://%s:%d", bindAddr, bindPort)
	caPath := filepath.Join(certDir, "ca.crt")
	if err := pki.WriteKubeconfig(kubeconfigOut, server, caPath); err != nil {
		return fmt.Errorf("kubeconfig: %w", err)
	}
	if err := patchKubeconfigForAnonymous(kubeconfigOut); err != nil {
		return fmt.Errorf("patch kubeconfig: %w", err)
	}

	srv, err := apiserver.Build(apiserver.Options{
		BindAddress:   bindAddr,
		BindPort:      bindPort,
		CertDir:       certDir,
		DataDir:       dataDir,
		KubeletLogURL: kubeletLogURL,
	})
	if err != nil {
		return fmt.Errorf("build: %w", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Printf("listening on %s", server)
	return apiserver.Run(ctx, srv)
}

// patchKubeconfigForAnonymous ensures the AuthInfo entry has a non-empty
// identity field so kubectl doesn't drop into an interactive
// "Please enter Username:" prompt. The apiserver uses anonymous
// authentication, so any Username value is accepted.
func patchKubeconfigForAnonymous(path string) error {
	cfg, err := clientcmd.LoadFromFile(path)
	if err != nil {
		return err
	}
	for _, ai := range cfg.AuthInfos {
		if ai.Username == "" && ai.Token == "" && len(ai.ClientCertificateData) == 0 &&
			ai.ClientCertificate == "" && ai.AuthProvider == nil && ai.Exec == nil {
			ai.Username = "anonymous"
		}
	}
	return clientcmd.WriteToFile(*cfg, path)
}
