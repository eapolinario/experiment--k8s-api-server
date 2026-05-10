// Package main is the kubelet-lite binary entrypoint.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/eapolinario/experiment-k8s-api-server/internal/kubelet"
	clientfeatures "k8s.io/client-go/features"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)

// staticGates returns client-go feature gate values matching the v1.35
// defaults. WatchListClient is left enabled because our fsstorage now
// emits the initial-events-end Bookmark required by the streaming-list
// path.
type staticGates struct{}

func (staticGates) Enabled(f clientfeatures.Feature) bool {
	switch f {
	case clientfeatures.WatchListClient:
		return true
	case clientfeatures.ClientsAllowCBOR, clientfeatures.ClientsPreferCBOR:
		return false
	case clientfeatures.InOrderInformers,
		clientfeatures.InOrderInformersBatchProcess,
		clientfeatures.InformerResourceVersion:
		return true
	default:
		return false
	}
}

func init() {
	clientfeatures.ReplaceFeatureGates(staticGates{})
}

func main() {
	var (
		kubeconfig = flag.String("kubeconfig", "./run/kubeconfig", "path to kubeconfig")
		resync     = flag.Duration("resync", 30*time.Second, "informer resync period")
		nodeName   = flag.String("node-name", "kubelet-lite", "advisory node name (informational; we own every Pod)")
		logAddr    = flag.String("log-addr", "127.0.0.1:10350", "host:port for the /containerLogs HTTP server")
		volumeRoot = flag.String("volume-root", "./run/volumes", "host directory for projected ConfigMap/Secret volume contents (bind-mounted into containers)")
	)
	klog.InitFlags(nil)
	flag.Parse()

	if err := run(*kubeconfig, *resync, *nodeName, *logAddr, *volumeRoot); err != nil {
		klog.Errorf("kubelet-lite: %v", err)
		os.Exit(1)
	}
}

func run(kubeconfig string, resync time.Duration, nodeName, logAddr, volumeRoot string) error {
	klog.Infof("kubelet-lite starting (node-name=%s, resync=%s, kubeconfig=%s, log-addr=%s, volume-root=%s)", nodeName, resync, kubeconfig, logAddr, volumeRoot)

	if volumeRoot != "" {
		abs, err := filepath.Abs(volumeRoot)
		if err != nil {
			return fmt.Errorf("resolve volume-root: %w", err)
		}
		volumeRoot = abs
		if err := os.MkdirAll(volumeRoot, 0o755); err != nil {
			return fmt.Errorf("create volume-root %q: %w", volumeRoot, err)
		}
	}

	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return err
	}
	cfg.UserAgent = "kubelet-lite/v1"

	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}

	docker, err := kubelet.NewDockerRuntime()
	if err != nil {
		return err
	}
	defer docker.Close()

	r := kubelet.New(clientset, docker, resync, volumeRoot)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Run the log HTTP server alongside the reconciler. Failure of either
	// is fatal so the supervisor (`make up`) restarts the whole process.
	logSrv := kubelet.NewLogServer(r)
	logErr := make(chan error, 1)
	go func() { logErr <- logSrv.Run(ctx, logAddr) }()

	recErr := make(chan error, 1)
	go func() { recErr <- r.Run(ctx) }()

	select {
	case err := <-logErr:
		cancel()
		<-recErr
		return err
	case err := <-recErr:
		cancel()
		<-logErr
		return err
	}
}
