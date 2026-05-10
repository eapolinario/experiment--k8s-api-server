// Package main is the kubelet-lite binary entrypoint.
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/eapolinario/experiment-k8s-api-server/internal/kubelet"
	clientfeatures "k8s.io/client-go/features"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)

// staticGates returns client-go feature gate values matching the v1.35
// defaults except that WatchListClient is forced off. Our apiserver does
// not emit a Bookmark on initial list, which the streaming list path
// requires; the regular LIST+WATCH path works fine.
type staticGates struct{}

func (staticGates) Enabled(f clientfeatures.Feature) bool {
	switch f {
	case clientfeatures.WatchListClient:
		return false
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
	)
	klog.InitFlags(nil)
	flag.Parse()

	if err := run(*kubeconfig, *resync, *nodeName, *logAddr); err != nil {
		klog.Errorf("kubelet-lite: %v", err)
		os.Exit(1)
	}
}

func run(kubeconfig string, resync time.Duration, nodeName, logAddr string) error {
	klog.Infof("kubelet-lite starting (node-name=%s, resync=%s, kubeconfig=%s, log-addr=%s)", nodeName, resync, kubeconfig, logAddr)

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

	r := kubelet.New(clientset, docker, resync)

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
