package apiserver

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"time"

	"github.com/eapolinario/experiment-k8s-api-server/internal/fsstorage"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/authentication/request/anonymous"
	"k8s.io/apiserver/pkg/authorization/authorizerfactory"
	openapinamer "k8s.io/apiserver/pkg/endpoints/openapi"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	genericserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/options"
	basecompatibility "k8s.io/component-base/compatibility"
	netutils "k8s.io/utils/net"
)

// Options configures the apiserver build. Paths default to working
// directory relative; cmd/apiserver fills these from CLI flags.
type Options struct {
	BindAddress   string // host or ip; e.g. "127.0.0.1"
	BindPort      int    // e.g. 6443
	CertDir       string // directory containing server.crt/server.key/ca.crt
	DataDir       string // root for fsstorage
	ExternalHost  string
	KubeletLogURL string // base URL of kubelet-lite's log server, e.g. http://127.0.0.1:10350
}

// Defaults fills sensible defaults if fields are zero-valued.
func (o *Options) Defaults() {
	if o.BindAddress == "" {
		o.BindAddress = "127.0.0.1"
	}
	if o.CertDir == "" {
		o.CertDir = "run/pki"
	}
	if o.DataDir == "" {
		o.DataDir = "data"
	}
	if o.ExternalHost == "" {
		o.ExternalHost = o.BindAddress
	}
	if o.KubeletLogURL == "" {
		o.KubeletLogURL = "http://127.0.0.1:10350"
	}
}

// Build constructs a fully wired GenericAPIServer ready to PrepareRun().
func Build(opts Options) (*genericserver.GenericAPIServer, error) {
	opts.Defaults()

	// --- scheme + codec ---
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(metav1.AddMetaToScheme(scheme))
	metav1.AddToGroupVersion(scheme, corev1.SchemeGroupVersion)

	codecs := serializer.NewCodecFactory(scheme)
	storageCodec := codecs.LegacyCodec(corev1.SchemeGroupVersion)

	// --- secure serving + loopback ---
	secureOpts := options.NewSecureServingOptions().WithLoopback()
	secureOpts.BindAddress = netutils.ParseIPSloppy(opts.BindAddress)
	secureOpts.BindPort = opts.BindPort
	secureOpts.ServerCert.CertKey.CertFile = filepath.Join(opts.CertDir, "server.crt")
	secureOpts.ServerCert.CertKey.KeyFile = filepath.Join(opts.CertDir, "server.key")

	// --- recommended config ---
	recommended := genericserver.NewRecommendedConfig(codecs)
	if err := secureOpts.ApplyTo(&recommended.Config.SecureServing, &recommended.Config.LoopbackClientConfig); err != nil {
		return nil, fmt.Errorf("apply secure serving: %w", err)
	}

	// External address: <host>:<port>
	recommended.Config.ExternalAddress = net.JoinHostPort(opts.ExternalHost, fmt.Sprintf("%d", opts.BindPort))
	recommended.Config.PublicAddress = netutils.ParseIPSloppy(opts.BindAddress)

	// Authn = anonymous, Authz = AlwaysAllow.
	recommended.Config.Authentication.Authenticator = anonymous.NewAuthenticator(nil)
	recommended.Config.Authorization.Authorizer = authorizerfactory.NewAlwaysAllowAuthorizer()

	// EffectiveVersion is required by Complete(); use a minimal one.
	recommended.Config.EffectiveVersion = basecompatibility.NewEffectiveVersionFromString("1.34.0", "", "")

	// OpenAPI v2 is a heavy chain of metav1/version types that we don't
	// want to inline. Skip /openapi/v2 entirely; v3 is sufficient for
	// kubectl discovery + dynamic clients in this experiment.
	defNamer := openapinamer.NewDefinitionNamer(scheme)
	recommended.Config.OpenAPIConfig = nil
	recommended.Config.OpenAPIV3Config = genericserver.DefaultOpenAPIV3Config(minimalOpenAPIDefinitions, defNamer)
	recommended.Config.OpenAPIV3Config.Info.Title = "experiment-k8s-apiserver"

	// We don't run any controllers / informers / lease backed features.
	// The recommended config defaults are otherwise fine.

	completed := recommended.Complete()
	server, err := completed.New("experiment-k8s-apiserver", genericserver.NewEmptyDelegate())
	if err != nil {
		return nil, fmt.Errorf("create server: %w", err)
	}

	// --- REST storage ---
	groupInfo := genericserver.NewDefaultAPIGroupInfo(corev1.GroupName, scheme, runtime.NewParameterCodec(scheme), codecs)
	v1Storage, err := buildV1Storage(scheme, opts.DataDir, storageCodec)
	if err != nil {
		return nil, err
	}
	logREST, err := newPodLogREST(opts.KubeletLogURL, nil)
	if err != nil {
		return nil, fmt.Errorf("pod log subresource: %w", err)
	}
	v1Storage["pods/log"] = logREST
	groupInfo.VersionedResourcesStorageMap["v1"] = v1Storage

	if err := server.InstallLegacyAPIGroup(genericserver.DefaultLegacyAPIPrefix, &groupInfo); err != nil {
		return nil, fmt.Errorf("install /api/v1: %w", err)
	}
	return server, nil
}

// buildV1Storage constructs the REST storage map for /api/v1.
func buildV1Storage(scheme *runtime.Scheme, dataDir string, codec runtime.Codec) (map[string]rest.Storage, error) {
	// One Counter shared across all resources rooted at dataDir, so RVs
	// remain globally monotonic.
	counter, err := fsstorage.NewCounter(dataDir)
	if err != nil {
		return nil, fmt.Errorf("rv counter: %w", err)
	}

	pods, err := newStore(resourceConfig{
		resource:    "pods",
		singular:    "pod",
		prefix:      "/pods",
		namespaced:  true,
		newFunc:     func() runtime.Object { return &corev1.Pod{} },
		newListFunc: func() runtime.Object { return &corev1.PodList{} },
		getAttrs: func(obj runtime.Object) (labels.Set, fields.Set, error) {
			p, ok := obj.(*corev1.Pod)
			if !ok {
				return nil, nil, fmt.Errorf("not a Pod: %T", obj)
			}
			return labels.Set(p.Labels), fields.Set{
				"metadata.name":      p.Name,
				"metadata.namespace": p.Namespace,
				"spec.nodeName":      p.Spec.NodeName,
				"status.phase":       string(p.Status.Phase),
			}, nil
		},
		create: newPodStrategy(scheme),
		update: newPodStrategy(scheme),
		del:    newPodStrategy(scheme),
	}, dataDir, codec, counter)
	if err != nil {
		return nil, err
	}

	podStatus := derivePodStatusStore(pods, newPodStatusStrategy(scheme))

	namespaces, err := newStore(resourceConfig{
		resource:    "namespaces",
		singular:    "namespace",
		prefix:      "/namespaces",
		namespaced:  false,
		newFunc:     func() runtime.Object { return &corev1.Namespace{} },
		newListFunc: func() runtime.Object { return &corev1.NamespaceList{} },
		getAttrs: func(obj runtime.Object) (labels.Set, fields.Set, error) {
			ns, ok := obj.(*corev1.Namespace)
			if !ok {
				return nil, nil, fmt.Errorf("not a Namespace: %T", obj)
			}
			return labels.Set(ns.Labels), fields.Set{
				"metadata.name": ns.Name,
				"status.phase":  string(ns.Status.Phase),
			}, nil
		},
		create: newNamespaceStrategy(scheme),
		update: newNamespaceStrategy(scheme),
		del:    newNamespaceStrategy(scheme),
	}, dataDir, codec, counter)
	if err != nil {
		return nil, err
	}

	return map[string]rest.Storage{
		"pods":        pods,
		"pods/status": podStatus,
		"namespaces":  namespaces,
	}, nil
}

// Run prepares and runs the server until ctx is canceled.
func Run(ctx context.Context, server *genericserver.GenericAPIServer) error {
	return server.PrepareRun().RunWithContext(ctx)
}

// Helper: small wait used by callers / tests to ensure the readyz endpoint
// responds positively before issuing client traffic.
func WaitForReady(ctx context.Context, dialAddr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		c, err := net.DialTimeout("tcp", dialAddr, 500*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("apiserver at %s not ready", dialAddr)
}

// ensure unused import warnings are pinned where intentional.
var _ = genericregistry.Store{}
