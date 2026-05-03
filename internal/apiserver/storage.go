package apiserver

import (
	"context"
	"fmt"

	"github.com/eapolinario/experiment-k8s-api-server/internal/fsstorage"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/endpoints/request"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"
)

// resourceConfig describes one resource we want to register.
type resourceConfig struct {
	resource    string // "pods"
	singular    string // "pod"
	prefix      string // "/pods"
	namespaced  bool
	newFunc     func() runtime.Object
	newListFunc func() runtime.Object
	getAttrs    func(obj runtime.Object) (labels.Set, fields.Set, error)
	create      rest.RESTCreateStrategy
	update      rest.RESTUpdateStrategy
	del         rest.RESTDeleteStrategy
}

// newStore wires a genericregistry.Store backed by an fsstorage.Interface.
// counter is the shared resourceVersion counter rooted at dataRoot — pass
// the same Counter into every newStore call sharing dataRoot to keep RVs
// globally monotonic.
func newStore(rc resourceConfig, dataRoot string, codec runtime.Codec, counter *fsstorage.Counter) (*genericregistry.Store, error) {
	// Storage keys are already namespaced under rc.prefix (e.g. "/pods/..."),
	// so the on-disk layout is dataRoot/pods/<namespace>/<name>.json without
	// duplicating the resource name.
	fs, err := fsstorage.New(fsstorage.Config{
		Root:      dataRoot,
		Codec:     codec,
		Newer:     rc.newFunc,
		NewerList: rc.newListFunc,
		Counter:   counter,
	})
	if err != nil {
		return nil, fmt.Errorf("fsstorage(%s): %w", rc.resource, err)
	}

	gr := schema.GroupResource{Resource: rc.resource}
	sgr := schema.GroupResource{Resource: rc.singular}

	store := &genericregistry.Store{
		NewFunc:                   rc.newFunc,
		NewListFunc:               rc.newListFunc,
		DefaultQualifiedResource:  gr,
		SingularQualifiedResource: sgr,

		KeyRootFunc: func(ctx context.Context) string {
			if !rc.namespaced {
				return rc.prefix
			}
			ns, _ := request.NamespaceFrom(ctx)
			if ns == "" {
				return rc.prefix
			}
			return rc.prefix + "/" + ns
		},
		KeyFunc: func(ctx context.Context, name string) (string, error) {
			if rc.namespaced {
				ns, ok := request.NamespaceFrom(ctx)
				if !ok || ns == "" {
					return "", fmt.Errorf("namespace required for resource %q", rc.resource)
				}
				return rc.prefix + "/" + ns + "/" + name, nil
			}
			return rc.prefix + "/" + name, nil
		},
		ObjectNameFunc: func(obj runtime.Object) (string, error) {
			a, err := meta.Accessor(obj)
			if err != nil {
				return "", err
			}
			return a.GetName(), nil
		},
		PredicateFunc: func(label labels.Selector, fld fields.Selector) storage.SelectionPredicate {
			return storage.SelectionPredicate{
				Label:    label,
				Field:    fld,
				GetAttrs: rc.getAttrs,
			}
		},

		CreateStrategy: rc.create,
		UpdateStrategy: rc.update,
		DeleteStrategy: rc.del,

		TableConvertor: rest.NewDefaultTableConvertor(gr),
	}
	store.Storage.Storage = fs
	store.Storage.Codec = codec
	store.ReadinessCheckFunc = fs.ReadinessCheck
	return store, nil
}

// derivePodStatusStore returns a Store that updates only Pod /status.
// It shares the same underlying fsstorage with the main pod store so
// reads/writes are consistent.
func derivePodStatusStore(podStore *genericregistry.Store, statusStrategy rest.RESTUpdateStrategy) *genericregistry.Store {
	clone := *podStore
	clone.UpdateStrategy = statusStrategy
	// CreateStrategy is irrelevant for /status (no create), but leave it
	// pointing at the parent so the store is internally consistent.
	return &clone
}
