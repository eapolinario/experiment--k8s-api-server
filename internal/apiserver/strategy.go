// Package apiserver wires the generic apiserver: scheme, REST storage,
// authentication/authorization, and serving config.
package apiserver

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
)

// objectStrategy is a minimal RESTCreate/Update/DeleteStrategy.
//
// We deliberately skip k8s.io/kubernetes/pkg/registry/core/{pod,namespace}
// to avoid pulling kubernetes/kubernetes as a dependency. For the
// experiment, validation is handled by the API machinery (decode +
// metadata) plus the few invariants this strategy enforces.
type objectStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
	namespaceScoped bool
	// resetStatus wipes the .status field on create. Only set for kinds
	// that have a status subresource (Pod, Namespace).
	resetStatus func(obj runtime.Object)
}

func newPodStrategy(typer runtime.ObjectTyper) *objectStrategy {
	return &objectStrategy{
		ObjectTyper:     typer,
		NameGenerator:   names.SimpleNameGenerator,
		namespaceScoped: true,
		resetStatus: func(obj runtime.Object) {
			if p, ok := obj.(*corev1.Pod); ok {
				p.Status = corev1.PodStatus{}
			}
		},
	}
}

func newNamespaceStrategy(typer runtime.ObjectTyper) *objectStrategy {
	return &objectStrategy{
		ObjectTyper:     typer,
		NameGenerator:   names.SimpleNameGenerator,
		namespaceScoped: false,
		resetStatus: func(obj runtime.Object) {
			if ns, ok := obj.(*corev1.Namespace); ok {
				ns.Status = corev1.NamespaceStatus{}
				if ns.Spec.Finalizers == nil {
					ns.Spec.Finalizers = []corev1.FinalizerName{corev1.FinalizerKubernetes}
				}
			}
		},
	}
}

func (s *objectStrategy) NamespaceScoped() bool { return s.namespaceScoped }

func (s *objectStrategy) PrepareForCreate(_ context.Context, obj runtime.Object) {
	if s.resetStatus != nil {
		s.resetStatus(obj)
	}
}

func (s *objectStrategy) Validate(_ context.Context, _ runtime.Object) field.ErrorList {
	return nil
}

func (s *objectStrategy) WarningsOnCreate(_ context.Context, _ runtime.Object) []string {
	return nil
}

func (s *objectStrategy) Canonicalize(_ runtime.Object) {}

func (s *objectStrategy) AllowCreateOnUpdate() bool { return false }

func (s *objectStrategy) PrepareForUpdate(_ context.Context, _, _ runtime.Object) {}

func (s *objectStrategy) ValidateUpdate(_ context.Context, _, _ runtime.Object) field.ErrorList {
	return nil
}

func (s *objectStrategy) WarningsOnUpdate(_ context.Context, _, _ runtime.Object) []string {
	return nil
}

func (s *objectStrategy) AllowUnconditionalUpdate() bool { return true }

var _ rest.RESTCreateStrategy = &objectStrategy{}
var _ rest.RESTUpdateStrategy = &objectStrategy{}
var _ rest.RESTDeleteStrategy = &objectStrategy{}

// podStatusStrategy only allows mutation of .status on update.
type podStatusStrategy struct {
	*objectStrategy
}

func newPodStatusStrategy(typer runtime.ObjectTyper) *podStatusStrategy {
	return &podStatusStrategy{objectStrategy: newPodStrategy(typer)}
}

func (s *podStatusStrategy) PrepareForUpdate(_ context.Context, obj, old runtime.Object) {
	newPod, _ := obj.(*corev1.Pod)
	oldPod, _ := old.(*corev1.Pod)
	if newPod == nil || oldPod == nil {
		return
	}
	// Only status changes are allowed on the /status subresource. Replay
	// spec and metadata from the existing object.
	newPod.Spec = oldPod.Spec
	newPod.ObjectMeta = oldPod.ObjectMeta
}
