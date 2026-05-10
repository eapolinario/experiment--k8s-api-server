package apiserver

import (
	"k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// minimalOpenAPIDefinitions returns just-enough OpenAPI v3 definitions to
// satisfy the generic apiserver's installAPIResources path. We return
// `type: object` schemas with no properties for each canonical Go type
// name we register. kubectl discovery still works; client-side schema
// validation is approximate.
//
// TODO(openapi): swap in the upstream generated GetOpenAPIDefinitions if
// we ever vendor pkg/generated/openapi.
func minimalOpenAPIDefinitions(_ common.ReferenceCallback) map[string]common.OpenAPIDefinition {
	objectSchema := func() spec.Schema {
		return spec.Schema{SchemaProps: spec.SchemaProps{Type: spec.StringOrArray{"object"}}}
	}
	defs := map[string]common.OpenAPIDefinition{}
	for _, name := range []string{
		"io.k8s.api.core.v1.Pod",
		"io.k8s.api.core.v1.PodList",
		"io.k8s.api.core.v1.PodSpec",
		"io.k8s.api.core.v1.PodStatus",
		"io.k8s.api.core.v1.Namespace",
		"io.k8s.api.core.v1.NamespaceList",
		"io.k8s.api.core.v1.NamespaceSpec",
		"io.k8s.api.core.v1.NamespaceStatus",
		"io.k8s.api.core.v1.Event",
		"io.k8s.api.core.v1.EventList",
		"io.k8s.api.core.v1.EventSource",
		"io.k8s.api.core.v1.EventSeries",
		"io.k8s.api.core.v1.ObjectReference",
		"io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta",
		"io.k8s.apimachinery.pkg.apis.meta.v1.ListMeta",
		"io.k8s.apimachinery.pkg.apis.meta.v1.Status",
	} {
		defs[name] = common.OpenAPIDefinition{Schema: objectSchema()}
	}
	return defs
}
