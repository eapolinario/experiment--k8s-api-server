// Package apiserver — fields.go
//
// Field-selector allowlists per resource. apimachinery's default
// conversion only accepts metadata.{name,namespace} for any kind that
// hasn't registered its own conversion func, which means
// `kubectl get events --field-selector involvedObject.name=nginx`
// (the selector kubectl describe issues internally) would otherwise be
// rejected with a 400.
//
// Real k8s registers these via per-package init in
// k8s.io/kubernetes/pkg/apis/core/v1/conversion.go; we inline the
// minimal set we need.
package apiserver

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// registerFieldLabelConversions wires AddFieldLabelConversionFunc for
// every kind whose getAttrs function returns fields beyond
// metadata.{name,namespace}.
func registerFieldLabelConversions(scheme *runtime.Scheme) error {
	if err := scheme.AddFieldLabelConversionFunc(
		corev1.SchemeGroupVersion.WithKind("Pod"),
		passthroughFieldSelectors(podSelectableFields),
	); err != nil {
		return err
	}
	if err := scheme.AddFieldLabelConversionFunc(
		corev1.SchemeGroupVersion.WithKind("Namespace"),
		passthroughFieldSelectors(namespaceSelectableFields),
	); err != nil {
		return err
	}
	if err := scheme.AddFieldLabelConversionFunc(
		corev1.SchemeGroupVersion.WithKind("Event"),
		passthroughFieldSelectors(eventSelectableFields),
	); err != nil {
		return err
	}
	if err := scheme.AddFieldLabelConversionFunc(
		corev1.SchemeGroupVersion.WithKind("Secret"),
		passthroughFieldSelectors(secretSelectableFields),
	); err != nil {
		return err
	}
	return nil
}

// passthroughFieldSelectors returns a FieldLabelConversionFunc that
// accepts metadata.{name,namespace} plus the supplied set of additional
// label/value pairs unchanged.
func passthroughFieldSelectors(allowed map[string]struct{}) runtime.FieldLabelConversionFunc {
	return func(label, value string) (string, string, error) {
		switch label {
		case "metadata.name", "metadata.namespace":
			return label, value, nil
		}
		if _, ok := allowed[label]; ok {
			return label, value, nil
		}
		return runtime.DefaultMetaV1FieldSelectorConversion(label, value)
	}
}

var podSelectableFields = map[string]struct{}{
	"spec.nodeName": {},
	"status.phase":  {},
}

var namespaceSelectableFields = map[string]struct{}{
	"status.phase": {},
}

var secretSelectableFields = map[string]struct{}{
	"type": {},
}

// Mirror the keys our event getAttrs publishes; this is what
// `kubectl describe` and `kubectl get events --field-selector` rely on.
var eventSelectableFields = map[string]struct{}{
	"involvedObject.kind":            {},
	"involvedObject.namespace":       {},
	"involvedObject.name":            {},
	"involvedObject.uid":             {},
	"involvedObject.apiVersion":      {},
	"involvedObject.resourceVersion": {},
	"involvedObject.fieldPath":       {},
	"reason":                         {},
	"reportingComponent":             {},
	"source":                         {},
	"type":                           {},
}
