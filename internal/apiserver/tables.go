// Package apiserver — tables.go
//
// Custom rest.TableConvertor implementations so `kubectl get pods` /
// `kubectl get namespaces` render the columns a Kubernetes user expects
// (READY/STATUS/RESTARTS/AGE for Pods, STATUS/AGE for Namespaces) instead
// of the bare NAME/CREATED AT default.
package apiserver

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/rest"
)

// podTableConvertor renders a pods list as the canonical kubectl table.
type podTableConvertor struct{}

var _ rest.TableConvertor = podTableConvertor{}

var podColumns = []metav1.TableColumnDefinition{
	{Name: "Name", Type: "string", Format: "name"},
	{Name: "Ready", Type: "string", Description: "Ready containers / total"},
	{Name: "Status", Type: "string", Description: "Pod phase or container reason"},
	{Name: "Restarts", Type: "integer", Description: "Sum of container restart counts"},
	{Name: "Age", Type: "string", Description: "Time since creation"},
	{Name: "IP", Type: "string", Priority: 1, Description: "Pod IP"},
	{Name: "Node", Type: "string", Priority: 1, Description: "Scheduled node"},
}

func (podTableConvertor) ConvertToTable(_ context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	t := &metav1.Table{}
	if !tableNoHeaders(tableOptions) {
		t.ColumnDefinitions = podColumns
	}
	add := func(p *corev1.Pod) {
		ready, total, restarts := podReadinessCounts(p)
		t.Rows = append(t.Rows, metav1.TableRow{
			Cells: []interface{}{
				p.Name,
				fmt.Sprintf("%d/%d", ready, total),
				podDisplayStatus(p),
				restarts,
				translateTimestampSince(p.CreationTimestamp),
				stringOrNone(p.Status.PodIP),
				stringOrNone(p.Spec.NodeName),
			},
			Object: runtime.RawExtension{Object: p},
		})
	}
	switch o := object.(type) {
	case *corev1.Pod:
		add(o)
	case *corev1.PodList:
		t.ResourceVersion = o.ResourceVersion
		for i := range o.Items {
			add(&o.Items[i])
		}
	default:
		return nil, fmt.Errorf("podTableConvertor: unexpected type %T", object)
	}
	return t, nil
}

// namespaceTableConvertor renders namespaces as NAME / STATUS / AGE.
type namespaceTableConvertor struct{}

var _ rest.TableConvertor = namespaceTableConvertor{}

var namespaceColumns = []metav1.TableColumnDefinition{
	{Name: "Name", Type: "string", Format: "name"},
	{Name: "Status", Type: "string", Description: "Namespace phase"},
	{Name: "Age", Type: "string", Description: "Time since creation"},
}

func (namespaceTableConvertor) ConvertToTable(_ context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	t := &metav1.Table{}
	if !tableNoHeaders(tableOptions) {
		t.ColumnDefinitions = namespaceColumns
	}
	add := func(ns *corev1.Namespace) {
		phase := string(ns.Status.Phase)
		if phase == "" {
			phase = "Active"
		}
		t.Rows = append(t.Rows, metav1.TableRow{
			Cells: []interface{}{
				ns.Name,
				phase,
				translateTimestampSince(ns.CreationTimestamp),
			},
			Object: runtime.RawExtension{Object: ns},
		})
	}
	switch o := object.(type) {
	case *corev1.Namespace:
		add(o)
	case *corev1.NamespaceList:
		t.ResourceVersion = o.ResourceVersion
		for i := range o.Items {
			add(&o.Items[i])
		}
	default:
		return nil, fmt.Errorf("namespaceTableConvertor: unexpected type %T", object)
	}
	if m, err := meta.ListAccessor(object); err == nil {
		t.ResourceVersion = m.GetResourceVersion()
	}
	return t, nil
}

// podDisplayStatus mirrors the column kubectl prints in the STATUS slot:
// terminated containers report Reason; otherwise the Pod phase. A pod with
// metadata.deletionTimestamp set renders as "Terminating" regardless of
// phase, matching kubectl behaviour.
func podDisplayStatus(p *corev1.Pod) string {
	if p.DeletionTimestamp != nil {
		return "Terminating"
	}
	for _, cs := range p.Status.ContainerStatuses {
		if cs.State.Waiting != nil && cs.State.Waiting.Reason != "" {
			return cs.State.Waiting.Reason
		}
		if cs.State.Terminated != nil && cs.State.Terminated.Reason != "" {
			return cs.State.Terminated.Reason
		}
	}
	if p.Status.Reason != "" {
		return p.Status.Reason
	}
	if p.Status.Phase == "" {
		return "Pending"
	}
	return string(p.Status.Phase)
}

// podReadinessCounts returns (ready, total, restarts).
func podReadinessCounts(p *corev1.Pod) (int, int, int32) {
	total := len(p.Spec.Containers)
	ready := 0
	var restarts int32
	for _, cs := range p.Status.ContainerStatuses {
		if cs.Ready {
			ready++
		}
		restarts += cs.RestartCount
	}
	return ready, total, restarts
}

// translateTimestampSince formats an age string ("3m12s", "2h", "5d") akin to
// kubectl's duration printer. Zero-time renders "<unknown>".
func translateTimestampSince(t metav1.Time) string {
	if t.IsZero() {
		return "<unknown>"
	}
	return shortHumanDuration(time.Since(t.Time))
}

// shortHumanDuration is a tiny port of duration.HumanDuration's output for
// the ranges kubectl actually shows in tables.
func shortHumanDuration(d time.Duration) string {
	switch {
	case d < time.Second:
		return "0s"
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	case d < 365*24*time.Hour:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	default:
		return fmt.Sprintf("%dy", int(d.Hours()/24/365))
	}
}

func stringOrNone(s string) string {
	if s == "" {
		return "<none>"
	}
	return s
}

func tableNoHeaders(opts runtime.Object) bool {
	if o, ok := opts.(*metav1.TableOptions); ok && o != nil {
		return o.NoHeaders
	}
	return false
}
