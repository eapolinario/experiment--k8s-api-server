// Package kubelet — events.go
//
// Minimal event recorder for kubelet-lite. Real kubelet uses
// k8s.io/client-go/tools/record with a per-broadcaster sink that
// aggregates duplicate events into series; we instead create one
// Event object per call (small loss of fidelity, big drop in
// complexity) and rely on the apiserver's stable RV ordering to
// give `kubectl describe pod` chronological output.
//
// Reasons emitted match upstream kubelet conventions so anyone with
// k8s muscle memory recognises them:
//
//   Pulling   — about to call docker pull
//   Pulled    — image is available locally
//   Failed    — pull / create / start error (Type=Warning)
//   Created   — container created in docker
//   Started   — container is running
//   Killing   — graceful termination has begun
//
// All events use Source.Component="kubelet-lite" so they're
// distinguishable from controller events later.
package kubelet

import (
	"context"
	"fmt"
	"sync/atomic"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const (
	EventReasonPulling = "Pulling"
	EventReasonPulled  = "Pulled"
	EventReasonFailed  = "Failed"
	EventReasonCreated = "Created"
	EventReasonStarted = "Started"
	EventReasonKilling = "Killing"

	eventSourceComponent = "kubelet-lite"
)

// EventRecorder posts corev1.Event objects to the apiserver about a target
// object (typically a Pod). Calls are best-effort: failures are logged and
// swallowed so an apiserver hiccup never blocks reconciliation.
type EventRecorder interface {
	Event(ctx context.Context, target *corev1.Pod, eventType, reason, message string)
	Eventf(ctx context.Context, target *corev1.Pod, eventType, reason, format string, args ...interface{})
}

type eventRecorder struct {
	kube kubernetes.Interface
	// host is the kubelet identity recorded on each event so multiple
	// kubelets in a cluster (we only run one, but parity with upstream
	// helps debugging) can be told apart.
	host string
	// seq monotonically increments to disambiguate event names emitted in
	// the same nanosecond.
	seq atomic.Uint64
}

// NewEventRecorder returns an EventRecorder that writes to kube. host is
// recorded as Source.Host and ReportingInstance.
func NewEventRecorder(kube kubernetes.Interface, host string) EventRecorder {
	return &eventRecorder{kube: kube, host: host}
}

func (r *eventRecorder) Event(ctx context.Context, target *corev1.Pod, eventType, reason, message string) {
	if target == nil {
		return
	}
	// Event names mirror upstream: <involvedName>.<base36 timestamp>.<seq>.
	// Uniqueness only matters within a namespace, but adding the seq
	// avoids collisions when several events fire in the same call site.
	now := metav1.Now()
	r.seq.Add(1)
	name := fmt.Sprintf("%s.%x.%d", target.Name, now.UnixNano(), r.seq.Load())
	if len(name) > 253 {
		// Defensive: extremely long pod names — fall back to a UID suffix.
		name = fmt.Sprintf("%s.%s", target.Name[:200], uuid.NewUUID())
	}
	e := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: target.Namespace,
		},
		InvolvedObject: corev1.ObjectReference{
			Kind:            "Pod",
			Namespace:       target.Namespace,
			Name:            target.Name,
			UID:             target.UID,
			APIVersion:      "v1",
			ResourceVersion: target.ResourceVersion,
		},
		Reason:              reason,
		Message:             message,
		Type:                eventType,
		FirstTimestamp:      now,
		LastTimestamp:       now,
		EventTime:           metav1.NewMicroTime(now.Time),
		Count:               1,
		Source:              corev1.EventSource{Component: eventSourceComponent, Host: r.host},
		ReportingController: "kubernetes.io/kubelet-lite",
		ReportingInstance:   r.host,
	}
	if _, err := r.kube.CoreV1().Events(target.Namespace).Create(ctx, e, metav1.CreateOptions{}); err != nil {
		// Don't propagate — event posting must never gate reconciliation.
		klog.V(2).Infof("event post failed (%s/%s reason=%s): %v", target.Namespace, target.Name, reason, err)
	}
}

func (r *eventRecorder) Eventf(ctx context.Context, target *corev1.Pod, eventType, reason, format string, args ...interface{}) {
	r.Event(ctx, target, eventType, reason, fmt.Sprintf(format, args...))
}

// noopEventRecorder is the zero-value used when a Reconciler is built
// without a recorder (older callers / tests). All methods are no-ops.
type noopEventRecorder struct{}

func (noopEventRecorder) Event(context.Context, *corev1.Pod, string, string, string) {}
func (noopEventRecorder) Eventf(context.Context, *corev1.Pod, string, string, string, ...interface{}) {
}

// Compile-time check that the recorder shape doesn't drift.
var _ EventRecorder = (*eventRecorder)(nil)
var _ EventRecorder = noopEventRecorder{}
