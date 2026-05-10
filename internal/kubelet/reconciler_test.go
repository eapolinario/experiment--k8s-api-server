package kubelet

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

// terminateDocker captures Stop+Inspect calls for assertions.
type terminateDocker struct {
	exists       bool
	inspectErr   error
	stopCalls    int32
	stopGrace    time.Duration
	stopErr      error
}

func (d *terminateDocker) EnsurePulled(context.Context, string, string) error { return nil }
func (d *terminateDocker) CreateAndStart(context.Context, string, ContainerSpec) (string, error) {
	return "", nil
}
func (d *terminateDocker) Inspect(_ context.Context, _ string) (ContainerView, bool, error) {
	return ContainerView{}, d.exists, d.inspectErr
}
func (d *terminateDocker) Stop(_ context.Context, _ string, g time.Duration) error {
	atomic.AddInt32(&d.stopCalls, 1)
	d.stopGrace = g
	return d.stopErr
}
func (d *terminateDocker) Remove(context.Context, string) error { return nil }
func (d *terminateDocker) ListManagedUIDs(context.Context) (map[string]string, error) {
	return nil, nil
}
func (d *terminateDocker) Logs(context.Context, string, LogOptions) (io.ReadCloser, error) {
	return nil, errors.New("not used")
}
func (d *terminateDocker) Close() error { return nil }

func mkTerminatingPod(grace int64) *corev1.Pod {
	now := metav1.Now()
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:                       "nginx",
			Namespace:                  "default",
			UID:                        types.UID("u1"),
			DeletionTimestamp:          &now,
			DeletionGracePeriodSeconds: &grace,
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "nginx", Image: "nginx"}}},
	}
}

// reconcilerWith builds a Reconciler around a fake kube client + provided docker.
func reconcilerWith(kube *fake.Clientset, docker DockerRuntime) *Reconciler {
	return &Reconciler{
		kube:       kube,
		docker:     docker,
		recorder:   NewEventRecorder(kube, "test-host"),
		failedPods: map[types.UID]string{},
	}
}

func TestTerminatePod_StopsContainerAndForceDeletes(t *testing.T) {
	kube := fake.NewClientset()
	var deleteCalled bool
	var deleteGrace *int64
	kube.PrependReactor("delete", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		da := action.(clienttesting.DeleteAction)
		deleteCalled = true
		deleteGrace = da.GetDeleteOptions().GracePeriodSeconds
		return true, nil, nil
	})
	doc := &terminateDocker{exists: true}

	r := reconcilerWith(kube, doc)
	pod := mkTerminatingPod(7)

	if err := r.reconcilePod(context.Background(), pod); err != nil {
		t.Fatalf("reconcilePod: %v", err)
	}
	if atomic.LoadInt32(&doc.stopCalls) != 1 {
		t.Errorf("docker.Stop calls=%d want 1", doc.stopCalls)
	}
	if doc.stopGrace != 7*time.Second {
		t.Errorf("stop grace=%s want 7s", doc.stopGrace)
	}
	if !deleteCalled {
		t.Errorf("expected force-delete via apiserver")
	}
	if deleteGrace == nil || *deleteGrace != 0 {
		t.Errorf("delete grace=%v want 0 (force)", deleteGrace)
	}
}

func TestTerminatePod_NoContainerStillForceDeletes(t *testing.T) {
	kube := fake.NewClientset()
	var deleteCalled bool
	kube.PrependReactor("delete", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		deleteCalled = true
		return true, nil, nil
	})
	doc := &terminateDocker{exists: false}
	r := reconcilerWith(kube, doc)

	if err := r.reconcilePod(context.Background(), mkTerminatingPod(5)); err != nil {
		t.Fatalf("reconcilePod: %v", err)
	}
	if doc.stopCalls != 0 {
		t.Errorf("stop should not be called when container missing, calls=%d", doc.stopCalls)
	}
	if !deleteCalled {
		t.Errorf("force-delete still expected so apiserver finalizes the object")
	}
}

func TestTerminatePod_NotFoundOnDeleteIsBenign(t *testing.T) {
	kube := fake.NewClientset()
	kube.PrependReactor("delete", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, "nginx")
	})
	doc := &terminateDocker{exists: true}
	r := reconcilerWith(kube, doc)

	if err := r.reconcilePod(context.Background(), mkTerminatingPod(2)); err != nil {
		t.Errorf("404 on final delete should be swallowed; got %v", err)
	}
}

func TestTerminatePod_DefaultsGraceWhenSpecOmitted(t *testing.T) {
	kube := fake.NewClientset()
	doc := &terminateDocker{exists: true}
	r := reconcilerWith(kube, doc)

	pod := mkTerminatingPod(0)
	pod.DeletionGracePeriodSeconds = nil // omitted entirely
	if err := r.reconcilePod(context.Background(), pod); err != nil {
		t.Fatalf("reconcilePod: %v", err)
	}
	if doc.stopGrace != 30*time.Second {
		t.Errorf("default grace=%s want 30s", doc.stopGrace)
	}
}

func TestTerminatePod_EmitsKillingEvent(t *testing.T) {
	kube := fake.NewClientset()
	doc := &terminateDocker{exists: true}
	r := reconcilerWith(kube, doc)

	if err := r.reconcilePod(context.Background(), mkTerminatingPod(3)); err != nil {
		t.Fatalf("reconcilePod: %v", err)
	}
	events, err := kube.CoreV1().Events("default").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	var killing *corev1.Event
	for i := range events.Items {
		if events.Items[i].Reason == EventReasonKilling {
			killing = &events.Items[i]
			break
		}
	}
	if killing == nil {
		t.Fatalf("expected a Killing event; got %+v", events.Items)
	}
	if killing.Type != corev1.EventTypeNormal {
		t.Errorf("Killing event Type=%q want Normal", killing.Type)
	}
	if killing.InvolvedObject.Kind != "Pod" || killing.InvolvedObject.Name != "nginx" {
		t.Errorf("InvolvedObject=%+v", killing.InvolvedObject)
	}
	if killing.Source.Component != "kubelet-lite" {
		t.Errorf("Source.Component=%q want kubelet-lite", killing.Source.Component)
	}
}

func TestTerminatePod_NoKillingEventWhenContainerMissing(t *testing.T) {
	kube := fake.NewClientset()
	doc := &terminateDocker{exists: false}
	r := reconcilerWith(kube, doc)

	if err := r.reconcilePod(context.Background(), mkTerminatingPod(3)); err != nil {
		t.Fatalf("reconcilePod: %v", err)
	}
	events, _ := kube.CoreV1().Events("default").List(context.Background(), metav1.ListOptions{})
	for _, e := range events.Items {
		if e.Reason == EventReasonKilling {
			t.Errorf("did not expect Killing event when container absent: %+v", e)
		}
	}
}
