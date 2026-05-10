package apiserver

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestPodGracefulStrategy_DefaultsTo30s(t *testing.T) {
	s := newPodGracefulStrategy(runtime.NewScheme())
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p"}}
	opts := &metav1.DeleteOptions{}
	if !s.CheckGracefulDelete(context.Background(), pod, opts) {
		t.Fatal("expected graceful delete")
	}
	if opts.GracePeriodSeconds == nil || *opts.GracePeriodSeconds != 30 {
		t.Errorf("default grace=%v want 30", opts.GracePeriodSeconds)
	}
}

func TestPodGracefulStrategy_HonorsSpecTermination(t *testing.T) {
	s := newPodGracefulStrategy(runtime.NewScheme())
	tg := int64(7)
	pod := &corev1.Pod{Spec: corev1.PodSpec{TerminationGracePeriodSeconds: &tg}}
	opts := &metav1.DeleteOptions{}
	if !s.CheckGracefulDelete(context.Background(), pod, opts) {
		t.Fatal("expected graceful delete")
	}
	if opts.GracePeriodSeconds == nil || *opts.GracePeriodSeconds != 7 {
		t.Errorf("grace=%v want 7 from spec", opts.GracePeriodSeconds)
	}
}

func TestPodGracefulStrategy_ZeroIsNotGraceful(t *testing.T) {
	s := newPodGracefulStrategy(runtime.NewScheme())
	zero := int64(0)
	opts := &metav1.DeleteOptions{GracePeriodSeconds: &zero}
	if s.CheckGracefulDelete(context.Background(), &corev1.Pod{}, opts) {
		t.Errorf("grace=0 must not be treated as graceful")
	}
}

func TestPodGracefulStrategy_UserOverridePreserved(t *testing.T) {
	s := newPodGracefulStrategy(runtime.NewScheme())
	tg := int64(60)
	pod := &corev1.Pod{Spec: corev1.PodSpec{TerminationGracePeriodSeconds: &tg}}
	user := int64(5)
	opts := &metav1.DeleteOptions{GracePeriodSeconds: &user}
	if !s.CheckGracefulDelete(context.Background(), pod, opts) {
		t.Fatal("expected graceful")
	}
	if *opts.GracePeriodSeconds != 5 {
		t.Errorf("grace=%d want 5 (user value preserved)", *opts.GracePeriodSeconds)
	}
}
