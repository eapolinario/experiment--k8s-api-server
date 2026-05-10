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

func TestSecretStrategy_StringDataFolded(t *testing.T) {
	s := newSecretStrategy(runtime.NewScheme())
	sec := &corev1.Secret{
		StringData: map[string]string{"TOKEN": "s3cret", "USER": "alice"},
		Data:       map[string][]byte{"USER": []byte("OVERWRITE_ME")},
	}
	s.PrepareForCreate(context.Background(), sec)
	if sec.StringData != nil {
		t.Errorf("StringData should be cleared, got %v", sec.StringData)
	}
	if string(sec.Data["TOKEN"]) != "s3cret" {
		t.Errorf("TOKEN=%q want s3cret", sec.Data["TOKEN"])
	}
	if string(sec.Data["USER"]) != "alice" {
		t.Errorf("USER=%q want alice (StringData should win over Data)", sec.Data["USER"])
	}

	// Same on update.
	sec2 := &corev1.Secret{StringData: map[string]string{"K": "v"}}
	s.PrepareForUpdate(context.Background(), sec2, &corev1.Secret{})
	if string(sec2.Data["K"]) != "v" {
		t.Errorf("PrepareForUpdate didn't fold StringData; got %v", sec2.Data)
	}
}
