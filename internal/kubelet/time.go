package kubelet

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Tiny indirection so translate_test.go can be deterministic if we ever want
// to inject a clock. Today these are direct passthroughs.
func metav1Now() metav1.Time          { return metav1.Now() }
func metav1NewTime(t time.Time) metav1.Time {
	return metav1.NewTime(t)
}
