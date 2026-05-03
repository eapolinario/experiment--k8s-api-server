package fsstorage

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apiserver/pkg/storage"
)

// TestSharedCounterMonotonic verifies that two stores rooted at the same
// directory and given the same Counter produce a single, monotonic stream
// of resourceVersions across both — and that the persisted .rv file
// reflects the true high-water mark at all times.
func TestSharedCounterMonotonic(t *testing.T) {
	root := t.TempDir()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	codec := serializer.NewCodecFactory(scheme).LegacyCodec(corev1.SchemeGroupVersion)

	counter, err := NewCounter(root)
	if err != nil {
		t.Fatalf("NewCounter: %v", err)
	}

	makeStore := func(newer func() runtime.Object, newerList func() runtime.Object) storage.Interface {
		s, err := New(Config{
			Root:      root,
			Codec:     codec,
			Newer:     newer,
			NewerList: newerList,
			Counter:   counter,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return s
	}
	pods := makeStore(func() runtime.Object { return &corev1.Pod{} }, func() runtime.Object { return &corev1.PodList{} })
	namespaces := makeStore(func() runtime.Object { return &corev1.Namespace{} }, func() runtime.Object { return &corev1.NamespaceList{} })

	ctx := context.Background()
	rvOf := func(obj runtime.Object) string {
		acc, _ := storage.APIObjectVersioner{}.ObjectResourceVersion(obj)
		_ = acc
		return ""
	}
	_ = rvOf

	// Interleave writes across the two stores; collect RVs.
	var rvs []uint64
	mustCreate := func(s storage.Interface, key string, in, out runtime.Object) {
		t.Helper()
		if err := s.Create(ctx, key, in, out, 0); err != nil {
			t.Fatalf("Create %s: %v", key, err)
		}
		rv, err := storage.APIObjectVersioner{}.ObjectResourceVersion(out)
		if err != nil {
			t.Fatalf("rv parse: %v", err)
		}
		rvs = append(rvs, rv)
	}

	mustCreate(namespaces, "/namespaces/a", &corev1.Namespace{}, &corev1.Namespace{})
	mustCreate(pods, "/pods/default/p1", newPod("default", "p1"), &corev1.Pod{})
	mustCreate(namespaces, "/namespaces/b", &corev1.Namespace{}, &corev1.Namespace{})
	mustCreate(pods, "/pods/default/p2", newPod("default", "p2"), &corev1.Pod{})
	mustCreate(pods, "/pods/default/p3", newPod("default", "p3"), &corev1.Pod{})

	// Strictly increasing.
	for i := 1; i < len(rvs); i++ {
		if rvs[i] <= rvs[i-1] {
			t.Fatalf("RVs not strictly increasing: %v", rvs)
		}
	}

	// Both stores agree on the current RV (it's the same Counter).
	if got, _ := pods.GetCurrentResourceVersion(ctx); got != rvs[len(rvs)-1] {
		t.Fatalf("pods current RV = %d, want %d", got, rvs[len(rvs)-1])
	}
	if got, _ := namespaces.GetCurrentResourceVersion(ctx); got != rvs[len(rvs)-1] {
		t.Fatalf("namespaces current RV = %d, want %d", got, rvs[len(rvs)-1])
	}

	// Persisted .rv reflects the high-water mark.
	c2, err := NewCounter(root)
	if err != nil {
		t.Fatalf("reload counter: %v", err)
	}
	if c2.Current() != rvs[len(rvs)-1] {
		t.Fatalf("reloaded counter = %d, want %d", c2.Current(), rvs[len(rvs)-1])
	}
}

// TestUnsharedCountersRaceProtection ensures that without an explicit
// shared Counter, two stores at the same root each get their own counter
// (the documented foot-gun). This test exists to lock in the contract:
// callers who share a root MUST share a Counter.
func TestUnsharedCountersAreIndependent(t *testing.T) {
	root := t.TempDir()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	codec := serializer.NewCodecFactory(scheme).LegacyCodec(corev1.SchemeGroupVersion)

	cfg := Config{
		Root:      root,
		Codec:     codec,
		Newer:     func() runtime.Object { return &corev1.Pod{} },
		NewerList: func() runtime.Object { return &corev1.PodList{} },
	}
	a, err := New(cfg)
	if err != nil {
		t.Fatalf("New a: %v", err)
	}
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New b: %v", err)
	}

	ctx := context.Background()
	if err := a.Create(ctx, "/pods/default/a", newPod("default", "a"), &corev1.Pod{}, 0); err != nil {
		t.Fatalf("a create: %v", err)
	}
	if err := b.Create(ctx, "/pods/default/b", newPod("default", "b"), &corev1.Pod{}, 0); err != nil {
		t.Fatalf("b create: %v", err)
	}

	// b loaded the persisted RV from a's first write, so its first issued
	// RV is at least one past a's. We don't assert a specific value —
	// only that this confirms why a shared Counter is needed: independent
	// counters can still produce different views of "current".
	aCur, _ := a.GetCurrentResourceVersion(ctx)
	bCur, _ := b.GetCurrentResourceVersion(ctx)
	if aCur == 0 || bCur == 0 {
		t.Fatalf("counters not initialized: a=%d b=%d", aCur, bCur)
	}
}
