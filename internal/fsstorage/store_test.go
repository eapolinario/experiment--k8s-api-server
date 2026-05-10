package fsstorage

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
)

func newTestStore(t *testing.T, root string) storage.Interface {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	codec := serializer.NewCodecFactory(scheme).LegacyCodec(corev1.SchemeGroupVersion)
	s, err := New(Config{
		Root:      root,
		Codec:     codec,
		Newer:     func() runtime.Object { return &corev1.Pod{} },
		NewerList: func() runtime.Object { return &corev1.PodList{} },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func newPod(ns, name string, labelKV ...string) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "c", Image: "nginx"}},
		},
	}
	if len(labelKV) >= 2 {
		p.Labels = map[string]string{labelKV[0]: labelKV[1]}
	}
	return p
}

func keyFor(ns, name string) string {
	if ns == "" {
		return "/pods/" + name
	}
	return "/pods/" + ns + "/" + name
}

func everything() storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label: labels.Everything(),
		Field: fields.Everything(),
		GetAttrs: func(obj runtime.Object) (labels.Set, fields.Set, error) {
			p, ok := obj.(*corev1.Pod)
			if !ok {
				return nil, nil, fmt.Errorf("not a Pod: %T", obj)
			}
			return labels.Set(p.Labels), fields.Set{"metadata.name": p.Name, "metadata.namespace": p.Namespace}, nil
		},
	}
}

func TestCRUDRoundTripAndMonotonicRV(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, t.TempDir())

	var lastRV uint64
	for i := 0; i < 5; i++ {
		in := newPod("default", fmt.Sprintf("pod-%d", i))
		out := &corev1.Pod{}
		if err := s.Create(ctx, keyFor("default", in.Name), in, out, 0); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if out.ResourceVersion == "" {
			t.Fatalf("ResourceVersion not stamped")
		}
		rv, _ := strconv.ParseUint(out.ResourceVersion, 10, 64)
		if rv <= lastRV {
			t.Fatalf("RV not monotonic: %d <= %d", rv, lastRV)
		}
		lastRV = rv

		got := &corev1.Pod{}
		if err := s.Get(ctx, keyFor("default", in.Name), storage.GetOptions{}, got); err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Name != in.Name || got.ResourceVersion != out.ResourceVersion {
			t.Fatalf("round-trip mismatch: got %+v want name=%s rv=%s", got.ObjectMeta, in.Name, out.ResourceVersion)
		}
		if got.UID == "" {
			t.Fatalf("UID not stamped")
		}
	}
}

func TestCreateOnExisting(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, t.TempDir())
	in := newPod("default", "p1")
	if err := s.Create(ctx, keyFor("default", "p1"), in, &corev1.Pod{}, 0); err != nil {
		t.Fatalf("Create1: %v", err)
	}
	err := s.Create(ctx, keyFor("default", "p1"), in, &corev1.Pod{}, 0)
	if err == nil {
		t.Fatalf("expected error on duplicate Create")
	}
	if se, ok := err.(*storage.StorageError); !ok || se.Code != storage.ErrCodeKeyExists {
		t.Fatalf("expected KeyExists error, got %v (%T)", err, err)
	}
}

func TestDeletePreconditions(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, t.TempDir())
	in := newPod("default", "p1")
	out := &corev1.Pod{}
	if err := s.Create(ctx, keyFor("default", "p1"), in, out, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	wrongUID := "no-such-uid"
	pre := storage.NewUIDPreconditions(wrongUID)
	err := s.Delete(ctx, keyFor("default", "p1"), &corev1.Pod{}, pre, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{})
	if err == nil {
		t.Fatalf("expected UID precondition failure")
	}

	// Happy path with correct UID.
	correctPre := storage.NewUIDPreconditions(string(out.UID))
	deleted := &corev1.Pod{}
	if err := s.Delete(ctx, keyFor("default", "p1"), deleted, correctPre, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{}); err != nil {
		t.Fatalf("Delete happy path: %v", err)
	}
	// Subsequent Get returns NotFound
	got := &corev1.Pod{}
	err = s.Get(ctx, keyFor("default", "p1"), storage.GetOptions{}, got)
	if err == nil {
		t.Fatalf("expected NotFound after delete")
	}
}

func TestGuaranteedUpdate(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, t.TempDir())
	in := newPod("default", "p1")
	created := &corev1.Pod{}
	if err := s.Create(ctx, keyFor("default", "p1"), in, created, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}
	createdRV, _ := strconv.ParseUint(created.ResourceVersion, 10, 64)

	out := &corev1.Pod{}
	err := s.GuaranteedUpdate(ctx, keyFor("default", "p1"), out, false, nil,
		func(input runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
			p := input.(*corev1.Pod).DeepCopy()
			if p.Labels == nil {
				p.Labels = map[string]string{}
			}
			p.Labels["touched"] = "true"
			return p, nil, nil
		}, nil)
	if err != nil {
		t.Fatalf("GuaranteedUpdate: %v", err)
	}
	if out.Labels["touched"] != "true" {
		t.Fatalf("update not applied: %+v", out.Labels)
	}
	updatedRV, _ := strconv.ParseUint(out.ResourceVersion, 10, 64)
	if updatedRV <= createdRV {
		t.Fatalf("update RV %d not greater than create RV %d", updatedRV, createdRV)
	}

	// Verify persisted.
	got := &corev1.Pod{}
	if err := s.Get(ctx, keyFor("default", "p1"), storage.GetOptions{}, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Labels["touched"] != "true" {
		t.Fatalf("persisted state missing label")
	}
}

func TestGetList(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, t.TempDir())
	for i := 0; i < 3; i++ {
		p := newPod("default", fmt.Sprintf("p%d", i), "team", "alpha")
		if err := s.Create(ctx, keyFor("default", p.Name), p, &corev1.Pod{}, 0); err != nil {
			t.Fatalf("Create: %v", err)
		}
	}
	for i := 0; i < 2; i++ {
		p := newPod("kube-system", fmt.Sprintf("k%d", i), "team", "beta")
		if err := s.Create(ctx, keyFor("kube-system", p.Name), p, &corev1.Pod{}, 0); err != nil {
			t.Fatalf("Create: %v", err)
		}
	}

	// List all pods recursively.
	list := &corev1.PodList{}
	err := s.GetList(ctx, "/pods/", storage.ListOptions{Recursive: true, Predicate: everything()}, list)
	if err != nil {
		t.Fatalf("GetList: %v", err)
	}
	if len(list.Items) != 5 {
		t.Fatalf("expected 5 items got %d", len(list.Items))
	}
	if list.ResourceVersion == "" {
		t.Fatalf("list RV not set")
	}

	// Filter by label selector.
	pred := everything()
	pred.Label = labels.SelectorFromSet(labels.Set{"team": "alpha"})
	filtered := &corev1.PodList{}
	if err := s.GetList(ctx, "/pods/", storage.ListOptions{Recursive: true, Predicate: pred}, filtered); err != nil {
		t.Fatalf("GetList filtered: %v", err)
	}
	if len(filtered.Items) != 3 {
		t.Fatalf("expected 3 alpha items got %d", len(filtered.Items))
	}

	// Namespace-scoped list.
	nsList := &corev1.PodList{}
	if err := s.GetList(ctx, "/pods/default", storage.ListOptions{Recursive: true, Predicate: everything()}, nsList); err != nil {
		t.Fatalf("GetList ns: %v", err)
	}
	if len(nsList.Items) != 3 {
		t.Fatalf("expected 3 default items got %d", len(nsList.Items))
	}
}

func TestWatchLive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := newTestStore(t, t.TempDir())

	w, err := s.Watch(ctx, "/pods/default", storage.ListOptions{
		ResourceVersion: "", Recursive: true, Predicate: everything(),
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()

	// Drain initial snapshot (empty).
	if got := drainEvents(w, 50*time.Millisecond); len(got) != 0 {
		t.Fatalf("expected no initial events, got %d", len(got))
	}

	created := &corev1.Pod{}
	if err := s.Create(ctx, keyFor("default", "p1"), newPod("default", "p1"), created, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := s.GuaranteedUpdate(ctx, keyFor("default", "p1"), &corev1.Pod{}, false, nil,
		func(in runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
			p := in.(*corev1.Pod).DeepCopy()
			p.Labels = map[string]string{"x": "y"}
			return p, nil, nil
		}, nil); err != nil {
		t.Fatalf("update: %v", err)
	}

	if err := s.Delete(ctx, keyFor("default", "p1"), &corev1.Pod{}, nil, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	events := drainEvents(w, 500*time.Millisecond)
	if len(events) < 3 {
		t.Fatalf("expected at least 3 events, got %d: %+v", len(events), events)
	}
	wantSeq := []watch.EventType{watch.Added, watch.Modified, watch.Deleted}
	for i, want := range wantSeq {
		if events[i].Type != want {
			t.Fatalf("event[%d]: got %v want %v", i, events[i].Type, want)
		}
	}
	// RVs should be monotonic
	var prev uint64
	for _, ev := range events[:3] {
		acc, _ := getRV(ev.Object)
		if acc <= prev {
			t.Fatalf("RV not monotonic: %d <= %d", acc, prev)
		}
		prev = acc
	}
}

func TestWatchFromRVZero(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := newTestStore(t, t.TempDir())
	for i := 0; i < 3; i++ {
		p := newPod("default", fmt.Sprintf("p%d", i))
		if err := s.Create(ctx, keyFor("default", p.Name), p, &corev1.Pod{}, 0); err != nil {
			t.Fatalf("Create: %v", err)
		}
	}
	w, err := s.Watch(ctx, "/pods/default", storage.ListOptions{
		ResourceVersion: "0", Recursive: true, Predicate: everything(),
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()
	events := drainEvents(w, 200*time.Millisecond)
	if len(events) != 3 {
		t.Fatalf("expected 3 initial events got %d", len(events))
	}
	for _, ev := range events {
		if ev.Type != watch.Added {
			t.Fatalf("expected Added got %v", ev.Type)
		}
	}
	// Live event after.
	if err := s.Create(ctx, keyFor("default", "later"), newPod("default", "later"), &corev1.Pod{}, 0); err != nil {
		t.Fatalf("Create later: %v", err)
	}
	more := drainEvents(w, 200*time.Millisecond)
	if len(more) != 1 || more[0].Type != watch.Added {
		t.Fatalf("expected 1 Added event got %+v", more)
	}
}

func TestWatchFromOldRV(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root := t.TempDir()

	// Build a store with a tiny ring buffer to easily exhaust the window.
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	codec := serializer.NewCodecFactory(scheme).LegacyCodec(corev1.SchemeGroupVersion)
	si, err := New(Config{
		Root:      root,
		Codec:     codec,
		Newer:     func() runtime.Object { return &corev1.Pod{} },
		NewerList: func() runtime.Object { return &corev1.PodList{} },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	st := si.(*store)
	st.broadcast.capacity = 2 // tiny window

	for i := 0; i < 5; i++ {
		p := newPod("default", fmt.Sprintf("p%d", i))
		if err := si.Create(ctx, keyFor("default", p.Name), p, &corev1.Pod{}, 0); err != nil {
			t.Fatalf("Create: %v", err)
		}
	}

	w, err := si.Watch(ctx, "/pods/default", storage.ListOptions{
		ResourceVersion: "1", Recursive: true, Predicate: everything(),
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()
	events := drainEvents(w, 200*time.Millisecond)
	if len(events) == 0 {
		t.Fatalf("expected error event")
	}
	if events[0].Type != watch.Error {
		t.Fatalf("expected Error event, got %v", events[0].Type)
	}
	if !apierrors.IsResourceExpired(toAPIError(events[0].Object)) {
		t.Fatalf("expected ResourceExpired, got %T %v", events[0].Object, events[0].Object)
	}
}

func TestPersistenceAcrossInstances(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s1 := newTestStore(t, root)

	out1 := &corev1.Pod{}
	if err := s1.Create(ctx, keyFor("default", "p1"), newPod("default", "p1"), out1, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}
	rv1, _ := strconv.ParseUint(out1.ResourceVersion, 10, 64)

	// New instance.
	s2 := newTestStore(t, root)
	got := &corev1.Pod{}
	if err := s2.Get(ctx, keyFor("default", "p1"), storage.GetOptions{}, got); err != nil {
		t.Fatalf("Get on reopen: %v", err)
	}
	if got.Name != "p1" {
		t.Fatalf("data not persisted")
	}

	out2 := &corev1.Pod{}
	if err := s2.Create(ctx, keyFor("default", "p2"), newPod("default", "p2"), out2, 0); err != nil {
		t.Fatalf("Create on reopen: %v", err)
	}
	rv2, _ := strconv.ParseUint(out2.ResourceVersion, 10, 64)
	if rv2 <= rv1 {
		t.Fatalf("RV did not continue monotonically across reopen: %d <= %d", rv2, rv1)
	}
}

func TestConcurrentWriters(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t, t.TempDir())

	const n = 50
	var wg sync.WaitGroup
	rvs := make([]uint64, n)
	var fails atomic.Int32
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out := &corev1.Pod{}
			if err := s.Create(ctx, keyFor("default", fmt.Sprintf("p%d", i)), newPod("default", fmt.Sprintf("p%d", i)), out, 0); err != nil {
				fails.Add(1)
				return
			}
			v, _ := strconv.ParseUint(out.ResourceVersion, 10, 64)
			rvs[i] = v
		}(i)
	}
	wg.Wait()
	if fails.Load() != 0 {
		t.Fatalf("concurrent Create failures: %d", fails.Load())
	}
	seen := map[uint64]bool{}
	for _, v := range rvs {
		if v == 0 {
			t.Fatalf("zero RV recorded")
		}
		if seen[v] {
			t.Fatalf("duplicate RV: %d", v)
		}
		seen[v] = true
	}
}

// --- helpers ---

func drainEvents(w watch.Interface, timeout time.Duration) []watch.Event {
	var out []watch.Event
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case ev, ok := <-w.ResultChan():
			if !ok {
				return out
			}
			out = append(out, ev)
		case <-deadline.C:
			return out
		}
	}
}

func getRV(obj runtime.Object) (uint64, error) {
	if obj == nil {
		return 0, fmt.Errorf("nil object")
	}
	type rvHaver interface {
		GetResourceVersion() string
	}
	if p, ok := obj.(*corev1.Pod); ok {
		v, _ := strconv.ParseUint(p.ResourceVersion, 10, 64)
		return v, nil
	}
	if h, ok := obj.(rvHaver); ok {
		v, _ := strconv.ParseUint(h.GetResourceVersion(), 10, 64)
		return v, nil
	}
	return 0, nil
}

func toAPIError(obj runtime.Object) error {
	if s, ok := obj.(*metav1.Status); ok {
		return &apierrors.StatusError{ErrStatus: *s}
	}
	return fmt.Errorf("not a Status: %T", obj)
}

func TestWatchInitialEventsBookmark(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := newTestStore(t, t.TempDir())
	for i := 0; i < 2; i++ {
		p := newPod("default", fmt.Sprintf("b%d", i))
		if err := s.Create(ctx, keyFor("default", p.Name), p, &corev1.Pod{}, 0); err != nil {
			t.Fatalf("Create: %v", err)
		}
	}
	send := true
	w, err := s.Watch(ctx, "/pods/default", storage.ListOptions{
		ResourceVersion:   "0",
		Recursive:         true,
		Predicate:         everything(),
		SendInitialEvents: &send,
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()
	events := drainEvents(w, 200*time.Millisecond)
	if len(events) != 3 {
		t.Fatalf("expected 2 Added + 1 Bookmark, got %d events: %+v", len(events), events)
	}
	if events[0].Type != watch.Added || events[1].Type != watch.Added {
		t.Fatalf("first two events should be Added, got %v %v", events[0].Type, events[1].Type)
	}
	bm := events[2]
	if bm.Type != watch.Bookmark {
		t.Fatalf("third event should be Bookmark, got %v", bm.Type)
	}
	acc, err := meta.Accessor(bm.Object)
	if err != nil {
		t.Fatalf("accessor: %v", err)
	}
	if got := acc.GetAnnotations()["k8s.io/initial-events-end"]; got != "true" {
		t.Errorf("missing initial-events-end annotation; got annotations=%v", acc.GetAnnotations())
	}
	if acc.GetResourceVersion() == "" || acc.GetResourceVersion() == "0" {
		t.Errorf("bookmark RV=%q expected non-zero", acc.GetResourceVersion())
	}
}

func TestWatchNoBookmarkWhenNotRequested(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := newTestStore(t, t.TempDir())
	if err := s.Create(ctx, keyFor("default", "x"), newPod("default", "x"), &corev1.Pod{}, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}
	w, err := s.Watch(ctx, "/pods/default", storage.ListOptions{
		ResourceVersion: "0", Recursive: true, Predicate: everything(),
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()
	events := drainEvents(w, 200*time.Millisecond)
	for _, ev := range events {
		if ev.Type == watch.Bookmark {
			t.Fatalf("unexpected bookmark when SendInitialEvents not set")
		}
	}
}
