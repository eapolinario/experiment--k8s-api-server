package fsstorage

import (
	"context"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
)

// event is an internal record stored in the broadcaster ring buffer.
type event struct {
	rv    uint64
	etype watch.EventType
	obj   runtime.Object
}

// broadcaster fans out events to subscribed watchers and maintains a small
// ring buffer of recent events for replay.
type broadcaster struct {
	mu       sync.Mutex
	capacity int
	ring     []event // ordered by rv ascending
	subs     map[*watcher]struct{}
}

func newBroadcaster(capacity int) *broadcaster {
	return &broadcaster{
		capacity: capacity,
		subs:     map[*watcher]struct{}{},
	}
}

func (b *broadcaster) publish(ev event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ring = append(b.ring, ev)
	if len(b.ring) > b.capacity {
		b.ring = b.ring[len(b.ring)-b.capacity:]
	}
	for w := range b.subs {
		if !w.matchesKey(ev.obj) {
			continue
		}
		ok, err := w.predicate.Matches(ev.obj)
		if err != nil || !ok {
			continue
		}
		w.deliver(watch.Event{Type: ev.etype, Object: ev.obj.DeepCopyObject()})
	}
}

// replayLocked returns events with rv > startRV. Caller must hold b.mu.
// Returns ok=false if startRV precedes the buffer window (i.e. expired).
func (b *broadcaster) replayLocked(startRV uint64) ([]event, bool) {
	if len(b.ring) == 0 {
		return nil, true
	}
	oldest := b.ring[0].rv
	if startRV+1 < oldest {
		return nil, false
	}
	out := make([]event, 0)
	for _, ev := range b.ring {
		if ev.rv > startRV {
			out = append(out, ev)
		}
	}
	return out, true
}

func (b *broadcaster) subscribeLocked(w *watcher) {
	b.subs[w] = struct{}{}
	w.onStop = func() {
		b.mu.Lock()
		delete(b.subs, w)
		b.mu.Unlock()
	}
}

// --- watcher ---

type watcher struct {
	ctx        context.Context
	predicate  storage.SelectionPredicate
	matchesKey func(runtime.Object) bool
	ch         chan watch.Event
	stopCh     chan struct{}
	stopOnce   sync.Once
	onStop     func()
}

func newWatcher(ctx context.Context, p storage.SelectionPredicate, matchesKey func(runtime.Object) bool, buf int) *watcher {
	w := &watcher{
		ctx:        ctx,
		predicate:  p,
		matchesKey: matchesKey,
		ch:         make(chan watch.Event, buf),
		stopCh:     make(chan struct{}),
	}
	go func() {
		select {
		case <-ctx.Done():
			w.Stop()
		case <-w.stopCh:
		}
	}()
	return w
}

func (w *watcher) deliver(ev watch.Event) {
	// best-effort non-blocking; if buffer full, drop watch by stopping with error.
	select {
	case <-w.stopCh:
		return
	case w.ch <- ev:
	default:
		// Buffer full; close the channel to surface a slow watcher.
		w.Stop()
	}
}

func (w *watcher) stopWithError(err error) {
	// Deliver an error event then stop.
	select {
	case w.ch <- watch.Event{Type: watch.Error, Object: errorStatus(err)}:
	default:
	}
	w.Stop()
}

func (w *watcher) Stop() {
	w.stopOnce.Do(func() {
		close(w.stopCh)
		if w.onStop != nil {
			w.onStop()
		}
		close(w.ch)
	})
}

func (w *watcher) ResultChan() <-chan watch.Event { return w.ch }

func errorStatus(err error) runtime.Object {
	if se, ok := err.(*apierrors.StatusError); ok {
		s := se.Status()
		return &s
	}
	return &metav1.Status{
		Status:  metav1.StatusFailure,
		Message: err.Error(),
		Reason:  metav1.StatusReasonInternalError,
	}
}
