// Package kubelet — reconciler.go
//
// Lifecycle note (verified against internal/fsstorage):
//
// The apiserver's fsstorage uses immediate deletion: it has no graceful
// strategy and no finalizer controller, so DELETE removes the object and
// emits a watch DELETE event without ever setting deletionTimestamp.
//
// As a result, kubelet-lite does NOT implement a finalizer / deletionTimestamp
// branch. Cleanup is driven by:
//
//  1. The informer's DeleteFunc, which receives the deleted Pod (or a
//     cache.DeletedFinalStateUnknown tombstone) and lets us reap the
//     container by Pod UID immediately.
//  2. A periodic orphan-reaper that lists all containers labeled
//     io.k8s.pod.uid=<UID> and removes any whose UID is no longer a known
//     Pod, plus the same scan at startup so we recover from a kubelet
//     restart.
package kubelet

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

// Reconciler watches Pods through the apiserver and ensures one Docker
// container per Pod (matched by Pod UID).
type Reconciler struct {
	kube    kubernetes.Interface
	docker  DockerRuntime
	factory informers.SharedInformerFactory
	lister  corelisters.PodLister
	informer cache.SharedIndexInformer

	queue   workqueue.TypedRateLimitingInterface[string]

	// orphanInterval is how often the orphan reaper runs.
	orphanInterval time.Duration

	// failedPods tracks pods we've already marked Failed (so we don't keep
	// re-PATCHing) keyed by UID -> reason.
	mu         sync.Mutex
	failedPods map[types.UID]string
}

// New constructs a Reconciler.
func New(kube kubernetes.Interface, docker DockerRuntime, resync time.Duration) *Reconciler {
	factory := informers.NewSharedInformerFactory(kube, resync)
	podInformer := factory.Core().V1().Pods()

	r := &Reconciler{
		kube:           kube,
		docker:         docker,
		factory:        factory,
		lister:         podInformer.Lister(),
		informer:       podInformer.Informer(),
		queue:          workqueue.NewTypedRateLimitingQueueWithConfig[string](workqueue.DefaultTypedControllerRateLimiter[string](), workqueue.TypedRateLimitingQueueConfig[string]{Name: "kubelet-lite"}),
		orphanInterval: 30 * time.Second,
		failedPods:     map[types.UID]string{},
	}

	_, _ = r.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if pod := asPod(obj); pod != nil {
				r.queue.Add(podKey(pod))
			}
		},
		UpdateFunc: func(_, obj interface{}) {
			if pod := asPod(obj); pod != nil {
				r.queue.Add(podKey(pod))
			}
		},
		DeleteFunc: func(obj interface{}) {
			pod := asPod(obj)
			if pod == nil {
				if t, ok := obj.(cache.DeletedFinalStateUnknown); ok {
					pod = asPod(t.Obj)
				}
			}
			if pod == nil {
				return
			}
			klog.V(2).Infof("watch DELETE pod %s/%s uid=%s — reaping container", pod.Namespace, pod.Name, pod.UID)
			r.mu.Lock()
			delete(r.failedPods, pod.UID)
			r.mu.Unlock()
			if err := r.reapByUID(context.Background(), string(pod.UID)); err != nil {
				klog.Warningf("reap by uid %s: %v", pod.UID, err)
			}
		},
	})

	return r
}

func asPod(obj interface{}) *corev1.Pod {
	p, _ := obj.(*corev1.Pod)
	return p
}

func podKey(p *corev1.Pod) string { return p.Namespace + "/" + p.Name }

// Run starts the informer + a single worker goroutine. Blocks until ctx is done.
func (r *Reconciler) Run(ctx context.Context) error {
	defer runtime.HandleCrash()
	defer r.queue.ShutDown()

	r.factory.Start(ctx.Done())
	klog.Info("kubelet-lite: waiting for informer cache sync")
	if !cache.WaitForCacheSync(ctx.Done(), r.informer.HasSynced) {
		return fmt.Errorf("cache sync failed")
	}
	klog.Info("kubelet-lite: informer synced")

	// Initial orphan reap, then periodic.
	if err := r.reapOrphans(ctx); err != nil {
		klog.Warningf("initial orphan reap: %v", err)
	}
	go wait.UntilWithContext(ctx, func(ctx context.Context) {
		if err := r.reapOrphans(ctx); err != nil {
			klog.Warningf("orphan reap: %v", err)
		}
	}, r.orphanInterval)

	// Single worker — correctness over throughput in v1.
	go wait.UntilWithContext(ctx, r.runWorker, time.Second)

	<-ctx.Done()
	klog.Info("kubelet-lite: shutting down")
	return nil
}

func (r *Reconciler) runWorker(ctx context.Context) {
	for r.processNext(ctx) {
	}
}

func (r *Reconciler) processNext(ctx context.Context) bool {
	key, shutdown := r.queue.Get()
	if shutdown {
		return false
	}
	defer r.queue.Done(key)

	if err := r.reconcileKey(ctx, key); err != nil {
		klog.Warningf("reconcile %s: %v", key, err)
		r.queue.AddRateLimited(key)
		return true
	}
	r.queue.Forget(key)
	return true
}

func (r *Reconciler) reconcileKey(ctx context.Context, key string) error {
	ns, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	pod, err := r.lister.Pods(ns).Get(name)
	if apierrors.IsNotFound(err) {
		// Already handled by DeleteFunc; nothing to do here.
		return nil
	}
	if err != nil {
		return err
	}
	return r.reconcilePod(ctx, pod)
}

func (r *Reconciler) reconcilePod(ctx context.Context, pod *corev1.Pod) error {
	uid := string(pod.UID)
	if uid == "" {
		klog.V(2).Infof("pod %s/%s has no UID yet; skipping", pod.Namespace, pod.Name)
		return nil
	}

	// Graceful termination: apiserver has set deletionTimestamp but the
	// object still exists in storage. Stop the container honoring the
	// grace period, then issue the final force-delete so the apiserver
	// removes the object and a watch DELETE fires.
	if pod.DeletionTimestamp != nil {
		return r.terminatePod(ctx, pod)
	}

	// One-container restriction.
	if len(pod.Spec.Containers) != 1 {
		msg := fmt.Sprintf("kubelet-lite v1 supports exactly one container per Pod (got %d)", len(pod.Spec.Containers))
		return r.markFailedOnce(ctx, pod, msg)
	}

	spec, err := ContainerSpecFromPod(pod)
	if err != nil {
		return r.markFailedOnce(ctx, pod, err.Error())
	}

	name := ContainerNameForUID(uid)
	view, exists, err := r.docker.Inspect(ctx, name)
	if err != nil {
		return fmt.Errorf("inspect: %w", err)
	}

	if !exists {
		// Pull then create.
		if err := r.docker.EnsurePulled(ctx, spec.Image, string(spec.PullPolicy)); err != nil {
			return r.markFailedOnce(ctx, pod, fmt.Sprintf("image pull failed: %v", err))
		}
		id, err := r.docker.CreateAndStart(ctx, name, spec)
		if err != nil {
			return r.markFailedOnce(ctx, pod, fmt.Sprintf("create/start failed: %v", err))
		}
		klog.Infof("started container %s id=%s for pod %s/%s", name, id, pod.Namespace, pod.Name)
		view, _, err = r.docker.Inspect(ctx, name)
		if err != nil {
			return fmt.Errorf("inspect after start: %w", err)
		}
	}

	return r.patchStatus(ctx, pod, PodStatusFromView(pod, view))
}

// terminatePod stops the pod's container honoring deletionGracePeriodSeconds
// and then issues a force-delete via the apiserver to finalize. Idempotent:
// safe to call repeatedly while termination is in flight.
func (r *Reconciler) terminatePod(ctx context.Context, pod *corev1.Pod) error {
	name := ContainerNameForUID(string(pod.UID))
	grace := 30 * time.Second
	if pod.DeletionGracePeriodSeconds != nil {
		grace = time.Duration(*pod.DeletionGracePeriodSeconds) * time.Second
	}
	klog.Infof("pod %s/%s terminating (grace=%s); stopping container %s", pod.Namespace, pod.Name, grace, name)
	if _, exists, err := r.docker.Inspect(ctx, name); err != nil {
		return fmt.Errorf("inspect during terminate: %w", err)
	} else if exists {
		if err := r.docker.Stop(ctx, name, grace); err != nil {
			klog.Warningf("stop %s during terminate: %v", name, err)
		}
	}
	// Final force-delete: GracePeriodSeconds=0 takes the immediate-delete
	// path in the apiserver's BeforeDelete and removes the object.
	zero := int64(0)
	err := r.kube.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: &zero})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (r *Reconciler) markFailedOnce(ctx context.Context, pod *corev1.Pod, msg string) error {
	r.mu.Lock()
	prev, ok := r.failedPods[pod.UID]
	r.mu.Unlock()
	if ok && prev == msg {
		return nil
	}
	klog.Warningf("pod %s/%s failing: %s", pod.Namespace, pod.Name, msg)
	if err := r.patchStatus(ctx, pod, FailedPodStatus(msg)); err != nil {
		return err
	}
	r.mu.Lock()
	r.failedPods[pod.UID] = msg
	r.mu.Unlock()
	return nil
}

func (r *Reconciler) patchStatus(ctx context.Context, pod *corev1.Pod, status corev1.PodStatus) error {
	body, err := json.Marshal(map[string]interface{}{"status": status})
	if err != nil {
		return err
	}
	_, err = r.kube.CoreV1().Pods(pod.Namespace).Patch(ctx, pod.Name, types.MergePatchType, body, metav1.PatchOptions{}, "status")
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// reapByUID stops+removes any container labeled with this UID. Idempotent.
func (r *Reconciler) reapByUID(ctx context.Context, uid string) error {
	name := ContainerNameForUID(uid)
	if _, exists, err := r.docker.Inspect(ctx, name); err != nil {
		return err
	} else if !exists {
		// Fall back to label scan in case the Pod predates the canonical
		// naming (e.g. older container we want to clean up anyway).
		uids, lerr := r.docker.ListManagedUIDs(ctx)
		if lerr != nil {
			return lerr
		}
		if cname, ok := uids[uid]; ok {
			name = cname
		} else {
			return nil
		}
	}
	if err := r.docker.Stop(ctx, name, 10*time.Second); err != nil {
		klog.Warningf("stop %s: %v", name, err)
	}
	if err := r.docker.Remove(ctx, name); err != nil {
		return fmt.Errorf("remove %s: %w", name, err)
	}
	klog.Infof("reaped container %s (uid=%s)", name, uid)
	return nil
}

// reapOrphans removes any container with our io.k8s.pod.uid label whose UID
// no longer corresponds to a known Pod.
func (r *Reconciler) reapOrphans(ctx context.Context) error {
	managed, err := r.docker.ListManagedUIDs(ctx)
	if err != nil {
		return err
	}
	if len(managed) == 0 {
		return nil
	}
	pods, err := r.lister.List(podSelectorAll())
	if err != nil {
		return err
	}
	known := make(map[string]struct{}, len(pods))
	for _, p := range pods {
		known[string(p.UID)] = struct{}{}
	}
	for uid, name := range managed {
		if _, ok := known[uid]; ok {
			continue
		}
		klog.Infof("orphan reaper: removing container %s (uid=%s, no matching pod)", name, uid)
		if err := r.docker.Stop(ctx, name, 10*time.Second); err != nil {
			klog.Warningf("orphan stop %s: %v", name, err)
		}
		if err := r.docker.Remove(ctx, name); err != nil {
			klog.Warningf("orphan remove %s: %v", name, err)
		}
	}
	return nil
}
