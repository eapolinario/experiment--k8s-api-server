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
	"os"
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
	kube       kubernetes.Interface
	docker     DockerRuntime
	recorder   EventRecorder
	factory    informers.SharedInformerFactory
	lister     corelisters.PodLister
	informer   cache.SharedIndexInformer
	volumeRoot string

	queue   workqueue.TypedRateLimitingInterface[string]

	// orphanInterval is how often the orphan reaper runs.
	orphanInterval time.Duration

	// failedPods tracks pods we've already marked Failed (so we don't keep
	// re-PATCHing) keyed by UID -> reason.
	mu         sync.Mutex
	failedPods map[types.UID]string
}

// New constructs a Reconciler. volumeRoot is the host directory under which
// per-pod ConfigMap/Secret projection dirs are created and bind-mounted
// into containers; pass "" to disable projection (env.valueFrom +
// configmap/secret volumes will be skipped with warnings).
func New(kube kubernetes.Interface, docker DockerRuntime, resync time.Duration, volumeRoot string) *Reconciler {
	factory := informers.NewSharedInformerFactory(kube, resync)
	podInformer := factory.Core().V1().Pods()

	r := &Reconciler{
		kube:           kube,
		docker:         docker,
		recorder:       NewEventRecorder(kube, "kubelet-lite"),
		factory:        factory,
		lister:         podInformer.Lister(),
		informer:       podInformer.Informer(),
		volumeRoot:     volumeRoot,
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
	// object still exists in storage. Stop every container honoring the
	// grace period, then issue the final force-delete so the apiserver
	// removes the object and a watch DELETE fires.
	if pod.DeletionTimestamp != nil {
		return r.terminatePod(ctx, pod)
	}

	specs, err := ContainerSpecsFromPod(ctx, r.kube, pod, r.volumeRoot)
	if err != nil {
		return r.markFailedOnce(ctx, pod, err.Error())
	}

	// Iterate in spec order so the first container (the netns sandbox)
	// is created before any sibling tries to attach to its netns.
	views := make(map[string]ContainerView, len(specs))
	for _, spec := range specs {
		containerDockerName := ContainerName(uid, spec.ContainerNm)
		view, exists, err := r.docker.Inspect(ctx, containerDockerName)
		if err != nil {
			return fmt.Errorf("inspect %s: %w", containerDockerName, err)
		}
		if !exists {
			r.recorder.Eventf(ctx, pod, corev1.EventTypeNormal, EventReasonPulling, "Pulling image %q", spec.Image)
			if err := r.docker.EnsurePulled(ctx, spec.Image, string(spec.PullPolicy)); err != nil {
				r.recorder.Eventf(ctx, pod, corev1.EventTypeWarning, EventReasonFailed, "Failed to pull image %q for container %q: %v", spec.Image, spec.ContainerNm, err)
				return r.markFailedOnce(ctx, pod, fmt.Sprintf("image pull failed for %q: %v", spec.ContainerNm, err))
			}
			r.recorder.Eventf(ctx, pod, corev1.EventTypeNormal, EventReasonPulled, "Successfully pulled image %q", spec.Image)
			id, err := r.docker.CreateAndStart(ctx, containerDockerName, spec)
			if err != nil {
				r.recorder.Eventf(ctx, pod, corev1.EventTypeWarning, EventReasonFailed, "Failed to create container %q: %v", spec.ContainerNm, err)
				return r.markFailedOnce(ctx, pod, fmt.Sprintf("create/start %q failed: %v", spec.ContainerNm, err))
			}
			klog.Infof("started container %s id=%s for pod %s/%s", containerDockerName, id, pod.Namespace, pod.Name)
			r.recorder.Eventf(ctx, pod, corev1.EventTypeNormal, EventReasonCreated, "Created container: %s", spec.ContainerNm)
			r.recorder.Eventf(ctx, pod, corev1.EventTypeNormal, EventReasonStarted, "Started container %s", spec.ContainerNm)
			view, _, err = r.docker.Inspect(ctx, containerDockerName)
			if err != nil {
				return fmt.Errorf("inspect %s after start: %w", containerDockerName, err)
			}
		}
		views[spec.ContainerNm] = view
	}

	return r.patchStatus(ctx, pod, PodStatusFromViews(pod, views))
}

// terminatePod stops every container for the pod (in reverse spec order so
// siblings stop before the netns sandbox) honoring deletionGracePeriodSeconds,
// then issues a force-delete via the apiserver to finalize. Idempotent.
func (r *Reconciler) terminatePod(ctx context.Context, pod *corev1.Pod) error {
	uid := string(pod.UID)
	grace := 30 * time.Second
	if pod.DeletionGracePeriodSeconds != nil {
		grace = time.Duration(*pod.DeletionGracePeriodSeconds) * time.Second
	}

	names := r.containerNamesForPod(ctx, pod)
	klog.Infof("pod %s/%s terminating (grace=%s); stopping %d container(s)", pod.Namespace, pod.Name, grace, len(names))
	// Reverse order: sandbox last so siblings can still see its netns
	// while they shut down.
	for i := len(names) - 1; i >= 0; i-- {
		name := names[i]
		if _, exists, err := r.docker.Inspect(ctx, name); err != nil {
			return fmt.Errorf("inspect %s during terminate: %w", name, err)
		} else if !exists {
			continue
		}
		r.recorder.Eventf(ctx, pod, corev1.EventTypeNormal, EventReasonKilling, "Stopping container %s (grace=%s)", name, grace)
		if err := r.docker.Stop(ctx, name, grace); err != nil {
			klog.Warningf("stop %s during terminate: %v", name, err)
		}
	}
	_ = uid // referenced via names; keep var for future
	zero := int64(0)
	err := r.kube.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: &zero})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// containerNamesForPod returns the docker container names belonging to a
// Pod, in pod.Spec.Containers order. If the spec is unavailable (e.g. the
// pod was already deleted from the lister), falls back to a label scan.
func (r *Reconciler) containerNamesForPod(ctx context.Context, pod *corev1.Pod) []string {
	if len(pod.Spec.Containers) > 0 {
		names := make([]string, 0, len(pod.Spec.Containers))
		for _, c := range pod.Spec.Containers {
			names = append(names, ContainerName(string(pod.UID), c.Name))
		}
		return names
	}
	return r.containerNamesByLabel(ctx, string(pod.UID))
}

func (r *Reconciler) containerNamesByLabel(ctx context.Context, uid string) []string {
	mgr, err := r.docker.ListManagedContainers(ctx)
	if err != nil {
		klog.Warningf("list managed containers: %v", err)
		return nil
	}
	var names []string
	for _, m := range mgr {
		if m.UID == uid {
			names = append(names, m.DockerName)
		}
	}
	return names
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

// reapByUID stops+removes every container labeled with this UID. Idempotent.
func (r *Reconciler) reapByUID(ctx context.Context, uid string) error {
	names := r.containerNamesByLabel(ctx, uid)
	if len(names) == 0 {
		return nil
	}
	// Reverse order: stop siblings first, sandbox last.
	for i := len(names) - 1; i >= 0; i-- {
		name := names[i]
		if err := r.docker.Stop(ctx, name, 10*time.Second); err != nil {
			klog.Warningf("stop %s: %v", name, err)
		}
		if err := r.docker.Remove(ctx, name); err != nil {
			return fmt.Errorf("remove %s: %w", name, err)
		}
		klog.Infof("reaped container %s (uid=%s)", name, uid)
	}
	if r.volumeRoot != "" {
		if err := os.RemoveAll(PodVolumeRoot(r.volumeRoot, uid)); err != nil {
			klog.Warningf("remove volume dir for uid %s: %v", uid, err)
		}
	}
	return nil
}

// reapOrphans removes any container with our io.k8s.pod.uid label whose UID
// no longer corresponds to a known Pod.
func (r *Reconciler) reapOrphans(ctx context.Context) error {
	managed, err := r.docker.ListManagedContainers(ctx)
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
	// Group orphan containers by UID so we reap a pod's siblings together.
	orphansByUID := map[string][]string{}
	for _, m := range managed {
		if _, ok := known[m.UID]; ok {
			continue
		}
		orphansByUID[m.UID] = append(orphansByUID[m.UID], m.DockerName)
	}
	for uid, names := range orphansByUID {
		klog.Infof("orphan reaper: removing %d container(s) for uid=%s", len(names), uid)
		for _, name := range names {
			if err := r.docker.Stop(ctx, name, 10*time.Second); err != nil {
				klog.Warningf("orphan stop %s: %v", name, err)
			}
			if err := r.docker.Remove(ctx, name); err != nil {
				klog.Warningf("orphan remove %s: %v", name, err)
			}
		}
		if r.volumeRoot != "" {
			if err := os.RemoveAll(PodVolumeRoot(r.volumeRoot, uid)); err != nil {
				klog.Warningf("orphan remove volume dir uid=%s: %v", uid, err)
			}
		}
	}
	return nil
}
