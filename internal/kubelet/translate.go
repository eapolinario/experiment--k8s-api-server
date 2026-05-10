package kubelet

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

// LabelPodUID is the canonical label that identifies a kubelet-lite-managed
// container by Pod UID.
const (
	LabelPodUID       = "io.k8s.pod.uid"
	LabelPodNamespace = "io.k8s.pod.namespace"
	LabelPodName      = "io.k8s.pod.name"
	LabelManagedBy    = "io.k8s.managed-by"
	ManagedByValue    = "kubelet-lite"

	// ContainerNamePrefix is prepended to <PodUID>_<ContainerName> to form
	// the Docker container name (Docker accepts [a-zA-Z0-9_.-]; UIDs use
	// hyphens, container names are DNS-label-safe).
	ContainerNamePrefix = "klite_"

	// PlaceholderPodIP is reported on every Running pod. The Docker bridge
	// address is fine, but for v1 we just need a non-empty value so kubectl
	// renders something.
	PlaceholderPodIP  = "10.0.0.1"
	PlaceholderHostIP = "127.0.0.1"
)

// ContainerName returns the Docker container name for a given Pod UID +
// pod-container name. The two-part scheme lets us host multiple containers
// per Pod and still recover the (uid, container) tuple from a label scan.
func ContainerName(uid, containerName string) string {
	return ContainerNamePrefix + uid + "_" + containerName
}

// LabelContainerName is the docker label carrying the pod-container name
// (the value of pod.spec.containers[i].name). Combined with LabelPodUID
// it uniquely identifies a managed container.
const LabelContainerName = "io.k8s.container.name"

// PodLabelsFor returns the canonical Docker labels we attach to a
// container managed by kubelet-lite. containerName is the value of
// pod.spec.containers[i].name and is required so the orphan reaper +
// log server can pick a specific container out of a multi-container Pod.
func PodLabelsFor(pod *corev1.Pod, containerName string) map[string]string {
	return map[string]string{
		LabelPodUID:        string(pod.UID),
		LabelPodNamespace:  pod.Namespace,
		LabelPodName:       pod.Name,
		LabelContainerName: containerName,
		LabelManagedBy:     ManagedByValue,
	}
}

// ContainerSpec is the pure-data result of translating a Pod spec into
// "what we'd ask Docker to create". It is decoupled from the docker SDK
// types so this package can be unit tested without a daemon.
type ContainerSpec struct {
	Image       string
	Cmd         []string
	Entrypoint  []string
	Env         []string // KEY=VAL form
	WorkingDir  string
	Labels      map[string]string
	PullPolicy  corev1.PullPolicy
	ContainerNm string      // pod-container name (pod.spec.containers[i].name)
	Mounts      []HostMount // bind mounts for projected ConfigMap/Secret volumes
	// NetworkMode controls Docker HostConfig.NetworkMode. Empty string
	// uses Docker's default (bridge). For sibling containers in a
	// multi-container Pod we set this to "container:<sandboxName>" so
	// every container shares the first container's netns — the same
	// trick real kubelet uses with its pause sandbox, minus the pause.
	NetworkMode string
}

// ContainerSpecsFromPod translates every container in pod.Spec.Containers
// into a ContainerSpec, in spec order. The first container is the netns
// "sandbox": its NetworkMode is left blank (default bridge). Every
// subsequent container's NetworkMode is set to "container:<sandboxName>"
// so siblings join the sandbox's netns (same loopback, same published
// ports). Pods with zero containers are rejected.
//
// kube + volumeRoot work as before — see resolveEnv / projectVolumes.
func ContainerSpecsFromPod(ctx context.Context, kube kubernetes.Interface, pod *corev1.Pod, volumeRoot string) ([]ContainerSpec, error) {
	if len(pod.Spec.Containers) == 0 {
		return nil, fmt.Errorf("pod %s/%s has zero containers", pod.Namespace, pod.Name)
	}
	uid := string(pod.UID)
	sandboxName := ContainerName(uid, pod.Spec.Containers[0].Name)

	specs := make([]ContainerSpec, 0, len(pod.Spec.Containers))
	for i := range pod.Spec.Containers {
		c := pod.Spec.Containers[i]

		var env []string
		var mounts []HostMount
		if kube != nil {
			var err error
			if env, err = resolveEnv(ctx, kube, pod.Namespace, &c); err != nil {
				return nil, fmt.Errorf("container %q: %w", c.Name, err)
			}
			if mounts, err = projectVolumes(ctx, kube, pod, &c, volumeRoot); err != nil {
				return nil, fmt.Errorf("container %q: %w", c.Name, err)
			}
		} else {
			env = make([]string, 0, len(c.Env))
			for _, e := range c.Env {
				if e.ValueFrom != nil {
					klog.Warningf("pod %s/%s container %q: env %q uses valueFrom but no kube client provided; skipping", pod.Namespace, pod.Name, c.Name, e.Name)
					continue
				}
				env = append(env, fmt.Sprintf("%s=%s", e.Name, e.Value))
			}
		}

		var cmd, entrypoint []string
		if len(c.Command) > 0 {
			entrypoint = append(entrypoint, c.Command...)
		}
		if len(c.Args) > 0 {
			cmd = append(cmd, c.Args...)
		}

		netMode := ""
		if i > 0 {
			netMode = "container:" + sandboxName
		}

		specs = append(specs, ContainerSpec{
			Image:       c.Image,
			Cmd:         cmd,
			Entrypoint:  entrypoint,
			Env:         env,
			WorkingDir:  c.WorkingDir,
			Labels:      PodLabelsFor(pod, c.Name),
			PullPolicy:  ResolvePullPolicy(c.ImagePullPolicy, c.Image),
			ContainerNm: c.Name,
			Mounts:      mounts,
			NetworkMode: netMode,
		})
	}
	return specs, nil
}

// ResolvePullPolicy returns the effective pull policy. The Kubernetes default
// is `Always` for `:latest` (or unset tag) and `IfNotPresent` otherwise.
func ResolvePullPolicy(p corev1.PullPolicy, image string) corev1.PullPolicy {
	if p != "" {
		return p
	}
	if isLatestOrUntagged(image) {
		return corev1.PullAlways
	}
	return corev1.PullIfNotPresent
}

func isLatestOrUntagged(image string) bool {
	// strip any digest
	if i := indexOf(image, '@'); i >= 0 {
		image = image[:i]
	}
	// find last ':' after last '/'
	slash := lastIndexOf(image, '/')
	colon := lastIndexOf(image, ':')
	if colon <= slash {
		return true // no tag
	}
	return image[colon+1:] == "latest"
}

func indexOf(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

func lastIndexOf(s string, b byte) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// ContainerView is a minimal projection of a Docker container inspect we use
// to build pod status. Decoupling this from the docker SDK keeps
// translate_test.go pure.
type ContainerView struct {
	ID         string
	Image      string
	ImageID    string
	Running    bool
	StartedAt  time.Time
	FinishedAt time.Time
	ExitCode   int
	OOMKilled  bool
}

// PodStatusFromViews builds a corev1.PodStatus reflecting per-container
// observed state. views maps pod-container name -> ContainerView; missing
// entries (container not yet created) become a Waiting ContainerStatus.
//
// Phase aggregation:
//   * any container missing or waiting   -> Pending
//   * all containers Running             -> Running
//   * all terminated, all exit 0         -> Succeeded
//   * any terminated with exit != 0      -> Failed
//   * mixed running + terminated-zero    -> Running (the terminated one
//     is just "done early", same as a sidecar that completed)
func PodStatusFromViews(pod *corev1.Pod, views map[string]ContainerView) corev1.PodStatus {
	now := metav1Now()

	status := corev1.PodStatus{
		HostIP: PlaceholderHostIP,
		PodIP:  PlaceholderPodIP,
	}

	containerStatuses := make([]corev1.ContainerStatus, 0, len(pod.Spec.Containers))
	var (
		anyMissing       bool
		anyRunning       bool
		anyFailed        bool
		allTerminated   = true
		earliestStart   time.Time
		failMessage     string
	)

	for _, c := range pod.Spec.Containers {
		v, ok := views[c.Name]
		cs := corev1.ContainerStatus{Name: c.Name, Image: c.Image, RestartCount: 0}
		switch {
		case !ok:
			// Not yet created.
			cs.State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}
			anyMissing = true
			allTerminated = false
		case v.Running:
			started := v.StartedAt
			if started.IsZero() {
				started = now.Time
			}
			t := metav1NewTime(started)
			cs.State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: t}}
			cs.Ready = true
			cs.Image = v.Image
			cs.ImageID = v.ImageID
			cs.ContainerID = "docker://" + v.ID
			anyRunning = true
			allTerminated = false
			if earliestStart.IsZero() || started.Before(earliestStart) {
				earliestStart = started
			}
		default:
			reason := "Completed"
			if v.ExitCode != 0 {
				reason = "Error"
			}
			if v.OOMKilled {
				reason = "OOMKilled"
			}
			fin := v.FinishedAt
			if fin.IsZero() {
				fin = now.Time
			}
			cs.State = corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					ExitCode:    int32(v.ExitCode),
					Reason:      reason,
					StartedAt:   metav1NewTime(v.StartedAt),
					FinishedAt:  metav1NewTime(fin),
					ContainerID: "docker://" + v.ID,
				},
			}
			cs.Ready = false
			cs.Image = v.Image
			cs.ImageID = v.ImageID
			cs.ContainerID = "docker://" + v.ID
			if v.ExitCode != 0 || v.OOMKilled {
				anyFailed = true
				if v.OOMKilled {
					failMessage = fmt.Sprintf("container %q OOMKilled (exit code %d)", c.Name, v.ExitCode)
				} else {
					failMessage = fmt.Sprintf("container %q exited with code %d", c.Name, v.ExitCode)
				}
			}
			if !v.StartedAt.IsZero() && (earliestStart.IsZero() || v.StartedAt.Before(earliestStart)) {
				earliestStart = v.StartedAt
			}
		}
		containerStatuses = append(containerStatuses, cs)
	}

	switch {
	case anyFailed:
		status.Phase = corev1.PodFailed
		status.Message = failMessage
	case anyMissing:
		status.Phase = corev1.PodPending
	case anyRunning:
		status.Phase = corev1.PodRunning
	case allTerminated:
		status.Phase = corev1.PodSucceeded
	default:
		status.Phase = corev1.PodPending
	}

	if !earliestStart.IsZero() {
		t := metav1NewTime(earliestStart)
		status.StartTime = &t
	}
	status.ContainerStatuses = containerStatuses
	return status
}

// FailedPodStatus returns a PodStatus with phase=Failed and the given message,
// used when the Pod cannot be admitted (e.g. multi-container, image pull
// failure) and we have no Docker container yet.
func FailedPodStatus(message string) corev1.PodStatus {
	return corev1.PodStatus{
		Phase:   corev1.PodFailed,
		Reason:  "KubeletLite",
		Message: message,
		HostIP:  PlaceholderHostIP,
	}
}
