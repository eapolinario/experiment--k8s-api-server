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

	// ContainerNamePrefix is prepended to the Pod UID to form the Docker
	// container name (Docker accepts [a-zA-Z0-9_.-]; UIDs use hyphens).
	ContainerNamePrefix = "klite_"

	// PlaceholderPodIP is reported on every Running pod. The Docker bridge
	// address is fine, but for v1 we just need a non-empty value so kubectl
	// renders something.
	PlaceholderPodIP  = "10.0.0.1"
	PlaceholderHostIP = "127.0.0.1"
)

// ContainerNameForUID returns the Docker container name for a given Pod UID.
func ContainerNameForUID(uid string) string {
	return ContainerNamePrefix + uid
}

// PodLabelsFor returns the canonical Docker labels we attach to every
// container managed by kubelet-lite.
func PodLabelsFor(pod *corev1.Pod) map[string]string {
	return map[string]string{
		LabelPodUID:       string(pod.UID),
		LabelPodNamespace: pod.Namespace,
		LabelPodName:      pod.Name,
		LabelManagedBy:    ManagedByValue,
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
	ContainerNm string      // single Pod container name (informational)
	Mounts      []HostMount // bind mounts for projected ConfigMap/Secret volumes
}

// ContainerSpecFromPod translates a Pod into a ContainerSpec. It assumes the
// Pod has exactly one container; callers must check this before invoking.
//
// kube + volumeRoot are used to resolve env / envFrom against ConfigMap /
// Secret objects in the apiserver and to materialise volume projections
// onto the host filesystem. Pass nil kube + empty volumeRoot for legacy
// callers / tests that don't exercise projection (env.valueFrom and
// volumes will be skipped with a warning in that case).
func ContainerSpecFromPod(ctx context.Context, kube kubernetes.Interface, pod *corev1.Pod, volumeRoot string) (ContainerSpec, error) {
	if len(pod.Spec.Containers) != 1 {
		return ContainerSpec{}, fmt.Errorf("kubelet-lite v1 supports exactly one container per Pod (got %d)", len(pod.Spec.Containers))
	}
	c := pod.Spec.Containers[0]

	var env []string
	var mounts []HostMount
	if kube != nil {
		var err error
		if env, err = resolveEnv(ctx, kube, pod.Namespace, &c); err != nil {
			return ContainerSpec{}, err
		}
		if mounts, err = projectVolumes(ctx, kube, pod, &c, volumeRoot); err != nil {
			return ContainerSpec{}, err
		}
	} else {
		// Legacy / test path: literal env values only, no projection.
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

	return ContainerSpec{
		Image:       c.Image,
		Cmd:         cmd,
		Entrypoint:  entrypoint,
		Env:         env,
		WorkingDir:  c.WorkingDir,
		Labels:      PodLabelsFor(pod),
		PullPolicy:  ResolvePullPolicy(c.ImagePullPolicy, c.Image),
		ContainerNm: c.Name,
		Mounts:      mounts,
	}, nil
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

// PodStatusFromView builds a corev1.PodStatus reflecting the observed
// container state. The pod's ObjectMeta is used only for the container name
// echo in containerStatuses.
func PodStatusFromView(pod *corev1.Pod, v ContainerView) corev1.PodStatus {
	now := metav1Now()
	containerName := ""
	if len(pod.Spec.Containers) == 1 {
		containerName = pod.Spec.Containers[0].Name
	}

	cs := corev1.ContainerStatus{
		Name:         containerName,
		Image:        v.Image,
		ImageID:      v.ImageID,
		ContainerID:  "docker://" + v.ID,
		Ready:        v.Running,
		RestartCount: 0,
	}

	status := corev1.PodStatus{
		HostIP: PlaceholderHostIP,
		PodIP:  PlaceholderPodIP,
	}

	if v.Running {
		started := v.StartedAt
		if started.IsZero() {
			started = now.Time
		}
		t := metav1NewTime(started)
		cs.State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: t}}
		status.Phase = corev1.PodRunning
		status.StartTime = &t
	} else {
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
		startedT := metav1NewTime(v.StartedAt)
		cs.State = corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{
				ExitCode:    int32(v.ExitCode),
				Reason:      reason,
				StartedAt:   startedT,
				FinishedAt:  metav1NewTime(fin),
				ContainerID: "docker://" + v.ID,
			},
		}
		cs.Ready = false
		if v.ExitCode == 0 && !v.OOMKilled {
			status.Phase = corev1.PodSucceeded
		} else {
			status.Phase = corev1.PodFailed
			if v.OOMKilled {
				status.Message = fmt.Sprintf("container OOMKilled (exit code %d)", v.ExitCode)
			} else {
				status.Message = fmt.Sprintf("container exited with code %d", v.ExitCode)
			}
		}
		if !v.StartedAt.IsZero() {
			t := metav1NewTime(v.StartedAt)
			status.StartTime = &t
		}
	}

	status.ContainerStatuses = []corev1.ContainerStatus{cs}
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
