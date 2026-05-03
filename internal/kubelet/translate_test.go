package kubelet

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func mkPod(name string, containers []corev1.Container) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			UID:       types.UID("uid-" + name),
		},
		Spec: corev1.PodSpec{Containers: containers},
	}
}

func TestContainerSpecFromPod_Basic(t *testing.T) {
	pod := mkPod("nginx", []corev1.Container{{
		Name:       "nginx",
		Image:      "nginx:1.27-alpine",
		Command:    []string{"/bin/sh", "-c"},
		Args:       []string{"nginx -g 'daemon off;'"},
		WorkingDir: "/srv",
		Env: []corev1.EnvVar{
			{Name: "FOO", Value: "bar"},
			{Name: "FROM_FIELD", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		},
	}})

	spec, err := ContainerSpecFromPod(pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if spec.Image != "nginx:1.27-alpine" {
		t.Errorf("image = %q", spec.Image)
	}
	if len(spec.Entrypoint) != 2 || spec.Entrypoint[0] != "/bin/sh" {
		t.Errorf("entrypoint = %v", spec.Entrypoint)
	}
	if len(spec.Cmd) != 1 || spec.Cmd[0] != "nginx -g 'daemon off;'" {
		t.Errorf("cmd = %v", spec.Cmd)
	}
	if spec.WorkingDir != "/srv" {
		t.Errorf("workingdir = %q", spec.WorkingDir)
	}
	if len(spec.Env) != 1 || spec.Env[0] != "FOO=bar" {
		t.Errorf("env = %v (valueFrom should be skipped)", spec.Env)
	}
	if spec.Labels[LabelPodUID] != "uid-nginx" {
		t.Errorf("missing uid label: %v", spec.Labels)
	}
	if spec.Labels[LabelManagedBy] != ManagedByValue {
		t.Errorf("missing managed-by label: %v", spec.Labels)
	}
	if spec.PullPolicy != corev1.PullIfNotPresent {
		t.Errorf("pull policy = %q (want IfNotPresent for tagged image)", spec.PullPolicy)
	}
}

func TestContainerSpecFromPod_RejectsMultiContainer(t *testing.T) {
	pod := mkPod("multi", []corev1.Container{
		{Name: "a", Image: "alpine"},
		{Name: "b", Image: "alpine"},
	})
	if _, err := ContainerSpecFromPod(pod); err == nil {
		t.Fatal("expected error for multi-container pod")
	}

	empty := mkPod("empty", nil)
	if _, err := ContainerSpecFromPod(empty); err == nil {
		t.Fatal("expected error for zero-container pod")
	}
}

func TestResolvePullPolicy(t *testing.T) {
	cases := []struct {
		name   string
		policy corev1.PullPolicy
		image  string
		want   corev1.PullPolicy
	}{
		{"explicit Always", corev1.PullAlways, "nginx:1.27", corev1.PullAlways},
		{"explicit Never", corev1.PullNever, "nginx", corev1.PullNever},
		{"unset tagged", "", "nginx:1.27-alpine", corev1.PullIfNotPresent},
		{"unset latest", "", "nginx:latest", corev1.PullAlways},
		{"unset untagged", "", "nginx", corev1.PullAlways},
		{"unset with digest only", "", "nginx@sha256:abc", corev1.PullAlways},
		{"registry path latest", "", "registry.example.com/x:latest", corev1.PullAlways},
		{"registry path tagged", "", "registry.example.com:5000/x:1.0", corev1.PullIfNotPresent},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ResolvePullPolicy(c.policy, c.image); got != c.want {
				t.Errorf("ResolvePullPolicy(%q, %q) = %q, want %q", c.policy, c.image, got, c.want)
			}
		})
	}
}

func TestPodStatusFromView_Running(t *testing.T) {
	pod := mkPod("nginx", []corev1.Container{{Name: "nginx", Image: "nginx:1.27-alpine"}})
	v := ContainerView{
		ID:        "abc123",
		Image:     "nginx:1.27-alpine",
		ImageID:   "sha256:deadbeef",
		Running:   true,
		StartedAt: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	st := PodStatusFromView(pod, v)
	if st.Phase != corev1.PodRunning {
		t.Errorf("phase = %q", st.Phase)
	}
	if st.PodIP != PlaceholderPodIP || st.HostIP != PlaceholderHostIP {
		t.Errorf("ips = %q / %q", st.PodIP, st.HostIP)
	}
	if st.StartTime == nil {
		t.Fatalf("missing startTime")
	}
	if len(st.ContainerStatuses) != 1 {
		t.Fatalf("container statuses: %v", st.ContainerStatuses)
	}
	cs := st.ContainerStatuses[0]
	if !cs.Ready || cs.State.Running == nil || cs.ContainerID != "docker://abc123" {
		t.Errorf("bad running cs: %+v", cs)
	}
}

func TestPodStatusFromView_ExitedNonZero(t *testing.T) {
	pod := mkPod("sh", []corev1.Container{{Name: "sh", Image: "alpine"}})
	v := ContainerView{
		ID:         "x",
		Running:    false,
		ExitCode:   137,
		OOMKilled:  true,
		StartedAt:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		FinishedAt: time.Date(2025, 1, 1, 0, 1, 0, 0, time.UTC),
	}
	st := PodStatusFromView(pod, v)
	if st.Phase != corev1.PodFailed {
		t.Errorf("phase = %q", st.Phase)
	}
	cs := st.ContainerStatuses[0]
	if cs.State.Terminated == nil || cs.State.Terminated.Reason != "OOMKilled" {
		t.Errorf("bad terminated state: %+v", cs)
	}
	if cs.Ready {
		t.Errorf("ready should be false on exit")
	}
}

func TestPodStatusFromView_ExitedZero(t *testing.T) {
	pod := mkPod("sh", []corev1.Container{{Name: "sh", Image: "alpine"}})
	v := ContainerView{ID: "x", Running: false, ExitCode: 0}
	st := PodStatusFromView(pod, v)
	if st.Phase != corev1.PodSucceeded {
		t.Errorf("phase = %q", st.Phase)
	}
	if st.ContainerStatuses[0].State.Terminated.Reason != "Completed" {
		t.Errorf("reason = %q", st.ContainerStatuses[0].State.Terminated.Reason)
	}
}
