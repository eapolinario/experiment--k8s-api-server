package kubelet

import (
	"context"
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

func TestContainerSpecsFromPod_Basic(t *testing.T) {
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

	specs, err := ContainerSpecsFromPod(context.Background(), nil, pod, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(specs) != 1 {
		t.Fatalf("len(specs)=%d want 1", len(specs))
	}
	spec := specs[0]
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
	if spec.Labels[LabelContainerName] != "nginx" {
		t.Errorf("missing container-name label: %v", spec.Labels)
	}
	if spec.Labels[LabelManagedBy] != ManagedByValue {
		t.Errorf("missing managed-by label: %v", spec.Labels)
	}
	if spec.PullPolicy != corev1.PullIfNotPresent {
		t.Errorf("pull policy = %q (want IfNotPresent for tagged image)", spec.PullPolicy)
	}
	if spec.NetworkMode != "" {
		t.Errorf("single-container NetworkMode=%q want empty (default bridge)", spec.NetworkMode)
	}
}

func TestContainerSpecsFromPod_MultiContainerSharesNetns(t *testing.T) {
	pod := mkPod("web", []corev1.Container{
		{Name: "main", Image: "nginx"},
		{Name: "sidecar", Image: "alpine"},
		{Name: "shipper", Image: "busybox"},
	})
	specs, err := ContainerSpecsFromPod(context.Background(), nil, pod, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(specs) != 3 {
		t.Fatalf("len(specs)=%d want 3", len(specs))
	}
	if specs[0].NetworkMode != "" {
		t.Errorf("sandbox NetworkMode=%q want empty", specs[0].NetworkMode)
	}
	wantSandbox := "container:" + ContainerName(string(pod.UID), "main")
	for i := 1; i < len(specs); i++ {
		if specs[i].NetworkMode != wantSandbox {
			t.Errorf("specs[%d].NetworkMode=%q want %q", i, specs[i].NetworkMode, wantSandbox)
		}
	}
	// Each container must carry its own LabelContainerName.
	for i, s := range specs {
		want := pod.Spec.Containers[i].Name
		if s.Labels[LabelContainerName] != want {
			t.Errorf("specs[%d].Labels[container.name]=%q want %q", i, s.Labels[LabelContainerName], want)
		}
	}
}

func TestContainerSpecsFromPod_RejectsZeroContainers(t *testing.T) {
	empty := mkPod("empty", nil)
	if _, err := ContainerSpecsFromPod(context.Background(), nil, empty, ""); err == nil {
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

func TestPodStatusFromViews_AllRunning(t *testing.T) {
	pod := mkPod("web", []corev1.Container{
		{Name: "nginx", Image: "nginx:1.27-alpine"},
		{Name: "sidecar", Image: "alpine"},
	})
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	views := map[string]ContainerView{
		"nginx":   {ID: "a", Image: "nginx:1.27-alpine", ImageID: "sha256:1", Running: true, StartedAt: start},
		"sidecar": {ID: "b", Image: "alpine", ImageID: "sha256:2", Running: true, StartedAt: start.Add(time.Second)},
	}
	st := PodStatusFromViews(pod, views)
	if st.Phase != corev1.PodRunning {
		t.Errorf("phase = %q", st.Phase)
	}
	if len(st.ContainerStatuses) != 2 {
		t.Fatalf("container statuses: %d want 2", len(st.ContainerStatuses))
	}
	if st.ContainerStatuses[0].Name != "nginx" || st.ContainerStatuses[1].Name != "sidecar" {
		t.Errorf("container order: %+v", st.ContainerStatuses)
	}
	for _, cs := range st.ContainerStatuses {
		if !cs.Ready || cs.State.Running == nil {
			t.Errorf("cs not running ready: %+v", cs)
		}
	}
	if st.StartTime == nil || !st.StartTime.Time.Equal(start) {
		t.Errorf("startTime=%v want earliest=%v", st.StartTime, start)
	}
}

func TestPodStatusFromViews_AnyMissingPending(t *testing.T) {
	pod := mkPod("web", []corev1.Container{
		{Name: "a", Image: "alpine"},
		{Name: "b", Image: "alpine"},
	})
	views := map[string]ContainerView{
		"a": {ID: "x", Running: true, StartedAt: time.Now()},
	}
	st := PodStatusFromViews(pod, views)
	if st.Phase != corev1.PodPending {
		t.Errorf("phase = %q want Pending", st.Phase)
	}
	if st.ContainerStatuses[1].State.Waiting == nil || st.ContainerStatuses[1].State.Waiting.Reason != "ContainerCreating" {
		t.Errorf("missing container should be Waiting/ContainerCreating: %+v", st.ContainerStatuses[1])
	}
}

func TestPodStatusFromViews_AnyFailedFails(t *testing.T) {
	pod := mkPod("web", []corev1.Container{
		{Name: "main", Image: "alpine"},
		{Name: "side", Image: "alpine"},
	})
	views := map[string]ContainerView{
		"main": {ID: "x", Running: true, StartedAt: time.Now()},
		"side": {ID: "y", Running: false, ExitCode: 137, OOMKilled: true},
	}
	st := PodStatusFromViews(pod, views)
	if st.Phase != corev1.PodFailed {
		t.Errorf("phase = %q want Failed", st.Phase)
	}
	if st.Message == "" {
		t.Errorf("expected non-empty failure message")
	}
}

func TestPodStatusFromViews_AllSucceeded(t *testing.T) {
	pod := mkPod("job", []corev1.Container{{Name: "a", Image: "alpine"}, {Name: "b", Image: "alpine"}})
	views := map[string]ContainerView{
		"a": {ID: "x", Running: false, ExitCode: 0},
		"b": {ID: "y", Running: false, ExitCode: 0},
	}
	st := PodStatusFromViews(pod, views)
	if st.Phase != corev1.PodSucceeded {
		t.Errorf("phase = %q want Succeeded", st.Phase)
	}
	for _, cs := range st.ContainerStatuses {
		if cs.State.Terminated == nil || cs.State.Terminated.Reason != "Completed" {
			t.Errorf("reason = %+v", cs.State.Terminated)
		}
	}
}

func TestPodStatusFromViews_RunningPlusCompletedSidecarStaysRunning(t *testing.T) {
	// Mirror real k8s: a sidecar that exits 0 while the main is still
	// Running keeps the pod in Running phase.
	pod := mkPod("web", []corev1.Container{
		{Name: "main", Image: "alpine"},
		{Name: "side", Image: "alpine"},
	})
	views := map[string]ContainerView{
		"main": {ID: "x", Running: true, StartedAt: time.Now()},
		"side": {ID: "y", Running: false, ExitCode: 0},
	}
	st := PodStatusFromViews(pod, views)
	if st.Phase != corev1.PodRunning {
		t.Errorf("phase = %q want Running (sidecar exit 0 while main still up)", st.Phase)
	}
}
