package apiserver

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func mustHeader(t *testing.T, names []metav1.TableColumnDefinition, want []string) {
	t.Helper()
	if len(names) != len(want) {
		t.Fatalf("columns=%d want %d (%v)", len(names), len(want), names)
	}
	for i, w := range want {
		if names[i].Name != w {
			t.Errorf("column[%d]=%q want %q", i, names[i].Name, w)
		}
	}
}

func TestPodTableConvertor_RunningPod(t *testing.T) {
	created := metav1.NewTime(time.Now().Add(-3 * time.Minute))
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "nginx", CreationTimestamp: created},
		Spec:       corev1.PodSpec{NodeName: "kubelet-lite", Containers: []corev1.Container{{Name: "nginx", Image: "nginx:1.27-alpine"}}},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.0.0.1",
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "nginx", Ready: true, RestartCount: 0,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
	tbl, err := podTableConvertor{}.ConvertToTable(context.Background(), pod, nil)
	if err != nil {
		t.Fatalf("ConvertToTable: %v", err)
	}
	mustHeader(t, tbl.ColumnDefinitions, []string{"Name", "Ready", "Status", "Restarts", "Age", "IP", "Node"})
	if len(tbl.Rows) != 1 {
		t.Fatalf("rows=%d", len(tbl.Rows))
	}
	cells := tbl.Rows[0].Cells
	if cells[0] != "nginx" || cells[1] != "1/1" || cells[2] != "Running" || cells[3] != int32(0) {
		t.Errorf("cells=%v", cells)
	}
	if cells[5] != "10.0.0.1" || cells[6] != "kubelet-lite" {
		t.Errorf("ip/node cells=%v", cells)
	}
	age := cells[4].(string)
	if age == "<unknown>" || age == "0s" {
		t.Errorf("age=%q expected ~3m", age)
	}
}

func TestPodTableConvertor_TerminatedPod(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "sleep"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "sleep"}}},
		Status: corev1.PodStatus{
			Phase: corev1.PodSucceeded,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "sleep",
				Ready: false,
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Completed"}},
			}},
		},
	}
	tbl, err := podTableConvertor{}.ConvertToTable(context.Background(), pod, nil)
	if err != nil {
		t.Fatalf("ConvertToTable: %v", err)
	}
	if tbl.Rows[0].Cells[2] != "Completed" {
		t.Errorf("status cell=%v want Completed", tbl.Rows[0].Cells[2])
	}
	if tbl.Rows[0].Cells[5] != "<none>" {
		t.Errorf("ip cell=%v want <none>", tbl.Rows[0].Cells[5])
	}
}

func TestPodTableConvertor_List(t *testing.T) {
	list := &corev1.PodList{
		ListMeta: metav1.ListMeta{ResourceVersion: "42"},
		Items: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Name: "a"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{}}}},
			{ObjectMeta: metav1.ObjectMeta{Name: "b"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{}}}},
		},
	}
	tbl, err := podTableConvertor{}.ConvertToTable(context.Background(), list, nil)
	if err != nil {
		t.Fatalf("ConvertToTable: %v", err)
	}
	if len(tbl.Rows) != 2 || tbl.ResourceVersion != "42" {
		t.Errorf("rows=%d rv=%q", len(tbl.Rows), tbl.ResourceVersion)
	}
}

func TestPodTableConvertor_NoHeaders(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "x"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{}}}}
	tbl, err := podTableConvertor{}.ConvertToTable(context.Background(), pod, &metav1.TableOptions{NoHeaders: true})
	if err != nil {
		t.Fatalf("ConvertToTable: %v", err)
	}
	if len(tbl.ColumnDefinitions) != 0 {
		t.Errorf("expected no headers, got %d", len(tbl.ColumnDefinitions))
	}
}

func TestNamespaceTableConvertor(t *testing.T) {
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", CreationTimestamp: metav1.NewTime(time.Now().Add(-2 * time.Hour))},
		Status:     corev1.NamespaceStatus{Phase: corev1.NamespaceActive},
	}
	tbl, err := namespaceTableConvertor{}.ConvertToTable(context.Background(), ns, nil)
	if err != nil {
		t.Fatalf("ConvertToTable: %v", err)
	}
	mustHeader(t, tbl.ColumnDefinitions, []string{"Name", "Status", "Age"})
	if tbl.Rows[0].Cells[0] != "demo" || tbl.Rows[0].Cells[1] != "Active" {
		t.Errorf("cells=%v", tbl.Rows[0].Cells)
	}
}

func TestNamespaceTableConvertor_DefaultStatus(t *testing.T) {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "x"}}
	tbl, _ := namespaceTableConvertor{}.ConvertToTable(context.Background(), ns, nil)
	if tbl.Rows[0].Cells[1] != "Active" {
		t.Errorf("default phase cell=%v want Active", tbl.Rows[0].Cells[1])
	}
}

func TestShortHumanDuration(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want string
	}{
		{0, "0s"},
		{500 * time.Millisecond, "0s"},
		{45 * time.Second, "45s"},
		{5 * time.Minute, "5m"},
		{2 * time.Hour, "2h"},
		{3 * 24 * time.Hour, "3d"},
		{2 * 365 * 24 * time.Hour, "2y"},
	}
	for _, c := range cases {
		if got := shortHumanDuration(c.d); got != c.want {
			t.Errorf("d=%s got=%q want=%q", c.d, got, c.want)
		}
	}
}

func TestPodTableConvertor_RejectsWrongType(t *testing.T) {
	if _, err := (podTableConvertor{}).ConvertToTable(context.Background(), &corev1.Namespace{}, nil); err == nil {
		t.Errorf("expected type error")
	}
}

func TestPodTableConvertor_TerminatingPod(t *testing.T) {
	now := metav1.Now()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "x", DeletionTimestamp: &now},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c"}}},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Ready: true,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
	tbl, err := podTableConvertor{}.ConvertToTable(context.Background(), pod, nil)
	if err != nil {
		t.Fatalf("ConvertToTable: %v", err)
	}
	if tbl.Rows[0].Cells[2] != "Terminating" {
		t.Errorf("status cell=%v want Terminating", tbl.Rows[0].Cells[2])
	}
}

func TestEventTableConvertor_Row(t *testing.T) {
	now := metav1.NewTime(time.Now().Add(-90 * time.Second))
	e := &corev1.Event{
		ObjectMeta:    metav1.ObjectMeta{Name: "nginx.abc", Namespace: "default"},
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Name: "nginx", Namespace: "default"},
		Reason:        "Pulled",
		Message:       "Successfully pulled image \"nginx\"",
		Type:          corev1.EventTypeNormal,
		FirstTimestamp: now, LastTimestamp: now,
		Source: corev1.EventSource{Component: "kubelet-lite"},
	}
	tbl, err := eventTableConvertor{}.ConvertToTable(context.Background(), e, nil)
	if err != nil {
		t.Fatalf("ConvertToTable: %v", err)
	}
	if len(tbl.Rows) != 1 {
		t.Fatalf("rows=%d want 1", len(tbl.Rows))
	}
	row := tbl.Rows[0]
	want := []interface{}{"1m" /* age */, "Normal", "Pulled", "Pod/nginx", "Successfully pulled image \"nginx\""}
	for i := 1; i < len(want); i++ {
		if row.Cells[i] != want[i] {
			t.Errorf("cell[%d]=%v want %v", i, row.Cells[i], want[i])
		}
	}
	if !strings.HasSuffix(row.Cells[0].(string), "s") && !strings.HasSuffix(row.Cells[0].(string), "m") {
		t.Errorf("age cell=%q expected duration-like suffix", row.Cells[0])
	}
}

func TestEventLastSeen_FallsBackThroughTimestamps(t *testing.T) {
	zero := metav1.Time{}
	a := metav1.NewTime(time.Now().Add(-time.Hour))
	b := metav1.NewTime(time.Now().Add(-time.Minute))
	if got := eventLastSeen(&corev1.Event{LastTimestamp: b, FirstTimestamp: a}); !got.Equal(&b) {
		t.Errorf("LastTimestamp should win, got %v", got)
	}
	if got := eventLastSeen(&corev1.Event{LastTimestamp: zero, FirstTimestamp: a}); !got.Equal(&a) {
		t.Errorf("FirstTimestamp fallback failed, got %v", got)
	}
	mt := metav1.NewMicroTime(a.Time)
	if got := eventLastSeen(&corev1.Event{EventTime: mt}); !got.Time.Equal(a.Time) {
		t.Errorf("EventTime fallback failed, got %v", got)
	}
}
