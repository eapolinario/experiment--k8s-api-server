package kubelet

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func mkProjPod(uid string, container corev1.Container, vols []corev1.Volume) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "proj", Namespace: "default", UID: types.UID(uid)},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{container},
			Volumes:    vols,
		},
	}
}

func TestResolveEnv_LiteralValuesPassThrough(t *testing.T) {
	c := corev1.Container{
		Env: []corev1.EnvVar{{Name: "FOO", Value: "bar"}, {Name: "BAZ", Value: "qux"}},
	}
	got, err := resolveEnv(context.Background(), fake.NewClientset(), "default", &c)
	if err != nil {
		t.Fatalf("resolveEnv: %v", err)
	}
	want := []string{"FOO=bar", "BAZ=qux"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("env=%v want %v", got, want)
	}
}

func TestResolveEnv_FromConfigMapAndSecret(t *testing.T) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "cfg", Namespace: "default"},
		Data:       map[string]string{"GREETING": "hello", "TARGET": "world"},
	}
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "sec", Namespace: "default"},
		Data:       map[string][]byte{"TOKEN": []byte("s3cret")},
	}
	kube := fake.NewClientset(cm, sec)

	c := corev1.Container{
		EnvFrom: []corev1.EnvFromSource{
			{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "cfg"}}, Prefix: "C_"},
		},
		Env: []corev1.EnvVar{
			{Name: "TOK", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "sec"}, Key: "TOKEN"}}},
			{Name: "MSG", ValueFrom: &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "cfg"}, Key: "GREETING"}}},
		},
	}
	got, err := resolveEnv(context.Background(), kube, "default", &c)
	if err != nil {
		t.Fatalf("resolveEnv: %v", err)
	}
	sort.Strings(got)
	want := []string{"C_GREETING=hello", "C_TARGET=world", "MSG=hello", "TOK=s3cret"}
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("env=%v want %v", got, want)
	}
}

func TestResolveEnv_MissingRequiredFails(t *testing.T) {
	c := corev1.Container{
		Env: []corev1.EnvVar{
			{Name: "X", ValueFrom: &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "missing"}, Key: "x"}}},
		},
	}
	if _, err := resolveEnv(context.Background(), fake.NewClientset(), "default", &c); err == nil {
		t.Fatalf("expected error for missing required configMap")
	}
}

func TestResolveEnv_MissingOptionalSkipped(t *testing.T) {
	tr := true
	c := corev1.Container{
		Env: []corev1.EnvVar{
			{Name: "X", ValueFrom: &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "missing"}, Key: "x", Optional: &tr}}},
			{Name: "Y", Value: "yes"},
		},
	}
	got, err := resolveEnv(context.Background(), fake.NewClientset(), "default", &c)
	if err != nil {
		t.Fatalf("resolveEnv: %v", err)
	}
	if len(got) != 1 || got[0] != "Y=yes" {
		t.Errorf("got=%v want [Y=yes]", got)
	}
}

func TestProjectVolumes_ConfigMapAndSecret(t *testing.T) {
	root := t.TempDir()
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "cfg", Namespace: "default"},
		Data:       map[string]string{"app.conf": "key=value\n"},
	}
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "sec", Namespace: "default"},
		Data:       map[string][]byte{"token": []byte("hunter2")},
	}
	kube := fake.NewClientset(cm, sec)

	pod := mkProjPod("u1",
		corev1.Container{
			Name: "c",
			VolumeMounts: []corev1.VolumeMount{
				{Name: "config-vol", MountPath: "/etc/cfg"},
				{Name: "secret-vol", MountPath: "/etc/sec"},
			},
		},
		[]corev1.Volume{
			{Name: "config-vol", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "cfg"}}}},
			{Name: "secret-vol", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "sec"}}},
		},
	)
	mounts, err := projectVolumes(context.Background(), kube, pod, &pod.Spec.Containers[0], root)
	if err != nil {
		t.Fatalf("projectVolumes: %v", err)
	}
	if len(mounts) != 2 {
		t.Fatalf("got %d mounts want 2: %+v", len(mounts), mounts)
	}
	for _, m := range mounts {
		if !m.ReadOnly {
			t.Errorf("mount %+v should be ReadOnly", m)
		}
	}

	cmFile := filepath.Join(root, "u1", "config-vol", "app.conf")
	if got, _ := os.ReadFile(cmFile); string(got) != "key=value\n" {
		t.Errorf("cm file=%q want %q", got, "key=value\n")
	}
	secFile := filepath.Join(root, "u1", "secret-vol", "token")
	if got, _ := os.ReadFile(secFile); string(got) != "hunter2" {
		t.Errorf("secret file=%q want %q", got, "hunter2")
	}
	st, _ := os.Stat(secFile)
	if st.Mode().Perm() != 0o600 {
		t.Errorf("secret mode=%v want 0600", st.Mode().Perm())
	}
}

func TestProjectVolumes_StaleKeysRemoved(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "u2", "v")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "stale"), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "cfg", Namespace: "default"},
		Data:       map[string]string{"new": "data"},
	}
	pod := mkProjPod("u2",
		corev1.Container{Name: "c", VolumeMounts: []corev1.VolumeMount{{Name: "v", MountPath: "/x"}}},
		[]corev1.Volume{{Name: "v", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "cfg"}}}}},
	)
	if _, err := projectVolumes(context.Background(), fake.NewClientset(cm), pod, &pod.Spec.Containers[0], root); err != nil {
		t.Fatalf("projectVolumes: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "stale")); !os.IsNotExist(err) {
		t.Errorf("stale file should have been removed; stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "new")); err != nil {
		t.Errorf("new file missing: %v", err)
	}
}

func TestProjectVolumes_UnsupportedTypeSkipped(t *testing.T) {
	root := t.TempDir()
	pod := mkProjPod("u3",
		corev1.Container{Name: "c", VolumeMounts: []corev1.VolumeMount{{Name: "ed", MountPath: "/x"}}},
		[]corev1.Volume{{Name: "ed", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}},
	)
	mounts, err := projectVolumes(context.Background(), fake.NewClientset(), pod, &pod.Spec.Containers[0], root)
	if err != nil {
		t.Fatalf("projectVolumes: %v", err)
	}
	if len(mounts) != 0 {
		t.Errorf("emptyDir should be skipped, got %+v", mounts)
	}
}

func TestProjectVolumes_OrphanMountReferenceFails(t *testing.T) {
	root := t.TempDir()
	pod := mkProjPod("u4",
		corev1.Container{Name: "c", VolumeMounts: []corev1.VolumeMount{{Name: "ghost", MountPath: "/x"}}},
		nil,
	)
	if _, err := projectVolumes(context.Background(), fake.NewClientset(), pod, &pod.Spec.Containers[0], root); err == nil {
		t.Fatalf("expected error for volumeMount with no matching volume")
	}
}

func TestWriteKeyFiles_RejectsTraversal(t *testing.T) {
	dir := t.TempDir()
	err := writeKeyFiles(dir, map[string][]byte{"../escape": []byte("nope")}, 0o600)
	if err == nil {
		t.Errorf("expected error for path-traversing key")
	}
}
