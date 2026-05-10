// Package kubelet — projection.go
//
// Resolves Pod env / envFrom and ConfigMap / Secret volume sources by
// fetching the referenced objects from the apiserver and materialising
// them locally:
//
//   * env values are returned as KEY=VAL strings appended to ContainerSpec.Env.
//   * envFrom expands every key in the referenced ConfigMap or Secret
//     (Optional + Prefix supported).
//   * volumes of type configMap / secret get a per-pod, per-volume host
//     directory under <volumeRoot>/<podUID>/<volumeName>/, one file per
//     key (mode 0644 for ConfigMap, 0600 for Secret). The container then
//     bind-mounts that directory at the matching VolumeMount.MountPath.
//
// All other volume types (emptyDir, hostPath, projected, downwardAPI,
// PVC, ...) are silently skipped with a Warning event so we don't fail
// the pod outright; tests assert that behaviour.
//
// We deliberately do not implement defaultMode / items / subPath — those
// matter for hardening but aren't on the path to "kubectl apply a pod
// that consumes a ConfigMap and observe it via kubectl logs".
package kubelet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

// HostMount is the data the docker layer needs to set up a bind mount.
// Decoupled from docker SDK types so translate stays unit-testable.
type HostMount struct {
	Source   string // host path (must exist + be readable)
	Target   string // container path
	ReadOnly bool
}

// resolveEnv expands a container's Env + EnvFrom into a flat KEY=VAL slice.
// Missing optional sources are skipped; missing required sources return an
// error so reconcileFails surfaces it as a Failed event.
func resolveEnv(ctx context.Context, kube kubernetes.Interface, namespace string, c *corev1.Container) ([]string, error) {
	out := []string{}

	// envFrom first so that explicit env entries below can override.
	for _, ef := range c.EnvFrom {
		switch {
		case ef.ConfigMapRef != nil:
			cm, err := kube.CoreV1().ConfigMaps(namespace).Get(ctx, ef.ConfigMapRef.Name, metav1.GetOptions{})
			if err != nil {
				if apierrors.IsNotFound(err) && ef.ConfigMapRef.Optional != nil && *ef.ConfigMapRef.Optional {
					continue
				}
				return nil, fmt.Errorf("envFrom configMap %q: %w", ef.ConfigMapRef.Name, err)
			}
			for _, k := range sortedStringKeys(cm.Data) {
				out = append(out, fmt.Sprintf("%s%s=%s", ef.Prefix, k, cm.Data[k]))
			}
		case ef.SecretRef != nil:
			s, err := kube.CoreV1().Secrets(namespace).Get(ctx, ef.SecretRef.Name, metav1.GetOptions{})
			if err != nil {
				if apierrors.IsNotFound(err) && ef.SecretRef.Optional != nil && *ef.SecretRef.Optional {
					continue
				}
				return nil, fmt.Errorf("envFrom secret %q: %w", ef.SecretRef.Name, err)
			}
			for _, k := range sortedBytesKeys(s.Data) {
				out = append(out, fmt.Sprintf("%s%s=%s", ef.Prefix, k, string(s.Data[k])))
			}
		}
	}

	for _, e := range c.Env {
		switch {
		case e.ValueFrom == nil:
			out = append(out, fmt.Sprintf("%s=%s", e.Name, e.Value))
		case e.ValueFrom.ConfigMapKeyRef != nil:
			r := e.ValueFrom.ConfigMapKeyRef
			cm, err := kube.CoreV1().ConfigMaps(namespace).Get(ctx, r.Name, metav1.GetOptions{})
			if err != nil {
				if apierrors.IsNotFound(err) && r.Optional != nil && *r.Optional {
					continue
				}
				return nil, fmt.Errorf("env %q from configMap %q: %w", e.Name, r.Name, err)
			}
			v, ok := cm.Data[r.Key]
			if !ok {
				if r.Optional != nil && *r.Optional {
					continue
				}
				return nil, fmt.Errorf("env %q: configMap %q has no key %q", e.Name, r.Name, r.Key)
			}
			out = append(out, fmt.Sprintf("%s=%s", e.Name, v))
		case e.ValueFrom.SecretKeyRef != nil:
			r := e.ValueFrom.SecretKeyRef
			s, err := kube.CoreV1().Secrets(namespace).Get(ctx, r.Name, metav1.GetOptions{})
			if err != nil {
				if apierrors.IsNotFound(err) && r.Optional != nil && *r.Optional {
					continue
				}
				return nil, fmt.Errorf("env %q from secret %q: %w", e.Name, r.Name, err)
			}
			b, ok := s.Data[r.Key]
			if !ok {
				if r.Optional != nil && *r.Optional {
					continue
				}
				return nil, fmt.Errorf("env %q: secret %q has no key %q", e.Name, r.Name, r.Key)
			}
			out = append(out, fmt.Sprintf("%s=%s", e.Name, string(b)))
		default:
			klog.Warningf("env %q uses unsupported valueFrom variant; skipping", e.Name)
		}
	}
	return out, nil
}

// projectVolumes materialises ConfigMap / Secret volumes referenced by
// the container's volumeMounts and returns the corresponding host bind
// mounts. Other volume types are skipped with a Warning log line.
//
// volumeRoot is a host directory that kubelet-lite owns; this function
// creates volumeRoot/<podUID>/<volumeName>/ on demand.
func projectVolumes(ctx context.Context, kube kubernetes.Interface, pod *corev1.Pod, c *corev1.Container, volumeRoot string) ([]HostMount, error) {
	if len(c.VolumeMounts) == 0 {
		return nil, nil
	}
	// Index volumes by name for O(1) lookup.
	volIndex := map[string]*corev1.Volume{}
	for i := range pod.Spec.Volumes {
		volIndex[pod.Spec.Volumes[i].Name] = &pod.Spec.Volumes[i]
	}

	var mounts []HostMount
	for _, vm := range c.VolumeMounts {
		v := volIndex[vm.Name]
		if v == nil {
			return nil, fmt.Errorf("volumeMount %q has no matching pod.spec.volumes entry", vm.Name)
		}
		switch {
		case v.ConfigMap != nil:
			src, err := materialiseConfigMap(ctx, kube, pod, v, volumeRoot)
			if err != nil {
				return nil, err
			}
			mounts = append(mounts, HostMount{Source: src, Target: vm.MountPath, ReadOnly: true})
			_ = vm.ReadOnly // CM/Secret projections are always RO; field accepted for parity.
		case v.Secret != nil:
			src, err := materialiseSecret(ctx, kube, pod, v, volumeRoot)
			if err != nil {
				return nil, err
			}
			mounts = append(mounts, HostMount{Source: src, Target: vm.MountPath, ReadOnly: true})
		default:
			klog.Warningf("pod %s/%s volume %q: unsupported volume source type; skipping mount %q", pod.Namespace, pod.Name, v.Name, vm.MountPath)
		}
	}
	return mounts, nil
}

func materialiseConfigMap(ctx context.Context, kube kubernetes.Interface, pod *corev1.Pod, v *corev1.Volume, volumeRoot string) (string, error) {
	src := volumeHostDir(volumeRoot, pod, v.Name)
	cm, err := kube.CoreV1().ConfigMaps(pod.Namespace).Get(ctx, v.ConfigMap.Name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) && v.ConfigMap.Optional != nil && *v.ConfigMap.Optional {
			return src, ensureDir(src)
		}
		return "", fmt.Errorf("volume %q configMap %q: %w", v.Name, v.ConfigMap.Name, err)
	}
	files := map[string][]byte{}
	for k, val := range cm.Data {
		files[k] = []byte(val)
	}
	for k, b := range cm.BinaryData {
		files[k] = b
	}
	if err := writeKeyFiles(src, files, 0o644); err != nil {
		return "", err
	}
	return src, nil
}

func materialiseSecret(ctx context.Context, kube kubernetes.Interface, pod *corev1.Pod, v *corev1.Volume, volumeRoot string) (string, error) {
	src := volumeHostDir(volumeRoot, pod, v.Name)
	s, err := kube.CoreV1().Secrets(pod.Namespace).Get(ctx, v.Secret.SecretName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) && v.Secret.Optional != nil && *v.Secret.Optional {
			return src, ensureDir(src)
		}
		return "", fmt.Errorf("volume %q secret %q: %w", v.Name, v.Secret.SecretName, err)
	}
	if err := writeKeyFiles(src, s.Data, 0o600); err != nil {
		return "", err
	}
	return src, nil
}

// volumeHostDir is the canonical on-host path for a single
// (pod, volumeName) tuple. We key on Pod UID so a recreated pod with the
// same name doesn't pick up the previous instance's stale mount.
func volumeHostDir(volumeRoot string, pod *corev1.Pod, volName string) string {
	return filepath.Join(volumeRoot, string(pod.UID), volName)
}

// PodVolumeRoot is the on-host directory containing every volume for a
// single pod, suitable for cleanup when the pod is reaped.
func PodVolumeRoot(volumeRoot string, podUID string) string {
	return filepath.Join(volumeRoot, podUID)
}

func writeKeyFiles(dir string, files map[string][]byte, mode os.FileMode) error {
	if err := ensureDir(dir); err != nil {
		return err
	}
	// Truncate any stale files from a previous projection (e.g. CM key removed).
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if _, keep := files[e.Name()]; !keep {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
	for k, b := range files {
		// Reject path-traversal attempts in keys; real apiserver validation
		// rejects these on Create, but we belt-and-braces here.
		if filepath.Base(k) != k {
			return fmt.Errorf("invalid key %q (must not contain path separators)", k)
		}
		if err := os.WriteFile(filepath.Join(dir, k), b, mode); err != nil {
			return fmt.Errorf("write %s: %w", k, err)
		}
	}
	return nil
}

func ensureDir(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}
	return nil
}

func sortedStringKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedBytesKeys(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
