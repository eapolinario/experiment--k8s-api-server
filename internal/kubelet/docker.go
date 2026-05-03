package kubelet

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	dockerclient "github.com/docker/docker/client"
	"k8s.io/klog/v2"
)

// DockerRuntime is a thin wrapper over the Docker SDK with only the methods
// kubelet-lite uses. It exists so the reconciler depends on a small, easy-to-
// fake interface instead of the full SDK surface.
type DockerRuntime interface {
	EnsurePulled(ctx context.Context, ref string, policy string) error
	CreateAndStart(ctx context.Context, name string, spec ContainerSpec) (string, error)
	Inspect(ctx context.Context, name string) (ContainerView, bool, error)
	Stop(ctx context.Context, name string, gracePeriod time.Duration) error
	Remove(ctx context.Context, name string) error
	ListManagedUIDs(ctx context.Context) (map[string]string, error) // uid -> container name
	Close() error
}

type dockerRT struct {
	cli *dockerclient.Client
}

// NewDockerRuntime dials the local Docker daemon via env (DOCKER_HOST etc)
// with API version negotiation.
func NewDockerRuntime() (DockerRuntime, error) {
	cli, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("docker client: %w", err)
	}
	return &dockerRT{cli: cli}, nil
}

func (r *dockerRT) Close() error { return r.cli.Close() }

func (r *dockerRT) imageExists(ctx context.Context, ref string) (bool, error) {
	_, err := r.cli.ImageInspect(ctx, ref)
	if err == nil {
		return true, nil
	}
	if dockerclient.IsErrNotFound(err) {
		return false, nil
	}
	return false, err
}

func (r *dockerRT) EnsurePulled(ctx context.Context, ref string, policy string) error {
	switch policy {
	case "Never":
		exists, err := r.imageExists(ctx, ref)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("image %q not present and pullPolicy=Never", ref)
		}
		return nil
	case "IfNotPresent":
		exists, err := r.imageExists(ctx, ref)
		if err != nil {
			return err
		}
		if exists {
			return nil
		}
		fallthrough
	case "Always", "":
		return r.pull(ctx, ref)
	default:
		return fmt.Errorf("unknown imagePullPolicy %q", policy)
	}
}

func (r *dockerRT) pull(ctx context.Context, ref string) error {
	klog.V(2).Infof("docker pull %s", ref)
	rc, err := r.cli.ImagePull(ctx, ref, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("ImagePull(%s): %w", ref, err)
	}
	defer rc.Close()

	// Drain the pull stream; log first and last lines for diagnostics.
	scanner := bufio.NewScanner(rc)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	var first, last string
	for scanner.Scan() {
		line := scanner.Text()
		if first == "" {
			first = line
		}
		last = line
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("read pull stream: %w", err)
	}
	if first != "" {
		klog.V(3).Infof("pull %s first: %s", ref, first)
	}
	if last != "" && last != first {
		klog.V(3).Infof("pull %s last:  %s", ref, last)
	}
	return nil
}

func (r *dockerRT) CreateAndStart(ctx context.Context, name string, spec ContainerSpec) (string, error) {
	cfg := &container.Config{
		Image:      spec.Image,
		Env:        spec.Env,
		Labels:     spec.Labels,
		WorkingDir: spec.WorkingDir,
	}
	if len(spec.Cmd) > 0 {
		cfg.Cmd = spec.Cmd
	}
	if len(spec.Entrypoint) > 0 {
		cfg.Entrypoint = spec.Entrypoint
	}
	host := &container.HostConfig{
		RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled},
	}
	resp, err := r.cli.ContainerCreate(ctx, cfg, host, nil, nil, name)
	if err != nil {
		return "", fmt.Errorf("ContainerCreate(%s): %w", name, err)
	}
	if err := r.cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		// Try to clean up so we don't leak a stopped container with our name.
		_ = r.cli.ContainerRemove(ctx, resp.ID, container.RemoveOptions{Force: true})
		return "", fmt.Errorf("ContainerStart(%s): %w", name, err)
	}
	return resp.ID, nil
}

func (r *dockerRT) Inspect(ctx context.Context, name string) (ContainerView, bool, error) {
	insp, err := r.cli.ContainerInspect(ctx, name)
	if err != nil {
		if dockerclient.IsErrNotFound(err) {
			return ContainerView{}, false, nil
		}
		return ContainerView{}, false, err
	}
	v := ContainerView{
		ID:      insp.ID,
		Image:   insp.Config.Image,
		ImageID: insp.Image,
	}
	if insp.State != nil {
		v.Running = insp.State.Running
		v.ExitCode = insp.State.ExitCode
		v.OOMKilled = insp.State.OOMKilled
		if t, err := time.Parse(time.RFC3339Nano, insp.State.StartedAt); err == nil {
			v.StartedAt = t
		}
		if t, err := time.Parse(time.RFC3339Nano, insp.State.FinishedAt); err == nil {
			v.FinishedAt = t
		}
	}
	return v, true, nil
}

func (r *dockerRT) Stop(ctx context.Context, name string, gracePeriod time.Duration) error {
	secs := int(gracePeriod.Seconds())
	opts := container.StopOptions{Timeout: &secs}
	if err := r.cli.ContainerStop(ctx, name, opts); err != nil {
		if dockerclient.IsErrNotFound(err) {
			return nil
		}
		return err
	}
	return nil
}

func (r *dockerRT) Remove(ctx context.Context, name string) error {
	err := r.cli.ContainerRemove(ctx, name, container.RemoveOptions{Force: true})
	if err != nil && !dockerclient.IsErrNotFound(err) {
		return err
	}
	return nil
}

func (r *dockerRT) ListManagedUIDs(ctx context.Context) (map[string]string, error) {
	args := filters.NewArgs(filters.KeyValuePair{Key: "label", Value: LabelPodUID})
	containers, err := r.cli.ContainerList(ctx, container.ListOptions{All: true, Filters: args})
	if err != nil {
		return nil, fmt.Errorf("ContainerList: %w", err)
	}
	out := make(map[string]string, len(containers))
	for _, c := range containers {
		uid := c.Labels[LabelPodUID]
		if uid == "" {
			continue
		}
		// Pick the first matching name (stripping leading slash).
		var name string
		for _, n := range c.Names {
			name = strings.TrimPrefix(n, "/")
			break
		}
		out[uid] = name
	}
	return out, nil
}
