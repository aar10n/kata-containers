package docker

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/platform"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/docker/docker/pkg/stdcopy"
)

const (
	labelSessionID    = "sandbox.cohere.com/session-id"
	labelManagedBy    = "sandbox.cohere.com/managed-by"
	labelLastActivity = "sandbox.cohere.com.last-activity"
	activityDirName   = ".sandbox-service/activity"
)

type Platform struct {
	docker         *client.Client
	defaultImage   string
	defaultCommand []string
}

func New(defaultImage string, defaultCommand []string) (*Platform, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("create docker client: %w", err)
	}
	return &Platform{
		docker:         cli,
		defaultImage:   defaultImage,
		defaultCommand: append([]string{}, defaultCommand...),
	}, nil
}

func (p *Platform) CreateSandbox(ctx context.Context, req platform.CreateSandboxRequest) (*platform.Sandbox, error) {
	name := containerName(req.SessionID)
	_, err := p.docker.ContainerInspect(ctx, name)
	if err == nil {
		return nil, platform.ErrAlreadyExists
	}
	if err != nil && !errdefs.IsNotFound(err) {
		return nil, fmt.Errorf("inspect container: %w", err)
	}

	image := req.Image
	if image == "" {
		image = p.defaultImage
	}
	command := req.Command
	if len(command) == 0 {
		command = append([]string{}, p.defaultCommand...)
	}

	now := time.Now().UTC()
	labels := map[string]string{
		labelSessionID:    req.SessionID,
		labelManagedBy:    "sandbox-service",
		labelLastActivity: now.Format(time.RFC3339),
	}
	for key, value := range req.Labels {
		labels[key] = value
	}

	resp, err := p.docker.ContainerCreate(ctx, &container.Config{
		Image:  image,
		Cmd:    command,
		Env:    mapToEnv(req.Env),
		Labels: labels,
	}, nil, nil, nil, name)
	if err != nil {
		return nil, fmt.Errorf("create container: %w", err)
	}

	if err := p.docker.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return nil, fmt.Errorf("start container: %w", err)
	}
	if err := p.writeLastActivity(resp.ID, now); err != nil {
		log.Printf("persist last activity failed for session %s: %v", req.SessionID, err)
	}

	return &platform.Sandbox{
		SessionID:   req.SessionID,
		SandboxID:   resp.ID,
		ContainerID: resp.ID,
		Status:      platform.StatusRunning,
		Host:        "docker",
		CreatedAt:   now,
		LastUsedAt:  now,
		Labels:      labels,
	}, nil
}

func (p *Platform) GetSandbox(ctx context.Context, sessionID string) (*platform.Sandbox, error) {
	name := containerName(sessionID)
	inspect, err := p.docker.ContainerInspect(ctx, name)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil, platform.ErrNotFound
		}
		return nil, fmt.Errorf("inspect container: %w", err)
	}

	status := platform.StatusRunning
	if inspect.State != nil {
		switch {
		case inspect.State.Running:
			status = platform.StatusRunning
		case inspect.State.OOMKilled || inspect.State.Dead || inspect.State.ExitCode != 0:
			status = platform.StatusFailed
		case inspect.State.Status == "exited":
			status = platform.StatusTerminated
		default:
			status = platform.StatusPending
		}
	}

	createdAt := time.Now().UTC()
	if inspect.Created != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, inspect.Created); err == nil {
			createdAt = parsed
		}
	}
	lastUsedAt := p.readLastActivity(inspect.ID, createdAt, inspect.Config.Labels)

	return &platform.Sandbox{
		SessionID:   sessionID,
		SandboxID:   inspect.ID,
		ContainerID: inspect.ID,
		Status:      status,
		Host:        "docker",
		CreatedAt:   createdAt,
		LastUsedAt:  lastUsedAt,
		Labels:      inspect.Config.Labels,
	}, nil
}

func (p *Platform) DeleteSandbox(ctx context.Context, sessionID string) error {
	name := containerName(sessionID)
	err := p.docker.ContainerRemove(ctx, name, container.RemoveOptions{Force: true})
	if err != nil {
		if errdefs.IsNotFound(err) {
			return platform.ErrNotFound
		}
		return fmt.Errorf("remove container: %w", err)
	}
	return nil
}

func (p *Platform) ListSandboxes(ctx context.Context) ([]*platform.Sandbox, error) {
	args := filters.NewArgs()
	args.Add("label", labelManagedBy+"=sandbox-service")
	items, err := p.docker.ContainerList(ctx, container.ListOptions{All: true, Filters: args})
	if err != nil {
		return nil, fmt.Errorf("list containers: %w", err)
	}

	result := make([]*platform.Sandbox, 0, len(items))
	for _, item := range items {
		sessionID := item.Labels[labelSessionID]
		status := platform.StatusPending
		if item.State == "running" {
			status = platform.StatusRunning
		} else if item.State == "exited" {
			status = platform.StatusTerminated
		}
		result = append(result, &platform.Sandbox{
			SessionID:   sessionID,
			SandboxID:   item.ID,
			ContainerID: item.ID,
			Status:      status,
			Host:        "docker",
			CreatedAt:   time.Unix(item.Created, 0).UTC(),
			LastUsedAt:  p.readLastActivity(item.ID, time.Unix(item.Created, 0).UTC(), item.Labels),
			Labels:      item.Labels,
		})
	}
	return result, nil
}

func (p *Platform) Exec(ctx context.Context, req platform.ExecRequest) (*platform.ExecResult, error) {
	name := containerName(req.SessionID)
	ctx, cancel := applyTimeout(ctx, req.Timeout)
	defer cancel()

	execResp, err := p.docker.ContainerExecCreate(ctx, name, types.ExecConfig{
		Cmd:          req.Command,
		Env:          mapToEnv(req.Env),
		WorkingDir:   req.WorkingDir,
		AttachStdout: true,
		AttachStderr: true,
	})
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil, platform.ErrNotFound
		}
		return nil, fmt.Errorf("create exec: %w", err)
	}

	attach, err := p.docker.ContainerExecAttach(ctx, execResp.ID, types.ExecStartCheck{})
	if err != nil {
		return nil, fmt.Errorf("attach exec: %w", err)
	}
	defer attach.Close()

	stdout, stderr, err := readExecOutput(ctx, attach.Reader)
	if err != nil {
		return nil, err
	}

	inspect, err := p.docker.ContainerExecInspect(ctx, execResp.ID)
	if err != nil {
		return nil, fmt.Errorf("inspect exec: %w", err)
	}

	result := &platform.ExecResult{
		ExitCode: inspect.ExitCode,
		Stdout:   stdout,
		Stderr:   stderr,
	}
	containerID := inspect.ContainerID
	if containerID == "" {
		containerID = name
	}
	if err := p.writeLastActivity(containerID, time.Now().UTC()); err != nil {
		log.Printf("persist last activity failed for session %s: %v", req.SessionID, err)
	}
	return result, nil
}

func applyTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

func readExecOutput(ctx context.Context, reader io.Reader) (string, string, error) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	result := make(chan error, 1)

	go func() {
		_, err := stdcopy.StdCopy(&stdout, &stderr, reader)
		result <- err
	}()

	select {
	case err := <-result:
		if err != nil {
			return "", "", fmt.Errorf("read exec output: %w", err)
		}
	case <-ctx.Done():
		return "", "", ctx.Err()
	}

	return stdout.String(), stderr.String(), nil
}

func mapToEnv(values map[string]string) []string {
	if len(values) == 0 {
		return nil
	}
	result := make([]string, 0, len(values))
	for key, value := range values {
		result = append(result, fmt.Sprintf("%s=%s", key, value))
	}
	return result
}

func (p *Platform) readLastActivity(containerID string, fallback time.Time, labels map[string]string) time.Time {
	if containerID != "" {
		if value, err := p.readLastActivityFromFile(containerID); err == nil && !value.IsZero() {
			return value
		}
	}
	if labels != nil {
		if value := strings.TrimSpace(labels[labelLastActivity]); value != "" {
			if parsed, err := time.Parse(time.RFC3339, value); err == nil {
				return parsed
			}
		}
	}
	return fallback
}

func (p *Platform) readLastActivityFromFile(containerID string) (time.Time, error) {
	path, err := activityPath(containerID)
	if err != nil {
		return time.Time{}, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return time.Time{}, err
	}
	value := strings.TrimSpace(string(data))
	if value == "" {
		return time.Time{}, fmt.Errorf("empty last activity")
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, err
	}
	return parsed, nil
}

func (p *Platform) writeLastActivity(containerID string, at time.Time) error {
	path, err := activityPath(containerID)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(at.UTC().Format(time.RFC3339)), 0o600)
}

func activityPath(containerID string) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, activityDirName, containerID), nil
}

func containerName(sessionID string) string {
	base := sanitizeName(sessionID)
	if base == "" {
		base = "sandbox"
	}
	name := "sandbox-" + base
	if len(name) <= 63 {
		return name
	}
	suffix := hashSuffix(sessionID)
	trim := 63 - len("sandbox-") - 1 - len(suffix)
	if trim < 1 {
		trim = 1
	}
	return "sandbox-" + base[:trim] + "-" + suffix
}

func sanitizeName(value string) string {
	value = strings.ToLower(value)
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '.' || r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	return strings.Trim(b.String(), "-._")
}

func hashSuffix(value string) string {
	sum := sha1.Sum([]byte(value))
	return hex.EncodeToString(sum[:4])
}
