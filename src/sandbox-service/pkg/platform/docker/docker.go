package docker

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
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
	docker    *client.Client
	mu        sync.RWMutex
	processes map[string]*dockerProcess
}

type dockerProcess struct {
	execID       string
	containerID  string
	conn         types.HijackedResponse
	buffer       []byte
	stdoutBuffer []byte
	stderrBuffer []byte
	terminal     bool
	mu           sync.Mutex
}

func New() (*Platform, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("create docker client: %w", err)
	}
	return &Platform{
		docker:    cli,
		processes: make(map[string]*dockerProcess),
	}, nil
}

func (p *Platform) CreateSandbox(ctx context.Context, req platform.CreateSandboxRequest) (*platform.Sandbox, error) {
	if req.Image == "" {
		return nil, errors.New("image is required")
	}
	if len(req.Command) == 0 {
		return nil, errors.New("command is required")
	}

	name := containerName(req.SessionID)
	_, err := p.docker.ContainerInspect(ctx, name)
	if err == nil {
		return nil, platform.ErrAlreadyExists
	}
	if err != nil && !errdefs.IsNotFound(err) {
		return nil, fmt.Errorf("inspect container: %w", err)
	}

	image := req.Image
	command := req.Command

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
		slog.Warn("persist last activity failed", "session_id", req.SessionID, "error", err)
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

	attach, err := p.docker.ContainerExecAttach(ctx, execResp.ID, types.ExecStartCheck{Tty: false})
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
		slog.Warn("persist last activity failed", "session_id", req.SessionID, "error", err)
	}
	return result, nil
}

func (p *Platform) StartProcess(ctx context.Context, req platform.StartProcessRequest) (*platform.Process, error) {
	name := containerName(req.SessionID)
	execResp, err := p.docker.ContainerExecCreate(ctx, name, types.ExecConfig{
		Cmd:          req.Command,
		Env:          req.Env,
		AttachStdin:  true,
		AttachStdout: true,
		AttachStderr: true,
		Tty:          req.Terminal,
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

	proc := &dockerProcess{
		execID:      execResp.ID,
		containerID: name,
		conn:        attach,
		terminal:    req.Terminal,
	}

	p.mu.Lock()
	p.processes[req.ExecID] = proc
	p.mu.Unlock()

	now := time.Now().UTC()
	if err := p.writeLastActivity(name, now); err != nil {
		slog.Warn("persist last activity failed", "session_id", req.SessionID, "error", err)
	}

	return &platform.Process{
		ExecID:    req.ExecID,
		StartedAt: now,
		Alive:     true,
	}, nil
}

func (p *Platform) WriteToProcess(ctx context.Context, sessionID, execID string, data []byte) error {
	proc := p.getProcess(execID)
	if proc == nil {
		return fmt.Errorf("process not found: %s", execID)
	}

	proc.mu.Lock()
	_, err := proc.conn.Conn.Write(data)
	proc.mu.Unlock()

	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
			proc.mu.Lock()
			proc.conn.Close()
			proc.mu.Unlock()
			p.mu.Lock()
			delete(p.processes, execID)
			p.mu.Unlock()
		}
		return err
	}
	if err := p.writeLastActivity(proc.containerID, time.Now().UTC()); err != nil {
		slog.Warn("persist last activity failed", "session_id", sessionID, "error", err)
	}
	return nil
}

func (p *Platform) ReadFromProcess(ctx context.Context, sessionID, execID string) (*platform.ProcessOutput, error) {
	proc := p.getProcess(execID)
	if proc == nil {
		return nil, fmt.Errorf("process not found: %s", execID)
	}

	buf := make([]byte, 4096)

	proc.mu.Lock()
	defer proc.mu.Unlock()

	if err := proc.conn.Conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		return nil, err
	}

	n, err := proc.conn.Reader.Read(buf)
	if err != nil && !isTimeout(err) {
		if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
			proc.conn.Close()
			p.mu.Lock()
			delete(p.processes, execID)
			p.mu.Unlock()
		}
		return nil, err
	}

	if n <= 0 {
		return &platform.ProcessOutput{}, nil
	}

	proc.buffer = append(proc.buffer, buf[:n]...)

	if proc.terminal {
		if isDockerMuxed(proc.buffer) {
			stdout, _, remaining, err := demuxDockerStream(proc.buffer)
			if err != nil {
				return nil, err
			}
			proc.buffer = remaining
			return &platform.ProcessOutput{Stdout: stdout}, nil
		}
		stdout := append([]byte{}, proc.buffer...)
		proc.buffer = nil
		return &platform.ProcessOutput{Stdout: stdout}, nil
	}

	stdout, stderr, remaining, err := demuxDockerStream(proc.buffer)
	if err != nil {
		return nil, err
	}
	proc.buffer = remaining

	return &platform.ProcessOutput{
		Stdout: stdout,
		Stderr: stderr,
	}, nil
}

// fillBuffers reads available data from the process stream and fills stdout/stderr buffers.
// Must be called with proc.mu held.
func (p *Platform) fillBuffers(proc *dockerProcess) error {
	buf := make([]byte, 4096)

	if err := proc.conn.Conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		return err
	}

	n, err := proc.conn.Reader.Read(buf)
	if err != nil && !isTimeout(err) {
		if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
			proc.conn.Close()
		}
		return err
	}

	if n <= 0 {
		return nil
	}

	proc.buffer = append(proc.buffer, buf[:n]...)

	if proc.terminal {
		// Terminal mode: all output is stdout
		if isDockerMuxed(proc.buffer) {
			stdout, _, remaining, err := demuxDockerStream(proc.buffer)
			if err != nil {
				return err
			}
			proc.buffer = remaining
			proc.stdoutBuffer = append(proc.stdoutBuffer, stdout...)
		} else {
			proc.stdoutBuffer = append(proc.stdoutBuffer, proc.buffer...)
			proc.buffer = nil
		}
	} else {
		// Non-terminal: demux into stdout and stderr
		stdout, stderr, remaining, err := demuxDockerStream(proc.buffer)
		if err != nil {
			return err
		}
		proc.buffer = remaining
		proc.stdoutBuffer = append(proc.stdoutBuffer, stdout...)
		proc.stderrBuffer = append(proc.stderrBuffer, stderr...)
	}

	return nil
}

// ReadStdout reads only stdout from a process.
func (p *Platform) ReadStdout(ctx context.Context, sessionID, execID string) ([]byte, error) {
	proc := p.getProcess(execID)
	if proc == nil {
		return nil, fmt.Errorf("process not found: %s", execID)
	}

	proc.mu.Lock()
	defer proc.mu.Unlock()

	if err := p.fillBuffers(proc); err != nil && !isTimeout(err) {
		return nil, err
	}

	stdout := proc.stdoutBuffer
	proc.stdoutBuffer = nil
	return stdout, nil
}

// ReadStderr reads only stderr from a process.
func (p *Platform) ReadStderr(ctx context.Context, sessionID, execID string) ([]byte, error) {
	proc := p.getProcess(execID)
	if proc == nil {
		return nil, fmt.Errorf("process not found: %s", execID)
	}

	proc.mu.Lock()
	defer proc.mu.Unlock()

	if err := p.fillBuffers(proc); err != nil && !isTimeout(err) {
		return nil, err
	}

	stderr := proc.stderrBuffer
	proc.stderrBuffer = nil
	return stderr, nil
}

func (p *Platform) KillProcess(ctx context.Context, sessionID, execID string) error {
	proc := p.getProcess(execID)
	if proc == nil {
		return platform.ErrNotFound
	}
	inspect, err := p.docker.ContainerExecInspect(ctx, proc.execID)
	if err != nil {
		return err
	}
	if inspect.Pid > 0 {
		if err := syscall.Kill(inspect.Pid, syscall.SIGKILL); err != nil {
			return err
		}
	}

	proc.mu.Lock()
	proc.conn.Close()
	proc.mu.Unlock()

	p.mu.Lock()
	delete(p.processes, execID)
	p.mu.Unlock()
	return nil
}

func (p *Platform) IsProcessAlive(ctx context.Context, sessionID, execID string) (bool, error) {
	proc := p.getProcess(execID)
	if proc == nil {
		return false, nil
	}
	inspect, err := p.docker.ContainerExecInspect(ctx, proc.execID)
	if err != nil {
		return false, err
	}
	if !inspect.Running {
		proc.mu.Lock()
		proc.conn.Close()
		proc.mu.Unlock()
		p.mu.Lock()
		delete(p.processes, execID)
		p.mu.Unlock()
		return false, nil
	}
	return true, nil
}

func (p *Platform) ResizeProcess(ctx context.Context, sessionID, execID string, rows, columns uint32) error {
	proc := p.getProcess(execID)
	if proc == nil {
		return platform.ErrNotFound
	}
	if !proc.terminal {
		return fmt.Errorf("process does not use a terminal: %s", execID)
	}
	return p.docker.ContainerExecResize(ctx, proc.execID, container.ResizeOptions{
		Height: uint(rows),
		Width:  uint(columns),
	})
}

const defaultStreamPollInterval = 50 * time.Millisecond

// StreamStdout returns a channel that streams stdout chunks from a process.
func (p *Platform) StreamStdout(ctx context.Context, req platform.StreamReadRequest) (<-chan platform.StreamChunk, error) {
	proc := p.getProcess(req.ExecID)
	if proc == nil {
		return nil, fmt.Errorf("process not found: %s", req.ExecID)
	}

	pollInterval := defaultStreamPollInterval
	if req.PollIntervalMs > 0 {
		pollInterval = time.Duration(req.PollIntervalMs) * time.Millisecond
	}

	ch := make(chan platform.StreamChunk)
	go func() {
		defer close(ch)
		consecutiveEmpty := 0
		const maxConsecutiveEmpty = 3

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			proc.mu.Lock()
			if err := p.fillBuffers(proc); err != nil && !isTimeout(err) {
				proc.mu.Unlock()
				ch <- platform.StreamChunk{EOF: true, Err: err}
				return
			}
			data := proc.stdoutBuffer
			proc.stdoutBuffer = nil
			proc.mu.Unlock()

			if len(data) > 0 {
				consecutiveEmpty = 0
				select {
				case ch <- platform.StreamChunk{Data: data}:
				case <-ctx.Done():
					return
				}
			} else {
				consecutiveEmpty++
				if consecutiveEmpty >= maxConsecutiveEmpty {
					ch <- platform.StreamChunk{EOF: true}
					return
				}
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(pollInterval):
			}
		}
	}()
	return ch, nil
}

// StreamStderr returns a channel that streams stderr chunks from a process.
func (p *Platform) StreamStderr(ctx context.Context, req platform.StreamReadRequest) (<-chan platform.StreamChunk, error) {
	proc := p.getProcess(req.ExecID)
	if proc == nil {
		return nil, fmt.Errorf("process not found: %s", req.ExecID)
	}

	pollInterval := defaultStreamPollInterval
	if req.PollIntervalMs > 0 {
		pollInterval = time.Duration(req.PollIntervalMs) * time.Millisecond
	}

	ch := make(chan platform.StreamChunk)
	go func() {
		defer close(ch)
		consecutiveEmpty := 0
		const maxConsecutiveEmpty = 3

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			proc.mu.Lock()
			if err := p.fillBuffers(proc); err != nil && !isTimeout(err) {
				proc.mu.Unlock()
				ch <- platform.StreamChunk{EOF: true, Err: err}
				return
			}
			data := proc.stderrBuffer
			proc.stderrBuffer = nil
			proc.mu.Unlock()

			if len(data) > 0 {
				consecutiveEmpty = 0
				select {
				case ch <- platform.StreamChunk{Data: data}:
				case <-ctx.Done():
					return
				}
			} else {
				consecutiveEmpty++
				if consecutiveEmpty >= maxConsecutiveEmpty {
					ch <- platform.StreamChunk{EOF: true}
					return
				}
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(pollInterval):
			}
		}
	}()
	return ch, nil
}

// StreamOutput returns a channel that streams both stdout and stderr chunks.
// For Docker, this uses polling to read from both buffers.
func (p *Platform) StreamOutput(ctx context.Context, req platform.StreamReadRequest) (<-chan platform.OutputChunk, error) {
	proc := p.getProcess(req.ExecID)
	if proc == nil {
		return nil, fmt.Errorf("process not found: %s", req.ExecID)
	}

	pollInterval := defaultStreamPollInterval
	if req.PollIntervalMs > 0 {
		pollInterval = time.Duration(req.PollIntervalMs) * time.Millisecond
	}

	ch := make(chan platform.OutputChunk, 16)
	go func() {
		defer close(ch)
		consecutiveEmpty := 0
		const maxConsecutiveEmpty = 3

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			proc.mu.Lock()
			if err := p.fillBuffers(proc); err != nil && !isTimeout(err) {
				proc.mu.Unlock()
				ch <- platform.OutputChunk{EOF: true, Err: err}
				return
			}
			stdout := proc.stdoutBuffer
			stderr := proc.stderrBuffer
			proc.stdoutBuffer = nil
			proc.stderrBuffer = nil
			proc.mu.Unlock()

			hasData := false
			if len(stdout) > 0 {
				hasData = true
				select {
				case ch <- platform.OutputChunk{Stream: platform.StreamStdout, Data: stdout}:
				case <-ctx.Done():
					return
				}
			}
			if len(stderr) > 0 {
				hasData = true
				select {
				case ch <- platform.OutputChunk{Stream: platform.StreamStderr, Data: stderr}:
				case <-ctx.Done():
					return
				}
			}

			if hasData {
				consecutiveEmpty = 0
			} else {
				consecutiveEmpty++
				if consecutiveEmpty >= maxConsecutiveEmpty {
					ch <- platform.OutputChunk{EOF: true}
					return
				}
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(pollInterval):
			}
		}
	}()
	return ch, nil
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

func (p *Platform) getProcess(execID string) *dockerProcess {
	p.mu.RLock()
	proc := p.processes[execID]
	p.mu.RUnlock()
	return proc
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

func demuxDockerStream(data []byte) ([]byte, []byte, []byte, error) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	for {
		if len(data) < 8 {
			break
		}
		streamID := data[0]
		size := binary.BigEndian.Uint32(data[4:8])
		if len(data) < 8+int(size) {
			break
		}
		payload := data[8 : 8+int(size)]
		switch streamID {
		case 1:
			stdout.Write(payload)
		case 2:
			stderr.Write(payload)
		}
		data = data[8+int(size):]
	}

	return stdout.Bytes(), stderr.Bytes(), data, nil
}

func isDockerMuxed(data []byte) bool {
	if len(data) < 8 {
		return false
	}
	if data[1] != 0 || data[2] != 0 || data[3] != 0 {
		return false
	}
	size := binary.BigEndian.Uint32(data[4:8])
	return int(size) <= len(data)-8
}

func isTimeout(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
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
