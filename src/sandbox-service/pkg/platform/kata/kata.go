package kata

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/api/pb"
	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/platform"
	agentgrpc "github.com/kata-containers/kata-containers/src/runtime/virtcontainers/pkg/agent/protocols/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	agentReadTimeout     = 2 * time.Second
	sandboxPollInitDelay = 100 * time.Millisecond
	sandboxPollMaxDelay  = 2 * time.Second
	sandboxReadyTimeout  = 30 * time.Second
	backoffMultiplier    = 1.5
	jitterFraction       = 0.2
)

// cachedSandbox holds cached sandbox info with expiry.
type cachedSandbox struct {
	sandbox   *platform.Sandbox
	expiresAt time.Time
}

const sandboxCacheTTL = 30 * time.Second

// Platform implements the sandbox platform interface using the sandbox-agent gRPC API.
type Platform struct {
	conn              *grpc.ClientConn
	client            pb.SandboxAgentClient
	mu                sync.RWMutex
	processContainers map[string]string            // execID -> containerID
	sandboxCache      map[string]*cachedSandbox    // sessionID -> cached sandbox
}

// Config holds configuration for the Kata platform.
type Config struct {
	SandboxAgentAddr string
}

// New creates a new Kata platform.
func New(cfg Config) (*Platform, error) {
	if strings.TrimSpace(cfg.SandboxAgentAddr) == "" {
		return nil, errors.New("sandbox agent address is required")
	}

	// Remove http:// prefix if present and use gRPC port
	addr := cfg.SandboxAgentAddr
	addr = strings.TrimPrefix(addr, "http://")
	addr = strings.TrimPrefix(addr, "https://")

	// Replace HTTP port 8080 with gRPC port 9090 if needed
	if strings.HasSuffix(addr, ":8080") {
		addr = strings.TrimSuffix(addr, ":8080") + ":9090"
	} else if !strings.Contains(addr, ":") {
		addr = addr + ":9090"
	}

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial sandbox-agent: %w", err)
	}

	return &Platform{
		conn:              conn,
		client:            pb.NewSandboxAgentClient(conn),
		processContainers: make(map[string]string),
		sandboxCache:      make(map[string]*cachedSandbox),
	}, nil
}

// Close closes the gRPC connection.
func (p *Platform) Close() error {
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}

// CreateSandbox creates a new sandbox via the agent.
func (p *Platform) CreateSandbox(ctx context.Context, req platform.CreateSandboxRequest) (*platform.Sandbox, error) {
	resp, err := p.client.CreateSandbox(ctx, &pb.CreateSandboxRequest{
		SessionId: req.SessionID,
		Image:     req.Image,
		Command:   req.Command,
		Env:       req.Env,
		Labels:    req.Labels,
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return nil, platform.ErrAlreadyExists
		}
		return nil, fmt.Errorf("create sandbox: %w", err)
	}

	return protoToSandbox(resp), nil
}

// GetSandbox retrieves sandbox info from the agent.
func (p *Platform) GetSandbox(ctx context.Context, sessionID string) (*platform.Sandbox, error) {
	resp, err := p.client.GetSandbox(ctx, &pb.GetSandboxRequest{
		SessionId: sessionID,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			p.invalidateCache(sessionID)
			return nil, platform.ErrNotFound
		}
		return nil, fmt.Errorf("get sandbox: %w", err)
	}

	sandbox := protoToSandbox(resp)
	p.cacheUpdate(sessionID, sandbox)
	return sandbox, nil
}

// getCachedSandbox returns cached sandbox info if available and not expired.
func (p *Platform) getCachedSandbox(sessionID string) *platform.Sandbox {
	p.mu.RLock()
	cached := p.sandboxCache[sessionID]
	p.mu.RUnlock()

	if cached == nil || time.Now().After(cached.expiresAt) {
		return nil
	}
	return cached.sandbox
}

// cacheUpdate updates the cache with new sandbox info.
func (p *Platform) cacheUpdate(sessionID string, sandbox *platform.Sandbox) {
	if sandbox == nil {
		return
	}
	p.mu.Lock()
	p.sandboxCache[sessionID] = &cachedSandbox{
		sandbox:   sandbox,
		expiresAt: time.Now().Add(sandboxCacheTTL),
	}
	p.mu.Unlock()
}

// invalidateCache removes a session from the cache.
func (p *Platform) invalidateCache(sessionID string) {
	p.mu.Lock()
	delete(p.sandboxCache, sessionID)
	p.mu.Unlock()
}

// getSandboxIDCached returns the sandboxID for a session, using cache when possible.
func (p *Platform) getSandboxIDCached(ctx context.Context, sessionID string) (string, error) {
	if cached := p.getCachedSandbox(sessionID); cached != nil && cached.SandboxID != "" {
		return cached.SandboxID, nil
	}

	sandbox, err := p.GetSandbox(ctx, sessionID)
	if err != nil {
		return "", err
	}
	return sandbox.SandboxID, nil
}

// DeleteSandbox deletes a sandbox via the agent.
func (p *Platform) DeleteSandbox(ctx context.Context, sessionID string) error {
	_, err := p.client.DeleteSandbox(ctx, &pb.DeleteSandboxRequest{
		SessionId: sessionID,
	})
	p.invalidateCache(sessionID)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return platform.ErrNotFound
		}
		return fmt.Errorf("delete sandbox: %w", err)
	}
	return nil
}

// ListSandboxes lists all sandboxes from the agent.
func (p *Platform) ListSandboxes(ctx context.Context) ([]*platform.Sandbox, error) {
	resp, err := p.client.ListSandboxes(ctx, &pb.ListSandboxesRequest{})
	if err != nil {
		return nil, fmt.Errorf("list sandboxes: %w", err)
	}

	result := make([]*platform.Sandbox, 0, len(resp.GetSandboxes()))
	for _, info := range resp.GetSandboxes() {
		result = append(result, protoToSandbox(info))
	}
	return result, nil
}

// Exec executes a command in a sandbox.
func (p *Platform) Exec(ctx context.Context, req platform.ExecRequest) (*platform.ExecResult, error) {
	sandbox, err := p.waitForSandboxReady(ctx, req.SessionID, req.ContainerName)
	if err != nil {
		return nil, err
	}

	containerID := getContainerID(sandbox, req.ContainerName)

	var timeoutMs int64
	if req.Timeout > 0 {
		timeoutMs = req.Timeout.Milliseconds()
	}

	resp, err := p.client.Exec(ctx, &pb.ExecRequest{
		VmId:        sandbox.SandboxID,
		ContainerId: containerID,
		Args:        req.Command,
		Env:         mapToEnv(req.Env),
		Cwd:         req.WorkingDir,
		TimeoutMs:   timeoutMs,
	})
	if err != nil {
		return nil, fmt.Errorf("exec: %w", err)
	}

	// Update activity
	p.updateActivity(ctx, req.SessionID)

	return &platform.ExecResult{
		ExitCode: int(resp.GetExitCode()),
		Stdout:   resp.GetStdout(),
		Stderr:   resp.GetStderr(),
	}, nil
}

// StartProcess starts a long-running process in a sandbox.
func (p *Platform) StartProcess(ctx context.Context, req platform.StartProcessRequest) (*platform.Process, error) {
	sandbox, err := p.waitForSandboxReady(ctx, req.SessionID, req.ContainerName)
	if err != nil {
		return nil, err
	}

	containerID := getContainerID(sandbox, req.ContainerName)

	_, err = p.client.ExecProcess(ctx, &pb.ExecProcessRequest{
		VmId: sandbox.SandboxID,
		Request: &agentgrpc.ExecProcessRequest{
			ContainerId: containerID,
			ExecId:      req.ExecID,
			Process: &agentgrpc.Process{
				Terminal: req.Terminal,
				Args:     req.Command,
				Env:      req.Env,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("exec process: %w", err)
	}

	p.mu.Lock()
	p.processContainers[req.ExecID] = containerID
	p.mu.Unlock()

	return &platform.Process{
		ExecID:    req.ExecID,
		StartedAt: time.Now().UTC(),
		Alive:     true,
	}, nil
}

// WriteToProcess writes data to a process stdin.
func (p *Platform) WriteToProcess(ctx context.Context, sessionID, execID string, data []byte) error {
	containerID, err := p.getProcessContainer(execID)
	if err != nil {
		return err
	}

	sandboxID, err := p.getSandboxIDCached(ctx, sessionID)
	if err != nil {
		return err
	}

	_, err = p.client.WriteStdin(ctx, &pb.WriteStdinRequest{
		VmId: sandboxID,
		Request: &agentgrpc.WriteStreamRequest{
			ContainerId: containerID,
			ExecId:      execID,
			Data:        data,
		},
	})
	if err != nil {
		return fmt.Errorf("write stdin: %w", err)
	}

	p.updateActivity(ctx, sessionID)
	return nil
}

// ReadFromProcess reads output from a process.
func (p *Platform) ReadFromProcess(ctx context.Context, sessionID, execID string) (*platform.ProcessOutput, error) {
	containerID, err := p.getProcessContainer(execID)
	if err != nil {
		return nil, err
	}

	sandboxID, err := p.getSandboxIDCached(ctx, sessionID)
	if err != nil {
		return nil, err
	}

	readCtx, cancel := context.WithTimeout(ctx, agentReadTimeout)
	defer cancel()

	var stdout, stderr []byte

	stdoutResp, err := p.client.ReadStdout(readCtx, &pb.ReadStdoutRequest{
		VmId: sandboxID,
		Request: &agentgrpc.ReadStreamRequest{
			ContainerId: containerID,
			ExecId:      execID,
			Len:         4096,
		},
	})
	if err != nil && !isTimeout(err) {
		return nil, fmt.Errorf("read stdout: %w", err)
	}
	if stdoutResp != nil {
		stdout = stdoutResp.GetData()
	}

	stderrResp, err := p.client.ReadStderr(readCtx, &pb.ReadStderrRequest{
		VmId: sandboxID,
		Request: &agentgrpc.ReadStreamRequest{
			ContainerId: containerID,
			ExecId:      execID,
			Len:         4096,
		},
	})
	if err != nil && !isTimeout(err) {
		return nil, fmt.Errorf("read stderr: %w", err)
	}
	if stderrResp != nil {
		stderr = stderrResp.GetData()
	}

	return &platform.ProcessOutput{
		Stdout: stdout,
		Stderr: stderr,
	}, nil
}

// KillProcess kills a process.
func (p *Platform) KillProcess(ctx context.Context, sessionID, execID string) error {
	containerID, err := p.getProcessContainer(execID)
	if err != nil {
		return err
	}

	sandboxID, err := p.getSandboxIDCached(ctx, sessionID)
	if err != nil {
		return err
	}

	// Send SIGKILL
	_, err = p.client.SignalProcess(ctx, &pb.SignalProcessRequest{
		VmId: sandboxID,
		Request: &agentgrpc.SignalProcessRequest{
			ContainerId: containerID,
			ExecId:      execID,
			Signal:      9,
		},
	})
	if err != nil {
		return fmt.Errorf("signal process: %w", err)
	}

	// Wait for exit
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, _ = p.client.WaitProcess(waitCtx, &pb.WaitProcessRequest{
		VmId: sandboxID,
		Request: &agentgrpc.WaitProcessRequest{
			ContainerId: containerID,
			ExecId:      execID,
		},
	})

	p.mu.Lock()
	delete(p.processContainers, execID)
	p.mu.Unlock()

	return nil
}

// IsProcessAlive checks if a process is still running.
func (p *Platform) IsProcessAlive(ctx context.Context, sessionID, execID string) (bool, error) {
	containerID, err := p.getProcessContainer(execID)
	if err != nil {
		return false, err
	}

	sandboxID, err := p.getSandboxIDCached(ctx, sessionID)
	if err != nil {
		return false, err
	}

	// Send signal 0 to check if alive
	_, err = p.client.SignalProcess(ctx, &pb.SignalProcessRequest{
		VmId: sandboxID,
		Request: &agentgrpc.SignalProcessRequest{
			ContainerId: containerID,
			ExecId:      execID,
			Signal:      0,
		},
	})
	if err != nil {
		return false, nil // Process is dead
	}
	return true, nil
}

// ResizeProcess resizes a process TTY.
func (p *Platform) ResizeProcess(ctx context.Context, sessionID, execID string, rows, columns uint32) error {
	containerID, err := p.getProcessContainer(execID)
	if err != nil {
		return err
	}

	sandboxID, err := p.getSandboxIDCached(ctx, sessionID)
	if err != nil {
		return err
	}

	_, err = p.client.TtyWinResize(ctx, &pb.TtyWinResizeRequest{
		VmId: sandboxID,
		Request: &agentgrpc.TtyWinResizeRequest{
			ContainerId: containerID,
			ExecId:      execID,
			Row:         rows,
			Column:      columns,
		},
	})
	if err != nil {
		return fmt.Errorf("tty resize: %w", err)
	}
	return nil
}

// waitForSandboxReady waits for a sandbox to be ready using exponential backoff with jitter.
func (p *Platform) waitForSandboxReady(ctx context.Context, sessionID, containerName string) (*platform.Sandbox, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, sandboxReadyTimeout)
	defer cancel()

	delay := sandboxPollInitDelay

	for {
		sandbox, err := p.GetSandbox(ctx, sessionID)
		if err != nil && !errors.Is(err, platform.ErrNotFound) {
			return nil, err
		}

		if sandbox != nil && sandbox.Status == platform.StatusRunning && sandbox.SandboxID != "" {
			if cid := getContainerID(sandbox, containerName); cid != "" {
				slog.Debug("sandbox ready", "session_id", sessionID, "elapsed", time.Since(start))
				return sandbox, nil
			}
		}

		select {
		case <-ctx.Done():
			return nil, platform.ErrNotReady
		case <-time.After(addJitter(delay)):
		}

		delay = time.Duration(float64(delay) * backoffMultiplier)
		if delay > sandboxPollMaxDelay {
			delay = sandboxPollMaxDelay
		}
	}
}

func addJitter(d time.Duration) time.Duration {
	jitter := time.Duration(float64(d) * jitterFraction * (rand.Float64()*2 - 1))
	return d + jitter
}

func (p *Platform) updateActivity(ctx context.Context, sessionID string) {
	_, err := p.client.UpdateSandboxActivity(ctx, &pb.UpdateSandboxActivityRequest{
		SessionId: sessionID,
	})
	if err != nil {
		slog.Warn("update activity failed", "session_id", sessionID, "error", err)
	}
}

func (p *Platform) getProcessContainer(execID string) (string, error) {
	p.mu.RLock()
	containerID, ok := p.processContainers[execID]
	p.mu.RUnlock()
	if !ok || strings.TrimSpace(containerID) == "" {
		return "", platform.ErrNotFound
	}
	return containerID, nil
}

// Helper functions

func protoToSandbox(info *pb.SandboxInfo) *platform.Sandbox {
	if info == nil {
		return nil
	}

	// Get the first container ID as the default
	var containerID string
	if len(info.GetContainers()) > 0 {
		containerID = info.GetContainers()[0].GetContainerId()
	}

	return &platform.Sandbox{
		SessionID:   info.GetSessionId(),
		SandboxID:   info.GetSandboxId(),
		ContainerID: containerID,
		Status:      platform.SandboxStatus(info.GetStatus()),
		Host:        info.GetNode(),
		CreatedAt:   time.Unix(info.GetCreatedAtUnix(), 0),
		LastUsedAt:  time.Unix(info.GetLastUsedAtUnix(), 0),
		Labels:      info.GetLabels(),
	}
}

func getContainerID(sandbox *platform.Sandbox, name string) string {
	if sandbox == nil {
		return ""
	}
	return sandbox.ContainerID
}

func mapToEnv(m map[string]string) []string {
	if len(m) == 0 {
		return nil
	}
	result := make([]string, 0, len(m))
	for k, v := range m {
		result = append(result, fmt.Sprintf("%s=%s", k, v))
	}
	return result
}

func isTimeout(err error) bool {
	if err == nil {
		return false
	}
	if status.Code(err) == codes.DeadlineExceeded {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return false
}
