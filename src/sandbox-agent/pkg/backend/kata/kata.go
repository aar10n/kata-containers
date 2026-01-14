package kata

import (
	"context"
	"time"

	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/agent"
	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/backend"
	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/shim_mgmt"
	agentgrpc "github.com/kata-containers/kata-containers/src/runtime/virtcontainers/pkg/agent/protocols/grpc"
)

// SandboxIDResolver resolves the sandbox ID (VM ID) from a container ID.
// This is needed because the kata-agent requires the sandbox ID to find the
// shim socket, but the ExecProcessRequest requires the container ID.
type SandboxIDResolver func(containerID string) (sandboxID string, ok bool)

// Backend implements ExecutionBackend for Kata VMs using the kata-agent.
type Backend struct {
	agentClient       *agent.Client
	shimClient        *shim_mgmt.Client
	sandboxIDResolver SandboxIDResolver
}

// Config holds configuration for the Kata backend.
type Config struct {
	AgentTimeout    time.Duration
	ReadChunkSize   int
	MaxOutputSize   int
	ShimDialTimeout time.Duration
}

// New creates a new Kata backend.
func New(cfg Config) *Backend {
	agentClient := agent.New(agent.Config{
		Timeout:       cfg.AgentTimeout,
		ReadChunkSize: cfg.ReadChunkSize,
		MaxOutputSize: cfg.MaxOutputSize,
	})

	shimClient := shim_mgmt.New(shim_mgmt.Config{
		DialTimeout: cfg.ShimDialTimeout,
	})

	return &Backend{
		agentClient: agentClient,
		shimClient:  shimClient,
	}
}

// NewFromClients creates a Kata backend from existing clients.
// This is useful when you want to reuse existing agent/shim clients.
func NewFromClients(agentClient *agent.Client, shimClient *shim_mgmt.Client) *Backend {
	return &Backend{
		agentClient: agentClient,
		shimClient:  shimClient,
	}
}

// AgentClient returns the underlying agent client for direct access.
// This is useful for operations not covered by the ExecutionBackend interface.
func (b *Backend) AgentClient() *agent.Client {
	return b.agentClient
}

// ShimClient returns the underlying shim management client.
func (b *Backend) ShimClient() *shim_mgmt.Client {
	return b.shimClient
}

// SetSandboxIDResolver sets the function used to resolve sandbox IDs from container IDs.
// This must be called before using the backend for command execution.
func (b *Backend) SetSandboxIDResolver(resolver SandboxIDResolver) {
	b.sandboxIDResolver = resolver
}

// resolveSandboxID gets the sandbox ID for a container ID using the resolver.
// If no resolver is set or the container is not found, falls back to using
// the container ID as the sandbox ID (legacy behavior).
func (b *Backend) resolveSandboxID(containerID string) string {
	if b.sandboxIDResolver == nil {
		return containerID
	}
	if sandboxID, ok := b.sandboxIDResolver(containerID); ok {
		return sandboxID
	}
	return containerID
}

// Exec executes a command in a Kata VM container.
func (b *Backend) Exec(ctx context.Context, containerID string, cmd []string, env []string, cwd string, timeout time.Duration) (*backend.ExecResult, error) {
	// sandboxID is needed to find the shim socket and connect to kata-agent
	// containerID is needed in the ExecProcessRequest to execute in the right container
	sandboxID := b.resolveSandboxID(containerID)
	result, err := b.agentClient.Exec(ctx, sandboxID, containerID, cmd, env, cwd, timeout)
	if err != nil {
		return nil, err
	}

	return &backend.ExecResult{
		Stdout:   result.Stdout,
		Stderr:   result.Stderr,
		ExitCode: result.ExitCode,
	}, nil
}

// StartProcess starts a long-running process in a Kata VM container.
func (b *Backend) StartProcess(ctx context.Context, containerID string, cmd []string, env []string, cwd string, tty bool) (*backend.Process, error) {
	sandboxID := b.resolveSandboxID(containerID)
	execID := generateExecID()

	if cwd == "" {
		cwd = "/"
	}

	req := &agentgrpc.ExecProcessRequest{
		ContainerId: containerID,
		ExecId:      execID,
		Process: &agentgrpc.Process{
			Terminal: tty,
			Args:     cmd,
			Env:      env,
			Cwd:      cwd,
		},
	}

	if err := b.agentClient.ExecProcess(ctx, sandboxID, req); err != nil {
		return nil, err
	}

	return &backend.Process{
		ID:          execID,
		ContainerID: containerID,
	}, nil
}

// WriteToProcess writes data to a process's stdin.
func (b *Backend) WriteToProcess(ctx context.Context, containerID, processID string, data []byte) error {
	sandboxID := b.resolveSandboxID(containerID)
	_, err := b.agentClient.WriteStdin(ctx, sandboxID, &agentgrpc.WriteStreamRequest{
		ContainerId: containerID,
		ExecId:      processID,
		Data:        data,
	})
	return err
}

// ReadStdout reads available stdout data from a process.
func (b *Backend) ReadStdout(ctx context.Context, containerID, processID string, maxBytes int) ([]byte, error) {
	sandboxID := b.resolveSandboxID(containerID)
	resp, err := b.agentClient.ReadStdout(ctx, sandboxID, &agentgrpc.ReadStreamRequest{
		ContainerId: containerID,
		ExecId:      processID,
		Len:         uint32(maxBytes),
	})
	if err != nil {
		return nil, err
	}
	return resp.Data, nil
}

// ReadStderr reads available stderr data from a process.
func (b *Backend) ReadStderr(ctx context.Context, containerID, processID string, maxBytes int) ([]byte, error) {
	sandboxID := b.resolveSandboxID(containerID)
	resp, err := b.agentClient.ReadStderr(ctx, sandboxID, &agentgrpc.ReadStreamRequest{
		ContainerId: containerID,
		ExecId:      processID,
		Len:         uint32(maxBytes),
	})
	if err != nil {
		return nil, err
	}
	return resp.Data, nil
}

// StreamStdout returns a channel that streams stdout data.
func (b *Backend) StreamStdout(ctx context.Context, containerID, processID string) (<-chan backend.StreamChunk, error) {
	sandboxID := b.resolveSandboxID(containerID)
	agentCh, err := b.agentClient.StreamStdout(ctx, sandboxID, &agentgrpc.StreamRequest{
		ContainerId: containerID,
		ExecId:      processID,
	})
	if err != nil {
		return nil, err
	}

	outCh := make(chan backend.StreamChunk, 16)
	go func() {
		defer close(outCh)
		for chunk := range agentCh {
			select {
			case outCh <- backend.StreamChunk{Data: chunk.Data, EOF: chunk.EOF}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return outCh, nil
}

// StreamStderr returns a channel that streams stderr data.
func (b *Backend) StreamStderr(ctx context.Context, containerID, processID string) (<-chan backend.StreamChunk, error) {
	sandboxID := b.resolveSandboxID(containerID)
	agentCh, err := b.agentClient.StreamStderr(ctx, sandboxID, &agentgrpc.StreamRequest{
		ContainerId: containerID,
		ExecId:      processID,
	})
	if err != nil {
		return nil, err
	}

	outCh := make(chan backend.StreamChunk, 16)
	go func() {
		defer close(outCh)
		for chunk := range agentCh {
			select {
			case outCh <- backend.StreamChunk{Data: chunk.Data, EOF: chunk.EOF}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return outCh, nil
}

// StreamOutput returns a channel that streams both stdout and stderr data.
func (b *Backend) StreamOutput(ctx context.Context, containerID, processID string) (<-chan backend.OutputChunk, error) {
	sandboxID := b.resolveSandboxID(containerID)

	// Start both streams
	stdoutCh, err := b.agentClient.StreamStdout(ctx, sandboxID, &agentgrpc.StreamRequest{
		ContainerId: containerID,
		ExecId:      processID,
	})
	if err != nil {
		return nil, err
	}

	stderrCh, err := b.agentClient.StreamStderr(ctx, sandboxID, &agentgrpc.StreamRequest{
		ContainerId: containerID,
		ExecId:      processID,
	})
	if err != nil {
		return nil, err
	}

	// Merge both streams
	outCh := make(chan backend.OutputChunk, 16)
	go func() {
		defer close(outCh)
		stdoutDone := false
		stderrDone := false

		for !stdoutDone || !stderrDone {
			select {
			case chunk, ok := <-stdoutCh:
				if !ok {
					stdoutDone = true
					continue
				}
				if chunk.EOF {
					stdoutDone = true
				}
				if len(chunk.Data) > 0 {
					select {
					case outCh <- backend.OutputChunk{Stream: backend.StreamStdout, Data: chunk.Data}:
					case <-ctx.Done():
						return
					}
				}
			case chunk, ok := <-stderrCh:
				if !ok {
					stderrDone = true
					continue
				}
				if chunk.EOF {
					stderrDone = true
				}
				if len(chunk.Data) > 0 {
					select {
					case outCh <- backend.OutputChunk{Stream: backend.StreamStderr, Data: chunk.Data}:
					case <-ctx.Done():
						return
					}
				}
			case <-ctx.Done():
				return
			}
		}
		// Send final EOF
		outCh <- backend.OutputChunk{EOF: true}
	}()

	return outCh, nil
}

// CloseStdin closes the stdin of a process.
func (b *Backend) CloseStdin(ctx context.Context, containerID, processID string) error {
	sandboxID := b.resolveSandboxID(containerID)
	return b.agentClient.CloseStdin(ctx, sandboxID, &agentgrpc.CloseStdinRequest{
		ContainerId: containerID,
		ExecId:      processID,
	})
}

// KillProcess terminates a process with the given signal.
func (b *Backend) KillProcess(ctx context.Context, containerID, processID string, signal int) error {
	sandboxID := b.resolveSandboxID(containerID)
	return b.agentClient.SignalProcess(ctx, sandboxID, &agentgrpc.SignalProcessRequest{
		ContainerId: containerID,
		ExecId:      processID,
		Signal:      uint32(signal),
	})
}

// WaitProcess waits for a process to exit and returns the exit code.
func (b *Backend) WaitProcess(ctx context.Context, containerID, processID string) (int32, error) {
	sandboxID := b.resolveSandboxID(containerID)
	resp, err := b.agentClient.WaitProcess(ctx, sandboxID, &agentgrpc.WaitProcessRequest{
		ContainerId: containerID,
		ExecId:      processID,
	})
	if err != nil {
		return -1, err
	}
	return resp.Status, nil
}

// ResizeTerminal resizes the terminal for a TTY process.
func (b *Backend) ResizeTerminal(ctx context.Context, containerID, processID string, rows, cols uint32) error {
	sandboxID := b.resolveSandboxID(containerID)
	return b.agentClient.TtyWinResize(ctx, sandboxID, &agentgrpc.TtyWinResizeRequest{
		ContainerId: containerID,
		ExecId:      processID,
		Row:         rows,
		Column:      cols,
	})
}

// SupportsStateOps returns true - Kata VMs support state save/restore.
func (b *Backend) SupportsStateOps() bool {
	return true
}

// SaveState saves the VM state to the given path.
func (b *Backend) SaveState(ctx context.Context, sandboxID string) error {
	// The actual state path is typically provided by the caller
	// For now, we'll use a default path based on sandbox ID
	statePath := "/var/lib/kata/snapshots/" + sandboxID
	return b.shimClient.SaveVMState(ctx, sandboxID, statePath)
}

// RestoreState restores the VM from a saved state.
func (b *Backend) RestoreState(ctx context.Context, sandboxID, snapshotID string) error {
	statePath := "/var/lib/kata/snapshots/" + snapshotID
	return b.shimClient.RestoreVMState(ctx, sandboxID, statePath)
}

// Close releases resources held by the backend.
func (b *Backend) Close() error {
	// The agent client manages its own cleanup via idle connection cleanup
	return nil
}

// generateExecID generates a unique exec ID.
func generateExecID() string {
	return "exec-" + generateRandomString(8)
}

// generateRandomString generates a random alphanumeric string.
func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[time.Now().UnixNano()%int64(len(charset))]
		time.Sleep(time.Nanosecond)
	}
	return string(b)
}

// Ensure Backend implements ExecutionBackend
var _ backend.ExecutionBackend = (*Backend)(nil)
