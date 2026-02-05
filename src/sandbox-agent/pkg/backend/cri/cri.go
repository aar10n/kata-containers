package cri

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/backend"
	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/cri"
	"github.com/google/uuid"
)

// Backend implements ExecutionBackend using the CRI API.
// This is used for regular Kubernetes pods (non-Kata).
type Backend struct {
	client *cri.Client

	// processStore tracks active streaming processes
	mu        sync.RWMutex
	processes map[string]*processState
}

// processState tracks the state of a streaming process.
type processState struct {
	containerID string
	execID      string
	streamURL   string
	tty         bool
	started     time.Time
	conn        *streamConn
}

// Config holds configuration for the CRI backend.
type Config struct {
	// Socket is the path to the CRI socket.
	Socket string

	// Timeout for CRI operations.
	Timeout time.Duration
}

// New creates a new CRI backend.
func New(cfg Config) (*Backend, error) {
	client, err := cri.New(cri.Config{
		Socket:  cfg.Socket,
		Timeout: cfg.Timeout,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create CRI client: %w", err)
	}

	return &Backend{
		client:    client,
		processes: make(map[string]*processState),
	}, nil
}

// NewFromClient creates a CRI backend from an existing client.
func NewFromClient(client *cri.Client) *Backend {
	return &Backend{
		client:    client,
		processes: make(map[string]*processState),
	}
}

// CRIClient returns the underlying CRI client for direct access.
func (b *Backend) CRIClient() *cri.Client {
	return b.client
}

// Exec executes a command synchronously in a container.
func (b *Backend) Exec(ctx context.Context, containerID string, cmd []string, env []string, cwd string, timeout time.Duration) (*backend.ExecResult, error) {
	// CRI ExecSync doesn't support env or cwd directly, so we wrap the command if needed
	execCmd := cmd
	if len(env) > 0 || cwd != "" {
		// Wrap command with env and cd
		shellCmd := buildShellCommand(cmd, env, cwd)
		execCmd = []string{"sh", "-c", shellCmd}
	}

	resp, err := b.client.ExecSync(ctx, containerID, execCmd, timeout)
	if err != nil {
		return nil, err
	}

	return &backend.ExecResult{
		Stdout:   string(resp.Stdout),
		Stderr:   string(resp.Stderr),
		ExitCode: resp.ExitCode,
	}, nil
}

// StartProcess starts a long-running process for interactive I/O.
// This uses the CRI streaming Exec API.
func (b *Backend) StartProcess(ctx context.Context, containerID string, cmd []string, env []string, cwd string, tty bool) (*backend.Process, error) {
	// Build the command with env/cwd if needed
	execCmd := cmd
	if len(env) > 0 || cwd != "" {
		shellCmd := buildShellCommand(cmd, env, cwd)
		execCmd = []string{"sh", "-c", shellCmd}
	}

	// Get streaming URL from CRI
	streamURL, err := b.client.Exec(ctx, containerID, execCmd, true, tty)
	if err != nil {
		return nil, fmt.Errorf("failed to start streaming exec: %w", err)
	}

	// Connect to the streaming endpoint
	conn, err := newStreamConn(ctx, streamURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to stream: %w", err)
	}

	execID := uuid.NewString()

	// Store process state
	b.mu.Lock()
	b.processes[execID] = &processState{
		containerID: containerID,
		execID:      execID,
		streamURL:   streamURL,
		tty:         tty,
		started:     time.Now(),
		conn:        conn,
	}
	b.mu.Unlock()

	return &backend.Process{
		ID:          execID,
		ContainerID: containerID,
	}, nil
}

// getProcess retrieves a process state by ID.
func (b *Backend) getProcess(processID string) (*processState, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	state, ok := b.processes[processID]
	if !ok {
		return nil, fmt.Errorf("process %s not found", processID)
	}
	return state, nil
}

// WriteToProcess writes data to a process's stdin.
func (b *Backend) WriteToProcess(ctx context.Context, containerID, processID string, data []byte) error {
	state, err := b.getProcess(processID)
	if err != nil {
		return err
	}

	if state.conn == nil {
		return backend.ErrNotSupported
	}

	return state.conn.Write(data)
}

// ReadStdout reads available stdout data from a process.
func (b *Backend) ReadStdout(ctx context.Context, containerID, processID string, maxBytes int) ([]byte, error) {
	state, err := b.getProcess(processID)
	if err != nil {
		return nil, err
	}

	if state.conn == nil {
		return nil, backend.ErrNotSupported
	}

	return state.conn.ReadStdout(maxBytes), nil
}

// ReadStderr reads available stderr data from a process.
func (b *Backend) ReadStderr(ctx context.Context, containerID, processID string, maxBytes int) ([]byte, error) {
	state, err := b.getProcess(processID)
	if err != nil {
		return nil, err
	}

	if state.conn == nil {
		return nil, backend.ErrNotSupported
	}

	return state.conn.ReadStderr(maxBytes), nil
}

// StreamStdout returns a channel that streams stdout data.
func (b *Backend) StreamStdout(ctx context.Context, containerID, processID string) (<-chan backend.StreamChunk, error) {
	state, err := b.getProcess(processID)
	if err != nil {
		return nil, err
	}

	if state.conn == nil {
		return nil, backend.ErrNotSupported
	}

	ch := make(chan backend.StreamChunk)
	go func() {
		defer close(ch)

		for {
			select {
			case <-ctx.Done():
				return
			case <-state.conn.Done():
				// Connection closed, send any remaining data
				if data := state.conn.ReadStdout(0); len(data) > 0 {
					ch <- backend.StreamChunk{Data: data}
				}
				ch <- backend.StreamChunk{EOF: true}
				return
			case <-state.conn.DataReady():
				// New data available, read all buffered stdout
				if data := state.conn.ReadStdout(0); len(data) > 0 {
					ch <- backend.StreamChunk{Data: data}
				}
			}
		}
	}()

	return ch, nil
}

// StreamStderr returns a channel that streams stderr data.
func (b *Backend) StreamStderr(ctx context.Context, containerID, processID string) (<-chan backend.StreamChunk, error) {
	state, err := b.getProcess(processID)
	if err != nil {
		return nil, err
	}

	if state.conn == nil {
		return nil, backend.ErrNotSupported
	}

	ch := make(chan backend.StreamChunk)
	go func() {
		defer close(ch)

		for {
			select {
			case <-ctx.Done():
				return
			case <-state.conn.Done():
				// Connection closed, send any remaining data
				if data := state.conn.ReadStderr(0); len(data) > 0 {
					ch <- backend.StreamChunk{Data: data}
				}
				ch <- backend.StreamChunk{EOF: true}
				return
			case <-state.conn.DataReady():
				// New data available, read all buffered stderr
				if data := state.conn.ReadStderr(0); len(data) > 0 {
					ch <- backend.StreamChunk{Data: data}
				}
			}
		}
	}()

	return ch, nil
}

// StreamOutput returns a channel that streams both stdout and stderr data.
func (b *Backend) StreamOutput(ctx context.Context, containerID, processID string) (<-chan backend.OutputChunk, error) {
	state, err := b.getProcess(processID)
	if err != nil {
		return nil, err
	}

	if state.conn == nil {
		return nil, backend.ErrNotSupported
	}

	slog.Info("CRI StreamOutput: starting", "process_id", processID)
	ch := make(chan backend.OutputChunk)
	go func() {
		defer close(ch)
		startTime := time.Now()
		iterations := 0

		for {
			iterations++
			select {
			case <-ctx.Done():
				slog.Info("CRI StreamOutput: context done", "process_id", processID, "iterations", iterations, "elapsed", time.Since(startTime))
				return
			case <-state.conn.Done():
				// Connection closed, send any remaining data
				slog.Info("CRI StreamOutput: connection done", "process_id", processID, "iterations", iterations, "elapsed", time.Since(startTime))
				if data := state.conn.ReadStdout(0); len(data) > 0 {
					ch <- backend.OutputChunk{Stream: backend.StreamStdout, Data: data}
				}
				if data := state.conn.ReadStderr(0); len(data) > 0 {
					ch <- backend.OutputChunk{Stream: backend.StreamStderr, Data: data}
				}
				ch <- backend.OutputChunk{EOF: true}
				return
			case <-state.conn.DataReady():
				// New data available, read both buffers
				elapsed := time.Since(startTime)
				stdoutData := state.conn.ReadStdout(0)
				stderrData := state.conn.ReadStderr(0)
				slog.Info("CRI StreamOutput: data ready", "process_id", processID, "iterations", iterations, "elapsed", elapsed, "stdout_bytes", len(stdoutData), "stderr_bytes", len(stderrData))
				if len(stdoutData) > 0 {
					ch <- backend.OutputChunk{Stream: backend.StreamStdout, Data: stdoutData}
				}
				if len(stderrData) > 0 {
					ch <- backend.OutputChunk{Stream: backend.StreamStderr, Data: stderrData}
				}
			}
		}
	}()

	return ch, nil
}

// CloseStdin closes the stdin of a process.
func (b *Backend) CloseStdin(ctx context.Context, containerID, processID string) error {
	state, err := b.getProcess(processID)
	if err != nil {
		return err
	}

	if state.conn == nil {
		return backend.ErrNotSupported
	}

	return state.conn.CloseStdin()
}

// KillProcess terminates a process.
// Signal 0 is used to check if the process exists without killing it.
func (b *Backend) KillProcess(ctx context.Context, containerID, processID string, signal int) error {
	b.mu.Lock()
	state, ok := b.processes[processID]
	b.mu.Unlock()

	if !ok {
		return fmt.Errorf("process %s not found", processID)
	}

	// Signal 0 just checks if process exists
	if signal == 0 {
		// Check if connection is still alive
		select {
		case <-state.conn.Done():
			return fmt.Errorf("process %s has exited", processID)
		default:
			return nil // Process is alive
		}
	}

	// Actually kill the process
	b.mu.Lock()
	delete(b.processes, processID)
	b.mu.Unlock()

	if state.conn != nil {
		state.conn.Close()
	}

	return nil
}

// WaitProcess waits for a process to exit and returns the exit code.
func (b *Backend) WaitProcess(ctx context.Context, containerID, processID string) (int32, error) {
	state, err := b.getProcess(processID)
	if err != nil {
		return -1, err
	}

	if state.conn == nil {
		return -1, backend.ErrNotSupported
	}

	// Wait for connection to close
	select {
	case <-ctx.Done():
		return -1, ctx.Err()
	case <-state.conn.Done():
		// CRI streaming doesn't give us exit codes directly
		// We'd need to query the container status
		return 0, nil
	}
}

// ResizeTerminal resizes the terminal for a TTY process.
func (b *Backend) ResizeTerminal(ctx context.Context, containerID, processID string, rows, cols uint32) error {
	state, err := b.getProcess(processID)
	if err != nil {
		return err
	}

	if state.conn == nil {
		return backend.ErrNotSupported
	}

	return state.conn.Resize(cols, rows)
}

// SupportsStateOps returns false - regular pods don't support VM state operations.
func (b *Backend) SupportsStateOps() bool {
	return false
}

// SaveState is not supported for CRI backend.
func (b *Backend) SaveState(ctx context.Context, sandboxID string) error {
	return backend.ErrNotSupported
}

// RestoreState is not supported for CRI backend.
func (b *Backend) RestoreState(ctx context.Context, sandboxID, snapshotID string) error {
	return backend.ErrNotSupported
}

// Close releases resources held by the backend.
func (b *Backend) Close() error {
	b.mu.Lock()
	for _, state := range b.processes {
		if state.conn != nil {
			state.conn.Close()
		}
	}
	b.processes = make(map[string]*processState)
	b.mu.Unlock()

	return b.client.Close()
}

// buildShellCommand builds a shell command string with environment variables and cwd.
func buildShellCommand(cmd []string, env []string, cwd string) string {
	var shellCmd string

	// Add environment variables
	for _, e := range env {
		shellCmd += fmt.Sprintf("export %s; ", e)
	}

	// Add cd if cwd is specified
	if cwd != "" {
		shellCmd += fmt.Sprintf("cd %s && ", shellQuote(cwd))
	}

	// Add the actual command
	for i, arg := range cmd {
		if i > 0 {
			shellCmd += " "
		}
		shellCmd += shellQuote(arg)
	}

	return shellCmd
}

// shellQuote quotes a string for shell use.
func shellQuote(s string) string {
	// Simple quoting - wrap in single quotes and escape existing single quotes
	return "'" + escapeShellArg(s) + "'"
}

// escapeShellArg escapes a string for use in a shell argument.
func escapeShellArg(s string) string {
	result := ""
	for _, c := range s {
		if c == '\'' {
			result += "'\\''"
		} else {
			result += string(c)
		}
	}
	return result
}

// Ensure Backend implements ExecutionBackend
var _ backend.ExecutionBackend = (*Backend)(nil)
