package kata

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/api/pb"
	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/platform"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	agentReadTimeout     = 100 * time.Millisecond // Short timeout for non-blocking reads
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

// nodeConn holds a gRPC connection to a specific sandbox-agent node.
type nodeConn struct {
	conn   *grpc.ClientConn
	client pb.SandboxAgentClient
}

// Platform implements the sandbox platform interface using the sandbox-agent gRPC API.
// It maintains a pool of connections to different sandbox-agent nodes and routes
// requests to the correct node based on session-to-node mapping.
type Platform struct {
	defaultAddr string // default sandbox-agent address (for discovery)

	mu                sync.RWMutex
	nodeConns         map[string]*nodeConn      // node address -> connection
	sessionNodes      map[string]string         // sessionID -> node address
	processContainers map[string]string         // processID -> containerID
	sandboxCache      map[string]*cachedSandbox // sessionID -> cached sandbox
}

// Config holds configuration for the Kata platform.
type Config struct {
	SandboxAgentAddr string
}

// redirectTarget extracts host and port from a redirect error.
func redirectTarget(err error) (string, int, bool) {
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		return "", 0, false
	}
	for _, detail := range st.Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if !ok || info.Reason != "REDIRECT" {
			continue
		}
		host := strings.TrimSpace(info.Metadata["host"])
		port, _ := strconv.Atoi(info.Metadata["port"])
		if host == "" {
			return "", 0, false
		}
		return host, port, true
	}
	return "", 0, false
}

// New creates a new Kata platform.
func New(cfg Config) (*Platform, error) {
	if strings.TrimSpace(cfg.SandboxAgentAddr) == "" {
		return nil, errors.New("sandbox agent address is required")
	}

	// Normalize the default address
	addr := normalizeAgentAddr(cfg.SandboxAgentAddr)

	return &Platform{
		defaultAddr:       addr,
		nodeConns:         make(map[string]*nodeConn),
		sessionNodes:      make(map[string]string),
		processContainers: make(map[string]string),
		sandboxCache:      make(map[string]*cachedSandbox),
	}, nil
}

// normalizeAgentAddr converts HTTP URLs to gRPC addresses.
func normalizeAgentAddr(addr string) string {
	addr = strings.TrimPrefix(addr, "http://")
	addr = strings.TrimPrefix(addr, "https://")

	// Replace HTTP port 8080 with gRPC port 9090 if needed
	if strings.HasSuffix(addr, ":8080") {
		addr = strings.TrimSuffix(addr, ":8080") + ":9090"
	} else if !strings.Contains(addr, ":") {
		addr = addr + ":9090"
	}
	return addr
}

// Close closes all gRPC connections.
func (p *Platform) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var lastErr error
	for addr, nc := range p.nodeConns {
		if err := nc.conn.Close(); err != nil {
			lastErr = err
			slog.Warn("failed to close connection", "addr", addr, "error", err)
		}
	}
	p.nodeConns = make(map[string]*nodeConn)
	return lastErr
}

// getClientForSession returns a gRPC client for the given session.
// If the session's node is known, returns a direct connection to that node.
// Otherwise, returns a connection to the default address.
func (p *Platform) getClientForSession(sessionID string) pb.SandboxAgentClient {
	p.mu.RLock()
	nodeAddr := p.sessionNodes[sessionID]
	p.mu.RUnlock()

	if nodeAddr == "" {
		nodeAddr = p.defaultAddr
	}
	return p.getClientForNode(nodeAddr)
}

// getClientForNode returns a gRPC client for the given node address.
// It maintains a connection pool, creating new connections as needed.
func (p *Platform) getClientForNode(addr string) pb.SandboxAgentClient {
	p.mu.RLock()
	nc := p.nodeConns[addr]
	p.mu.RUnlock()

	if nc != nil {
		return nc.client
	}

	// Need to create a new connection
	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check after acquiring write lock
	if nc = p.nodeConns[addr]; nc != nil {
		return nc.client
	}

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("failed to dial sandbox-agent", "addr", addr, "error", err)
		// Return a client anyway - the error will surface on first call
		return nil
	}

	nc = &nodeConn{
		conn:   conn,
		client: pb.NewSandboxAgentClient(conn),
	}
	p.nodeConns[addr] = nc
	slog.Debug("created new connection to sandbox-agent", "addr", addr)
	return nc.client
}

// updateSessionNode updates the session-to-node mapping.
func (p *Platform) updateSessionNode(sessionID, nodeAddr string) {
	p.mu.Lock()
	p.sessionNodes[sessionID] = nodeAddr
	p.mu.Unlock()
}

// clearSessionNode removes a session from the node cache.
// This should be called when a connection error occurs to force re-discovery.
func (p *Platform) clearSessionNode(sessionID string) {
	p.mu.Lock()
	delete(p.sessionNodes, sessionID)
	p.mu.Unlock()
}

// handleRedirect checks if the error is a redirect or connection error and updates routing.
// For redirects: updates the session-to-node mapping and returns a client for the new node.
// For connection errors: clears the stale cache and returns a client for the default address.
// Returns the new client to use and true if the error was handled (caller should retry).
func (p *Platform) handleRedirect(sessionID string, err error) (pb.SandboxAgentClient, bool) {
	// First check for explicit redirect
	host, port, ok := redirectTarget(err)
	if ok {
		// Build the new address
		newAddr := host
		if port > 0 {
			newAddr = net.JoinHostPort(host, strconv.Itoa(port))
		}

		slog.Debug("handling redirect", "session_id", sessionID, "new_addr", newAddr)
		p.updateSessionNode(sessionID, newAddr)
		return p.getClientForNode(newAddr), true
	}

	// Check for connection errors (e.g., node IP changed, pod restarted)
	if isConnectionError(err) {
		// Check if we had a cached node for this session
		p.mu.RLock()
		cachedAddr := p.sessionNodes[sessionID]
		p.mu.RUnlock()

		if cachedAddr != "" && cachedAddr != p.defaultAddr {
			slog.Debug("clearing stale node cache due to connection error",
				"session_id", sessionID,
				"stale_addr", cachedAddr,
				"error", err)
			p.clearSessionNode(sessionID)
			// Return default client for retry - this will re-discover the correct node
			return p.getClientForNode(p.defaultAddr), true
		}
	}

	return nil, false
}

// client returns the default sandbox-agent client.
// This is a helper for backward compatibility.
func (p *Platform) client() pb.SandboxAgentClient {
	return p.getClientForNode(p.defaultAddr)
}

// CreateSandbox creates a new sandbox via the agent.
func (p *Platform) CreateSandbox(ctx context.Context, req platform.CreateSandboxRequest) (*platform.Sandbox, error) {
	client := p.getClientForSession(req.SessionID)
	if client == nil {
		return nil, fmt.Errorf("no connection to sandbox-agent")
	}

	pbReq := &pb.CreateSandboxRequest{
		SessionId:   req.SessionID,
		Image:       req.Image,
		Command:     req.Command,
		Env:         req.Env,
		Labels:      req.Labels,
		DownloadUrl: req.DownloadURL,
		UserId:      req.UserID,
	}

	resp, err := client.CreateSandbox(ctx, pbReq)
	if err != nil {
		// Handle redirect
		if newClient, ok := p.handleRedirect(req.SessionID, err); ok && newClient != nil {
			resp, err = newClient.CreateSandbox(ctx, pbReq)
		}
	}
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return nil, platform.ErrAlreadyExists
		}
		return nil, fmt.Errorf("create sandbox: %w", err)
	}

	// Cache the node from response
	sandbox := protoToSandbox(resp)
	if sandbox.Host != "" {
		p.updateSessionNode(req.SessionID, normalizeAgentAddr(sandbox.Host))
	}

	return sandbox, nil
}

// GetSandbox retrieves sandbox info from the agent.
func (p *Platform) GetSandbox(ctx context.Context, sessionID string) (*platform.Sandbox, error) {
	client := p.getClientForSession(sessionID)
	pbReq := &pb.GetSandboxRequest{SessionId: sessionID}

	resp, err := client.GetSandbox(ctx, pbReq)
	if err != nil {
		// Handle redirect
		if newClient, ok := p.handleRedirect(sessionID, err); ok && newClient != nil {
			resp, err = newClient.GetSandbox(ctx, pbReq)
		}
	}
	if err != nil {
		if status.Code(err) == codes.NotFound {
			p.invalidateCache(sessionID)
			return nil, platform.ErrNotFound
		}
		return nil, fmt.Errorf("get sandbox: %w", err)
	}

	sandbox := protoToSandbox(resp)
	// Update node routing cache
	if sandbox.Host != "" {
		p.updateSessionNode(sessionID, normalizeAgentAddr(sandbox.Host))
	}
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

// DeleteSandbox deletes a sandbox via the agent.
func (p *Platform) DeleteSandbox(ctx context.Context, sessionID string) error {
	client := p.getClientForSession(sessionID)
	_, err := client.DeleteSandbox(ctx, &pb.DeleteSandboxRequest{
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
	resp, err := p.client().ListSandboxes(ctx, &pb.ListSandboxesRequest{})
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
	_, err := p.waitForSandboxReady(ctx, req.SessionID, req.ContainerName)
	if err != nil {
		return nil, err
	}

	var timeoutMs int64
	if req.Timeout > 0 {
		timeoutMs = req.Timeout.Milliseconds()
	}

	client := p.getClientForSession(req.SessionID)
	pbReq := &pb.ExecRequest{
		SessionId: req.SessionID,
		Args:      req.Command,
		Env:       mapToEnv(req.Env),
		Cwd:       req.WorkingDir,
		TimeoutMs: timeoutMs,
		UserId:    req.UserID,
	}

	resp, err := client.Exec(ctx, pbReq)
	if err != nil {
		if newClient, ok := p.handleRedirect(req.SessionID, err); ok && newClient != nil {
			resp, err = newClient.Exec(ctx, pbReq)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("exec: %w", err)
	}

	// Update activity - disabled temporarily to reduce K8s API load
	// p.updateActivity(ctx, req.SessionID)

	return &platform.ExecResult{
		ExitCode: int(resp.GetExitCode()),
		Stdout:   resp.GetStdout(),
		Stderr:   resp.GetStderr(),
	}, nil
}

// StartProcess starts a long-running process in a sandbox.
func (p *Platform) StartProcess(ctx context.Context, req platform.StartProcessRequest) (*platform.Process, error) {
	_, err := p.waitForSandboxReady(ctx, req.SessionID, req.ContainerName)
	if err != nil {
		return nil, err
	}

	client := p.getClientForSession(req.SessionID)
	pbReq := &pb.StartProcessRequest{
		SessionId: req.SessionID,
		Command:   req.Command,
		Env:       req.Env,
		Tty:       req.Terminal,
		UserId:    req.UserID,
	}

	resp, err := client.StartProcess(ctx, pbReq)
	if err != nil {
		if newClient, ok := p.handleRedirect(req.SessionID, err); ok && newClient != nil {
			resp, err = newClient.StartProcess(ctx, pbReq)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("start process: %w", err)
	}

	processID := resp.GetProcessId()
	containerID := resp.GetContainerId()

	p.mu.Lock()
	p.processContainers[processID] = containerID
	p.mu.Unlock()

	return &platform.Process{
		ExecID:    processID,
		StartedAt: time.Now().UTC(),
		Alive:     true,
	}, nil
}

// WriteToProcess writes data to a process stdin.
func (p *Platform) WriteToProcess(ctx context.Context, sessionID, execID string, data []byte) error {
	client := p.getClientForSession(sessionID)
	pbReq := &pb.WriteToProcessRequest{
		SessionId: sessionID,
		ProcessId: execID,
		Data:      data,
	}

	_, err := client.WriteToProcess(ctx, pbReq)
	if err != nil {
		if newClient, ok := p.handleRedirect(sessionID, err); ok && newClient != nil {
			_, err = newClient.WriteToProcess(ctx, pbReq)
		}
	}
	if err != nil {
		return fmt.Errorf("write to process: %w", err)
	}

	// p.updateActivity(ctx, sessionID)  // disabled temporarily
	return nil
}

// ReadFromProcess reads output from a process.
// Reads stdout and stderr in parallel to avoid blocking on one while the other has data.
func (p *Platform) ReadFromProcess(ctx context.Context, sessionID, execID string) (*platform.ProcessOutput, error) {
	readCtx, cancel := context.WithTimeout(ctx, agentReadTimeout)
	defer cancel()

	// Read stdout and stderr in parallel to avoid blocking
	type readResult struct {
		data []byte
		err  error
	}

	stdoutCh := make(chan readResult, 1)
	stderrCh := make(chan readResult, 1)

	go func() {
		resp, err := p.getClientForSession(sessionID).ReadProcessStdout(readCtx, &pb.ReadProcessOutputRequest{
			SessionId: sessionID,
			ProcessId: execID,
			MaxBytes:  4096,
		})
		var data []byte
		if resp != nil {
			data = resp.GetData()
		}
		stdoutCh <- readResult{data: data, err: err}
	}()

	go func() {
		resp, err := p.getClientForSession(sessionID).ReadProcessStderr(readCtx, &pb.ReadProcessOutputRequest{
			SessionId: sessionID,
			ProcessId: execID,
			MaxBytes:  4096,
		})
		var data []byte
		if resp != nil {
			data = resp.GetData()
		}
		stderrCh <- readResult{data: data, err: err}
	}()

	stdoutRes := <-stdoutCh
	stderrRes := <-stderrCh

	// Only return errors for non-timeout failures
	if stdoutRes.err != nil && !isTimeout(stdoutRes.err) {
		return nil, fmt.Errorf("read stdout: %w", stdoutRes.err)
	}
	if stderrRes.err != nil && !isTimeout(stderrRes.err) {
		return nil, fmt.Errorf("read stderr: %w", stderrRes.err)
	}

	return &platform.ProcessOutput{
		Stdout: stdoutRes.data,
		Stderr: stderrRes.data,
	}, nil
}

// ReadStdout reads only stdout from a process with a short timeout for non-blocking behavior.
func (p *Platform) ReadStdout(ctx context.Context, sessionID, execID string) ([]byte, error) {
	readCtx, cancel := context.WithTimeout(ctx, agentReadTimeout)
	defer cancel()

	resp, err := p.getClientForSession(sessionID).ReadProcessStdout(readCtx, &pb.ReadProcessOutputRequest{
		SessionId: sessionID,
		ProcessId: execID,
		MaxBytes:  4096,
	})
	if err != nil && !isTimeout(err) {
		return nil, fmt.Errorf("read stdout: %w", err)
	}
	if resp != nil {
		return resp.GetData(), nil
	}
	return nil, nil
}

// ReadStderr reads only stderr from a process with a short timeout for non-blocking behavior.
func (p *Platform) ReadStderr(ctx context.Context, sessionID, execID string) ([]byte, error) {
	readCtx, cancel := context.WithTimeout(ctx, agentReadTimeout)
	defer cancel()

	resp, err := p.getClientForSession(sessionID).ReadProcessStderr(readCtx, &pb.ReadProcessOutputRequest{
		SessionId: sessionID,
		ProcessId: execID,
		MaxBytes:  4096,
	})
	if err != nil && !isTimeout(err) {
		return nil, fmt.Errorf("read stderr: %w", err)
	}
	if resp != nil {
		return resp.GetData(), nil
	}
	return nil, nil
}

// KillProcess kills a process.
func (p *Platform) KillProcess(ctx context.Context, sessionID, execID string) error {
	client := p.getClientForSession(sessionID)
	// Send SIGKILL
	_, err := client.KillProcess(ctx, &pb.KillProcessRequest{
		SessionId: sessionID,
		ProcessId: execID,
		Signal:    9,
	})
	if err != nil {
		return fmt.Errorf("kill process: %w", err)
	}

	// Wait for exit
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, _ = client.WaitProcess(waitCtx, &pb.WaitProcessRequest{
		SessionId: sessionID,
		ProcessId: execID,
	})

	p.mu.Lock()
	delete(p.processContainers, execID)
	p.mu.Unlock()

	return nil
}

// IsProcessAlive checks if a process is still running.
func (p *Platform) IsProcessAlive(ctx context.Context, sessionID, execID string) (bool, error) {
	client := p.getClientForSession(sessionID)
	// Send signal 0 to check if alive
	_, err := client.KillProcess(ctx, &pb.KillProcessRequest{
		SessionId: sessionID,
		ProcessId: execID,
		Signal:    0,
	})
	if err != nil {
		return false, nil // Process is dead
	}
	return true, nil
}

// ResizeProcess resizes a process TTY.
func (p *Platform) ResizeProcess(ctx context.Context, sessionID, execID string, rows, columns uint32) error {
	client := p.getClientForSession(sessionID)
	_, err := client.ResizeTerminal(ctx, &pb.ResizeTerminalRequest{
		SessionId: sessionID,
		ProcessId: execID,
		Rows:      rows,
		Cols:      columns,
	})
	if err != nil {
		return fmt.Errorf("resize terminal: %w", err)
	}
	return nil
}

// StreamStdout returns a channel that streams stdout chunks from a process.
// Note: Streaming is implemented via polling in the new API.
func (p *Platform) StreamStdout(ctx context.Context, req platform.StreamReadRequest) (<-chan platform.StreamChunk, error) {
	ch := make(chan platform.StreamChunk)
	pollInterval := time.Duration(req.PollIntervalMs) * time.Millisecond
	if pollInterval <= 0 {
		pollInterval = 50 * time.Millisecond
	}

	go func() {
		defer close(ch)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			data, err := p.ReadStdout(ctx, req.SessionID, req.ExecID)
			if err != nil {
				ch <- platform.StreamChunk{EOF: true, Err: err}
				return
			}
			if len(data) > 0 {
				select {
				case ch <- platform.StreamChunk{Data: data}:
				case <-ctx.Done():
					return
				}
			}

			select {
			case <-time.After(pollInterval):
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
}

// StreamStderr returns a channel that streams stderr chunks from a process.
// Note: Streaming is implemented via polling in the new API.
func (p *Platform) StreamStderr(ctx context.Context, req platform.StreamReadRequest) (<-chan platform.StreamChunk, error) {
	ch := make(chan platform.StreamChunk)
	pollInterval := time.Duration(req.PollIntervalMs) * time.Millisecond
	if pollInterval <= 0 {
		pollInterval = 50 * time.Millisecond
	}

	go func() {
		defer close(ch)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			data, err := p.ReadStderr(ctx, req.SessionID, req.ExecID)
			if err != nil {
				ch <- platform.StreamChunk{EOF: true, Err: err}
				return
			}
			if len(data) > 0 {
				select {
				case ch <- platform.StreamChunk{Data: data}:
				case <-ctx.Done():
					return
				}
			}

			select {
			case <-time.After(pollInterval):
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
}

// StreamOutput returns a channel that streams both stdout and stderr using gRPC streaming.
// This is more efficient than polling ReadStdout/ReadStderr.
func (p *Platform) StreamOutput(ctx context.Context, req platform.StreamReadRequest) (<-chan platform.OutputChunk, error) {
	ch := make(chan platform.OutputChunk, 16)

	// Get the client for this session
	client := p.getClientForSession(req.SessionID)
	if client == nil {
		close(ch)
		return nil, fmt.Errorf("no connection to sandbox-agent")
	}

	pbReq := &pb.StreamProcessOutputRequest{
		SessionId: req.SessionID,
		ProcessId: req.ExecID,
	}

	// Start the streaming call
	stream, err := client.StreamProcessOutput(ctx, pbReq)
	if err != nil {
		// Handle redirect
		if newClient, ok := p.handleRedirect(req.SessionID, err); ok && newClient != nil {
			stream, err = newClient.StreamProcessOutput(ctx, pbReq)
		}
	}
	if err != nil {
		close(ch)
		return nil, fmt.Errorf("stream process output: %w", err)
	}

	go func() {
		defer close(ch)
		for {
			chunk, err := stream.Recv()
			if err != nil {
				// Check for normal stream end
				if err.Error() == "EOF" || status.Code(err) == codes.Canceled {
					ch <- platform.OutputChunk{EOF: true}
					return
				}
				// Check for redirect and retry
				if newClient, ok := p.handleRedirect(req.SessionID, err); ok && newClient != nil {
					newStream, retryErr := newClient.StreamProcessOutput(ctx, pbReq)
					if retryErr == nil {
						stream = newStream
						continue
					}
				}
				ch <- platform.OutputChunk{EOF: true, Err: err}
				return
			}

			// Convert proto stream type to platform type
			streamType := platform.StreamStdout
			if chunk.GetStream() == pb.ProcessOutputChunk_STDERR {
				streamType = platform.StreamStderr
			}

			select {
			case ch <- platform.OutputChunk{Stream: streamType, Data: chunk.GetData()}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
}

// waitForSandboxReady waits for a sandbox to be ready using exponential backoff with jitter.
func (p *Platform) waitForSandboxReady(ctx context.Context, sessionID, containerName string) (*platform.Sandbox, error) {
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
	_, err := p.client().UpdateSandboxActivity(ctx, &pb.UpdateSandboxActivityRequest{
		SessionId: sessionID,
	})
	if err != nil {
		slog.Warn("update activity failed", "session_id", sessionID, "error", err)
	}
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

// isConnectionError returns true if the error indicates a connection failure
// (e.g., the target node is unavailable, connection refused, etc.)
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}
	code := status.Code(err)
	return code == codes.Unavailable || code == codes.Internal
}
