package cri

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const (
	// Default socket paths for CRI runtimes
	ContainerdSocket = "/run/containerd/containerd.sock"
	CRIOSocket       = "/var/run/crio/crio.sock"

	// Default timeout for CRI operations
	DefaultTimeout = 30 * time.Second

	// Maximum message size for gRPC (16MB)
	maxMsgSize = 16 * 1024 * 1024
)

// Config holds CRI client configuration.
type Config struct {
	// Socket is the path to the CRI socket.
	// If empty, auto-detects containerd or CRI-O socket.
	Socket string

	// Timeout for CRI operations.
	Timeout time.Duration
}

// Client provides access to the CRI RuntimeService.
type Client struct {
	conn    *grpc.ClientConn
	runtime runtimeapi.RuntimeServiceClient
	timeout time.Duration
}

// ExecSyncResponse holds the response from ExecSync.
type ExecSyncResponse struct {
	Stdout   []byte
	Stderr   []byte
	ExitCode int32
}

// New creates a new CRI client.
func New(cfg Config) (*Client, error) {
	socketPath := cfg.Socket
	if socketPath == "" {
		var err error
		socketPath, err = detectSocket()
		if err != nil {
			return nil, err
		}
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, "unix://"+socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMsgSize),
			grpc.MaxCallSendMsgSize(maxMsgSize),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to CRI socket %s: %w", socketPath, err)
	}

	return &Client{
		conn:    conn,
		runtime: runtimeapi.NewRuntimeServiceClient(conn),
		timeout: timeout,
	}, nil
}

// detectSocket tries to find an available CRI socket.
func detectSocket() (string, error) {
	// Try containerd first
	if _, err := os.Stat(ContainerdSocket); err == nil {
		return ContainerdSocket, nil
	}

	// Try CRI-O
	if _, err := os.Stat(CRIOSocket); err == nil {
		return CRIOSocket, nil
	}

	return "", errors.New("no CRI socket found: tried " + ContainerdSocket + " and " + CRIOSocket)
}

// Close closes the CRI client connection.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// ExecSync executes a command synchronously and returns the result.
// This is suitable for short-running commands.
func (c *Client) ExecSync(ctx context.Context, containerID string, cmd []string, timeout time.Duration) (*ExecSyncResponse, error) {
	if timeout <= 0 {
		timeout = c.timeout
	}

	// Strip container ID prefix (e.g., "containerd://", "docker://")
	containerID = stripContainerIDPrefix(containerID)

	resp, err := c.runtime.ExecSync(ctx, &runtimeapi.ExecSyncRequest{
		ContainerId: containerID,
		Cmd:         cmd,
		Timeout:     int64(timeout.Seconds()),
	})
	if err != nil {
		return nil, fmt.Errorf("exec sync failed: %w", err)
	}

	return &ExecSyncResponse{
		Stdout:   resp.Stdout,
		Stderr:   resp.Stderr,
		ExitCode: resp.ExitCode,
	}, nil
}

// Exec requests a streaming exec endpoint URL.
// The caller is responsible for connecting to the returned URL.
func (c *Client) Exec(ctx context.Context, containerID string, cmd []string, stdin, tty bool) (string, error) {
	containerID = stripContainerIDPrefix(containerID)

	resp, err := c.runtime.Exec(ctx, &runtimeapi.ExecRequest{
		ContainerId: containerID,
		Cmd:         cmd,
		Stdin:       stdin,
		Stdout:      true,
		Stderr:      !tty, // stderr merged with stdout in tty mode
		Tty:         tty,
	})
	if err != nil {
		return "", fmt.Errorf("exec request failed: %w", err)
	}

	return resp.Url, nil
}

// Attach requests a streaming attach endpoint URL.
func (c *Client) Attach(ctx context.Context, containerID string, stdin, tty bool) (string, error) {
	containerID = stripContainerIDPrefix(containerID)

	resp, err := c.runtime.Attach(ctx, &runtimeapi.AttachRequest{
		ContainerId: containerID,
		Stdin:       stdin,
		Stdout:      true,
		Stderr:      !tty,
		Tty:         tty,
	})
	if err != nil {
		return "", fmt.Errorf("attach request failed: %w", err)
	}

	return resp.Url, nil
}

// ListContainers lists containers matching the given filter.
func (c *Client) ListContainers(ctx context.Context, filter *runtimeapi.ContainerFilter) ([]*runtimeapi.Container, error) {
	resp, err := c.runtime.ListContainers(ctx, &runtimeapi.ListContainersRequest{
		Filter: filter,
	})
	if err != nil {
		return nil, fmt.Errorf("list containers failed: %w", err)
	}
	return resp.Containers, nil
}

// ContainerStatus returns the status of a container.
func (c *Client) ContainerStatus(ctx context.Context, containerID string) (*runtimeapi.ContainerStatus, error) {
	containerID = stripContainerIDPrefix(containerID)

	resp, err := c.runtime.ContainerStatus(ctx, &runtimeapi.ContainerStatusRequest{
		ContainerId: containerID,
		Verbose:     false,
	})
	if err != nil {
		return nil, fmt.Errorf("container status failed: %w", err)
	}
	return resp.Status, nil
}

// stripContainerIDPrefix removes the runtime prefix from container IDs.
// e.g., "containerd://abc123" -> "abc123"
func stripContainerIDPrefix(containerID string) string {
	if idx := strings.Index(containerID, "://"); idx != -1 {
		return containerID[idx+3:]
	}
	return containerID
}
