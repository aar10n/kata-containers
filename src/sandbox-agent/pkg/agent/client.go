package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	cdshim "github.com/containerd/containerd/runtime/v2/shim"
	"github.com/google/uuid"
	agenttypes "github.com/kata-containers/kata-containers/src/runtime/virtcontainers/pkg/agent/protocols"
	kataclient "github.com/kata-containers/kata-containers/src/runtime/virtcontainers/pkg/agent/protocols/client"
	agentgrpc "github.com/kata-containers/kata-containers/src/runtime/virtcontainers/pkg/agent/protocols/grpc"
)

const agentURLPath = "/agent-url"
const (
	legacyShimStoragePath = "/run/vc/sbs"
	rustShimStoragePath   = "/run/kata"
	shimSocketName        = "shim-monitor.sock"
)

type Config struct {
	Timeout       time.Duration
	ReadChunkSize int
	MaxOutputSize int
}

type Client struct {
	timeout       time.Duration
	readChunkSize int
	maxOutputSize int
}

type ExecResult struct {
	Stdout   string
	Stderr   string
	ExitCode int32
}

func New(cfg Config) *Client {
	readSize := cfg.ReadChunkSize
	if readSize <= 0 {
		readSize = 4096
	}
	maxOutput := cfg.MaxOutputSize
	if maxOutput <= 0 {
		maxOutput = 1024 * 1024
	}

	return &Client{
		timeout:       cfg.Timeout,
		readChunkSize: readSize,
		maxOutputSize: maxOutput,
	}
}

func (c *Client) Exec(ctx context.Context, sandboxID, containerID string, args, env []string, cwd string, timeout time.Duration) (ExecResult, error) {
	if sandboxID == "" {
		return ExecResult{}, errors.New("sandbox id is required")
	}
	if containerID == "" {
		containerID = sandboxID
	}
	if len(args) == 0 {
		return ExecResult{}, errors.New("args cannot be empty")
	}
	if cwd == "" {
		cwd = "/"
	}
	if timeout <= 0 {
		timeout = c.timeout
	}

	agentURL, err := c.agentURL(sandboxID, timeout)
	if err != nil {
		return ExecResult{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	client, err := kataclient.NewAgentClient(ctx, agentURL, uint32(timeout.Seconds()))
	if err != nil {
		return ExecResult{}, fmt.Errorf("connect agent: %w", err)
	}
	defer client.Close()

	execID := uuid.NewString()
	req := &agentgrpc.ExecProcessRequest{
		ContainerId: containerID,
		ExecId:      execID,
		Process: &agentgrpc.Process{
			Terminal: false,
			Args:     args,
			Env:      env,
			Cwd:      cwd,
		},
	}

	if _, err := client.AgentServiceClient.ExecProcess(ctx, req); err != nil {
		return ExecResult{}, fmt.Errorf("exec process: %w", err)
	}

	type readResult struct {
		data string
		err  error
	}
	stdoutCh := make(chan readResult, 1)
	stderrCh := make(chan readResult, 1)

	go func() {
		out, err := c.readStream(ctx, func(readCtx context.Context, req *agentgrpc.ReadStreamRequest) (*agentgrpc.ReadStreamResponse, error) {
			return client.AgentServiceClient.ReadStdout(readCtx, req)
		}, containerID, execID)
		stdoutCh <- readResult{data: out, err: err}
	}()

	go func() {
		out, err := c.readStream(ctx, func(readCtx context.Context, req *agentgrpc.ReadStreamRequest) (*agentgrpc.ReadStreamResponse, error) {
			return client.AgentServiceClient.ReadStderr(readCtx, req)
		}, containerID, execID)
		stderrCh <- readResult{data: out, err: err}
	}()

	stdoutRes := <-stdoutCh
	if stdoutRes.err != nil {
		return ExecResult{}, fmt.Errorf("read stdout: %w", stdoutRes.err)
	}

	stderrRes := <-stderrCh
	if stderrRes.err != nil {
		return ExecResult{}, fmt.Errorf("read stderr: %w", stderrRes.err)
	}

	waitResp, err := client.AgentServiceClient.WaitProcess(ctx, &agentgrpc.WaitProcessRequest{
		ContainerId: containerID,
		ExecId:      execID,
	})
	if err != nil {
		return ExecResult{}, fmt.Errorf("wait process: %w", err)
	}

	return ExecResult{
		Stdout:   stdoutRes.data,
		Stderr:   stderrRes.data,
		ExitCode: waitResp.Status,
	}, nil
}

func (c *Client) CreateContainer(ctx context.Context, sandboxID string, req *agentgrpc.CreateContainerRequest) error {
	return c.withClient(ctx, sandboxID, 0, func(ctx context.Context, client *kataclient.AgentClient) error {
		_, err := client.AgentServiceClient.CreateContainer(ctx, req)
		return err
	})
}

func (c *Client) StartContainer(ctx context.Context, sandboxID string, req *agentgrpc.StartContainerRequest) error {
	return c.withClient(ctx, sandboxID, 0, func(ctx context.Context, client *kataclient.AgentClient) error {
		_, err := client.AgentServiceClient.StartContainer(ctx, req)
		return err
	})
}

func (c *Client) RemoveContainer(ctx context.Context, sandboxID string, req *agentgrpc.RemoveContainerRequest) error {
	return c.withClient(ctx, sandboxID, 0, func(ctx context.Context, client *kataclient.AgentClient) error {
		_, err := client.AgentServiceClient.RemoveContainer(ctx, req)
		return err
	})
}

func (c *Client) ExecProcess(ctx context.Context, sandboxID string, req *agentgrpc.ExecProcessRequest) error {
	return c.withClient(ctx, sandboxID, 0, func(ctx context.Context, client *kataclient.AgentClient) error {
		_, err := client.AgentServiceClient.ExecProcess(ctx, req)
		return err
	})
}

func (c *Client) SignalProcess(ctx context.Context, sandboxID string, req *agentgrpc.SignalProcessRequest) error {
	return c.withClient(ctx, sandboxID, 0, func(ctx context.Context, client *kataclient.AgentClient) error {
		_, err := client.AgentServiceClient.SignalProcess(ctx, req)
		return err
	})
}

func (c *Client) WaitProcess(ctx context.Context, sandboxID string, req *agentgrpc.WaitProcessRequest) (*agentgrpc.WaitProcessResponse, error) {
	return withClientResp(ctx, c, sandboxID, 0, func(ctx context.Context, client *kataclient.AgentClient) (*agentgrpc.WaitProcessResponse, error) {
		return client.AgentServiceClient.WaitProcess(ctx, req)
	})
}

func (c *Client) UpdateContainer(ctx context.Context, sandboxID string, req *agentgrpc.UpdateContainerRequest) error {
	return c.withClient(ctx, sandboxID, 0, func(ctx context.Context, client *kataclient.AgentClient) error {
		_, err := client.AgentServiceClient.UpdateContainer(ctx, req)
		return err
	})
}

func (c *Client) UpdateEphemeralMounts(ctx context.Context, sandboxID string, req *agentgrpc.UpdateEphemeralMountsRequest) error {
	return c.withClient(ctx, sandboxID, 0, func(ctx context.Context, client *kataclient.AgentClient) error {
		_, err := client.AgentServiceClient.UpdateEphemeralMounts(ctx, req)
		return err
	})
}

func (c *Client) StatsContainer(ctx context.Context, sandboxID string, req *agentgrpc.StatsContainerRequest) (*agentgrpc.StatsContainerResponse, error) {
	return withClientResp(ctx, c, sandboxID, 0, func(ctx context.Context, client *kataclient.AgentClient) (*agentgrpc.StatsContainerResponse, error) {
		return client.AgentServiceClient.StatsContainer(ctx, req)
	})
}

func (c *Client) PauseContainer(ctx context.Context, sandboxID string, req *agentgrpc.PauseContainerRequest) error {
	return c.withClient(ctx, sandboxID, 0, func(ctx context.Context, client *kataclient.AgentClient) error {
		_, err := client.AgentServiceClient.PauseContainer(ctx, req)
		return err
	})
}

func (c *Client) ResumeContainer(ctx context.Context, sandboxID string, req *agentgrpc.ResumeContainerRequest) error {
	return c.withClient(ctx, sandboxID, 0, func(ctx context.Context, client *kataclient.AgentClient) error {
		_, err := client.AgentServiceClient.ResumeContainer(ctx, req)
		return err
	})
}

func (c *Client) WriteStdin(ctx context.Context, sandboxID string, req *agentgrpc.WriteStreamRequest) (*agentgrpc.WriteStreamResponse, error) {
	return withClientResp(ctx, c, sandboxID, 0, func(ctx context.Context, client *kataclient.AgentClient) (*agentgrpc.WriteStreamResponse, error) {
		return client.AgentServiceClient.WriteStdin(ctx, req)
	})
}

func (c *Client) ReadStdout(ctx context.Context, sandboxID string, req *agentgrpc.ReadStreamRequest) (*agentgrpc.ReadStreamResponse, error) {
	return withClientResp(ctx, c, sandboxID, 0, func(ctx context.Context, client *kataclient.AgentClient) (*agentgrpc.ReadStreamResponse, error) {
		return client.AgentServiceClient.ReadStdout(ctx, req)
	})
}

func (c *Client) ReadStderr(ctx context.Context, sandboxID string, req *agentgrpc.ReadStreamRequest) (*agentgrpc.ReadStreamResponse, error) {
	return withClientResp(ctx, c, sandboxID, 0, func(ctx context.Context, client *kataclient.AgentClient) (*agentgrpc.ReadStreamResponse, error) {
		return client.AgentServiceClient.ReadStderr(ctx, req)
	})
}

func (c *Client) CloseStdin(ctx context.Context, sandboxID string, req *agentgrpc.CloseStdinRequest) error {
	return c.withClient(ctx, sandboxID, 0, func(ctx context.Context, client *kataclient.AgentClient) error {
		_, err := client.AgentServiceClient.CloseStdin(ctx, req)
		return err
	})
}

func (c *Client) TtyWinResize(ctx context.Context, sandboxID string, req *agentgrpc.TtyWinResizeRequest) error {
	return c.withClient(ctx, sandboxID, 0, func(ctx context.Context, client *kataclient.AgentClient) error {
		_, err := client.AgentServiceClient.TtyWinResize(ctx, req)
		return err
	})
}

func (c *Client) UpdateInterface(ctx context.Context, sandboxID string, req *agentgrpc.UpdateInterfaceRequest) (*agenttypes.Interface, error) {
	return withClientResp(ctx, c, sandboxID, 0, func(ctx context.Context, client *kataclient.AgentClient) (*agenttypes.Interface, error) {
		return client.AgentServiceClient.UpdateInterface(ctx, req)
	})
}

func (c *Client) UpdateRoutes(ctx context.Context, sandboxID string, req *agentgrpc.UpdateRoutesRequest) (*agentgrpc.Routes, error) {
	return withClientResp(ctx, c, sandboxID, 0, func(ctx context.Context, client *kataclient.AgentClient) (*agentgrpc.Routes, error) {
		return client.AgentServiceClient.UpdateRoutes(ctx, req)
	})
}

func (c *Client) ListInterfaces(ctx context.Context, sandboxID string, req *agentgrpc.ListInterfacesRequest) (*agentgrpc.Interfaces, error) {
	return withClientResp(ctx, c, sandboxID, 0, func(ctx context.Context, client *kataclient.AgentClient) (*agentgrpc.Interfaces, error) {
		return client.AgentServiceClient.ListInterfaces(ctx, req)
	})
}

func (c *Client) ListRoutes(ctx context.Context, sandboxID string, req *agentgrpc.ListRoutesRequest) (*agentgrpc.Routes, error) {
	return withClientResp(ctx, c, sandboxID, 0, func(ctx context.Context, client *kataclient.AgentClient) (*agentgrpc.Routes, error) {
		return client.AgentServiceClient.ListRoutes(ctx, req)
	})
}

func (c *Client) AddARPNeighbors(ctx context.Context, sandboxID string, req *agentgrpc.AddARPNeighborsRequest) error {
	return c.withClient(ctx, sandboxID, 0, func(ctx context.Context, client *kataclient.AgentClient) error {
		_, err := client.AgentServiceClient.AddARPNeighbors(ctx, req)
		return err
	})
}

func (c *Client) GetIPTables(ctx context.Context, sandboxID string, req *agentgrpc.GetIPTablesRequest) (*agentgrpc.GetIPTablesResponse, error) {
	return withClientResp(ctx, c, sandboxID, 0, func(ctx context.Context, client *kataclient.AgentClient) (*agentgrpc.GetIPTablesResponse, error) {
		return client.AgentServiceClient.GetIPTables(ctx, req)
	})
}

func (c *Client) SetIPTables(ctx context.Context, sandboxID string, req *agentgrpc.SetIPTablesRequest) (*agentgrpc.SetIPTablesResponse, error) {
	return withClientResp(ctx, c, sandboxID, 0, func(ctx context.Context, client *kataclient.AgentClient) (*agentgrpc.SetIPTablesResponse, error) {
		return client.AgentServiceClient.SetIPTables(ctx, req)
	})
}

func (c *Client) GetMetrics(ctx context.Context, sandboxID string, req *agentgrpc.GetMetricsRequest) (*agentgrpc.Metrics, error) {
	return withClientResp(ctx, c, sandboxID, 0, func(ctx context.Context, client *kataclient.AgentClient) (*agentgrpc.Metrics, error) {
		return client.AgentServiceClient.GetMetrics(ctx, req)
	})
}

func (c *Client) MemAgentMemcgSet(ctx context.Context, sandboxID string, req *agentgrpc.MemAgentMemcgConfig) error {
	return c.withClient(ctx, sandboxID, 0, func(ctx context.Context, client *kataclient.AgentClient) error {
		_, err := client.AgentServiceClient.MemAgentMemcgSet(ctx, req)
		return err
	})
}

func (c *Client) MemAgentCompactSet(ctx context.Context, sandboxID string, req *agentgrpc.MemAgentCompactConfig) error {
	return c.withClient(ctx, sandboxID, 0, func(ctx context.Context, client *kataclient.AgentClient) error {
		_, err := client.AgentServiceClient.MemAgentCompactSet(ctx, req)
		return err
	})
}

func (c *Client) SetGuestDateTime(ctx context.Context, sandboxID string, req *agentgrpc.SetGuestDateTimeRequest) error {
	return c.withClient(ctx, sandboxID, 0, func(ctx context.Context, client *kataclient.AgentClient) error {
		_, err := client.AgentServiceClient.SetGuestDateTime(ctx, req)
		return err
	})
}

func (c *Client) CopyFile(ctx context.Context, sandboxID string, req *agentgrpc.CopyFileRequest) error {
	return c.withClient(ctx, sandboxID, 0, func(ctx context.Context, client *kataclient.AgentClient) error {
		_, err := client.AgentServiceClient.CopyFile(ctx, req)
		return err
	})
}

func (c *Client) GetVolumeStats(ctx context.Context, sandboxID string, req *agentgrpc.VolumeStatsRequest) (*agentgrpc.VolumeStatsResponse, error) {
	return withClientResp(ctx, c, sandboxID, 0, func(ctx context.Context, client *kataclient.AgentClient) (*agentgrpc.VolumeStatsResponse, error) {
		return client.AgentServiceClient.GetVolumeStats(ctx, req)
	})
}

func (c *Client) ResizeVolume(ctx context.Context, sandboxID string, req *agentgrpc.ResizeVolumeRequest) error {
	return c.withClient(ctx, sandboxID, 0, func(ctx context.Context, client *kataclient.AgentClient) error {
		_, err := client.AgentServiceClient.ResizeVolume(ctx, req)
		return err
	})
}

func (c *Client) agentURL(sandboxID string, timeout time.Duration) (string, error) {
	resp, err := shimDoGet(sandboxID, timeout, agentURLPath)
	if err != nil {
		return "", fmt.Errorf("fetch agent url: %w", err)
	}

	url := strings.TrimSpace(string(resp))
	if url == "" {
		return "", errors.New("agent url is empty")
	}

	return url, nil
}

func shimDoGet(sandboxID string, timeout time.Duration, urlPath string) ([]byte, error) {
	client, err := buildShimClient(sandboxID, timeout)
	if err != nil {
		return nil, err
	}

	resp, err := client.Get(fmt.Sprintf("http://shim%s", urlPath))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

func buildShimClient(sandboxID string, timeout time.Duration) (*http.Client, error) {
	socketAddress, err := shimClientSocketAddress(sandboxID)
	if err != nil {
		return nil, err
	}

	transport := &http.Transport{
		DisableKeepAlives: true,
		Dial: func(proto, addr string) (net.Conn, error) {
			return cdshim.AnonDialer(socketAddress, timeout)
		},
	}

	client := &http.Client{Transport: transport}
	if timeout > 0 {
		client.Timeout = timeout
	}

	return client, nil
}

func shimClientSocketAddress(sandboxID string) (string, error) {
	socketPath := shimSocketPath(legacyShimStoragePath, sandboxID)
	if _, err := os.Stat(socketPath); err != nil {
		fallbackPath, fallbackErr := shimSocketPathWithPrefix(legacyShimStoragePath, sandboxID)
		if fallbackErr == nil {
			socketPath = fallbackPath
		} else {
			altPath := shimSocketPath(rustShimStoragePath, sandboxID)
			if _, err := os.Stat(altPath); err != nil {
				altFallback, altFallbackErr := shimSocketPathWithPrefix(rustShimStoragePath, sandboxID)
				if altFallbackErr != nil {
					return "", fmt.Errorf("missing shim socket at %s and %s: %w", socketPath, altPath, err)
				}
				socketPath = altFallback
			} else {
				socketPath = altPath
			}
		}
	}

	return fmt.Sprintf("unix://%s", socketPath), nil
}

func shimSocketPath(storagePath, sandboxID string) string {
	return filepath.Join(string(filepath.Separator), storagePath, sandboxID, shimSocketName)
}

func shimSocketPathWithPrefix(storagePath, sandboxID string) (string, error) {
	entries, err := os.ReadDir(storagePath)
	if err != nil {
		return "", err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(sandboxID, name) {
			socketPath := filepath.Join(string(filepath.Separator), storagePath, name, shimSocketName)
			if _, err := os.Stat(socketPath); err == nil {
				return socketPath, nil
			}
		}
	}

	return "", fmt.Errorf("no shim socket found for id prefix %s", sandboxID)
}

type readFn func(context.Context, *agentgrpc.ReadStreamRequest) (*agentgrpc.ReadStreamResponse, error)

func (c *Client) readStream(ctx context.Context, reader readFn, containerID, execID string) (string, error) {
	var builder strings.Builder
	remaining := c.maxOutputSize

	for remaining > 0 {
		chunk := c.readChunkSize
		if chunk > remaining {
			chunk = remaining
		}

		resp, err := reader(ctx, &agentgrpc.ReadStreamRequest{
			ContainerId: containerID,
			ExecId:      execID,
			Len:         uint32(chunk),
		})
		if err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "eof") {
				break
			}
			return builder.String(), err
		}
		if resp == nil || len(resp.Data) == 0 {
			break
		}
		builder.Write(resp.Data)
		remaining -= len(resp.Data)
	}

	return builder.String(), nil
}

func (c *Client) withClient(ctx context.Context, sandboxID string, timeout time.Duration, fn func(context.Context, *kataclient.AgentClient) error) error {
	if sandboxID == "" {
		return errors.New("sandbox id is required")
	}
	if timeout <= 0 {
		timeout = c.timeout
	}

	agentURL, err := c.agentURL(sandboxID, timeout)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	client, err := kataclient.NewAgentClient(ctx, agentURL, uint32(timeout.Seconds()))
	if err != nil {
		return fmt.Errorf("connect agent: %w", err)
	}
	defer client.Close()

	return fn(ctx, client)
}

func withClientResp[T any](ctx context.Context, c *Client, sandboxID string, timeout time.Duration, fn func(context.Context, *kataclient.AgentClient) (T, error)) (T, error) {
	var zero T
	if sandboxID == "" {
		return zero, errors.New("sandbox id is required")
	}
	if timeout <= 0 {
		timeout = c.timeout
	}

	agentURL, err := c.agentURL(sandboxID, timeout)
	if err != nil {
		return zero, err
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	client, err := kataclient.NewAgentClient(ctx, agentURL, uint32(timeout.Seconds()))
	if err != nil {
		return zero, fmt.Errorf("connect agent: %w", err)
	}
	defer client.Close()

	return fn(ctx, client)
}
