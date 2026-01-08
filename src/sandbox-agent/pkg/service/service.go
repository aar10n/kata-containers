package service

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/agent"
	apierrors "github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/errors"
	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/k8s"
	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/shim_mgmt"
	agenttypes "github.com/kata-containers/kata-containers/src/runtime/virtcontainers/pkg/agent/protocols"
	agentgrpc "github.com/kata-containers/kata-containers/src/runtime/virtcontainers/pkg/agent/protocols/grpc"
)

type Config struct {
	NodeName    string
	ExecTimeout time.Duration
}

type Service interface {
	// Sandbox lifecycle
	CreateSandbox(ctx context.Context, req CreateSandboxRequest) (*k8s.SandboxInfo, error)
	GetSandbox(ctx context.Context, sessionID string) (*k8s.SandboxInfo, error)
	DeleteSandbox(ctx context.Context, sessionID string) error
	ListSandboxes(ctx context.Context, nodeName string) []*k8s.SandboxInfo
	UpdateSandboxActivity(ctx context.Context, sessionID string) error

	// VM operations
	NodeForVM(vmID string) (string, bool)
	SandboxAgentAddressForNode(nodeName string) (string, bool)
	ResolveSandboxID(ctx context.Context, containerID string) (string, error)
	Exec(ctx context.Context, req ExecRequest) (ExecResponse, error)
	SaveVMState(ctx context.Context, vmID, statePath string) error
	RestoreVMState(ctx context.Context, vmID, statePath string) error
	CreateContainer(ctx context.Context, vmID string, req *agentgrpc.CreateContainerRequest) error
	StartContainer(ctx context.Context, vmID string, req *agentgrpc.StartContainerRequest) error
	RemoveContainer(ctx context.Context, vmID string, req *agentgrpc.RemoveContainerRequest) error
	ExecProcess(ctx context.Context, vmID string, req *agentgrpc.ExecProcessRequest) error
	SignalProcess(ctx context.Context, vmID string, req *agentgrpc.SignalProcessRequest) error
	WaitProcess(ctx context.Context, vmID string, req *agentgrpc.WaitProcessRequest) (*agentgrpc.WaitProcessResponse, error)
	UpdateContainer(ctx context.Context, vmID string, req *agentgrpc.UpdateContainerRequest) error
	UpdateEphemeralMounts(ctx context.Context, vmID string, req *agentgrpc.UpdateEphemeralMountsRequest) error
	StatsContainer(ctx context.Context, vmID string, req *agentgrpc.StatsContainerRequest) (*agentgrpc.StatsContainerResponse, error)
	PauseContainer(ctx context.Context, vmID string, req *agentgrpc.PauseContainerRequest) error
	ResumeContainer(ctx context.Context, vmID string, req *agentgrpc.ResumeContainerRequest) error
	WriteStdin(ctx context.Context, vmID string, req *agentgrpc.WriteStreamRequest) (*agentgrpc.WriteStreamResponse, error)
	ReadStdout(ctx context.Context, vmID string, req *agentgrpc.ReadStreamRequest) (*agentgrpc.ReadStreamResponse, error)
	ReadStderr(ctx context.Context, vmID string, req *agentgrpc.ReadStreamRequest) (*agentgrpc.ReadStreamResponse, error)
	CloseStdin(ctx context.Context, vmID string, req *agentgrpc.CloseStdinRequest) error
	TtyWinResize(ctx context.Context, vmID string, req *agentgrpc.TtyWinResizeRequest) error
	UpdateInterface(ctx context.Context, vmID string, req *agentgrpc.UpdateInterfaceRequest) (*agenttypes.Interface, error)
	UpdateRoutes(ctx context.Context, vmID string, req *agentgrpc.UpdateRoutesRequest) (*agentgrpc.Routes, error)
	ListInterfaces(ctx context.Context, vmID string, req *agentgrpc.ListInterfacesRequest) (*agentgrpc.Interfaces, error)
	ListRoutes(ctx context.Context, vmID string, req *agentgrpc.ListRoutesRequest) (*agentgrpc.Routes, error)
	AddARPNeighbors(ctx context.Context, vmID string, req *agentgrpc.AddARPNeighborsRequest) error
	GetIPTables(ctx context.Context, vmID string, req *agentgrpc.GetIPTablesRequest) (*agentgrpc.GetIPTablesResponse, error)
	SetIPTables(ctx context.Context, vmID string, req *agentgrpc.SetIPTablesRequest) (*agentgrpc.SetIPTablesResponse, error)
	GetMetrics(ctx context.Context, vmID string, req *agentgrpc.GetMetricsRequest) (*agentgrpc.Metrics, error)
	MemAgentMemcgSet(ctx context.Context, vmID string, req *agentgrpc.MemAgentMemcgConfig) error
	MemAgentCompactSet(ctx context.Context, vmID string, req *agentgrpc.MemAgentCompactConfig) error
	SetGuestDateTime(ctx context.Context, vmID string, req *agentgrpc.SetGuestDateTimeRequest) error
	CopyFile(ctx context.Context, vmID string, req *agentgrpc.CopyFileRequest) error
	GetVolumeStats(ctx context.Context, vmID string, req *agentgrpc.VolumeStatsRequest) (*agentgrpc.VolumeStatsResponse, error)
	ResizeVolume(ctx context.Context, vmID string, req *agentgrpc.ResizeVolumeRequest) error

	// Streaming read operations
	StreamReadStdout(ctx context.Context, req StreamReadRequest, send func(StreamChunk) error) error
	StreamReadStderr(ctx context.Context, req StreamReadRequest, send func(StreamChunk) error) error
}

// StreamReadRequest holds parameters for streaming read operations.
type StreamReadRequest struct {
	VMID           string
	ContainerID    string
	ExecID         string
	PollIntervalMs int32
}

// StreamChunk represents a chunk of streaming output.
type StreamChunk struct {
	Data []byte
	EOF  bool
}

// CreateSandboxRequest holds parameters for creating a sandbox.
type CreateSandboxRequest struct {
	SessionID string
	Image     string
	Command   []string
	Env       map[string]string
	Labels    map[string]string
}

type service struct {
	config      Config
	store       *k8s.Store
	manager     *k8s.Manager
	agentClient *agent.Client
	shimClient  *shim_mgmt.Client
}

func New(cfg Config, store *k8s.Store, manager *k8s.Manager, agentClient *agent.Client, shimClient *shim_mgmt.Client) Service {
	if shimClient == nil {
		shimClient = shim_mgmt.New(shim_mgmt.Config{})
	}
	return &service{
		config:      cfg,
		store:       store,
		manager:     manager,
		agentClient: agentClient,
		shimClient:  shimClient,
	}
}

type ExecRequest struct {
	VMID        string
	ContainerID string
	Args        []string
	Env         []string
	Cwd         string
	Timeout     time.Duration
}

type ExecResponse struct {
	Stdout   string
	Stderr   string
	ExitCode int32
}

func (s *service) NodeForVM(vmID string) (string, bool) {
	return s.store.NodeForVM(vmID)
}

func (s *service) SandboxAgentAddressForNode(nodeName string) (string, bool) {
	return s.store.SandboxAgentAddressForNode(nodeName)
}

func (s *service) ResolveSandboxID(ctx context.Context, containerID string) (string, error) {
	if strings.TrimSpace(containerID) == "" {
		return "", fmt.Errorf("%w: container id is required", apierrors.ErrInvalidArgument)
	}
	sandboxID := k8s.ResolveSandboxIDFromContainerID(containerID)
	if sandboxID == "" {
		return "", fmt.Errorf("%w: sandbox id not found", apierrors.ErrNotFound)
	}
	return sandboxID, nil
}

func (s *service) Exec(ctx context.Context, req ExecRequest) (ExecResponse, error) {
	if req.VMID == "" {
		return ExecResponse{}, fmt.Errorf("%w: vm id is required", apierrors.ErrInvalidArgument)
	}

	result, err := s.agentClient.Exec(ctx, req.VMID, req.ContainerID, req.Args, req.Env, req.Cwd, s.execTimeout(req.Timeout))
	if err != nil {
		return ExecResponse{}, err
	}

	return ExecResponse{
		Stdout:   result.Stdout,
		Stderr:   result.Stderr,
		ExitCode: result.ExitCode,
	}, nil
}

func (s *service) CreateSandbox(ctx context.Context, req CreateSandboxRequest) (*k8s.SandboxInfo, error) {
	return s.manager.CreateSandbox(ctx, k8s.CreateSandboxRequest{
		SessionID: req.SessionID,
		Image:     req.Image,
		Command:   req.Command,
		Env:       req.Env,
		Labels:    req.Labels,
	})
}

func (s *service) GetSandbox(ctx context.Context, sessionID string) (*k8s.SandboxInfo, error) {
	info, ok := s.store.GetSandbox(sessionID)
	if !ok {
		return nil, k8s.ErrSandboxNotFound
	}
	return info, nil
}

func (s *service) DeleteSandbox(ctx context.Context, sessionID string) error {
	return s.manager.DeleteSandbox(ctx, sessionID)
}

func (s *service) ListSandboxes(ctx context.Context, nodeName string) []*k8s.SandboxInfo {
	sandboxes := s.store.ListSandboxes(nodeName)
	// Sort by node then session ID for consistent ordering
	sort.Slice(sandboxes, func(i, j int) bool {
		if sandboxes[i].Node == sandboxes[j].Node {
			return sandboxes[i].SessionID < sandboxes[j].SessionID
		}
		return sandboxes[i].Node < sandboxes[j].Node
	})
	return sandboxes
}

func (s *service) UpdateSandboxActivity(ctx context.Context, sessionID string) error {
	return s.manager.UpdateSandboxActivity(ctx, sessionID)
}

func (s *service) SaveVMState(ctx context.Context, vmID, statePath string) error {
	if err := requireVMID(vmID); err != nil {
		return err
	}
	if strings.TrimSpace(statePath) == "" {
		return fmt.Errorf("%w: state path is required", apierrors.ErrInvalidArgument)
	}
	return s.shimClient.SaveVMState(ctx, vmID, statePath)
}

func (s *service) RestoreVMState(ctx context.Context, vmID, statePath string) error {
	if err := requireVMID(vmID); err != nil {
		return err
	}
	if strings.TrimSpace(statePath) == "" {
		return fmt.Errorf("%w: state path is required", apierrors.ErrInvalidArgument)
	}
	return s.shimClient.RestoreVMState(ctx, vmID, statePath)
}

func (s *service) CreateContainer(ctx context.Context, vmID string, req *agentgrpc.CreateContainerRequest) error {
	return s.callAgent(ctx, vmID, func(ctx context.Context) error {
		return s.agentClient.CreateContainer(ctx, vmID, req)
	})
}

func (s *service) StartContainer(ctx context.Context, vmID string, req *agentgrpc.StartContainerRequest) error {
	return s.callAgent(ctx, vmID, func(ctx context.Context) error {
		return s.agentClient.StartContainer(ctx, vmID, req)
	})
}

func (s *service) RemoveContainer(ctx context.Context, vmID string, req *agentgrpc.RemoveContainerRequest) error {
	return s.callAgent(ctx, vmID, func(ctx context.Context) error {
		return s.agentClient.RemoveContainer(ctx, vmID, req)
	})
}

func (s *service) ExecProcess(ctx context.Context, vmID string, req *agentgrpc.ExecProcessRequest) error {
	return s.callAgent(ctx, vmID, func(ctx context.Context) error {
		return s.agentClient.ExecProcess(ctx, vmID, req)
	})
}

func (s *service) SignalProcess(ctx context.Context, vmID string, req *agentgrpc.SignalProcessRequest) error {
	return s.callAgent(ctx, vmID, func(ctx context.Context) error {
		return s.agentClient.SignalProcess(ctx, vmID, req)
	})
}

func (s *service) WaitProcess(ctx context.Context, vmID string, req *agentgrpc.WaitProcessRequest) (*agentgrpc.WaitProcessResponse, error) {
	if err := requireVMID(vmID); err != nil {
		return nil, err
	}
	return s.agentClient.WaitProcess(ctx, vmID, req)
}

func (s *service) UpdateContainer(ctx context.Context, vmID string, req *agentgrpc.UpdateContainerRequest) error {
	return s.callAgent(ctx, vmID, func(ctx context.Context) error {
		return s.agentClient.UpdateContainer(ctx, vmID, req)
	})
}

func (s *service) UpdateEphemeralMounts(ctx context.Context, vmID string, req *agentgrpc.UpdateEphemeralMountsRequest) error {
	return s.callAgent(ctx, vmID, func(ctx context.Context) error {
		return s.agentClient.UpdateEphemeralMounts(ctx, vmID, req)
	})
}

func (s *service) StatsContainer(ctx context.Context, vmID string, req *agentgrpc.StatsContainerRequest) (*agentgrpc.StatsContainerResponse, error) {
	if err := requireVMID(vmID); err != nil {
		return nil, err
	}
	return s.agentClient.StatsContainer(ctx, vmID, req)
}

func (s *service) PauseContainer(ctx context.Context, vmID string, req *agentgrpc.PauseContainerRequest) error {
	return s.callAgent(ctx, vmID, func(ctx context.Context) error {
		return s.agentClient.PauseContainer(ctx, vmID, req)
	})
}

func (s *service) ResumeContainer(ctx context.Context, vmID string, req *agentgrpc.ResumeContainerRequest) error {
	return s.callAgent(ctx, vmID, func(ctx context.Context) error {
		return s.agentClient.ResumeContainer(ctx, vmID, req)
	})
}

func (s *service) WriteStdin(ctx context.Context, vmID string, req *agentgrpc.WriteStreamRequest) (*agentgrpc.WriteStreamResponse, error) {
	if err := requireVMID(vmID); err != nil {
		return nil, err
	}
	return s.agentClient.WriteStdin(ctx, vmID, req)
}

func (s *service) ReadStdout(ctx context.Context, vmID string, req *agentgrpc.ReadStreamRequest) (*agentgrpc.ReadStreamResponse, error) {
	if err := requireVMID(vmID); err != nil {
		return nil, err
	}
	return s.agentClient.ReadStdout(ctx, vmID, req)
}

func (s *service) ReadStderr(ctx context.Context, vmID string, req *agentgrpc.ReadStreamRequest) (*agentgrpc.ReadStreamResponse, error) {
	if err := requireVMID(vmID); err != nil {
		return nil, err
	}
	return s.agentClient.ReadStderr(ctx, vmID, req)
}

func (s *service) CloseStdin(ctx context.Context, vmID string, req *agentgrpc.CloseStdinRequest) error {
	return s.callAgent(ctx, vmID, func(ctx context.Context) error {
		return s.agentClient.CloseStdin(ctx, vmID, req)
	})
}

func (s *service) TtyWinResize(ctx context.Context, vmID string, req *agentgrpc.TtyWinResizeRequest) error {
	return s.callAgent(ctx, vmID, func(ctx context.Context) error {
		return s.agentClient.TtyWinResize(ctx, vmID, req)
	})
}

func (s *service) UpdateInterface(ctx context.Context, vmID string, req *agentgrpc.UpdateInterfaceRequest) (*agenttypes.Interface, error) {
	if err := requireVMID(vmID); err != nil {
		return nil, err
	}
	return s.agentClient.UpdateInterface(ctx, vmID, req)
}

func (s *service) UpdateRoutes(ctx context.Context, vmID string, req *agentgrpc.UpdateRoutesRequest) (*agentgrpc.Routes, error) {
	if err := requireVMID(vmID); err != nil {
		return nil, err
	}
	return s.agentClient.UpdateRoutes(ctx, vmID, req)
}

func (s *service) ListInterfaces(ctx context.Context, vmID string, req *agentgrpc.ListInterfacesRequest) (*agentgrpc.Interfaces, error) {
	if err := requireVMID(vmID); err != nil {
		return nil, err
	}
	return s.agentClient.ListInterfaces(ctx, vmID, req)
}

func (s *service) ListRoutes(ctx context.Context, vmID string, req *agentgrpc.ListRoutesRequest) (*agentgrpc.Routes, error) {
	if err := requireVMID(vmID); err != nil {
		return nil, err
	}
	return s.agentClient.ListRoutes(ctx, vmID, req)
}

func (s *service) AddARPNeighbors(ctx context.Context, vmID string, req *agentgrpc.AddARPNeighborsRequest) error {
	return s.callAgent(ctx, vmID, func(ctx context.Context) error {
		return s.agentClient.AddARPNeighbors(ctx, vmID, req)
	})
}

func (s *service) GetIPTables(ctx context.Context, vmID string, req *agentgrpc.GetIPTablesRequest) (*agentgrpc.GetIPTablesResponse, error) {
	if err := requireVMID(vmID); err != nil {
		return nil, err
	}
	return s.agentClient.GetIPTables(ctx, vmID, req)
}

func (s *service) SetIPTables(ctx context.Context, vmID string, req *agentgrpc.SetIPTablesRequest) (*agentgrpc.SetIPTablesResponse, error) {
	if err := requireVMID(vmID); err != nil {
		return nil, err
	}
	return s.agentClient.SetIPTables(ctx, vmID, req)
}

func (s *service) GetMetrics(ctx context.Context, vmID string, req *agentgrpc.GetMetricsRequest) (*agentgrpc.Metrics, error) {
	if err := requireVMID(vmID); err != nil {
		return nil, err
	}
	return s.agentClient.GetMetrics(ctx, vmID, req)
}

func (s *service) MemAgentMemcgSet(ctx context.Context, vmID string, req *agentgrpc.MemAgentMemcgConfig) error {
	return s.callAgent(ctx, vmID, func(ctx context.Context) error {
		return s.agentClient.MemAgentMemcgSet(ctx, vmID, req)
	})
}

func (s *service) MemAgentCompactSet(ctx context.Context, vmID string, req *agentgrpc.MemAgentCompactConfig) error {
	return s.callAgent(ctx, vmID, func(ctx context.Context) error {
		return s.agentClient.MemAgentCompactSet(ctx, vmID, req)
	})
}

func (s *service) SetGuestDateTime(ctx context.Context, vmID string, req *agentgrpc.SetGuestDateTimeRequest) error {
	return s.callAgent(ctx, vmID, func(ctx context.Context) error {
		return s.agentClient.SetGuestDateTime(ctx, vmID, req)
	})
}

func (s *service) CopyFile(ctx context.Context, vmID string, req *agentgrpc.CopyFileRequest) error {
	return s.callAgent(ctx, vmID, func(ctx context.Context) error {
		return s.agentClient.CopyFile(ctx, vmID, req)
	})
}

func (s *service) GetVolumeStats(ctx context.Context, vmID string, req *agentgrpc.VolumeStatsRequest) (*agentgrpc.VolumeStatsResponse, error) {
	if err := requireVMID(vmID); err != nil {
		return nil, err
	}
	return s.agentClient.GetVolumeStats(ctx, vmID, req)
}

func (s *service) ResizeVolume(ctx context.Context, vmID string, req *agentgrpc.ResizeVolumeRequest) error {
	return s.callAgent(ctx, vmID, func(ctx context.Context) error {
		return s.agentClient.ResizeVolume(ctx, vmID, req)
	})
}

func (s *service) callAgent(ctx context.Context, vmID string, fn func(context.Context) error) error {
	if err := requireVMID(vmID); err != nil {
		return err
	}
	return fn(ctx)
}

func requireVMID(vmID string) error {
	if vmID == "" {
		return fmt.Errorf("%w: vm id is required", apierrors.ErrInvalidArgument)
	}
	return nil
}

func (s *service) execTimeout(override time.Duration) time.Duration {
	if override > 0 {
		return override
	}
	if s.config.ExecTimeout > 0 {
		return s.config.ExecTimeout
	}
	return 30 * time.Second
}

func (s *service) StreamReadStdout(ctx context.Context, req StreamReadRequest, send func(StreamChunk) error) error {
	return s.streamReadNative(ctx, req, send, true)
}

func (s *service) StreamReadStderr(ctx context.Context, req StreamReadRequest, send func(StreamChunk) error) error {
	return s.streamReadNative(ctx, req, send, false)
}

// streamReadNative uses the kata-agent's native streaming API for efficient output reading.
func (s *service) streamReadNative(ctx context.Context, req StreamReadRequest, send func(StreamChunk) error, stdout bool) error {
	if err := requireVMID(req.VMID); err != nil {
		return err
	}

	containerID := req.ContainerID
	if containerID == "" {
		containerID = req.VMID
	}

	streamReq := &agentgrpc.StreamRequest{
		ContainerId: containerID,
		ExecId:      req.ExecID,
	}

	var ch <-chan agent.StreamChunk
	var err error
	if stdout {
		ch, err = s.agentClient.StreamStdout(ctx, req.VMID, streamReq)
	} else {
		ch, err = s.agentClient.StreamStderr(ctx, req.VMID, streamReq)
	}
	if err != nil {
		return fmt.Errorf("start stream: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case chunk, ok := <-ch:
			if !ok {
				// Channel closed without EOF marker
				return send(StreamChunk{EOF: true})
			}
			if chunk.EOF {
				return send(StreamChunk{EOF: true})
			}
			if len(chunk.Data) > 0 {
				if err := send(StreamChunk{Data: chunk.Data}); err != nil {
					return err
				}
			}
		}
	}
}
