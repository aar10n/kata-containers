package service

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/agent"
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
	NodeForVM(vmID string) (string, bool)
	SandboxAgentAddressForNode(nodeName string) (string, bool)
	Exec(ctx context.Context, req ExecRequest) (ExecResponse, error)
	ListVMs(nodeName string) []VMInfo
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
}

type VMInfo struct {
	VMID     string
	NodeName string
}

type service struct {
	config      Config
	store       *k8s.Store
	agentClient *agent.Client
	shimClient  *shim_mgmt.Client
}

func New(cfg Config, store *k8s.Store, agentClient *agent.Client, shimClient *shim_mgmt.Client) Service {
	if shimClient == nil {
		shimClient = shim_mgmt.New(shim_mgmt.Config{})
	}
	return &service{
		config:      cfg,
		store:       store,
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

func (s *service) Exec(ctx context.Context, req ExecRequest) (ExecResponse, error) {
	if req.VMID == "" {
		return ExecResponse{}, errors.New("vm id is required")
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

func (s *service) ListVMs(nodeName string) []VMInfo {
	entries := s.store.ListVMs(nodeName)
	if len(entries) == 0 {
		return nil
	}

	vms := make([]VMInfo, 0, len(entries))
	for vmID, node := range entries {
		vms = append(vms, VMInfo{
			VMID:     vmID,
			NodeName: node,
		})
	}
	sort.Slice(vms, func(i, j int) bool {
		if vms[i].NodeName == vms[j].NodeName {
			return vms[i].VMID < vms[j].VMID
		}
		return vms[i].NodeName < vms[j].NodeName
	})

	return vms
}

func (s *service) SaveVMState(ctx context.Context, vmID, statePath string) error {
	if err := requireVMID(vmID); err != nil {
		return err
	}
	if strings.TrimSpace(statePath) == "" {
		return errors.New("state path is required")
	}
	return s.shimClient.SaveVMState(ctx, vmID, statePath)
}

func (s *service) RestoreVMState(ctx context.Context, vmID, statePath string) error {
	if err := requireVMID(vmID); err != nil {
		return err
	}
	if strings.TrimSpace(statePath) == "" {
		return errors.New("state path is required")
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
		return errors.New("vm id is required")
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
