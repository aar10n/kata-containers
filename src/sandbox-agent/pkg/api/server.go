package api

import (
	"context"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/api/pb"
	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/service"
	agenttypes "github.com/kata-containers/kata-containers/src/runtime/virtcontainers/pkg/agent/protocols"
	agentgrpc "github.com/kata-containers/kata-containers/src/runtime/virtcontainers/pkg/agent/protocols/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Config struct {
	HTTPAddr    string
	GRPCAddr    string
	NodeName    string
	DialTimeout time.Duration
}

type Server struct {
	pb.UnimplementedSandboxAgentServer
	cfg      Config
	svc      service.Service
	grpcPort int
	httpPort int
}

func NewServer(cfg Config, svc service.Service) *Server {
	return &Server{
		cfg:      cfg,
		svc:      svc,
		grpcPort: parseListenPort(cfg.GRPCAddr),
		httpPort: parseListenPort(cfg.HTTPAddr),
	}
}

func (s *Server) Register(grpcServer *grpc.Server) {
	pb.RegisterSandboxAgentServer(grpcServer, s)
}

func (s *Server) Health(ctx context.Context, _ *emptypb.Empty) (*pb.HealthResponse, error) {
	return &pb.HealthResponse{Status: "ok"}, nil
}

func (s *Server) ListVMs(ctx context.Context, req *pb.ListVMsRequest) (*pb.ListVMsResponse, error) {
	node := ""
	if req != nil {
		node = strings.TrimSpace(req.Node)
	}

	vms := s.svc.ListVMs(node)
	resp := &pb.ListVMsResponse{Vms: make([]*pb.VMInfo, 0, len(vms))}
	for _, vm := range vms {
		resp.Vms = append(resp.Vms, &pb.VMInfo{
			VmId: vm.VMID,
			Node: vm.NodeName,
		})
	}

	return resp, nil
}

func (s *Server) Exec(ctx context.Context, req *pb.ExecRequest) (*pb.ExecResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	return callUnary(ctx, s, req.GetVmId(), func(ctx context.Context) (*pb.ExecResponse, error) {
		resp, err := s.svc.Exec(ctx, service.ExecRequest{
			VMID:        req.GetVmId(),
			ContainerID: req.GetContainerId(),
			Args:        req.GetArgs(),
			Env:         req.GetEnv(),
			Cwd:         req.GetCwd(),
			Timeout:     time.Duration(req.GetTimeoutMs()) * time.Millisecond,
		})
		if err != nil {
			return nil, status.Errorf(codes.Internal, "exec failed: %v", err)
		}
		return &pb.ExecResponse{
			Stdout:   resp.Stdout,
			Stderr:   resp.Stderr,
			ExitCode: resp.ExitCode,
		}, nil
	}, func(ctx context.Context, client pb.SandboxAgentClient) (*pb.ExecResponse, error) {
		return client.Exec(ctx, req)
	})
}

func (s *Server) SaveVMState(ctx context.Context, req *pb.SaveVMStateRequest) (*emptypb.Empty, error) {
	return s.callEmpty(ctx, req.GetVmId(), func(ctx context.Context) error {
		return s.svc.SaveVMState(ctx, req.GetVmId(), req.GetPath())
	}, func(ctx context.Context, client pb.SandboxAgentClient) error {
		_, err := client.SaveVMState(ctx, req)
		return err
	})
}

func (s *Server) RestoreVMState(ctx context.Context, req *pb.RestoreVMStateRequest) (*emptypb.Empty, error) {
	return s.callEmpty(ctx, req.GetVmId(), func(ctx context.Context) error {
		return s.svc.RestoreVMState(ctx, req.GetVmId(), req.GetPath())
	}, func(ctx context.Context, client pb.SandboxAgentClient) error {
		_, err := client.RestoreVMState(ctx, req)
		return err
	})
}

func (s *Server) CreateContainer(ctx context.Context, req *pb.CreateContainerRequest) (*emptypb.Empty, error) {
	return s.callEmpty(ctx, req.GetVmId(), func(ctx context.Context) error {
		return s.svc.CreateContainer(ctx, req.GetVmId(), req.GetRequest())
	}, func(ctx context.Context, client pb.SandboxAgentClient) error {
		_, err := client.CreateContainer(ctx, req)
		return err
	})
}

func (s *Server) StartContainer(ctx context.Context, req *pb.StartContainerRequest) (*emptypb.Empty, error) {
	return s.callEmpty(ctx, req.GetVmId(), func(ctx context.Context) error {
		return s.svc.StartContainer(ctx, req.GetVmId(), req.GetRequest())
	}, func(ctx context.Context, client pb.SandboxAgentClient) error {
		_, err := client.StartContainer(ctx, req)
		return err
	})
}

func (s *Server) RemoveContainer(ctx context.Context, req *pb.RemoveContainerRequest) (*emptypb.Empty, error) {
	return s.callEmpty(ctx, req.GetVmId(), func(ctx context.Context) error {
		return s.svc.RemoveContainer(ctx, req.GetVmId(), req.GetRequest())
	}, func(ctx context.Context, client pb.SandboxAgentClient) error {
		_, err := client.RemoveContainer(ctx, req)
		return err
	})
}

func (s *Server) ExecProcess(ctx context.Context, req *pb.ExecProcessRequest) (*emptypb.Empty, error) {
	return s.callEmpty(ctx, req.GetVmId(), func(ctx context.Context) error {
		return s.svc.ExecProcess(ctx, req.GetVmId(), req.GetRequest())
	}, func(ctx context.Context, client pb.SandboxAgentClient) error {
		_, err := client.ExecProcess(ctx, req)
		return err
	})
}

func (s *Server) SignalProcess(ctx context.Context, req *pb.SignalProcessRequest) (*emptypb.Empty, error) {
	return s.callEmpty(ctx, req.GetVmId(), func(ctx context.Context) error {
		return s.svc.SignalProcess(ctx, req.GetVmId(), req.GetRequest())
	}, func(ctx context.Context, client pb.SandboxAgentClient) error {
		_, err := client.SignalProcess(ctx, req)
		return err
	})
}

func (s *Server) WaitProcess(ctx context.Context, req *pb.WaitProcessRequest) (*agentgrpc.WaitProcessResponse, error) {
	return callUnary(ctx, s, req.GetVmId(), func(ctx context.Context) (*agentgrpc.WaitProcessResponse, error) {
		resp, err := s.svc.WaitProcess(ctx, req.GetVmId(), req.GetRequest())
		if err != nil {
			return nil, err
		}
		return resp, nil
	}, func(ctx context.Context, client pb.SandboxAgentClient) (*agentgrpc.WaitProcessResponse, error) {
		return client.WaitProcess(ctx, req)
	})
}

func (s *Server) UpdateContainer(ctx context.Context, req *pb.UpdateContainerRequest) (*emptypb.Empty, error) {
	return s.callEmpty(ctx, req.GetVmId(), func(ctx context.Context) error {
		return s.svc.UpdateContainer(ctx, req.GetVmId(), req.GetRequest())
	}, func(ctx context.Context, client pb.SandboxAgentClient) error {
		_, err := client.UpdateContainer(ctx, req)
		return err
	})
}

func (s *Server) UpdateEphemeralMounts(ctx context.Context, req *pb.UpdateEphemeralMountsRequest) (*emptypb.Empty, error) {
	return s.callEmpty(ctx, req.GetVmId(), func(ctx context.Context) error {
		return s.svc.UpdateEphemeralMounts(ctx, req.GetVmId(), req.GetRequest())
	}, func(ctx context.Context, client pb.SandboxAgentClient) error {
		_, err := client.UpdateEphemeralMounts(ctx, req)
		return err
	})
}

func (s *Server) StatsContainer(ctx context.Context, req *pb.StatsContainerRequest) (*agentgrpc.StatsContainerResponse, error) {
	return callUnary(ctx, s, req.GetVmId(), func(ctx context.Context) (*agentgrpc.StatsContainerResponse, error) {
		resp, err := s.svc.StatsContainer(ctx, req.GetVmId(), req.GetRequest())
		if err != nil {
			return nil, err
		}
		return resp, nil
	}, func(ctx context.Context, client pb.SandboxAgentClient) (*agentgrpc.StatsContainerResponse, error) {
		return client.StatsContainer(ctx, req)
	})
}

func (s *Server) PauseContainer(ctx context.Context, req *pb.PauseContainerRequest) (*emptypb.Empty, error) {
	return s.callEmpty(ctx, req.GetVmId(), func(ctx context.Context) error {
		return s.svc.PauseContainer(ctx, req.GetVmId(), req.GetRequest())
	}, func(ctx context.Context, client pb.SandboxAgentClient) error {
		_, err := client.PauseContainer(ctx, req)
		return err
	})
}

func (s *Server) ResumeContainer(ctx context.Context, req *pb.ResumeContainerRequest) (*emptypb.Empty, error) {
	return s.callEmpty(ctx, req.GetVmId(), func(ctx context.Context) error {
		return s.svc.ResumeContainer(ctx, req.GetVmId(), req.GetRequest())
	}, func(ctx context.Context, client pb.SandboxAgentClient) error {
		_, err := client.ResumeContainer(ctx, req)
		return err
	})
}

func (s *Server) WriteStdin(ctx context.Context, req *pb.WriteStdinRequest) (*agentgrpc.WriteStreamResponse, error) {
	return callUnary(ctx, s, req.GetVmId(), func(ctx context.Context) (*agentgrpc.WriteStreamResponse, error) {
		resp, err := s.svc.WriteStdin(ctx, req.GetVmId(), req.GetRequest())
		if err != nil {
			return nil, err
		}
		return resp, nil
	}, func(ctx context.Context, client pb.SandboxAgentClient) (*agentgrpc.WriteStreamResponse, error) {
		return client.WriteStdin(ctx, req)
	})
}

func (s *Server) ReadStdout(ctx context.Context, req *pb.ReadStdoutRequest) (*agentgrpc.ReadStreamResponse, error) {
	return callUnary(ctx, s, req.GetVmId(), func(ctx context.Context) (*agentgrpc.ReadStreamResponse, error) {
		resp, err := s.svc.ReadStdout(ctx, req.GetVmId(), req.GetRequest())
		if err != nil {
			return nil, err
		}
		return resp, nil
	}, func(ctx context.Context, client pb.SandboxAgentClient) (*agentgrpc.ReadStreamResponse, error) {
		return client.ReadStdout(ctx, req)
	})
}

func (s *Server) ReadStderr(ctx context.Context, req *pb.ReadStderrRequest) (*agentgrpc.ReadStreamResponse, error) {
	return callUnary(ctx, s, req.GetVmId(), func(ctx context.Context) (*agentgrpc.ReadStreamResponse, error) {
		resp, err := s.svc.ReadStderr(ctx, req.GetVmId(), req.GetRequest())
		if err != nil {
			return nil, err
		}
		return resp, nil
	}, func(ctx context.Context, client pb.SandboxAgentClient) (*agentgrpc.ReadStreamResponse, error) {
		return client.ReadStderr(ctx, req)
	})
}

func (s *Server) CloseStdin(ctx context.Context, req *pb.CloseStdinRequest) (*emptypb.Empty, error) {
	return s.callEmpty(ctx, req.GetVmId(), func(ctx context.Context) error {
		return s.svc.CloseStdin(ctx, req.GetVmId(), req.GetRequest())
	}, func(ctx context.Context, client pb.SandboxAgentClient) error {
		_, err := client.CloseStdin(ctx, req)
		return err
	})
}

func (s *Server) TtyWinResize(ctx context.Context, req *pb.TtyWinResizeRequest) (*emptypb.Empty, error) {
	return s.callEmpty(ctx, req.GetVmId(), func(ctx context.Context) error {
		return s.svc.TtyWinResize(ctx, req.GetVmId(), req.GetRequest())
	}, func(ctx context.Context, client pb.SandboxAgentClient) error {
		_, err := client.TtyWinResize(ctx, req)
		return err
	})
}

func (s *Server) UpdateInterface(ctx context.Context, req *pb.UpdateInterfaceRequest) (*agenttypes.Interface, error) {
	return callUnary(ctx, s, req.GetVmId(), func(ctx context.Context) (*agenttypes.Interface, error) {
		resp, err := s.svc.UpdateInterface(ctx, req.GetVmId(), req.GetRequest())
		if err != nil {
			return nil, err
		}
		return resp, nil
	}, func(ctx context.Context, client pb.SandboxAgentClient) (*agenttypes.Interface, error) {
		return client.UpdateInterface(ctx, req)
	})
}

func (s *Server) UpdateRoutes(ctx context.Context, req *pb.UpdateRoutesRequest) (*agentgrpc.Routes, error) {
	return callUnary(ctx, s, req.GetVmId(), func(ctx context.Context) (*agentgrpc.Routes, error) {
		resp, err := s.svc.UpdateRoutes(ctx, req.GetVmId(), req.GetRequest())
		if err != nil {
			return nil, err
		}
		return resp, nil
	}, func(ctx context.Context, client pb.SandboxAgentClient) (*agentgrpc.Routes, error) {
		return client.UpdateRoutes(ctx, req)
	})
}

func (s *Server) ListInterfaces(ctx context.Context, req *pb.ListInterfacesRequest) (*agentgrpc.Interfaces, error) {
	return callUnary(ctx, s, req.GetVmId(), func(ctx context.Context) (*agentgrpc.Interfaces, error) {
		resp, err := s.svc.ListInterfaces(ctx, req.GetVmId(), &agentgrpc.ListInterfacesRequest{})
		if err != nil {
			return nil, err
		}
		return resp, nil
	}, func(ctx context.Context, client pb.SandboxAgentClient) (*agentgrpc.Interfaces, error) {
		return client.ListInterfaces(ctx, req)
	})
}

func (s *Server) ListRoutes(ctx context.Context, req *pb.ListRoutesRequest) (*agentgrpc.Routes, error) {
	return callUnary(ctx, s, req.GetVmId(), func(ctx context.Context) (*agentgrpc.Routes, error) {
		resp, err := s.svc.ListRoutes(ctx, req.GetVmId(), &agentgrpc.ListRoutesRequest{})
		if err != nil {
			return nil, err
		}
		return resp, nil
	}, func(ctx context.Context, client pb.SandboxAgentClient) (*agentgrpc.Routes, error) {
		return client.ListRoutes(ctx, req)
	})
}

func (s *Server) AddARPNeighbors(ctx context.Context, req *pb.AddARPNeighborsRequest) (*emptypb.Empty, error) {
	return s.callEmpty(ctx, req.GetVmId(), func(ctx context.Context) error {
		return s.svc.AddARPNeighbors(ctx, req.GetVmId(), req.GetRequest())
	}, func(ctx context.Context, client pb.SandboxAgentClient) error {
		_, err := client.AddARPNeighbors(ctx, req)
		return err
	})
}

func (s *Server) GetIPTables(ctx context.Context, req *pb.GetIPTablesRequest) (*agentgrpc.GetIPTablesResponse, error) {
	return callUnary(ctx, s, req.GetVmId(), func(ctx context.Context) (*agentgrpc.GetIPTablesResponse, error) {
		resp, err := s.svc.GetIPTables(ctx, req.GetVmId(), req.GetRequest())
		if err != nil {
			return nil, err
		}
		return resp, nil
	}, func(ctx context.Context, client pb.SandboxAgentClient) (*agentgrpc.GetIPTablesResponse, error) {
		return client.GetIPTables(ctx, req)
	})
}

func (s *Server) SetIPTables(ctx context.Context, req *pb.SetIPTablesRequest) (*agentgrpc.SetIPTablesResponse, error) {
	return callUnary(ctx, s, req.GetVmId(), func(ctx context.Context) (*agentgrpc.SetIPTablesResponse, error) {
		resp, err := s.svc.SetIPTables(ctx, req.GetVmId(), req.GetRequest())
		if err != nil {
			return nil, err
		}
		return resp, nil
	}, func(ctx context.Context, client pb.SandboxAgentClient) (*agentgrpc.SetIPTablesResponse, error) {
		return client.SetIPTables(ctx, req)
	})
}

func (s *Server) GetMetrics(ctx context.Context, req *pb.GetMetricsRequest) (*agentgrpc.Metrics, error) {
	return callUnary(ctx, s, req.GetVmId(), func(ctx context.Context) (*agentgrpc.Metrics, error) {
		resp, err := s.svc.GetMetrics(ctx, req.GetVmId(), &agentgrpc.GetMetricsRequest{})
		if err != nil {
			return nil, err
		}
		return resp, nil
	}, func(ctx context.Context, client pb.SandboxAgentClient) (*agentgrpc.Metrics, error) {
		return client.GetMetrics(ctx, req)
	})
}

func (s *Server) MemAgentMemcgSet(ctx context.Context, req *pb.MemAgentMemcgSetRequest) (*emptypb.Empty, error) {
	return s.callEmpty(ctx, req.GetVmId(), func(ctx context.Context) error {
		return s.svc.MemAgentMemcgSet(ctx, req.GetVmId(), req.GetRequest())
	}, func(ctx context.Context, client pb.SandboxAgentClient) error {
		_, err := client.MemAgentMemcgSet(ctx, req)
		return err
	})
}

func (s *Server) MemAgentCompactSet(ctx context.Context, req *pb.MemAgentCompactSetRequest) (*emptypb.Empty, error) {
	return s.callEmpty(ctx, req.GetVmId(), func(ctx context.Context) error {
		return s.svc.MemAgentCompactSet(ctx, req.GetVmId(), req.GetRequest())
	}, func(ctx context.Context, client pb.SandboxAgentClient) error {
		_, err := client.MemAgentCompactSet(ctx, req)
		return err
	})
}

func (s *Server) SetGuestDateTime(ctx context.Context, req *pb.SetGuestDateTimeRequest) (*emptypb.Empty, error) {
	return s.callEmpty(ctx, req.GetVmId(), func(ctx context.Context) error {
		return s.svc.SetGuestDateTime(ctx, req.GetVmId(), req.GetRequest())
	}, func(ctx context.Context, client pb.SandboxAgentClient) error {
		_, err := client.SetGuestDateTime(ctx, req)
		return err
	})
}

func (s *Server) CopyFile(ctx context.Context, req *pb.CopyFileRequest) (*emptypb.Empty, error) {
	return s.callEmpty(ctx, req.GetVmId(), func(ctx context.Context) error {
		return s.svc.CopyFile(ctx, req.GetVmId(), req.GetRequest())
	}, func(ctx context.Context, client pb.SandboxAgentClient) error {
		_, err := client.CopyFile(ctx, req)
		return err
	})
}

func (s *Server) GetVolumeStats(ctx context.Context, req *pb.GetVolumeStatsRequest) (*agentgrpc.VolumeStatsResponse, error) {
	return callUnary(ctx, s, req.GetVmId(), func(ctx context.Context) (*agentgrpc.VolumeStatsResponse, error) {
		resp, err := s.svc.GetVolumeStats(ctx, req.GetVmId(), req.GetRequest())
		if err != nil {
			return nil, err
		}
		return resp, nil
	}, func(ctx context.Context, client pb.SandboxAgentClient) (*agentgrpc.VolumeStatsResponse, error) {
		return client.GetVolumeStats(ctx, req)
	})
}

func (s *Server) ResizeVolume(ctx context.Context, req *pb.ResizeVolumeRequest) (*emptypb.Empty, error) {
	return s.callEmpty(ctx, req.GetVmId(), func(ctx context.Context) error {
		return s.svc.ResizeVolume(ctx, req.GetVmId(), req.GetRequest())
	}, func(ctx context.Context, client pb.SandboxAgentClient) error {
		_, err := client.ResizeVolume(ctx, req)
		return err
	})
}

func (s *Server) callEmpty(ctx context.Context, vmID string, local func(context.Context) error, remote func(context.Context, pb.SandboxAgentClient) error) (*emptypb.Empty, error) {
	_, err := call(ctx, s, vmID, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, local(ctx)
	}, func(ctx context.Context, client pb.SandboxAgentClient) (struct{}, error) {
		return struct{}{}, remote(ctx, client)
	})
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func callUnary[T any](ctx context.Context, s *Server, vmID string, local func(context.Context) (T, error), remote func(context.Context, pb.SandboxAgentClient) (T, error)) (T, error) {
	return call(ctx, s, vmID, local, remote)
}

func call[T any](ctx context.Context, s *Server, vmID string, local func(context.Context) (T, error), remote func(context.Context, pb.SandboxAgentClient) (T, error)) (T, error) {
	var zero T
	if strings.TrimSpace(vmID) == "" {
		return zero, status.Error(codes.InvalidArgument, "vm_id is required")
	}

	nodeName, ok := s.svc.NodeForVM(vmID)
	if !ok {
		return zero, status.Error(codes.NotFound, "vm id not found")
	}

	if nodeName == s.cfg.NodeName {
		return local(ctx)
	}

	if shouldRedirect(ctx) {
		addr, ok := s.svc.SandboxAgentAddressForNode(nodeName)
		if !ok {
			return zero, status.Error(codes.Unavailable, "target node address unavailable")
		}
		return zero, redirectError(addr, s.httpPort)
	}

	client, conn, err := s.remoteClient(ctx, nodeName)
	if err != nil {
		return zero, err
	}
	defer conn.Close()

	return remote(ctx, client)
}

func (s *Server) remoteClient(ctx context.Context, nodeName string) (pb.SandboxAgentClient, *grpc.ClientConn, error) {
	addr, ok := s.svc.SandboxAgentAddressForNode(nodeName)
	if !ok {
		return nil, nil, status.Error(codes.Unavailable, "target node address unavailable")
	}

	target := addr
	if s.grpcPort != 0 {
		target = net.JoinHostPort(addr, strconv.Itoa(s.grpcPort))
	}

	dialCtx := ctx
	if s.cfg.DialTimeout > 0 {
		var cancel context.CancelFunc
		dialCtx, cancel = context.WithTimeout(ctx, s.cfg.DialTimeout)
		defer cancel()
	}

	conn, err := grpc.DialContext(dialCtx, target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, status.Errorf(codes.Unavailable, "dial %s: %v", target, err)
	}

	return pb.NewSandboxAgentClient(conn), conn, nil
}

func parseListenPort(listenAddr string) int {
	_, portStr, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return 0
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 0
	}

	return port
}
