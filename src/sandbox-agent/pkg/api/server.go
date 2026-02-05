package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/api/pb"
	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/backend"
	apierrors "github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/errors"
	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/k8s"
	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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
	resp := &pb.HealthResponse{
		Status: "ok",
		Mode:   s.svc.Mode(),
	}

	// Include local capacity info if available
	if cap := s.svc.GetCapacity(ctx); cap != nil {
		resp.Capacity = &pb.NodeCapacity{
			NodeName:         cap.NodeName,
			MaxSandboxes:     cap.MaxSandboxes,
			CurrentSandboxes: cap.CurrentSandboxes,
			CalculatedAtUnix: cap.CalculatedAt.Unix(),
		}
	}

	// Note: Cluster capacity is now aggregated by sandbox-service, not agents

	return resp, nil
}

// Sandbox lifecycle

func (s *Server) CreateSandbox(ctx context.Context, req *pb.CreateSandboxRequest) (*pb.SandboxInfo, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	info, err := s.svc.CreateSandbox(ctx, service.CreateSandboxRequest{
		SessionID:   req.GetSessionId(),
		Image:       req.GetImage(),
		Command:     req.GetCommand(),
		Env:         req.GetEnv(),
		Labels:      req.GetLabels(),
		DownloadURL: req.GetDownloadUrl(),
		UserID:      req.GetUserId(),
		Features:    req.GetFeatures(),
	})
	if err != nil {
		if errors.Is(err, k8s.ErrSandboxAlreadyExists) {
			return nil, status.Error(codes.AlreadyExists, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "create sandbox: %v", err)
	}

	return sandboxInfoToProto(info), nil
}

func (s *Server) GetSandbox(ctx context.Context, req *pb.GetSandboxRequest) (*pb.SandboxInfo, error) {
	if req == nil || strings.TrimSpace(req.GetSessionId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}

	info, err := s.svc.GetSandbox(ctx, req.GetSessionId())
	if err != nil {
		if errors.Is(err, k8s.ErrSandboxNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "get sandbox: %v", err)
	}

	return sandboxInfoToProto(info), nil
}

func (s *Server) DeleteSandbox(ctx context.Context, req *pb.DeleteSandboxRequest) (*emptypb.Empty, error) {
	if req == nil || strings.TrimSpace(req.GetSessionId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}

	if err := s.svc.DeleteSandbox(ctx, req.GetSessionId()); err != nil {
		if errors.Is(err, k8s.ErrSandboxNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "delete sandbox: %v", err)
	}

	return &emptypb.Empty{}, nil
}

func (s *Server) ListSandboxes(ctx context.Context, req *pb.ListSandboxesRequest) (*pb.ListSandboxesResponse, error) {
	node := ""
	if req != nil {
		node = strings.TrimSpace(req.GetNode())
	}

	sandboxes := s.svc.ListSandboxes(ctx, node)
	resp := &pb.ListSandboxesResponse{
		Sandboxes: make([]*pb.SandboxInfo, 0, len(sandboxes)),
	}
	for _, sb := range sandboxes {
		resp.Sandboxes = append(resp.Sandboxes, sandboxInfoToProto(sb))
	}

	return resp, nil
}

func (s *Server) UpdateSandboxActivity(ctx context.Context, req *pb.UpdateSandboxActivityRequest) (*emptypb.Empty, error) {
	if req == nil || strings.TrimSpace(req.GetSessionId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}

	if err := s.svc.UpdateSandboxActivity(ctx, req.GetSessionId()); err != nil {
		if errors.Is(err, k8s.ErrSandboxNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "update activity: %v", err)
	}

	return &emptypb.Empty{}, nil
}

func (s *Server) SuspendSandbox(ctx context.Context, req *pb.SuspendSandboxRequest) (*pb.SuspendSandboxResponse, error) {
	if req == nil || strings.TrimSpace(req.GetSessionId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}

	result, err := s.svc.SuspendSandbox(ctx, req.GetSessionId())
	if err != nil {
		if errors.Is(err, k8s.ErrSandboxNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		if errors.Is(err, apierrors.ErrNotSupported) {
			return nil, status.Error(codes.Unimplemented, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "suspend sandbox: %v", err)
	}

	return &pb.SuspendSandboxResponse{
		SnapshotSize: result.SnapshotSize,
		DurationMs:   result.Duration.Milliseconds(),
	}, nil
}

func sandboxInfoToProto(info *k8s.SandboxInfo) *pb.SandboxInfo {
	if info == nil {
		return nil
	}

	containers := make([]*pb.ContainerInfo, 0, len(info.Containers))
	for _, c := range info.Containers {
		containers = append(containers, &pb.ContainerInfo{
			Name:        c.Name,
			ContainerId: c.ContainerID,
		})
	}

	return &pb.SandboxInfo{
		SessionId:      info.SessionID,
		SandboxId:      info.SandboxID,
		Containers:     containers,
		Status:         string(info.Status),
		Node:           info.Node,
		CreatedAtUnix:  info.CreatedAt.Unix(),
		LastUsedAtUnix: info.LastUsedAt.Unix(),
		Labels:         info.Labels,
	}
}

// Command execution

func (s *Server) Exec(ctx context.Context, req *pb.ExecRequest) (*pb.ExecResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	sessionID := req.GetSessionId()
	if sessionID == "" {
		// Fallback for backwards compatibility during migration
		sessionID = req.GetVmId()
	}
	if sessionID == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}

	return callUnary(ctx, s, sessionID, func(ctx context.Context) (*pb.ExecResponse, error) {
		resp, err := s.svc.Exec(ctx, service.ExecRequest{
			SessionID: sessionID,
			Args:      req.GetArgs(),
			Env:       req.GetEnv(),
			Cwd:       req.GetCwd(),
			Timeout:   time.Duration(req.GetTimeoutMs()) * time.Millisecond,
		})
		if err != nil {
			return nil, toGRPCError(err)
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

// Process management

func (s *Server) StartProcess(ctx context.Context, req *pb.StartProcessRequest) (*pb.StartProcessResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	sessionID := req.GetSessionId()
	if sessionID == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}

	return callUnary(ctx, s, sessionID, func(ctx context.Context) (*pb.StartProcessResponse, error) {
		proc, err := s.svc.StartProcess(ctx, service.StartProcessRequest{
			SessionID: sessionID,
			Command:   req.GetCommand(),
			Env:       req.GetEnv(),
			Cwd:       req.GetCwd(),
			TTY:       req.GetTty(),
		})
		if err != nil {
			return nil, toGRPCError(err)
		}
		return &pb.StartProcessResponse{
			ProcessId:   proc.ProcessID,
			ContainerId: proc.ContainerID,
		}, nil
	}, func(ctx context.Context, client pb.SandboxAgentClient) (*pb.StartProcessResponse, error) {
		return client.StartProcess(ctx, req)
	})
}

func (s *Server) WriteToProcess(ctx context.Context, req *pb.WriteToProcessRequest) (*emptypb.Empty, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	sessionID := req.GetSessionId()
	if sessionID == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}

	return s.callEmpty(ctx, sessionID, func(ctx context.Context) error {
		return s.svc.WriteToProcess(ctx, sessionID, req.GetProcessId(), req.GetData())
	}, func(ctx context.Context, client pb.SandboxAgentClient) error {
		_, err := client.WriteToProcess(ctx, req)
		return err
	})
}

func (s *Server) ReadProcessStdout(ctx context.Context, req *pb.ReadProcessOutputRequest) (*pb.ReadProcessOutputResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	sessionID := req.GetSessionId()
	if sessionID == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}

	return callUnary(ctx, s, sessionID, func(ctx context.Context) (*pb.ReadProcessOutputResponse, error) {
		data, err := s.svc.ReadStdout(ctx, sessionID, req.GetProcessId(), int(req.GetMaxBytes()))
		if err != nil {
			return nil, toGRPCError(err)
		}
		return &pb.ReadProcessOutputResponse{Data: data}, nil
	}, func(ctx context.Context, client pb.SandboxAgentClient) (*pb.ReadProcessOutputResponse, error) {
		return client.ReadProcessStdout(ctx, req)
	})
}

func (s *Server) ReadProcessStderr(ctx context.Context, req *pb.ReadProcessOutputRequest) (*pb.ReadProcessOutputResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	sessionID := req.GetSessionId()
	if sessionID == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}

	return callUnary(ctx, s, sessionID, func(ctx context.Context) (*pb.ReadProcessOutputResponse, error) {
		data, err := s.svc.ReadStderr(ctx, sessionID, req.GetProcessId(), int(req.GetMaxBytes()))
		if err != nil {
			return nil, toGRPCError(err)
		}
		return &pb.ReadProcessOutputResponse{Data: data}, nil
	}, func(ctx context.Context, client pb.SandboxAgentClient) (*pb.ReadProcessOutputResponse, error) {
		return client.ReadProcessStderr(ctx, req)
	})
}

func (s *Server) CloseProcessStdin(ctx context.Context, req *pb.CloseProcessStdinRequest) (*emptypb.Empty, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	sessionID := req.GetSessionId()
	if sessionID == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}

	return s.callEmpty(ctx, sessionID, func(ctx context.Context) error {
		return s.svc.CloseStdin(ctx, sessionID, req.GetProcessId())
	}, func(ctx context.Context, client pb.SandboxAgentClient) error {
		_, err := client.CloseProcessStdin(ctx, req)
		return err
	})
}

func (s *Server) KillProcess(ctx context.Context, req *pb.KillProcessRequest) (*emptypb.Empty, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	sessionID := req.GetSessionId()
	if sessionID == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}

	return s.callEmpty(ctx, sessionID, func(ctx context.Context) error {
		return s.svc.KillProcess(ctx, sessionID, req.GetProcessId(), int(req.GetSignal()))
	}, func(ctx context.Context, client pb.SandboxAgentClient) error {
		_, err := client.KillProcess(ctx, req)
		return err
	})
}

func (s *Server) WaitProcess(ctx context.Context, req *pb.WaitProcessRequest) (*pb.WaitProcessResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	sessionID := req.GetSessionId()
	if sessionID == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}

	return callUnary(ctx, s, sessionID, func(ctx context.Context) (*pb.WaitProcessResponse, error) {
		exitCode, err := s.svc.WaitProcess(ctx, sessionID, req.GetProcessId())
		if err != nil {
			return nil, toGRPCError(err)
		}
		return &pb.WaitProcessResponse{ExitCode: exitCode}, nil
	}, func(ctx context.Context, client pb.SandboxAgentClient) (*pb.WaitProcessResponse, error) {
		return client.WaitProcess(ctx, req)
	})
}

func (s *Server) ResizeTerminal(ctx context.Context, req *pb.ResizeTerminalRequest) (*emptypb.Empty, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	sessionID := req.GetSessionId()
	if sessionID == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}

	return s.callEmpty(ctx, sessionID, func(ctx context.Context) error {
		return s.svc.ResizeTerminal(ctx, sessionID, req.GetProcessId(), req.GetRows(), req.GetCols())
	}, func(ctx context.Context, client pb.SandboxAgentClient) error {
		_, err := client.ResizeTerminal(ctx, req)
		return err
	})
}

// StreamProcessOutput streams stdout and stderr from a process.
// This is a server-side streaming RPC - it does not support cross-node forwarding.
// The client should connect directly to the correct node's sandbox-agent.
func (s *Server) StreamProcessOutput(req *pb.StreamProcessOutputRequest, stream pb.SandboxAgent_StreamProcessOutputServer) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "request is required")
	}

	sessionID := req.GetSessionId()
	if sessionID == "" {
		return status.Error(codes.InvalidArgument, "session_id is required")
	}

	processID := req.GetProcessId()
	if processID == "" {
		return status.Error(codes.InvalidArgument, "process_id is required")
	}

	slog.Info("StreamProcessOutput: starting", "session_id", sessionID, "process_id", processID)

	// Check if sandbox is local
	info, err := s.svc.GetSandbox(stream.Context(), sessionID)
	if err != nil {
		return toGRPCError(err)
	}
	if info.Node != "" && info.Node != s.cfg.NodeName {
		// Not local - return error with redirect info
		addr, ok := s.svc.SandboxAgentAddressForNode(info.Node)
		if !ok {
			return status.Error(codes.Unavailable, "target node address unavailable")
		}
		return redirectError(addr, s.grpcPort)
	}

	// Start streaming from backend
	outCh, err := s.svc.StreamOutput(stream.Context(), sessionID, processID)
	if err != nil {
		slog.Error("StreamProcessOutput: failed to start", "session_id", sessionID, "error", err)
		return toGRPCError(err)
	}
	slog.Info("StreamProcessOutput: stream started, waiting for data", "session_id", sessionID)

	// Stream chunks to client
	chunkNum := 0
	for chunk := range outCh {
		chunkNum++
		if chunk.EOF {
			slog.Info("StreamProcessOutput: EOF", "session_id", sessionID, "chunks_sent", chunkNum)
			return nil
		}
		pbStream := pb.ProcessOutputChunk_STDOUT
		if chunk.Stream == backend.StreamStderr {
			pbStream = pb.ProcessOutputChunk_STDERR
		}
		slog.Debug("StreamProcessOutput: sending chunk", "session_id", sessionID, "chunk_num", chunkNum, "bytes", len(chunk.Data))
		if err := stream.Send(&pb.ProcessOutputChunk{
			Stream: pbStream,
			Data:   chunk.Data,
		}); err != nil {
			slog.Error("StreamProcessOutput: send failed", "session_id", sessionID, "error", err)
			return err
		}
	}

	slog.Info("StreamProcessOutput: channel closed", "session_id", sessionID, "chunks_sent", chunkNum)
	return nil
}

// File operations

func (s *Server) ReadFile(ctx context.Context, req *pb.ReadFileRequest) (*pb.ReadFileResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	sessionID := req.GetSessionId()
	if sessionID == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}

	return callUnary(ctx, s, sessionID, func(ctx context.Context) (*pb.ReadFileResponse, error) {
		content, err := s.svc.ReadFile(ctx, sessionID, req.GetPath())
		if err != nil {
			return nil, toGRPCError(err)
		}
		return &pb.ReadFileResponse{Content: content}, nil
	}, func(ctx context.Context, client pb.SandboxAgentClient) (*pb.ReadFileResponse, error) {
		return client.ReadFile(ctx, req)
	})
}

func (s *Server) WriteFile(ctx context.Context, req *pb.WriteFileRequest) (*emptypb.Empty, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	sessionID := req.GetSessionId()
	if sessionID == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}

	return s.callEmpty(ctx, sessionID, func(ctx context.Context) error {
		return s.svc.WriteFile(ctx, sessionID, req.GetPath(), req.GetContent(), req.GetMode())
	}, func(ctx context.Context, client pb.SandboxAgentClient) error {
		_, err := client.WriteFile(ctx, req)
		return err
	})
}

func (s *Server) ReadArchive(req *pb.ReadArchiveRequest, stream pb.SandboxAgent_ReadArchiveServer) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "request is required")
	}

	sessionID := req.GetSessionId()
	if sessionID == "" {
		return status.Error(codes.InvalidArgument, "session_id is required")
	}

	ctx := stream.Context()

	// Check if local
	info, err := s.svc.GetSandbox(ctx, sessionID)
	if err != nil {
		return toGRPCError(err)
	}

	if info.Node != "" && info.Node != s.cfg.NodeName {
		// Not local - return redirect error
		addr, ok := s.svc.SandboxAgentAddressForNode(info.Node)
		if !ok {
			return status.Error(codes.Unavailable, "target node address unavailable")
		}
		return redirectError(addr, s.grpcPort)
	}

	// Local execution
	reader, err := s.svc.ReadArchive(ctx, sessionID, req.GetPath())
	if err != nil {
		return toGRPCError(err)
	}
	defer reader.Close()

	buf := make([]byte, 32*1024)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			if err := stream.Send(&pb.ArchiveChunk{Data: buf[:n]}); err != nil {
				return err
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return status.Errorf(codes.Internal, "read archive: %v", err)
		}
	}
}

func (s *Server) WriteArchive(stream pb.SandboxAgent_WriteArchiveServer) error {
	ctx := stream.Context()

	// First message should be header
	first, err := stream.Recv()
	if err != nil {
		return status.Error(codes.InvalidArgument, "failed to receive header")
	}

	header := first.GetHeader()
	if header == nil {
		return status.Error(codes.InvalidArgument, "first message must be header")
	}

	sessionID := header.GetSessionId()
	if sessionID == "" {
		return status.Error(codes.InvalidArgument, "session_id is required")
	}

	// Check if local
	info, err := s.svc.GetSandbox(ctx, sessionID)
	if err != nil {
		return toGRPCError(err)
	}

	if info.Node != "" && info.Node != s.cfg.NodeName {
		// Not local - return redirect error
		addr, ok := s.svc.SandboxAgentAddressForNode(info.Node)
		if !ok {
			return status.Error(codes.Unavailable, "target node address unavailable")
		}
		return redirectError(addr, s.grpcPort)
	}

	// Local execution - create pipe and stream data
	pr, pw := io.Pipe()

	errCh := make(chan error, 1)
	go func() {
		defer pw.Close()
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				errCh <- nil
				return
			}
			if err != nil {
				errCh <- err
				return
			}
			if data := msg.GetData(); len(data) > 0 {
				if _, err := pw.Write(data); err != nil {
					errCh <- err
					return
				}
			}
		}
	}()

	if err := s.svc.WriteArchive(ctx, sessionID, header.GetDestDir(), pr); err != nil {
		return toGRPCError(err)
	}

	if err := <-errCh; err != nil {
		return status.Errorf(codes.Internal, "stream error: %v", err)
	}

	return stream.SendAndClose(&pb.WriteArchiveResponse{})
}

// VM state operations (kata mode only)

func (s *Server) SaveVMState(ctx context.Context, req *pb.SaveVMStateRequest) (*emptypb.Empty, error) {
	sessionID := req.GetSessionId()
	if sessionID == "" {
		sessionID = req.GetVmId() // backwards compatibility
	}
	if sessionID == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}

	return s.callEmpty(ctx, sessionID, func(ctx context.Context) error {
		return s.svc.SaveVMState(ctx, sessionID, req.GetPath())
	}, func(ctx context.Context, client pb.SandboxAgentClient) error {
		_, err := client.SaveVMState(ctx, req)
		return err
	})
}

func (s *Server) RestoreVMState(ctx context.Context, req *pb.RestoreVMStateRequest) (*emptypb.Empty, error) {
	sessionID := req.GetSessionId()
	if sessionID == "" {
		sessionID = req.GetVmId() // backwards compatibility
	}
	if sessionID == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}

	return s.callEmpty(ctx, sessionID, func(ctx context.Context) error {
		return s.svc.RestoreVMState(ctx, sessionID, req.GetPath())
	}, func(ctx context.Context, client pb.SandboxAgentClient) error {
		_, err := client.RestoreVMState(ctx, req)
		return err
	})
}

// Internal helpers

func (s *Server) callEmpty(ctx context.Context, sessionID string, local func(context.Context) error, remote func(context.Context, pb.SandboxAgentClient) error) (*emptypb.Empty, error) {
	_, err := call(ctx, s, sessionID, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, local(ctx)
	}, func(ctx context.Context, client pb.SandboxAgentClient) (struct{}, error) {
		return struct{}{}, remote(ctx, client)
	})
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func callUnary[T any](ctx context.Context, s *Server, sessionID string, local func(context.Context) (T, error), remote func(context.Context, pb.SandboxAgentClient) (T, error)) (T, error) {
	return call(ctx, s, sessionID, local, remote)
}

func call[T any](ctx context.Context, s *Server, sessionID string, local func(context.Context) (T, error), _ func(context.Context, pb.SandboxAgentClient) (T, error)) (T, error) {
	var zero T
	if strings.TrimSpace(sessionID) == "" {
		return zero, status.Error(codes.InvalidArgument, "session_id is required")
	}

	// Get sandbox info to determine node
	info, err := s.svc.GetSandbox(ctx, sessionID)
	if err != nil {
		if errors.Is(err, k8s.ErrSandboxNotFound) {
			return zero, status.Error(codes.NotFound, "sandbox not found")
		}
		return zero, status.Errorf(codes.Internal, "get sandbox: %v", err)
	}

	// If local, handle directly
	if info.Node == "" || info.Node == s.cfg.NodeName {
		return local(ctx)
	}

	// Not local - return redirect error with target node info.
	// The client is responsible for routing to the correct node.
	addr, ok := s.svc.SandboxAgentAddressForNode(info.Node)
	if !ok {
		return zero, status.Error(codes.Unavailable, "target node address unavailable")
	}
	return zero, redirectError(addr, s.grpcPort)
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

func toGRPCError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, apierrors.ErrNotFound) {
		return status.Error(codes.NotFound, err.Error())
	}
	if errors.Is(err, apierrors.ErrInvalidArgument) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if errors.Is(err, apierrors.ErrNotSupported) {
		return status.Error(codes.Unimplemented, err.Error())
	}
	if errors.Is(err, apierrors.ErrSandboxPending) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	if errors.Is(err, apierrors.ErrSandboxFailed) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	if errors.Is(err, k8s.ErrSandboxNotFound) {
		return status.Error(codes.NotFound, err.Error())
	}
	return status.Errorf(codes.Internal, "%v", err)
}
