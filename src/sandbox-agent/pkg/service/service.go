package service

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/backend"
	apierrors "github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/errors"
	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/hostfs"
	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/k8s"
)

// Config holds service configuration.
type Config struct {
	NodeName    string
	ExecTimeout time.Duration
	Mode        string // "kata" or "pod"
}

// Service defines the sandbox agent service interface.
type Service interface {
	// Sandbox lifecycle
	CreateSandbox(ctx context.Context, req CreateSandboxRequest) (*k8s.SandboxInfo, error)
	GetSandbox(ctx context.Context, sessionID string) (*k8s.SandboxInfo, error)
	DeleteSandbox(ctx context.Context, sessionID string) error
	ListSandboxes(ctx context.Context, nodeName string) []*k8s.SandboxInfo
	UpdateSandboxActivity(ctx context.Context, sessionID string) error

	// Routing helpers
	NodeForVM(vmID string) (string, bool)
	SandboxAgentAddressForNode(nodeName string) (string, bool)
	ResolveSandboxID(ctx context.Context, containerID string) (string, error)

	// Command execution
	Exec(ctx context.Context, req ExecRequest) (ExecResponse, error)

	// Process management (interactive/streaming)
	StartProcess(ctx context.Context, req StartProcessRequest) (*ProcessInfo, error)
	WriteToProcess(ctx context.Context, sessionID, processID string, data []byte) error
	ReadStdout(ctx context.Context, sessionID, processID string, maxBytes int) ([]byte, error)
	ReadStderr(ctx context.Context, sessionID, processID string, maxBytes int) ([]byte, error)
	StreamStdout(ctx context.Context, sessionID, processID string) (<-chan backend.StreamChunk, error)
	StreamStderr(ctx context.Context, sessionID, processID string) (<-chan backend.StreamChunk, error)
	CloseStdin(ctx context.Context, sessionID, processID string) error
	KillProcess(ctx context.Context, sessionID, processID string, signal int) error
	WaitProcess(ctx context.Context, sessionID, processID string) (int32, error)
	ResizeTerminal(ctx context.Context, sessionID, processID string, rows, cols uint32) error

	// File operations (via emptyDir host access)
	ReadFile(ctx context.Context, sessionID, path string) ([]byte, error)
	WriteFile(ctx context.Context, sessionID, path string, content []byte, mode uint32) error
	ReadArchive(ctx context.Context, sessionID, path string) (io.ReadCloser, error)
	WriteArchive(ctx context.Context, sessionID, destDir string, tarData io.Reader) error

	// VM state operations (kata mode only)
	SupportsStateOps() bool
	SaveVMState(ctx context.Context, sessionID, statePath string) error
	RestoreVMState(ctx context.Context, sessionID, statePath string) error

	// Mode info
	Mode() string

	// Cleanup
	Close() error
}

// CreateSandboxRequest holds parameters for creating a sandbox.
type CreateSandboxRequest struct {
	SessionID string
	Image     string
	Command   []string
	Env       map[string]string
	Labels    map[string]string
}

// ExecRequest holds parameters for command execution.
type ExecRequest struct {
	SessionID string
	Args      []string
	Env       []string
	Cwd       string
	Timeout   time.Duration
}

// ExecResponse holds command execution results.
type ExecResponse struct {
	Stdout   string
	Stderr   string
	ExitCode int32
}

// StartProcessRequest holds parameters for starting an interactive process.
type StartProcessRequest struct {
	SessionID string
	Command   []string
	Env       []string
	Cwd       string
	TTY       bool
}

// ProcessInfo holds information about a started process.
type ProcessInfo struct {
	ProcessID   string
	ContainerID string
}

type service struct {
	config  Config
	store   *k8s.Store
	manager *k8s.Manager
	backend backend.ExecutionBackend
	hostfs  *hostfs.HostFS
}

// New creates a new service instance.
func New(cfg Config, store *k8s.Store, manager *k8s.Manager, be backend.ExecutionBackend, hfs *hostfs.HostFS) Service {
	return &service{
		config:  cfg,
		store:   store,
		manager: manager,
		backend: be,
		hostfs:  hfs,
	}
}

func (s *service) Mode() string {
	return s.config.Mode
}

func (s *service) Close() error {
	if s.backend != nil {
		return s.backend.Close()
	}
	return nil
}

// Sandbox lifecycle

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

// Routing helpers

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

// Command execution

func (s *service) Exec(ctx context.Context, req ExecRequest) (ExecResponse, error) {
	containerID, err := s.resolveContainerID(req.SessionID)
	if err != nil {
		return ExecResponse{}, err
	}

	result, err := s.backend.Exec(ctx, containerID, req.Args, req.Env, req.Cwd, s.execTimeout(req.Timeout))
	if err != nil {
		return ExecResponse{}, err
	}

	return ExecResponse{
		Stdout:   result.Stdout,
		Stderr:   result.Stderr,
		ExitCode: result.ExitCode,
	}, nil
}

// Process management

func (s *service) StartProcess(ctx context.Context, req StartProcessRequest) (*ProcessInfo, error) {
	containerID, err := s.resolveContainerID(req.SessionID)
	if err != nil {
		return nil, err
	}

	proc, err := s.backend.StartProcess(ctx, containerID, req.Command, req.Env, req.Cwd, req.TTY)
	if err != nil {
		return nil, err
	}

	return &ProcessInfo{
		ProcessID:   proc.ID,
		ContainerID: proc.ContainerID,
	}, nil
}

func (s *service) WriteToProcess(ctx context.Context, sessionID, processID string, data []byte) error {
	containerID, err := s.resolveContainerID(sessionID)
	if err != nil {
		return err
	}
	return s.backend.WriteToProcess(ctx, containerID, processID, data)
}

func (s *service) ReadStdout(ctx context.Context, sessionID, processID string, maxBytes int) ([]byte, error) {
	containerID, err := s.resolveContainerID(sessionID)
	if err != nil {
		return nil, err
	}
	return s.backend.ReadStdout(ctx, containerID, processID, maxBytes)
}

func (s *service) ReadStderr(ctx context.Context, sessionID, processID string, maxBytes int) ([]byte, error) {
	containerID, err := s.resolveContainerID(sessionID)
	if err != nil {
		return nil, err
	}
	return s.backend.ReadStderr(ctx, containerID, processID, maxBytes)
}

func (s *service) StreamStdout(ctx context.Context, sessionID, processID string) (<-chan backend.StreamChunk, error) {
	containerID, err := s.resolveContainerID(sessionID)
	if err != nil {
		return nil, err
	}
	return s.backend.StreamStdout(ctx, containerID, processID)
}

func (s *service) StreamStderr(ctx context.Context, sessionID, processID string) (<-chan backend.StreamChunk, error) {
	containerID, err := s.resolveContainerID(sessionID)
	if err != nil {
		return nil, err
	}
	return s.backend.StreamStderr(ctx, containerID, processID)
}

func (s *service) CloseStdin(ctx context.Context, sessionID, processID string) error {
	containerID, err := s.resolveContainerID(sessionID)
	if err != nil {
		return err
	}
	return s.backend.CloseStdin(ctx, containerID, processID)
}

func (s *service) KillProcess(ctx context.Context, sessionID, processID string, signal int) error {
	containerID, err := s.resolveContainerID(sessionID)
	if err != nil {
		return err
	}
	return s.backend.KillProcess(ctx, containerID, processID, signal)
}

func (s *service) WaitProcess(ctx context.Context, sessionID, processID string) (int32, error) {
	containerID, err := s.resolveContainerID(sessionID)
	if err != nil {
		return -1, err
	}
	return s.backend.WaitProcess(ctx, containerID, processID)
}

func (s *service) ResizeTerminal(ctx context.Context, sessionID, processID string, rows, cols uint32) error {
	containerID, err := s.resolveContainerID(sessionID)
	if err != nil {
		return err
	}
	return s.backend.ResizeTerminal(ctx, containerID, processID, rows, cols)
}

// File operations

func (s *service) ReadFile(ctx context.Context, sessionID, path string) ([]byte, error) {
	if s.hostfs == nil {
		return nil, fmt.Errorf("%w: file operations not available", apierrors.ErrNotSupported)
	}

	podUID, ok := s.store.PodUIDForSession(sessionID)
	if !ok {
		return nil, fmt.Errorf("%w: sandbox not found", apierrors.ErrNotFound)
	}

	return s.hostfs.ReadFile(ctx, podUID, path)
}

func (s *service) WriteFile(ctx context.Context, sessionID, path string, content []byte, mode uint32) error {
	if s.hostfs == nil {
		return fmt.Errorf("%w: file operations not available", apierrors.ErrNotSupported)
	}

	podUID, ok := s.store.PodUIDForSession(sessionID)
	if !ok {
		return fmt.Errorf("%w: sandbox not found", apierrors.ErrNotFound)
	}

	return s.hostfs.WriteFile(ctx, podUID, path, content, modeToFileMode(mode))
}

func (s *service) ReadArchive(ctx context.Context, sessionID, path string) (io.ReadCloser, error) {
	if s.hostfs == nil {
		return nil, fmt.Errorf("%w: file operations not available", apierrors.ErrNotSupported)
	}

	podUID, ok := s.store.PodUIDForSession(sessionID)
	if !ok {
		return nil, fmt.Errorf("%w: sandbox not found", apierrors.ErrNotFound)
	}

	return s.hostfs.ReadArchive(ctx, podUID, path)
}

func (s *service) WriteArchive(ctx context.Context, sessionID, destDir string, tarData io.Reader) error {
	if s.hostfs == nil {
		return fmt.Errorf("%w: file operations not available", apierrors.ErrNotSupported)
	}

	podUID, ok := s.store.PodUIDForSession(sessionID)
	if !ok {
		return fmt.Errorf("%w: sandbox not found", apierrors.ErrNotFound)
	}

	return s.hostfs.WriteArchive(ctx, podUID, destDir, tarData)
}

// VM state operations

func (s *service) SupportsStateOps() bool {
	return s.backend.SupportsStateOps()
}

func (s *service) SaveVMState(ctx context.Context, sessionID, statePath string) error {
	if !s.backend.SupportsStateOps() {
		return fmt.Errorf("%w: VM state operations not supported in %s mode", apierrors.ErrNotSupported, s.config.Mode)
	}

	sandboxID, err := s.resolveSandboxID(sessionID)
	if err != nil {
		return err
	}

	if strings.TrimSpace(statePath) == "" {
		return fmt.Errorf("%w: state path is required", apierrors.ErrInvalidArgument)
	}

	return s.backend.SaveState(ctx, sandboxID)
}

func (s *service) RestoreVMState(ctx context.Context, sessionID, statePath string) error {
	if !s.backend.SupportsStateOps() {
		return fmt.Errorf("%w: VM state operations not supported in %s mode", apierrors.ErrNotSupported, s.config.Mode)
	}

	sandboxID, err := s.resolveSandboxID(sessionID)
	if err != nil {
		return err
	}

	if strings.TrimSpace(statePath) == "" {
		return fmt.Errorf("%w: state path is required", apierrors.ErrInvalidArgument)
	}

	return s.backend.RestoreState(ctx, sandboxID, statePath)
}

// Internal helpers

func (s *service) resolveContainerID(sessionID string) (string, error) {
	if sessionID == "" {
		return "", fmt.Errorf("%w: session id is required", apierrors.ErrInvalidArgument)
	}

	// In pod mode, use container ID from pod status
	if s.config.Mode == "pod" {
		containerID, ok := s.store.ContainerIDForSession(sessionID)
		if !ok {
			return "", fmt.Errorf("%w: container not found for session", apierrors.ErrNotFound)
		}
		return containerID, nil
	}

	// In kata mode, use sandbox ID
	sandboxID, err := s.resolveSandboxID(sessionID)
	if err != nil {
		return "", err
	}
	return sandboxID, nil
}

func (s *service) resolveSandboxID(sessionID string) (string, error) {
	info, ok := s.store.GetSandbox(sessionID)
	if !ok {
		return "", fmt.Errorf("%w: sandbox not found", apierrors.ErrNotFound)
	}
	if info.SandboxID == "" {
		return "", fmt.Errorf("%w: sandbox id not yet available", apierrors.ErrNotFound)
	}
	return info.SandboxID, nil
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

func modeToFileMode(mode uint32) os.FileMode {
	if mode == 0 {
		return 0644
	}
	return os.FileMode(mode)
}
