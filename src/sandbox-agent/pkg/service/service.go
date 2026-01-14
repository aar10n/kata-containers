package service

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/backend"
	apierrors "github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/errors"
	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/hostfs"
	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/k8s"
	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/storage"
)

// SnapshotFileName is the default name for sandbox snapshot files.
const SnapshotFileName = "snapshot.tar.gz"

// Config holds service configuration.
type Config struct {
	NodeName    string
	ExecTimeout time.Duration
	Mode        string // "kata" or "pod"

	// Storage configuration
	StorageEnabled   bool
	StorageAddr      string
	StorageTimeout   time.Duration
	StorageInitImage string
}

// Service defines the sandbox agent service interface.
type Service interface {
	// Sandbox lifecycle
	CreateSandbox(ctx context.Context, req CreateSandboxRequest) (*k8s.SandboxInfo, error)
	GetSandbox(ctx context.Context, sessionID string) (*k8s.SandboxInfo, error)
	DeleteSandbox(ctx context.Context, sessionID string) error
	ListSandboxes(ctx context.Context, nodeName string) []*k8s.SandboxInfo
	UpdateSandboxActivity(ctx context.Context, sessionID string) error
	// SuspendSandbox snapshots the sandbox's /data directory, uploads to S3, and deletes the pod.
	// Only supported in pod mode with storage enabled.
	SuspendSandbox(ctx context.Context, sessionID string) (*SuspendSandboxResult, error)

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
	StreamOutput(ctx context.Context, sessionID, processID string) (<-chan backend.OutputChunk, error)
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
	// DownloadURL is a presigned GET URL for downloading a snapshot to restore.
	// If set, an init container will download and extract the snapshot before
	// the main container starts. Only used in pod mode with storage enabled.
	DownloadURL string
}

// SuspendSandboxResult holds the result of a suspend operation.
type SuspendSandboxResult struct {
	// SnapshotSize is the size of the uploaded snapshot in bytes.
	SnapshotSize int64
	// Duration is the time taken to create and upload the snapshot.
	Duration time.Duration
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
	config        Config
	store         *k8s.Store
	manager       *k8s.Manager
	backend       backend.ExecutionBackend
	hostfs        *hostfs.HostFS
	storageClient *storage.Client
}

// New creates a new service instance.
func New(cfg Config, store *k8s.Store, manager *k8s.Manager, be backend.ExecutionBackend, hfs *hostfs.HostFS) (Service, error) {
	svc := &service{
		config:  cfg,
		store:   store,
		manager: manager,
		backend: be,
		hostfs:  hfs,
	}

	// Set local node name for filtering finalizer handling
	// Only the agent on the pod's node should handle the finalizer
	if cfg.NodeName != "" {
		store.SetLocalNodeName(cfg.NodeName)
	}

	// Initialize storage client if enabled
	if cfg.StorageEnabled {
		client, err := storage.NewClient(storage.ClientConfig{
			Addr:    cfg.StorageAddr,
			Timeout: cfg.StorageTimeout,
		})
		if err != nil {
			return nil, fmt.Errorf("create storage client: %w", err)
		}
		svc.storageClient = client

		// Register callback to auto-save snapshots when pods are deleted
		store.SetPodDeletingCallback(svc.handlePodDeleting)
		slog.Info("registered auto-save callback for pod deletions", "node", cfg.NodeName)
	}

	return svc, nil
}

func (s *service) Mode() string {
	return s.config.Mode
}

func (s *service) Close() error {
	var errs []error
	if s.storageClient != nil {
		if err := s.storageClient.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.backend != nil {
		if err := s.backend.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// Sandbox lifecycle

func (s *service) CreateSandbox(ctx context.Context, req CreateSandboxRequest) (*k8s.SandboxInfo, error) {
	return s.manager.CreateSandbox(ctx, k8s.CreateSandboxRequest{
		SessionID:               req.SessionID,
		Image:                   req.Image,
		Command:                 req.Command,
		Env:                     req.Env,
		Labels:                  req.Labels,
		DownloadURL:             req.DownloadURL,
		InitImage:               s.config.StorageInitImage,
		EnableSnapshotFinalizer: s.storageClient != nil, // Enable finalizer when storage is enabled
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

func (s *service) SuspendSandbox(ctx context.Context, sessionID string) (*SuspendSandboxResult, error) {
	start := time.Now()

	// Validate prerequisites
	if s.config.Mode != "pod" {
		return nil, fmt.Errorf("%w: suspend is only supported in pod mode", apierrors.ErrNotSupported)
	}
	if s.storageClient == nil {
		return nil, fmt.Errorf("%w: storage is not enabled", apierrors.ErrNotSupported)
	}
	if s.hostfs == nil {
		return nil, fmt.Errorf("%w: hostfs is not available", apierrors.ErrNotSupported)
	}

	// Save snapshot
	archiveSize, err := s.saveSnapshot(ctx, sessionID)
	if err != nil {
		return nil, err
	}

	// Remove finalizer first so the pod can be deleted
	if err := s.manager.RemoveSnapshotFinalizer(ctx, sessionID); err != nil && err != k8s.ErrSandboxNotFound {
		slog.Warn("failed to remove finalizer after suspend", "session_id", sessionID, "error", err)
	}

	// Delete the pod
	if err := s.manager.DeleteSandbox(ctx, sessionID); err != nil {
		slog.Warn("failed to delete pod after suspend", "session_id", sessionID, "error", err)
		// Don't fail - snapshot was uploaded successfully
	}

	return &SuspendSandboxResult{
		SnapshotSize: archiveSize,
		Duration:     time.Since(start),
	}, nil
}

// saveSnapshot creates and uploads a snapshot for the given session.
// Returns the size of the uploaded snapshot in bytes.
func (s *service) saveSnapshot(ctx context.Context, sessionID string) (int64, error) {
	// Get sandbox info
	info, ok := s.store.GetSandbox(sessionID)
	if !ok {
		return 0, fmt.Errorf("%w: sandbox not found", apierrors.ErrNotFound)
	}
	if info.PodUID == "" {
		return 0, fmt.Errorf("%w: pod UID not available", apierrors.ErrNotFound)
	}

	// Step 1: Create tar archive of /data directory
	slog.Info("creating snapshot", "session_id", sessionID, "pod_uid", info.PodUID)
	archiveReader, err := s.hostfs.ReadArchive(ctx, info.PodUID, ".")
	if err != nil {
		return 0, fmt.Errorf("create archive: %w", err)
	}

	// Read archive and gzip-compress it for upload
	var archiveBuf bytes.Buffer
	gzWriter := gzip.NewWriter(&archiveBuf)
	if _, err := io.Copy(gzWriter, archiveReader); err != nil {
		archiveReader.Close()
		gzWriter.Close()
		return 0, fmt.Errorf("compress archive: %w", err)
	}
	archiveReader.Close()
	if err := gzWriter.Close(); err != nil {
		return 0, fmt.Errorf("finalize gzip: %w", err)
	}
	archiveSize := int64(archiveBuf.Len())

	slog.Info("snapshot created", "session_id", sessionID, "size_bytes", archiveSize)

	// Step 2: Get presigned upload URL from storage service
	uploadResult, err := s.storageClient.GetUploadURL(ctx, sessionID, SnapshotFileName)
	if err != nil {
		return 0, fmt.Errorf("get upload URL: %w", err)
	}

	// Step 3: Upload archive to S3
	slog.Info("uploading snapshot", "session_id", sessionID, "key", uploadResult.Key)
	if err := uploadToPresignedURL(ctx, uploadResult.URL, archiveBuf.Bytes()); err != nil {
		return 0, fmt.Errorf("upload snapshot: %w", err)
	}

	slog.Info("snapshot uploaded", "session_id", sessionID, "size_bytes", archiveSize)
	return archiveSize, nil
}

// handlePodDeleting is called when a pod with our finalizer is being deleted.
// It saves a snapshot and removes the finalizer to allow deletion to proceed.
func (s *service) handlePodDeleting(sessionID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	slog.Info("auto-saving snapshot on pod deletion", "session_id", sessionID)

	// Clear the pending state when done (success or failure)
	defer s.store.ClearPendingSnapshotSave(sessionID)

	// Try to save the snapshot
	if _, err := s.saveSnapshot(ctx, sessionID); err != nil {
		slog.Error("failed to auto-save snapshot", "session_id", sessionID, "error", err)
		// Still remove the finalizer so the pod can be deleted
	}

	// Remove finalizer to allow pod deletion to proceed
	if err := s.manager.RemoveSnapshotFinalizer(ctx, sessionID); err != nil {
		slog.Error("failed to remove finalizer", "session_id", sessionID, "error", err)
	} else {
		slog.Info("removed finalizer, pod will be deleted", "session_id", sessionID)
	}
}

// uploadToPresignedURL uploads data to a presigned PUT URL.
func uploadToPresignedURL(ctx context.Context, url string, data []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/gzip")
	req.ContentLength = int64(len(data))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("upload: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upload failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
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

func (s *service) StreamOutput(ctx context.Context, sessionID, processID string) (<-chan backend.OutputChunk, error) {
	containerID, err := s.resolveContainerID(sessionID)
	if err != nil {
		return nil, err
	}
	return s.backend.StreamOutput(ctx, containerID, processID)
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

	// In both pod and kata modes, use container ID from pod status.
	// For kata mode, the backend will resolve the sandbox ID separately
	// using the SandboxIDResolver to connect to the correct shim.
	containerID, ok := s.store.ContainerIDForSession(sessionID)
	if !ok {
		return "", fmt.Errorf("%w: container not found for session", apierrors.ErrNotFound)
	}
	return containerID, nil
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
