package platform

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNotFound      = errors.New("sandbox not found")
	ErrAlreadyExists = errors.New("sandbox already exists")
	ErrNotReady      = errors.New("sandbox not ready for exec")
)

// Platform defines the execution platform interface for managing
// isolated sandbox environments.
type Platform interface {
	CreateSandbox(ctx context.Context, req CreateSandboxRequest) (*Sandbox, error)
	GetSandbox(ctx context.Context, sessionID string) (*Sandbox, error)
	DeleteSandbox(ctx context.Context, sessionID string) error
	ListSandboxes(ctx context.Context) ([]*Sandbox, error)
	Exec(ctx context.Context, req ExecRequest) (*ExecResult, error)
	StartProcess(ctx context.Context, req StartProcessRequest) (*Process, error)
	WriteToProcess(ctx context.Context, sessionID, execID string, data []byte) error
	ReadFromProcess(ctx context.Context, sessionID, execID string) (*ProcessOutput, error)
	ReadStdout(ctx context.Context, sessionID, execID string) ([]byte, error)
	ReadStderr(ctx context.Context, sessionID, execID string) ([]byte, error)
	KillProcess(ctx context.Context, sessionID, execID string) error
	IsProcessAlive(ctx context.Context, sessionID, execID string) (bool, error)
	ResizeProcess(ctx context.Context, sessionID, execID string, rows, columns uint32) error

	StreamStdout(ctx context.Context, req StreamReadRequest) (<-chan StreamChunk, error)
	StreamStderr(ctx context.Context, req StreamReadRequest) (<-chan StreamChunk, error)
	StreamOutput(ctx context.Context, req StreamReadRequest) (<-chan OutputChunk, error)
}

type CreateSandboxRequest struct {
	SessionID string
	Image     string
	Command   []string
	Env       map[string]string
	Labels    map[string]string
	// DownloadURL is a presigned GET URL for downloading a snapshot to restore.
	// If set, sandbox-agent will create an init container to restore the snapshot.
	DownloadURL string
	// UserID is an optional user identifier. When provided and FUSE storage is enabled,
	// enables the /mydrive mount backed by S3 at assets_bucket/my-drive/{user_id}/.
	UserID string
}

type Sandbox struct {
	SessionID   string
	SandboxID   string
	ContainerID string
	Status      SandboxStatus
	Host        string
	CreatedAt   time.Time
	LastUsedAt  time.Time
	Labels      map[string]string
}

type SandboxStatus string

const (
	StatusPending    SandboxStatus = "pending"
	StatusRunning    SandboxStatus = "running"
	StatusFailed     SandboxStatus = "failed"
	StatusTerminated SandboxStatus = "terminated"
)

type ExecRequest struct {
	SessionID     string
	ContainerName string
	Command       []string
	Env           map[string]string
	WorkingDir    string
	Timeout       time.Duration
	// UserID is an optional user identifier passed through to the sandbox-agent.
	UserID string
}

type ExecResult struct {
	ExitCode int
	Stdout   string
	Stderr   string
}

type StartProcessRequest struct {
	SessionID     string
	ContainerName string
	ExecID        string
	Command       []string
	Env           []string
	Terminal      bool
	// UserID is an optional user identifier passed through to the sandbox-agent.
	UserID string
}

type Process struct {
	ExecID    string
	StartedAt time.Time
	Alive     bool
}

type ProcessOutput struct {
	Stdout []byte
	Stderr []byte
}

// StreamChunk represents a chunk of streaming output.
type StreamChunk struct {
	Data []byte
	EOF  bool
	Err  error
}

// OutputChunk represents a chunk of combined stdout/stderr output.
type OutputChunk struct {
	Stream StreamType
	Data   []byte
	EOF    bool
	Err    error
}

// StreamType indicates the source of output data.
type StreamType int

const (
	StreamStdout StreamType = iota
	StreamStderr
)

// StreamReadRequest holds parameters for streaming read operations.
type StreamReadRequest struct {
	SessionID      string
	ExecID         string
	PollIntervalMs int32
}
