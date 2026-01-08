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
}

type CreateSandboxRequest struct {
	SessionID string
	Image     string
	Command   []string
	Env       map[string]string
	Labels    map[string]string
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

// StreamReadRequest holds parameters for streaming read operations.
type StreamReadRequest struct {
	SessionID      string
	ExecID         string
	PollIntervalMs int32
}
