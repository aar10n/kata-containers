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
	SessionID  string
	Command    []string
	Env        map[string]string
	WorkingDir string
	Timeout    time.Duration
}

type ExecResult struct {
	ExitCode int
	Stdout   string
	Stderr   string
}
