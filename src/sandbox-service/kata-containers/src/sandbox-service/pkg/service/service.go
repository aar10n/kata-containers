package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/platform"
)

const (
	defaultReadyTimeout = 30 * time.Second
	readyPollInterval   = 500 * time.Millisecond
)

type Service struct {
	platform         platform.Platform
	defaultImage     string
	defaultCommand   []string
	defaultTimeout   time.Duration
	maxOutputBytes   int
	readyTimeout     time.Duration
	readyPollDelay   time.Duration
}

type ExecInput struct {
	Command    []string
	Env        map[string]string
	WorkingDir string
	Timeout    time.Duration
	Image      string
}

type SessionInfo struct {
	SessionID string
	Status    platform.SandboxStatus
	CreatedAt time.Time
}

func New(p platform.Platform, defaultImage string, defaultCommand []string, defaultTimeout time.Duration, maxOutputBytes int) *Service {
	return &Service{
		platform:       p,
		defaultImage:   defaultImage,
		defaultCommand: append([]string{}, defaultCommand...),
		defaultTimeout: defaultTimeout,
		maxOutputBytes: maxOutputBytes,
		readyTimeout:   defaultReadyTimeout,
		readyPollDelay: readyPollInterval,
	}
}

func (s *Service) Exec(ctx context.Context, sessionID string, input ExecInput) (*platform.ExecResult, error) {
	if sessionID == "" {
		return nil, errors.New("session_id is required")
	}
	if len(input.Command) == 0 {
		return nil, errors.New("command is required")
	}

	if input.Timeout == 0 {
		input.Timeout = s.defaultTimeout
	}

	if err := s.ensureReady(ctx, sessionID, input.Image); err != nil {
		return nil, err
	}

	result, err := s.platform.Exec(ctx, platform.ExecRequest{
		SessionID:  sessionID,
		Command:    input.Command,
		Env:        input.Env,
		WorkingDir: input.WorkingDir,
		Timeout:    input.Timeout,
	})
	if err != nil {
		return nil, err
	}

	s.truncateOutput(result)
	return result, nil
}

func (s *Service) GetSession(ctx context.Context, sessionID string) (*platform.Sandbox, error) {
	return s.platform.GetSandbox(ctx, sessionID)
}

func (s *Service) ListSessions(ctx context.Context) ([]SessionInfo, error) {
	items, err := s.platform.ListSandboxes(ctx)
	if err != nil {
		return nil, err
	}
	info := make([]SessionInfo, 0, len(items))
	for _, sandbox := range items {
		info = append(info, SessionInfo{
			SessionID: sandbox.SessionID,
			Status:    sandbox.Status,
			CreatedAt: sandbox.CreatedAt,
		})
	}
	return info, nil
}

func (s *Service) DeleteSession(ctx context.Context, sessionID string) error {
	return s.platform.DeleteSandbox(ctx, sessionID)
}

func (s *Service) ensureReady(ctx context.Context, sessionID string, image string) error {
	sandbox, err := s.platform.GetSandbox(ctx, sessionID)
	if err != nil {
		if !errors.Is(err, platform.ErrNotFound) {
			return err
		}
		if image == "" {
			image = s.defaultImage
		}
		_, err := s.platform.CreateSandbox(ctx, platform.CreateSandboxRequest{
			SessionID: sessionID,
			Image:     image,
			Command:   append([]string{}, s.defaultCommand...),
		})
		if err != nil && !errors.Is(err, platform.ErrAlreadyExists) {
			return err
		}
	} else if sandbox.Status == platform.StatusRunning {
		return nil
	}

	return s.waitForReady(ctx, sessionID)
}

func (s *Service) waitForReady(ctx context.Context, sessionID string) error {
	ctx, cancel := context.WithTimeout(ctx, s.readyTimeout)
	defer cancel()

	ticker := time.NewTicker(s.readyPollDelay)
	defer ticker.Stop()

	for {
		sandbox, err := s.platform.GetSandbox(ctx, sessionID)
		if err != nil {
			return err
		}
		switch sandbox.Status {
		case platform.StatusRunning:
			return nil
		case platform.StatusFailed:
			return fmt.Errorf("sandbox failed to start")
		case platform.StatusTerminated:
			return fmt.Errorf("sandbox terminated before ready")
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("sandbox not ready: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func (s *Service) truncateOutput(result *platform.ExecResult) {
	if s.maxOutputBytes <= 0 || result == nil {
		return
	}
	result.Stdout = truncateString(result.Stdout, s.maxOutputBytes)
	result.Stderr = truncateString(result.Stderr, s.maxOutputBytes)
}

func truncateString(value string, maxBytes int) string {
	if len(value) <= maxBytes {
		return value
	}
	if maxBytes <= 0 {
		return ""
	}
	if maxBytes < len("...(truncated)") {
		return value[:maxBytes]
	}
	return value[:maxBytes-len("...(truncated)")] + "...(truncated)"
}
