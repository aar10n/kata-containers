package service

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/platform"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

const (
	defaultReadyTimeout = 30 * time.Second
	readyPollInterval   = 200 * time.Millisecond // Faster polling for quicker response
	defaultProcTimeout  = 30 * time.Second
	maxCompletedJobs    = 50
)

const shellInitScript = `
bind 'set enable-bracketed-paste off'
stty -echo
export PS1=
unset PROMPT_COMMAND
printf '%s'
`

const pythonREPLWrapper = `#!/usr/bin/env python3
"""
REPL wrapper for sandbox-service.
Reads code blocks delimited by <<<EXEC>>>, executes them, and outputs <<<DONE>>>.
"""
import sys
import traceback
import io

# Global context for persistent state
__context__ = {'__builtins__': __builtins__}

def execute_code(code):
    """Execute code and return (stdout, stderr)."""
    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()

    old_stdout, old_stderr = sys.stdout, sys.stderr
    sys.stdout, sys.stderr = stdout_capture, stderr_capture

    try:
        # Try to compile as expression first
        try:
            compiled = compile(code, '<repl>', 'eval')
            result = eval(compiled, __context__)
            if result is not None:
                print(repr(result))
        except SyntaxError:
            # Fall back to exec for statements
            compiled = compile(code, '<repl>', 'exec')
            exec(compiled, __context__)
    except Exception:
        traceback.print_exc()
    finally:
        sys.stdout, sys.stderr = old_stdout, old_stderr

    return stdout_capture.getvalue(), stderr_capture.getvalue()

def main():
    code_buffer = []

    for line in sys.stdin:
        if line.strip() == '<<<EXEC>>>':
            code = ''.join(code_buffer)
            code_buffer = []

            stdout, stderr = execute_code(code)

            # Output results
            if stdout:
                sys.stdout.write(stdout)
            if stderr:
                sys.stderr.write(stderr)

            # Signal completion
            sys.stdout.write('<<<DONE>>>\n')
            sys.stdout.flush()
            sys.stderr.flush()
        else:
            code_buffer.append(line)

if __name__ == '__main__':
    main()
`

// LeaderElectionConfig holds settings for K8s leader election.
// When enabled, only the leader instance runs the cleanup loop.
type LeaderElectionConfig struct {
	Enabled       bool
	Namespace     string
	LeaseName     string
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
}

type Service struct {
	platform        platform.Platform
	defaultImage    string
	defaultCommand  []string
	mainContainer   string
	shellContainer  string
	defaultTimeout  time.Duration
	maxOutputBytes  int
	readyTimeout    time.Duration
	readyPollDelay  time.Duration
	defaultTTL      time.Duration
	cleanupInterval time.Duration
	leaderElection  LeaderElectionConfig
	stopCh          chan struct{}
	stopOnce        sync.Once
	mu              sync.Mutex
	sessions        map[string]*Session
}

type ExecInput struct {
	Command    []string
	Env        map[string]string
	WorkingDir string
	Timeout    time.Duration
	Image      string
}

type Session struct {
	SessionID string
	Shell     *ProcessState
	REPLs     map[string]*ProcessState
	Jobs      map[string]*JobState
}

type ProcessState struct {
	ExecID     string
	StartedAt  time.Time
	LastUsedAt time.Time
}

type JobState struct {
	JobID      string
	PID        int
	Name       string
	Command    string
	Status     string // "running", "completed", "failed", "killed"
	ExitCode   *int
	StartedAt  time.Time
	FinishedAt time.Time
}

type ShellResult struct {
	Output   string
	ExitCode int
	Error    string
}

type REPLResult struct {
	Output string
	Error  string
}

type ProcessStatus struct {
	Alive      bool
	ExecID     string
	StartedAt  time.Time
	LastUsedAt time.Time
	Message    string
}

type SessionInfo struct {
	SessionID  string
	Status     platform.SandboxStatus
	CreatedAt  time.Time
	LastUsedAt time.Time
}

func New(p platform.Platform, defaultImage string, defaultCommand []string, mainContainer string, shellContainer string, defaultTimeout time.Duration, maxOutputBytes int, defaultTTL time.Duration, cleanupInterval time.Duration, leaderElection LeaderElectionConfig) *Service {
	if strings.TrimSpace(mainContainer) == "" {
		mainContainer = "sandbox"
	}
	svc := &Service{
		platform:        p,
		defaultImage:    defaultImage,
		defaultCommand:  append([]string{}, defaultCommand...),
		mainContainer:   mainContainer,
		shellContainer:  strings.TrimSpace(shellContainer),
		defaultTimeout:  defaultTimeout,
		maxOutputBytes:  maxOutputBytes,
		readyTimeout:    defaultReadyTimeout,
		readyPollDelay:  readyPollInterval,
		defaultTTL:      defaultTTL,
		cleanupInterval: cleanupInterval,
		leaderElection:  leaderElection,
		stopCh:          make(chan struct{}),
		sessions:        make(map[string]*Session),
	}
	if defaultTTL > 0 && cleanupInterval > 0 {
		if leaderElection.Enabled {
			go svc.cleanupLoopWithLeaderElection()
		} else {
			go svc.cleanupLoop()
		}
	}
	return svc
}

func (s *Service) getSession(sessionID string) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	session, ok := s.sessions[sessionID]
	if !ok {
		session = &Session{
			SessionID: sessionID,
			REPLs:     make(map[string]*ProcessState),
			Jobs:      make(map[string]*JobState),
		}
		s.sessions[sessionID] = session
	}
	return session
}

func (s *Service) clearSession(sessionID string) {
	s.mu.Lock()
	delete(s.sessions, sessionID)
	s.mu.Unlock()
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
		SessionID:     sessionID,
		ContainerName: s.mainContainer,
		Command:       input.Command,
		Env:           input.Env,
		WorkingDir:    input.WorkingDir,
		Timeout:       input.Timeout,
	})
	if err != nil {
		return nil, err
	}

	s.truncateOutput(result)
	return result, nil
}

func (s *Service) ExecShell(ctx context.Context, sessionID, command string, timeout time.Duration) (*ShellResult, error) {
	if sessionID == "" {
		return nil, errors.New("session_id is required")
	}
	if strings.TrimSpace(command) == "" {
		return nil, errors.New("command is required")
	}
	if timeout <= 0 {
		timeout = defaultProcTimeout
	}

	readyCtx, readyCancel := context.WithTimeout(ctx, s.readyTimeout)
	defer readyCancel()

	if err := s.ensureReady(readyCtx, sessionID, ""); err != nil {
		return nil, err
	}

	proc, err := s.ensureShell(readyCtx, sessionID)
	if err != nil {
		return nil, err
	}

	cmdCtx, cmdCancel := context.WithTimeout(ctx, timeout)
	defer cmdCancel()

	marker := fmt.Sprintf("__MARKER_%d_%s__", time.Now().UnixNano(), randomString(8))
	cmdWithMarker := fmt.Sprintf("%s\nprintf '<<<EXIT:%%d:%s>>>\\n' $?\n", command, marker)

	if err := s.platform.WriteToProcess(cmdCtx, sessionID, proc.ExecID, []byte(cmdWithMarker)); err != nil {
		return nil, err
	}

	output, exitCode, err := s.readUntilMarker(cmdCtx, sessionID, proc.ExecID, marker)
	s.updateProcessLastUsed(sessionID, "shell")
	output = normalizeShellOutput(output)
	if err != nil {
		if isTimeoutErr(err) {
			return &ShellResult{
				Output:   output,
				ExitCode: -1,
				Error:    "command timed out - shell may be stuck, consider calling /shell/reset",
			}, nil
		}
		return nil, err
	}

	return &ShellResult{
		Output:   output,
		ExitCode: exitCode,
	}, nil
}

func (s *Service) ResetShell(ctx context.Context, sessionID string) error {
	session := s.getSession(sessionID)
	var execID string

	s.mu.Lock()
	if session.Shell != nil {
		execID = session.Shell.ExecID
		session.Shell = nil
	}
	// Clear all jobs - they will be killed when the shell process dies
	session.Jobs = make(map[string]*JobState)
	s.mu.Unlock()

	if execID != "" {
		if err := s.platform.KillProcess(ctx, sessionID, execID); err != nil && !errors.Is(err, platform.ErrNotFound) {
			return err
		}
		if err := s.waitForProcessExit(ctx, sessionID, execID, 5*time.Second); err != nil {
			return err
		}
		// Clean up job log files
		_, _ = s.platform.Exec(ctx, platform.ExecRequest{
			SessionID:     sessionID,
			ContainerName: s.shellContainer,
			Command:       []string{"/bin/sh", "-c", "rm -rf /tmp/jobs 2>/dev/null || true"},
			Timeout:       5 * time.Second,
		})
	}
	return nil
}

func (s *Service) ShellStatus(ctx context.Context, sessionID string) (*ProcessStatus, error) {
	session := s.getSession(sessionID)

	s.mu.Lock()
	shell := session.Shell
	var execID string
	var startedAt, lastUsedAt time.Time
	if shell != nil {
		execID = shell.ExecID
		startedAt = shell.StartedAt
		lastUsedAt = shell.LastUsedAt
	}
	s.mu.Unlock()

	if shell == nil {
		return &ProcessStatus{Alive: false, Message: "shell not started"}, nil
	}

	alive, err := s.platform.IsProcessAlive(ctx, sessionID, execID)
	if err != nil {
		return nil, err
	}
	if !alive {
		s.mu.Lock()
		if session.Shell != nil && session.Shell.ExecID == execID {
			session.Shell = nil
		}
		s.mu.Unlock()
		return &ProcessStatus{Alive: false, Message: "shell not started"}, nil
	}

	return &ProcessStatus{
		Alive:      true,
		ExecID:     execID,
		StartedAt:  startedAt,
		LastUsedAt: lastUsedAt,
	}, nil
}

func (s *Service) ResizeShell(ctx context.Context, sessionID string, columns, rows uint32) error {
	session := s.getSession(sessionID)

	s.mu.Lock()
	shell := session.Shell
	var execID string
	if shell != nil {
		execID = shell.ExecID
	}
	s.mu.Unlock()

	if shell == nil {
		return errors.New("shell not started")
	}

	alive, err := s.platform.IsProcessAlive(ctx, sessionID, execID)
	if err != nil {
		return err
	}
	if !alive {
		s.mu.Lock()
		if session.Shell != nil && session.Shell.ExecID == execID {
			session.Shell = nil
		}
		s.mu.Unlock()
		return errors.New("shell not started")
	}

	return s.platform.ResizeProcess(ctx, sessionID, execID, rows, columns)
}

func (s *Service) ExecPython(ctx context.Context, sessionID, code string, timeout time.Duration) (*REPLResult, error) {
	if sessionID == "" {
		return nil, errors.New("session_id is required")
	}
	if strings.TrimSpace(code) == "" {
		return nil, errors.New("code is required")
	}
	if timeout <= 0 {
		timeout = defaultProcTimeout
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if err := s.ensureReady(ctx, sessionID, ""); err != nil {
		return nil, err
	}

	proc, err := s.ensurePythonREPL(ctx, sessionID)
	if err != nil {
		return nil, err
	}

	payload := code + "\n<<<EXEC>>>\n"
	if err := s.platform.WriteToProcess(ctx, sessionID, proc.ExecID, []byte(payload)); err != nil {
		return nil, err
	}

	output, err := s.readUntilDone(ctx, sessionID, proc.ExecID)
	s.updateProcessLastUsed(sessionID, "python")
	if err != nil {
		return nil, err
	}

	return &REPLResult{
		Output: string(output.Stdout),
		Error:  string(output.Stderr),
	}, nil
}

func (s *Service) ResetPython(ctx context.Context, sessionID string) error {
	session := s.getSession(sessionID)
	var execID string

	s.mu.Lock()
	if session.REPLs != nil {
		if repl := session.REPLs["python"]; repl != nil {
			execID = repl.ExecID
			delete(session.REPLs, "python")
		}
	}
	s.mu.Unlock()

	if execID != "" {
		_ = s.platform.KillProcess(ctx, sessionID, execID)
	}
	return nil
}

func (s *Service) PythonStatus(ctx context.Context, sessionID string) (*ProcessStatus, error) {
	session := s.getSession(sessionID)

	s.mu.Lock()
	repl := session.REPLs["python"]
	var execID string
	var startedAt, lastUsedAt time.Time
	if repl != nil {
		execID = repl.ExecID
		startedAt = repl.StartedAt
		lastUsedAt = repl.LastUsedAt
	}
	s.mu.Unlock()

	if repl == nil {
		return &ProcessStatus{Alive: false, Message: "python repl not started"}, nil
	}

	alive, err := s.platform.IsProcessAlive(ctx, sessionID, execID)
	if err != nil {
		return nil, err
	}
	if !alive {
		s.mu.Lock()
		if session.REPLs["python"] != nil && session.REPLs["python"].ExecID == execID {
			delete(session.REPLs, "python")
		}
		s.mu.Unlock()
		return &ProcessStatus{Alive: false, Message: "python repl not started"}, nil
	}

	return &ProcessStatus{
		Alive:      true,
		ExecID:     execID,
		StartedAt:  startedAt,
		LastUsedAt: lastUsedAt,
	}, nil
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
			SessionID:  sandbox.SessionID,
			Status:     sandbox.Status,
			CreatedAt:  sandbox.CreatedAt,
			LastUsedAt: sandbox.LastUsedAt,
		})
	}
	return info, nil
}

func (s *Service) DeleteSession(ctx context.Context, sessionID string) error {
	err := s.platform.DeleteSandbox(ctx, sessionID)
	if err == nil || errors.Is(err, platform.ErrNotFound) {
		s.clearSession(sessionID)
	}
	return err
}

func (s *Service) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
}

func (s *Service) cleanupLoop() {
	ticker := time.NewTicker(s.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.cleanupExpired(context.Background())
		case <-s.stopCh:
			return
		}
	}
}

func (s *Service) cleanupLoopWithLeaderElection() {
	// Build in-cluster K8s client
	config, err := rest.InClusterConfig()
	if err != nil {
		slog.Warn("cleanup leader election: failed to get in-cluster config, falling back to simple cleanup loop", "error", err)
		s.cleanupLoop()
		return
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		slog.Warn("cleanup leader election: failed to create k8s client, falling back to simple cleanup loop", "error", err)
		s.cleanupLoop()
		return
	}

	// Get pod identity from hostname
	podName := os.Getenv("HOSTNAME")
	if podName == "" {
		podName = fmt.Sprintf("sandbox-service-%d", time.Now().UnixNano())
	}

	// Determine namespace
	namespace := s.leaderElection.Namespace
	if namespace == "" {
		// Try to read from service account
		if data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
			namespace = strings.TrimSpace(string(data))
		}
	}
	if namespace == "" {
		namespace = "default"
	}

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      s.leaderElection.LeaseName,
			Namespace: namespace,
		},
		Client: client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: podName,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-s.stopCh
		cancel()
	}()

	slog.Info("cleanup leader election: starting", "identity", podName, "namespace", namespace, "lease", s.leaderElection.LeaseName)

	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   s.leaderElection.LeaseDuration,
		RenewDeadline:   s.leaderElection.RenewDeadline,
		RetryPeriod:     s.leaderElection.RetryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				slog.Info("cleanup leader election: became leader")
				s.runCleanupAsLeader(ctx)
			},
			OnStoppedLeading: func() {
				slog.Info("cleanup leader election: lost leadership")
			},
			OnNewLeader: func(identity string) {
				if identity != podName {
					slog.Info("cleanup leader election: new leader elected", "leader", identity)
				}
			},
		},
	})
}

func (s *Service) runCleanupAsLeader(ctx context.Context) {
	ticker := time.NewTicker(s.cleanupInterval)
	defer ticker.Stop()

	// Run immediately on becoming leader
	s.cleanupExpired(ctx)

	for {
		select {
		case <-ticker.C:
			s.cleanupExpired(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (s *Service) cleanupExpired(ctx context.Context) {
	if s.defaultTTL <= 0 {
		return
	}
	sandboxes, err := s.platform.ListSandboxes(ctx)
	if err != nil {
		slog.Error("cleanup list sandboxes failed", "error", err)
		return
	}
	now := time.Now().UTC()
	for _, sb := range sandboxes {
		lastUsed := sb.LastUsedAt
		if lastUsed.IsZero() {
			lastUsed = sb.CreatedAt
		}
		idleFor := now.Sub(lastUsed)
		if idleFor <= s.defaultTTL {
			continue
		}
		if err := s.platform.DeleteSandbox(ctx, sb.SessionID); err != nil && !errors.Is(err, platform.ErrNotFound) {
			slog.Error("cleanup delete sandbox failed", "session_id", sb.SessionID, "error", err)
			continue
		}
		s.clearSession(sb.SessionID)
		slog.Info("deleted expired sandbox", "session_id", sb.SessionID, "idle_for", idleFor.Round(time.Second))
	}
}

func (s *Service) ensureShell(ctx context.Context, sessionID string) (*ProcessState, error) {
	session := s.getSession(sessionID)

	s.mu.Lock()
	shell := session.Shell
	s.mu.Unlock()
	if shell != nil {
		alive, err := s.platform.IsProcessAlive(ctx, sessionID, shell.ExecID)
		if err == nil && alive {
			return shell, nil
		}
		s.mu.Lock()
		session.Shell = nil
		s.mu.Unlock()
	}

	proc, err := s.platform.StartProcess(ctx, platform.StartProcessRequest{
		SessionID:     sessionID,
		ContainerName: s.shellContainer,
		ExecID:        fmt.Sprintf("%s-shell", sessionID),
		Command: []string{
			"/usr/bin/env",
			"bash",
			"--noprofile",
			"--norc",
			"-i",
		},
		Env:      []string{"PS1=", "PROMPT_COMMAND=", "TERM=xterm-256color"},
		Terminal: true,
	})
	if err != nil {
		return nil, err
	}

	state := &ProcessState{
		ExecID:     proc.ExecID,
		StartedAt:  proc.StartedAt,
		LastUsedAt: proc.StartedAt,
	}

	if err := s.initShell(ctx, sessionID, proc.ExecID); err != nil {
		_ = s.platform.KillProcess(ctx, sessionID, proc.ExecID)
		return nil, err
	}

	s.mu.Lock()
	session.Shell = state
	s.mu.Unlock()

	s.drainProcessOutput(ctx, sessionID, proc.ExecID, 200*time.Millisecond)
	return state, nil
}

func (s *Service) ensurePythonREPL(ctx context.Context, sessionID string) (*ProcessState, error) {
	session := s.getSession(sessionID)

	s.mu.Lock()
	repl := session.REPLs["python"]
	s.mu.Unlock()
	if repl != nil {
		alive, err := s.platform.IsProcessAlive(ctx, sessionID, repl.ExecID)
		if err == nil && alive {
			return repl, nil
		}
		s.mu.Lock()
		delete(session.REPLs, "python")
		s.mu.Unlock()
	}

	wrapperPath := "/tmp/repl-wrapper.py"
	_, err := s.platform.Exec(ctx, platform.ExecRequest{
		SessionID:     sessionID,
		ContainerName: s.mainContainer,
		Command:       []string{"/bin/sh", "-c", fmt.Sprintf("cat > %s << 'WRAPPER_EOF'\n%s\nWRAPPER_EOF", wrapperPath, pythonREPLWrapper)},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to inject REPL wrapper: %w", err)
	}

	proc, err := s.platform.StartProcess(ctx, platform.StartProcessRequest{
		SessionID:     sessionID,
		ContainerName: s.mainContainer,
		ExecID:        fmt.Sprintf("%s-python", sessionID),
		Command:       []string{"/usr/local/bin/python3", wrapperPath},
		Terminal:      false,
	})
	if err != nil {
		return nil, err
	}

	state := &ProcessState{
		ExecID:     proc.ExecID,
		StartedAt:  proc.StartedAt,
		LastUsedAt: proc.StartedAt,
	}

	s.mu.Lock()
	if session.REPLs == nil {
		session.REPLs = make(map[string]*ProcessState)
	}
	session.REPLs["python"] = state
	s.mu.Unlock()
	return state, nil
}

func (s *Service) updateProcessLastUsed(sessionID, process string) {
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	session := s.sessions[sessionID]
	if session == nil {
		return
	}
	switch process {
	case "shell":
		if session.Shell != nil {
			session.Shell.LastUsedAt = now
		}
	case "python":
		if session.REPLs != nil && session.REPLs["python"] != nil {
			session.REPLs["python"].LastUsedAt = now
		}
	}
}

func (s *Service) readUntilMarker(ctx context.Context, sessionID, execID, marker string) (string, int, error) {
	var stdoutBuf bytes.Buffer
	var stderrBuf bytes.Buffer
	pattern := regexp.MustCompile(`<<<EXIT:(\d+):` + regexp.QuoteMeta(marker) + `>>>\r?\n?`)

	// Read stdout and stderr separately with early exit when marker is found.
	// The marker appears in stdout, so we prioritize reading stdout.
	// Stderr is read opportunistically without blocking.
	for {
		if ctx.Err() != nil {
			return stdoutBuf.String() + stderrBuf.String(), -1, ctx.Err()
		}

		// Read stdout (this is where the marker will be)
		stdout, err := s.platform.ReadStdout(ctx, sessionID, execID)
		if err != nil {
			return stdoutBuf.String() + stderrBuf.String(), -1, err
		}
		if len(stdout) > 0 {
			stdoutBuf.Write(stdout)
		}

		// Check for marker in stdout - if found, return immediately
		content := stdoutBuf.String()
		if matches := pattern.FindStringSubmatch(content); matches != nil {
			exitCode, _ := strconv.Atoi(matches[1])
			output := pattern.ReplaceAllString(content, "")
			// Include any stderr we've collected
			if stderrBuf.Len() > 0 {
				output = output + stderrBuf.String()
			}
			return output, exitCode, nil
		}

		// Read stderr opportunistically (non-blocking due to short timeout)
		stderr, _ := s.platform.ReadStderr(ctx, sessionID, execID)
		if len(stderr) > 0 {
			stderrBuf.Write(stderr)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (s *Service) readUntilDone(ctx context.Context, sessionID, execID string) (*platform.ProcessOutput, error) {
	var stdoutBuf bytes.Buffer
	var stderrBuf bytes.Buffer

	for {
		if ctx.Err() != nil {
			return &platform.ProcessOutput{Stdout: stdoutBuf.Bytes(), Stderr: stderrBuf.Bytes()}, ctx.Err()
		}

		// Read stdout (this is where the DONE marker will be)
		stdout, err := s.platform.ReadStdout(ctx, sessionID, execID)
		if err != nil {
			return &platform.ProcessOutput{Stdout: stdoutBuf.Bytes(), Stderr: stderrBuf.Bytes()}, err
		}
		if len(stdout) > 0 {
			stdoutBuf.Write(stdout)
		}

		// Check for marker in stdout - if found, return immediately
		if bytes.Contains(stdoutBuf.Bytes(), []byte("<<<DONE>>>")) {
			result := bytes.Replace(stdoutBuf.Bytes(), []byte("<<<DONE>>>\n"), []byte(""), 1)
			return &platform.ProcessOutput{
				Stdout: result,
				Stderr: stderrBuf.Bytes(),
			}, nil
		}

		// Read stderr opportunistically
		stderr, _ := s.platform.ReadStderr(ctx, sessionID, execID)
		if len(stderr) > 0 {
			stderrBuf.Write(stderr)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (s *Service) initShell(ctx context.Context, sessionID, execID string) error {
	token := fmt.Sprintf("<<<READY_%s>>>", randomString(8))
	setup := fmt.Sprintf(shellInitScript, token)
	if err := s.platform.WriteToProcess(ctx, sessionID, execID, []byte(setup)); err != nil {
		return err
	}
	_, err := s.readUntilToken(ctx, sessionID, execID, token)
	return err
}

func (s *Service) drainProcessOutput(ctx context.Context, sessionID, execID string, max time.Duration) {
	deadline := time.Now().Add(max)
	for time.Now().Before(deadline) {
		stdout, err := s.platform.ReadStdout(ctx, sessionID, execID)
		if err != nil {
			return
		}
		// For shell (TTY), all output is on stdout, so we only need to drain that
		if len(stdout) == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (s *Service) readUntilToken(ctx context.Context, sessionID, execID, token string) (string, error) {
	var stdoutBuf bytes.Buffer
	for {
		if ctx.Err() != nil {
			return stdoutBuf.String(), ctx.Err()
		}

		// Read stdout (this is where the token will be for shell)
		stdout, err := s.platform.ReadStdout(ctx, sessionID, execID)
		if err != nil {
			return stdoutBuf.String(), err
		}
		if len(stdout) > 0 {
			stdoutBuf.Write(stdout)
		}

		content := stdoutBuf.String()
		if strings.Contains(content, token) {
			content = strings.Replace(content, token, "", 1)
			return content, nil
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (s *Service) waitForProcessExit(ctx context.Context, sessionID, execID string, max time.Duration) error {
	deadline := time.Now().Add(max)
	for time.Now().Before(deadline) {
		alive, err := s.platform.IsProcessAlive(ctx, sessionID, execID)
		if err != nil {
			if errors.Is(err, platform.ErrNotFound) {
				return nil
			}
			return err
		}
		if !alive {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return errors.New("shell reset timed out: process still alive")
}

func randomString(length int) string {
	if length <= 0 {
		return ""
	}
	bytesLen := (length + 1) / 2
	buf := make([]byte, bytesLen)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)[:length]
}

func isTimeoutErr(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "timeout")
}

func normalizeNewlines(value string) string {
	value = strings.ReplaceAll(value, "\r\n", "\n") // Windows CRLF → Unix LF
	value = strings.ReplaceAll(value, "\r", "")     // Strip standalone CR (TTY cursor movement)
	return value
}

func normalizeShellOutput(value string) string {
	value = normalizeNewlines(value)
	value = strings.ReplaceAll(value, "\u001b[?2004h", "")
	value = strings.ReplaceAll(value, "\u001b[?2004l", "")
	return value
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
			if errors.Is(err, platform.ErrNotFound) {
				select {
				case <-ctx.Done():
					return fmt.Errorf("sandbox not ready: %w", ctx.Err())
				case <-ticker.C:
					continue
				}
			}
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

// shellQuote safely quotes a string for use in shell commands.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'"
}

// StartJob starts a background job that inherits the shell's environment.
func (s *Service) StartJob(ctx context.Context, sessionID, command, name string) (*JobState, error) {
	if sessionID == "" {
		return nil, errors.New("session_id is required")
	}
	if strings.TrimSpace(command) == "" {
		return nil, errors.New("command is required")
	}

	// Ensure shell is running so we can inherit its environment
	if _, err := s.ensureShell(ctx, sessionID); err != nil {
		return nil, fmt.Errorf("failed to ensure shell: %w", err)
	}

	session := s.getSession(sessionID)
	jobID := randomString(12)
	now := time.Now().UTC()

	// Clean up old completed jobs before adding new one
	s.cleanupOldJobs(ctx, session)

	// Spawn the job from within the shell so it inherits the environment.
	// Jobs are children of the shell and will be killed when the shell is reset.
	spawnCmd := fmt.Sprintf(
		"mkdir -p /tmp/jobs && sh -c %s > /tmp/jobs/%s.stdout 2> /tmp/jobs/%s.stderr & echo \"JOB_PID:$!\"",
		shellQuote(command),
		jobID,
		jobID,
	)

	result, err := s.ExecShell(ctx, sessionID, spawnCmd, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to spawn job: %w", err)
	}

	// Parse PID from output
	pid := 0
	lines := strings.Split(result.Output, "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "JOB_PID:") {
			pidStr := strings.TrimPrefix(line, "JOB_PID:")
			pid, _ = strconv.Atoi(strings.TrimSpace(pidStr))
			break
		}
	}
	if pid == 0 {
		return nil, fmt.Errorf("failed to get job PID from output: %s", result.Output)
	}

	job := &JobState{
		JobID:     jobID,
		PID:       pid,
		Name:      name,
		Command:   command,
		Status:    "running",
		StartedAt: now,
	}

	s.mu.Lock()
	if session.Jobs == nil {
		session.Jobs = make(map[string]*JobState)
	}
	session.Jobs[jobID] = job
	s.mu.Unlock()

	return job, nil
}

// ListJobs returns all jobs for a session.
func (s *Service) ListJobs(ctx context.Context, sessionID string) ([]*JobState, error) {
	if sessionID == "" {
		return nil, errors.New("session_id is required")
	}

	session := s.getSession(sessionID)

	s.mu.Lock()
	jobs := make([]*JobState, 0, len(session.Jobs))
	for _, job := range session.Jobs {
		jobs = append(jobs, job)
	}
	s.mu.Unlock()

	// Refresh status for running jobs
	for _, job := range jobs {
		if job.Status == "running" {
			s.refreshJobStatus(ctx, sessionID, job)
		}
	}

	// Sort by StartedAt descending (newest first)
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].StartedAt.After(jobs[j].StartedAt)
	})

	return jobs, nil
}

// GetJob returns a specific job by ID.
func (s *Service) GetJob(ctx context.Context, sessionID, jobID string) (*JobState, error) {
	if sessionID == "" {
		return nil, errors.New("session_id is required")
	}
	if jobID == "" {
		return nil, errors.New("job_id is required")
	}

	session := s.getSession(sessionID)

	s.mu.Lock()
	job, ok := session.Jobs[jobID]
	s.mu.Unlock()

	if !ok {
		return nil, platform.ErrNotFound
	}

	// Refresh status if running
	if job.Status == "running" {
		s.refreshJobStatus(ctx, sessionID, job)
	}

	return job, nil
}

// GetJobLogs returns the stdout and stderr logs for a job.
func (s *Service) GetJobLogs(ctx context.Context, sessionID, jobID string, tail int) (stdout, stderr string, err error) {
	if sessionID == "" {
		return "", "", errors.New("session_id is required")
	}
	if jobID == "" {
		return "", "", errors.New("job_id is required")
	}

	session := s.getSession(sessionID)

	s.mu.Lock()
	job, ok := session.Jobs[jobID]
	s.mu.Unlock()

	if !ok {
		return "", "", platform.ErrNotFound
	}

	// Read stdout
	stdoutCmd := fmt.Sprintf("cat /tmp/jobs/%s.stdout 2>/dev/null || true", job.JobID)
	if tail > 0 {
		stdoutCmd = fmt.Sprintf("tail -n %d /tmp/jobs/%s.stdout 2>/dev/null || true", tail, job.JobID)
	}
	stdoutResult, err := s.platform.Exec(ctx, platform.ExecRequest{
		SessionID:     sessionID,
		ContainerName: s.shellContainer,
		Command:       []string{"/bin/sh", "-c", stdoutCmd},
		Timeout:       10 * time.Second,
	})
	if err != nil {
		return "", "", fmt.Errorf("failed to read stdout: %w", err)
	}
	stdout = stdoutResult.Stdout

	// Read stderr
	stderrCmd := fmt.Sprintf("cat /tmp/jobs/%s.stderr 2>/dev/null || true", job.JobID)
	if tail > 0 {
		stderrCmd = fmt.Sprintf("tail -n %d /tmp/jobs/%s.stderr 2>/dev/null || true", tail, job.JobID)
	}
	stderrResult, err := s.platform.Exec(ctx, platform.ExecRequest{
		SessionID:     sessionID,
		ContainerName: s.shellContainer,
		Command:       []string{"/bin/sh", "-c", stderrCmd},
		Timeout:       10 * time.Second,
	})
	if err != nil {
		return stdout, "", fmt.Errorf("failed to read stderr: %w", err)
	}
	stderr = stderrResult.Stdout

	return stdout, stderr, nil
}

// KillJob terminates a running job.
func (s *Service) KillJob(ctx context.Context, sessionID, jobID string) error {
	if sessionID == "" {
		return errors.New("session_id is required")
	}
	if jobID == "" {
		return errors.New("job_id is required")
	}

	session := s.getSession(sessionID)

	s.mu.Lock()
	job, ok := session.Jobs[jobID]
	s.mu.Unlock()

	if !ok {
		return platform.ErrNotFound
	}

	// If already not running, return success
	if job.Status != "running" {
		return nil
	}

	// Send SIGTERM
	killCmd := fmt.Sprintf("kill -TERM %d 2>/dev/null || true", job.PID)
	_, err := s.platform.Exec(ctx, platform.ExecRequest{
		SessionID:     sessionID,
		ContainerName: s.shellContainer,
		Command:       []string{"/bin/sh", "-c", killCmd},
		Timeout:       5 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("failed to kill job: %w", err)
	}

	// Update job status
	s.mu.Lock()
	job.Status = "killed"
	job.FinishedAt = time.Now().UTC()
	s.mu.Unlock()

	return nil
}

// refreshJobStatus checks if a running job has completed and updates its status.
func (s *Service) refreshJobStatus(ctx context.Context, sessionID string, job *JobState) {
	if job.Status != "running" {
		return
	}

	// Check if process is alive using kill -0
	checkCmd := fmt.Sprintf("kill -0 %d 2>/dev/null; echo $?", job.PID)
	result, err := s.platform.Exec(ctx, platform.ExecRequest{
		SessionID:     sessionID,
		ContainerName: s.shellContainer,
		Command:       []string{"/bin/sh", "-c", checkCmd},
		Timeout:       5 * time.Second,
	})
	if err != nil {
		return // Can't check, leave as running
	}

	exitStr := strings.TrimSpace(result.Stdout)
	if exitStr == "0" {
		// Process is still alive
		return
	}

	// Process has exited, try to get exit code from wait
	// Note: wait may not work if the process was disowned, so we default to unknown
	s.mu.Lock()
	defer s.mu.Unlock()

	job.FinishedAt = time.Now().UTC()

	// Try to determine if it was successful by checking if stderr has content
	stderrCmd := fmt.Sprintf("test -s /tmp/jobs/%s.stderr && echo 'has_errors' || echo 'no_errors'", job.JobID)
	stderrResult, err := s.platform.Exec(ctx, platform.ExecRequest{
		SessionID:     sessionID,
		ContainerName: s.shellContainer,
		Command:       []string{"/bin/sh", "-c", stderrCmd},
		Timeout:       5 * time.Second,
	})

	if err == nil && strings.TrimSpace(stderrResult.Stdout) == "has_errors" {
		job.Status = "failed"
	} else {
		job.Status = "completed"
	}
	exitCode := 0
	job.ExitCode = &exitCode
}

// cleanupOldJobs removes old completed jobs when over the limit.
func (s *Service) cleanupOldJobs(ctx context.Context, session *Session) {
	s.mu.Lock()

	if session.Jobs == nil {
		s.mu.Unlock()
		return
	}

	var completed []*JobState
	for _, job := range session.Jobs {
		if job.Status != "running" {
			completed = append(completed, job)
		}
	}

	if len(completed) < maxCompletedJobs {
		s.mu.Unlock()
		return
	}

	sort.Slice(completed, func(i, j int) bool {
		return completed[i].FinishedAt.Before(completed[j].FinishedAt)
	})

	toRemove := len(completed) - maxCompletedJobs + 1
	jobIDs := make([]string, 0, toRemove)
	for i := 0; i < toRemove; i++ {
		job := completed[i]
		delete(session.Jobs, job.JobID)
		jobIDs = append(jobIDs, job.JobID)
	}
	sessionID := session.SessionID
	s.mu.Unlock()

	if len(jobIDs) > 0 {
		go func() {
			var parts []string
			for _, id := range jobIDs {
				parts = append(parts, fmt.Sprintf("/tmp/jobs/%s.stdout", id), fmt.Sprintf("/tmp/jobs/%s.stderr", id))
			}
			cleanupCmd := fmt.Sprintf("rm -f %s 2>/dev/null || true", strings.Join(parts, " "))
			_, _ = s.platform.Exec(context.Background(), platform.ExecRequest{
				SessionID:     sessionID,
				ContainerName: s.shellContainer,
				Command:       []string{"/bin/sh", "-c", cleanupCmd},
				Timeout:       10 * time.Second,
			})
		}()
	}
}
