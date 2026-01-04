package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/platform"
	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/service"
)

type Server struct {
	svc *service.Service
}

type ExecRequest struct {
	Command    []string          `json:"command"`
	Env        map[string]string `json:"env,omitempty"`
	WorkingDir string            `json:"working_dir,omitempty"`
	TimeoutMs  int64             `json:"timeout_ms,omitempty"`
	Image      string            `json:"image,omitempty"`
}

type ShellRequest struct {
	Command   string `json:"command"`
	TimeoutMs int64  `json:"timeout_ms,omitempty"`
}

type PythonRequest struct {
	Code      string `json:"code"`
	TimeoutMs int64  `json:"timeout_ms,omitempty"`
}

type ExecResponse struct {
	ExitCode int    `json:"exit_code"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
}

type ShellResponse struct {
	Output   string `json:"output"`
	ExitCode int    `json:"exit_code"`
	Error    string `json:"error,omitempty"`
}

type REPLResponse struct {
	Output string `json:"output"`
	Error  string `json:"error"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

type StatusResponse struct {
	SessionID   string `json:"session_id"`
	SandboxID   string `json:"sandbox_id"`
	ContainerID string `json:"container_id"`
	Status      string `json:"status"`
	Host        string `json:"host"`
	CreatedAt   string `json:"created_at"`
	LastUsedAt  string `json:"last_used_at"`
}

type SessionsResponse struct {
	Sessions []SessionEntry `json:"sessions"`
}

type SessionEntry struct {
	SessionID  string `json:"session_id"`
	Status     string `json:"status"`
	CreatedAt  string `json:"created_at"`
	LastUsedAt string `json:"last_used_at"`
}

type DeleteResponse struct {
	Deleted bool `json:"deleted"`
}

type ResetResponse struct {
	Reset   bool   `json:"reset"`
	Message string `json:"message"`
}

type ResizeResponse struct {
	Resized bool `json:"resized"`
}

type HealthResponse struct {
	Status string `json:"status"`
}

type ProcessStatusResponse struct {
	Alive      bool   `json:"alive"`
	ExecID     string `json:"exec_id"`
	StartedAt  string `json:"started_at,omitempty"`
	LastUsedAt string `json:"last_used_at,omitempty"`
	Message    string `json:"message,omitempty"`
}

// Job API types

type StartJobRequest struct {
	Command string `json:"command"`
	Name    string `json:"name,omitempty"`
}

type StartJobResponse struct {
	JobID     string `json:"job_id"`
	PID       int    `json:"pid"`
	Name      string `json:"name,omitempty"`
	StartedAt string `json:"started_at"`
}

type ListJobsResponse struct {
	Jobs []JobEntry `json:"jobs"`
}

type JobEntry struct {
	JobID      string `json:"job_id"`
	PID        int    `json:"pid"`
	Name       string `json:"name,omitempty"`
	Command    string `json:"command"`
	Status     string `json:"status"`
	ExitCode   *int   `json:"exit_code,omitempty"`
	StartedAt  string `json:"started_at"`
	FinishedAt string `json:"finished_at,omitempty"`
}

type JobStatusResponse struct {
	JobEntry
}

type JobLogsResponse struct {
	Stdout string `json:"stdout"`
	Stderr string `json:"stderr"`
}

type KillJobResponse struct {
	Killed  bool   `json:"killed"`
	Message string `json:"message,omitempty"`
}

func NewServer(svc *service.Service) *Server {
	return &Server{svc: svc}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/v1/sessions", s.handleSessions)
	mux.HandleFunc("/v1/", s.handleSession)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, HealthResponse{Status: "ok"})
}

func (s *Server) handleSessions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	items, err := s.svc.ListSessions(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	resp := SessionsResponse{Sessions: make([]SessionEntry, 0, len(items))}
	for _, item := range items {
		resp.Sessions = append(resp.Sessions, SessionEntry{
			SessionID:  item.SessionID,
			Status:     string(item.Status),
			CreatedAt:  item.CreatedAt.UTC().Format(time.RFC3339),
			LastUsedAt: item.LastUsedAt.UTC().Format(time.RFC3339),
		})
	}
	writeJSON(w, http.StatusOK, resp)
}

type routeParams struct {
	sessionID string
	jobID     string
}

func (s *Server) handleSession(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/")
	if path == "" || path == r.URL.Path {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}

	params := routeParams{sessionID: parts[0]}
	route := strings.Join(parts[1:], "/")

	type routeEntry struct {
		method  string
		pattern string
		handler func(http.ResponseWriter, *http.Request, routeParams)
	}

	routes := []routeEntry{
		{http.MethodDelete, "", s.handleDeleteSession},
		{http.MethodPost, "exec", s.handleExecRoute},
		{http.MethodGet, "status", s.handleStatusRoute},
		{http.MethodPost, "shell", s.handleShellExecRoute},
		{http.MethodPost, "shell/reset", s.handleShellResetRoute},
		{http.MethodGet, "shell/status", s.handleShellStatusRoute},
		{http.MethodPost, "shell/resize", s.handleShellResizeRoute},
		{http.MethodPost, "shell/jobs", s.handleStartJobRoute},
		{http.MethodGet, "shell/jobs", s.handleListJobsRoute},
		{http.MethodPost, "repl/python", s.handlePythonExecRoute},
		{http.MethodPost, "repl/python/reset", s.handlePythonResetRoute},
		{http.MethodGet, "repl/python/status", s.handlePythonStatusRoute},
	}

	for _, entry := range routes {
		if r.Method == entry.method && route == entry.pattern {
			entry.handler(w, r, params)
			return
		}
	}

	if strings.HasPrefix(route, "shell/jobs/") {
		jobPath := strings.TrimPrefix(route, "shell/jobs/")
		jobParts := strings.Split(jobPath, "/")
		if len(jobParts) >= 1 && jobParts[0] != "" {
			params.jobID = jobParts[0]
			switch {
			case r.Method == http.MethodGet && len(jobParts) == 1:
				s.handleGetJobRoute(w, r, params)
				return
			case r.Method == http.MethodGet && len(jobParts) == 2 && jobParts[1] == "logs":
				s.handleGetJobLogsRoute(w, r, params)
				return
			case r.Method == http.MethodDelete && len(jobParts) == 1:
				s.handleKillJobRoute(w, r, params)
				return
			}
		}
	}

	writeError(w, http.StatusNotFound, "not found")
}

func (s *Server) handleDeleteSession(w http.ResponseWriter, r *http.Request, p routeParams) {
	s.handleDelete(w, r, p.sessionID)
}

func (s *Server) handleExecRoute(w http.ResponseWriter, r *http.Request, p routeParams) {
	s.handleExec(w, r, p.sessionID)
}

func (s *Server) handleStatusRoute(w http.ResponseWriter, r *http.Request, p routeParams) {
	s.handleStatus(w, r, p.sessionID)
}

func (s *Server) handleShellExecRoute(w http.ResponseWriter, r *http.Request, p routeParams) {
	s.handleShellExec(w, r, p.sessionID)
}

func (s *Server) handleShellResetRoute(w http.ResponseWriter, r *http.Request, p routeParams) {
	s.handleShellReset(w, r, p.sessionID)
}

func (s *Server) handleShellStatusRoute(w http.ResponseWriter, r *http.Request, p routeParams) {
	s.handleShellStatus(w, r, p.sessionID)
}

func (s *Server) handleShellResizeRoute(w http.ResponseWriter, r *http.Request, p routeParams) {
	s.handleShellResize(w, r, p.sessionID)
}

func (s *Server) handleStartJobRoute(w http.ResponseWriter, r *http.Request, p routeParams) {
	s.handleStartJob(w, r, p.sessionID)
}

func (s *Server) handleListJobsRoute(w http.ResponseWriter, r *http.Request, p routeParams) {
	s.handleListJobs(w, r, p.sessionID)
}

func (s *Server) handleGetJobRoute(w http.ResponseWriter, r *http.Request, p routeParams) {
	s.handleGetJob(w, r, p.sessionID, p.jobID)
}

func (s *Server) handleGetJobLogsRoute(w http.ResponseWriter, r *http.Request, p routeParams) {
	s.handleGetJobLogs(w, r, p.sessionID, p.jobID)
}

func (s *Server) handleKillJobRoute(w http.ResponseWriter, r *http.Request, p routeParams) {
	s.handleKillJob(w, r, p.sessionID, p.jobID)
}

func (s *Server) handlePythonExecRoute(w http.ResponseWriter, r *http.Request, p routeParams) {
	s.handlePythonExec(w, r, p.sessionID)
}

func (s *Server) handlePythonResetRoute(w http.ResponseWriter, r *http.Request, p routeParams) {
	s.handlePythonReset(w, r, p.sessionID)
}

func (s *Server) handlePythonStatusRoute(w http.ResponseWriter, r *http.Request, p routeParams) {
	s.handlePythonStatus(w, r, p.sessionID)
}

func (s *Server) handleExec(w http.ResponseWriter, r *http.Request, sessionID string) {
	var req ExecRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	input := service.ExecInput{
		Command:    req.Command,
		Env:        req.Env,
		WorkingDir: req.WorkingDir,
		Image:      req.Image,
	}
	if req.TimeoutMs > 0 {
		input.Timeout = time.Duration(req.TimeoutMs) * time.Millisecond
	}

	result, err := s.svc.Exec(r.Context(), sessionID, input)
	if err != nil {
		writeServiceError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, ExecResponse{
		ExitCode: result.ExitCode,
		Stdout:   result.Stdout,
		Stderr:   result.Stderr,
	})
}

func (s *Server) handleShellExec(w http.ResponseWriter, r *http.Request, sessionID string) {
	var req ShellRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	timeout := time.Duration(req.TimeoutMs) * time.Millisecond
	result, err := s.svc.ExecShell(r.Context(), sessionID, req.Command, timeout)
	if err != nil {
		writeServiceError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, ShellResponse{
		Output:   result.Output,
		ExitCode: result.ExitCode,
		Error:    result.Error,
	})
}

func (s *Server) handleShellReset(w http.ResponseWriter, r *http.Request, sessionID string) {
	if err := s.svc.ResetShell(r.Context(), sessionID); err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ResetResponse{
		Reset:   true,
		Message: "shell process restarted",
	})
}

func (s *Server) handleShellStatus(w http.ResponseWriter, r *http.Request, sessionID string) {
	status, err := s.svc.ShellStatus(r.Context(), sessionID)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ProcessStatusResponse{
		Alive:      status.Alive,
		ExecID:     status.ExecID,
		StartedAt:  formatTime(status.StartedAt),
		LastUsedAt: formatTime(status.LastUsedAt),
		Message:    status.Message,
	})
}

func (s *Server) handleShellResize(w http.ResponseWriter, r *http.Request, sessionID string) {
	query := r.URL.Query()
	colValue := strings.TrimSpace(query.Get("x"))
	rowValue := strings.TrimSpace(query.Get("y"))
	if colValue == "" || rowValue == "" {
		writeError(w, http.StatusBadRequest, "x and y are required")
		return
	}
	columns, err := strconv.Atoi(colValue)
	if err != nil || columns <= 0 {
		writeError(w, http.StatusBadRequest, "x must be a positive integer")
		return
	}
	rows, err := strconv.Atoi(rowValue)
	if err != nil || rows <= 0 {
		writeError(w, http.StatusBadRequest, "y must be a positive integer")
		return
	}

	if err := s.svc.ResizeShell(r.Context(), sessionID, uint32(columns), uint32(rows)); err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ResizeResponse{Resized: true})
}

func (s *Server) handlePythonExec(w http.ResponseWriter, r *http.Request, sessionID string) {
	var req PythonRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	timeout := time.Duration(req.TimeoutMs) * time.Millisecond
	result, err := s.svc.ExecPython(r.Context(), sessionID, req.Code, timeout)
	if err != nil {
		writeServiceError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, REPLResponse{
		Output: result.Output,
		Error:  result.Error,
	})
}

func (s *Server) handlePythonReset(w http.ResponseWriter, r *http.Request, sessionID string) {
	if err := s.svc.ResetPython(r.Context(), sessionID); err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ResetResponse{
		Reset:   true,
		Message: "python repl restarted",
	})
}

func (s *Server) handlePythonStatus(w http.ResponseWriter, r *http.Request, sessionID string) {
	status, err := s.svc.PythonStatus(r.Context(), sessionID)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ProcessStatusResponse{
		Alive:      status.Alive,
		ExecID:     status.ExecID,
		StartedAt:  formatTime(status.StartedAt),
		LastUsedAt: formatTime(status.LastUsedAt),
		Message:    status.Message,
	})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request, sessionID string) {
	sandbox, err := s.svc.GetSession(r.Context(), sessionID)
	if err != nil {
		writeServiceError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, StatusResponse{
		SessionID:   sandbox.SessionID,
		SandboxID:   sandbox.SandboxID,
		ContainerID: sandbox.ContainerID,
		Status:      string(sandbox.Status),
		Host:        sandbox.Host,
		CreatedAt:   sandbox.CreatedAt.UTC().Format(time.RFC3339),
		LastUsedAt:  sandbox.LastUsedAt.UTC().Format(time.RFC3339),
	})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, sessionID string) {
	if err := s.svc.DeleteSession(r.Context(), sessionID); err != nil && !errors.Is(err, platform.ErrNotFound) {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, DeleteResponse{Deleted: true})
}

// Job handlers

func (s *Server) handleStartJob(w http.ResponseWriter, r *http.Request, sessionID string) {
	var req StartJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	job, err := s.svc.StartJob(r.Context(), sessionID, req.Command, req.Name)
	if err != nil {
		writeServiceError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, StartJobResponse{
		JobID:     job.JobID,
		PID:       job.PID,
		Name:      job.Name,
		StartedAt: job.StartedAt.UTC().Format(time.RFC3339),
	})
}

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request, sessionID string) {
	jobs, err := s.svc.ListJobs(r.Context(), sessionID)
	if err != nil {
		writeServiceError(w, err)
		return
	}

	entries := make([]JobEntry, 0, len(jobs))
	for _, job := range jobs {
		entries = append(entries, jobToEntry(job))
	}

	writeJSON(w, http.StatusOK, ListJobsResponse{Jobs: entries})
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request, sessionID, jobID string) {
	job, err := s.svc.GetJob(r.Context(), sessionID, jobID)
	if err != nil {
		writeServiceError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, JobStatusResponse{JobEntry: jobToEntry(job)})
}

func (s *Server) handleGetJobLogs(w http.ResponseWriter, r *http.Request, sessionID, jobID string) {
	// Parse optional tail parameter
	tail := 0
	if tailStr := r.URL.Query().Get("tail"); tailStr != "" {
		if parsed, err := strconv.Atoi(tailStr); err == nil && parsed > 0 {
			tail = parsed
		}
	}

	stdout, stderr, err := s.svc.GetJobLogs(r.Context(), sessionID, jobID, tail)
	if err != nil {
		writeServiceError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, JobLogsResponse{
		Stdout: stdout,
		Stderr: stderr,
	})
}

func (s *Server) handleKillJob(w http.ResponseWriter, r *http.Request, sessionID, jobID string) {
	err := s.svc.KillJob(r.Context(), sessionID, jobID)
	if err != nil {
		writeServiceError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, KillJobResponse{
		Killed:  true,
		Message: "job terminated",
	})
}

func jobToEntry(job *service.JobState) JobEntry {
	entry := JobEntry{
		JobID:     job.JobID,
		PID:       job.PID,
		Name:      job.Name,
		Command:   job.Command,
		Status:    job.Status,
		ExitCode:  job.ExitCode,
		StartedAt: job.StartedAt.UTC().Format(time.RFC3339),
	}
	if !job.FinishedAt.IsZero() {
		entry.FinishedAt = job.FinishedAt.UTC().Format(time.RFC3339)
	}
	return entry
}

func writeServiceError(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	switch {
	case errors.Is(err, platform.ErrNotFound):
		code = http.StatusNotFound
	case errors.Is(err, platform.ErrAlreadyExists):
		code = http.StatusConflict
	case errors.Is(err, platform.ErrNotReady):
		code = http.StatusServiceUnavailable
	case errors.Is(err, context.DeadlineExceeded):
		code = http.StatusGatewayTimeout
	case errors.Is(err, context.Canceled):
		code = 499 // Client Closed Request
	}
	writeError(w, code, err.Error())
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, ErrorResponse{Error: message})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339)
}
