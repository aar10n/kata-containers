package api

import (
	"encoding/json"
	"errors"
	"net/http"
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

type ExecResponse struct {
	ExitCode int    `json:"exit_code"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
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
}

type SessionsResponse struct {
	Sessions []SessionEntry `json:"sessions"`
}

type SessionEntry struct {
	SessionID string `json:"session_id"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
}

type DeleteResponse struct {
	Deleted bool `json:"deleted"`
}

type HealthResponse struct {
	Status string `json:"status"`
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
			SessionID: item.SessionID,
			Status:    string(item.Status),
			CreatedAt: item.CreatedAt.UTC().Format(time.RFC3339),
		})
	}
	writeJSON(w, http.StatusOK, resp)
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

	sessionID := parts[0]
	action := ""
	if len(parts) > 1 {
		action = parts[1]
	}

	switch {
	case action == "exec" && r.Method == http.MethodPost:
		s.handleExec(w, r, sessionID)
	case action == "status" && r.Method == http.MethodGet:
		s.handleStatus(w, r, sessionID)
	case action == "" && r.Method == http.MethodDelete:
		s.handleDelete(w, r, sessionID)
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
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
	})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, sessionID string) {
	if err := s.svc.DeleteSession(r.Context(), sessionID); err != nil && !errors.Is(err, platform.ErrNotFound) {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, DeleteResponse{Deleted: true})
}

func writeServiceError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, platform.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, platform.ErrAlreadyExists):
		status = http.StatusConflict
	case errors.Is(err, platform.ErrNotReady):
		status = http.StatusInternalServerError
	}
	writeError(w, status, err.Error())
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, ErrorResponse{Error: message})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
