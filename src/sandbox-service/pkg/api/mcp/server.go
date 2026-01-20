package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/service"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// Server wraps the MCP server with sandbox-service integration.
type Server struct {
	svc       *service.Service
	mcpServer *server.MCPServer
	sseServer *server.SSEServer
}

// NewServer creates a new MCP server with sandbox tools.
func NewServer(svc *service.Service) *Server {
	s := &Server{svc: svc}

	// Create MCP server
	s.mcpServer = server.NewMCPServer(
		"sandbox-service",
		"1.0.0",
		server.WithToolCapabilities(true),
	)

	// Register tools
	s.registerTools()

	return s
}

// ListenAndServe starts the MCP server on the given address.
func (s *Server) ListenAndServe(addr string) error {
	s.sseServer = server.NewSSEServer(s.mcpServer)

	httpServer := &http.Server{
		Addr:              addr,
		Handler:           s.sseServer,
		ReadHeaderTimeout: 5 * time.Second,
	}

	return httpServer.ListenAndServe()
}

// Handler returns the HTTP handler for the MCP server.
func (s *Server) Handler() http.Handler {
	if s.sseServer == nil {
		s.sseServer = server.NewSSEServer(s.mcpServer)
	}
	return s.sseServer
}

func (s *Server) registerTools() {
	// shell - Execute shell command
	s.mcpServer.AddTool(
		mcp.NewTool("shell",
			mcp.WithDescription("Execute a shell command in the sandbox. The shell maintains state between calls (environment variables, working directory, etc.)."),
			mcp.WithDestructiveHintAnnotation(false),
			mcp.WithOpenWorldHintAnnotation(false),
			mcp.WithString("command",
				mcp.Required(),
				mcp.Description("Shell command to execute"),
			),
			mcp.WithNumber("timeout_ms",
				mcp.Description("Timeout in milliseconds (default 30000)"),
			),
		),
		s.handleShell,
	)

	// python - Execute Python code
	s.mcpServer.AddTool(
		mcp.NewTool("python",
			mcp.WithDescription("Execute Python code in a stateful REPL. Variables and imports persist between calls."),
			mcp.WithDestructiveHintAnnotation(false),
			mcp.WithOpenWorldHintAnnotation(false),
			mcp.WithString("code",
				mcp.Required(),
				mcp.Description("Python code to execute"),
			),
			mcp.WithNumber("timeout_ms",
				mcp.Description("Timeout in milliseconds (default 30000)"),
			),
		),
		s.handlePython,
	)

	// shell_reset - Reset shell state
	s.mcpServer.AddTool(
		mcp.NewTool("shell_reset",
			mcp.WithDescription("Reset the shell to a clean state. Clears environment variables, working directory changes, and kills any background jobs."),
			mcp.WithDestructiveHintAnnotation(false),
			mcp.WithOpenWorldHintAnnotation(false),
		),
		s.handleShellReset,
	)

	// python_reset - Reset Python REPL
	s.mcpServer.AddTool(
		mcp.NewTool("python_reset",
			mcp.WithDescription("Reset the Python REPL. Clears all variables, imports, and state."),
			mcp.WithDestructiveHintAnnotation(false),
			mcp.WithOpenWorldHintAnnotation(false),
		),
		s.handlePythonReset,
	)

	// job_start - Start background job
	s.mcpServer.AddTool(
		mcp.NewTool("job_start",
			mcp.WithDescription("Start a long-running command as a background job. Jobs inherit the shell's environment and run independently."),
			mcp.WithDestructiveHintAnnotation(false),
			mcp.WithOpenWorldHintAnnotation(false),
			mcp.WithString("command",
				mcp.Required(),
				mcp.Description("Command to run in background"),
			),
			mcp.WithString("name",
				mcp.Description("Optional human-readable name for the job"),
			),
		),
		s.handleJobStart,
	)

	// job_status - Get job status and logs
	s.mcpServer.AddTool(
		mcp.NewTool("job_status",
			mcp.WithDescription("Get the status and logs of a background job."),
			mcp.WithReadOnlyHintAnnotation(true),
			mcp.WithDestructiveHintAnnotation(false),
			mcp.WithOpenWorldHintAnnotation(false),
			mcp.WithString("job_id",
				mcp.Required(),
				mcp.Description("Job ID returned from job_start"),
			),
			mcp.WithNumber("tail_logs",
				mcp.Description("Number of log lines to return (default: all)"),
			),
		),
		s.handleJobStatus,
	)

	// job_kill - Kill running job
	s.mcpServer.AddTool(
		mcp.NewTool("job_kill",
			mcp.WithDescription("Terminate a running background job."),
			mcp.WithDestructiveHintAnnotation(false),
			mcp.WithOpenWorldHintAnnotation(false),
			mcp.WithString("job_id",
				mcp.Required(),
				mcp.Description("Job ID to terminate"),
			),
		),
		s.handleJobKill,
	)

	// jobs_list - List all jobs
	s.mcpServer.AddTool(
		mcp.NewTool("jobs_list",
			mcp.WithDescription("List all background jobs for the session."),
			mcp.WithReadOnlyHintAnnotation(true),
			mcp.WithDestructiveHintAnnotation(false),
			mcp.WithOpenWorldHintAnnotation(false),
		),
		s.handleJobsList,
	)
}

// getSessionID extracts the session ID from the MCP client session.
func getSessionID(ctx context.Context) (string, error) {
	clientSession := server.ClientSessionFromContext(ctx)
	if clientSession == nil {
		return "", fmt.Errorf("no MCP client session found")
	}
	return clientSession.SessionID(), nil
}

func (s *Server) handleShell(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	sessionID, err := getSessionID(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	command, err := req.RequireString("command")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	timeoutMs := int(req.GetFloat("timeout_ms", 0))
	timeout := time.Duration(timeoutMs) * time.Millisecond

	result, err := s.svc.ExecShell(ctx, sessionID, command, timeout, "")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	output := ShellOutput{
		ExitCode: result.ExitCode,
		Stdout:   result.Output,
		Stderr:   result.Error,
	}

	return jsonResult(output)
}

func (s *Server) handlePython(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	sessionID, err := getSessionID(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	code, err := req.RequireString("code")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	timeoutMs := int(req.GetFloat("timeout_ms", 0))
	timeout := time.Duration(timeoutMs) * time.Millisecond

	result, err := s.svc.ExecPython(ctx, sessionID, code, timeout, "")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	output := PythonOutput{
		Output: result.Output,
		Error:  result.Error,
	}

	return jsonResult(output)
}

func (s *Server) handleShellReset(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	sessionID, err := getSessionID(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	err = s.svc.ResetShell(ctx, sessionID)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	output := ShellResetOutput{
		Reset: true,
	}

	return jsonResult(output)
}

func (s *Server) handlePythonReset(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	sessionID, err := getSessionID(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	err = s.svc.ResetPython(ctx, sessionID)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	output := PythonResetOutput{
		Reset: true,
	}

	return jsonResult(output)
}

func (s *Server) handleJobStart(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	sessionID, err := getSessionID(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	command, err := req.RequireString("command")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	name := req.GetString("name", "")

	job, err := s.svc.StartJob(ctx, sessionID, command, name, "")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	output := JobStartOutput{
		JobID: job.JobID,
		PID:   job.PID,
		Name:  job.Name,
	}

	return jsonResult(output)
}

func (s *Server) handleJobStatus(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	sessionID, err := getSessionID(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	jobID, err := req.RequireString("job_id")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	tailLogs := int(req.GetFloat("tail_logs", 0))

	job, err := s.svc.GetJob(ctx, sessionID, jobID)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	stdout, stderr, err := s.svc.GetJobLogs(ctx, sessionID, jobID, tailLogs)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	output := JobStatusOutput{
		JobID:    job.JobID,
		Status:   job.Status,
		PID:      job.PID,
		Name:     job.Name,
		ExitCode: job.ExitCode,
		Stdout:   stdout,
		Stderr:   stderr,
	}

	return jsonResult(output)
}

func (s *Server) handleJobKill(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	sessionID, err := getSessionID(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	jobID, err := req.RequireString("job_id")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	err = s.svc.KillJob(ctx, sessionID, jobID)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	output := JobKillOutput{
		JobID:  jobID,
		Killed: true,
	}

	return jsonResult(output)
}

func (s *Server) handleJobsList(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	sessionID, err := getSessionID(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	jobs, err := s.svc.ListJobs(ctx, sessionID)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	summaries := make([]JobSummary, 0, len(jobs))
	for _, job := range jobs {
		summaries = append(summaries, JobSummary{
			JobID:   job.JobID,
			Name:    job.Name,
			Command: job.Command,
			Status:  job.Status,
			PID:     job.PID,
		})
	}

	output := JobsListOutput{
		Jobs: summaries,
	}

	return jsonResult(output)
}

func jsonResult(v any) (*mcp.CallToolResult, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to marshal result: %v", err)), nil
	}
	return mcp.NewToolResultText(string(data)), nil
}
