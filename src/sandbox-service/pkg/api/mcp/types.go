package mcp

// Input types for MCP tools

// ShellInput is the input for the shell tool.
type ShellInput struct {
	Command   string `json:"command" jsonschema:"required,description=Shell command to execute"`
	TimeoutMs int    `json:"timeout_ms,omitempty" jsonschema:"description=Timeout in milliseconds (default 30000)"`
}

// PythonInput is the input for the python tool.
type PythonInput struct {
	Code      string `json:"code" jsonschema:"required,description=Python code to execute"`
	TimeoutMs int    `json:"timeout_ms,omitempty" jsonschema:"description=Timeout in milliseconds (default 30000)"`
}

// JobStartInput is the input for the job_start tool.
type JobStartInput struct {
	Command string `json:"command" jsonschema:"required,description=Command to run in background"`
	Name    string `json:"name,omitempty" jsonschema:"description=Optional human-readable name for the job"`
}

// JobStatusInput is the input for the job_status tool.
type JobStatusInput struct {
	JobID    string `json:"job_id" jsonschema:"required,description=Job ID returned from job_start"`
	TailLogs int    `json:"tail_logs,omitempty" jsonschema:"description=Number of log lines to return (default: all)"`
}

// JobKillInput is the input for the job_kill tool.
type JobKillInput struct {
	JobID string `json:"job_id" jsonschema:"required,description=Job ID to terminate"`
}

// Output types for MCP tools

// ShellOutput is the output for the shell tool.
type ShellOutput struct {
	ExitCode int    `json:"exit_code"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
}

// PythonOutput is the output for the python tool.
type PythonOutput struct {
	Output string `json:"output"`
	Error  string `json:"error"`
}

// ShellResetOutput is the output for the shell_reset tool.
type ShellResetOutput struct {
	Reset bool `json:"reset"`
}

// PythonResetOutput is the output for the python_reset tool.
type PythonResetOutput struct {
	Reset bool `json:"reset"`
}

// JobStartOutput is the output for the job_start tool.
type JobStartOutput struct {
	JobID string `json:"job_id"`
	PID   int    `json:"pid"`
	Name  string `json:"name,omitempty"`
}

// JobStatusOutput is the output for the job_status tool.
type JobStatusOutput struct {
	JobID    string `json:"job_id"`
	Status   string `json:"status"`
	PID      int    `json:"pid"`
	Name     string `json:"name,omitempty"`
	ExitCode *int   `json:"exit_code,omitempty"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
}

// JobKillOutput is the output for the job_kill tool.
type JobKillOutput struct {
	JobID  string `json:"job_id"`
	Killed bool   `json:"killed"`
}

// JobsListOutput is the output for the jobs_list tool.
type JobsListOutput struct {
	Jobs []JobSummary `json:"jobs"`
}

// JobSummary is a summary of a job for the jobs_list output.
type JobSummary struct {
	JobID   string `json:"job_id"`
	Name    string `json:"name,omitempty"`
	Command string `json:"command"`
	Status  string `json:"status"`
	PID     int    `json:"pid"`
}
