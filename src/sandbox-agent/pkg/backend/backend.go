package backend

import (
	"context"
	"errors"
	"io"
	"os"
	"time"
)

// ErrNotSupported is returned when an operation is not supported by the backend.
var ErrNotSupported = errors.New("operation not supported by this backend")

// ErrNotInDataVolume is returned when a file path is outside the emptyDir data volume.
var ErrNotInDataVolume = errors.New("path is outside the data volume")

// ExecResult holds the result of a command execution.
type ExecResult struct {
	Stdout   string
	Stderr   string
	ExitCode int32
}

// Process represents a long-running process with streaming I/O.
type Process struct {
	ID          string
	ContainerID string
}

// StreamChunk represents a chunk of streamed data.
type StreamChunk struct {
	Data []byte
	EOF  bool
}

// OutputChunk represents a chunk from stdout or stderr.
type OutputChunk struct {
	Stream StreamType
	Data   []byte
	EOF    bool
}

// StreamType indicates stdout or stderr.
type StreamType int

const (
	StreamStdout StreamType = iota
	StreamStderr
)

// ExecutionBackend defines the interface for executing commands in containers.
// This abstraction allows different implementations for Kata VMs (via kata-agent)
// and regular pods (via CRI).
type ExecutionBackend interface {
	// Exec executes a command and waits for completion.
	// This is for one-shot commands where we want to capture all output.
	Exec(ctx context.Context, containerID string, cmd []string, env []string, cwd string, timeout time.Duration) (*ExecResult, error)

	// StartProcess starts a long-running process for interactive I/O.
	StartProcess(ctx context.Context, containerID string, cmd []string, env []string, cwd string, tty bool) (*Process, error)

	// WriteToProcess writes data to a process's stdin.
	WriteToProcess(ctx context.Context, containerID, processID string, data []byte) error

	// ReadStdout reads available stdout data from a process (non-blocking).
	ReadStdout(ctx context.Context, containerID, processID string, maxBytes int) ([]byte, error)

	// ReadStderr reads available stderr data from a process (non-blocking).
	ReadStderr(ctx context.Context, containerID, processID string, maxBytes int) ([]byte, error)

	// StreamStdout returns a channel that streams stdout data.
	StreamStdout(ctx context.Context, containerID, processID string) (<-chan StreamChunk, error)

	// StreamStderr returns a channel that streams stderr data.
	StreamStderr(ctx context.Context, containerID, processID string) (<-chan StreamChunk, error)

	// StreamOutput returns a channel that streams both stdout and stderr data.
	// This is more efficient than calling StreamStdout and StreamStderr separately
	// as it uses a single goroutine and avoids coordination issues.
	StreamOutput(ctx context.Context, containerID, processID string) (<-chan OutputChunk, error)

	// CloseStdin closes the stdin of a process.
	CloseStdin(ctx context.Context, containerID, processID string) error

	// KillProcess terminates a process.
	KillProcess(ctx context.Context, containerID, processID string, signal int) error

	// WaitProcess waits for a process to exit and returns the exit code.
	WaitProcess(ctx context.Context, containerID, processID string) (int32, error)

	// ResizeTerminal resizes the terminal for a TTY process.
	ResizeTerminal(ctx context.Context, containerID, processID string, rows, cols uint32) error

	// SupportsStateOps returns true if this backend supports VM state operations.
	SupportsStateOps() bool

	// SaveState saves the VM/container state (snapshot).
	// Returns ErrNotSupported if the backend doesn't support state operations.
	SaveState(ctx context.Context, sandboxID string) error

	// RestoreState restores the VM/container from a saved state.
	// Returns ErrNotSupported if the backend doesn't support state operations.
	RestoreState(ctx context.Context, sandboxID, snapshotID string) error

	// Close releases any resources held by the backend.
	Close() error
}

// FileBackend defines the interface for file operations in containers.
// This is separate from ExecutionBackend to allow different implementations
// (e.g., direct host filesystem access for emptyDir volumes vs CRI exec for other paths).
type FileBackend interface {
	// ReadFile reads a file from the container.
	ReadFile(ctx context.Context, containerID, path string) ([]byte, error)

	// WriteFile writes a file to the container.
	WriteFile(ctx context.Context, containerID, path string, content []byte, mode os.FileMode) error

	// ReadArchive reads files/directories as a tar archive stream.
	ReadArchive(ctx context.Context, containerID, path string) (io.ReadCloser, error)

	// WriteArchive extracts a tar archive to a directory in the container.
	WriteArchive(ctx context.Context, containerID, destDir string, tarData io.Reader) error
}

// HostFSFileBackend provides direct file access to emptyDir volumes on the host.
// This is the preferred method for file operations as it bypasses exec overhead.
type HostFSFileBackend interface {
	FileBackend

	// DataPath returns the host path for the pod's data volume.
	// Returns empty string if the pod UID is not known.
	DataPath(podUID string) string

	// ReadFileFromData reads a file from the pod's emptyDir data volume.
	ReadFileFromData(ctx context.Context, podUID, relativePath string) ([]byte, error)

	// WriteFileToData writes a file to the pod's emptyDir data volume.
	WriteFileToData(ctx context.Context, podUID, relativePath string, content []byte, mode os.FileMode) error

	// ReadArchiveFromData reads files from the data volume as a tar archive.
	ReadArchiveFromData(ctx context.Context, podUID, relativePath string) (io.ReadCloser, error)

	// WriteArchiveToData extracts a tar archive to the data volume.
	WriteArchiveToData(ctx context.Context, podUID, destDir string, tarData io.Reader) error
}
