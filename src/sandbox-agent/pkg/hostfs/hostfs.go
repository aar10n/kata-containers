package hostfs

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	// DefaultKubeletRoot is the default kubelet root directory.
	DefaultKubeletRoot = "/var/lib/kubelet"

	// SandboxDataVolumeName is the name of the emptyDir volume for sandbox data.
	SandboxDataVolumeName = "sandbox-data"

	// SandboxDataMountPath is the mount path inside the container.
	SandboxDataMountPath = "/data"
)

// Config holds configuration for the HostFS.
type Config struct {
	// KubeletRoot is the path to the kubelet root directory.
	// Default: /var/lib/kubelet
	KubeletRoot string
}

// HostFS provides direct file access to pod emptyDir volumes on the host.
type HostFS struct {
	kubeletRoot string
}

// New creates a new HostFS.
func New(cfg Config) *HostFS {
	kubeletRoot := cfg.KubeletRoot
	if kubeletRoot == "" {
		kubeletRoot = DefaultKubeletRoot
	}

	return &HostFS{
		kubeletRoot: kubeletRoot,
	}
}

// DataPath returns the host path for a pod's sandbox-data emptyDir volume.
// The path is: /var/lib/kubelet/pods/{pod_uid}/volumes/kubernetes.io~empty-dir/sandbox-data/
func (h *HostFS) DataPath(podUID string) string {
	if podUID == "" {
		return ""
	}
	return filepath.Join(
		h.kubeletRoot,
		"pods",
		podUID,
		"volumes",
		"kubernetes.io~empty-dir",
		SandboxDataVolumeName,
	)
}

// ReadFile reads a file from the pod's emptyDir data volume.
func (h *HostFS) ReadFile(ctx context.Context, podUID, relativePath string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	basePath := h.DataPath(podUID)
	if basePath == "" {
		return nil, fmt.Errorf("invalid pod UID")
	}

	fullPath := filepath.Join(basePath, relativePath)

	// Security check: ensure path doesn't escape the data directory
	if !isSubpath(basePath, fullPath) {
		return nil, fmt.Errorf("path %q escapes data directory", relativePath)
	}

	return os.ReadFile(fullPath)
}

// WriteFile writes a file to the pod's emptyDir data volume.
func (h *HostFS) WriteFile(ctx context.Context, podUID, relativePath string, content []byte, mode os.FileMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	basePath := h.DataPath(podUID)
	if basePath == "" {
		return fmt.Errorf("invalid pod UID")
	}

	fullPath := filepath.Join(basePath, relativePath)

	// Security check: ensure path doesn't escape the data directory
	if !isSubpath(basePath, fullPath) {
		return fmt.Errorf("path %q escapes data directory", relativePath)
	}

	// Create parent directories if needed
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return fmt.Errorf("failed to create parent directories: %w", err)
	}

	if mode == 0 {
		mode = 0644
	}

	return os.WriteFile(fullPath, content, mode)
}

// ReadArchive reads files/directories from the data volume as a tar archive.
func (h *HostFS) ReadArchive(ctx context.Context, podUID, relativePath string) (io.ReadCloser, error) {
	basePath := h.DataPath(podUID)
	if basePath == "" {
		return nil, fmt.Errorf("invalid pod UID")
	}

	sourcePath := filepath.Join(basePath, relativePath)

	// Security check
	if !isSubpath(basePath, sourcePath) {
		return nil, fmt.Errorf("path %q escapes data directory", relativePath)
	}

	// Check if path exists
	info, err := os.Stat(sourcePath)
	if err != nil {
		return nil, err
	}

	// Create a pipe for streaming
	pr, pw := io.Pipe()

	go func() {
		tw := tar.NewWriter(pw)
		defer func() {
			tw.Close()
			pw.Close()
		}()

		if info.IsDir() {
			// Walk directory and add all files
			err := filepath.Walk(sourcePath, func(path string, fi os.FileInfo, err error) error {
				if err != nil {
					return err
				}

				// Check context
				if ctx.Err() != nil {
					return ctx.Err()
				}

				// Get relative path for tar header
				relPath, err := filepath.Rel(sourcePath, path)
				if err != nil {
					return err
				}
				if relPath == "." {
					relPath = filepath.Base(sourcePath)
				} else {
					relPath = filepath.Join(filepath.Base(sourcePath), relPath)
				}

				return addFileToTar(tw, path, relPath, fi)
			})
			if err != nil {
				pw.CloseWithError(err)
			}
		} else {
			// Single file
			if err := addFileToTar(tw, sourcePath, filepath.Base(sourcePath), info); err != nil {
				pw.CloseWithError(err)
			}
		}
	}()

	return pr, nil
}

// WriteArchive extracts a tar archive to the data volume.
func (h *HostFS) WriteArchive(ctx context.Context, podUID, destDir string, tarData io.Reader) error {
	basePath := h.DataPath(podUID)
	if basePath == "" {
		return fmt.Errorf("invalid pod UID")
	}

	destPath := filepath.Join(basePath, destDir)

	// Security check
	if !isSubpath(basePath, destPath) {
		return fmt.Errorf("path %q escapes data directory", destDir)
	}

	// Create destination directory
	if err := os.MkdirAll(destPath, 0755); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	tr := tar.NewReader(tarData)

	for {
		// Check context
		if err := ctx.Err(); err != nil {
			return err
		}

		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading tar: %w", err)
		}

		// Construct target path
		targetPath := filepath.Join(destPath, header.Name)

		// Security check for each file
		if !isSubpath(destPath, targetPath) {
			return fmt.Errorf("tar entry %q escapes destination directory", header.Name)
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(targetPath, os.FileMode(header.Mode)); err != nil {
				return fmt.Errorf("failed to create directory %s: %w", targetPath, err)
			}

		case tar.TypeReg:
			// Create parent directory
			if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
				return fmt.Errorf("failed to create parent directory: %w", err)
			}

			// Create file
			f, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(header.Mode))
			if err != nil {
				return fmt.Errorf("failed to create file %s: %w", targetPath, err)
			}

			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return fmt.Errorf("failed to write file %s: %w", targetPath, err)
			}
			f.Close()

		case tar.TypeSymlink:
			// Create symlink
			if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
				return fmt.Errorf("failed to create parent directory: %w", err)
			}
			if err := os.Symlink(header.Linkname, targetPath); err != nil {
				return fmt.Errorf("failed to create symlink %s: %w", targetPath, err)
			}

		case tar.TypeLink:
			// Create hard link
			linkTarget := filepath.Join(destPath, header.Linkname)
			if err := os.Link(linkTarget, targetPath); err != nil {
				return fmt.Errorf("failed to create hard link %s: %w", targetPath, err)
			}
		}
	}

	return nil
}

// DeleteFile deletes a file from the data volume.
func (h *HostFS) DeleteFile(ctx context.Context, podUID, relativePath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	basePath := h.DataPath(podUID)
	if basePath == "" {
		return fmt.Errorf("invalid pod UID")
	}

	fullPath := filepath.Join(basePath, relativePath)

	// Security check
	if !isSubpath(basePath, fullPath) {
		return fmt.Errorf("path %q escapes data directory", relativePath)
	}

	return os.RemoveAll(fullPath)
}

// ListFiles lists files in a directory within the data volume.
func (h *HostFS) ListFiles(ctx context.Context, podUID, relativePath string) ([]os.FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	basePath := h.DataPath(podUID)
	if basePath == "" {
		return nil, fmt.Errorf("invalid pod UID")
	}

	fullPath := filepath.Join(basePath, relativePath)

	// Security check
	if !isSubpath(basePath, fullPath) {
		return nil, fmt.Errorf("path %q escapes data directory", relativePath)
	}

	entries, err := os.ReadDir(fullPath)
	if err != nil {
		return nil, err
	}

	infos := make([]os.FileInfo, len(entries))
	for i, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		infos[i] = info
	}

	return infos, nil
}

// Stat returns file info for a path in the data volume.
func (h *HostFS) Stat(ctx context.Context, podUID, relativePath string) (os.FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	basePath := h.DataPath(podUID)
	if basePath == "" {
		return nil, fmt.Errorf("invalid pod UID")
	}

	fullPath := filepath.Join(basePath, relativePath)

	// Security check
	if !isSubpath(basePath, fullPath) {
		return nil, fmt.Errorf("path %q escapes data directory", relativePath)
	}

	return os.Stat(fullPath)
}

// addFileToTar adds a file to a tar archive.
func addFileToTar(tw *tar.Writer, sourcePath, tarPath string, fi os.FileInfo) error {
	header, err := tar.FileInfoHeader(fi, "")
	if err != nil {
		return err
	}

	header.Name = tarPath

	// Handle symlinks
	if fi.Mode()&os.ModeSymlink != 0 {
		link, err := os.Readlink(sourcePath)
		if err != nil {
			return err
		}
		header.Linkname = link
	}

	if err := tw.WriteHeader(header); err != nil {
		return err
	}

	// Write file content if it's a regular file
	if fi.Mode().IsRegular() {
		f, err := os.Open(sourcePath)
		if err != nil {
			return err
		}
		defer f.Close()

		if _, err := io.Copy(tw, f); err != nil {
			return err
		}
	}

	return nil
}

// isSubpath checks if child is a subpath of parent.
// This prevents path traversal attacks.
func isSubpath(parent, child string) bool {
	// Clean and make absolute
	parent = filepath.Clean(parent)
	child = filepath.Clean(child)

	// Ensure parent ends with separator for accurate prefix matching
	if !strings.HasSuffix(parent, string(filepath.Separator)) {
		parent += string(filepath.Separator)
	}

	// Child must start with parent path
	return strings.HasPrefix(child+string(filepath.Separator), parent) || child == filepath.Clean(parent[:len(parent)-1])
}
