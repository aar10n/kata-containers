package shim_mgmt

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	cdshim "github.com/containerd/containerd/runtime/v2/shim"
)

const (
	legacyShimStoragePath = "/run/vc/sbs"
	rustShimStoragePath   = "/run/kata"
	shimSocketName        = "shim-monitor.sock"
	vmStatePathKey        = "path"
	saveVMStatePath       = "/vm-state/save"
	restoreVMStatePath    = "/vm-state/restore"
	defaultDialTimeout    = 5 * time.Second
)

type Config struct {
	DialTimeout time.Duration
}

type Client struct {
	dialTimeout time.Duration
}

func New(cfg Config) *Client {
	timeout := cfg.DialTimeout
	if timeout <= 0 {
		timeout = defaultDialTimeout
	}
	return &Client{dialTimeout: timeout}
}

func (c *Client) SaveVMState(ctx context.Context, sandboxID, statePath string) error {
	return c.postVMState(ctx, sandboxID, saveVMStatePath, statePath)
}

func (c *Client) RestoreVMState(ctx context.Context, sandboxID, statePath string) error {
	return c.postVMState(ctx, sandboxID, restoreVMStatePath, statePath)
}

func (c *Client) postVMState(ctx context.Context, sandboxID, endpoint, statePath string) error {
	if strings.TrimSpace(sandboxID) == "" {
		return errors.New("sandbox id is required")
	}
	if strings.TrimSpace(statePath) == "" {
		return errors.New("state path is required")
	}

	client, err := c.buildShimClient(ctx, sandboxID)
	if err != nil {
		return err
	}

	reqURL := url.URL{
		Scheme: "http",
		Host:   "shim",
		Path:   endpoint,
	}
	query := reqURL.Query()
	query.Set(vmStatePathKey, statePath)
	reqURL.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL.String(), nil)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("shim mgmt %s failed: status %s: %s", endpoint, resp.Status, strings.TrimSpace(string(body)))
	}

	return nil
}

func (c *Client) buildShimClient(ctx context.Context, sandboxID string) (*http.Client, error) {
	socketAddress, err := shimClientSocketAddress(sandboxID)
	if err != nil {
		return nil, err
	}

	dialTimeout := c.dialTimeout
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining > 0 && remaining < dialTimeout {
			dialTimeout = remaining
		}
	}

	transport := &http.Transport{
		DisableKeepAlives: true,
		Dial: func(proto, addr string) (net.Conn, error) {
			return cdshim.AnonDialer(socketAddress, dialTimeout)
		},
	}

	return &http.Client{Transport: transport}, nil
}

func shimClientSocketAddress(sandboxID string) (string, error) {
	socketPath := shimSocketPath(legacyShimStoragePath, sandboxID)
	if _, err := os.Stat(socketPath); err != nil {
		fallbackPath, fallbackErr := shimSocketPathWithPrefix(legacyShimStoragePath, sandboxID)
		if fallbackErr == nil {
			socketPath = fallbackPath
		} else {
			altPath := shimSocketPath(rustShimStoragePath, sandboxID)
			if _, err := os.Stat(altPath); err != nil {
				altFallback, altFallbackErr := shimSocketPathWithPrefix(rustShimStoragePath, sandboxID)
				if altFallbackErr != nil {
					return "", fmt.Errorf("missing shim socket at %s and %s: %w", socketPath, altPath, err)
				}
				socketPath = altFallback
			} else {
				socketPath = altPath
			}
		}
	}

	return fmt.Sprintf("unix://%s", socketPath), nil
}

func shimSocketPath(storagePath, sandboxID string) string {
	return filepath.Join(string(filepath.Separator), storagePath, sandboxID, shimSocketName)
}

func shimSocketPathWithPrefix(storagePath, sandboxID string) (string, error) {
	entries, err := os.ReadDir(storagePath)
	if err != nil {
		return "", err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(sandboxID, name) {
			socketPath := filepath.Join(string(filepath.Separator), storagePath, name, shimSocketName)
			if _, err := os.Stat(socketPath); err == nil {
				return socketPath, nil
			}
		}
	}

	return "", fmt.Errorf("no shim socket found for id prefix %s", sandboxID)
}
