package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/storage/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Client is a gRPC client for the storage service.
type Client struct {
	conn   *grpc.ClientConn
	client pb.StorageServiceClient
}

// ClientConfig holds configuration for the storage client.
type ClientConfig struct {
	// Addr is the address of the storage gRPC server
	Addr string
	// Timeout is the default timeout for RPC calls
	Timeout time.Duration
}

// NewClient creates a new storage client.
func NewClient(cfg ClientConfig) (*Client, error) {
	if cfg.Addr == "" {
		return nil, fmt.Errorf("storage server address is required")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}

	conn, err := grpc.NewClient(cfg.Addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to storage server: %w", err)
	}

	return &Client{
		conn:   conn,
		client: pb.NewStorageServiceClient(conn),
	}, nil
}

// Close closes the client connection.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// UploadURLResult holds the result of getting an upload URL.
type UploadURLResult struct {
	URL              string
	Key              string
	ExpiresInSeconds int64
}

// GetUploadURL gets a presigned URL for uploading a file.
func (c *Client) GetUploadURL(ctx context.Context, sessionID, fileName string) (*UploadURLResult, error) {
	resp, err := c.client.GetUploadURL(ctx, &pb.GetUploadURLRequest{
		SessionId: sessionID,
		FileName:  fileName,
	})
	if err != nil {
		return nil, fmt.Errorf("get upload URL: %w", err)
	}
	return &UploadURLResult{
		URL:              resp.Url,
		Key:              resp.Key,
		ExpiresInSeconds: resp.ExpiresInSeconds,
	}, nil
}

// DownloadURLResult holds the result of getting a download URL.
type DownloadURLResult struct {
	URL              string
	Key              string
	ExpiresInSeconds int64
}

// ErrFileNotFound is returned when a file doesn't exist.
var ErrFileNotFound = fmt.Errorf("file not found")

// GetDownloadURL gets a presigned URL for downloading a file.
// Returns ErrFileNotFound if no file exists for the session.
func (c *Client) GetDownloadURL(ctx context.Context, sessionID, fileName string) (*DownloadURLResult, error) {
	resp, err := c.client.GetDownloadURL(ctx, &pb.GetDownloadURLRequest{
		SessionId: sessionID,
		FileName:  fileName,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, ErrFileNotFound
		}
		return nil, fmt.Errorf("get download URL: %w", err)
	}
	return &DownloadURLResult{
		URL:              resp.Url,
		Key:              resp.Key,
		ExpiresInSeconds: resp.ExpiresInSeconds,
	}, nil
}

// FileInfo holds metadata about a file.
type FileInfo struct {
	Exists           bool
	SizeBytes        int64
	LastModifiedUnix int64
}

// CheckFileExists checks if a file exists for the given session.
func (c *Client) CheckFileExists(ctx context.Context, sessionID, fileName string) (*FileInfo, error) {
	resp, err := c.client.CheckFileExists(ctx, &pb.CheckFileExistsRequest{
		SessionId: sessionID,
		FileName:  fileName,
	})
	if err != nil {
		return nil, fmt.Errorf("check file exists: %w", err)
	}
	return &FileInfo{
		Exists:           resp.Exists,
		SizeBytes:        resp.SizeBytes,
		LastModifiedUnix: resp.LastModifiedUnix,
	}, nil
}

// DeleteFile deletes a file for a session.
func (c *Client) DeleteFile(ctx context.Context, sessionID, fileName string) (bool, error) {
	resp, err := c.client.DeleteFile(ctx, &pb.DeleteFileRequest{
		SessionId: sessionID,
		FileName:  fileName,
	})
	if err != nil {
		return false, fmt.Errorf("delete file: %w", err)
	}
	return resp.Deleted, nil
}
