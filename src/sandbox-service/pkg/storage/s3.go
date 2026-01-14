package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Config holds configuration for S3-compatible storage.
type S3Config struct {
	// Endpoint is the S3-compatible endpoint URL (e.g., "http://s3proxy.kata-system:80")
	Endpoint string
	// Region is the AWS region (required but may be ignored by s3proxy)
	Region string
	// Bucket is the bucket name for file storage
	Bucket string
	// AccessKeyID for authentication
	AccessKeyID string
	// SecretAccessKey for authentication
	SecretAccessKey string
	// PresignExpiry is the duration for presigned URL validity
	PresignExpiry time.Duration
	// ForcePathStyle forces path-style URLs instead of virtual-hosted (required for s3proxy)
	ForcePathStyle bool
}

// S3Client provides presigned URL generation for file storage.
type S3Client struct {
	client        *s3.Client
	presigner     *s3.PresignClient
	bucket        string
	presignExpiry time.Duration
}

// NewS3Client creates a new S3 client for file storage.
func NewS3Client(cfg S3Config) (*S3Client, error) {
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("s3 endpoint is required")
	}
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3 bucket is required")
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if cfg.PresignExpiry == 0 {
		cfg.PresignExpiry = 15 * time.Minute
	}

	// Create custom endpoint resolver
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL:               cfg.Endpoint,
			HostnameImmutable: true,
		}, nil
	})

	// Load AWS config with custom settings
	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(cfg.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID,
			cfg.SecretAccessKey,
			"",
		)),
		config.WithEndpointResolverWithOptions(customResolver),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = cfg.ForcePathStyle
	})

	return &S3Client{
		client:        client,
		presigner:     s3.NewPresignClient(client),
		bucket:        cfg.Bucket,
		presignExpiry: cfg.PresignExpiry,
	}, nil
}

// FileKey returns the S3 key for a session's file.
func FileKey(sessionID, fileName string) string {
	return fmt.Sprintf("sessions/%s/%s", sessionID, fileName)
}

// GenerateUploadURL generates a presigned PUT URL for uploading a file.
func (c *S3Client) GenerateUploadURL(ctx context.Context, key string) (string, error) {
	req, err := c.presigner.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket: &c.bucket,
		Key:    &key,
	}, s3.WithPresignExpires(c.presignExpiry))
	if err != nil {
		return "", fmt.Errorf("presign put: %w", err)
	}
	return req.URL, nil
}

// GenerateDownloadURL generates a presigned GET URL for downloading a file.
func (c *S3Client) GenerateDownloadURL(ctx context.Context, key string) (string, error) {
	req, err := c.presigner.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: &c.bucket,
		Key:    &key,
	}, s3.WithPresignExpires(c.presignExpiry))
	if err != nil {
		return "", fmt.Errorf("presign get: %w", err)
	}
	return req.URL, nil
}

// FileInfo contains metadata about a stored file.
type FileInfo struct {
	Exists       bool
	SizeBytes    int64
	LastModified int64 // Unix timestamp
}

// HeadFile returns metadata about a file, or Exists=false if not found.
func (c *S3Client) HeadFile(ctx context.Context, sessionID, fileName string) (*FileInfo, error) {
	key := FileKey(sessionID, fileName)
	resp, err := c.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &c.bucket,
		Key:    &key,
	})
	if err != nil {
		// Check if it's a "not found" error - return empty info
		return &FileInfo{Exists: false}, nil
	}
	info := &FileInfo{
		Exists: true,
	}
	if resp.ContentLength != nil {
		info.SizeBytes = *resp.ContentLength
	}
	if resp.LastModified != nil {
		info.LastModified = resp.LastModified.Unix()
	}
	return info, nil
}

// DeleteFile deletes a file for a session.
func (c *S3Client) DeleteFile(ctx context.Context, sessionID, fileName string) (bool, error) {
	key := FileKey(sessionID, fileName)
	_, err := c.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &c.bucket,
		Key:    &key,
	})
	if err != nil {
		return false, fmt.Errorf("delete object: %w", err)
	}
	return true, nil
}

// PresignExpiry returns the presign expiry duration in seconds.
func (c *S3Client) PresignExpiry() int64 {
	return int64(c.presignExpiry.Seconds())
}
