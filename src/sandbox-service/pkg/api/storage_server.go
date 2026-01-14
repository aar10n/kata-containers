package api

import (
	"context"
	"log/slog"

	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/api/pb"
	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StorageServer implements the StorageService gRPC service.
type StorageServer struct {
	pb.UnimplementedStorageServiceServer
	s3Client *storage.S3Client
}

// NewStorageServer creates a new storage gRPC server.
func NewStorageServer(s3Client *storage.S3Client) *StorageServer {
	return &StorageServer{
		s3Client: s3Client,
	}
}

// GetUploadURL returns a presigned PUT URL for uploading a file.
func (s *StorageServer) GetUploadURL(ctx context.Context, req *pb.GetUploadURLRequest) (*pb.GetUploadURLResponse, error) {
	if req.SessionId == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}
	if req.FileName == "" {
		return nil, status.Error(codes.InvalidArgument, "file_name is required")
	}

	key := storage.FileKey(req.SessionId, req.FileName)
	url, err := s.s3Client.GenerateUploadURL(ctx, key)
	if err != nil {
		slog.Error("failed to generate upload URL", "session_id", req.SessionId, "file_name", req.FileName, "error", err)
		return nil, status.Errorf(codes.Internal, "failed to generate upload URL: %v", err)
	}

	slog.Info("generated upload URL", "session_id", req.SessionId, "file_name", req.FileName, "key", key)
	return &pb.GetUploadURLResponse{
		Url:              url,
		Key:              key,
		ExpiresInSeconds: s.s3Client.PresignExpiry(),
	}, nil
}

// GetDownloadURL returns a presigned GET URL for downloading a file.
func (s *StorageServer) GetDownloadURL(ctx context.Context, req *pb.GetDownloadURLRequest) (*pb.GetDownloadURLResponse, error) {
	if req.SessionId == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}
	if req.FileName == "" {
		return nil, status.Error(codes.InvalidArgument, "file_name is required")
	}

	// Check if file exists first
	info, err := s.s3Client.HeadFile(ctx, req.SessionId, req.FileName)
	if err != nil {
		slog.Error("failed to check file", "session_id", req.SessionId, "file_name", req.FileName, "error", err)
		return nil, status.Errorf(codes.Internal, "failed to check file: %v", err)
	}
	if !info.Exists {
		return nil, status.Error(codes.NotFound, "file not found")
	}

	key := storage.FileKey(req.SessionId, req.FileName)
	url, err := s.s3Client.GenerateDownloadURL(ctx, key)
	if err != nil {
		slog.Error("failed to generate download URL", "session_id", req.SessionId, "file_name", req.FileName, "error", err)
		return nil, status.Errorf(codes.Internal, "failed to generate download URL: %v", err)
	}

	slog.Info("generated download URL", "session_id", req.SessionId, "file_name", req.FileName, "key", key)
	return &pb.GetDownloadURLResponse{
		Url:              url,
		Key:              key,
		ExpiresInSeconds: s.s3Client.PresignExpiry(),
	}, nil
}

// CheckFileExists checks if a file exists for the given session.
func (s *StorageServer) CheckFileExists(ctx context.Context, req *pb.CheckFileExistsRequest) (*pb.CheckFileExistsResponse, error) {
	if req.SessionId == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}
	if req.FileName == "" {
		return nil, status.Error(codes.InvalidArgument, "file_name is required")
	}

	info, err := s.s3Client.HeadFile(ctx, req.SessionId, req.FileName)
	if err != nil {
		slog.Error("failed to check file", "session_id", req.SessionId, "file_name", req.FileName, "error", err)
		return nil, status.Errorf(codes.Internal, "failed to check file: %v", err)
	}

	return &pb.CheckFileExistsResponse{
		Exists:           info.Exists,
		SizeBytes:        info.SizeBytes,
		LastModifiedUnix: info.LastModified,
	}, nil
}

// DeleteFile deletes a file for a session.
func (s *StorageServer) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	if req.SessionId == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}
	if req.FileName == "" {
		return nil, status.Error(codes.InvalidArgument, "file_name is required")
	}

	deleted, err := s.s3Client.DeleteFile(ctx, req.SessionId, req.FileName)
	if err != nil {
		slog.Error("failed to delete file", "session_id", req.SessionId, "file_name", req.FileName, "error", err)
		return nil, status.Errorf(codes.Internal, "failed to delete file: %v", err)
	}

	slog.Info("deleted file", "session_id", req.SessionId, "file_name", req.FileName, "deleted", deleted)
	return &pb.DeleteFileResponse{
		Deleted: deleted,
	}, nil
}
