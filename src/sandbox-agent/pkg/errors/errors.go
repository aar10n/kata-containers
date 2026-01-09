package errors

import "errors"

var (
	ErrNotFound         = errors.New("not found")
	ErrSandboxNotFound  = errors.New("sandbox not found")
	ErrAlreadyExists    = errors.New("already exists")
	ErrNotReady         = errors.New("sandbox not ready")
	ErrInvalidArgument  = errors.New("invalid argument")
	ErrConnectionClosed = errors.New("connection closed")
	ErrEOF              = errors.New("end of stream")
	ErrNotSupported     = errors.New("operation not supported")
)
