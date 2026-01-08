// Copyright (c) 2024 Ant Group
//
// SPDX-License-Identifier: Apache-2.0
//

package client

import (
	"context"
	"io"

	"github.com/containerd/ttrpc"
	agentgrpc "github.com/kata-containers/kata-containers/src/runtime/virtcontainers/pkg/agent/protocols/grpc"
)

// StdioStream represents a streaming connection for stdout/stderr.
type StdioStream interface {
	// Recv receives the next chunk of data from the stream.
	// Returns io.EOF when the stream is closed.
	Recv() (*agentgrpc.StreamResponse, error)
	// Close closes the stream.
	Close() error
}

type stdioStream struct {
	cs     ttrpc.ClientStream
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *stdioStream) Recv() (*agentgrpc.StreamResponse, error) {
	resp := &agentgrpc.StreamResponse{}
	if err := s.cs.RecvMsg(resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *stdioStream) Close() error {
	s.cancel()
	return nil
}

// StreamStdout creates a streaming connection to read stdout from a process.
func (c *AgentClient) StreamStdout(ctx context.Context, req *agentgrpc.StreamRequest) (StdioStream, error) {
	return c.newStdioStream(ctx, "StreamStdout", req)
}

// StreamStderr creates a streaming connection to read stderr from a process.
func (c *AgentClient) StreamStderr(ctx context.Context, req *agentgrpc.StreamRequest) (StdioStream, error) {
	return c.newStdioStream(ctx, "StreamStderr", req)
}

func (c *AgentClient) newStdioStream(ctx context.Context, method string, req *agentgrpc.StreamRequest) (StdioStream, error) {
	ctx, cancel := context.WithCancel(ctx)

	// Server-streaming: client sends one request, server sends multiple responses
	desc := &ttrpc.StreamDesc{
		StreamingClient: false,
		StreamingServer: true,
	}

	cs, err := c.conn.NewStream(ctx, desc, "grpc.AgentService", method, req)
	if err != nil {
		cancel()
		return nil, err
	}

	return &stdioStream{
		cs:     cs,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

// StreamStdoutChannel is a convenience method that returns a channel of data chunks.
// The channel is closed when the stream ends or an error occurs.
func (c *AgentClient) StreamStdoutChannel(ctx context.Context, req *agentgrpc.StreamRequest) (<-chan []byte, <-chan error) {
	return c.streamToChannel(ctx, req, c.StreamStdout)
}

// StreamStderrChannel is a convenience method that returns a channel of data chunks.
// The channel is closed when the stream ends or an error occurs.
func (c *AgentClient) StreamStderrChannel(ctx context.Context, req *agentgrpc.StreamRequest) (<-chan []byte, <-chan error) {
	return c.streamToChannel(ctx, req, c.StreamStderr)
}

type streamFactory func(context.Context, *agentgrpc.StreamRequest) (StdioStream, error)

func (c *AgentClient) streamToChannel(ctx context.Context, req *agentgrpc.StreamRequest, factory streamFactory) (<-chan []byte, <-chan error) {
	dataCh := make(chan []byte, 16)
	errCh := make(chan error, 1)

	go func() {
		defer close(dataCh)
		defer close(errCh)

		stream, err := factory(ctx, req)
		if err != nil {
			errCh <- err
			return
		}
		defer stream.Close()

		for {
			resp, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					errCh <- err
				}
				return
			}
			if len(resp.Data) > 0 {
				select {
				case dataCh <- resp.Data:
				case <-ctx.Done():
					errCh <- ctx.Err()
					return
				}
			}
		}
	}()

	return dataCh, errCh
}
