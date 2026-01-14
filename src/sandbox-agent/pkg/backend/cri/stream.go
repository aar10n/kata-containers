package cri

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"

	"github.com/gorilla/websocket"
)

// terminalSize is the JSON structure for resize messages.
type terminalSize struct {
	Width  uint16 `json:"Width"`
	Height uint16 `json:"Height"`
}

const (
	streamTypeStdin  byte = 0
	streamTypeStdout byte = 1
	streamTypeStderr byte = 2
	streamTypeError  byte = 3
	streamTypeResize byte = 4
)

// streamConn manages a WebSocket connection to a CRI streaming endpoint.
type streamConn struct {
	ws     *websocket.Conn
	mu     sync.Mutex
	closed bool

	// Buffered output
	stdoutBuf []byte
	stderrBuf []byte
	bufMu     sync.Mutex

	// Signals
	done      chan struct{}
	dataReady chan struct{} // signaled when new data arrives
}

// newStreamConn connects to a CRI streaming URL and returns a managed connection.
func newStreamConn(ctx context.Context, streamURL string) (*streamConn, error) {
	// Parse and upgrade URL scheme for WebSocket
	u, err := url.Parse(streamURL)
	if err != nil {
		return nil, fmt.Errorf("parse stream URL: %w", err)
	}

	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	}

	// Connect via WebSocket
	dialer := websocket.Dialer{
		Subprotocols: []string{"v4.channel.k8s.io"},
	}

	headers := http.Header{}
	ws, resp, err := dialer.DialContext(ctx, u.String(), headers)
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("websocket dial failed with status %d: %w", resp.StatusCode, err)
		}
		return nil, fmt.Errorf("websocket dial: %w", err)
	}

	conn := &streamConn{
		ws:        ws,
		done:      make(chan struct{}),
		dataReady: make(chan struct{}, 1), // buffered to avoid blocking
	}

	// Start reading in background
	go conn.readLoop()

	return conn, nil
}

// readLoop continuously reads from the WebSocket and buffers output.
func (c *streamConn) readLoop() {
	defer close(c.done)

	for {
		_, data, err := c.ws.ReadMessage()
		if err != nil {
			// Connection closed or error
			return
		}

		if len(data) < 1 {
			continue
		}

		streamType := data[0]
		payload := data[1:]

		c.bufMu.Lock()
		switch streamType {
		case streamTypeStdout:
			c.stdoutBuf = append(c.stdoutBuf, payload...)
		case streamTypeStderr:
			c.stderrBuf = append(c.stderrBuf, payload...)
		}
		c.bufMu.Unlock()

		// Signal that new data is available (non-blocking)
		select {
		case c.dataReady <- struct{}{}:
		default:
		}
	}
}

// Write writes data to stdin.
func (c *streamConn) Write(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return io.ErrClosedPipe
	}

	// Prepend stdin stream type
	msg := make([]byte, len(data)+1)
	msg[0] = streamTypeStdin
	copy(msg[1:], data)

	return c.ws.WriteMessage(websocket.BinaryMessage, msg)
}

// ReadStdout reads and clears buffered stdout.
func (c *streamConn) ReadStdout(maxBytes int) []byte {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

	if len(c.stdoutBuf) == 0 {
		return nil
	}

	n := len(c.stdoutBuf)
	if maxBytes > 0 && n > maxBytes {
		n = maxBytes
	}

	data := make([]byte, n)
	copy(data, c.stdoutBuf[:n])
	c.stdoutBuf = c.stdoutBuf[n:]

	return data
}

// ReadStderr reads and clears buffered stderr.
func (c *streamConn) ReadStderr(maxBytes int) []byte {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

	if len(c.stderrBuf) == 0 {
		return nil
	}

	n := len(c.stderrBuf)
	if maxBytes > 0 && n > maxBytes {
		n = maxBytes
	}

	data := make([]byte, n)
	copy(data, c.stderrBuf[:n])
	c.stderrBuf = c.stderrBuf[n:]

	return data
}

// CloseStdin sends an EOF on stdin by closing write.
func (c *streamConn) CloseStdin() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Send close message for stdin channel
	return c.ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
}

// Resize sends a terminal resize message.
func (c *streamConn) Resize(cols, rows uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return io.ErrClosedPipe
	}

	// Encode terminal size as JSON
	size := terminalSize{
		Width:  uint16(cols),
		Height: uint16(rows),
	}
	sizeJSON, err := json.Marshal(size)
	if err != nil {
		return fmt.Errorf("marshal terminal size: %w", err)
	}

	// Prepend resize stream type
	msg := make([]byte, len(sizeJSON)+1)
	msg[0] = streamTypeResize
	copy(msg[1:], sizeJSON)

	return c.ws.WriteMessage(websocket.BinaryMessage, msg)
}

// Close closes the WebSocket connection.
func (c *streamConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	return c.ws.Close()
}

// Done returns a channel that's closed when the connection ends.
func (c *streamConn) Done() <-chan struct{} {
	return c.done
}

// DataReady returns a channel that receives when new data is available.
func (c *streamConn) DataReady() <-chan struct{} {
	return c.dataReady
}
