package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/service"
)

type ResolveRequest struct {
	ContainerID string `json:"container_id"`
	Node        string `json:"node,omitempty"`
}

type ResolveResponse struct {
	SandboxID string `json:"sandbox_id"`
}

type resolveError struct {
	Error string `json:"error"`
}

func ResolveHandler(cfg Config, svc service.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeResolveError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeResolveError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		var req ResolveRequest
		if err := json.Unmarshal(body, &req); err != nil {
			writeResolveError(w, http.StatusBadRequest, "invalid json")
			return
		}

		if node := strings.TrimSpace(req.Node); node != "" && node != cfg.NodeName {
			forwardResolve(w, r.Context(), cfg, svc, node, body)
			return
		}

		sandboxID, err := svc.ResolveSandboxID(r.Context(), req.ContainerID)
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, context.DeadlineExceeded) {
				status = http.StatusGatewayTimeout
			} else if strings.Contains(err.Error(), "not found") {
				status = http.StatusNotFound
			} else if strings.Contains(err.Error(), "required") {
				status = http.StatusBadRequest
			}
			writeResolveError(w, status, err.Error())
			return
		}

		writeResolveJSON(w, http.StatusOK, ResolveResponse{SandboxID: sandboxID})
	}
}

func forwardResolve(w http.ResponseWriter, ctx context.Context, cfg Config, svc service.Service, node string, body []byte) {
	addr, ok := svc.SandboxAgentAddressForNode(node)
	if !ok {
		writeResolveError(w, http.StatusServiceUnavailable, "target node address unavailable")
		return
	}

	url := "http://" + addr + ":" + strconv.Itoa(parseListenPort(cfg.HTTPAddr)) + "/v1/resolve"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		writeResolveError(w, http.StatusInternalServerError, "failed to build forward request")
		return
	}
	request.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(request)
	if err != nil {
		writeResolveError(w, http.StatusBadGateway, "forward request failed")
		return
	}
	defer resp.Body.Close()

	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		writeResolveError(w, http.StatusBadGateway, "failed to read forward response")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(payload)
}

func writeResolveError(w http.ResponseWriter, status int, message string) {
	writeResolveJSON(w, status, resolveError{Error: message})
}

func writeResolveJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
