package api

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const preferRedirectHeader = "prefer-redirect"

func IncomingHeaderMatcher(key string) (string, bool) {
	switch strings.ToLower(key) {
	case "prefer-redirect", "x-prefer-redirect":
		return preferRedirectHeader, true
	default:
		return runtime.DefaultHeaderMatcher(key)
	}
}

func shouldRedirect(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	values := md.Get(preferRedirectHeader)
	for _, value := range values {
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "1", "true", "yes", "redirect":
			return true
		}
	}
	return false
}

func redirectError(host string, port int) error {
	if strings.TrimSpace(host) == "" {
		return status.Error(codes.Unavailable, "target host unavailable")
	}

	st := status.New(codes.FailedPrecondition, "redirect")
	info := &errdetails.ErrorInfo{
		Reason: "REDIRECT",
		Metadata: map[string]string{
			"host": host,
			"port": strconv.Itoa(port),
		},
	}
	withDetails, err := st.WithDetails(info)
	if err != nil {
		return st.Err()
	}
	return withDetails.Err()
}

func redirectTarget(err error) (string, int, bool) {
	st, ok := status.FromError(err)
	if !ok {
		return "", 0, false
	}
	for _, detail := range st.Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if !ok || info.Reason != "REDIRECT" {
			continue
		}
		host := strings.TrimSpace(info.Metadata["host"])
		port, _ := strconv.Atoi(info.Metadata["port"])
		if host == "" {
			return "", 0, false
		}
		return host, port, true
	}
	return "", 0, false
}

func GatewayErrorHandler(ctx context.Context, mux *runtime.ServeMux, marshaler runtime.Marshaler, w http.ResponseWriter, r *http.Request, err error) {
	host, port, ok := redirectTarget(err)
	if ok {
		redirectURL := buildRedirectURL(r, host, port)
		http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
		return
	}
	runtime.DefaultHTTPErrorHandler(ctx, mux, marshaler, w, r, err)
}

func buildRedirectURL(r *http.Request, host string, port int) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if forwarded := r.Header.Get("X-Forwarded-Proto"); forwarded != "" {
		scheme = forwarded
	}

	targetHost := host
	if port > 0 {
		targetHost = host + ":" + strconv.Itoa(port)
	}

	u := &url.URL{
		Scheme:   scheme,
		Host:     targetHost,
		Path:     r.URL.Path,
		RawQuery: r.URL.RawQuery,
	}
	return u.String()
}
