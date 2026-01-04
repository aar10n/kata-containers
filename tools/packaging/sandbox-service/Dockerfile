# syntax=docker/dockerfile:1.7
FROM golang:1.25.1 AS builder

WORKDIR /workspace

# Copy go.mod/go.sum for all modules (sandbox-service depends on sandbox-agent and runtime via replace)
COPY src/sandbox-service/go.mod src/sandbox-service/go.sum ./sandbox-service/
COPY src/sandbox-agent/go.mod src/sandbox-agent/go.sum ./sandbox-agent/
COPY src/runtime/go.mod src/runtime/go.sum ./runtime/

WORKDIR /workspace/sandbox-service
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

WORKDIR /workspace
COPY src/sandbox-service ./sandbox-service
COPY src/sandbox-agent ./sandbox-agent
COPY src/runtime ./runtime

WORKDIR /workspace/sandbox-service
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath -ldflags="-s -w" -o /out/sandbox-service ./main.go

FROM alpine:3.20

COPY --from=builder /out/sandbox-service /usr/local/bin/sandbox-service

RUN adduser -D -H -s /bin/sh appuser
USER appuser

ENTRYPOINT ["/usr/local/bin/sandbox-service"]
