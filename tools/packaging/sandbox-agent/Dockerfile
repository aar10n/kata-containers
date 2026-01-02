FROM golang:1.25.1 AS builder

WORKDIR /workspace

COPY src/runtime ./runtime
COPY src/sandbox-agent ./sandbox-agent

WORKDIR /workspace/sandbox-agent

RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath -ldflags="-s -w" -o /out/sandbox-agent ./main.go

FROM gcr.io/distroless/static:nonroot

COPY --from=builder /out/sandbox-agent /usr/local/bin/sandbox-agent

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/sandbox-agent"]
