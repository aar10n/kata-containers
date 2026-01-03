FROM golang:1.25.1 AS builder

WORKDIR /workspace

COPY src/sandbox-service/go.mod ./sandbox-service/
COPY src/sandbox-service/go.sum ./sandbox-service/

WORKDIR /workspace/sandbox-service
RUN go mod download

WORKDIR /workspace
COPY src/sandbox-service ./sandbox-service

WORKDIR /workspace/sandbox-service
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath -ldflags="-s -w" -o /out/sandbox-service ./main.go

FROM gcr.io/distroless/static:nonroot

COPY --from=builder /out/sandbox-service /usr/local/bin/sandbox-service

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/sandbox-service"]
