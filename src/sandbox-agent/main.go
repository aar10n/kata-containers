package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/agent"
	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/api"
	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/api/pb"
	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/k8s"
	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/service"
	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/shim_mgmt"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	var httpAddr string
	var grpcAddr string
	var kubeconfig string
	var resyncInterval time.Duration
	var execTimeout time.Duration
	var agentTimeout time.Duration
	var grpcDialTimeout time.Duration
	var maxOutputBytes int
	var readChunkSize int

	flag.StringVar(&httpAddr, "http-addr", ":8080", "HTTP listen address")
	flag.StringVar(&grpcAddr, "grpc-addr", ":9090", "gRPC listen address")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig (optional, defaults to in-cluster config)")
	flag.DurationVar(&resyncInterval, "resync-interval", 5*time.Minute, "Kubernetes informer resync interval")
	flag.DurationVar(&execTimeout, "exec-timeout", 30*time.Second, "Command execution timeout")
	flag.DurationVar(&agentTimeout, "agent-timeout", 10*time.Second, "Agent connection timeout")
	flag.DurationVar(&grpcDialTimeout, "grpc-dial-timeout", 5*time.Second, "Remote sandbox-agent dial timeout")
	flag.IntVar(&maxOutputBytes, "max-output-bytes", 1024*1024, "Maximum stdout/stderr bytes to return")
	flag.IntVar(&readChunkSize, "read-chunk-size", 4096, "Per-read byte size from agent streams")
	flag.Parse()

	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		log.Fatal("NODE_NAME env var is required")
	}

	clientset, err := k8s.NewClient(kubeconfig)
	if err != nil {
		log.Fatalf("failed to initialize k8s client: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watcher := k8s.NewWatcher(clientset, resyncInterval)
	go func() {
		if err := watcher.Start(ctx); err != nil {
			log.Fatalf("failed to start k8s watchers: %v", err)
		}
	}()

	if err := watcher.WaitForSync(ctx); err != nil {
		log.Fatalf("failed to sync k8s watchers: %v", err)
	}

	agentClient := agent.New(agent.Config{
		Timeout:       agentTimeout,
		ReadChunkSize: readChunkSize,
		MaxOutputSize: maxOutputBytes,
	})
	shimClient := shim_mgmt.New(shim_mgmt.Config{DialTimeout: agentTimeout})

	svc := service.New(service.Config{
		NodeName:    nodeName,
		ExecTimeout: execTimeout,
	}, watcher.Store(), agentClient, shimClient)

	grpcServer := grpc.NewServer()
	apiServer := api.NewServer(api.Config{
		HTTPAddr:    httpAddr,
		GRPCAddr:    grpcAddr,
		NodeName:    nodeName,
		DialTimeout: grpcDialTimeout,
	}, svc)
	apiServer.Register(grpcServer)

	grpcListener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("failed to listen on gRPC addr %s: %v", grpcAddr, err)
	}

	go func() {
		if err := grpcServer.Serve(grpcListener); err != nil {
			log.Fatalf("gRPC server error: %v", err)
		}
	}()

	gatewayMux := runtime.NewServeMux(
		runtime.WithIncomingHeaderMatcher(api.IncomingHeaderMatcher),
		runtime.WithErrorHandler(api.GatewayErrorHandler),
	)
	grpcEndpoint := grpcEndpoint(grpcAddr)
	if err := pb.RegisterSandboxAgentHandlerFromEndpoint(ctx, gatewayMux, grpcEndpoint, []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}); err != nil {
		log.Fatalf("failed to register grpc-gateway: %v", err)
	}

	httpServer := &http.Server{
		Addr:              httpAddr,
		Handler:           gatewayMux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	shutdownCh := make(chan os.Signal, 1)
	signal.Notify(shutdownCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-shutdownCh
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		grpcServer.GracefulStop()
		if err := httpServer.Shutdown(ctx); err != nil {
			log.Printf("http shutdown error: %v", err)
		}
	}()

	log.Printf("sandbox-agent listening on %s (http) and %s (grpc)", httpAddr, grpcAddr)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("http server error: %v", err)
	}
}

func grpcEndpoint(grpcAddr string) string {
	host, port, err := net.SplitHostPort(grpcAddr)
	if err != nil {
		return grpcAddr
	}
	if host == "" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}
