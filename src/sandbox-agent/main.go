package main

import (
	"context"
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
	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/backend"
	cribackend "github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/backend/cri"
	katabackend "github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/backend/kata"
	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/config"
	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/cri"
	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/hostfs"
	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/k8s"
	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/service"
	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/shim_mgmt"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Register and parse flags
	flags := config.RegisterFlags()
	pflag.Parse()

	// Load configuration
	cfg, err := config.Load(flags)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		log.Fatal("NODE_NAME env var is required")
	}

	log.Printf("starting sandbox-agent in %s mode", cfg.Mode)

	clientset, err := k8s.NewClient(cfg.Kubernetes.Kubeconfig)
	if err != nil {
		log.Fatalf("failed to initialize k8s client: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watcher := k8s.NewWatcher(clientset, cfg.Kubernetes.ResyncInterval)
	go func() {
		if err := watcher.Start(ctx); err != nil {
			log.Fatalf("failed to start k8s watchers: %v", err)
		}
	}()

	if err := watcher.WaitForSync(ctx); err != nil {
		log.Fatalf("failed to sync k8s watchers: %v", err)
	}

	// Create backend and hostfs based on mode
	var be backend.ExecutionBackend
	var hfs *hostfs.HostFS

	switch cfg.Mode {
	case config.ModeKata:
		agentClient := agent.New(agent.Config{
			Timeout:       cfg.Agent.Timeout,
			ReadChunkSize: cfg.Agent.ReadChunkSize,
			MaxOutputSize: cfg.Agent.MaxOutputSize,
		})
		shimClient := shim_mgmt.New(shim_mgmt.Config{DialTimeout: cfg.Agent.Timeout})
		kataBackend := katabackend.NewFromClients(agentClient, shimClient)
		// Set the resolver so the backend can look up sandbox ID from container ID
		kataBackend.SetSandboxIDResolver(watcher.Store().SandboxIDForContainerID)
		be = kataBackend
		log.Printf("initialized kata backend with sandbox ID resolver")

	case config.ModePod:
		criClient, err := cri.New(cri.Config{
			Socket:  cfg.CRI.Socket,
			Timeout: cfg.CRI.Timeout,
		})
		if err != nil {
			log.Fatalf("failed to create CRI client: %v", err)
		}
		be = cribackend.NewFromClient(criClient)

		// Create hostfs for direct file access
		hfs = hostfs.New(hostfs.Config{
			KubeletRoot: cfg.HostFS.KubeletRoot,
		})
		log.Printf("initialized pod backend with CRI client and hostfs")

	default:
		log.Fatalf("unknown mode: %s", cfg.Mode)
	}

	// Create manager with mode-appropriate config
	manager := k8s.NewManager(clientset, k8s.ManagerConfig{
		Namespace:        cfg.Sandbox.Namespace,
		RuntimeClassName: cfg.Sandbox.RuntimeClassName,
		NodeSelector:     cfg.Sandbox.NodeSelector,
		PodMode:          cfg.IsPodMode(),
	}, watcher.Store())

	// Set up sandbox ID callback for kata mode
	if cfg.IsKataMode() {
		watcher.Store().SetSandboxIDCallback(func(sessionID, sandboxID string) {
			// Immediately update the store so API calls don't fail while waiting for K8s propagation
			watcher.Store().UpdateSandboxID(sessionID, sandboxID)

			const maxRetries = 3
			var err error
			for i := 0; i < maxRetries; i++ {
				if err = manager.UpdateSandboxIDAnnotation(context.Background(), sessionID, sandboxID); err == nil {
					log.Printf("persisted sandbox-id annotation for session %s: %s", sessionID, sandboxID)
					return
				}
				if i < maxRetries-1 {
					time.Sleep(time.Duration(100*(i+1)) * time.Millisecond)
				}
			}
			log.Printf("failed to update sandbox-id annotation for session %s after %d attempts: %v", sessionID, maxRetries, err)
		})
	}

	svc, err := service.New(service.Config{
		NodeName:         nodeName,
		ExecTimeout:      cfg.Exec.Timeout,
		Mode:             string(cfg.Mode),
		StorageEnabled:   cfg.Storage.Enabled,
		StorageAddr:      cfg.Storage.Addr,
		StorageTimeout:   cfg.Storage.Timeout,
		StorageInitImage: cfg.Storage.InitImage,
	}, watcher.Store(), manager, be, hfs)
	if err != nil {
		log.Fatalf("create service: %v", err)
	}

	grpcServer := grpc.NewServer()
	apiCfg := api.Config{
		HTTPAddr:    cfg.HTTP.Addr,
		GRPCAddr:    cfg.GRPC.Addr,
		NodeName:    nodeName,
		DialTimeout: cfg.GRPC.DialTimeout,
	}
	apiServer := api.NewServer(apiCfg, svc)
	apiServer.Register(grpcServer)

	grpcListener, err := net.Listen("tcp", cfg.GRPC.Addr)
	if err != nil {
		log.Fatalf("failed to listen on gRPC addr %s: %v", cfg.GRPC.Addr, err)
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
	grpcEndpoint := grpcEndpoint(cfg.GRPC.Addr)
	if err := pb.RegisterSandboxAgentHandlerFromEndpoint(ctx, gatewayMux, grpcEndpoint, []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}); err != nil {
		log.Fatalf("failed to register grpc-gateway: %v", err)
	}

	rootMux := http.NewServeMux()
	rootMux.HandleFunc("/v1/resolve", api.ResolveHandler(apiCfg, svc))
	rootMux.Handle("/", gatewayMux)

	httpServer := &http.Server{
		Addr:              cfg.HTTP.Addr,
		Handler:           rootMux,
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
		if err := svc.Close(); err != nil {
			log.Printf("service close error: %v", err)
		}
	}()

	log.Printf("sandbox-agent listening on %s (http) and %s (grpc)", cfg.HTTP.Addr, cfg.GRPC.Addr)
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
