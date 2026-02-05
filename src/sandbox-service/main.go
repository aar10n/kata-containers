package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/api"
	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/api/mcp"
	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/api/pb"
	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/capacity"
	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/config"
	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/platform"
	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/platform/docker"
	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/platform/kata"
	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/service"
	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/storage"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func main() {
	// Register and parse flags
	flags := config.RegisterFlags()
	pflag.Parse()

	// Load configuration
	cfg, err := config.Load(flags)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	platformImpl, err := buildPlatform(cfg)
	if err != nil {
		log.Fatalf("init platform: %v", err)
	}

	// Enable leader election for Kata platform (running in K8s)
	leaderElection := service.LeaderElectionConfig{}
	if cfg.Platform == "kata" {
		leaderElection = service.LeaderElectionConfig{
			Enabled:       true,
			Namespace:     cfg.Kata.LeaderElection.Namespace,
			LeaseName:     cfg.Kata.LeaderElection.LeaseName,
			LeaseDuration: cfg.Kata.LeaderElection.LeaseDuration,
			RenewDeadline: cfg.Kata.LeaderElection.RenewDeadline,
			RetryPeriod:   cfg.Kata.LeaderElection.RetryPeriod,
		}
	}

	svc := service.New(
		platformImpl,
		cfg.Sandbox.DefaultImage,
		cfg.Sandbox.DefaultCommand,
		cfg.Kata.MainContainer,
		cfg.Kata.ShellContainer,
		cfg.Exec.DefaultTimeout,
		cfg.Exec.MaxOutputBytes,
		cfg.Sandbox.DefaultTTL,
		cfg.Sandbox.CleanupInterval,
		leaderElection,
		cfg.Storage.StateTTL,
		cfg.Storage.StateCleanupInterval,
	)
	defer svc.Stop()

	// Initialize capacity tracker
	tracker := capacity.NewTracker(capacity.Config{
		MaxSandboxes:      cfg.Sandbox.MaxSandboxes,
		OvercommitPercent: cfg.Sandbox.OvercommitPercent,
	})
	svc.SetCapacityTracker(tracker)
	svc.SetCapacityConfig(
		cfg.Sandbox.CapacityRefreshInterval,
		cfg.Sandbox.EvictionInterval,
		cfg.Sandbox.EvictionEnabled,
	)
	svc.StartCapacityLoops()
	slog.Info("capacity tracking enabled",
		"max", cfg.Sandbox.MaxSandboxes,
		"overcommit_percent", cfg.Sandbox.OvercommitPercent,
		"refresh_interval", cfg.Sandbox.CapacityRefreshInterval,
		"eviction_enabled", cfg.Sandbox.EvictionEnabled)

	// Start agent watcher if enabled (for dynamic capacity tracking)
	if cfg.Sandbox.AgentWatcher.Enabled {
		k8sConfig, err := rest.InClusterConfig()
		if err != nil {
			log.Fatalf("failed to get in-cluster config: %v", err)
		}
		k8sClient, err := kubernetes.NewForConfig(k8sConfig)
		if err != nil {
			log.Fatalf("failed to create k8s client: %v", err)
		}

		agentWatcher := capacity.NewAgentWatcher(
			capacity.AgentWatcherConfig{
				Namespace:     cfg.Sandbox.AgentWatcher.Namespace,
				LabelSelector: cfg.Sandbox.AgentWatcher.LabelSelector,
				HTTPPort:      cfg.Sandbox.AgentWatcher.HTTPPort,
				PollInterval:  cfg.Sandbox.CapacityRefreshInterval,
				PollTimeout:   cfg.Sandbox.AgentWatcher.PollTimeout,
			},
			k8sClient,
			tracker,
		)

		ctx := context.Background()
		if err := agentWatcher.Start(ctx); err != nil {
			log.Fatalf("failed to start agent watcher: %v", err)
		}
		slog.Info("agent watcher started",
			"namespace", cfg.Sandbox.AgentWatcher.Namespace,
			"selector", cfg.Sandbox.AgentWatcher.LabelSelector)
	}

	// Start MCP server if enabled
	if cfg.MCP.Enabled {
		mcpServer := mcp.NewServer(svc)
		go func() {
			log.Printf("MCP server listening on %s", cfg.MCP.Addr)
			if err := mcpServer.ListenAndServe(cfg.MCP.Addr); err != nil && err != http.ErrServerClosed {
				log.Printf("MCP server error: %v", err)
			}
		}()
	}

	// Start storage gRPC server if enabled
	if cfg.Storage.Enabled {
		s3Client, err := storage.NewS3Client(storage.S3Config{
			Endpoint:        cfg.Storage.Endpoint,
			Region:          cfg.Storage.Region,
			Bucket:          cfg.Storage.Bucket,
			AccessKeyID:     cfg.Storage.AccessKeyID,
			SecretAccessKey: cfg.Storage.SecretAccessKey,
			PresignExpiry:   cfg.Storage.PresignExpiry,
			ForcePathStyle:  cfg.Storage.ForcePathStyle,
		})
		if err != nil {
			log.Fatalf("init s3 client: %v", err)
		}

		// Set storage client on service for snapshot restore
		svc.SetStorageClient(s3Client)

		// Set state cleanup client if state TTL is configured
		if cfg.Storage.StateTTL > 0 {
			svc.SetStateCleanupClient(s3Client)
			log.Printf("state cleanup enabled: ttl=%v, interval=%v", cfg.Storage.StateTTL, cfg.Storage.StateCleanupInterval)
		}

		storageServer := api.NewStorageServer(s3Client)
		grpcServer := grpc.NewServer()
		pb.RegisterStorageServiceServer(grpcServer, storageServer)

		go func() {
			lis, err := net.Listen("tcp", cfg.GRPC.Addr)
			if err != nil {
				log.Fatalf("failed to listen on %s: %v", cfg.GRPC.Addr, err)
			}
			log.Printf("storage gRPC server listening on %s", cfg.GRPC.Addr)
			if err := grpcServer.Serve(lis); err != nil {
				log.Printf("gRPC server error: %v", err)
			}
		}()
	}

	// Start HTTP API server
	server := api.NewServer(svc)
	httpServer := &http.Server{
		Addr:              cfg.HTTP.Addr,
		Handler:           server.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("sandbox-service listening on %s", cfg.HTTP.Addr)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("http server error: %v", err)
	}
}

func buildPlatform(cfg config.Config) (platform.Platform, error) {
	switch cfg.Platform {
	case "kata":
		return kata.New(kata.Config{
			SandboxAgentAddr: cfg.Kata.SandboxAgentAddr,
		})
	case "docker":
		return docker.New()
	default:
		return nil, fmt.Errorf("unknown platform: %s", cfg.Platform)
	}
}
