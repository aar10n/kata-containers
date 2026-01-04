package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/api"
	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/config"
	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/platform"
	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/platform/docker"
	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/platform/kata"
	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/service"
	"github.com/spf13/pflag"
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

	platformImpl, defaultImage, defaultCommand, err := buildPlatform(cfg)
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
		defaultImage,
		defaultCommand,
		cfg.Kata.MainContainer,
		cfg.Kata.ShellContainer,
		cfg.Exec.DefaultTimeout,
		cfg.Exec.MaxOutputBytes,
		cfg.Sandbox.DefaultTTL,
		cfg.Sandbox.CleanupInterval,
		leaderElection,
	)
	defer svc.Stop()
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

func buildPlatform(cfg config.Config) (platform.Platform, string, []string, error) {
	switch cfg.Platform {
	case "kata":
		plat, err := kata.New(kata.Config{
			SandboxAgentAddr: cfg.Kata.SandboxAgentAddr,
		})
		// For Kata, image/command are managed by sandbox-agent
		return plat, "", nil, err
	case "docker":
		plat, err := docker.New(cfg.Docker.DefaultImage, cfg.Docker.DefaultCommand)
		return plat, cfg.Docker.DefaultImage, cfg.Docker.DefaultCommand, err
	default:
		return nil, "", nil, fmt.Errorf("unknown platform: %s", cfg.Platform)
	}
}
