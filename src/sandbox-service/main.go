package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/api"
	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/config"
	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/platform"
	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/platform/docker"
	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/platform/kata"
	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/service"
)

func main() {
	configPath := flag.String("config", defaultConfigPath(), "path to config file")
	platformOverride := flag.String("platform", "", "platform override (kata or docker)")
	httpAddr := flag.String("http-addr", "", "http listen address")
	kataNamespace := flag.String("kata-namespace", "", "kubernetes namespace for kata pods")
	kataRuntimeClass := flag.String("kata-runtime-class", "", "runtime class for kata pods")
	kataSandboxAgent := flag.String("kata-sandbox-agent-addr", "", "sandbox-agent address")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	config.ApplyEnvOverrides(&cfg)

	if *platformOverride != "" {
		cfg.Platform = *platformOverride
	}
	if *httpAddr != "" {
		cfg.HTTP.Addr = *httpAddr
	}
	if *kataNamespace != "" {
		cfg.Kata.Namespace = *kataNamespace
	}
	if *kataRuntimeClass != "" {
		cfg.Kata.RuntimeClass = *kataRuntimeClass
	}
	if *kataSandboxAgent != "" {
		cfg.Kata.SandboxAgentAddr = *kataSandboxAgent
	}

	platformImpl, defaultImage, defaultCommand, err := buildPlatform(cfg)
	if err != nil {
		log.Fatalf("init platform: %v", err)
	}

	svc := service.New(
		platformImpl,
		defaultImage,
		defaultCommand,
		cfg.Exec.DefaultTimeout.Duration,
		cfg.Exec.MaxOutputBytes,
		cfg.Sandbox.DefaultTTL.Duration,
		cfg.Sandbox.CleanupInterval.Duration,
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

func defaultConfigPath() string {
	if value := os.Getenv("SANDBOX_SERVICE_CONFIG"); value != "" {
		return value
	}
	return "config.yaml"
}

func buildPlatform(cfg config.Config) (platform.Platform, string, []string, error) {
	switch cfg.Platform {
	case "kata":
		plat, err := kata.New(cfg.Kata.Namespace, cfg.Kata.RuntimeClass, cfg.Kata.NodeSelector, cfg.Kata.DefaultImage, cfg.Kata.DefaultCommand, cfg.Kata.SandboxAgentAddr)
		return plat, cfg.Kata.DefaultImage, cfg.Kata.DefaultCommand, err
	case "docker":
		plat, err := docker.New(cfg.Docker.DefaultImage, cfg.Docker.DefaultCommand)
		return plat, cfg.Docker.DefaultImage, cfg.Docker.DefaultCommand, err
	default:
		return nil, "", nil, fmt.Errorf("unknown platform: %s", cfg.Platform)
	}
}
