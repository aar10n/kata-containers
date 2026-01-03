package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	switch value.Kind {
	case yaml.ScalarNode:
		parsed, err := time.ParseDuration(value.Value)
		if err == nil {
			d.Duration = parsed
			return nil
		}
		return fmt.Errorf("invalid duration %q: %w", value.Value, err)
	default:
		return fmt.Errorf("invalid duration node kind %v", value.Kind)
	}
}

type Config struct {
	HTTP struct {
		Addr string `yaml:"addr"`
	} `yaml:"http"`
	Platform string        `yaml:"platform"`
	Kata     KataConfig    `yaml:"kata"`
	Docker   DockerConfig  `yaml:"docker"`
	Exec     ExecConfig    `yaml:"exec"`
	Sandbox  SandboxConfig `yaml:"sandbox"`
}

type KataConfig struct {
	Namespace        string            `yaml:"namespace"`
	RuntimeClass     string            `yaml:"runtime_class"`
	NodeSelector     map[string]string `yaml:"node_selector"`
	DefaultImage     string            `yaml:"default_image"`
	DefaultCommand   []string          `yaml:"default_command"`
	SandboxAgentAddr string            `yaml:"sandbox_agent_addr"`
}

type DockerConfig struct {
	DefaultImage   string   `yaml:"default_image"`
	DefaultCommand []string `yaml:"default_command"`
}

type ExecConfig struct {
	DefaultTimeout Duration `yaml:"default_timeout"`
	MaxOutputBytes int      `yaml:"max_output_bytes"`
}

type SandboxConfig struct {
	DefaultTTL      Duration `yaml:"default_ttl"`
	CleanupInterval Duration `yaml:"cleanup_interval"`
}

func DefaultConfig() Config {
	var cfg Config
	cfg.HTTP.Addr = ":8080"
	cfg.Platform = "kata"
	cfg.Kata.Namespace = "kata-sandboxes"
	cfg.Kata.RuntimeClass = "kata-qemu"
	cfg.Kata.NodeSelector = map[string]string{
		"katacontainers.io/kata-runtime": "true",
	}
	cfg.Kata.DefaultImage = "python:3.11-slim"
	cfg.Kata.DefaultCommand = []string{"sleep", "infinity"}
	cfg.Kata.SandboxAgentAddr = "http://kata-deploy-sandbox-agent.kata-system:8080"
	cfg.Docker.DefaultImage = "python:3.11-slim"
	cfg.Docker.DefaultCommand = []string{"sleep", "infinity"}
	cfg.Exec.DefaultTimeout = Duration{Duration: 30 * time.Second}
	cfg.Exec.MaxOutputBytes = 1024 * 1024
	cfg.Sandbox.DefaultTTL = Duration{Duration: 10 * time.Minute}
	cfg.Sandbox.CleanupInterval = Duration{Duration: 30 * time.Second}
	return cfg
}

func Load(path string) (Config, error) {
	cfg := DefaultConfig()
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("read config: %w", err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("parse config: %w", err)
	}
	if cfg.HTTP.Addr == "" {
		return cfg, errors.New("http.addr is required")
	}
	if cfg.Platform == "" {
		return cfg, errors.New("platform is required")
	}
	return cfg, nil
}

func ApplyEnvOverrides(cfg *Config) {
	if value := os.Getenv("SANDBOX_SERVICE_PLATFORM"); value != "" {
		cfg.Platform = value
	}
	if value := os.Getenv("SANDBOX_SERVICE_HTTP_ADDR"); value != "" {
		cfg.HTTP.Addr = value
	}
}
