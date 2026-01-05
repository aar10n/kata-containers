package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Config holds all configuration for sandbox-service.
type Config struct {
	HTTP     HTTPConfig    `mapstructure:"http"`
	MCP      MCPConfig     `mapstructure:"mcp"`
	Platform string        `mapstructure:"platform"`
	Kata     KataConfig    `mapstructure:"kata"`
	Exec     ExecConfig    `mapstructure:"exec"`
	Sandbox  SandboxConfig `mapstructure:"sandbox"`
}

// HTTPConfig holds HTTP server configuration.
type HTTPConfig struct {
	Addr string `mapstructure:"addr"`
}

// MCPConfig holds MCP server configuration.
type MCPConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Addr    string `mapstructure:"addr"`
}

// KataConfig holds Kata platform configuration.
type KataConfig struct {
	// SandboxAgentAddr is the address of the sandbox-agent service.
	// The agent handles all K8s/Kata-specific operations.
	SandboxAgentAddr string `mapstructure:"sandbox_agent_addr"`

	// Container names (optional, for multi-container sandbox support)
	MainContainer  string `mapstructure:"main_container"`
	ShellContainer string `mapstructure:"shell_container"`

	// Leader election for cleanup coordination
	LeaderElection LeaderElectionConfig `mapstructure:"leader_election"`
}

// LeaderElectionConfig holds settings for K8s leader election.
type LeaderElectionConfig struct {
	// Namespace for the Lease resource (defaults to pod namespace)
	Namespace string `mapstructure:"namespace"`
	// LeaseName is the name of the Lease resource
	LeaseName string `mapstructure:"lease_name"`
	// LeaseDuration is how long a leader holds the lease
	LeaseDuration time.Duration `mapstructure:"lease_duration"`
	// RenewDeadline is how long the leader has to renew before losing leadership
	RenewDeadline time.Duration `mapstructure:"renew_deadline"`
	// RetryPeriod is how often non-leaders retry to acquire the lease
	RetryPeriod time.Duration `mapstructure:"retry_period"`
}

// ExecConfig holds command execution configuration.
type ExecConfig struct {
	DefaultTimeout time.Duration `mapstructure:"default_timeout"`
	MaxOutputBytes int           `mapstructure:"max_output_bytes"`
}

// SandboxConfig holds sandbox lifecycle configuration.
type SandboxConfig struct {
	DefaultTTL      time.Duration `mapstructure:"default_ttl"`
	CleanupInterval time.Duration `mapstructure:"cleanup_interval"`
	// DefaultImage is the container image to use for new sandboxes.
	DefaultImage string `mapstructure:"default_image"`
	// DefaultCommand is the command to run in new sandbox containers.
	DefaultCommand []string `mapstructure:"default_command"`
}

// Flags defines command-line flags that can override config values.
type Flags struct {
	ConfigFile       string
	Platform         string
	HTTPAddr         string
	MCPAddr          string
	KataAgentAddr    string
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		HTTP: HTTPConfig{
			Addr: ":8080",
		},
		MCP: MCPConfig{
			Enabled: true,
			Addr:    ":8081",
		},
		Platform: "kata",
		Kata: KataConfig{
			SandboxAgentAddr: "http://kata-deploy-sandbox-agent.kata-system:8080",
			MainContainer:    "sandbox",
			ShellContainer:   "shell",
			LeaderElection: LeaderElectionConfig{
				LeaseName:     "sandbox-service-cleanup",
				LeaseDuration: 15 * time.Second,
				RenewDeadline: 10 * time.Second,
				RetryPeriod:   2 * time.Second,
			},
		},
		Exec: ExecConfig{
			DefaultTimeout: 30 * time.Second,
			MaxOutputBytes: 1024 * 1024,
		},
		Sandbox: SandboxConfig{
			DefaultTTL:      10 * time.Minute,
			CleanupInterval: 30 * time.Second,
			DefaultImage:    "python:3.11-slim",
			DefaultCommand:  []string{"sleep", "infinity"},
		},
	}
}

// RegisterFlags registers command-line flags and returns a Flags struct.
// These flags can override config file values when bound with viper.
func RegisterFlags() *Flags {
	flags := &Flags{}

	pflag.StringVarP(&flags.ConfigFile, "config", "c", "", "Path to config file")
	pflag.StringVarP(&flags.Platform, "platform", "p", "", "Platform type: kata or docker (overrides config)")
	pflag.StringVar(&flags.HTTPAddr, "http-addr", "", "HTTP listen address (overrides config)")
	pflag.StringVar(&flags.MCPAddr, "mcp-addr", "", "MCP server listen address (overrides config)")
	pflag.StringVar(&flags.KataAgentAddr, "kata-agent-addr", "", "Sandbox-agent address for Kata platform (overrides config)")

	return flags
}

// Load loads configuration from file, environment, and flags.
// Priority (highest to lowest): flags > env > config file > defaults
func Load(flags *Flags) (Config, error) {
	v := viper.New()

	// Set defaults
	defaults := DefaultConfig()
	v.SetDefault("http.addr", defaults.HTTP.Addr)
	v.SetDefault("mcp.enabled", defaults.MCP.Enabled)
	v.SetDefault("mcp.addr", defaults.MCP.Addr)
	v.SetDefault("platform", defaults.Platform)
	v.SetDefault("kata.sandbox_agent_addr", defaults.Kata.SandboxAgentAddr)
	v.SetDefault("kata.main_container", defaults.Kata.MainContainer)
	v.SetDefault("kata.shell_container", defaults.Kata.ShellContainer)
	v.SetDefault("kata.leader_election.lease_name", defaults.Kata.LeaderElection.LeaseName)
	v.SetDefault("kata.leader_election.lease_duration", defaults.Kata.LeaderElection.LeaseDuration)
	v.SetDefault("kata.leader_election.renew_deadline", defaults.Kata.LeaderElection.RenewDeadline)
	v.SetDefault("kata.leader_election.retry_period", defaults.Kata.LeaderElection.RetryPeriod)
	v.SetDefault("exec.default_timeout", defaults.Exec.DefaultTimeout)
	v.SetDefault("exec.max_output_bytes", defaults.Exec.MaxOutputBytes)
	v.SetDefault("sandbox.default_ttl", defaults.Sandbox.DefaultTTL)
	v.SetDefault("sandbox.cleanup_interval", defaults.Sandbox.CleanupInterval)
	v.SetDefault("sandbox.default_image", defaults.Sandbox.DefaultImage)
	v.SetDefault("sandbox.default_command", defaults.Sandbox.DefaultCommand)

	// Enable environment variable overrides
	// Environment variables use SANDBOX_SERVICE_ prefix with underscores
	// e.g., SANDBOX_SERVICE_HTTP_ADDR, SANDBOX_SERVICE_PLATFORM
	v.SetEnvPrefix("SANDBOX_SERVICE")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Load config file if specified
	if flags.ConfigFile != "" {
		v.SetConfigFile(flags.ConfigFile)
		if err := v.ReadInConfig(); err != nil {
			return Config{}, fmt.Errorf("read config file: %w", err)
		}
	}

	// Apply flag overrides (highest priority)
	if flags.Platform != "" {
		v.Set("platform", flags.Platform)
	}
	if flags.HTTPAddr != "" {
		v.Set("http.addr", flags.HTTPAddr)
	}
	if flags.MCPAddr != "" {
		v.Set("mcp.addr", flags.MCPAddr)
	}
	if flags.KataAgentAddr != "" {
		v.Set("kata.sandbox_agent_addr", flags.KataAgentAddr)
	}

	// Unmarshal into config struct
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return Config{}, fmt.Errorf("unmarshal config: %w", err)
	}

	// Validate required fields
	if cfg.HTTP.Addr == "" {
		return Config{}, fmt.Errorf("http.addr is required")
	}
	if cfg.Platform == "" {
		return Config{}, fmt.Errorf("platform is required")
	}
	if cfg.Platform != "kata" && cfg.Platform != "docker" {
		return Config{}, fmt.Errorf("platform must be 'kata' or 'docker', got: %s", cfg.Platform)
	}

	return cfg, nil
}
