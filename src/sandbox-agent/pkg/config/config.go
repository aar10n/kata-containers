package config

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var (
	labelKeyRegex   = regexp.MustCompile(`^([a-zA-Z0-9]([a-zA-Z0-9\-_.]*[a-zA-Z0-9])?/)?[a-zA-Z0-9]([a-zA-Z0-9\-_.]*[a-zA-Z0-9])?$`)
	labelValueRegex = regexp.MustCompile(`^([a-zA-Z0-9]([a-zA-Z0-9\-_.]*[a-zA-Z0-9])?)?$`)
)

// Config holds all configuration for sandbox-agent.
type Config struct {
	HTTP       HTTPConfig       `mapstructure:"http"`
	GRPC       GRPCConfig       `mapstructure:"grpc"`
	Kubernetes KubernetesConfig `mapstructure:"kubernetes"`
	Agent      AgentConfig      `mapstructure:"agent"`
	Exec       ExecConfig       `mapstructure:"exec"`
	Sandbox    SandboxConfig    `mapstructure:"sandbox"`
}

// HTTPConfig holds HTTP server configuration.
type HTTPConfig struct {
	Addr string `mapstructure:"addr"`
}

// GRPCConfig holds gRPC server configuration.
type GRPCConfig struct {
	Addr        string        `mapstructure:"addr"`
	DialTimeout time.Duration `mapstructure:"dial_timeout"`
}

// KubernetesConfig holds Kubernetes client configuration.
type KubernetesConfig struct {
	Kubeconfig     string        `mapstructure:"kubeconfig"`
	ResyncInterval time.Duration `mapstructure:"resync_interval"`
}

// AgentConfig holds kata-agent client configuration.
type AgentConfig struct {
	Timeout       time.Duration `mapstructure:"timeout"`
	ReadChunkSize int           `mapstructure:"read_chunk_size"`
	MaxOutputSize int           `mapstructure:"max_output_size"`
}

// ExecConfig holds command execution configuration.
type ExecConfig struct {
	Timeout time.Duration `mapstructure:"timeout"`
}

// SandboxConfig holds sandbox pod management configuration.
type SandboxConfig struct {
	Namespace        string            `mapstructure:"namespace"`
	RuntimeClassName string            `mapstructure:"runtime_class"`
	NodeSelector     map[string]string `mapstructure:"node_selector"`
}

// Flags defines command-line flags that can override config values.
type Flags struct {
	ConfigFile string
	HTTPAddr   string
	GRPCAddr   string
	Kubeconfig string
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		HTTP: HTTPConfig{
			Addr: ":8080",
		},
		GRPC: GRPCConfig{
			Addr:        ":9090",
			DialTimeout: 5 * time.Second,
		},
		Kubernetes: KubernetesConfig{
			Kubeconfig:     "",
			ResyncInterval: 5 * time.Minute,
		},
		Agent: AgentConfig{
			Timeout:       10 * time.Second,
			ReadChunkSize: 4096,
			MaxOutputSize: 1024 * 1024,
		},
		Exec: ExecConfig{
			Timeout: 30 * time.Second,
		},
		Sandbox: SandboxConfig{
			Namespace:        "default",
			RuntimeClassName: "",
		},
	}
}

// RegisterFlags registers command-line flags and returns a Flags struct.
// These flags can override config file values when bound with viper.
func RegisterFlags() *Flags {
	flags := &Flags{}

	pflag.StringVarP(&flags.ConfigFile, "config", "c", "", "Path to config file")
	pflag.StringVar(&flags.HTTPAddr, "http-addr", "", "HTTP listen address (overrides config)")
	pflag.StringVar(&flags.GRPCAddr, "grpc-addr", "", "gRPC listen address (overrides config)")
	pflag.StringVar(&flags.Kubeconfig, "kubeconfig", "", "Path to kubeconfig file (overrides config)")

	return flags
}

// Load loads configuration from file, environment, and flags.
// Priority (highest to lowest): flags > env > config file > defaults
func Load(flags *Flags) (Config, error) {
	// Use a delimiter that won't conflict with Kubernetes label keys
	// (which contain dots like "cloud.google.com/gke-os-distribution")
	v := viper.NewWithOptions(viper.KeyDelimiter("::"))

	// Set defaults
	defaults := DefaultConfig()
	v.SetDefault("http::addr", defaults.HTTP.Addr)
	v.SetDefault("grpc::addr", defaults.GRPC.Addr)
	v.SetDefault("grpc::dial_timeout", defaults.GRPC.DialTimeout)
	v.SetDefault("kubernetes::kubeconfig", defaults.Kubernetes.Kubeconfig)
	v.SetDefault("kubernetes::resync_interval", defaults.Kubernetes.ResyncInterval)
	v.SetDefault("agent::timeout", defaults.Agent.Timeout)
	v.SetDefault("agent::read_chunk_size", defaults.Agent.ReadChunkSize)
	v.SetDefault("agent::max_output_size", defaults.Agent.MaxOutputSize)
	v.SetDefault("exec::timeout", defaults.Exec.Timeout)
	v.SetDefault("sandbox::namespace", defaults.Sandbox.Namespace)
	v.SetDefault("sandbox::runtime_class", defaults.Sandbox.RuntimeClassName)

	// Enable environment variable overrides
	// Environment variables use SANDBOX_AGENT_ prefix with underscores
	// e.g., SANDBOX_AGENT_HTTP_ADDR, SANDBOX_AGENT_GRPC_ADDR
	v.SetEnvPrefix("SANDBOX_AGENT")
	v.SetEnvKeyReplacer(strings.NewReplacer("::", "_"))
	v.AutomaticEnv()

	// Load config file if specified
	if flags.ConfigFile != "" {
		v.SetConfigFile(flags.ConfigFile)
		if err := v.ReadInConfig(); err != nil {
			return Config{}, fmt.Errorf("read config file: %w", err)
		}
	}

	// Apply flag overrides (highest priority)
	if flags.HTTPAddr != "" {
		v.Set("http::addr", flags.HTTPAddr)
	}
	if flags.GRPCAddr != "" {
		v.Set("grpc::addr", flags.GRPCAddr)
	}
	if flags.Kubeconfig != "" {
		v.Set("kubernetes::kubeconfig", flags.Kubeconfig)
	}

	// Unmarshal into config struct
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return Config{}, fmt.Errorf("unmarshal config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// Validate checks configuration values for correctness.
func (c *Config) Validate() error {
	for k, v := range c.Sandbox.NodeSelector {
		if len(k) > 253 || !labelKeyRegex.MatchString(k) {
			return fmt.Errorf("invalid node selector key %q: must be a valid Kubernetes label key", k)
		}
		if len(v) > 63 || !labelValueRegex.MatchString(v) {
			return fmt.Errorf("invalid node selector value %q for key %q: must be a valid Kubernetes label value", v, k)
		}
	}
	return nil
}
