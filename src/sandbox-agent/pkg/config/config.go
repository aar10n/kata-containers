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

// Mode represents the sandbox execution mode.
type Mode string

const (
	// ModeKata runs sandboxes as Kata VMs using kata-agent.
	ModeKata Mode = "kata"
	// ModePod runs sandboxes as regular Kubernetes pods using CRI.
	ModePod Mode = "pod"
)

// Config holds all configuration for sandbox-agent.
type Config struct {
	// Mode specifies the sandbox execution mode: "kata" or "pod".
	// Default: "kata"
	Mode Mode `mapstructure:"mode"`

	HTTP       HTTPConfig       `mapstructure:"http"`
	GRPC       GRPCConfig       `mapstructure:"grpc"`
	Kubernetes KubernetesConfig `mapstructure:"kubernetes"`
	Agent      AgentConfig      `mapstructure:"agent"`
	CRI        CRIConfig        `mapstructure:"cri"`
	HostFS     HostFSConfig     `mapstructure:"hostfs"`
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

// CRIConfig holds CRI client configuration (for pod mode).
type CRIConfig struct {
	// Socket is the path to the CRI socket.
	// Auto-detected if empty: tries /run/containerd/containerd.sock then /var/run/crio/crio.sock
	Socket string `mapstructure:"socket"`
	// Timeout for CRI operations.
	Timeout time.Duration `mapstructure:"timeout"`
}

// HostFSConfig holds host filesystem configuration for direct emptyDir access.
type HostFSConfig struct {
	// KubeletRoot is the path to the kubelet root directory.
	// Default: /var/lib/kubelet
	KubeletRoot string `mapstructure:"kubelet_root"`
}

// SandboxConfig holds sandbox pod management configuration.
type SandboxConfig struct {
	Namespace        string            `mapstructure:"namespace"`
	RuntimeClassName string            `mapstructure:"runtime_class"`
	NodeSelector     map[string]string `mapstructure:"node_selector"`
}

// Flags defines command-line flags that can override config values.
type Flags struct {
	ConfigFile  string
	Mode        string
	HTTPAddr    string
	GRPCAddr    string
	Kubeconfig  string
	CRISocket   string
	KubeletRoot string
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		Mode: ModeKata, // Default to kata mode for backwards compatibility
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
		CRI: CRIConfig{
			Socket:  "", // Auto-detect
			Timeout: 30 * time.Second,
		},
		HostFS: HostFSConfig{
			KubeletRoot: "/var/lib/kubelet",
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
	pflag.StringVar(&flags.Mode, "mode", "", "Sandbox execution mode: kata or pod (overrides config)")
	pflag.StringVar(&flags.HTTPAddr, "http-addr", "", "HTTP listen address (overrides config)")
	pflag.StringVar(&flags.GRPCAddr, "grpc-addr", "", "gRPC listen address (overrides config)")
	pflag.StringVar(&flags.Kubeconfig, "kubeconfig", "", "Path to kubeconfig file (overrides config)")
	pflag.StringVar(&flags.CRISocket, "cri-socket", "", "CRI socket path (overrides config, pod mode only)")
	pflag.StringVar(&flags.KubeletRoot, "kubelet-root", "", "Kubelet root directory (overrides config)")

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
	v.SetDefault("mode", string(defaults.Mode))
	v.SetDefault("http::addr", defaults.HTTP.Addr)
	v.SetDefault("grpc::addr", defaults.GRPC.Addr)
	v.SetDefault("grpc::dial_timeout", defaults.GRPC.DialTimeout)
	v.SetDefault("kubernetes::kubeconfig", defaults.Kubernetes.Kubeconfig)
	v.SetDefault("kubernetes::resync_interval", defaults.Kubernetes.ResyncInterval)
	v.SetDefault("agent::timeout", defaults.Agent.Timeout)
	v.SetDefault("agent::read_chunk_size", defaults.Agent.ReadChunkSize)
	v.SetDefault("agent::max_output_size", defaults.Agent.MaxOutputSize)
	v.SetDefault("cri::socket", defaults.CRI.Socket)
	v.SetDefault("cri::timeout", defaults.CRI.Timeout)
	v.SetDefault("hostfs::kubelet_root", defaults.HostFS.KubeletRoot)
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
	if flags.Mode != "" {
		v.Set("mode", flags.Mode)
	}
	if flags.HTTPAddr != "" {
		v.Set("http::addr", flags.HTTPAddr)
	}
	if flags.GRPCAddr != "" {
		v.Set("grpc::addr", flags.GRPCAddr)
	}
	if flags.Kubeconfig != "" {
		v.Set("kubernetes::kubeconfig", flags.Kubeconfig)
	}
	if flags.CRISocket != "" {
		v.Set("cri::socket", flags.CRISocket)
	}
	if flags.KubeletRoot != "" {
		v.Set("hostfs::kubelet_root", flags.KubeletRoot)
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
	// Validate mode
	if c.Mode != ModeKata && c.Mode != ModePod {
		return fmt.Errorf("invalid mode %q: must be 'kata' or 'pod'", c.Mode)
	}

	// Validate node selector labels
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

// IsPodMode returns true if the agent is running in pod mode.
func (c *Config) IsPodMode() bool {
	return c.Mode == ModePod
}

// IsKataMode returns true if the agent is running in kata mode.
func (c *Config) IsKataMode() bool {
	return c.Mode == ModeKata
}
