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

	HTTP        HTTPConfig        `mapstructure:"http"`
	GRPC        GRPCConfig        `mapstructure:"grpc"`
	Kubernetes  KubernetesConfig  `mapstructure:"kubernetes"`
	Agent       AgentConfig       `mapstructure:"agent"`
	CRI         CRIConfig         `mapstructure:"cri"`
	HostFS      HostFSConfig      `mapstructure:"hostfs"`
	Exec        ExecConfig        `mapstructure:"exec"`
	Sandbox     SandboxConfig     `mapstructure:"sandbox"`
	Storage     StorageConfig     `mapstructure:"storage"`
	NodeLabel   NodeLabelConfig   `mapstructure:"node_label"`
	FuseStorage FuseStorageConfig `mapstructure:"fuse_storage"`
	ActivityDB  ActivityDBConfig  `mapstructure:"activity_db"`
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
	Tolerations      []Toleration      `mapstructure:"tolerations"`
	ImagePullSecrets []ImagePullSecret `mapstructure:"image_pull_secrets"`
	// Resources specifies the default resource requests and limits for sandbox containers.
	// Values should be in Kubernetes resource quantity format (e.g., "100m", "256Mi", "1Gi").
	Resources ResourcesConfig `mapstructure:"resources"`
	// DefaultImage is the default container image used for sandboxes.
	// If set and PrefetchImages is enabled, this image will be pre-pulled on startup.
	DefaultImage string `mapstructure:"default_image"`
	// PrefetchImages controls whether to pre-pull the default image on startup.
	// Requires DefaultImage to be set.
	PrefetchImages bool `mapstructure:"prefetch_images"`
	// CapacityRefreshInterval is how often to recalculate node capacity.
	// Default: 30s
	CapacityRefreshInterval time.Duration `mapstructure:"capacity_refresh_interval"`
	// ClusterCapacityRefreshInterval is how often to poll peer agents for cluster capacity.
	// Default: 15s
	ClusterCapacityRefreshInterval time.Duration `mapstructure:"cluster_capacity_refresh_interval"`
	// PeerPollTimeout is the timeout for polling a single peer agent.
	// Default: 3s
	PeerPollTimeout time.Duration `mapstructure:"peer_poll_timeout"`
}

// ImagePullSecret represents a Kubernetes image pull secret reference.
type ImagePullSecret struct {
	Name string `mapstructure:"name"`
}

// Toleration represents a Kubernetes toleration for sandbox pods.
type Toleration struct {
	Key               string `mapstructure:"key"`
	Operator          string `mapstructure:"operator"` // "Exists" or "Equal"
	Value             string `mapstructure:"value"`
	Effect            string `mapstructure:"effect"` // "NoSchedule", "PreferNoSchedule", or "NoExecute"
	TolerationSeconds *int64 `mapstructure:"toleration_seconds"`
}

// ResourcesConfig holds Kubernetes-style resource requirements for sandbox containers.
// Values should be in Kubernetes resource quantity format (e.g., "100m", "256Mi", "1Gi").
type ResourcesConfig struct {
	Requests ResourceList `mapstructure:"requests"`
	Limits   ResourceList `mapstructure:"limits"`
}

// ResourceList holds CPU and memory resource quantities.
type ResourceList struct {
	CPU    string `mapstructure:"cpu"`
	Memory string `mapstructure:"memory"`
}

// StorageConfig holds configuration for the storage service client.
type StorageConfig struct {
	// Enabled indicates whether storage/snapshot functionality is enabled.
	// When enabled, sandbox-agent will connect to sandbox-service's storage gRPC server
	// to get presigned URLs for uploading/downloading snapshots.
	Enabled bool `mapstructure:"enabled"`
	// Addr is the address of the sandbox-service storage gRPC server.
	// Example: "sandbox-service.kata-system:9090"
	Addr string `mapstructure:"addr"`
	// Timeout for storage RPC calls.
	Timeout time.Duration `mapstructure:"timeout"`
	// InitImage is the container image used for the init container that
	// downloads and extracts snapshots during pod creation.
	// Should contain curl and tar.
	InitImage string `mapstructure:"init_image"`
}

// NodeLabelConfig holds configuration for node labeling.
type NodeLabelConfig struct {
	// Enabled indicates whether the agent should label its node on startup.
	// When enabled, the agent will add a label to identify nodes ready for sandboxes.
	Enabled bool `mapstructure:"enabled"`
	// Key is the label key to apply to the node.
	// Default: "sandbox.kata.io/agent"
	Key string `mapstructure:"key"`
	// Value is the label value to apply to the node.
	// Default: "true"
	Value string `mapstructure:"value"`
}

// ActivityDBConfig holds configuration for the SQLite activity database.
// When enabled, sandbox activity timestamps are stored locally instead of
// updating Kubernetes annotations, avoiding API rate limiting.
type ActivityDBConfig struct {
	// Enabled indicates whether to use SQLite for activity tracking.
	// When disabled, falls back to K8s annotation updates (existing behavior).
	Enabled bool `mapstructure:"enabled"`
	// Path is the directory for the SQLite database file.
	// Default: /var/lib/sandbox-agent
	Path string `mapstructure:"path"`
	// Filename is the database filename.
	// Default: activity.db
	Filename string `mapstructure:"filename"`
}

// FuseStorageConfig holds configuration for the FUSE sidecar S3 mounts.
// When enabled, sandbox pods get a sidecar container that mounts S3 buckets
// using mountpoint-s3. This replaces the emptyDir-based storage and snapshot system.
type FuseStorageConfig struct {
	// Enabled indicates whether FUSE sidecar should be added to sandbox pods.
	Enabled bool `mapstructure:"enabled"`
	// Image is the container image for the FUSE sidecar (mountpoint-s3).
	Image string `mapstructure:"image"`
	// Endpoint is the S3-compatible endpoint URL (e.g., "http://s3proxy.kata-system:80").
	Endpoint string `mapstructure:"endpoint"`
	// Region is the AWS region.
	Region string `mapstructure:"region"`
	// AssetsBucket is the bucket for all sandbox data.
	// - User drives (/mydrive): {assets_bucket}/my_drive/{user_id}/
	// - Session data (/data): {assets_bucket}/sandboxes/{session_id}/
	AssetsBucket string `mapstructure:"assets_bucket"`
	// AccessKeyID for S3 authentication.
	AccessKeyID string `mapstructure:"access_key_id"`
	// SecretAccessKey for S3 authentication (direct value).
	SecretAccessKey string `mapstructure:"secret_access_key"`
	// SecretAccessKeySecretName is the name of a Kubernetes Secret containing the secret access key.
	// When set, SecretAccessKey is ignored and the sidecar env var uses valueFrom.secretKeyRef.
	SecretAccessKeySecretName string `mapstructure:"secret_access_key_secret_name"`
	// SecretAccessKeySecretKey is the key within the secret. Defaults to "secretAccessKey".
	SecretAccessKeySecretKey string `mapstructure:"secret_access_key_secret_key"`
	// UID is the user ID for the sandbox user that owns the FUSE mounts.
	// Default: 1001
	UID int `mapstructure:"uid"`
	// GID is the group ID for the sandbox user that owns the FUSE mounts.
	// Default: 1001
	GID int `mapstructure:"gid"`
	// SidecarResources specifies resource requests/limits for the FUSE sidecar container.
	// These are also used by the capacity manager to calculate accurate node capacity.
	SidecarResources ResourcesConfig `mapstructure:"sidecar_resources"`
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
			Namespace:                      "default",
			RuntimeClassName:               "",
			CapacityRefreshInterval:        30 * time.Second,
			ClusterCapacityRefreshInterval: 15 * time.Second,
			PeerPollTimeout:                3 * time.Second,
		},
		Storage: StorageConfig{
			Enabled:   false,
			Addr:      "sandbox-service.kata-system:9090",
			Timeout:   30 * time.Second,
			InitImage: "busybox:1.36",
		},
		NodeLabel: NodeLabelConfig{
			Enabled: false,
			Key:     "sandbox.kata.io/agent",
			Value:   "true",
		},
		FuseStorage: FuseStorageConfig{
			Enabled: false,
			Image:   "amazon/aws-mountpoint-s3:latest",
			Region:  "us-east-1",
			UID:     1001,
			GID:     1001,
			SidecarResources: ResourcesConfig{
				Requests: ResourceList{CPU: "200m", Memory: "128Mi"},
			},
		},
		ActivityDB: ActivityDBConfig{
			Enabled:  true, // Enable by default since this solves the rate limiting issue
			Path:     "/var/lib/sandbox-agent",
			Filename: "activity.db",
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
	v.SetDefault("sandbox::default_image", defaults.Sandbox.DefaultImage)
	v.SetDefault("sandbox::prefetch_images", defaults.Sandbox.PrefetchImages)
	v.SetDefault("sandbox::capacity_refresh_interval", defaults.Sandbox.CapacityRefreshInterval)
	v.SetDefault("sandbox::cluster_capacity_refresh_interval", defaults.Sandbox.ClusterCapacityRefreshInterval)
	v.SetDefault("sandbox::peer_poll_timeout", defaults.Sandbox.PeerPollTimeout)
	v.SetDefault("storage::enabled", defaults.Storage.Enabled)
	v.SetDefault("storage::addr", defaults.Storage.Addr)
	v.SetDefault("storage::timeout", defaults.Storage.Timeout)
	v.SetDefault("storage::init_image", defaults.Storage.InitImage)
	v.SetDefault("node_label::enabled", defaults.NodeLabel.Enabled)
	v.SetDefault("node_label::key", defaults.NodeLabel.Key)
	v.SetDefault("node_label::value", defaults.NodeLabel.Value)
	v.SetDefault("fuse_storage::enabled", defaults.FuseStorage.Enabled)
	v.SetDefault("fuse_storage::image", defaults.FuseStorage.Image)
	v.SetDefault("fuse_storage::endpoint", defaults.FuseStorage.Endpoint)
	v.SetDefault("fuse_storage::region", defaults.FuseStorage.Region)
	v.SetDefault("fuse_storage::assets_bucket", defaults.FuseStorage.AssetsBucket)
	v.SetDefault("fuse_storage::access_key_id", defaults.FuseStorage.AccessKeyID)
	v.SetDefault("fuse_storage::secret_access_key", defaults.FuseStorage.SecretAccessKey)
	v.SetDefault("fuse_storage::uid", defaults.FuseStorage.UID)
	v.SetDefault("fuse_storage::gid", defaults.FuseStorage.GID)
	v.SetDefault("fuse_storage::sidecar_resources::requests::cpu", defaults.FuseStorage.SidecarResources.Requests.CPU)
	v.SetDefault("fuse_storage::sidecar_resources::requests::memory", defaults.FuseStorage.SidecarResources.Requests.Memory)
	v.SetDefault("fuse_storage::sidecar_resources::limits::cpu", defaults.FuseStorage.SidecarResources.Limits.CPU)
	v.SetDefault("fuse_storage::sidecar_resources::limits::memory", defaults.FuseStorage.SidecarResources.Limits.Memory)
	v.SetDefault("activity_db::enabled", defaults.ActivityDB.Enabled)
	v.SetDefault("activity_db::path", defaults.ActivityDB.Path)
	v.SetDefault("activity_db::filename", defaults.ActivityDB.Filename)

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

	// Validate node label config
	if c.NodeLabel.Enabled {
		if c.NodeLabel.Key == "" {
			return fmt.Errorf("node_label.key is required when node_label.enabled is true")
		}
		if len(c.NodeLabel.Key) > 253 || !labelKeyRegex.MatchString(c.NodeLabel.Key) {
			return fmt.Errorf("invalid node_label.key %q: must be a valid Kubernetes label key", c.NodeLabel.Key)
		}
		if len(c.NodeLabel.Value) > 63 || !labelValueRegex.MatchString(c.NodeLabel.Value) {
			return fmt.Errorf("invalid node_label.value %q: must be a valid Kubernetes label value", c.NodeLabel.Value)
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
