package prefetch

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/cri"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// Config holds configuration for the image prefetcher.
type Config struct {
	// Images is the list of images to prefetch.
	Images []string
	// Namespace is the Kubernetes namespace where image pull secrets are stored.
	Namespace string
	// ImagePullSecrets is the list of secret names containing registry credentials.
	ImagePullSecrets []string
}

// Prefetcher handles pre-pulling container images.
type Prefetcher struct {
	criClient *cri.Client
	k8sClient kubernetes.Interface
	config    Config
}

// New creates a new Prefetcher.
func New(criClient *cri.Client, k8sClient kubernetes.Interface, cfg Config) *Prefetcher {
	return &Prefetcher{
		criClient: criClient,
		k8sClient: k8sClient,
		config:    cfg,
	}
}

// PrefetchAll prefetches all configured images.
// Returns errors for images that failed to pull, but continues attempting all images.
func (p *Prefetcher) PrefetchAll(ctx context.Context) error {
	if len(p.config.Images) == 0 {
		return nil
	}

	// Load all credentials from secrets
	creds, err := p.loadCredentials(ctx)
	if err != nil {
		slog.Warn("failed to load image pull secrets", "error", err)
		// Continue without credentials - public images will still work
	}

	var errs []string
	for _, image := range p.config.Images {
		if err := p.prefetchImage(ctx, image, creds); err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", image, err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("failed to prefetch images: %s", strings.Join(errs, "; "))
	}
	return nil
}

// prefetchImage pulls a single image with appropriate credentials.
func (p *Prefetcher) prefetchImage(ctx context.Context, image string, creds map[string]*runtimeapi.AuthConfig) error {
	// Check if image already exists
	status, err := p.criClient.ImageStatus(ctx, image)
	if err != nil {
		slog.Warn("failed to check image status", "image", image, "error", err)
	} else if status != nil {
		slog.Info("image already present", "image", image, "id", status.Id)
		return nil
	}

	// Find matching credentials for this image
	auth := p.findAuthForImage(image, creds)
	if auth != nil {
		slog.Info("pulling image with credentials", "image", image)
	} else {
		slog.Info("pulling image without credentials", "image", image)
	}

	imageRef, err := p.criClient.PullImage(ctx, image, auth)
	if err != nil {
		return err
	}

	slog.Info("successfully pulled image", "image", image, "ref", imageRef)
	return nil
}

// loadCredentials loads credentials from all configured image pull secrets.
func (p *Prefetcher) loadCredentials(ctx context.Context) (map[string]*runtimeapi.AuthConfig, error) {
	creds := make(map[string]*runtimeapi.AuthConfig)

	if len(p.config.ImagePullSecrets) == 0 {
		slog.Info("no image pull secrets configured for prefetch")
		return creds, nil
	}

	for _, secretName := range p.config.ImagePullSecrets {
		secret, err := p.k8sClient.CoreV1().Secrets(p.config.Namespace).Get(ctx, secretName, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("get secret %s: %w", secretName, err)
		}

		secretCreds, err := parseDockerConfig(secret)
		if err != nil {
			return nil, fmt.Errorf("parse secret %s: %w", secretName, err)
		}

		registries := make([]string, 0, len(secretCreds))
		for registry := range secretCreds {
			registries = append(registries, registry)
		}
		slog.Info("loaded registry credentials from secret", "secret", secretName, "registries", registries)

		// Merge credentials (later secrets override earlier ones)
		for registry, auth := range secretCreds {
			creds[registry] = auth
		}
	}

	return creds, nil
}

// findAuthForImage finds the appropriate auth config for an image.
func (p *Prefetcher) findAuthForImage(image string, creds map[string]*runtimeapi.AuthConfig) *runtimeapi.AuthConfig {
	if len(creds) == 0 {
		return nil
	}

	registry := extractRegistry(image)
	slog.Info("looking for credentials", "registry", registry, "image", image)

	// Try exact match first
	if auth, ok := creds[registry]; ok {
		return auth
	}

	// Try with https:// prefix
	if auth, ok := creds["https://"+registry]; ok {
		return auth
	}

	// Try with https:// prefix and /v1/ suffix (Docker Hub format)
	if auth, ok := creds["https://"+registry+"/v1/"]; ok {
		return auth
	}

	// Try without port for registries with default ports
	if idx := strings.LastIndex(registry, ":"); idx != -1 {
		registryWithoutPort := registry[:idx]
		if auth, ok := creds[registryWithoutPort]; ok {
			return auth
		}
		if auth, ok := creds["https://"+registryWithoutPort]; ok {
			return auth
		}
	}

	return nil
}

// extractRegistry extracts the registry from an image reference.
// For images without an explicit registry, returns docker.io.
func extractRegistry(image string) string {
	// Remove tag/digest
	image = strings.Split(image, "@")[0]
	image = strings.Split(image, ":")[0]

	// Check if there's a registry prefix
	parts := strings.Split(image, "/")
	if len(parts) == 1 {
		// No slash - official Docker Hub image (e.g., "python")
		return "docker.io"
	}

	// Check if first part looks like a registry (has dot or colon or is localhost)
	firstPart := parts[0]
	if strings.Contains(firstPart, ".") || strings.Contains(firstPart, ":") || firstPart == "localhost" {
		return firstPart
	}

	// User/repo format - Docker Hub (e.g., "library/python")
	return "docker.io"
}

// DockerConfigJSON represents the structure of a .dockerconfigjson secret.
type DockerConfigJSON struct {
	Auths map[string]DockerConfigEntry `json:"auths"`
}

// DockerConfigEntry represents credentials for a single registry.
type DockerConfigEntry struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Email    string `json:"email"`
	Auth     string `json:"auth"` // base64(username:password)
}

// parseDockerConfig parses a Kubernetes docker config secret.
func parseDockerConfig(secret *corev1.Secret) (map[string]*runtimeapi.AuthConfig, error) {
	creds := make(map[string]*runtimeapi.AuthConfig)

	var data []byte
	var ok bool

	switch secret.Type {
	case corev1.SecretTypeDockerConfigJson:
		data, ok = secret.Data[corev1.DockerConfigJsonKey]
		if !ok {
			return nil, fmt.Errorf("secret missing %s key", corev1.DockerConfigJsonKey)
		}
	case corev1.SecretTypeDockercfg:
		data, ok = secret.Data[corev1.DockerConfigKey]
		if !ok {
			return nil, fmt.Errorf("secret missing %s key", corev1.DockerConfigKey)
		}
		// .dockercfg format is just the auths map directly
		var dockercfg map[string]DockerConfigEntry
		if err := json.Unmarshal(data, &dockercfg); err != nil {
			return nil, fmt.Errorf("unmarshal .dockercfg: %w", err)
		}
		for registry, entry := range dockercfg {
			auth, err := entryToAuthConfig(entry)
			if err != nil {
				slog.Warn("failed to parse credentials", "registry", registry, "error", err)
				continue
			}
			creds[registry] = auth
		}
		return creds, nil
	default:
		return nil, fmt.Errorf("unsupported secret type: %s", secret.Type)
	}

	var dockerConfig DockerConfigJSON
	if err := json.Unmarshal(data, &dockerConfig); err != nil {
		return nil, fmt.Errorf("unmarshal docker config: %w", err)
	}

	for registry, entry := range dockerConfig.Auths {
		auth, err := entryToAuthConfig(entry)
		if err != nil {
			slog.Warn("failed to parse credentials", "registry", registry, "error", err)
			continue
		}
		creds[registry] = auth
	}

	return creds, nil
}

// entryToAuthConfig converts a DockerConfigEntry to a CRI AuthConfig.
func entryToAuthConfig(entry DockerConfigEntry) (*runtimeapi.AuthConfig, error) {
	username := entry.Username
	password := entry.Password

	// If username/password not provided, try to decode from auth field
	if username == "" && password == "" && entry.Auth != "" {
		decoded, err := base64.StdEncoding.DecodeString(entry.Auth)
		if err != nil {
			return nil, fmt.Errorf("decode auth: %w", err)
		}
		parts := strings.SplitN(string(decoded), ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid auth format")
		}
		username = parts[0]
		password = parts[1]
	}

	if username == "" {
		return nil, fmt.Errorf("no username found")
	}

	return &runtimeapi.AuthConfig{
		Username: username,
		Password: password,
	}, nil
}
