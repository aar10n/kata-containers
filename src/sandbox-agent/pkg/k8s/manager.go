package k8s

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

const (
	// SandboxDataVolumeName is the name of the emptyDir volume for sandbox data.
	SandboxDataVolumeName = "sandbox-data"
	// SandboxDataMountPath is the mount path inside the container for the data volume.
	SandboxDataMountPath = "/data"
	// SnapshotFinalizer is added to pods when storage is enabled to ensure
	// snapshots are saved before pod deletion.
	SnapshotFinalizer = "sandbox.cohere.com/snapshot"

	// FUSE sidecar constants
	FuseMountVolumeName  = "fuse-mounts"
	FuseDeviceVolumeName = "fuse-device"
	FuseMountPath        = "/mnt"
)

var (
	ErrSandboxNotFound      = errors.New("sandbox not found")
	ErrSandboxAlreadyExists = errors.New("sandbox already exists")
)

// ManagerConfig holds configuration for the sandbox manager.
type ManagerConfig struct {
	Namespace        string
	RuntimeClassName string
	NodeSelector     map[string]string
	Tolerations      []corev1.Toleration
	ImagePullSecrets []corev1.LocalObjectReference
	// Resources specifies the default resource requests and limits for sandbox containers.
	Resources corev1.ResourceRequirements
	// PodMode indicates whether to create regular pods (true) or Kata VMs (false).
	// When true, RuntimeClassName is not set and emptyDir volumes are added.
	PodMode bool
}

// Manager handles K8s pod CRUD operations for sandboxes.
type Manager struct {
	client kubernetes.Interface
	config ManagerConfig
	store  *Store
}

// NewManager creates a new sandbox manager.
func NewManager(client kubernetes.Interface, config ManagerConfig, store *Store) *Manager {
	if config.Namespace == "" {
		config.Namespace = "default"
	}
	return &Manager{
		client: client,
		config: config,
		store:  store,
	}
}

// FuseSidecarConfig holds configuration for the FUSE sidecar container.
type FuseSidecarConfig struct {
	Image           string
	Endpoint        string
	Region          string
	AssetsBucket    string
	AccessKeyID     string
	SecretAccessKey string
	// SecretAccessKeySecretName is the name of a Kubernetes Secret containing the secret access key.
	// When set, SecretAccessKey is ignored and the value is loaded from the secret.
	SecretAccessKeySecretName string
	// SecretAccessKeySecretKey is the key within the secret. Defaults to "secretAccessKey".
	SecretAccessKeySecretKey string
	// UID is the user ID for the sandbox user that owns the FUSE mounts.
	UID int
	// GID is the group ID for the sandbox user that owns the FUSE mounts.
	GID int
	// SidecarResources specifies resource requests/limits for the sidecar container.
	SidecarResources corev1.ResourceRequirements
}

// CreateSandboxRequest holds parameters for creating a sandbox.
type CreateSandboxRequest struct {
	SessionID string
	Image     string
	Command   []string
	Env       map[string]string
	Labels    map[string]string
	// DownloadURL is a presigned GET URL for downloading a snapshot to restore.
	// If set, an init container will download and extract the snapshot into /data.
	// Ignored when FuseConfig is set (FUSE provides persistent storage).
	DownloadURL string
	// InitImage is the container image for the init container.
	// Should contain wget/curl and tar. Defaults to busybox:1.36.
	InitImage string
	// EnableSnapshotFinalizer adds a finalizer to the pod that blocks deletion
	// until a snapshot is saved. This ensures data is preserved on pod eviction.
	// Ignored when FuseConfig is set (FUSE provides persistent storage).
	EnableSnapshotFinalizer bool
	// UserID is an optional user identifier. When set along with FuseConfig,
	// enables the /mydrive mount backed by S3 at assets_bucket/my-drive/{user_id}/.
	UserID string
	// Features is a map of optional feature flags passed through from the API.
	// Known features:
	//   - "support_bundles": "true" - enables /mnt/support_bundles/ S3 mount
	Features map[string]string
	// FuseConfig enables the FUSE sidecar for S3-backed storage.
	// When set, replaces emptyDir with FUSE mounts and disables snapshot system.
	FuseConfig *FuseSidecarConfig
}

// CreateSandbox creates a new sandbox pod.
func (m *Manager) CreateSandbox(ctx context.Context, req CreateSandboxRequest) (*SandboxInfo, error) {
	sessionID := strings.TrimSpace(req.SessionID)
	if sessionID == "" {
		return nil, errors.New("session_id is required")
	}

	// Check if already exists
	if _, ok := m.store.GetSandbox(sessionID); ok {
		return nil, ErrSandboxAlreadyExists
	}

	image := strings.TrimSpace(req.Image)
	if image == "" {
		return nil, errors.New("image is required")
	}

	command := req.Command
	if len(command) == 0 {
		return nil, errors.New("command is required")
	}

	labels := map[string]string{
		"sandbox.cohere.com/vm":         "true",
		"sandbox.cohere.com/session-id": sessionID,
	}
	for k, v := range req.Labels {
		labels[k] = v
	}

	annotations := map[string]string{
		lastActivityAnnotationKey: time.Now().UTC().Format(time.RFC3339),
	}

	podName := podNameForSession(sessionID)

	// Build container spec
	container := corev1.Container{
		Name:      "sandbox",
		Image:     image,
		Command:   command,
		Env:       mapToEnvVars(req.Env),
		Resources: m.config.Resources,
	}

	// Build pod spec
	terminationGracePeriod := int64(5) // Short grace period since preStop hook handles cleanup
	podSpec := corev1.PodSpec{
		NodeSelector:     copyMap(m.config.NodeSelector),
		Tolerations:      m.config.Tolerations,
		ImagePullSecrets: m.config.ImagePullSecrets,
		Containers:       []corev1.Container{container},
		RestartPolicy:    corev1.RestartPolicyNever,
		SecurityContext: &corev1.PodSecurityContext{
			SeccompProfile: &corev1.SeccompProfile{
				Type: "RuntimeDefault",
			},
		},
		TerminationGracePeriodSeconds: &terminationGracePeriod,
	}

	// Configure based on mode
	var finalizers []string
	if m.config.PodMode {
		if req.FuseConfig != nil {
			// FUSE mode: use S3-backed storage via mountpoint-s3 sidecar
			// This replaces emptyDir and the snapshot system
			hostPathCharDev := corev1.HostPathCharDev
			mountPropBidirectional := corev1.MountPropagationBidirectional
			mountPropHostToContainer := corev1.MountPropagationHostToContainer

			podSpec.Volumes = []corev1.Volume{
				{
					Name: FuseMountVolumeName,
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
				{
					Name: FuseDeviceVolumeName,
					VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{
							Path: "/dev/fuse",
							Type: &hostPathCharDev,
						},
					},
				},
			}

			// Build mounts for main container
			// Mount the shared volume at /mnt with HostToContainer propagation
			// The sidecar creates FUSE mounts at /mnt/data and /mnt/mydrive
			// which propagate to the main container
			mainMounts := []corev1.VolumeMount{
				{
					Name:             FuseMountVolumeName,
					MountPath:        FuseMountPath,
					MountPropagation: &mountPropHostToContainer,
				},
			}
			if userID := strings.TrimSpace(req.UserID); userID != "" {
				labels["sandbox.cohere.com/user-id"] = userID
			}
			podSpec.Containers[0].VolumeMounts = mainMounts

			// Add FUSE sidecar as a native Kubernetes sidecar (init container with restartPolicy: Always)
			// This ensures: 1) sidecar starts before main container, 2) terminates when main container exits
			sidecar := buildFuseSidecar(req.FuseConfig, sessionID, req.UserID, req.Features, &mountPropBidirectional)
			podSpec.InitContainers = append(podSpec.InitContainers, sidecar)

			// Add lifecycle hook to create minimal venv (non-blocking).
			// Uses --without-pip for instant creation (just symlinks), then installs a
			// pip wrapper that lazily creates full venv on first pip use.
			podSpec.Containers[0].Lifecycle = &corev1.Lifecycle{
				PostStart: &corev1.LifecycleHandler{
					Exec: &corev1.ExecAction{
						Command: []string{
							"/bin/sh", "-c",
							`python3 -m venv --system-site-packages --without-pip /mnt/data/.venv 2>/dev/null; cat > /mnt/data/.venv/bin/pip << 'WRAPPER'
#!/bin/sh
# Lazy pip initializer - creates full venv on first use
VENV=/mnt/data/.venv
MARKER="$VENV/.pip-ready"
if [ ! -f "$MARKER" ]; then
  python3 -m venv --system-site-packages "$VENV" 2>/dev/null
  touch "$MARKER"
fi
exec "$VENV/bin/pip" "$@"
WRAPPER
chmod +x /mnt/data/.venv/bin/pip`,
						},
					},
				},
			}
		} else {
			// Legacy mode: emptyDir volume with optional snapshot restore
			podSpec.Volumes = []corev1.Volume{
				{
					Name: SandboxDataVolumeName,
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
			}
			podSpec.Containers[0].VolumeMounts = []corev1.VolumeMount{
				{
					Name:      SandboxDataVolumeName,
					MountPath: SandboxDataMountPath,
				},
			}

			// Add init container if restoring from snapshot
			if downloadURL := strings.TrimSpace(req.DownloadURL); downloadURL != "" {
				initImage := strings.TrimSpace(req.InitImage)
				if initImage == "" {
					initImage = "busybox:1.36"
				}

				podSpec.InitContainers = []corev1.Container{
					{
						Name:  "restore-snapshot",
						Image: initImage,
						Command: []string{
							"/bin/sh",
							"-c",
							fmt.Sprintf("wget -q -O - '%s' | tar -xzf - --strip-components=1 -C %s", downloadURL, SandboxDataMountPath),
						},
						VolumeMounts: []corev1.VolumeMount{
							{
								Name:      SandboxDataVolumeName,
								MountPath: SandboxDataMountPath,
							},
						},
					},
				}
				annotations["sandbox.cohere.com/restored-from-snapshot"] = "true"
			}

			// Only enable snapshot finalizer in legacy mode
			if req.EnableSnapshotFinalizer {
				finalizers = []string{SnapshotFinalizer}
			}
		}
	} else {
		// Kata mode: set RuntimeClassName if configured
		podSpec.RuntimeClassName = runtimeClassName(m.config.RuntimeClassName)
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        podName,
			Namespace:   m.config.Namespace,
			Labels:      labels,
			Annotations: annotations,
			Finalizers:  finalizers,
		},
		Spec: podSpec,
	}

	created, err := m.client.CoreV1().Pods(m.config.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil, ErrSandboxAlreadyExists
		}
		return nil, fmt.Errorf("create pod: %w", err)
	}

	// Return initial info (status will be pending)
	return &SandboxInfo{
		SessionID:  sessionID,
		SandboxID:  "", // Not yet available
		PodUID:     string(created.UID),
		Containers: nil,
		Status:     SandboxStatusPending,
		Node:       created.Spec.NodeName,
		CreatedAt:  created.CreationTimestamp.Time,
		LastUsedAt: time.Now().UTC(),
		Labels:     labels,
		PodName:    created.Name,
	}, nil
}

// DeleteSandbox deletes a sandbox pod.
func (m *Manager) DeleteSandbox(ctx context.Context, sessionID string) error {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return errors.New("session_id is required")
	}

	podName, ok := m.store.PodNameForSession(sessionID)
	if !ok {
		return ErrSandboxNotFound
	}

	policy := metav1.DeletePropagationForeground
	err := m.client.CoreV1().Pods(m.config.Namespace).Delete(ctx, podName, metav1.DeleteOptions{
		PropagationPolicy: &policy,
	})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ErrSandboxNotFound
		}
		return fmt.Errorf("delete pod: %w", err)
	}
	return nil
}

// RemoveSnapshotFinalizer removes the snapshot finalizer from a pod, allowing it to be deleted.
// This should be called after the snapshot has been saved successfully.
func (m *Manager) RemoveSnapshotFinalizer(ctx context.Context, sessionID string) error {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return errors.New("session_id is required")
	}

	podName, ok := m.store.PodNameForSession(sessionID)
	if !ok {
		return ErrSandboxNotFound
	}

	// Get current pod to find existing finalizers
	pod, err := m.client.CoreV1().Pods(m.config.Namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ErrSandboxNotFound
		}
		return fmt.Errorf("get pod: %w", err)
	}

	// Filter out our finalizer
	var newFinalizers []string
	for _, f := range pod.Finalizers {
		if f != SnapshotFinalizer {
			newFinalizers = append(newFinalizers, f)
		}
	}

	// If finalizer wasn't present, nothing to do
	if len(newFinalizers) == len(pod.Finalizers) {
		return nil
	}

	// Patch to remove finalizer
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"finalizers": newFinalizers,
		},
	}
	data, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal patch: %w", err)
	}

	_, err = m.client.CoreV1().Pods(m.config.Namespace).Patch(
		ctx, podName, types.MergePatchType, data, metav1.PatchOptions{},
	)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ErrSandboxNotFound
		}
		return fmt.Errorf("patch pod: %w", err)
	}

	return nil
}

// UpdateSandboxActivity updates the last activity timestamp for a sandbox.
func (m *Manager) UpdateSandboxActivity(ctx context.Context, sessionID string) error {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return errors.New("session_id is required")
	}

	podName, ok := m.store.PodNameForSession(sessionID)
	if !ok {
		return ErrSandboxNotFound
	}

	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]string{
				lastActivityAnnotationKey: time.Now().UTC().Format(time.RFC3339),
			},
		},
	}
	data, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal patch: %w", err)
	}

	_, err = m.client.CoreV1().Pods(m.config.Namespace).Patch(
		ctx, podName, types.MergePatchType, data, metav1.PatchOptions{},
	)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ErrSandboxNotFound
		}
		return fmt.Errorf("patch pod: %w", err)
	}
	return nil
}

// UpdateSandboxIDAnnotation updates the sandbox ID annotation on a pod.
func (m *Manager) UpdateSandboxIDAnnotation(ctx context.Context, sessionID, sandboxID string) error {
	sessionID = strings.TrimSpace(sessionID)
	sandboxID = strings.TrimSpace(sandboxID)
	if sessionID == "" || sandboxID == "" {
		return nil
	}

	podName, ok := m.store.PodNameForSession(sessionID)
	if !ok {
		return nil // Silently ignore if not found
	}

	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]string{
				sandboxAnnotationKey:    sandboxID,
				sandboxAnnotationAltKey: sandboxID,
			},
		},
	}
	data, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal patch: %w", err)
	}

	_, err = m.client.CoreV1().Pods(m.config.Namespace).Patch(
		ctx, podName, types.MergePatchType, data, metav1.PatchOptions{},
	)
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("patch pod: %w", err)
	}
	return nil
}

func podNameForSession(sessionID string) string {
	base := sanitizeName(sessionID)
	if base == "" {
		base = "sandbox"
	}
	name := "sandbox-" + base
	if len(name) <= 63 {
		return name
	}
	suffix := hashSuffix(sessionID)
	trim := 63 - len("sandbox-") - 1 - len(suffix)
	if trim < 1 {
		trim = 1
	}
	return "sandbox-" + base[:trim] + "-" + suffix
}

func sanitizeName(value string) string {
	value = strings.ToLower(value)
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '.' || r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	return strings.Trim(b.String(), "-._")
}

func hashSuffix(value string) string {
	sum := sha1.Sum([]byte(value))
	return hex.EncodeToString(sum[:4])
}

func runtimeClassName(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func copyMap(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	result := make(map[string]string, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}

func mapToEnvVars(m map[string]string) []corev1.EnvVar {
	if len(m) == 0 {
		return nil
	}
	result := make([]corev1.EnvVar, 0, len(m))
	for k, v := range m {
		result = append(result, corev1.EnvVar{Name: k, Value: v})
	}
	return result
}

// buildFuseSidecar creates the FUSE sidecar container spec using fuse-adapter.
// fuse-adapter (https://github.com/aar10n/fuse-adapter) supports multiple FUSE backends
// via YAML config and provides POSIX permissions (via S3 metadata headers), which is
// required for pip install --user to work.
func buildFuseSidecar(cfg *FuseSidecarConfig, sessionID, userID string, features map[string]string, mountProp *corev1.MountPropagationMode) corev1.Container {
	userID = strings.TrimSpace(userID)
	enableSupportBundles := features["support_bundles"] == "true"

	// Build YAML config for fuse-adapter
	// Always mount /data -> assets_bucket/sandboxes/{session_id}/
	// Optionally mount /mydrive -> assets_bucket/my_drive/{user_id}/ (if userID provided)
	// Optionally mount /support_bundles -> assets_bucket/support-bundles/ (if feature enabled)
	//
	// Common S3 config is defined under connectors.s3, mounts reference it by type.
	// The cache layer is required for random write operations (S3 only supports full object writes).
	var yamlBuilder strings.Builder
	yamlBuilder.WriteString(fmt.Sprintf(`logging:
  level: info

connectors:
  s3:
    bucket: %s
    region: %s
    endpoint: "%s"
    force_path_style: true

mounts:
  - path: %s/data
    uid: %d
    gid: %d
    connector:
      type: s3
      prefix: "sandboxes/%s/"
    cache:
      type: filesystem
      path: /tmp/fuse-adapter-cache/data
      max_size: "512MB"
      flush_interval: 30s
      exclude_from_sync:
        # Exclude venv binaries and tools (platform-specific, recreatable)
        - ".venv/bin/*"
        - ".venv/include/*"
        # Exclude pip itself (comes with Python)
        - ".venv/lib/python*/site-packages/pip/*"
        - ".venv/lib/python*/site-packages/pip-*.dist-info/*"
        # Exclude setuptools (comes with Python)
        - ".venv/lib/python*/site-packages/setuptools/*"
        - ".venv/lib/python*/site-packages/setuptools-*.dist-info/*"
        # Exclude compiled Python files (recreatable)
        - "**/__pycache__/**"
        - "**/*.pyc"
`, cfg.AssetsBucket, cfg.Region, cfg.Endpoint,
		FuseMountPath, cfg.UID, cfg.GID, sessionID))

	// Add mydrive mount if userID is provided
	if userID != "" {
		yamlBuilder.WriteString(fmt.Sprintf(`  - path: %s/mydrive
    uid: %d
    gid: %d
    connector:
      type: s3
      prefix: "my_drive/%s/"
    read_only: true
`, FuseMountPath, cfg.UID, cfg.GID, userID))
	}

	// Add support_bundles mount if feature is enabled
	if enableSupportBundles {
		yamlBuilder.WriteString(fmt.Sprintf(`  - path: %s/support_bundles
    uid: %d
    gid: %d
    connector:
      type: s3
      prefix: "support-bundles/"
    read_only: true
`, FuseMountPath, cfg.UID, cfg.GID))
	}

	yamlConfig := yamlBuilder.String()

	// Yaml doesn't allow tabs
	yamlConfig = strings.ReplaceAll(yamlConfig, "\t", "  ")

	// Script to write config and run fuse-adapter
	// The YAML config is passed via FUSE_ADAPTER_CONFIG env var for easy debugging
	// (visible in kubectl describe pod). Using printf '%s' safely handles all characters.
	mkdirPaths := []string{FuseMountPath + "/data"}
	if userID != "" {
		mkdirPaths = append(mkdirPaths, FuseMountPath+"/mydrive")
	}
	if enableSupportBundles {
		mkdirPaths = append(mkdirPaths, FuseMountPath+"/support_bundles")
	}
	mkdirPath := strings.Join(mkdirPaths, " ")

	script := fmt.Sprintf(`#!/bin/sh
set -e

mkdir -p %s
chown %d:%d %s
printf '%%s' "$FUSE_ADAPTER_CONFIG" > /tmp/fuse-adapter.yaml

exec /usr/local/bin/fuse-adapter /tmp/fuse-adapter.yaml
`, mkdirPath, cfg.UID, cfg.GID, mkdirPath)

	restartAlways := corev1.ContainerRestartPolicyAlways

	// Build env vars - use secretKeyRef if secret name is provided
	// FUSE_ADAPTER_CONFIG contains the YAML config, making it visible in kubectl describe pod
	envVars := []corev1.EnvVar{
		{Name: "AWS_ACCESS_KEY_ID", Value: cfg.AccessKeyID},
		{Name: "FUSE_ADAPTER_CONFIG", Value: yamlConfig},
	}
	if cfg.SecretAccessKeySecretName != "" {
		secretKey := cfg.SecretAccessKeySecretKey
		if secretKey == "" {
			secretKey = "secretAccessKey"
		}
		envVars = append(envVars, corev1.EnvVar{
			Name: "AWS_SECRET_ACCESS_KEY",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: cfg.SecretAccessKeySecretName,
					},
					Key: secretKey,
				},
			},
		})
	} else {
		envVars = append(envVars, corev1.EnvVar{
			Name: "AWS_SECRET_ACCESS_KEY", Value: cfg.SecretAccessKey,
		})
	}

	return corev1.Container{
		Name:          "fuse-sidecar",
		Image:         cfg.Image,
		Command:       []string{"/bin/sh", "-c", script},
		RestartPolicy: &restartAlways, // Native sidecar: runs alongside main container, terminates when pod exits
		Env:           envVars,
		Resources:     cfg.SidecarResources,
		SecurityContext: &corev1.SecurityContext{
			Privileged: boolPtr(true), // Required for bidirectional mount propagation
		},
		Lifecycle: &corev1.Lifecycle{
			PreStop: &corev1.LifecycleHandler{
				Exec: &corev1.ExecAction{
					// Send SIGINT to fuse-adapter for graceful shutdown (unmounts all filesystems)
					Command: []string{"/bin/sh", "-c", "pkill -INT fuse-adapter; sleep 2; pkill -KILL fuse-adapter 2>/dev/null; exit 0"},
				},
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      FuseDeviceVolumeName,
				MountPath: "/dev/fuse",
			},
			{
				Name:             FuseMountVolumeName,
				MountPath:        FuseMountPath,
				MountPropagation: mountProp,
			},
		},
	}
}

func boolPtr(b bool) *bool {
	return &b
}
