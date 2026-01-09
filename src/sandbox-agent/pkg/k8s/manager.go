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

// CreateSandboxRequest holds parameters for creating a sandbox.
type CreateSandboxRequest struct {
	SessionID string
	Image     string
	Command   []string
	Env       map[string]string
	Labels    map[string]string
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
		Name:    "sandbox",
		Image:   image,
		Command: command,
		Env:     mapToEnvVars(req.Env),
	}

	// Build pod spec
	podSpec := corev1.PodSpec{
		NodeSelector:  copyMap(m.config.NodeSelector),
		Containers:    []corev1.Container{container},
		RestartPolicy: corev1.RestartPolicyNever,
	}

	// Configure based on mode
	if m.config.PodMode {
		// Pod mode: add emptyDir volume for data, no RuntimeClassName
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
