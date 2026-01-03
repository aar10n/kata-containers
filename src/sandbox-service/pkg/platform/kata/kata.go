package kata

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/cohere-ai/kata-containers/src/sandbox-service/pkg/platform"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	labelSandboxVM      = "sandbox.cohere.com/vm"
	labelSandboxSession = "sandbox.cohere.com/session-id"
	sandboxAnnotationID = "io.kubernetes.cri.sandbox-id"
	lastActivityKey     = "sandbox.cohere.com/last-activity"
)

type Platform struct {
	client           kubernetes.Interface
	namespace        string
	runtimeClass     string
	nodeSelector     map[string]string
	defaultImage     string
	defaultCommand   []string
	sandboxAgentAddr string
	httpClient       *http.Client
}

type ExecRequest struct {
	VMID        string   `json:"vm_id"`
	ContainerID string   `json:"container_id"`
	Args        []string `json:"args"`
	Env         []string `json:"env,omitempty"`
	Cwd         string   `json:"cwd,omitempty"`
	TimeoutMs   int64    `json:"timeout_ms,omitempty"`
}

type ExecResponse struct {
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	ExitCode int    `json:"exit_code"`
}

type ResolveRequest struct {
	ContainerID string `json:"container_id"`
	Node        string `json:"node,omitempty"`
}

type ResolveResponse struct {
	SandboxID string `json:"sandbox_id"`
}

func New(namespace, runtimeClass string, nodeSelector map[string]string, defaultImage string, defaultCommand []string, sandboxAgentAddr string) (*Platform, error) {
	client, err := newClient()
	if err != nil {
		return nil, err
	}
	return &Platform{
		client:           client,
		namespace:        namespace,
		runtimeClass:     runtimeClass,
		nodeSelector:     copyMap(nodeSelector),
		defaultImage:     defaultImage,
		defaultCommand:   append([]string{}, defaultCommand...),
		sandboxAgentAddr: strings.TrimRight(sandboxAgentAddr, "/"),
		httpClient:       &http.Client{Timeout: 30 * time.Second},
	}, nil
}

func (p *Platform) CreateSandbox(ctx context.Context, req platform.CreateSandboxRequest) (*platform.Sandbox, error) {
	pod, err := p.findPod(ctx, req.SessionID)
	if err == nil && pod != nil {
		return nil, platform.ErrAlreadyExists
	}
	if err != nil && !errors.Is(err, platform.ErrNotFound) {
		return nil, err
	}

	image := req.Image
	if image == "" {
		image = p.defaultImage
	}
	command := req.Command
	if len(command) == 0 {
		command = append([]string{}, p.defaultCommand...)
	}

	labels := map[string]string{
		labelSandboxVM:      "true",
		labelSandboxSession: req.SessionID,
	}
	for key, value := range req.Labels {
		labels[key] = value
	}

	podName := podNameForSession(req.SessionID)
	podSpec := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: p.namespace,
			Labels:    labels,
			Annotations: map[string]string{
				lastActivityKey: time.Now().UTC().Format(time.RFC3339),
			},
		},
		Spec: corev1.PodSpec{
			RuntimeClassName: runtimeClassName(p.runtimeClass),
			NodeSelector:     copyMap(p.nodeSelector),
			Containers: []corev1.Container{
				{
					Name:    "sandbox",
					Image:   image,
					Command: command,
					Env:     mapToEnvVars(req.Env),
				},
			},
			RestartPolicy: corev1.RestartPolicyNever,
		},
	}

	created, err := p.client.CoreV1().Pods(p.namespace).Create(ctx, podSpec, metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil, platform.ErrAlreadyExists
		}
		return nil, fmt.Errorf("create pod: %w", err)
	}

	return sandboxFromPod(created), nil
}

func (p *Platform) GetSandbox(ctx context.Context, sessionID string) (*platform.Sandbox, error) {
	pod, err := p.findPod(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	return sandboxFromPod(pod), nil
}

func (p *Platform) DeleteSandbox(ctx context.Context, sessionID string) error {
	pod, err := p.findPod(ctx, sessionID)
	if err != nil {
		if errors.Is(err, platform.ErrNotFound) {
			return platform.ErrNotFound
		}
		return err
	}

	policy := metav1.DeletePropagationForeground
	if err := p.client.CoreV1().Pods(p.namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{PropagationPolicy: &policy}); err != nil {
		if apierrors.IsNotFound(err) {
			return platform.ErrNotFound
		}
		return fmt.Errorf("delete pod: %w", err)
	}
	return nil
}

func (p *Platform) ListSandboxes(ctx context.Context) ([]*platform.Sandbox, error) {
	selector := labels.Set{labelSandboxVM: "true"}.AsSelector()
	list, err := p.client.CoreV1().Pods(p.namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}

	result := make([]*platform.Sandbox, 0, len(list.Items))
	for i := range list.Items {
		pod := &list.Items[i]
		sessionID := pod.Labels[labelSandboxSession]
		if sessionID == "" {
			continue
		}
		result = append(result, sandboxFromPod(pod))
	}
	return result, nil
}

func (p *Platform) Exec(ctx context.Context, req platform.ExecRequest) (*platform.ExecResult, error) {
	pod, err := p.findPod(ctx, req.SessionID)
	if err != nil {
		return nil, err
	}
	info := sandboxFromPod(pod)
	if info.Status != platform.StatusRunning {
		return nil, platform.ErrNotReady
	}
	if info.SandboxID == "" && info.ContainerID != "" {
		sandboxID, err := p.resolveSandboxID(ctx, pod.Spec.NodeName, info.ContainerID)
		if err != nil {
			return nil, platform.ErrNotReady
		}
		info.SandboxID = sandboxID
	}
	if info.SandboxID == "" {
		return nil, platform.ErrNotReady
	}

	payload := ExecRequest{
		VMID:        info.SandboxID,
		ContainerID: info.ContainerID,
		Args:        req.Command,
		Env:         mapToEnv(req.Env),
		Cwd:         req.WorkingDir,
	}
	if req.Timeout > 0 {
		payload.TimeoutMs = req.Timeout.Milliseconds()
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal exec payload: %w", err)
	}

	url := p.sandboxAgentAddr + "/v1/exec"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create exec request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")

	resp, err := p.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("exec request: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read exec response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("sandbox-agent exec error: %s", strings.TrimSpace(string(data)))
	}

	var execResp ExecResponse
	if err := json.Unmarshal(data, &execResp); err != nil {
		return nil, fmt.Errorf("decode exec response: %w", err)
	}
	if err := p.updateLastActivity(ctx, pod.Name); err != nil {
		log.Printf("update last activity failed for session %s: %v", req.SessionID, err)
	}

	return &platform.ExecResult{
		ExitCode: execResp.ExitCode,
		Stdout:   execResp.Stdout,
		Stderr:   execResp.Stderr,
	}, nil
}

func (p *Platform) resolveSandboxID(ctx context.Context, nodeName, containerID string) (string, error) {
	if p.sandboxAgentAddr == "" || containerID == "" {
		return "", errors.New("sandbox agent address or container id missing")
	}

	payload := ResolveRequest{
		ContainerID: containerID,
		Node:        nodeName,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal resolve payload: %w", err)
	}

	url := p.sandboxAgentAddr + "/v1/resolve"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("create resolve request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")

	resp, err := p.httpClient.Do(request)
	if err != nil {
		return "", fmt.Errorf("resolve request: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read resolve response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("sandbox-agent resolve error: %s", strings.TrimSpace(string(data)))
	}

	var resolveResp ResolveResponse
	if err := json.Unmarshal(data, &resolveResp); err != nil {
		return "", fmt.Errorf("decode resolve response: %w", err)
	}
	if resolveResp.SandboxID == "" {
		return "", errors.New("sandbox id not found")
	}
	return resolveResp.SandboxID, nil
}

func (p *Platform) findPod(ctx context.Context, sessionID string) (*corev1.Pod, error) {
	selector := labels.Set{labelSandboxSession: sessionID}.AsSelector()
	list, err := p.client.CoreV1().Pods(p.namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}
	if len(list.Items) == 0 {
		return nil, platform.ErrNotFound
	}

	pod := &list.Items[0]
	for i := range list.Items {
		candidate := &list.Items[i]
		if pod.CreationTimestamp.After(candidate.CreationTimestamp.Time) {
			pod = candidate
		}
	}
	return pod, nil
}

func sandboxFromPod(pod *corev1.Pod) *platform.Sandbox {
	status := podStatus(pod)
	return &platform.Sandbox{
		SessionID:   pod.Labels[labelSandboxSession],
		SandboxID:   sandboxIDFromPod(pod),
		ContainerID: containerIDFromPod(pod),
		Status:      status,
		Host:        pod.Spec.NodeName,
		CreatedAt:   pod.CreationTimestamp.Time,
		LastUsedAt:  lastActivityFromPod(pod),
		Labels:      pod.Labels,
	}
}

func podStatus(pod *corev1.Pod) platform.SandboxStatus {
	if pod.DeletionTimestamp != nil {
		return platform.StatusTerminated
	}
	switch pod.Status.Phase {
	case corev1.PodPending:
		return platform.StatusPending
	case corev1.PodRunning:
		if isPodReady(pod) {
			return platform.StatusRunning
		}
		return platform.StatusPending
	case corev1.PodSucceeded:
		return platform.StatusTerminated
	case corev1.PodFailed:
		return platform.StatusFailed
	default:
		return platform.StatusPending
	}
}

func isPodReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

func sandboxIDFromPod(pod *corev1.Pod) string {
	if pod.Annotations != nil {
		if id := normalizeID(pod.Annotations[sandboxAnnotationID]); id != "" {
			return id
		}
	}
	return ""
}

func lastActivityFromPod(pod *corev1.Pod) time.Time {
	if pod.Annotations != nil {
		if value := strings.TrimSpace(pod.Annotations[lastActivityKey]); value != "" {
			if parsed, err := time.Parse(time.RFC3339, value); err == nil {
				return parsed
			}
		}
	}
	return pod.CreationTimestamp.Time
}

func containerIDFromPod(pod *corev1.Pod) string {
	for _, status := range pod.Status.ContainerStatuses {
		if status.ContainerID != "" {
			return normalizeID(status.ContainerID)
		}
	}
	return ""
}

func mapToEnvVars(values map[string]string) []corev1.EnvVar {
	if len(values) == 0 {
		return nil
	}
	result := make([]corev1.EnvVar, 0, len(values))
	for key, value := range values {
		result = append(result, corev1.EnvVar{Name: key, Value: value})
	}
	return result
}

func mapToEnv(values map[string]string) []string {
	if len(values) == 0 {
		return nil
	}
	result := make([]string, 0, len(values))
	for key, value := range values {
		result = append(result, fmt.Sprintf("%s=%s", key, value))
	}
	return result
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

func copyMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	copy := make(map[string]string, len(values))
	for key, value := range values {
		copy[key] = value
	}
	return copy
}

func normalizeID(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if strings.Contains(value, "://") {
		parts := strings.SplitN(value, "://", 2)
		return strings.TrimSpace(parts[1])
	}
	return value
}

func (p *Platform) updateLastActivity(ctx context.Context, podName string) error {
	patch := map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]string{
				lastActivityKey: time.Now().UTC().Format(time.RFC3339),
			},
		},
	}
	data, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal last activity patch: %w", err)
	}
	_, err = p.client.CoreV1().Pods(p.namespace).Patch(ctx, podName, types.MergePatchType, data, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("patch last activity: %w", err)
	}
	return nil
}

func newClient() (*kubernetes.Clientset, error) {
	config, err := buildConfig()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(config)
}

func buildConfig() (*rest.Config, error) {
	if kubeconfig := strings.TrimSpace(os.Getenv("KUBECONFIG")); kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("in-cluster config not available: %w", err)
	}
	return config, nil
}
