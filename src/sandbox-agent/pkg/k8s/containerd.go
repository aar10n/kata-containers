package k8s

import (
	"encoding/json"
	"os"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"
)

const containerdTaskConfigDir = "/run/containerd/io.containerd.runtime.v2.task/k8s.io"

type containerdTaskConfig struct {
	Annotations map[string]string `json:"annotations"`
}

func resolveSandboxIDFromPod(pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}

	for _, status := range pod.Status.ContainerStatuses {
		if sandboxID := resolveSandboxIDFromContainerID(status.ContainerID); sandboxID != "" {
			return sandboxID
		}
	}
	for _, status := range pod.Status.InitContainerStatuses {
		if sandboxID := resolveSandboxIDFromContainerID(status.ContainerID); sandboxID != "" {
			return sandboxID
		}
	}
	for _, status := range pod.Status.EphemeralContainerStatuses {
		if sandboxID := resolveSandboxIDFromContainerID(status.ContainerID); sandboxID != "" {
			return sandboxID
		}
	}

	return ""
}

func resolveSandboxIDFromContainerID(containerID string) string {
	id := normalizeID(containerID)
	if id == "" {
		return ""
	}

	configPath := filepath.Join(containerdTaskConfigDir, id, "config.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return ""
	}

	var cfg containerdTaskConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return ""
	}

	if cfg.Annotations == nil {
		return ""
	}

	return normalizeID(cfg.Annotations[sandboxAnnotationKey])
}
