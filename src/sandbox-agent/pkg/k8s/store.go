package k8s

import (
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
)

const sandboxAnnotationKey = "io.kubernetes.cri.sandbox-id"
const sandboxServiceLabelKey = "app.kubernetes.io/name"
const sandboxServiceLabelValue = "sandbox-agent"
const kataPodLabelSelector = "sandbox.cohere.com/vm=true"

type Store struct {
	mu                 sync.RWMutex
	vmToNode           map[string]string
	nodeToAddr         map[string]string
	nodeToSandboxPodIP map[string]string
}

func NewStore() *Store {
	return &Store{
		vmToNode:           make(map[string]string),
		nodeToAddr:         make(map[string]string),
		nodeToSandboxPodIP: make(map[string]string),
	}
}

func (s *Store) SetPod(pod *corev1.Pod) {
	if pod == nil {
		return
	}

	nodeName := pod.Spec.NodeName
	if nodeName == "" {
		return
	}

	if isSandboxServicePod(pod) && pod.Status.PodIP != "" {
		s.mu.Lock()
		s.nodeToSandboxPodIP[nodeName] = pod.Status.PodIP
		s.mu.Unlock()
	}

	if !isKataPod(pod) {
		return
	}

	ids := extractPodIDs(pod)
	if len(ids) == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		s.vmToNode[id] = nodeName
	}
}

func (s *Store) DeletePod(pod *corev1.Pod) {
	if pod == nil {
		return
	}

	if isSandboxServicePod(pod) {
		s.mu.Lock()
		delete(s.nodeToSandboxPodIP, pod.Spec.NodeName)
		s.mu.Unlock()
	}

	if !isKataPod(pod) {
		return
	}

	ids := extractPodIDs(pod)
	if len(ids) == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		delete(s.vmToNode, id)
	}
}

func (s *Store) SetNode(node *corev1.Node) {
	if node == nil {
		return
	}

	addr := nodeAddress(node)
	if addr == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodeToAddr[node.Name] = addr
}

func (s *Store) DeleteNode(node *corev1.Node) {
	if node == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.nodeToAddr, node.Name)
}

func (s *Store) NodeForVM(id string) (string, bool) {
	id = normalizeID(id)
	if id == "" {
		return "", false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	node, ok := s.vmToNode[id]
	if !ok {
		return "", false
	}
	if _, agentOK := s.nodeToSandboxPodIP[node]; !agentOK {
		return "", false
	}
	return node, true
}

func (s *Store) AddressForNode(nodeName string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	addr, ok := s.nodeToAddr[nodeName]
	return addr, ok
}

func (s *Store) SandboxAgentAddressForNode(nodeName string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	addr, ok := s.nodeToSandboxPodIP[nodeName]
	return addr, ok
}

func (s *Store) ListVMs(nodeName string) map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := make(map[string]string)
	for id, node := range s.vmToNode {
		if nodeName != "" && node != nodeName {
			continue
		}
		if _, agentOK := s.nodeToSandboxPodIP[node]; !agentOK {
			continue
		}
		entries[id] = node
	}

	return entries
}

func extractPodIDs(pod *corev1.Pod) []string {
	var ids []string

	if pod.Annotations != nil {
		if sandboxID := normalizeID(pod.Annotations[sandboxAnnotationKey]); sandboxID != "" {
			ids = append(ids, sandboxID)
		}
	}

	if len(ids) == 0 {
		if sandboxID := resolveSandboxIDFromPod(pod); sandboxID != "" {
			ids = append(ids, sandboxID)
		}
	}

	return ids
}

func normalizeID(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return ""
	}
	if strings.Contains(id, "://") {
		parts := strings.SplitN(id, "://", 2)
		return strings.TrimSpace(parts[1])
	}
	return id
}

func nodeAddress(node *corev1.Node) string {
	var hostname string

	for _, addr := range node.Status.Addresses {
		switch addr.Type {
		case corev1.NodeInternalIP:
			return addr.Address
		case corev1.NodeExternalIP:
			hostname = addr.Address
		case corev1.NodeHostName:
			if hostname == "" {
				hostname = addr.Address
			}
		}
	}

	return hostname
}

func isSandboxServicePod(pod *corev1.Pod) bool {
	if pod == nil || pod.Labels == nil {
		return false
	}
	return pod.Labels[sandboxServiceLabelKey] == sandboxServiceLabelValue
}

func isKataPod(pod *corev1.Pod) bool {
	if pod == nil || pod.Labels == nil {
		return false
	}
	selector := strings.SplitN(kataPodLabelSelector, "=", 2)
	if len(selector) != 2 {
		return false
	}
	return pod.Labels[selector[0]] == selector[1]
}
