package k8s

import (
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
)

const sandboxAnnotationKey = "io.kubernetes.cri.sandbox-id"
const sandboxAnnotationAltKey = "sandbox.cohere.com/sandbox-id"
const sandboxServiceLabelKey = "app.kubernetes.io/name"
const sandboxServiceLabelValue = "sandbox-agent"
const kataPodLabelSelector = "sandbox.cohere.com/vm=true"
const sessionIDLabelKey = "sandbox.cohere.com/session-id"
const lastActivityAnnotationKey = "sandbox.cohere.com/last-activity"

// SandboxStatus represents the lifecycle state of a sandbox.
type SandboxStatus string

const (
	SandboxStatusPending    SandboxStatus = "pending"
	SandboxStatusRunning    SandboxStatus = "running"
	SandboxStatusFailed     SandboxStatus = "failed"
	SandboxStatusTerminated SandboxStatus = "terminated"
)

// ContainerInfo holds information about a container in a sandbox.
type ContainerInfo struct {
	Name        string
	ContainerID string
}

// SandboxInfo holds complete information about a sandbox.
type SandboxInfo struct {
	SessionID   string
	SandboxID   string
	Containers  []ContainerInfo
	Status      SandboxStatus
	Node        string
	CreatedAt   time.Time
	LastUsedAt  time.Time
	Labels      map[string]string
	PodName     string // internal use for updates
}

// SandboxIDCallback is called when a sandboxId is discovered locally
// (from containerd) and needs to be persisted to the pod annotation.
type SandboxIDCallback func(sessionID, sandboxID string)

type Store struct {
	mu                 sync.RWMutex
	sandboxes          map[string]*SandboxInfo // session_id -> sandbox info
	vmToSession        map[string]string       // sandbox_id (vm_id) -> session_id
	nodeToAddr         map[string]string
	nodeToSandboxPodIP map[string]string
	onSandboxIDFound   SandboxIDCallback
}

func NewStore() *Store {
	return &Store{
		sandboxes:          make(map[string]*SandboxInfo),
		vmToSession:        make(map[string]string),
		nodeToAddr:         make(map[string]string),
		nodeToSandboxPodIP: make(map[string]string),
	}
}

// SetSandboxIDCallback sets a callback that is invoked when a sandboxId
// is discovered from local containerd and needs to be persisted.
func (s *Store) SetSandboxIDCallback(cb SandboxIDCallback) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onSandboxIDFound = cb
}

// UpdateSandboxID updates the sandbox ID for a session and populates the vmToSession mapping.
// This should be called when a sandbox ID is discovered from containerd to avoid waiting
// for the K8s annotation update to propagate.
func (s *Store) UpdateSandboxID(sessionID, sandboxID string) {
	sandboxID = normalizeID(sandboxID)
	if sessionID == "" || sandboxID == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	info, ok := s.sandboxes[sessionID]
	if !ok {
		return
	}

	// Update sandbox ID in info
	if info.SandboxID == "" {
		info.SandboxID = sandboxID
	}

	// Update vmToSession mapping
	s.vmToSession[sandboxID] = sessionID
}

func (s *Store) SetPod(pod *corev1.Pod) {
	if pod == nil {
		return
	}

	nodeName := pod.Spec.NodeName

	// Track sandbox-agent pods for routing
	if isSandboxServicePod(pod) && pod.Status.PodIP != "" && nodeName != "" {
		s.mu.Lock()
		s.nodeToSandboxPodIP[nodeName] = pod.Status.PodIP
		s.mu.Unlock()
	}

	if !isKataPod(pod) {
		return
	}

	sessionID := extractSessionID(pod)
	if sessionID == "" {
		return
	}

	info := s.extractSandboxInfo(pod)
	if info == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.sandboxes[sessionID] = info

	// Track sandbox_id -> session_id mapping for VM operations
	if info.SandboxID != "" {
		s.vmToSession[info.SandboxID] = sessionID
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

	sessionID := extractSessionID(pod)
	if sessionID == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if info, ok := s.sandboxes[sessionID]; ok {
		if info.SandboxID != "" {
			delete(s.vmToSession, info.SandboxID)
		}
		delete(s.sandboxes, sessionID)
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

func (s *Store) NodeForVM(vmID string) (string, bool) {
	vmID = normalizeID(vmID)
	if vmID == "" {
		return "", false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	sessionID, ok := s.vmToSession[vmID]
	if !ok {
		return "", false
	}
	info, ok := s.sandboxes[sessionID]
	if !ok {
		return "", false
	}
	if info.Node == "" {
		return "", false
	}
	if _, agentOK := s.nodeToSandboxPodIP[info.Node]; !agentOK {
		return "", false
	}
	return info.Node, true
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

// GetSandbox returns sandbox info by session ID.
func (s *Store) GetSandbox(sessionID string) (*SandboxInfo, bool) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil, false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	info, ok := s.sandboxes[sessionID]
	if !ok {
		return nil, false
	}
	// Only return if the node has an agent
	if _, agentOK := s.nodeToSandboxPodIP[info.Node]; !agentOK && info.Node != "" {
		return nil, false
	}
	return info, true
}

// GetSandboxByVMID returns sandbox info by VM/sandbox ID.
func (s *Store) GetSandboxByVMID(vmID string) (*SandboxInfo, bool) {
	vmID = normalizeID(vmID)
	if vmID == "" {
		return nil, false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	sessionID, ok := s.vmToSession[vmID]
	if !ok {
		return nil, false
	}
	info, ok := s.sandboxes[sessionID]
	if !ok {
		return nil, false
	}
	return info, true
}

// ListSandboxes returns all sandboxes, optionally filtered by node.
func (s *Store) ListSandboxes(nodeName string) []*SandboxInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	nodeName = strings.TrimSpace(nodeName)
	result := make([]*SandboxInfo, 0, len(s.sandboxes))
	for _, info := range s.sandboxes {
		if nodeName != "" && info.Node != nodeName {
			continue
		}
		// Only include if the node has an agent (or node not yet assigned)
		if info.Node != "" {
			if _, agentOK := s.nodeToSandboxPodIP[info.Node]; !agentOK {
				continue
			}
		}
		result = append(result, info)
	}
	return result
}

// PodNameForSession returns the pod name for a session, used for updates.
func (s *Store) PodNameForSession(sessionID string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	info, ok := s.sandboxes[sessionID]
	if !ok {
		return "", false
	}
	return info.PodName, true
}

func extractPodIDs(pod *corev1.Pod) []string {
	var ids []string

	if pod.Annotations != nil {
		if sandboxID := normalizeID(pod.Annotations[sandboxAnnotationAltKey]); sandboxID != "" {
			ids = append(ids, sandboxID)
		}
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

func extractSessionID(pod *corev1.Pod) string {
	if pod == nil || pod.Labels == nil {
		return ""
	}
	return strings.TrimSpace(pod.Labels[sessionIDLabelKey])
}

func (s *Store) extractSandboxInfo(pod *corev1.Pod) *SandboxInfo {
	if pod == nil {
		return nil
	}

	sessionID := extractSessionID(pod)
	if sessionID == "" {
		return nil
	}

	// Extract sandbox ID from annotations or containerd
	sandboxID := ""
	sandboxIDFromAnnotation := false
	if pod.Annotations != nil {
		sandboxID = normalizeID(pod.Annotations[sandboxAnnotationAltKey])
		if sandboxID == "" {
			sandboxID = normalizeID(pod.Annotations[sandboxAnnotationKey])
		}
		if sandboxID != "" {
			sandboxIDFromAnnotation = true
		}
	}
	if sandboxID == "" {
		sandboxID = resolveSandboxIDFromPod(pod)
		// If we found sandboxId from local containerd and it's not in annotations,
		// trigger callback to persist it
		if sandboxID != "" && !sandboxIDFromAnnotation {
			s.mu.RLock()
			cb := s.onSandboxIDFound
			s.mu.RUnlock()
			if cb != nil {
				go cb(sessionID, sandboxID)
			}
		}
	}

	// Extract container info
	containers := make([]ContainerInfo, 0, len(pod.Status.ContainerStatuses))
	for _, cs := range pod.Status.ContainerStatuses {
		containers = append(containers, ContainerInfo{
			Name:        cs.Name,
			ContainerID: normalizeID(cs.ContainerID),
		})
	}

	// Determine status
	status := podToSandboxStatus(pod)

	// Extract timestamps
	createdAt := pod.CreationTimestamp.Time
	lastUsedAt := createdAt
	if pod.Annotations != nil {
		if ts := strings.TrimSpace(pod.Annotations[lastActivityAnnotationKey]); ts != "" {
			if parsed, err := time.Parse(time.RFC3339, ts); err == nil {
				lastUsedAt = parsed
			}
		}
	}

	// Copy labels (exclude internal ones)
	labels := make(map[string]string)
	for k, v := range pod.Labels {
		labels[k] = v
	}

	return &SandboxInfo{
		SessionID:  sessionID,
		SandboxID:  sandboxID,
		Containers: containers,
		Status:     status,
		Node:       pod.Spec.NodeName,
		CreatedAt:  createdAt,
		LastUsedAt: lastUsedAt,
		Labels:     labels,
		PodName:    pod.Name,
	}
}

func podToSandboxStatus(pod *corev1.Pod) SandboxStatus {
	if pod.DeletionTimestamp != nil {
		return SandboxStatusTerminated
	}
	switch pod.Status.Phase {
	case corev1.PodPending:
		return SandboxStatusPending
	case corev1.PodRunning:
		// Check if actually ready
		for _, cond := range pod.Status.Conditions {
			if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
				return SandboxStatusRunning
			}
		}
		return SandboxStatusPending
	case corev1.PodSucceeded:
		return SandboxStatusTerminated
	case corev1.PodFailed:
		return SandboxStatusFailed
	default:
		return SandboxStatusPending
	}
}
