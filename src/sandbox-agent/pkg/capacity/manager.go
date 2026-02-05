package capacity

import (
	"context"
	"log/slog"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	// DefaultRefreshInterval is the default interval for refreshing capacity.
	DefaultRefreshInterval = 30 * time.Second
	// OverheadFactor is the factor applied to account for system overhead.
	OverheadFactor = 0.9
)

// Config holds capacity manager configuration.
type Config struct {
	// NodeName is the name of the node this agent is running on.
	NodeName string
	// RefreshInterval is how often to recalculate capacity.
	RefreshInterval time.Duration
	// SandboxCPU is the CPU request per sandbox container (e.g., "100m").
	SandboxCPU string
	// SandboxMemory is the memory request per sandbox container (e.g., "256Mi").
	SandboxMemory string
	// SidecarCPU is the CPU request for the FUSE sidecar (e.g., "200m").
	// Only used when sidecar is enabled.
	SidecarCPU string
	// SidecarMemory is the memory request for the FUSE sidecar (e.g., "128Mi").
	// Only used when sidecar is enabled.
	SidecarMemory string
}

// NodeCapacity holds computed capacity information for a single node.
type NodeCapacity struct {
	NodeName         string
	MaxSandboxes     int32
	CurrentSandboxes int32
	CalculatedAt     time.Time
}

// SandboxCounter is an interface for counting current sandboxes.
type SandboxCounter interface {
	CountSandboxesOnNode(nodeName string) int32
}

// Manager computes and caches local node capacity for sandboxes.
type Manager struct {
	cfg            Config
	client         kubernetes.Interface
	sandboxCounter SandboxCounter
	sandboxCPU     resource.Quantity
	sandboxMemory  resource.Quantity
	sidecarCPU     resource.Quantity
	sidecarMemory  resource.Quantity

	mu       sync.RWMutex
	capacity *NodeCapacity

	stopCh   chan struct{}
	stopOnce sync.Once
}

// New creates a new capacity manager.
func New(cfg Config, client kubernetes.Interface, counter SandboxCounter) (*Manager, error) {
	if cfg.RefreshInterval <= 0 {
		cfg.RefreshInterval = DefaultRefreshInterval
	}

	m := &Manager{
		cfg:            cfg,
		client:         client,
		sandboxCounter: counter,
		stopCh:         make(chan struct{}),
	}

	// Parse resource quantities if specified
	if cfg.SandboxCPU != "" {
		qty, err := resource.ParseQuantity(cfg.SandboxCPU)
		if err != nil {
			slog.Warn("invalid sandbox CPU quantity, using default", "value", cfg.SandboxCPU, "error", err)
		} else {
			m.sandboxCPU = qty
		}
	}
	if cfg.SandboxMemory != "" {
		qty, err := resource.ParseQuantity(cfg.SandboxMemory)
		if err != nil {
			slog.Warn("invalid sandbox memory quantity, using default", "value", cfg.SandboxMemory, "error", err)
		} else {
			m.sandboxMemory = qty
		}
	}
	// Parse sidecar resources (used when FUSE storage is enabled)
	if cfg.SidecarCPU != "" {
		qty, err := resource.ParseQuantity(cfg.SidecarCPU)
		if err != nil {
			slog.Warn("invalid sidecar CPU quantity", "value", cfg.SidecarCPU, "error", err)
		} else {
			m.sidecarCPU = qty
		}
	}
	if cfg.SidecarMemory != "" {
		qty, err := resource.ParseQuantity(cfg.SidecarMemory)
		if err != nil {
			slog.Warn("invalid sidecar memory quantity", "value", cfg.SidecarMemory, "error", err)
		} else {
			m.sidecarMemory = qty
		}
	}

	// Do initial capacity calculation
	m.refresh(context.Background())

	// Start background refresh loop
	go m.refreshLoop()

	return m, nil
}

// GetCapacity returns the current node capacity.
func (m *Manager) GetCapacity() *NodeCapacity {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.capacity == nil {
		return nil
	}

	// Return a copy with updated current count
	cap := *m.capacity
	if m.sandboxCounter != nil {
		cap.CurrentSandboxes = m.sandboxCounter.CountSandboxesOnNode(m.cfg.NodeName)
	}
	return &cap
}

// Stop stops the background refresh loop.
func (m *Manager) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopCh)
	})
}

func (m *Manager) refreshLoop() {
	ticker := time.NewTicker(m.cfg.RefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.refresh(context.Background())
		case <-m.stopCh:
			return
		}
	}
}

func (m *Manager) refresh(ctx context.Context) {
	if m.cfg.NodeName == "" {
		return
	}

	node, err := m.client.CoreV1().Nodes().Get(ctx, m.cfg.NodeName, metav1.GetOptions{})
	if err != nil {
		slog.Error("failed to get node for capacity calculation", "node", m.cfg.NodeName, "error", err)
		return
	}

	max := m.computeMax(node)
	var current int32
	if m.sandboxCounter != nil {
		current = m.sandboxCounter.CountSandboxesOnNode(m.cfg.NodeName)
	}

	m.mu.Lock()
	m.capacity = &NodeCapacity{
		NodeName:         m.cfg.NodeName,
		MaxSandboxes:     max,
		CurrentSandboxes: current,
		CalculatedAt:     time.Now(),
	}
	m.mu.Unlock()

	slog.Debug("refreshed node capacity", "node", m.cfg.NodeName, "max", max, "current", current)
}

func (m *Manager) computeMax(node *corev1.Node) int32 {
	allocatable := node.Status.Allocatable

	// If no sandbox resources configured, use node max pods
	if m.sandboxCPU.IsZero() && m.sandboxMemory.IsZero() {
		pods := allocatable.Pods()
		if pods != nil {
			return int32(pods.Value())
		}
		return 110 // Default Kubernetes maxPods
	}

	// Calculate total resources per pod (sandbox + sidecar if configured)
	totalCPU := m.sandboxCPU.MilliValue()
	if !m.sidecarCPU.IsZero() {
		totalCPU += m.sidecarCPU.MilliValue()
	}
	totalMemory := m.sandboxMemory.Value()
	if !m.sidecarMemory.IsZero() {
		totalMemory += m.sidecarMemory.Value()
	}

	var maxByCPU, maxByMem int64 = 1<<31 - 1, 1<<31 - 1 // Start with max int32

	// Calculate max based on CPU
	if totalCPU > 0 {
		allocCPU := allocatable.Cpu()
		if allocCPU != nil {
			maxByCPU = allocCPU.MilliValue() / totalCPU
		}
	}

	// Calculate max based on memory
	if totalMemory > 0 {
		allocMem := allocatable.Memory()
		if allocMem != nil {
			maxByMem = allocMem.Value() / totalMemory
		}
	}

	// Get max pods limit
	var maxByPods int64 = 1<<31 - 1
	pods := allocatable.Pods()
	if pods != nil {
		maxByPods = pods.Value()
	}

	// Take minimum
	max := maxByCPU
	if maxByMem < max {
		max = maxByMem
	}
	if maxByPods < max {
		max = maxByPods
	}

	// Apply overhead factor (90% of theoretical max)
	max = int64(float64(max) * OverheadFactor)

	// Ensure at least 1
	if max < 1 {
		max = 1
	}

	return int32(max)
}
