package capacity

import (
	"log/slog"
	"sync"
	"time"
)

const (
	// DefaultOvercommitPercent allows slight overcommit.
	DefaultOvercommitPercent = 5
	// DefaultStaleThreshold marks nodes as stale if not updated within this duration.
	DefaultStaleThreshold = 2 * time.Minute
)

// NodeInfo holds capacity information for a single node.
type NodeInfo struct {
	Name             string
	MaxSandboxes     int32
	CurrentSandboxes int32
	LastUpdated      time.Time
}

// IsStale returns true if the node info hasn't been updated recently.
func (n *NodeInfo) IsStale(threshold time.Duration) bool {
	return time.Since(n.LastUpdated) > threshold
}

// Available returns the number of sandboxes that can still be created on this node.
func (n *NodeInfo) Available() int32 {
	avail := n.MaxSandboxes - n.CurrentSandboxes
	if avail < 0 {
		return 0
	}
	return avail
}

// Config holds tracker configuration.
type Config struct {
	// MaxSandboxes is an optional hard limit (0 = use dynamic capacity from agents).
	MaxSandboxes int
	// OvercommitPercent allows slight overcommit (default: 5%).
	OvercommitPercent int
	// StaleThreshold marks nodes as stale (default: 2min).
	StaleThreshold time.Duration
}

// Tracker aggregates capacity information from multiple sandbox-agent nodes.
type Tracker struct {
	cfg   Config
	mu    sync.RWMutex
	nodes map[string]*NodeInfo
}

// NewTracker creates a new capacity tracker.
func NewTracker(cfg Config) *Tracker {
	if cfg.OvercommitPercent == 0 {
		cfg.OvercommitPercent = DefaultOvercommitPercent
	}
	if cfg.StaleThreshold == 0 {
		cfg.StaleThreshold = DefaultStaleThreshold
	}

	return &Tracker{
		cfg:   cfg,
		nodes: make(map[string]*NodeInfo),
	}
}

// UpdateNode updates capacity information for a node.
func (t *Tracker) UpdateNode(name string, maxSandboxes, currentSandboxes int32) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.nodes[name] = &NodeInfo{
		Name:             name,
		MaxSandboxes:     maxSandboxes,
		CurrentSandboxes: currentSandboxes,
		LastUpdated:      time.Now(),
	}

	slog.Debug("updated node capacity",
		"node", name,
		"max", maxSandboxes,
		"current", currentSandboxes,
	)
}

// RemoveNode removes a node from tracking.
func (t *Tracker) RemoveNode(name string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.nodes, name)
}

// CanCreateSandbox returns true if sandbox creation is allowed.
func (t *Tracker) CanCreateSandbox() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// If hard limit is set, check total current vs limit
	if t.cfg.MaxSandboxes > 0 {
		total := t.totalCurrent()
		limit := t.effectiveLimit(int32(t.cfg.MaxSandboxes))
		return total < limit
	}

	// Dynamic capacity: check if any node has capacity
	totalMax := t.totalMax()
	totalCurrent := t.totalCurrent()

	// Allow creation if under capacity (with overcommit)
	limit := t.effectiveLimit(totalMax)
	return totalCurrent < limit
}

// GetExcessSandboxes returns how many sandboxes are over capacity (0 if under).
func (t *Tracker) GetExcessSandboxes() int32 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var limit int32
	if t.cfg.MaxSandboxes > 0 {
		limit = int32(t.cfg.MaxSandboxes)
	} else {
		limit = t.totalMax()
	}

	current := t.totalCurrent()
	if current > limit {
		return current - limit
	}
	return 0
}

// GetCapacitySummary returns aggregate capacity information.
func (t *Tracker) GetCapacitySummary() (totalMax, totalCurrent, availableNodes int32) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	totalMax = t.totalMax()
	totalCurrent = t.totalCurrent()

	for _, node := range t.nodes {
		if !node.IsStale(t.cfg.StaleThreshold) && node.Available() > 0 {
			availableNodes++
		}
	}

	return totalMax, totalCurrent, availableNodes
}

// GetNodes returns a copy of all tracked nodes.
func (t *Tracker) GetNodes() []NodeInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()

	nodes := make([]NodeInfo, 0, len(t.nodes))
	for _, n := range t.nodes {
		nodes = append(nodes, *n)
	}
	return nodes
}

// ClusterCapacity holds aggregated cluster-wide capacity from an agent.
type ClusterCapacity struct {
	TotalMaxSandboxes     int32
	TotalCurrentSandboxes int32
	AvailableNodes        int32
	Nodes                 []NodeCapacityInfo
	CalculatedAt          time.Time
}

// NodeCapacityInfo holds capacity info for a single node (from cluster response).
type NodeCapacityInfo struct {
	NodeName         string
	MaxSandboxes     int32
	CurrentSandboxes int32
	CalculatedAt     time.Time
}

// UpdateFromCluster updates all node capacity information from a cluster-wide response.
// This is more efficient than polling each agent individually.
func (t *Tracker) UpdateFromCluster(cluster *ClusterCapacity) {
	if cluster == nil || len(cluster.Nodes) == 0 {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	for _, node := range cluster.Nodes {
		t.nodes[node.NodeName] = &NodeInfo{
			Name:             node.NodeName,
			MaxSandboxes:     node.MaxSandboxes,
			CurrentSandboxes: node.CurrentSandboxes,
			LastUpdated:      time.Now(),
		}
	}

	slog.Debug("updated capacity from cluster",
		"nodes", len(cluster.Nodes),
		"total_max", cluster.TotalMaxSandboxes,
		"total_current", cluster.TotalCurrentSandboxes,
	)
}

// totalMax returns the sum of max sandboxes across all non-stale nodes.
func (t *Tracker) totalMax() int32 {
	var total int32
	for _, node := range t.nodes {
		if !node.IsStale(t.cfg.StaleThreshold) {
			total += node.MaxSandboxes
		}
	}
	return total
}

// totalCurrent returns the sum of current sandboxes across all nodes.
func (t *Tracker) totalCurrent() int32 {
	var total int32
	for _, node := range t.nodes {
		total += node.CurrentSandboxes
	}
	return total
}

// effectiveLimit applies overcommit percentage to a limit.
func (t *Tracker) effectiveLimit(limit int32) int32 {
	overcommit := float64(t.cfg.OvercommitPercent) / 100.0
	return int32(float64(limit) * (1.0 + overcommit))
}
