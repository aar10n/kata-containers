package capacity

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

// AgentWatcherConfig holds configuration for the agent watcher.
type AgentWatcherConfig struct {
	// Namespace where sandbox-agent pods are running.
	Namespace string
	// LabelSelector to find sandbox-agent pods (e.g., "app=sandbox-agent").
	LabelSelector string
	// HTTPPort is the port agents listen on for HTTP/health requests.
	HTTPPort int
	// PollInterval is how often to poll all agents for capacity.
	PollInterval time.Duration
	// PollTimeout is the timeout for polling a single agent.
	PollTimeout time.Duration
}

// AgentWatcher watches sandbox-agent pods and polls them for capacity.
// It immediately removes capacity when an agent pod is deleted.
type AgentWatcher struct {
	cfg        AgentWatcherConfig
	client     kubernetes.Interface
	tracker    *Tracker
	httpClient *http.Client

	mu     sync.RWMutex
	agents map[string]string // pod name -> pod IP

	stopCh   chan struct{}
	stopOnce sync.Once
}

// NewAgentWatcher creates a new agent watcher.
func NewAgentWatcher(cfg AgentWatcherConfig, client kubernetes.Interface, tracker *Tracker) *AgentWatcher {
	if cfg.HTTPPort <= 0 {
		cfg.HTTPPort = 8080
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 15 * time.Second
	}
	if cfg.PollTimeout <= 0 {
		cfg.PollTimeout = 5 * time.Second
	}

	return &AgentWatcher{
		cfg:     cfg,
		client:  client,
		tracker: tracker,
		httpClient: &http.Client{
			Timeout: cfg.PollTimeout,
		},
		agents: make(map[string]string),
		stopCh: make(chan struct{}),
	}
}

// Start begins watching agent pods and polling for capacity.
func (w *AgentWatcher) Start(ctx context.Context) error {
	// Initial discovery
	if err := w.discoverAgents(ctx); err != nil {
		return fmt.Errorf("initial agent discovery: %w", err)
	}

	// Start watch loop for pod events
	go w.watchLoop(ctx)

	// Start poll loop for capacity updates
	go w.pollLoop(ctx)

	slog.Info("agent watcher started",
		"namespace", w.cfg.Namespace,
		"selector", w.cfg.LabelSelector,
		"poll_interval", w.cfg.PollInterval)

	return nil
}

// Stop stops the agent watcher.
func (w *AgentWatcher) Stop() {
	w.stopOnce.Do(func() {
		close(w.stopCh)
	})
}

// discoverAgents lists all current sandbox-agent pods.
func (w *AgentWatcher) discoverAgents(ctx context.Context) error {
	pods, err := w.client.CoreV1().Pods(w.cfg.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: w.cfg.LabelSelector,
	})
	if err != nil {
		return fmt.Errorf("list pods: %w", err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	for _, pod := range pods.Items {
		if pod.Status.PodIP != "" && pod.Status.Phase == corev1.PodRunning {
			w.agents[pod.Name] = pod.Status.PodIP
			slog.Debug("discovered agent", "pod", pod.Name, "ip", pod.Status.PodIP, "node", pod.Spec.NodeName)
		}
	}

	slog.Info("discovered agents", "count", len(w.agents))
	return nil
}

// watchLoop watches for pod add/delete events.
func (w *AgentWatcher) watchLoop(ctx context.Context) {
	for {
		select {
		case <-w.stopCh:
			return
		case <-ctx.Done():
			return
		default:
		}

		watcher, err := w.client.CoreV1().Pods(w.cfg.Namespace).Watch(ctx, metav1.ListOptions{
			LabelSelector: w.cfg.LabelSelector,
		})
		if err != nil {
			slog.Error("failed to watch pods", "error", err)
			time.Sleep(5 * time.Second)
			continue
		}

		w.handleWatchEvents(ctx, watcher)
		watcher.Stop()
	}
}

// handleWatchEvents processes pod watch events.
func (w *AgentWatcher) handleWatchEvents(ctx context.Context, watcher watch.Interface) {
	for {
		select {
		case <-w.stopCh:
			return
		case <-ctx.Done():
			return
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return // watch closed, will reconnect
			}

			pod, ok := event.Object.(*corev1.Pod)
			if !ok {
				continue
			}

			switch event.Type {
			case watch.Added, watch.Modified:
				if pod.Status.PodIP != "" && pod.Status.Phase == corev1.PodRunning {
					w.mu.Lock()
					if _, exists := w.agents[pod.Name]; !exists {
						w.agents[pod.Name] = pod.Status.PodIP
						slog.Info("agent added", "pod", pod.Name, "ip", pod.Status.PodIP, "node", pod.Spec.NodeName)
					}
					w.mu.Unlock()
				}

			case watch.Deleted:
				w.mu.Lock()
				if _, exists := w.agents[pod.Name]; exists {
					delete(w.agents, pod.Name)
					slog.Info("agent removed", "pod", pod.Name, "node", pod.Spec.NodeName)
				}
				w.mu.Unlock()

				// Immediately remove capacity for this node
				if pod.Spec.NodeName != "" {
					w.tracker.RemoveNode(pod.Spec.NodeName)
					slog.Info("removed capacity for node", "node", pod.Spec.NodeName)
				}
			}
		}
	}
}

// pollLoop periodically polls all agents for capacity.
func (w *AgentWatcher) pollLoop(ctx context.Context) {
	// Initial poll
	w.pollAllAgents(ctx)

	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.pollAllAgents(ctx)
		case <-w.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

// pollAllAgents polls all known agents for their capacity.
func (w *AgentWatcher) pollAllAgents(ctx context.Context) {
	w.mu.RLock()
	agents := make(map[string]string, len(w.agents))
	for k, v := range w.agents {
		agents[k] = v
	}
	w.mu.RUnlock()

	if len(agents) == 0 {
		slog.Debug("no agents to poll")
		return
	}

	var wg sync.WaitGroup
	for podName, podIP := range agents {
		wg.Add(1)
		go func(podName, podIP string) {
			defer wg.Done()
			w.pollAgent(ctx, podName, podIP)
		}(podName, podIP)
	}
	wg.Wait()
}

// pollAgent polls a single agent for its capacity.
func (w *AgentWatcher) pollAgent(ctx context.Context, podName, podIP string) {
	url := fmt.Sprintf("http://%s:%d/healthz", podIP, w.cfg.HTTPPort)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		slog.Debug("failed to create request", "pod", podName, "error", err)
		return
	}

	resp, err := w.httpClient.Do(req)
	if err != nil {
		slog.Debug("failed to poll agent", "pod", podName, "error", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		slog.Debug("agent returned error", "pod", podName, "status", resp.StatusCode)
		return
	}

	// Parse health response - protobuf marshals int64 as string in JSON
	var health struct {
		Status   string `json:"status"`
		Mode     string `json:"mode"`
		Capacity *struct {
			NodeName         string `json:"nodeName"`
			MaxSandboxes     int32  `json:"maxSandboxes"`
			CurrentSandboxes int32  `json:"currentSandboxes"`
			CalculatedAtUnix string `json:"calculatedAtUnix"`
		} `json:"capacity"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		slog.Debug("failed to decode response", "pod", podName, "error", err)
		return
	}

	if health.Capacity == nil {
		slog.Debug("no capacity in response", "pod", podName)
		return
	}

	// Update capacity tracker
	w.tracker.UpdateNode(
		health.Capacity.NodeName,
		health.Capacity.MaxSandboxes,
		health.Capacity.CurrentSandboxes,
	)

	var calculatedAt time.Time
	if ts, err := strconv.ParseInt(health.Capacity.CalculatedAtUnix, 10, 64); err == nil {
		calculatedAt = time.Unix(ts, 0)
	}

	slog.Debug("polled agent",
		"pod", podName,
		"node", health.Capacity.NodeName,
		"max", health.Capacity.MaxSandboxes,
		"current", health.Capacity.CurrentSandboxes,
		"calculated_at", calculatedAt)
}
