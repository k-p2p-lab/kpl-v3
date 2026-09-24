package agent

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func (s *Server) metricsHandler() http.Handler {
	return promhttp.HandlerFor(newAgentMetricsRegistry(s), promhttp.HandlerOpts{})
}

// metricsOnlyHandler deliberately exposes no Agent control or status routes.
// Swarm may publish this listener on every worker while the full control API
// remains reachable only over the private overlay network.
func (s *Server) metricsOnlyHandler() http.Handler {
	metrics := s.metricsHandler()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if r.URL.Path != "/metrics" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		metrics.ServeHTTP(w, r)
	})
}

func validateMetricsEndpoint(listen, advertisedURL string) error {
	if strings.TrimSpace(listen) != listen || strings.TrimSpace(advertisedURL) != advertisedURL {
		return fmt.Errorf("metrics listen address and URL cannot have surrounding whitespace")
	}
	if (listen == "") != (advertisedURL == "") {
		return fmt.Errorf("metrics listen address and URL must be configured together")
	}
	if advertisedURL == "" {
		return nil
	}
	parsed, err := url.Parse(advertisedURL)
	if err != nil || parsed.Opaque != "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" {
		return fmt.Errorf("metrics URL must be an absolute HTTP(S) URL")
	}
	if parsed.User != nil {
		return fmt.Errorf("metrics URL cannot contain user information")
	}
	if !metricsHostIsReachable(parsed.Hostname()) {
		return fmt.Errorf("metrics URL must use a remotely reachable host")
	}
	if parsed.Path != "/metrics" || parsed.RawPath != "" {
		return fmt.Errorf("metrics URL path must be exactly /metrics")
	}
	if parsed.RawQuery != "" || parsed.ForceQuery {
		return fmt.Errorf("metrics URL cannot contain a query")
	}
	if strings.Contains(advertisedURL, "#") || parsed.Fragment != "" || parsed.RawFragment != "" {
		return fmt.Errorf("metrics URL cannot contain a fragment")
	}
	if strings.HasSuffix(parsed.Host, ":") {
		return fmt.Errorf("metrics URL contains an empty port")
	}
	if rawPort := parsed.Port(); rawPort != "" {
		port, parseErr := strconv.Atoi(rawPort)
		if parseErr != nil || port < 1 || port > 65535 {
			return fmt.Errorf("metrics URL port must be between 1 and 65535")
		}
	}
	return nil
}

func metricsHostIsReachable(host string) bool {
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	if host == "localhost" || host == "localhost.localdomain" || strings.HasSuffix(host, ".localhost") {
		return false
	}
	if ip := net.ParseIP(host); ip != nil && (ip.IsLoopback() || ip.IsUnspecified()) {
		return false
	}
	return true
}

func newAgentMetricsRegistry(s *Server) *prometheus.Registry {
	registry := prometheus.NewRegistry()
	registry.MustRegister(newLocalCollector(s))
	// These describe the Agent's own Go runtime and OS process. Peer containers
	// have separate processes and are not included in these CPU/memory metrics.
	processRegistry := prometheus.WrapRegistererWith(prometheus.Labels{"agent_id": s.config.ID}, registry)
	processRegistry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return registry
}

type localCollector struct {
	server                                          *Server
	nodes, capacity, cleanupPending, telemetryQueue *prometheus.Desc
}

func newLocalCollector(s *Server) *localCollector {
	return &localCollector{
		server:         s,
		nodes:          prometheus.NewDesc("kpl_local_nodes", "Peer records held by this Agent, including retained stopped and failed records, by current state and peer runtime.", []string{"agent_id", "state", "runtime"}, nil),
		capacity:       prometheus.NewDesc("kpl_local_capacity", "Configured peer admission capacity of this Agent; this is not a CPU or memory limit.", []string{"agent_id"}, nil),
		cleanupPending: prometheus.NewDesc("kpl_local_cleanup_pending", "Docker peers awaiting cleanup or whose previous container cleanup failed.", []string{"agent_id"}, nil),
		telemetryQueue: prometheus.NewDesc("kpl_local_telemetry_queue_events", "Telemetry events currently queued in this Agent for forwarding to the Controller.", []string{"agent_id"}, nil),
	}
}

func (c *localCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.nodes
	ch <- c.capacity
	ch <- c.cleanupPending
	ch <- c.telemetryQueue
}

func (c *localCollector) Collect(ch chan<- prometheus.Metric) {
	type nodeKey struct{ state, runtime string }
	s := c.server
	s.mu.RLock()
	agentID, capacity := s.config.ID, s.capacityLocked()
	const defaultRuntime = "docker"
	counts := make(map[nodeKey]int)
	for _, state := range []string{model.NodeStarting, model.NodeReady, model.NodeStopping, model.NodeStopped, model.NodeFailed} {
		counts[nodeKey{state, defaultRuntime}] = 0
	}
	cleanupPending := 0
	for _, proc := range s.processes {
		counts[nodeKey{metricsNodeState(proc.node.State), defaultRuntime}]++
		if proc.cleanupErr != nil || !proc.exited && proc.node.State == model.NodeStopping {
			cleanupPending++
		}
	}
	s.mu.RUnlock()
	s.eventsMu.Lock()
	queued := len(s.events)
	s.eventsMu.Unlock()
	for key, count := range counts {
		ch <- prometheus.MustNewConstMetric(c.nodes, prometheus.GaugeValue, float64(count), agentID, key.state, key.runtime)
	}
	ch <- prometheus.MustNewConstMetric(c.capacity, prometheus.GaugeValue, float64(capacity), agentID)
	ch <- prometheus.MustNewConstMetric(c.cleanupPending, prometheus.GaugeValue, float64(cleanupPending), agentID)
	ch <- prometheus.MustNewConstMetric(c.telemetryQueue, prometheus.GaugeValue, float64(queued), agentID)
}

func metricsNodeState(value string) string {
	switch value {
	case model.NodeStarting, model.NodeReady, model.NodeStopping, model.NodeStopped, model.NodeFailed:
		return value
	default:
		return "unknown"
	}
}
