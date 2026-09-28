package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const resourceInterval = 10 * time.Second
const resourceTimeout = 8 * time.Second
const resourceWorkers = 4

type containerResourceStats struct {
	Read time.Time `json:"read"`
	CPU  struct {
		OnlineCPUs int `json:"online_cpus"`
		Usage      struct {
			Total  *uint64  `json:"total_usage"`
			PerCPU []uint64 `json:"percpu_usage"`
		} `json:"cpu_usage"`
	} `json:"cpu_stats"`
	Memory struct {
		Usage *uint64           `json:"usage"`
		Stats map[string]uint64 `json:"stats"`
	} `json:"memory_stats"`
}

type resourceSampler struct {
	client                 *http.Client
	baseURL                string
	agentID, network, self string
	previous               map[string]containerResourceStats
	warmup                 time.Duration
	cpuCapacity            int
	cpuCapacityAt          time.Time
}

func newResourceSampler(config Config) *resourceSampler {
	socket := config.DockerSocket
	if socket == "" {
		socket = "/var/run/docker.sock"
	}
	transport := &http.Transport{MaxConnsPerHost: resourceWorkers, MaxIdleConnsPerHost: resourceWorkers, IdleConnTimeout: time.Minute,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		},
	}
	return &resourceSampler{client: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, baseURL: "http://docker", agentID: config.ID, network: config.DockerNetwork, self: config.SelfContainer, previous: map[string]containerResourceStats{}, warmup: time.Second}
}

// Read-only Engine calls use their own connection pool and deadline. Neither
// a metrics scrape nor a heartbeat performs Docker I/O or starts a sample.
func (s *resourceSampler) get(ctx context.Context, path string, target any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.baseURL+path, nil)
	if err != nil {
		return err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Docker resource request returned HTTP %d", resp.StatusCode)
	}
	const limit = 4 << 20
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return err
	}
	if len(data) > limit {
		return errors.New("Docker resource response exceeds size limit")
	}
	return json.Unmarshal(data, target)
}

func (s *resourceSampler) inventory(ctx context.Context) ([]string, error) {
	if s.self == "" {
		return nil, errors.New("Agent container identity is unavailable; configure --self-container")
	}
	type container struct {
		ID     string `json:"Id"`
		Names  []string
		Labels map[string]string
	}
	var peers, own []container
	filters, _ := json.Marshal(map[string][]string{"label": {"io.kpl.managed=true", "io.kpl.agent=" + s.agentID, "io.kpl.network=" + s.network}})
	if err := s.get(ctx, "/containers/json?filters="+url.QueryEscape(string(filters)), &peers); err != nil {
		return nil, err
	}
	filters, _ = json.Marshal(map[string][]string{"name": {s.self}})
	if err := s.get(ctx, "/containers/json?filters="+url.QueryEscape(string(filters)), &own); err != nil {
		return nil, err
	}
	ids := map[string]bool{}
	foundSelf := false
	for _, c := range own {
		for _, name := range c.Names {
			if strings.TrimPrefix(name, "/") == s.self && dockerContainerID.MatchString(c.ID) {
				ids[c.ID], foundSelf = true, true
			}
		}
	}
	if !foundSelf {
		return nil, errors.New("Agent container was not found in the local Docker daemon")
	}
	for _, c := range peers {
		// Recheck ownership even if a proxy/daemon ignores the requested filters.
		if c.Labels["io.kpl.managed"] == "true" && c.Labels["io.kpl.agent"] == s.agentID && c.Labels["io.kpl.network"] == s.network && dockerContainerID.MatchString(c.ID) {
			ids[c.ID] = true
		}
	}
	result := make([]string, 0, len(ids))
	for id := range ids {
		result = append(result, id)
	}
	sort.Strings(result)
	return result, nil
}

func (s *resourceSampler) readStats(ctx context.Context, ids []string) map[string]containerResourceStats {
	results := make([]containerResourceStats, len(ids))
	jobs := make(chan int, len(ids))
	for i := range ids {
		jobs <- i
	}
	close(jobs)
	var workers sync.WaitGroup
	for range min(resourceWorkers, len(ids)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range jobs {
				if ctx.Err() != nil {
					return
				}
				var value containerResourceStats
				if s.get(ctx, "/containers/"+ids[i]+"/stats?stream=false&one-shot=true", &value) == nil && !value.Read.IsZero() && value.CPU.Usage.Total != nil && value.Memory.Usage != nil {
					results[i] = value
				}
			}
		}()
	}
	workers.Wait()
	found := make(map[string]containerResourceStats, len(ids))
	for i, value := range results {
		if !value.Read.IsZero() {
			found[ids[i]] = value
		}
	}
	return found
}

func resourceCPU(previous, current containerResourceStats) (float64, bool) {
	if previous.CPU.Usage.Total == nil || current.CPU.Usage.Total == nil || !current.Read.After(previous.Read) || *current.CPU.Usage.Total < *previous.CPU.Usage.Total {
		return 0, false
	}
	elapsed := current.Read.Sub(previous.Read)
	if elapsed > 3*resourceInterval {
		return 0, false
	}
	return float64(*current.CPU.Usage.Total-*previous.CPU.Usage.Total) / float64(elapsed.Nanoseconds()), true
}

// Prefer the host-wide CPU count already returned by stats. The fallback is
// cached and bounded, so normal sampling adds no Docker requests.
func (s *resourceSampler) hostCPUCapacity(ctx context.Context, current map[string]containerResourceStats) int {
	capacity := 0
	for _, value := range current {
		count := value.CPU.OnlineCPUs
		if count <= 0 {
			count = len(value.CPU.Usage.PerCPU)
		}
		capacity = max(capacity, count)
	}
	now := time.Now()
	if capacity > 0 {
		s.cpuCapacity, s.cpuCapacityAt = capacity, now
		return capacity
	}
	if s.cpuCapacity > 0 && now.Sub(s.cpuCapacityAt) < time.Minute {
		return s.cpuCapacity
	}
	infoCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	var info struct {
		NCPU int `json:"NCPU"`
	}
	if s.get(infoCtx, "/info", &info) == nil && info.NCPU > 0 {
		s.cpuCapacity, s.cpuCapacityAt = info.NCPU, now
		return info.NCPU
	}
	return 0
}

func (s *resourceSampler) sample(ctx context.Context) *model.AgentResources {
	ctx, cancel := context.WithTimeout(ctx, resourceTimeout)
	defer cancel()
	result := &model.AgentResources{SampledAt: time.Now().UTC()}
	ids, err := s.inventory(ctx)
	if err != nil {
		result.Error = err.Error()
		s.previous = map[string]containerResourceStats{}
		return result
	}
	result.Containers = len(ids)
	current := s.readStats(ctx, ids)
	newIDs := []string{}
	for _, id := range ids {
		value, ok := current[id]
		if !ok {
			continue
		}
		if _, valid := resourceCPU(s.previous[id], value); !valid {
			s.previous[id] = value
			newIDs = append(newIDs, id)
		}
	}
	// A new container needs two counter readings. Warm all new containers
	// together; do not block a worker for a second per Peer or spawn N processes.
	if len(newIDs) > 0 {
		timer := time.NewTimer(s.warmup)
		defer timer.Stop()
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
		for _, id := range newIDs {
			delete(current, id)
		}
		for id, value := range s.readStats(ctx, newIDs) {
			current[id] = value
		}
	}
	var cpu float64
	var usage, working uint64
	for _, id := range ids {
		value, ok := current[id]
		if !ok {
			continue
		}
		cores, ok := resourceCPU(s.previous[id], value)
		if !ok {
			continue
		}
		cache, exists := value.Memory.Stats["total_inactive_file"]
		if !exists {
			cache = value.Memory.Stats["inactive_file"]
		}
		used := *value.Memory.Usage
		usage += used
		// Match Docker CLI: inconsistent cache accounting must not
		// turn an occupied container into zero memory usage.
		if cache < used {
			working += used - cache
		} else {
			working += used
		}
		cpu += cores
		result.MeasuredContainers++
	}
	s.previous = current // Bound retained state to the current inventory.
	result.CPUCapacityCores = s.hostCPUCapacity(ctx, current)
	result.Complete = result.MeasuredContainers == result.Containers && ctx.Err() == nil
	if result.MeasuredContainers > 0 {
		result.CPUCores, result.MemoryUsageBytes, result.MemoryWorkingSetBytes = &cpu, &usage, &working
	}
	if !result.Complete {
		result.Error = "Some containers could not be measured; showing available samples and retrying automatically"
	}
	return result
}

func (s *Server) resourceLoop(ctx context.Context) {
	config := s.config
	if s.docker != nil {
		config.DockerNetwork = s.docker.network
	}
	sampler := newResourceSampler(config)
	defer sampler.client.CloseIdleConnections()
	ticker := time.NewTicker(resourceInterval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		sample := sampler.sample(ctx)
		s.mu.Lock()
		s.resources = sample
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
