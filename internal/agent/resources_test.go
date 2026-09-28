package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestResourcesSumOnlySelfAndOwnedPeers(t *testing.T) {
	id := func(i int) string { return fmt.Sprintf("%064x", i) }
	var mu sync.Mutex
	reads := map[string]int{}
	missing, omit, missingAgent, emptyPeers := false, false, false, false
	var active, maxActive atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Error("resource sampler attempted a mutation")
		}
		n := active.Add(1)
		defer active.Add(-1)
		for old := maxActive.Load(); n > old && !maxActive.CompareAndSwap(old, n); old = maxActive.Load() {
		}
		time.Sleep(time.Millisecond)
		if r.URL.Path == "/containers/json" {
			var filters map[string][]string
			if err := json.Unmarshal([]byte(r.URL.Query().Get("filters")), &filters); err != nil {
				t.Error(err)
			}
			if _, self := filters["name"]; self {
				json.NewEncoder(w).Encode([]any{map[string]any{"Id": id(1), "Names": []string{"/agent-task"}}, map[string]any{"Id": id(6), "Names": []string{"/agent-task-extra"}}})
				return
			}
			mu.Lock()
			defer mu.Unlock()
			peers := []any{}
			for i := 2; i <= 5; i++ {
				if emptyPeers || omit && i == 3 {
					continue
				}
				owner, network := "worker", "network"
				if i == 4 {
					owner = "other"
				}
				if i == 5 {
					network = "other"
				}
				peers = append(peers, map[string]any{"Id": id(i), "Labels": map[string]string{"io.kpl.managed": "true", "io.kpl.agent": owner, "io.kpl.network": network}})
			}
			json.NewEncoder(w).Encode(peers)
			return
		}
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) != 4 || parts[1] != "containers" || parts[3] != "stats" {
			t.Errorf("unexpected path %s", r.URL.Path)
			w.WriteHeader(404)
			return
		}
		if r.URL.Query().Get("stream") != "false" || r.URL.Query().Get("one-shot") != "true" {
			t.Error("streaming or blocking Docker stats enabled")
		}
		container := parts[2]
		if container != id(1) && container != id(2) && container != id(3) {
			t.Errorf("sampled unrelated container %s", container)
		}
		mu.Lock()
		reads[container]++
		count := reads[container]
		failed := missing && container == id(3) || missingAgent && container == id(1)
		mu.Unlock()
		if failed {
			w.WriteHeader(404)
			return
		}
		cache := "inactive_file"
		if container == id(2) {
			cache = "total_inactive_file"
		}
		json.NewEncoder(w).Encode(map[string]any{"read": time.Unix(1000+int64(count), 0).UTC(), "cpu_stats": map[string]any{"online_cpus": 8, "cpu_usage": map[string]any{"total_usage": uint64(count) * 500000000}}, "memory_stats": map[string]any{"usage": 1000, "stats": map[string]uint64{cache: 200}}})
	}))
	defer server.Close()
	sampler := &resourceSampler{client: server.Client(), baseURL: server.URL, agentID: "worker", network: "network", self: "agent-task", previous: map[string]containerResourceStats{}}
	first := sampler.sample(context.Background())
	if !first.Valid() || *first.CPUCores != 1.5 || *first.MemoryUsageBytes != 3000 || *first.MemoryWorkingSetBytes != 2400 || first.Containers != 3 {
		t.Fatalf("sum: %+v", first)
	}
	if percent, ok := first.CPUPercent(); !ok || percent != 18.75 {
		t.Fatalf("host CPU percent: %v %v", percent, ok)
	}
	if first.Agent == nil || first.Peers == nil || !first.ScopeValid("agent") || !first.ScopeValid("peers") ||
		*first.Agent.CPUCores != 0.5 || *first.Peers.CPUCores != 1 || *first.Agent.MemoryWorkingSetBytes != 800 || *first.Peers.MemoryWorkingSetBytes != 1600 || first.Agent.Containers != 1 || first.Peers.Containers != 2 {
		t.Fatalf("incorrect component split: %+v %+v", first.Agent, first.Peers)
	}
	if len(sampler.selfMemory) == 0 || sampler.selfMemoryAt.IsZero() {
		t.Fatal("missing self memory sample")
	}
	second := sampler.sample(context.Background())
	if !second.Valid() || *second.CPUCores != 1.5 {
		t.Fatalf("cached baseline: %+v", second)
	}
	mu.Lock()
	for id, n := range reads {
		if n != 3 {
			t.Errorf("container %s read %d times; unchanged containers should need one read after warmup", id, n)
		}
	}
	missing = true
	mu.Unlock()
	failed := sampler.sample(context.Background())
	if failed.Complete || !failed.Valid() || *failed.CPUCores != 1 || *failed.MemoryWorkingSetBytes != 1600 || failed.MeasuredContainers != 2 || failed.CPUCapacityCores != 8 {
		t.Fatalf("partial sum lost: %+v", failed)
	}
	if !failed.Agent.Complete || failed.Peers.Complete || failed.Peers.MeasuredContainers != 1 || *failed.Peers.CPUCores != 0.5 {
		t.Fatalf("partial Peer sample affected Agent sample: %+v", failed)
	}
	mu.Lock()
	missing = false
	omit = true
	mu.Unlock()
	third := sampler.sample(context.Background())
	if !third.Valid() || third.Containers != 2 || len(sampler.previous) != 2 {
		t.Fatalf("departed container cache retained: %+v", third)
	}
	mu.Lock()
	missingAgent = true
	mu.Unlock()
	withoutAgent := sampler.sample(context.Background())
	if withoutAgent.ScopeValid("agent") || withoutAgent.Agent.CPUCores != nil || !withoutAgent.ScopeValid("peers") || !withoutAgent.Peers.Complete || *withoutAgent.CPUCores != *withoutAgent.Peers.CPUCores {
		t.Fatal("unavailable Agent hid measured Peers or became zero")
	}
	mu.Lock()
	emptyPeers, missingAgent = true, false
	mu.Unlock()
	empty := sampler.sample(context.Background())
	if !empty.ScopeValid("peers") || *empty.Peers.CPUCores != 0 || *empty.Peers.MemoryUsageBytes != 0 || empty.Peers.Containers != 0 || !empty.Peers.Complete || *empty.Agent.CPUCores != *empty.CPUCores {
		t.Fatalf("empty Peer inventory was not measured zero: %+v", empty)
	}
	mu.Lock()
	missingAgent = true
	mu.Unlock()
	empty = sampler.sample(context.Background())
	if sampler.selfMemory != nil || !sampler.selfMemoryAt.IsZero() {
		t.Fatal("failed Agent sample retained stale memory counters")
	}
	if empty.Valid() || empty.ScopeValid("agent") || !empty.ScopeValid("peers") {
		t.Fatal("failed Agent sample invalidated known-empty Peer inventory")
	}
	if maxActive.Load() > resourceWorkers {
		t.Fatalf("unbounded requests: %d", maxActive.Load())
	}
}

func TestResourceSamplerCancellationAndMissingSelf(t *testing.T) {
	requests := atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); <-r.Context().Done() }))
	defer server.Close()
	sampler := &resourceSampler{client: server.Client(), baseURL: server.URL, previous: map[string]containerResourceStats{}}
	if value := sampler.sample(context.Background()); value.Complete || !strings.Contains(value.Error, "identity") {
		t.Fatalf("missing self: %+v", value)
	}
	if requests.Load() != 0 {
		t.Fatal("missing self attempted to aggregate all Docker containers")
	}
	sampler.self = "self"
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if value := sampler.sample(ctx); value.Complete || value.CPUCores != nil || ctx.Err() == nil {
		t.Fatalf("cancel: %+v", value)
	}
}

func TestResourceCPUCountersNeedValidIntervals(t *testing.T) {
	stats := func(counter uint64, second int64) containerResourceStats {
		var value containerResourceStats
		if err := json.Unmarshal([]byte(`{"read":"`+time.Unix(second, 0).UTC().Format(time.RFC3339)+`","cpu_stats":{"cpu_usage":{"total_usage":`+strconv.FormatUint(counter, 10)+`}}}`), &value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	for _, pair := range [][2]containerResourceStats{{stats(5, 1), stats(4, 2)}, {stats(5, 1), stats(6, 1)}, {stats(5, 1), stats(6, 40)}, {{}, stats(6, 2)}} {
		if _, ok := resourceCPU(pair[0], pair[1]); ok {
			t.Fatal("invalid CPU counter interval accepted")
		}
	}
	if value, ok := resourceCPU(stats(1000000000, 1), stats(5000000000, 3)); !ok || value != 2 {
		t.Fatalf("multi-core CPU: %v %v", value, ok)
	}
}

func TestResourceStatsLargeInventoryHasBoundedConcurrency(t *testing.T) {
	var active, peak atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
		}
		time.Sleep(2 * time.Millisecond)
		json.NewEncoder(w).Encode(map[string]any{"read": time.Now().UTC(), "cpu_stats": map[string]any{"cpu_usage": map[string]any{"total_usage": 1}}, "memory_stats": map[string]any{"usage": 1024}})
	}))
	defer server.Close()
	sampler := &resourceSampler{client: server.Client(), baseURL: server.URL}
	ids := make([]string, 201)
	for i := range ids {
		ids[i] = fmt.Sprintf("%064x", i+1)
	}
	values := sampler.readStats(context.Background(), ids)
	if len(values) != len(ids) {
		t.Fatalf("missing large-inventory stats: %d/%d", len(values), len(ids))
	}
	if peak.Load() > resourceWorkers || peak.Load() < 2 {
		t.Fatalf("unexpected collection parallelism: %d", peak.Load())
	}
}

func TestResourceHostCPUCapacityUsesStatsAndBoundedFallback(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path != "/info" {
			t.Errorf("unexpected fallback: %s", r.URL.Path)
		}
		fmt.Fprint(w, `{"NCPU":32}`)
	}))
	defer server.Close()
	sampler := &resourceSampler{client: server.Client(), baseURL: server.URL}
	var stats containerResourceStats
	stats.CPU.OnlineCPUs = 16
	if got := sampler.hostCPUCapacity(context.Background(), map[string]containerResourceStats{"self": stats}); got != 16 {
		t.Fatalf("online CPUs: %d", got)
	}
	stats.CPU.OnlineCPUs = 0
	stats.CPU.Usage.PerCPU = make([]uint64, 8)
	if got := sampler.hostCPUCapacity(context.Background(), map[string]containerResourceStats{"self": stats}); got != 8 {
		t.Fatalf("per-CPU fallback: %d", got)
	}
	if got := sampler.hostCPUCapacity(context.Background(), nil); got != 8 || requests.Load() != 0 {
		t.Fatalf("cached capacity: %d requests=%d", got, requests.Load())
	}
	sampler.cpuCapacityAt = time.Now().Add(-2 * time.Minute)
	if got := sampler.hostCPUCapacity(context.Background(), nil); got != 32 || requests.Load() != 1 {
		t.Fatalf("Engine info: %d requests=%d", got, requests.Load())
	}
	if got := sampler.hostCPUCapacity(context.Background(), nil); got != 32 || requests.Load() != 1 {
		t.Fatal("uncached info request")
	}
	sampler.cpuCapacityAt = time.Now().Add(-2 * time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := sampler.hostCPUCapacity(ctx, nil); got != 0 {
		t.Fatal("fabricated capacity after expired cache and failed refresh")
	}
}

func TestAgentMemoryBreakdownDistinguishesCacheAndAnonymousMemory(t *testing.T) {
	for _, test := range []struct {
		name  string
		stats map[string]uint64
		want  map[string]uint64
	}{
		{"v2", map[string]uint64{"anon": 600, "file": 300, "inactive_file": 200, "kernel": 100, "slab": 80, "slab_reclaimable": 50, "slab_unreclaimable": 30}, map[string]uint64{"usage": 1000, "working_set": 800, "anonymous": 600, "file": 300, "inactive_file": 200, "kernel": 100, "slab": 80, "slab_reclaimable": 50, "slab_unreclaimable": 30}},
		{"v1", map[string]uint64{"total_rss": 600, "rss": 1, "total_cache": 300, "cache": 2, "total_inactive_file": 200, "inactive_file": 3}, map[string]uint64{"usage": 1000, "working_set": 800, "anonymous": 600, "file": 300, "inactive_file": 200}},
		{"absent", nil, map[string]uint64{"usage": 1000, "working_set": 1000}},
		{"inconsistent_cache", map[string]uint64{"inactive_file": 2000}, map[string]uint64{"usage": 1000, "working_set": 1000, "inactive_file": 2000}},
	} {
		t.Run(test.name, func(t *testing.T) {
			value := containerResourceStats{}
			usage := uint64(1000)
			value.Memory.Usage = &usage
			value.Memory.Stats = test.stats
			if got := agentMemoryBreakdown(value); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("memory breakdown=%v want=%v", got, test.want)
			}
		})
	}
	if got := agentMemoryBreakdown(containerResourceStats{}); got != nil {
		t.Fatal("unavailable memory reported as measured zero")
	}
}
