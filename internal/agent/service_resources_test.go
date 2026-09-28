package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestServiceResourcesIsolateStackAndPeersAndPreservePartialSamples(t *testing.T) {
	id := func(i int) string { return fmt.Sprintf("%064x", i) }
	var mu sync.Mutex
	reads := map[string]int{}
	phase := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Method != "GET" {
			t.Error("Docker mutation attempted")
		}
		if r.URL.Path == "/containers/json" {
			if !strings.Contains(r.URL.Query().Get("filters"), "io.kpl.resource-monitor=true") {
				t.Error("unbounded Docker inventory")
			}
			if phase == 3 {
				w.WriteHeader(503)
				return
			}
			inventory := []any{}
			for i := 1; i <= 8; i++ {
				name, stack, optin, managed, state := "prometheus", "lab", "true", "", "running"
				switch i {
				case 1:
					name = "controller"
				case 3:
					stack = "other"
				case 4:
					optin = "false"
				case 5:
					name = "agent"
				case 6:
					managed = "true"
				case 7:
					state = "exited"
				}
				if phase == 2 && (i == 2 || i == 8) {
					continue
				}
				inventory = append(inventory, map[string]any{"Id": id(i), "State": state, "Labels": map[string]string{"com.docker.stack.namespace": stack, "com.docker.swarm.service.name": stack + "_" + name, "io.kpl.resource-monitor": optin, "io.kpl.managed": managed}})
			}
			json.NewEncoder(w).Encode(inventory)
			return
		}
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) != 4 || parts[3] != "stats" {
			t.Errorf("unexpected Docker path %s", r.URL.Path)
			w.WriteHeader(404)
			return
		}
		container := parts[2]
		if container != id(1) && container != id(2) && container != id(8) {
			t.Errorf("sampled unrelated container %s", container)
		}
		if r.URL.Query().Get("stream") != "false" || r.URL.Query().Get("one-shot") != "true" {
			t.Error("blocking/streaming stats enabled")
		}
		reads[container]++
		n := reads[container]
		if phase == 1 && container == id(8) {
			w.WriteHeader(404)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"read": time.Unix(1000+int64(n), 0), "cpu_stats": map[string]any{"online_cpus": 8, "cpu_usage": map[string]any{"total_usage": uint64(n) * 500000000}}, "memory_stats": map[string]any{"usage": 1000, "stats": map[string]uint64{"inactive_file": 200}}})
	}))
	defer server.Close()
	sampler := &serviceSampler{docker: &resourceSampler{client: server.Client(), baseURL: server.URL, previous: map[string]containerResourceStats{}}, config: ServiceMonitorConfig{Stack: "lab", NodeID: "node1", NodeName: "manager"}, known: map[string]bool{"grafana": true}}
	lookup := func(report model.ServiceResourceReport, name string) model.ServiceResources {
		t.Helper()
		for _, row := range report.Services {
			if row.Service == name {
				return row
			}
		}
		t.Fatalf("missing service %s", name)
		return model.ServiceResources{}
	}
	first := sampler.sample(context.Background())
	if len(first.Services) != 3 {
		t.Fatalf("unrelated services: %+v", first)
	}
	prom := lookup(first, "prometheus")
	if prom.State != "running" || !prom.Resources.Valid() || !prom.Resources.Complete || *prom.Resources.CPUCores != 1 || *prom.Resources.MemoryWorkingSetBytes != 1600 {
		t.Fatalf("prometheus: %+v", prom)
	}
	if p, ok := prom.Resources.CPUPercent(); !ok || p != 12.5 {
		t.Fatalf("host percentage %v %v", p, ok)
	}
	if row := lookup(first, "grafana"); row.State != "not_running" || row.Resources != nil {
		t.Fatalf("missing service fabricated: %+v", row)
	}
	mu.Lock()
	phase = 1
	mu.Unlock()
	partial := lookup(sampler.sample(context.Background()), "prometheus")
	if !partial.Resources.Valid() || partial.Resources.Complete || partial.Resources.MeasuredContainers != 1 || partial.Resources.Containers != 2 || *partial.Resources.MemoryWorkingSetBytes != 800 {
		t.Fatalf("partial unavailable: %+v", partial.Resources)
	}
	mu.Lock()
	phase = 2
	mu.Unlock()
	stopped := lookup(sampler.sample(context.Background()), "prometheus")
	if stopped.State != "not_running" || stopped.Resources != nil {
		t.Fatalf("stopped service reused CPU: %+v", stopped)
	}
	if len(sampler.docker.previous) != 1 {
		t.Fatal("retained removed container counters")
	}
	mu.Lock()
	phase = 3
	mu.Unlock()
	failed := sampler.sample(context.Background())
	if failed.Error == "" {
		t.Fatal("inventory failure hidden")
	}
	for _, row := range failed.Services {
		if row.State != "unknown" || row.Resources != nil {
			t.Fatalf("failed inventory became zero or running: %+v", row)
		}
	}
	if len(sampler.docker.previous) != 0 {
		t.Fatal("retained counters after inventory failure")
	}
}
