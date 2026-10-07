package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// ServiceMonitor runs independently of Agent registration, availability and
// scheduling. Only this process has Docker access; the web Controller does not.
type ServiceMonitorConfig struct {
	DockerSocket, ControllerURL, Token, Stack, NodeID, NodeName, ControlNodeID string
}
type serviceSampler struct {
	docker *resourceSampler
	config ServiceMonitorConfig
	known  map[string]bool
}

func (s *serviceSampler) sample(ctx context.Context) model.ServiceResourceReport {
	ctx, cancel := context.WithTimeout(ctx, resourceTimeout)
	defer cancel()
	report := model.ServiceResourceReport{NodeID: s.config.NodeID, NodeName: s.config.NodeName, Services: []model.ServiceResources{}}
	sampledAt := time.Now().UTC()
	filters, _ := json.Marshal(map[string][]string{"label": {"com.docker.stack.namespace=" + s.config.Stack, "io.kpl.resource-monitor=true"}})
	var containers []struct {
		ID     string `json:"Id"`
		State  string
		Labels map[string]string
	}
	err := s.docker.get(ctx, "/containers/json?filters="+url.QueryEscape(string(filters)), &containers)
	groups := map[string][]string{}
	ids := []string{}
	seen := map[string]bool{}
	if len(containers) > 512 {
		err = errors.New("service container inventory exceeds limit")
	}
	if err == nil {
		for _, c := range containers {
			if c.State != "running" || c.Labels["com.docker.stack.namespace"] != s.config.Stack || c.Labels["io.kpl.resource-monitor"] != "true" || !dockerContainerID.MatchString(c.ID) || seen[c.ID] {
				continue
			}
			full := c.Labels["com.docker.swarm.service.name"]
			if c.Labels["io.kpl.network-gateway"] == s.config.Stack {
				full = s.config.Stack + "_network-gateway"
			}
			if !strings.HasPrefix(full, s.config.Stack+"_") {
				continue
			}
			name := strings.TrimPrefix(full, s.config.Stack+"_")
			// Agents and Peers already have their own collector, even if mislabeled.
			if name == "" || name == "agent" || c.Labels["io.kpl.managed"] == "true" {
				continue
			}
			if !s.known[name] && len(s.known) >= 64 {
				continue
			}
			s.known[name] = true
			seen[c.ID] = true
			groups[name] = append(groups[name], c.ID)
			ids = append(ids, c.ID)
		}
	} else {
		report.Error = "Service container inventory unavailable"
	}
	current := s.docker.readStats(ctx, ids)
	newIDs := []string{}
	for id, value := range current {
		if _, ok := resourceCPU(s.docker.previous[id], value); !ok {
			s.docker.previous[id] = value
			newIDs = append(newIDs, id)
		}
	}
	if len(newIDs) > 0 {
		timer := time.NewTimer(s.docker.warmup)
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
		timer.Stop()
		for _, id := range newIDs {
			delete(current, id)
		}
		for id, value := range s.docker.readStats(ctx, newIDs) {
			current[id] = value
		}
	}
	capacity := 0
	if len(ids) > 0 {
		capacity = s.docker.hostCPUCapacity(ctx, current)
	}
	for name := range s.known {
		item := model.ServiceResources{Service: name, State: "not_running"}
		if err != nil {
			item.State = "unknown"
		}
		if group := groups[name]; len(group) > 0 {
			item.State = "running"
			r := &model.AgentResources{SampledAt: sampledAt, CPUCapacityCores: capacity, Containers: len(group)}
			var cpu float64
			var used, working uint64
			for _, id := range group {
				value, exists := current[id]
				if !exists {
					continue
				}
				cores, ok := resourceCPU(s.docker.previous[id], value)
				if !ok {
					continue
				}
				cache, ok := value.Memory.Stats["total_inactive_file"]
				if !ok {
					cache = value.Memory.Stats["inactive_file"]
				}
				memory := *value.Memory.Usage
				used += memory
				working += memory
				if cache < memory {
					working -= cache
				}
				cpu += cores
				r.MeasuredContainers++
			}
			r.Complete = r.MeasuredContainers == r.Containers && ctx.Err() == nil
			if r.MeasuredContainers > 0 {
				r.CPUCores = &cpu
				r.MemoryUsageBytes = &used
				r.MemoryWorkingSetBytes = &working
			}
			if !r.Complete {
				r.Error = "Some service containers could not be measured; available samples are shown"
			}
			item.Resources = r
		}
		report.Services = append(report.Services, item)
	}
	s.docker.previous = current
	sort.Slice(report.Services, func(i, j int) bool { return report.Services[i].Service < report.Services[j].Service })
	report.ReportedAt = time.Now().UTC()
	return report
}

func RunServiceMonitor(ctx context.Context, config ServiceMonitorConfig, logger *slog.Logger) error {
	target, err := url.Parse(config.ControllerURL)
	if err != nil || target.Host == "" || target.User != nil || target.RawQuery != "" || target.Fragment != "" || (target.Scheme != "http" && target.Scheme != "https") || config.Token == "" || config.Stack == "" || config.NodeID == "" {
		return errors.New("resource-monitor requires a Controller URL, authentication, stack and node ID")
	}
	target.Path = strings.TrimRight(target.Path, "/") + "/api/v1/services/resources/report"
	sampler := &serviceSampler{docker: newResourceSampler(Config{DockerSocket: config.DockerSocket}), config: config, known: map[string]bool{}}
	if config.NodeID == config.ControlNodeID {
		for _, name := range []string{"controller", "prometheus", "grafana"} {
			sampler.known[name] = true
		}
	}
	defer sampler.docker.client.CloseIdleConnections()
	client := &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	ticker := time.NewTicker(resourceInterval)
	defer ticker.Stop()
	logger.Info("service resource monitor started", "node", config.NodeID, "stack", config.Stack)
	for ctx.Err() == nil {
		report := sampler.sample(ctx)
		if ctx.Err() != nil {
			break
		}
		data, _ := json.Marshal(report)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), bytes.NewReader(data))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+config.Token)
		req.Header.Set("Content-Type", "application/json")
		response, err := client.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
			response.Body.Close()
			if response.StatusCode != http.StatusNoContent {
				err = fmt.Errorf("Controller returned HTTP %d", response.StatusCode)
			}
		}
		if err != nil && ctx.Err() == nil {
			logger.Warn("service resource report failed; retrying", "node", config.NodeID, "error", err)
		}
		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}
	return nil
}
