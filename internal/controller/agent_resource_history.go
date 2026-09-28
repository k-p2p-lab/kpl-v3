package controller

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

type resourceHistoryPoint struct {
	At    time.Time `json:"at"`
	Value float64   `json:"value"`
}
type resourceHistorySeries struct {
	AgentID string                 `json:"agentId"`
	Metric  string                 `json:"metric"`
	Unit    string                 `json:"unit"`
	Samples []resourceHistoryPoint `json:"samples"`
	Mean    float64                `json:"mean"`
	Min     float64                `json:"min"`
	Max     float64                `json:"max"`
}
type resourceHistory struct {
	From   time.Time               `json:"from"`
	To     time.Time               `json:"to"`
	Scope  string                  `json:"scope"`
	Series []resourceHistorySeries `json:"series"`
}

// Read raw Prometheus range-vector samples so peaks and gaps are retained.
// History stays in the existing TSDB, not SSE, Agent memory or the NAS runs tree.
func (s *Server) agentResourceHistory(ctx context.Context, window time.Duration, to time.Time) (resourceHistory, error) {
	result := resourceHistory{From: to.Add(-window), To: to, Scope: "agent_and_peers", Series: []resourceHistorySeries{}}
	base, err := url.Parse(s.config.PrometheusURL)
	if err != nil || base.Host == "" || (base.Scheme != "http" && base.Scheme != "https") || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return result, errors.New("Configure a valid Controller Prometheus query URL")
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/api/v1/query"
	query := `{job="kpl-controller",__name__=~"kpl_agent_cpu_usage_percent|kpl_agent_memory_working_set_bytes|kpl_agent_memory_usage_bytes|kpl_agent_resource_containers"}[` + strconv.Itoa(max(1, int(math.Ceil(window.Seconds())))) + `s]`
	params := url.Values{"query": {query}, "time": {fmt.Sprintf("%d.%09d", to.Unix(), to.Nanosecond())}, "timeout": {"12s"}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base.String(), strings.NewReader(params.Encode()))
	if err != nil {
		return result, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	// Do not forward Dashboard cookies or the KPL service token to Prometheus.
	client := &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(req)
	if err != nil {
		return result, errors.New("Prometheus history is unavailable; check the Controller Prometheus URL and service")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return result, fmt.Errorf("Prometheus history returned HTTP %d", response.StatusCode)
	}
	const maxBody = 16 << 20
	data, err := io.ReadAll(io.LimitReader(response.Body, maxBody+1))
	if err != nil {
		return result, err
	}
	if len(data) > maxBody {
		return result, errors.New("Resource history is too large; select a shorter period")
	}
	var raw struct {
		Status   string
		Warnings []string
		Data     struct {
			ResultType string
			Result     []struct {
				Metric map[string]string
				Values [][2]json.RawMessage
			}
		}
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return result, errors.New("Invalid Prometheus history response")
	}
	if raw.Status != "success" || raw.Data.ResultType != "matrix" || len(raw.Warnings) > 0 {
		return result, errors.New("Prometheus could not provide complete resource history")
	}
	count := 0
	byKey := map[string]*resourceHistorySeries{}
	for _, entry := range raw.Data.Result {
		id, metric := entry.Metric["agent_id"], entry.Metric["__name__"]
		if id == "" {
			continue
		}
		unit := "bytes"
		switch metric {
		case "kpl_agent_cpu_usage_percent":
			metric, unit = "cpu_percent", "percent_host"
		case "kpl_agent_resource_containers":
			kind := entry.Metric["kind"]
			if kind != "expected" && kind != "measured" {
				continue
			}
			metric, unit = "containers_"+kind, "containers"
		case "kpl_agent_memory_working_set_bytes":
			metric = "memory_working_set_bytes"
		case "kpl_agent_memory_usage_bytes":
			metric = "memory_usage_bytes"
		default:
			continue
		}
		key := id + "\x00" + metric
		series := byKey[key]
		if series == nil {
			series = &resourceHistorySeries{AgentID: id, Metric: metric, Unit: unit, Samples: []resourceHistoryPoint{}}
			byKey[key] = series
		}
		for _, pair := range entry.Values {
			count++
			if count > 250000 {
				return result, errors.New("Resource history is too large; select a shorter period")
			}
			var timestamp float64
			var text string
			if json.Unmarshal(pair[0], &timestamp) != nil || json.Unmarshal(pair[1], &text) != nil || math.IsNaN(timestamp) || math.IsInf(timestamp, 0) {
				return result, errors.New("Invalid Prometheus resource sample")
			}
			value, err := strconv.ParseFloat(text, 64)
			if err != nil {
				return result, errors.New("Invalid Prometheus resource value")
			}
			if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
				continue
			}
			if timestamp < float64(result.From.Unix())-1 || timestamp > float64(result.To.Unix())+1 {
				continue
			}
			// Prometheus stores millisecond timestamps. Preserve exact boundaries
			// instead of introducing nanosecond drift through float multiplication.
			at := time.UnixMilli(int64(math.Round(timestamp * 1000))).UTC()
			if !at.After(result.From) || at.After(result.To) {
				continue
			}
			series.Samples = append(series.Samples, resourceHistoryPoint{At: at, Value: value})
		}
	}
	for _, series := range byKey {
		sort.Slice(series.Samples, func(i, j int) bool { return series.Samples[i].At.Before(series.Samples[j].At) })
		// A scrape target change can leave two historical label sets. Exact
		// duplicate samples are harmless; conflicting samples must not be summed.
		clean := series.Samples[:0]
		for _, p := range series.Samples {
			if len(clean) > 0 && clean[len(clean)-1].At.Equal(p.At) {
				if clean[len(clean)-1].Value != p.Value {
					return result, errors.New("Conflicting resource samples for one Agent; check duplicate Controller scrape targets")
				}
				continue
			}
			clean = append(clean, p)
		}
		series.Samples = clean
		if len(clean) == 0 {
			continue
		}
		series.Min, series.Max = clean[0].Value, clean[0].Value
		for i, p := range clean {
			series.Mean += (p.Value - series.Mean) / float64(i+1)
			series.Min = min(series.Min, p.Value)
			series.Max = max(series.Max, p.Value)
		}
		result.Series = append(result.Series, *series)
	}
	sort.Slice(result.Series, func(i, j int) bool {
		if result.Series[i].AgentID != result.Series[j].AgentID {
			return result.Series[i].AgentID < result.Series[j].AgentID
		}
		return result.Series[i].Metric < result.Series[j].Metric
	})
	return result, nil
}

func (s *Server) handleAgentResourceHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	window, err := time.ParseDuration(r.URL.Query().Get("range"))
	if err != nil || window < time.Minute || window > 24*time.Hour {
		writeError(w, http.StatusBadRequest, "range must be between 1m and 24h")
		return
	}
	to := time.Now().UTC()
	s.serveResourceHistory(w, r, to.Add(-window), to, "kpl-agent-resources")
}

func (s *Server) serveResourceHistory(w http.ResponseWriter, r *http.Request, from, to time.Time, filename string) {
	format, kind := r.URL.Query().Get("format"), r.URL.Query().Get("kind")
	if format != "" && format != "csv" || kind != "" && kind != "samples" && kind != "summary" {
		writeError(w, http.StatusBadRequest, "Unsupported resource export format")
		return
	}
	select {
	case s.resourceHistorySlots <- struct{}{}:
		defer func() { <-s.resourceHistorySlots }()
	default:
		writeError(w, http.StatusServiceUnavailable, "Resource history exports are busy; retry shortly")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	data, err := s.agentResourceHistory(ctx, to.Sub(from), to)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	if format != "csv" {
		writeJSON(w, http.StatusOK, data)
		return
	}
	if len(data.Series) == 0 {
		writeError(w, http.StatusNotFound, "No KPL resource samples in this period; verify Agent updates and the Controller scrape target")
		return
	}
	if kind == "summary" {
		resourceCSVHeaders(w, filename+"-summary")
	} else {
		resourceCSVHeaders(w, filename+"-history")
	}
	out := csv.NewWriter(w)
	defer out.Flush()
	if kind == "summary" {
		_ = out.Write([]string{"agent_id", "scope", "metric", "unit", "from_utc", "to_utc", "first_sample_utc", "last_sample_utc", "samples", "sample_mean", "min", "max"})
		for _, series := range data.Series {
			if err := out.Write([]string{resourceCSVCell(series.AgentID), data.Scope, series.Metric, series.Unit, data.From.Format(time.RFC3339Nano), data.To.Format(time.RFC3339Nano), series.Samples[0].At.Format(time.RFC3339Nano), series.Samples[len(series.Samples)-1].At.Format(time.RFC3339Nano), strconv.Itoa(len(series.Samples)), csvFloat(series.Mean), csvFloat(series.Min), csvFloat(series.Max)}); err != nil {
				return
			}
		}
	} else {
		_ = out.Write([]string{"agent_id", "scope", "metric", "unit", "sampled_at_utc", "value"})
		for _, series := range data.Series {
			for _, p := range series.Samples {
				if err := out.Write([]string{resourceCSVCell(series.AgentID), data.Scope, series.Metric, series.Unit, p.At.Format(time.RFC3339Nano), csvFloat(p.Value)}); err != nil {
					return
				}
			}
		}
	}
}
