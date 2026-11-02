package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

type cancelJoinSampleSource struct {
	rand.Source
	cancel context.CancelFunc
	calls  int
	after  int
}

func (s *cancelJoinSampleSource) Int63() int64 {
	s.calls++
	if s.calls == s.after {
		s.cancel()
	}
	return s.Source.Int63()
}

func TestJoinPreparationStopsSamplingOnCancellation(t *testing.T) {
	for _, after := range []int{100, 5100} {
		t.Run(fmt.Sprint(after), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			source := &cancelJoinSampleSource{Source: rand.NewSource(42), cancel: cancel, after: after}
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			phase := scenario.Phase{Action: "join", Group: "workers", Count: 5000,
				Interval: scenario.Distribution{Model: "pareto", XM: "1ns", Alpha: 2.5, Max: "1s"}}
			if err := phase.Interval.Validate(false); err != nil {
				t.Fatal(err)
			}
			err := s.runJoin(ctx, "run", 1, phase, rand.New(source))
			if !errors.Is(err, context.Canceled) || source.calls != after {
				t.Fatalf("join kept preparing canceled work: calls=%d want=%d err=%v", source.calls, after, err)
			}
			if len(s.state.reservations) != 0 {
				t.Fatal("canceled preparation reserved Agent capacity")
			}
		})
	}
}

func TestJoinPreparationPreservesSeedLifetimeIDsAndRNGState(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		t.Run(fmt.Sprint(parallel), func(t *testing.T) {
			var mu sync.Mutex
			var requests []model.CreateNodeRequest
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request model.CreateNodeRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
				}
				mu.Lock()
				requests = append(requests, request)
				mu.Unlock()
				writeJSON(w, http.StatusCreated, model.Node{ID: request.ID, State: model.NodeStarting})
			}))
			defer endpoint.Close()
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			if _, err := s.state.registerAgent(model.Agent{ID: "a", URL: endpoint.URL, Capacity: 100}); err != nil {
				t.Fatal(err)
			}
			s.nodeSeq.Store(100)
			config, ok := model.BuiltInNodeConfig("full")
			if !ok {
				t.Fatal("full preset not found")
			}
			config.Sink = &model.ProbabilityAttack{}
			config.GossipSub.Score = &model.PeerScoreConfig{Topics: map[string]model.TopicScoreConfig{"kpl/default": {}}}
			beforeConfig, err := json.Marshal(config)
			if err != nil {
				t.Fatal(err)
			}
			phase := scenario.Phase{Action: "join", Group: "workers", Role: "worker", NodeType: "full", Profile: "baseline", Count: 12, Node: config,
				Parallel: parallel, Parallelism: 4,
				Lifetime: scenario.Distribution{Model: "pareto", XM: "1s", Alpha: 2.5, Max: "3s"},
				Interval: scenario.Distribution{Model: "pareto", XM: "1ns", Alpha: 2.5, Max: "10ns"}}
			if err := phase.Lifetime.Validate(false); err != nil {
				t.Fatal(err)
			}
			if err := phase.Interval.Validate(false); err != nil {
				t.Fatal(err)
			}
			rng, oracle := rand.New(rand.NewSource(37)), rand.New(rand.NewSource(37))
			want := make([]model.CreateNodeRequest, phase.Count)
			for i := range want {
				lifetime := phase.Lifetime.Sample(oracle).String()
				want[i] = model.CreateNodeRequest{ID: fmt.Sprintf("run-workers-%05d", 101+i), RunID: "run", Generation: 3,
					Group: phase.Group, Role: phase.Role, Type: phase.NodeType, Profile: phase.Profile, Seed: oracle.Int63(), Config: config.WithDefaults(), Lifetime: lifetime}
			}
			for range want {
				phase.Interval.Sample(oracle)
			}
			if err := s.runJoin(t.Context(), "run", 3, phase, rng); err != nil {
				t.Fatal(err)
			}
			// Canonicalize the expected requests through the same wire representation.
			data, err := json.Marshal(want)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(data, &want); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			sort.Slice(requests, func(i, j int) bool { return requests[i].ID < requests[j].ID })
			if !reflect.DeepEqual(requests, want) || rng.Int63() != oracle.Int63() || s.nodeSeq.Load() != 112 {
				t.Fatal("join preparation changed request identity, sampling order, or RNG state for later repetitions")
			}
			afterConfig, err := json.Marshal(config)
			if err != nil || !bytes.Equal(beforeConfig, afterConfig) {
				t.Fatal("concurrent request construction mutated the shared profile")
			}
		})
	}
}

func BenchmarkJoinPreparationBeforeRejectedAdmission(b *testing.B) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "creation rejected", http.StatusBadRequest)
	}))
	defer endpoint.Close()
	s := New(ServerConfig{DataDir: b.TempDir()}, nil)
	if _, err := s.state.registerAgent(model.Agent{ID: "a", URL: endpoint.URL, Capacity: 100}); err != nil {
		b.Fatal(err)
	}
	config, ok := model.BuiltInNodeConfig("full")
	if !ok {
		b.Fatal("full preset not found")
	}
	phase := scenario.Phase{Action: "join", Group: "workers", Role: "worker", NodeType: "full", Count: 5000, Node: config,
		Lifetime: scenario.Distribution{Model: "pareto", XM: "1s", Alpha: 2.5, Max: "3s"}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.runJoin(context.Background(), "run", 1, phase, rand.New(rand.NewSource(42))); err == nil {
			b.Fatal("fixture must reject the first dispatch")
		}
	}
	b.StopTimer()
}
