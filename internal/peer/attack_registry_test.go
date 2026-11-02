package peer

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestAttackRegistryResponseSizeAndTrailingData(t *testing.T) {
	const limit = 8 << 20
	for _, test := range []struct {
		name      string
		body      string
		wantError bool
	}{
		{"exact-limit", `[]` + strings.Repeat(" ", limit-2), false},
		{"one-byte-over", `[]` + strings.Repeat(" ", limit-1), true},
		{"hidden-second-response", `[]` + strings.Repeat(" ", limit-2) + `[]`, true},
		{"hidden-garbage", `[]` + strings.Repeat(" ", limit-2) + `invalid`, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(test.body))
			}))
			defer api.Close()
			runtime := newAttackRuntime(&Server{config: model.PeerProcessConfig{ControllerURL: api.URL}})
			previous := newAttackRegistry([]model.AttackTarget{{NodeID: "preserved"}}, time.Now())
			runtime.registry.Store(previous)
			err := runtime.fetchTargets(t.Context())
			if (err != nil) != test.wantError {
				t.Errorf("response size=%d, error=%v; wantError=%t", len(test.body), err, test.wantError)
			}
			if test.wantError && runtime.registry.Load() != previous {
				t.Error("invalid response replaced the last valid registry")
			}
		})
	}
}

func TestAttackRegistryRefreshAndPreparationDoNotCross(t *testing.T) {
	// The periodic refresh can overlap a censorship prepare request. Hold the
	// older response until the second caller has exhausted its request context;
	// it must leave the queue without issuing another request or publishing a
	// newer registry that the delayed response could then overwrite.
	var calls atomic.Int32
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		call := calls.Add(1)
		if call == 1 {
			close(firstStarted)
			select {
			case <-releaseFirst:
			case <-request.Context().Done():
				return
			}
		}
		_, _ = fmt.Fprintf(w, `[{"nodeId":"snapshot-%d","peerId":"transport"}]`, call)
	}))
	defer api.Close()
	defer func() {
		select {
		case <-releaseFirst:
		default:
			close(releaseFirst)
		}
	}()
	runtime := newAttackRuntime(&Server{config: model.PeerProcessConfig{ControllerURL: api.URL}})
	firstDone := make(chan error, 1)
	go func() { firstDone <- runtime.fetchTargets(t.Context()) }()
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first registry request did not start")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	err := runtime.fetchTargets(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("concurrent registry request did not wait for the earlier snapshot: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("canceled registry caller issued an overlapping HTTP request: calls=%d", got)
	}
	beforeRelease := runtime.registry.Load()
	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if beforeRelease != nil && beforeRelease.targets[0].NodeID != runtime.registry.Load().targets[0].NodeID {
		t.Errorf("delayed older response replaced newer snapshot %q with %q", beforeRelease.targets[0].NodeID, runtime.registry.Load().targets[0].NodeID)
	}
	if err := runtime.fetchTargets(t.Context()); err != nil {
		t.Fatal(err)
	}
	registry := runtime.registry.Load()
	if registry == nil || len(registry.targets) != 1 || registry.targets[0].NodeID != "snapshot-2" {
		t.Fatalf("refresh did not resume after cancellation: %+v", registry)
	}
}

func TestAttackRegistryFetchIncludesCallerGeneration(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Query().Get("runId") != "run" || request.URL.Query().Get("requesterNodeId") != "attacker" || request.URL.Query().Get("generation") != "7" {
			t.Errorf("lost caller scope: %s", request.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer api.Close()
	server := &Server{config: model.PeerProcessConfig{Node: model.Node{ID: "attacker", RunID: "run", Generation: 7}, ControllerURL: api.URL}}
	runtime := newAttackRuntime(server)
	if err := runtime.fetchTargets(t.Context()); err != nil {
		t.Fatal(err)
	}
	if runtime.registry.Load() == nil {
		t.Fatal("scoped registry not accepted")
	}
}

func TestAttackRegistryIndexesPreserveSharedIdentitySelections(t *testing.T) {
	var rows []model.AttackTarget
	for i := 0; i < 1000; i++ {
		rows = append(rows, model.AttackTarget{NodeID: fmt.Sprint(i), Group: fmt.Sprintf("g%d", i%3), PeerID: fmt.Sprintf("transport-%d", i%17), AuthorPeerID: fmt.Sprintf("author-%d", i%23)})
	}
	runtime := &attackRuntime{}
	runtime.registry.Store(newAttackRegistry(rows, time.Now()))
	for _, selector := range []*model.AttackSelector{{Groups: []string{"g0"}}, {Groups: []string{"g0", "g1", "g2"}}, {Groups: []string{"none"}}, {NodeIDs: []string{"0"}}, {Groups: []string{"g0"}, NodeIDs: []string{"0"}}, {Groups: []string{"g0"}, PeerIDs: []string{"author-0"}}} {
		for _, author := range []bool{false, true} {
			for i := 0; i < 25; i++ {
				id := fmt.Sprintf("transport-%d", i)
				lookup := runtime.matchTransport
				if author {
					id, lookup = fmt.Sprintf("author-%d", i), runtime.matchPublisher
				}
				selected, excluded := false, false
				for _, row := range rows {
					candidate := row.PeerID
					if author {
						candidate = row.AuthorPeerID
					}
					if candidate != id {
						continue
					}
					if selector.Matches(row.Group, row.NodeID, id) {
						selected = true
					} else {
						excluded = true
					}
				}
				wantMatch, wantKnown := selected && !excluded, (selected || excluded) && !(selected && excluded)
				if match, known := lookup(selector, id); match != wantMatch || known != wantKnown {
					t.Fatalf("author=%t id=%s selector=%+v got=%t/%t want=%t/%t", author, id, selector, match, known, wantMatch, wantKnown)
				}
			}
		}
	}
}
