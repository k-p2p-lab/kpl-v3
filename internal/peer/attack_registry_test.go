package peer

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

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
