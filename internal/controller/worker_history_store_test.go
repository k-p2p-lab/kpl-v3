package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	bolt "go.etcd.io/bbolt"
)

func historyTestNode(id string) model.Node {
	return model.Node{ID: id, AgentID: "agent", RunID: "legacy-run", Generation: 2, Group: "workers", Role: "worker", Type: "full", Profile: "original-profile", PeerID: "original-peer", State: model.NodeStopped,
		Addresses: []string{"original-address"}, ConnectedPeers: []string{"connected"}, RoutingPeers: []string{"routing"}, MeshPeers: map[string][]string{"topic": {"mesh"}}, TopicPeers: map[string]int{"topic": 2}, PeerScores: map[string]float64{"connected": 0.25}, ScoreSample: &model.PeerScoreSample{},
		Metadata: map[string]string{"cleanupComplete": "true", "topicsJSON": "[\"topic\",\"other\",\"topic\"]", "annotation": "original"}, Error: "original evidence", StartedAt: time.Unix(123, 0).UTC(), LastSeen: time.Unix(456, 0).UTC(), OverlayObservedAt: time.Unix(400, 0).UTC()}
}

func historyTestReconcile(t *testing.T, h *terminalNodeHistory, nodes ...model.Node) map[string]model.Node {
	t.Helper()
	cold := map[string]bool{}
	for _, node := range nodes {
		cold[node.ID] = true
	}
	result, err := h.reconcile(nodes, cold)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func historyTestCounts(t *testing.T, h *terminalNodeHistory, exclude map[string]bool) map[nodeMetricLabels]int {
	t.Helper()
	result := map[nodeMetricLabels]int{}
	if err := h.counts(exclude, func(labels nodeMetricLabels, count int) error { result[labels] = count; return nil }); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestTerminalHistoryPreservesOriginalNodeAndRejectsResurrection(t *testing.T) {
	directory := t.TempDir()
	h := newTerminalNodeHistory(directory)
	original := historyTestNode("node")
	frozen := historyTestReconcile(t, h, original)
	if !reflect.DeepEqual(frozen[original.ID], original) {
		t.Fatalf("history stripped original evidence: %+v", frozen)
	}
	// Mutating either the input or returned value cannot mutate durable evidence.
	frozen[original.ID].Metadata["annotation"] = "mutated-return"
	original.Metadata["annotation"] = "mutated-input"
	live := historyTestNode("node")
	live.State = model.NodeReady
	live.Metadata = nil
	live.PeerID = "new-peer"
	h = newTerminalNodeHistory(directory)
	result, err := h.reconcile([]model.Node{live}, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := historyTestNode("node")
	if !reflect.DeepEqual(result["node"], want) {
		t.Fatalf("live replay changed immutable terminal node: %+v", result)
	}
	for range 3 {
		historyTestReconcile(t, h, want)
	}
	page, after, done, err := h.page(t.Context(), "", time.Time{}, 10)
	if err != nil || !done || after != "node" || len(page) != 1 || !reflect.DeepEqual(page[0], want) {
		t.Fatalf("restart/paging lost evidence: %+v %s %t %v", page, after, done, err)
	}
	counts := historyTestCounts(t, h, nil)
	if len(counts) != 1 || counts[terminalHistoryLabels(want)] != 1 {
		t.Fatalf("duplicate replay inflated counts: %+v", counts)
	}
	topics := map[string]bool{}
	if err := h.propagation(func(run, agent, topic string) error {
		if run != want.RunID || agent != want.AgentID {
			t.Fatal("propagation owner changed")
		}
		topics[topic] = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(topics) != 2 || !topics["topic"] || !topics["other"] {
		t.Fatalf("topic baselines were lost or duplicated: %v", topics)
	}
	if _, err := os.Stat(filepath.Join(directory, retiredRuntimeDirectory, want.RunID+".done")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("individual history invented Run retirement: %v", err)
	}
}

func TestTerminalHistoryBatchOwnershipAndValidationAreAtomic(t *testing.T) {
	h := newTerminalNodeHistory(t.TempDir())
	known := historyTestNode("known")
	historyTestReconcile(t, h, known)
	for _, mode := range []string{"owner", "run", "duplicate-owner", "unconfirmed", "record-limit", "key-limit", "aggregate-key-limit"} {
		t.Run(mode, func(t *testing.T) {
			fresh := historyTestNode("fresh")
			bad := historyTestNode("known")
			switch mode {
			case "owner":
				bad.AgentID = "other-agent"
			case "run":
				bad.RunID = "other-run"
			case "duplicate-owner":
				bad = historyTestNode("fresh")
				bad.AgentID = "other-agent"
			case "unconfirmed":
				bad = historyTestNode("bad")
				delete(bad.Metadata, "cleanupComplete")
			case "record-limit":
				bad = historyTestNode("bad")
				bad.Metadata["payload"] = strings.Repeat("x", terminalHistoryRecordLimit)
			case "key-limit":
				bad = historyTestNode(strings.Repeat("x", bolt.MaxKeySize+1))
			case "aggregate-key-limit":
				bad = historyTestNode("bad")
				bad.Group = strings.Repeat("x", bolt.MaxKeySize)
			}
			if result, err := h.reconcile([]model.Node{fresh, bad}, map[string]bool{fresh.ID: true, bad.ID: true}); err == nil || result != nil {
				t.Fatalf("contradictory/invalid batch was acknowledged: %v %+v", err, result)
			}
			page, _, done, err := h.page(t.Context(), "", time.Time{}, 10)
			if err != nil || !done || len(page) != 1 || page[0].ID != "known" {
				t.Fatalf("failed batch partially persisted: %+v %v", page, err)
			}
			if count := historyTestCounts(t, h, nil)[terminalHistoryLabels(known)]; count != 1 {
				t.Fatalf("failed batch changed aggregates: %d", count)
			}
		})
	}
}

func TestTerminalHistoryPagingFreezesCutoffAndBoundsPages(t *testing.T) {
	h := newTerminalNodeHistory(t.TempDir())
	historyTestReconcile(t, h, historyTestNode("a"), historyTestNode("c"), historyTestNode("e"))
	asOf := time.Now().UTC()
	historyTestReconcile(t, h, historyTestNode("b"), historyTestNode("d"))
	after := ""
	var ids []string
	for pages := 0; ; pages++ {
		if pages > 8 {
			t.Fatal("filtered page did not advance")
		}
		nodes, last, done, err := h.page(t.Context(), after, asOf, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(nodes) > 1 || last <= after {
			t.Fatalf("invalid page progress: %q -> %q %+v", after, last, nodes)
		}
		for _, node := range nodes {
			ids = append(ids, node.ID)
		}
		after = last
		if done {
			break
		}
	}
	if !reflect.DeepEqual(ids, []string{"a", "c", "e"}) {
		t.Fatalf("cutoff leaked new records: %v", ids)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, _, err := h.page(ctx, "", asOf, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled page returned evidence: %v", err)
	}
	for _, mode := range []string{"count", "bytes"} {
		t.Run(mode, func(t *testing.T) {
			h := newTerminalNodeHistory(t.TempDir())
			n := terminalHistoryPageNodes + 1
			if mode == "bytes" {
				n = 8
			}
			var incoming []model.Node
			for i := range n {
				node := historyTestNode(fmt.Sprintf("node-%04d", i))
				if mode == "bytes" {
					node.Metadata["payload"] = strings.Repeat("x", 700<<10)
				}
				incoming = append(incoming, node)
			}
			historyTestReconcile(t, h, incoming...)
			first, last, done, err := h.page(t.Context(), "", time.Time{}, 100000)
			if err != nil || done || len(first) < 1 || len(first) > terminalHistoryPageNodes {
				t.Fatalf("unbounded first page: n=%d done=%t err=%v", len(first), done, err)
			}
			if mode == "bytes" && len(first) > 5 {
				t.Fatalf("page exceeded its raw byte budget: %d", len(first))
			}
			second, _, done, err := h.page(t.Context(), last, time.Time{}, 100000)
			if err != nil || !done || len(first)+len(second) != n {
				t.Fatalf("page boundary lost records: first=%d second=%d done=%t err=%v", len(first), len(second), done, err)
			}
		})
	}
}

func TestTerminalHistoryLargeLegacyNodesRetainFullShapeAcrossPages(t *testing.T) {
	h := newTerminalNodeHistory(t.TempDir())
	originals := map[string]model.Node{}
	for i, size := range []int{2 << 20, 10 << 20} {
		node := historyTestNode(fmt.Sprintf("large-%d", i))
		node.Metadata["payload"] = ""
		base, err := json.Marshal(node)
		if err != nil {
			t.Fatal(err)
		}
		node.Metadata["payload"] = strings.Repeat("x", size-len(base))
		encoded, err := json.Marshal(node)
		if err != nil || len(encoded) != size {
			t.Fatalf("legacy fixture has wrong wire size: %d %v", len(encoded), err)
		}
		originals[node.ID] = node
		historyTestReconcile(t, h, node)
	}
	tail := historyTestNode("tail")
	originals[tail.ID] = tail
	historyTestReconcile(t, h, tail)
	after := ""
	for i, want := range []string{"large-0", "large-1", "tail"} {
		page, last, done, err := h.page(t.Context(), after, time.Time{}, terminalHistoryPageNodes)
		if err != nil || len(page) != 1 || page[0].ID != want || !reflect.DeepEqual(page[0], originals[want]) || done != (i == 2) {
			t.Fatalf("large original was truncated or paging failed: ids=%v last=%s done=%t err=%v", func() []string {
				var ids []string
				for _, node := range page {
					ids = append(ids, node.ID)
				}
				return ids
			}(), last, done, err)
		}
		after = last
	}
}

func TestTerminalHistoryAcceptsMaximumBTreeKeyWithoutPathInterpretation(t *testing.T) {
	h := newTerminalNodeHistory(t.TempDir())
	node := historyTestNode(strings.Repeat("z", bolt.MaxKeySize))
	historyTestReconcile(t, h, node)
	page, last, done, err := h.page(t.Context(), "", time.Time{}, 1)
	if err != nil || len(page) != 1 || page[0].ID != node.ID || last != node.ID || !done {
		t.Fatalf("maximum B-tree key was altered: count=%d done=%t err=%v", len(page), done, err)
	}
}

func TestTerminalHistoryAggregatesAvoidFullNodeReadsAndExcludeHotIDs(t *testing.T) {
	h := newTerminalNodeHistory(t.TempDir())
	a, b, c := historyTestNode("a"), historyTestNode("b"), historyTestNode("c")
	c.Group = "other"
	delete(c.Metadata, "topicsJSON")
	historyTestReconcile(t, h, a, b, c)
	counts := historyTestCounts(t, h, map[string]bool{"a": true, "absent": true})
	if counts[terminalHistoryLabels(a)] != 1 || counts[terminalHistoryLabels(c)] != 1 {
		t.Fatalf("hot/cold transition was counted twice: %v", counts)
	}
	// A damaged unrelated Node must be reported by paging, while aggregate-only
	// scrapes read their own keys without decoding every historical payload.
	root, db, err := h.open(false)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("nodes")).Put([]byte("b"), []byte("broken")) })
	db.Close()
	root.Close()
	if err != nil {
		t.Fatal(err)
	}
	counts = historyTestCounts(t, h, nil)
	if counts[terminalHistoryLabels(a)] != 2 {
		t.Fatal("aggregate query decoded an unrelated damaged Node")
	}
	topics := map[string]bool{}
	if err := h.propagation(func(_, _, topic string) error { topics[topic] = true; return nil }); err != nil {
		t.Fatal(err)
	}
	if !topics["kpl/default"] || len(topics) != 3 {
		t.Fatalf("default or explicit topic baseline missing: %v", topics)
	}
	if _, _, _, err := h.page(t.Context(), "", time.Time{}, 10); err == nil {
		t.Fatal("damaged original evidence was hidden")
	}
	if err := h.counts(map[string]bool{"b": true}, func(nodeMetricLabels, int) error { return nil }); err == nil {
		t.Fatal("invalid excluded Node was silently subtracted")
	}
}

func TestTerminalHistoryQueryCallbacksReleaseDatabaseAndReadersCreateNothing(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "uncreated")
	h := newTerminalNodeHistory(directory)
	if err := h.counts(nil, func(nodeMetricLabels, int) error { t.Fatal("empty store emitted a count"); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := h.propagation(func(string, string, string) error { t.Fatal("empty store emitted a baseline"); return nil }); err != nil {
		t.Fatal(err)
	}
	if nodes, _, done, err := h.page(t.Context(), "", time.Time{}, 1); err != nil || !done || len(nodes) != 0 {
		t.Fatalf("empty history page: %+v %t %v", nodes, done, err)
	}
	if _, err := os.Stat(directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read query created a directory: %v", err)
	}
	historyTestReconcile(t, h, historyTestNode("a"))
	complete := make(chan error, 1)
	go func() {
		complete <- h.counts(nil, func(nodeMetricLabels, int) error {
			_, err := h.reconcile([]model.Node{historyTestNode("b")}, map[string]bool{"b": true})
			return err
		})
	}()
	select {
	case err := <-complete:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("aggregate callback retained a DB lock/descriptor")
	}
	if count := historyTestCounts(t, h, nil)[terminalHistoryLabels(historyTestNode("a"))]; count != 2 {
		t.Fatalf("callback admission was lost: %d", count)
	}
}

func TestTerminalHistoryMultipleInstancesCountDuplicateOnce(t *testing.T) {
	directory := t.TempDir()
	var wg sync.WaitGroup
	failures := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h := newTerminalNodeHistory(directory)
			_, err := h.reconcile([]model.Node{historyTestNode("same")}, map[string]bool{"same": true})
			failures <- err
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	h := newTerminalNodeHistory(directory)
	if count := historyTestCounts(t, h, nil)[terminalHistoryLabels(historyTestNode("same"))]; count != 1 {
		t.Fatalf("concurrent instances duplicated the row: %d", count)
	}
}

func TestTerminalHistoryRejectsUnsafeFileAndUnknownSchema(t *testing.T) {
	for _, mode := range []string{"symlink", "schema"} {
		t.Run(mode, func(t *testing.T) {
			directory := t.TempDir()
			h := newTerminalNodeHistory(directory)
			if mode == "symlink" {
				target := filepath.Join(t.TempDir(), "untouched")
				if err := os.WriteFile(target, []byte("unchanged"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, filepath.Join(directory, terminalHistoryFile)); err != nil {
					t.Fatal(err)
				}
			} else {
				historyTestReconcile(t, h, historyTestNode("a"))
				root, db, err := h.open(false)
				if err != nil {
					t.Fatal(err)
				}
				err = db.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("meta")).Put([]byte("version"), []byte{2}) })
				db.Close()
				root.Close()
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := h.reconcile([]model.Node{historyTestNode("new")}, map[string]bool{"new": true}); err == nil {
				t.Fatal("unsafe storage accepted terminal evidence")
			}
			if _, _, _, err := h.page(t.Context(), "", time.Time{}, 10); err == nil {
				t.Fatal("unsafe storage returned history")
			}
		})
	}
}
