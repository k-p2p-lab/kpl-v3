package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	bolt "go.etcd.io/bbolt"
)

func historyTestNodes(count int) []model.Node {
	nodes := make([]model.Node, count)
	for i := range nodes {
		nodes[i] = historyTerminal(fmt.Sprintf("peer-%05d", i), "run", time.Unix(int64(i+1), 0)).node
	}
	return nodes
}

func legacyHistoryFixture(t *testing.T, count int) (string, *agentHistory, []model.Node) {
	t.Helper()
	dir := t.TempDir()
	h, err := openAgentHistory(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.close() })
	if err := os.MkdirAll(filepath.Join(h.directory, "nodes"), 0700); err != nil {
		t.Fatal(err)
	}
	nodes := historyTestNodes(count)
	for _, node := range nodes {
		data, _ := json.Marshal(node)
		if err := os.WriteFile(h.path("nodes", node.ID), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	// Production legacy retirement writes the run index before its first Peer.
	if err := os.WriteFile(h.path("runs", "run"), []byte(`"run"`), 0600); err != nil {
		t.Fatal(err)
	}
	return dir, h, nodes
}

func TestHistoryStoreMigrationPreservesIdentitiesReplayAndRollbackGuard(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("legacy conversion uses Linux atomic rename exchange")
	}
	dir, h, nodes := legacyHistoryFixture(t, 300)
	// An unfinished pre-install copy must be replaced from the intact source.
	staging := filepath.Join(h.directory, "nodes.migrating")
	if err := os.WriteFile(staging, []byte("interrupted copy"), 0600); err != nil {
		t.Fatal(err)
	}
	if found, err := h.contains(nodes[0].ID); err != nil || !found {
		t.Fatalf("migration: found=%v err=%v", found, err)
	}
	info, err := os.Stat(filepath.Join(h.directory, "nodes"))
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		t.Fatalf("store format/permissions: %v %v", info, err)
	}
	if err := os.MkdirAll(filepath.Join(h.directory, "nodes"), 0700); err == nil {
		t.Fatal("older Agent could silently ignore new identities")
	}
	if err := h.close(); err != nil {
		t.Fatal(err)
	}
	h, err = openAgentHistory(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.close() })
	seen, pages := map[string]bool{}, 0
	var last []byte
	for {
		page, next, eof, err := h.replayPage(context.Background(), last)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) > historyPruneBatch {
			t.Fatal("unbounded replay page")
		}
		for _, node := range page {
			if seen[node.ID] {
				t.Fatalf("duplicate replay: %s", node.ID)
			}
			seen[node.ID] = true
		}
		pages++
		last = next
		if eof {
			break
		}
	}
	if len(seen) != len(nodes) || pages != 3 {
		t.Fatalf("lost history on reopen: nodes=%d pages=%d", len(seen), pages)
	}
	if err := h.cleanLegacyNodes(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(staging)
	if err != nil || len(entries) != len(nodes)-historyPruneBatch {
		t.Fatalf("cleanup was unbounded: %d %v", len(entries), err)
	}
	for range 4 {
		if err := h.cleanLegacyNodes(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(staging); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("legacy inode cleanup incomplete: %v", err)
	}
	for _, node := range nodes {
		if found, err := h.contains(node.ID); err != nil || !found {
			t.Fatalf("cleanup lost identity %s: %v", node.ID, err)
		}
	}
}

func TestHistoryStoreCorruptMigrationLeavesLegacyEvidenceUntouched(t *testing.T) {
	_, h, nodes := legacyHistoryFixture(t, 3)
	corrupt := h.path("nodes", nodes[1].ID)
	if err := os.WriteFile(corrupt, []byte("not JSON"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store(); err == nil {
		t.Fatal("corrupt record accepted")
	}
	info, err := os.Stat(filepath.Join(h.directory, "nodes"))
	if err != nil || !info.IsDir() {
		t.Fatal("failed migration replaced original history")
	}
	for _, node := range nodes {
		if _, err := os.Stat(h.path("nodes", node.ID)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestHistoryStoreCleanupRetainsBackupIfInstalledRecordDiffers(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("legacy conversion uses Linux atomic rename exchange")
	}
	_, h, nodes := legacyHistoryFixture(t, 1)
	if _, err := h.store(); err != nil {
		t.Fatal(err)
	}
	nodes[0].Error = "changed evidence"
	if err := h.saveNodes(nodes); err != nil {
		t.Fatal(err)
	}
	if err := h.cleanLegacyNodes(); err == nil {
		t.Fatal("different backup deleted")
	}
	entries, err := os.ReadDir(filepath.Join(h.directory, "nodes.migrating"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("backup lost: %v %v", entries, err)
	}
}

func TestHistoryStoreRepeatedRunsKeepOnePeerHistoryFile(t *testing.T) {
	h, err := openAgentHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.close()
	const rounds, perRound = 12, 512
	for round := range rounds {
		for first := 0; first < perRound; first += historyPruneBatch {
			nodes := historyTestNodes(historyPruneBatch)
			for i := range nodes {
				nodes[i].ID = fmt.Sprintf("run-%03d-peer-%05d", round, first+i)
				nodes[i].RunID = fmt.Sprintf("run-%03d", round)
			}
			if err := h.saveNodes(nodes); err != nil {
				t.Fatal(err)
			}
		}
		entries, err := os.ReadDir(h.directory)
		if err != nil || len(entries) != 3 {
			t.Fatalf("history added files per Peer: round=%d entries=%v err=%v", round, entries, err)
		}
		// Reads must not create negative per-Peer dentries either.
		if found, err := h.contains(fmt.Sprintf("never-seen-%d", round)); err != nil || found {
			t.Fatalf("unused identity lookup: %v %v", found, err)
		}
	}
	db, err := h.store()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *bolt.Tx) error {
		stats := tx.Bucket(historyNodesBucket).Stats()
		if stats.KeyN != rounds*perRound {
			t.Fatalf("stored=%d want=%d", stats.KeyN, rounds*perRound)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d retired Peers across %d runs use one Peer history file; per-run indexes=%d", rounds*perRound, rounds, rounds)
}

func TestHistoryStoreFailedTransactionPreservesWholeBatch(t *testing.T) {
	h, err := openAgentHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.close()
	nodes := historyTestNodes(2)
	nodes[1].State = model.NodeReady
	if err := h.saveNodes(nodes); err == nil {
		t.Fatal("invalid terminal state stored")
	}
	if found, err := h.contains(nodes[0].ID); err != nil || found {
		t.Fatalf("failed transaction committed a prefix: %v %v", found, err)
	}
}

func TestHistoryStoreReplayBoundsLargePagesAndClosesTransactions(t *testing.T) {
	h, err := openAgentHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.close()
	nodes := historyTestNodes(5)
	for i := range nodes {
		nodes[i].Error = strings.Repeat("x", 600<<10)
	}
	if err := h.saveNodes(nodes); err != nil {
		t.Fatal(err)
	}
	page, last, eof, err := h.replayPage(context.Background(), nil)
	if err != nil || len(page) != 1 || len(last) != 32 || eof {
		t.Fatalf("unbounded page: nodes=%d eof=%v err=%v", len(page), eof, err)
	}
	key := bytes.Clone(last)
	extra := historyTestNodes(1)
	extra[0].ID = "extra"
	if err := h.saveNodes(extra); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(key, last) {
		t.Fatal("replay cursor retained an mmap view")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, _, err := h.replayPage(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled replay: %v", err)
	}
}
