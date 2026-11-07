package controller

import (
	"bytes"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func libraryGroupIDs(snapshot libraryGroupsSnapshot) []string {
	ids := make([]string, len(snapshot.Groups))
	for i, group := range snapshot.Groups {
		ids[i] = group.ID
	}
	return ids
}

func TestLibraryGroupOrderPersistsAndPreservesNamesMembershipsAndSources(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	_, source := resultFixture(t, s, "ordered-run", "completed", time.Now())
	snapshot := emptyLibraryGroups()
	for _, name := range []string{"Zulu", "Alpha", "연구 그룹"} {
		snapshot = newTestLibraryGroup(t, s, name, snapshot.Revision)
	}
	initial := append([]libraryGroup(nil), snapshot.Groups...)
	snapshot = moveTestLibraryItems(t, s, initial[1].ID, []string{"run:ordered-run"}, snapshot.Revision)
	beforeRevision := snapshot.Revision
	order := []string{initial[2].ID, initial[0].ID, initial[1].ID}
	updated := librarySnapshot(t, libraryRequest(t, s, http.MethodPut, "/order", map[string]any{"ids": order, "revision": beforeRevision}), http.StatusOK)
	if !reflect.DeepEqual(libraryGroupIDs(updated), order) || updated.Revision == beforeRevision {
		t.Fatalf("group order was not persisted: %+v", updated)
	}
	if !reflect.DeepEqual(updated.Groups, []libraryGroup{initial[2], initial[0], initial[1]}) || !reflect.DeepEqual(updated.Memberships, snapshot.Memberships) {
		t.Fatal("ordering changed names, IDs, or memberships")
	}
	path := filepath.Join(s.config.DataDir, libraryGroupsFile)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	noOp := librarySnapshot(t, libraryRequest(t, s, http.MethodPut, "/order", map[string]any{"ids": order, "revision": updated.Revision}), http.StatusOK)
	after, err := os.ReadFile(path)
	if err != nil || noOp.Revision != updated.Revision || !bytes.Equal(before, after) {
		t.Fatal("unchanged order rewrote group metadata")
	}
	restarted := New(ServerConfig{DataDir: s.config.DataDir}, nil)
	loaded := librarySnapshot(t, libraryRequest(t, restarted, http.MethodGet, "", nil), http.StatusOK)
	if !reflect.DeepEqual(loaded, noOp) {
		t.Fatal("restart changed saved group order")
	}
	withNew := newTestLibraryGroup(t, restarted, "New last group", loaded.Revision)
	if !reflect.DeepEqual(libraryGroupIDs(withNew)[:3], order) || withNew.Groups[3].Name != "New last group" {
		t.Fatal("new group changed existing order instead of appending")
	}
	deleted := librarySnapshot(t, libraryRequest(t, restarted, http.MethodDelete, "/"+initial[0].ID, map[string]string{"revision": withNew.Revision}), http.StatusOK)
	if want := []string{initial[2].ID, initial[1].ID, withNew.Groups[3].ID}; !reflect.DeepEqual(libraryGroupIDs(deleted), want) {
		t.Fatal("deleting one group changed remaining relative order")
	}
	actualSource, err := os.ReadFile(filepath.Join(s.config.DataDir, currentRunsDirectory, "ordered-run", "experiment.json"))
	if err != nil || !bytes.Equal(source, actualSource) {
		t.Fatal("group ordering modified experiment metadata")
	}
}

func TestLibraryGroupOrderRejectsInvalidAndStalePermutationsAtomically(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	snapshot := newTestLibraryGroup(t, s, "One", "0")
	snapshot = newTestLibraryGroup(t, s, "Two", snapshot.Revision)
	ids := libraryGroupIDs(snapshot)
	path := filepath.Join(s.config.DataDir, libraryGroupsFile)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []any{nil, []string{}, ids[:1], []string{ids[0], ids[0]}, []string{ids[0], strings.Repeat("f", 32)}, []string{ids[0], "*"}, []string{ids[0], ""}, []string{ids[0], "../outside"}, "wrong-type"} {
		response := libraryRequest(t, s, http.MethodPut, "/order", map[string]any{"ids": invalid, "revision": snapshot.Revision})
		if response.Code != http.StatusBadRequest {
			t.Fatalf("invalid order accepted: %v: %d %s", invalid, response.Code, response.Body)
		}
		after, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(before, after) {
			t.Fatal("invalid order changed persisted metadata")
		}
	}
	for _, body := range []map[string]any{{"revision": snapshot.Revision}, {"ids": ids}, {"ids": ids, "revision": snapshot.Revision, "name": "Ignored"}, {"ids": ids, "revision": snapshot.Revision, "keys": []string{"run:x"}}} {
		if got := libraryRequest(t, s, http.MethodPut, "/order", body); got.Code != http.StatusBadRequest {
			t.Fatalf("malformed order body accepted: %d %s", got.Code, got.Body)
		}
	}
	withNew := newTestLibraryGroup(t, s, "Created elsewhere", snapshot.Revision)
	if got := libraryRequest(t, s, http.MethodPut, "/order", map[string]any{"ids": []string{ids[1], ids[0]}, "revision": snapshot.Revision}); got.Code != http.StatusConflict {
		t.Fatalf("stale order after creation was not a conflict: %d %s", got.Code, got.Body)
	}
	loaded := librarySnapshot(t, libraryRequest(t, s, http.MethodGet, "", nil), http.StatusOK)
	if !reflect.DeepEqual(loaded, withNew) {
		t.Fatal("stale order lost a concurrently created group")
	}
	deleted := librarySnapshot(t, libraryRequest(t, s, http.MethodDelete, "/"+ids[0], map[string]string{"revision": loaded.Revision}), http.StatusOK)
	if got := libraryRequest(t, s, http.MethodPut, "/order", map[string]any{"ids": libraryGroupIDs(loaded), "revision": loaded.Revision}); got.Code != http.StatusConflict {
		t.Fatalf("stale order after deletion was not a conflict: %d %s", got.Code, got.Body)
	}
	if current := librarySnapshot(t, libraryRequest(t, s, http.MethodGet, "", nil), http.StatusOK); !reflect.DeepEqual(current, deleted) {
		t.Fatal("stale order recreated a deleted group")
	}
}

func TestLibraryGroupOrderUsesSharedRevisionAndAuthentication(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "fixture-password"}, nil)
	if response := scenarioAPIRequest(t, s, http.MethodPut, "/api/v1/library-groups/order", map[string]any{"ids": []string{}, "revision": "0"}, false); response.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous ordering returned %d", response.Code)
	}
	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodPatch, http.MethodDelete} {
		if got := libraryRequest(t, s, method, "/order", nil); got.Code != http.StatusMethodNotAllowed || got.Header().Get("Allow") != "PUT" {
			t.Fatalf("unsupported order method accepted: %s %d", method, got.Code)
		}
	}
	empty := librarySnapshot(t, libraryRequest(t, s, http.MethodPut, "/order", map[string]any{"ids": []string{}, "revision": "0"}), http.StatusOK)
	if empty.Revision != "0" || len(empty.Groups) != 0 {
		t.Fatal("empty order changed initial state")
	}
	snapshot := newTestLibraryGroup(t, s, "One", "0")
	snapshot = newTestLibraryGroup(t, s, "Two", snapshot.Revision)
	ids := libraryGroupIDs(snapshot)
	start := make(chan struct{})
	results := make(chan int, 2)
	var done sync.WaitGroup
	for _, rename := range []bool{false, true} {
		done.Add(1)
		go func(rename bool) {
			defer done.Done()
			<-start
			if rename {
				results <- libraryRequest(t, s, http.MethodPatch, "/"+ids[0], map[string]string{"name": "Renamed", "revision": snapshot.Revision}).Code
			} else {
				results <- libraryRequest(t, s, http.MethodPut, "/order", map[string]any{"ids": []string{ids[1], ids[0]}, "revision": snapshot.Revision}).Code
			}
		}(rename)
	}
	close(start)
	done.Wait()
	close(results)
	codes := map[int]int{}
	for result := range results {
		codes[result]++
	}
	if codes[http.StatusOK] != 1 || codes[http.StatusConflict] != 1 {
		t.Fatalf("reorder bypassed shared revision protection: %v", codes)
	}
}
