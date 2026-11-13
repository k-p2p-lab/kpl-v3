package controller

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestScenarioListRevalidatesInPlaceRewriteWithPreservedSizeAndMtime(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		name := "valid-rewrite"
		if corrupt {
			name = "corrupt-rewrite"
		}
		t.Run(name, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			response := scenarioAPIRequest(t, s, http.MethodPost, "/api/v1/scenarios", scenarioSubmission{Name: "before", YAML: validSavedScenarioYAML("before")}, false)
			var saved savedScenario
			if response.Code != http.StatusCreated || json.Unmarshal(response.Body.Bytes(), &saved) != nil {
				t.Fatalf("create status=%d body=%s", response.Code, response.Body)
			}
			if response := scenarioAPIRequest(t, s, http.MethodGet, "/api/v1/scenarios", nil, false); response.Code != http.StatusOK {
				t.Fatalf("warm list: %d %s", response.Code, response.Body)
			}
			path := filepath.Join(s.config.DataDir, "scenarios", saved.ID+".json")
			before, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if corrupt {
				data = bytes.Replace(data, []byte("action: wait"), []byte("action: nope"), 1)
			} else {
				data = bytes.ReplaceAll(data, []byte("before"), []byte("change"))
			}
			if int64(len(data)) != before.Size() {
				t.Fatal("rewrite fixture changed the record size")
			}
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chtimes(path, before.ModTime(), before.ModTime()); err != nil {
				t.Fatal(err)
			}
			after, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if !os.SameFile(before, after) || after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) || sourceFileIdentity(before.Sys()) == sourceFileIdentity(after.Sys()) {
				t.Fatal("rewrite fixture must preserve inode/size/mtime and change the filesystem identity")
			}
			list := scenarioAPIRequest(t, s, http.MethodGet, "/api/v1/scenarios", nil, false)
			loaded := scenarioAPIRequest(t, s, http.MethodGet, "/api/v1/scenarios/"+saved.ID, nil, false)
			if corrupt {
				if list.Code != http.StatusInternalServerError || loaded.Code != http.StatusInternalServerError {
					t.Fatalf("warm list trusted a rewritten invalid scenario: list=%d %s load=%d %s", list.Code, list.Body, loaded.Code, loaded.Body)
				}
			} else if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), `"name":"change"`) || loaded.Code != http.StatusOK || !strings.Contains(loaded.Body.String(), `"name":"change"`) {
				t.Fatalf("warm list returned a stale scenario: list=%d %s load=%d %s", list.Code, list.Body, loaded.Code, loaded.Body)
			}
		})
	}
}
