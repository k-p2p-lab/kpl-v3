package experimentnet

import (
	"bytes"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestManagerHTTPReplayBindsEpochToOriginalRequest(t *testing.T) {
	for _, changed := range []string{"identical", "same-instant", "agent-url", "agent-set", "added-agent", "older-time", "newer-time", "another-run"} {
		t.Run(changed, func(t *testing.T) {
			m, fake, request := managerFixture(t)
			request.Agents["agent-two"] = "http://agent-two:8090"
			if _, err := m.prepare(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			stored, err := os.ReadFile(filepath.Join(m.config.DataDir, "request.json"))
			if err != nil {
				t.Fatal(err)
			}
			request.Agents = maps.Clone(request.Agents)
			switch changed {
			case "identical":
				request.Agents = map[string]string{"agent-two": request.Agents["agent-two"], "agent": request.Agents["agent"]}
			case "same-instant":
				request.RequestedAt = request.RequestedAt.In(time.FixedZone("test-offset", 9*60*60))
			case "agent-url":
				request.Agents["agent"] = "http://replacement-agent:8090"
			case "agent-set":
				delete(request.Agents, "agent-two")
			case "added-agent":
				request.Agents["agent-three"] = "http://agent-three:8090"
			case "older-time":
				request.RequestedAt = request.RequestedAt.Add(-time.Hour)
			case "newer-time":
				request.RequestedAt = request.RequestedAt.Add(time.Hour)
			case "another-run":
				request.RunID = "another-run"
			}
			payload, err := json.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(m.Handler())
			defer server.Close()
			post, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/api/v1/network/prepare", bytes.NewReader(payload))
			if err != nil {
				t.Fatal(err)
			}
			post.Header.Set("Authorization", "Bearer "+m.config.Token)
			commands, mutations := len(fake.calls), len(fake.mutations)
			response, err := server.Client().Do(post)
			if err != nil {
				t.Fatal(err)
			}
			_ = response.Body.Close()
			valid := changed == "identical" || changed == "same-instant"
			if valid {
				if response.StatusCode != http.StatusOK || len(fake.mutations) != mutations {
					t.Fatalf("identical replay was not read-only: status=%d mutations before=%d after=%d", response.StatusCode, mutations, len(fake.mutations))
				}
				return
			}
			current, err := os.ReadFile(filepath.Join(m.config.DataDir, "request.json"))
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != http.StatusConflict || len(fake.calls) != commands || len(fake.mutations) != mutations || !bytes.Equal(stored, current) {
				t.Fatalf("changed epoch payload reached Docker or replaced its durable identity: status=%d commands before=%d after=%d mutations before=%d after=%d recordChanged=%t", response.StatusCode, commands, len(fake.calls), mutations, len(fake.mutations), !bytes.Equal(stored, current))
			}
		})
	}
}
