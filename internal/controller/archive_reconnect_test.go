package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func reconnectRequest(s *Server, body string) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	s.handleArchiveReconnect(response, httptest.NewRequest(http.MethodPost, "/api/v1/result-storage/reconnect", strings.NewReader(body)))
	return response
}

func reconnectFixture(t *testing.T) (*Server, map[string][]byte, []byte) {
	t.Helper()
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, s, "archived", "completed", time.Now())
	if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{storageEvent("archived", "preserved")}}); err != nil {
		t.Fatal(err)
	}
	original := storageZIP(t, s, "archived")
	archiveTestRun(t, s, "archived")
	pin, err := os.ReadFile(filepath.Join(s.config.DataDir, ".archive-store-id"))
	if err != nil {
		t.Fatal(err)
	}
	return s, original, pin
}

func requireReconnectSourcesEqual(t *testing.T, before, after map[string][]byte) {
	t.Helper()
	for _, name := range resultSourceFiles {
		if !bytes.Equal(before[name], after[name]) {
			t.Fatalf("reconnect changed source %s", name)
		}
	}
}

func TestArchiveReconnectAdmissionIsLocalAndDeduplicated(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	s.archiveIOCheck = func() { t.Fatal("reconnect admission performed NAS I/O") }
	for _, body := range []string{"", "{}", "null", `{"confirmCurrentArchive":false}`, `{"confirmCurrentArchive":true,"unexpected":true}`, `{"confirmCurrentArchive":true} {}`, `{"confirmCurrentArchive":true}` + strings.Repeat(" ", 1025)} {
		if response := reconnectRequest(s, body); response.Code != http.StatusBadRequest || s.archiveReconnectState != "" {
			t.Fatalf("invalid request admitted: %d %s state=%q", response.Code, response.Body, s.archiveReconnectState)
		}
	}
	response := httptest.NewRecorder()
	s.handleArchiveReconnect(response, httptest.NewRequest(http.MethodGet, "/api/v1/result-storage/reconnect", nil))
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET: %d", response.Code)
	}
	for range 2 {
		if response := reconnectRequest(s, `{"confirmCurrentArchive":true}`); response.Code != http.StatusAccepted || s.archiveReconnectState != "queued" {
			t.Fatalf("queue: %d %s state=%q", response.Code, response.Body, s.archiveReconnectState)
		}
	}
	s.archiveReconnectState = "running"
	if response := reconnectRequest(s, `{"confirmCurrentArchive":true}`); response.Code != http.StatusAccepted || s.archiveReconnectState != "running" {
		t.Fatalf("duplicate running request: %d %s", response.Code, response.Body)
	}
	s.shuttingDown = true
	if response := reconnectRequest(s, `{"confirmCurrentArchive":true}`); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("shutdown admission: %d %s", response.Code, response.Body)
	}
}

func TestArchiveReconnectUsesSessionAuthenticationAndCSRF(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "test-password"}, nil)
	s.archiveIOCheck = func() { t.Fatal("authenticated reconnect admission performed NAS I/O") }
	handler := s.Handler(t.Context())
	cookie := loginCookie(t, s)
	for _, test := range []struct {
		name   string
		cookie bool
		header bool
		status int
	}{
		{name: "anonymous", status: http.StatusUnauthorized},
		{name: "without csrf", cookie: true, status: http.StatusForbidden},
		{name: "dashboard", cookie: true, header: true, status: http.StatusAccepted},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/api/v1/result-storage/reconnect", strings.NewReader(`{"confirmCurrentArchive":true}`))
			if test.cookie {
				request.AddCookie(cookie)
			}
			if test.header {
				request.Header.Set("X-KPL-Request", "dashboard")
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("status=%d want=%d body=%s", response.Code, test.status, response.Body)
			}
			if test.status != http.StatusAccepted && s.archiveReconnectState != "" {
				t.Fatal("unauthorized request queued a reconnect")
			}
		})
	}
}

func TestArchiveReconnectRestoresVerifiedResults(t *testing.T) {
	for _, missing := range []bool{false, true} {
		name := "changed identity"
		if missing {
			name = "missing identity"
		}
		t.Run(name, func(t *testing.T) {
			s, original, oldPin := reconnectFixture(t)
			remotePin := filepath.Join(s.config.DataDir, archivedRunsDirectory, ".kpl-store-id")
			if missing {
				if err := os.Remove(remotePin); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(remotePin, []byte("restored-archive"), 0600); err != nil {
				t.Fatal(err)
			}
			if root, err := s.openArchiveStore(); err == nil {
				root.Close()
				t.Fatal("identity mismatch was adopted without an explicit request")
			}
			if response := reconnectRequest(s, `{"confirmCurrentArchive":true}`); response.Code != http.StatusAccepted {
				t.Fatal(response.Body)
			}
			s.archiveLastScan = time.Now()
			if err := s.processArchiveReconnect(t.Context()); err != nil {
				t.Fatal(err)
			}
			pin, err := os.ReadFile(filepath.Join(s.config.DataDir, ".archive-store-id"))
			if err != nil || bytes.Equal(pin, oldPin) {
				t.Fatalf("local identity not replaced: %v", err)
			}
			actual, err := os.ReadFile(remotePin)
			if err != nil || !bytes.Equal(pin, actual) {
				t.Fatalf("identity mismatch after reconnect: %v", err)
			}
			requireReconnectSourcesEqual(t, original, storageZIP(t, s, "archived"))
			if s.archiveReconnectState != "completed" || s.archiveReconnectError != "" || !s.archiveLastScan.IsZero() || len(s.resultReadPins) != 0 {
				t.Fatalf("completion did not release state: %s %s pins=%v", s.archiveReconnectState, s.archiveReconnectError, s.resultReadPins)
			}
		})
	}
}

func TestArchiveReconnectRejectsUnverifiableReferences(t *testing.T) {
	for _, damage := range []string{"missing", "truncated", "corrupted", "file reference", "legacy", "invalid digest", "broken manifest", "unfinished import"} {
		t.Run(damage, func(t *testing.T) {
			s, _, oldPin := reconnectFixture(t)
			local, err := s.analysisDirectory("archived")
			if err != nil {
				t.Fatal(err)
			}
			manifest, err := readRunArchive(local, "archived")
			if err != nil {
				t.Fatal(err)
			}
			segment := manifest.Logs["events.jsonl"][0]
			object := filepath.Join(s.config.DataDir, archivedRunsDirectory, "archived", segment.Object)
			switch damage {
			case "missing":
				err = os.Remove(object)
			case "truncated":
				err = os.Truncate(object, segment.Size-1)
			case "corrupted":
				err = os.WriteFile(object, bytes.Repeat([]byte("x"), int(segment.Size)), 0600)
			case "file reference":
				stored := manifest.Files["scenario.yaml"]
				err = os.WriteFile(filepath.Join(s.config.DataDir, archivedRunsDirectory, "archived", stored.Object), bytes.Repeat([]byte("x"), int(stored.Size)), 0600)
			case "legacy", "invalid digest":
				segment.SHA256 = ""
				if damage == "invalid digest" {
					segment.SHA256 = strings.Repeat("z", 64)
				}
				manifest.Logs["events.jsonl"][0] = segment
				err = writeRunArchive(local, manifest)
			case "broken manifest":
				err = local.WriteFile(runArchiveManifest, []byte("{"), 0600)
			case "unfinished import":
				err = local.WriteFile(runArchiveImportFile, []byte("pending"), 0600)
			}
			local.Close()
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(s.config.DataDir, archivedRunsDirectory, ".kpl-store-id")); err != nil {
				t.Fatal(err)
			}
			reconnectRequest(s, `{"confirmCurrentArchive":true}`)
			if err := s.processArchiveReconnect(t.Context()); err == nil {
				t.Fatal("unverifiable archive was adopted")
			}
			pin, err := os.ReadFile(filepath.Join(s.config.DataDir, ".archive-store-id"))
			if err != nil || !bytes.Equal(pin, oldPin) {
				t.Fatalf("failure replaced the local pin: %v", err)
			}
			if _, err := os.Stat(filepath.Join(s.config.DataDir, archivedRunsDirectory, ".kpl-store-id")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failure initialized unverified remote identity: %v", err)
			}
			if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, "archived", "experiment.json")); err != nil {
				t.Fatalf("failure discarded local metadata: %v", err)
			}
			if s.archiveReconnectState != "failed" || s.archiveReconnectError == "" || len(s.resultReadPins) != 0 {
				t.Fatalf("failure state: %q %q pins=%v", s.archiveReconnectState, s.archiveReconnectError, s.resultReadPins)
			}
			s.archiveIOCheck = func() { t.Fatal("failed reconnect retried automatically") }
			if err := s.processArchiveReconnect(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestArchiveReconnectExplicitlyAdoptsCurrentStoreWithoutReferences(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, s, "pending", "completed", time.Now())
	if err := os.Mkdir(filepath.Join(s.config.DataDir, archivedRunsDirectory), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.config.DataDir, ".archive-store-id"), []byte("old-missing-archive"), 0600); err != nil {
		t.Fatal(err)
	}
	before := storageZIP(t, s, "pending")
	if root, err := s.openArchiveStore(); err == nil {
		root.Close()
		t.Fatal("automatic open adopted a replacement archive")
	}
	reconnectRequest(s, `{"confirmCurrentArchive":true}`)
	if err := s.processArchiveReconnect(t.Context()); err != nil {
		t.Fatal(err)
	}
	requireReconnectSourcesEqual(t, before, storageZIP(t, s, "pending"))
	archiveTestRun(t, s, "pending")
	if got := listedStorageState(t, s, "pending"); got != "archived" {
		t.Fatalf("reconnected archive did not receive pending result: %s", got)
	}
}

func TestArchiveReconnectDetectsChangesBeforeCommit(t *testing.T) {
	for _, change := range []string{"directory", "remote identity", "local identity"} {
		t.Run(change, func(t *testing.T) {
			s, _, oldPin := reconnectFixture(t)
			remote := filepath.Join(s.config.DataDir, archivedRunsDirectory)
			calls := 0
			s.archiveIOCheck = func() {
				calls++
				if calls != 2 {
					return
				}
				var err error
				switch change {
				case "directory":
					err = os.Rename(remote, remote+"-original")
					if err == nil {
						err = os.Mkdir(remote, 0700)
					}
				case "remote identity":
					err = os.WriteFile(filepath.Join(remote, ".kpl-store-id"), []byte("changed-during-verification"), 0600)
				case "local identity":
					err = os.WriteFile(filepath.Join(s.config.DataDir, ".archive-store-id"), []byte("other-local-owner"), 0600)
				}
				if err != nil {
					t.Error(err)
				}
			}
			reconnectRequest(s, `{"confirmCurrentArchive":true}`)
			if err := s.processArchiveReconnect(t.Context()); err == nil {
				t.Fatal("changed identity or directory was adopted")
			}
			pin, err := os.ReadFile(filepath.Join(s.config.DataDir, ".archive-store-id"))
			if change == "local identity" {
				oldPin = []byte("other-local-owner")
			}
			if err != nil || !bytes.Equal(pin, oldPin) {
				t.Fatalf("reconnect overwrote another owner's pin: %v", err)
			}
		})
	}
}

func TestArchiveReconnectBlockedNASDoesNotHoldControlLocks(t *testing.T) {
	s, _, _ := reconnectFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	s.archiveIOCheck = func() { once.Do(func() { close(entered); <-release }) }
	reconnectRequest(s, `{"confirmCurrentArchive":true}`)
	done := make(chan error, 1)
	go func() { done <- s.processArchiveReconnect(t.Context()) }()
	<-entered
	checks := make(chan error, 1)
	go func() {
		if err := s.deleteSavedResult("archived"); !errors.Is(err, errResultBusy) {
			checks <- errors.New("verification did not protect its local manifest from deletion")
			return
		}
		response := reconnectRequest(s, `{"confirmCurrentArchive":true}`)
		if response.Code != http.StatusAccepted {
			checks <- errors.New("duplicate admission failed while NAS was blocked")
			return
		}
		status := resultRequest(s, http.MethodGet, "/api/v1/result-storage")
		var body resultStorageOverview
		if err := json.Unmarshal(status.Body.Bytes(), &body); err != nil || body.ReconnectState != "running" {
			checks <- errors.New("local storage status failed while NAS was blocked")
			return
		}
		checks <- s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{storageEvent("archived", "late-during-reconnect")}})
	}()
	select {
	case err := <-checks:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(2 * time.Second):
		t.Error("NAS reconnection held a control or persistence lock")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(s.resultReadPins) != 0 {
		t.Fatal("reconnect left local result pins")
	}
}

func TestArchiveReconnectCancellationDoesNotChangeIdentity(t *testing.T) {
	s, _, oldPin := reconnectFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	s.archiveIOCheck = cancel
	reconnectRequest(s, `{"confirmCurrentArchive":true}`)
	if err := s.processArchiveReconnect(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	pin, err := os.ReadFile(filepath.Join(s.config.DataDir, ".archive-store-id"))
	if err != nil || !bytes.Equal(pin, oldPin) || len(s.resultReadPins) != 0 {
		t.Fatalf("cancellation changed identity or left pins: %v", err)
	}
}

func TestArchiveReconnectMissingLocalPinStillRequiresVerification(t *testing.T) {
	for _, damaged := range []bool{false, true} {
		name := "verified"
		if damaged {
			name = "damaged"
		}
		t.Run(name, func(t *testing.T) {
			s, original, _ := reconnectFixture(t)
			pinPath := filepath.Join(s.config.DataDir, ".archive-store-id")
			if err := os.Remove(pinPath); err != nil {
				t.Fatal(err)
			}
			if damaged {
				local, err := s.analysisDirectory("archived")
				if err != nil {
					t.Fatal(err)
				}
				manifest, err := readRunArchive(local, "archived")
				local.Close()
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(filepath.Join(s.config.DataDir, archivedRunsDirectory, "archived", manifest.Logs["events.jsonl"][0].Object)); err != nil {
					t.Fatal(err)
				}
			}
			if root, err := s.openArchiveStore(); err == nil {
				root.Close()
				t.Fatal("existing remote references were rebound without verification")
			}
			reconnectRequest(s, `{"confirmCurrentArchive":true}`)
			err := s.processArchiveReconnect(t.Context())
			if damaged {
				if err == nil {
					t.Fatal("missing referenced object passed verification")
				}
				if root, err := s.openArchiveStore(); err == nil {
					root.Close()
					t.Fatal("failed verification was bypassed by automatic initialization")
				}
				if _, err := os.Stat(pinPath); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("unverified candidate received a local identity pin: %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				requireReconnectSourcesEqual(t, original, storageZIP(t, s, "archived"))
			}
		})
	}
}

func TestArchiveReconnectFailureBlocksUnpinnedInitializationUntilRetry(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, s, "partial", "completed", time.Now())
	if err := os.Mkdir(filepath.Join(s.config.DataDir, archivedRunsDirectory), 0700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(s.config.DataDir, currentRunsDirectory, "partial", runArchiveImportFile)
	if err := os.WriteFile(marker, []byte("pending"), 0600); err != nil {
		t.Fatal(err)
	}
	s.archiveIOCheck = func() { t.Fatal("failed or pending reconnect allowed automatic NAS initialization") }
	reconnectRequest(s, `{"confirmCurrentArchive":true}`)
	if root, err := s.openArchiveStore(); err == nil {
		root.Close()
		t.Fatal("queued reconnect allowed automatic initialization")
	}
	if err := s.processArchiveReconnect(t.Context()); err == nil {
		t.Fatal("unfinished import was accepted")
	}
	if root, err := s.openArchiveStore(); err == nil {
		root.Close()
		t.Fatal("failed reconnect allowed automatic initialization")
	}
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	s.archiveIOCheck = nil
	reconnectRequest(s, `{"confirmCurrentArchive":true}`)
	if err := s.processArchiveReconnect(t.Context()); err != nil {
		t.Fatal(err)
	}
	root, err := s.openArchiveStore()
	if err != nil {
		t.Fatalf("explicit retry did not recover after local repair: %v", err)
	}
	root.Close()
	// A failed request must not invalidate a previously established store pin.
	if err := os.WriteFile(marker, []byte("pending"), 0600); err != nil {
		t.Fatal(err)
	}
	reconnectRequest(s, `{"confirmCurrentArchive":true}`)
	if err := s.processArchiveReconnect(t.Context()); err == nil {
		t.Fatal("unfinished import was accepted")
	}
	root, err = s.openArchiveStore()
	if err != nil {
		t.Fatalf("failed reconnect invalidated an established archive: %v", err)
	}
	root.Close()
}
