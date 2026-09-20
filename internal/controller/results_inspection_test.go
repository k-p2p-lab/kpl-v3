package controller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestResultDeletionDuringArchiveMeasurement(t *testing.T) {
	for _, method := range []string{http.MethodHead, http.MethodGet} {
		t.Run(method, func(t *testing.T) {
			server := New(ServerConfig{DataDir: t.TempDir()}, nil)
			experiment, _ := resultFixture(t, server, "run-measuring", "completed", time.Now().UTC())
			// Hold all measurement slots so the request has captured its files but
			// cannot finish before deletion. This reproduces a large or queued ZIP.
			held := 0
			for range resultArchiveMeasureLimit {
				if err := server.acquireResultArchiveSlot(context.Background()); err != nil {
					t.Fatal(err)
				}
				held++
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan *httptest.ResponseRecorder, 1)
			go func() {
				response := httptest.NewRecorder()
				request := httptest.NewRequest(method, "/api/v1/experiments/"+experiment.ID+"/download", nil).WithContext(ctx)
				server.apiTestHandler(context.Background()).ServeHTTP(response, request)
				done <- response
			}()
			defer func() {
				cancel()
				for held > 0 {
					server.releaseResultArchiveSlot()
					held--
				}
			}()
			deadline := time.Now().Add(2 * time.Second)
			for {
				server.resultArchiveMu.Lock()
				started := server.resultArchiveFlights[experiment.ID] != nil
				server.resultArchiveMu.Unlock()
				if started {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("archive measurement did not start")
				}
				time.Sleep(time.Millisecond)
			}
			deleted := resultRequest(server, http.MethodDelete, "/api/v1/results/"+experiment.ID)
			wantStatus := http.StatusNoContent
			if method == http.MethodGet {
				wantStatus = http.StatusConflict
			}
			if deleted.Code != wantStatus {
				t.Fatalf("delete during %s measurement: status=%d, want %d; %s", method, deleted.Code, wantStatus, deleted.Body)
			}
			for held > 0 {
				server.releaseResultArchiveSlot()
				held--
			}
			select {
			case response := <-done:
				if response.Code != http.StatusOK {
					t.Fatalf("captured %s failed: %d %s", method, response.Code, response.Body)
				}
				if method == http.MethodGet {
					decodeResultZIP(t, response.Body.Bytes())
					if response := resultRequest(server, http.MethodDelete, "/api/v1/results/"+experiment.ID); response.Code != http.StatusNoContent {
						t.Fatalf("delete after download: %d %s", response.Code, response.Body)
					}
				}
			case <-time.After(2 * time.Second):
				t.Fatal("archive request did not release its resources")
			}
			if _, err := os.Stat(filepath.Join(server.config.DataDir, "runs", experiment.ID)); !os.IsNotExist(err) {
				t.Fatalf("deleted directory remains: %v", err)
			}
			server.resultArchiveMu.Lock()
			_, cached := server.resultArchives[experiment.ID]
			flights := len(server.resultArchiveFlights)
			server.resultArchiveMu.Unlock()
			if cached || flights != 0 {
				t.Fatalf("measurement retained deleted cache or flight: cached=%v flights=%d", cached, flights)
			}
		})
	}
}
