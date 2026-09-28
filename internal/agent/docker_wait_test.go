package agent

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDockerEngineWaitsAreConcurrentCancelableAndStartNoProcesses(t *testing.T) {
	dir, err := os.MkdirTemp("", "kpl-wait-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "docker.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	const peers = 64
	entered := make(chan struct{}, peers)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/containers/"+fakeContainerID+"/wait" || r.URL.Query().Get("condition") != "not-running" {
			t.Error("incorrect wait API request")
		}
		entered <- struct{}{}
		<-r.Context().Done()
	})}
	defer server.Close()
	go server.Serve(listener)
	d := &dockerRuntime{waitClient: newDockerWaitClient(socket), waitURL: "http://docker", commandContext: func(context.Context, string, ...string) *exec.Cmd {
		t.Error("wait started a child process")
		return nil
	}}
	defer d.waitClient.CloseIdleConnections()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	for range peers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := d.wait(ctx, fakeContainerID); !errors.Is(err, context.Canceled) {
				t.Errorf("cancellation lost: %v", err)
			}
		}()
	}
	for range peers {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			cancel()
			wg.Wait()
			t.Fatal("wait connections serialized behind a small pool")
		}
	}
	if d.activeWaits.Load() != peers {
		t.Fatalf("wait connections: %d", d.activeWaits.Load())
	}
	cancel()
	wg.Wait()
	if d.activeWaits.Load() != 0 {
		t.Fatal("wait connection accounting leaked")
	}
}

func TestDockerWaitDaemonErrorsNeverReportSuccess(t *testing.T) {
	for _, test := range []struct {
		name, body string
		status     int
	}{
		{"daemon_error", `{"StatusCode":0,"Error":{"Message":"wait failed"}}`, 200},
		{"missing_exit", `{}`, 200}, {"missing_container", `{"message":"missing"}`, 404},
		{"oversized", string(make([]byte, (64<<10)+1)), 200},
	} {
		t.Run(test.name, func(t *testing.T) {
			d := &dockerRuntime{waitURL: "http://docker", waitClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: test.status, Body: io.NopCloser(strings.NewReader(test.body))}, nil
			})}}
			if err := d.wait(context.Background(), fakeContainerID); err == nil {
				t.Fatal("invalid wait response accepted")
			}
		})
	}
}
