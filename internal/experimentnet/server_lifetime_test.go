package experimentnet

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestNetworkHandlerShutdownJoinHasBoundAndRejectsNewAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started, release := make(chan struct{}), make(chan struct{})
		lifetime := &networkHandlerLifetime{done: make(chan struct{}), handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			close(started)
			<-release
		})}
		go lifetime.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
		<-started
		lifetime.stop()
		rejected := httptest.NewRecorder()
		lifetime.ServeHTTP(rejected, httptest.NewRequest(http.MethodGet, "/", nil))
		if rejected.Code != http.StatusServiceUnavailable {
			t.Fatalf("new request entered shutdown: %d", rejected.Code)
		}
		ctx, cancel := context.WithTimeout(t.Context(), networkServiceShutdownTimeout)
		defer cancel()
		before := time.Now()
		if err := lifetime.wait(ctx); !errors.Is(err, context.DeadlineExceeded) || time.Since(before) != 3*time.Minute {
			t.Fatalf("noncooperating handler escaped the shutdown bound: elapsed=%v err=%v", time.Since(before), err)
		}
		close(release)
		if err := lifetime.wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func startNetworkLifetimeServer(t *testing.T, handler http.Handler) (string, context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	records := make(chan slog.Record, 1)
	done := make(chan error, 1)
	go func() { done <- serve(ctx, "127.0.0.1:0", handler, slog.New(listenLogHandler{records: records})) }()
	select {
	case record := <-records:
		var address string
		record.Attrs(func(attribute slog.Attr) bool {
			if attribute.Key == "listen" {
				address = attribute.Value.String()
			}
			return true
		})
		return "http://" + address, cancel, done
	case <-time.After(3 * time.Second):
		t.Fatal("network lifetime fixture did not start")
		return "", cancel, done
	}
}

func waitNetworkLifetimeStop(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("network service did not join canceled requests")
	}
}

func TestNetworkServerShutdownCancelsIncompleteManagerRequestBody(t *testing.T) {
	m, fake, _ := managerFixture(t)
	entered := make(chan struct{})
	endpoint, cancel, done := startNetworkLifetimeServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		m.Handler().ServeHTTP(w, r)
	}))
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint+"/api/v1/network/prepare", reader)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+m.config.Token)
	client := &http.Client{Timeout: 3 * time.Second}
	defer client.CloseIdleConnections()
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		if response, err := client.Do(request); err == nil {
			_ = response.Body.Close()
		}
	}()
	if _, err := io.WriteString(writer, `{"runId":`); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("incomplete body never reached the Manager")
	}
	cancel()
	waitNetworkLifetimeStop(t, done)
	if len(fake.calls) != 0 || len(fake.mutations) != 0 {
		t.Fatal("incomplete request entered Docker preparation")
	}
	_ = writer.Close()
	select {
	case <-requestDone:
	case <-time.After(2 * time.Second):
		t.Fatal("incomplete request remained connected after shutdown")
	}
}

func TestNetworkServerShutdownCancelsGatewayUpstreamStream(t *testing.T) {
	upstreamDone := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(upstreamDone)
		_, _ = io.WriteString(w, "stream started\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer upstream.Close()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	defer transport.CloseIdleConnections()
	handler, err := GatewayHandler(GatewayConfig{Token: "fixture-token", ControllerURL: upstream.URL, Subnet: "10.11.0.0/16"}, transport)
	if err != nil {
		t.Fatal(err)
	}
	endpoint, cancel, done := startNetworkLifetimeServer(t, handler)
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, endpoint+"/controller/api/v1/discovery", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer fixture-token")
	client := &http.Client{Timeout: 3 * time.Second}
	defer client.CloseIdleConnections()
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data := make([]byte, len("stream started\n"))
	if _, err := io.ReadFull(response.Body, data); err != nil || !strings.HasPrefix(string(data), "stream started") {
		t.Fatalf("upstream stream did not begin: %v", err)
	}
	cancel()
	waitNetworkLifetimeStop(t, done)
	select {
	case <-upstreamDone:
	case <-time.After(2 * time.Second):
		t.Fatal("gateway upstream remained live after shutdown")
	}
}
