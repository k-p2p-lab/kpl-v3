package peer

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

func TestMeasurementStartReportsActualSubscriptionTopics(t *testing.T) {
	for _, mode := range []string{"subscribe", "relay", "publish", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			config := model.NodeConfig{GossipSub: model.GossipSubConfig{TopicMode: mode, Topics: []string{"topic-b", "topic-a"}}}.WithDefaults()
			server := &Server{config: model.PeerProcessConfig{NodeConfig: config}, host: newConfigTestHost(t), topics: make(map[string]*pubsub.Topic), telemetry: &telemetry{events: make(chan model.TraceEvent, 100)}}
			defer server.Close()
			if mode == "disabled" {
				server.telemetry.startMeasurement(nil)
			} else {
				if err := server.startPubSub(ctx); err != nil {
					t.Fatal(err)
				}
			}
			server.telemetry.checkpoint()
			server.Close()
			server.telemetry.checkpoint()
			var starts, checkpoints, stops int
			var start, stop model.TraceEvent
			for len(server.telemetry.events) > 0 {
				event := <-server.telemetry.events
				switch event.Type {
				case "measurement_start":
					starts++
					start = event
				case "measurement_checkpoint":
					checkpoints++
					if stops > 0 {
						t.Fatal("checkpoint follows stop")
					}
				case "measurement_stop":
					stops++
					stop = event
				}
			}
			want := []string{}
			if mode == "subscribe" {
				want = []string{"topic-b", "topic-a"}
			}
			if starts != 1 || checkpoints != 1 || stops != 1 || !reflect.DeepEqual(start.Fields["subscribedTopics"], want) {
				t.Fatalf("session events start=%d checkpoint=%d stop=%d topics=%#v", starts, checkpoints, stops, start.Fields["subscribedTopics"])
			}
			if start.SessionID == "" || start.SessionID != stop.SessionID || start.Sequence >= stop.Sequence || !stop.Timestamp.After(start.Timestamp) {
				t.Fatalf("invalid session interval: %+v %+v", start, stop)
			}
		})
	}
}

func TestServerRunWaitsForFinalMeasurementDrain(t *testing.T) {
	ready, stopAttempt := make(chan struct{}), make(chan struct{})
	checkpointReceived := make(chan struct{})
	releaseStop := make(chan struct{})
	var readyOnce, stopOnce sync.Once
	var checkpointOnce sync.Once
	var mu sync.Mutex
	var received []model.TraceEvent
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/health":
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "time": time.Now().UTC()})
		case "/api/v1/bootstrap":
			_, _ = io.WriteString(w, "[]")
		case "/api/v1/nodes/node/status":
			readyOnce.Do(func() { close(ready) })
			w.WriteHeader(http.StatusAccepted)
		case "/api/v1/telemetry":
			var batch model.EventBatch
			if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
				t.Error(err)
				return
			}
			for _, event := range batch.Events {
				if event.Type == "measurement_stop" {
					stopOnce.Do(func() { close(stopAttempt) })
					select {
					case <-releaseStop:
					case <-r.Context().Done():
						return
					}
				}
			}
			mu.Lock()
			received = append(received, batch.Events...)
			mu.Unlock()
			for _, event := range batch.Events {
				if event.Type == "measurement_checkpoint" {
					checkpointOnce.Do(func() { close(checkpointReceived) })
				}
			}
			w.WriteHeader(http.StatusAccepted)
		default:
			http.NotFound(w, r)
		}
	}))
	defer endpoint.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	disabled := false
	port, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	apiAddress := port.Addr().String()
	_ = port.Close()
	config := model.PeerProcessConfig{Node: model.Node{ID: "node", RunID: "run", Role: "boot"}, AgentURL: endpoint.URL, ControllerURL: endpoint.URL, APListen: apiAddress, NodeConfig: model.NodeConfig{Kademlia: model.KademliaConfig{Enabled: &disabled}, GossipSub: model.GossipSubConfig{TopicMode: "subscribe", Topics: []string{"topic-a"}}}}
	server := newRunTestServer(t, config)
	server.bandwidth = newBandwidthReporter()
	server.bandwidth.LogSentMessage(123)
	server.bandwidth.LogSentMessageStream(123, "/test", "remote")
	defer server.Close()
	server.telemetry.shutdownTimeout = 2 * time.Second
	done := make(chan error, 1)
	go func() { done <- server.Run(ctx) }()
	select {
	case <-ready:
	case <-time.After(3 * time.Second):
		close(releaseStop)
		t.Fatal("peer did not become ready")
	}
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(3 * time.Second)
	for {
		response, err := client.Get("http://" + apiAddress + "/health")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			close(releaseStop)
			t.Fatal("peer API did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	response, err := client.Post("http://"+apiAddress+"/publish", "application/json", strings.NewReader(`{"topic":"topic-a","payloadSize":32}`))
	if err != nil {
		close(releaseStop)
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		close(releaseStop)
		t.Fatalf("publish returned %s", response.Status)
	}
	select {
	case <-checkpointReceived:
	case <-time.After(4 * time.Second):
		close(releaseStop)
		t.Fatal("periodic measurement checkpoint missing")
	}
	cancel()
	select {
	case <-stopAttempt:
	case <-time.After(3 * time.Second):
		close(releaseStop)
		t.Fatal("missing final stop attempt")
	}
	select {
	case err := <-done:
		close(releaseStop)
		t.Fatalf("Run returned before drain response: %v", err)
	default:
	}
	close(releaseStop)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not finish after final drain")
	}
	mu.Lock()
	defer mu.Unlock()
	var start, stop model.TraceEvent
	var publish, deliver, checkpoint, finalBandwidth model.TraceEvent
	for i, event := range received {
		if event.Sequence != uint64(i+1) {
			t.Fatalf("source sequence not continuous at %d: %+v", i, event)
		}
		if event.Type == "bandwidth" && event.Bandwidth != nil && event.Bandwidth.Final {
			finalBandwidth = event
		}
		if event.Type == "measurement_start" {
			start = event
		}
		if event.Type == "measurement_stop" {
			stop = event
		}
		if event.Type == "publish" {
			publish = event
		}
		if event.Type == "deliver" {
			deliver = event
		}
		if event.Type == "measurement_checkpoint" {
			checkpoint = event
		}
	}
	if finalBandwidth.SessionID != stop.SessionID || finalBandwidth.Sequence <= stop.Sequence || finalBandwidth.Bandwidth == nil || finalBandwidth.Bandwidth.SentBytes != 123 {
		t.Fatalf("final bandwidth not drained after stop: %+v", received)
	}
	if start.SessionID == "" || stop.SessionID != start.SessionID || stop.Sequence <= start.Sequence {
		t.Fatalf("missing final session records: %+v", received)
	}
	if topics, ok := start.Fields["subscribedTopics"].([]any); !ok || !reflect.DeepEqual(topics, []any{"topic-a"}) {
		t.Fatalf("incorrect session subscriptions: %+v", start)
	}
	if start.Fields["clockBasis"] != controllerClockBasis {
		t.Fatalf("measurement timestamps were not synchronized to the Controller: %+v", start.Fields)
	}
	if publish.MessageID == "" || publish.MessageID != deliver.MessageID || deliver.Sequence <= start.Sequence || publish.Sequence <= start.Sequence || checkpoint.Sequence <= deliver.Sequence || stop.Sequence <= checkpoint.Sequence {
		t.Fatalf("missing ordered application measurement: %+v", received)
	}
}
