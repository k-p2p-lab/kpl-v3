package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"reflect"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

func TestControllerPlacementWorkerHelper(t *testing.T) {
	if len(os.Args) < 2 || os.Args[len(os.Args)-1] != "placement-helper" {
		return
	}
	factory := func(ctx context.Context, kind string, raw json.RawMessage, peer *workerproc.Peer) (workerproc.Handler, error) {
		var start struct {
			RunID string
			Seed  int64
		}
		if err := json.Unmarshal(raw, &start); err != nil {
			return nil, err
		}
		rng := rand.New(rand.NewSource(start.Seed))
		return func(callCtx context.Context, method string, input json.RawMessage) (any, error) {
			switch method {
			case "placement-draw":
				return rng.Int63(), nil
			case "execute":
				var values []string
				for i := 0; i < 64; i++ {
					id := fmt.Sprintf("%s-%d", start.RunID, i)
					var reply workerReply
					if err := peer.Call(callCtx, "reserve", workerReservation{NodeID: id, Random: true}, &reply); err != nil {
						return nil, err
					}
					if reply.Error != nil {
						return nil, reply.Error.decode()
					}
					var agent model.Agent
					if err := json.Unmarshal(reply.Value, &agent); err != nil {
						return nil, err
					}
					values = append(values, agent.ID)
					if err := peer.Call(callCtx, "release", id, &reply); err != nil {
						return nil, err
					}
					if reply.Error != nil {
						return nil, reply.Error.decode()
					}
				}
				return values, nil
			default:
				return nil, errors.New("unknown placement test operation")
			}
		}, nil
	}
	if err := workerproc.Serve(context.Background(), "controller-placement", factory); err != nil {
		os.Exit(2)
	}
	os.Exit(0)
}

func TestWorkerPlacementPreservesOriginalRandomStream(t *testing.T) {
	const runID = "placement"
	const seed = int64(6728)
	server := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
	for i := 0; i < 7; i++ {
		id := fmt.Sprintf("agent-%d", i)
		server.state.agents[id] = model.Agent{ID: id, Capacity: 100, RunProcesses: true, LastSeen: time.Now(), State: "online"}
	}
	broker := &runWorkerBroker{server: server, runID: runID}
	command := func(string) *exec.Cmd {
		return exec.Command(os.Args[0], "-test.run=^TestControllerPlacementWorkerHelper$", "--", "placement-helper")
	}
	process, err := workerproc.Start(t.Context(), "controller-placement", struct {
		RunID string `json:"runId"`
		Seed  int64  `json:"seed"`
	}{runID, seed}, broker.handle, workerproc.Options{Command: command, Stdout: io.Discard, Stderr: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	broker.process = process
	var got []string
	if err := process.Call(t.Context(), "execute", nil, &got); err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(seed))
	want := make([]string, 64)
	for i := range want {
		want[i] = fmt.Sprintf("agent-%d", rng.Intn(7))
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("worker changed seeded Agent selection: got=%v want=%v", got, want)
	}
	if err := process.Close(); err != nil {
		t.Fatal(err)
	}
}
