package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

// NewRunWorkerHandler owns only ephemeral experiment state. It never opens an
// Agent data directory, Docker socket, public listener, or account credentials.
func NewRunWorkerHandler(ctx context.Context, kind string, raw json.RawMessage, peer *workerproc.Peer) (workerproc.Handler, error) {
	var config RunWorkerConfig
	if err := json.Unmarshal(raw, &config); err != nil {
		return nil, err
	}
	if kind != "agent-run" || config.AgentID == "" || config.RunID == "" || config.RunID != peer.RunID() {
		return nil, errors.New("invalid Agent experiment process bootstrap")
	}
	engine := &Server{config: Config{ID: config.AgentID}, processes: make(map[string]*process), runFences: make(map[string]uint64), client: &http.Client{Timeout: 10 * time.Second, Transport: runWorkerRoundTripper{peer: peer}}}
	var timersMu sync.Mutex
	timers := make(map[string]context.CancelFunc)
	stopTimers := func() {
		timersMu.Lock()
		defer timersMu.Unlock()
		for id, cancel := range timers {
			cancel()
			delete(timers, id)
		}
	}
	go func() { <-peer.Done(); stopTimers() }()
	nodeReply := func(id string) model.Node {
		engine.mu.RLock()
		defer engine.mu.RUnlock()
		if proc := engine.processes[id]; proc != nil {
			return compactRunWorkerNode(cloneNodeStatus(proc.node))
		}
		return model.Node{}
	}
	return func(callCtx context.Context, method string, body json.RawMessage) (any, error) {
		switch method {
		case "node.register":
			var node model.Node
			if err := json.Unmarshal(body, &node); err != nil {
				return nil, err
			}
			if node.ID == "" || node.RunID != config.RunID || node.AgentID != config.AgentID {
				return nil, errors.New("Node does not belong to this experiment process")
			}
			engine.mu.Lock()
			defer engine.mu.Unlock()
			if engine.processes[node.ID] != nil {
				return nil, errors.New("Node is already registered in this experiment process")
			}
			engine.processes[node.ID] = &process{node: node}
			return nil, nil
		case "node.lifecycle":
			var update runWorkerNode
			if err := json.Unmarshal(body, &update); err != nil {
				return nil, err
			}
			engine.mu.Lock()
			proc := engine.processes[update.Node.ID]
			if update.Node.RunID != config.RunID || proc == nil && !update.Exited {
				engine.mu.Unlock()
				return nil, errors.New("unknown experiment process Node")
			}
			if update.Exited {
				delete(engine.processes, update.Node.ID)
				engine.mu.Unlock()
				timersMu.Lock()
				if cancel := timers[update.Node.ID]; cancel != nil {
					cancel()
					delete(timers, update.Node.ID)
				}
				timersMu.Unlock()
				return nil, nil
			}
			// Only the supervisor resolves the real Peer route. Gateway URLs
			// contain /peers/<IP>, which must not become part of the child-to-
			// supervisor operation path. This origin is never dialed: the private
			// transport sends the Node ID and endpoint to the owning supervisor.
			proc.apiURL = ""
			if update.APIURL != "" {
				proc.apiURL = "http://peer-control.invalid"
			}
			proc.exited = update.Exited
			if proc.node.Metadata == nil {
				proc.node.Metadata = make(map[string]string)
			}
			for _, key := range []string{"createRequestHash", "createAdmission", "containerId", "containerCreatedAt", "containerStartedAt", "cleanupComplete", "stoppedAt", "stopRequestedAt"} {
				if value, ok := update.Node.Metadata[key]; ok {
					proc.node.Metadata[key] = value
				}
			}
			if update.Node.State == model.NodeStopping {
				proc.node = update.Node
			}
			engine.mu.Unlock()
			return nil, nil
		case "node.observe":
			var update model.Node
			if err := json.Unmarshal(body, &update); err != nil {
				return nil, err
			}
			if err := engine.updateNodeLocal(update); err != nil {
				return nil, err
			}
			return nodeReply(update.ID), nil
		case "node.snapshots":
			var after string
			if err := json.Unmarshal(body, &after); err != nil {
				return nil, err
			}
			engine.mu.RLock()
			ids := make([]string, 0, len(engine.processes))
			for id := range engine.processes {
				if id > after {
					ids = append(ids, id)
				}
			}
			engine.mu.RUnlock()
			sort.Strings(ids)
			page := runWorkerSnapshots{Done: true}
			bytes := 0
			for _, id := range ids {
				engine.mu.RLock()
				proc := engine.processes[id]
				var node model.Node
				if proc != nil {
					node = cloneNodeStatus(proc.node)
				}
				engine.mu.RUnlock()
				if node.ID == "" {
					continue
				}
				encoded, err := json.Marshal(node)
				if err != nil {
					return nil, err
				}
				if len(page.Nodes) > 0 && bytes+len(encoded) > 1<<20 {
					page.Done = false
					break
				}
				page.Nodes = append(page.Nodes, node)
				page.Next = id
				bytes += len(encoded)
			}
			return page, nil
		case "timer.start":
			var request runWorkerTimer
			if err := json.Unmarshal(body, &request); err != nil {
				return nil, err
			}
			if nodeReply(request.NodeID).ID == "" {
				return nil, errors.New("unknown lifetime timer Node")
			}
			timerCtx, cancel := context.WithCancel(ctx)
			timersMu.Lock()
			if old := timers[request.NodeID]; old != nil {
				old()
			}
			timers[request.NodeID] = cancel
			timersMu.Unlock()
			scheduleLifetimeStop(timerCtx, request.Lifetime, request.CreatedAt, func() { _ = peer.Call(timerCtx, "peer.stop", request.NodeID, nil) })
			return nil, nil
		case "peer.control":
			var request runWorkerControl
			if err := json.Unmarshal(body, &request); err != nil {
				return nil, err
			}
			var result any
			var err error
			switch request.Action {
			case "publish":
				var input model.PublishRequest
				if err = json.Unmarshal(request.Body, &input); err == nil {
					err = engine.proxyPublish(callCtx, request.NodeID, input)
				}
			case "profile":
				var input model.ProfileUpdateRequest
				if err = json.Unmarshal(request.Body, &input); err == nil {
					result, err = engine.proxyProfileUpdate(callCtx, request.NodeID, input)
				}
			case "topology":
				var input model.TopologyRequest
				if err = json.Unmarshal(request.Body, &input); err == nil {
					result, err = engine.proxyTopology(callCtx, request.NodeID, input)
				}
			case "mesh-freeze":
				var input model.MeshFreezeRequest
				if err = json.Unmarshal(request.Body, &input); err == nil {
					result, err = engine.proxyMeshFreeze(callCtx, request.NodeID, input)
				}
			default:
				err = errors.New("unsupported Peer experiment process action")
			}
			reply := runWorkerReply{Node: nodeReply(request.NodeID)}
			if err != nil {
				reply.Error, reply.Status = err.Error(), topologyProxyErrorStatus(err)
			} else if result != nil {
				reply.Result, err = json.Marshal(result)
			}
			return reply, err
		case "run.finish":
			stopTimers()
			return nil, nil
		default:
			return nil, errors.New("unsupported Agent experiment process operation")
		}
	}, nil
}

type runWorkerRoundTripper struct{ peer *workerproc.Peer }

func (t runWorkerRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	var body []byte
	var err error
	if request.Body != nil {
		body, err = io.ReadAll(io.LimitReader(request.Body, model.MaxTopologyRequestBytes+1))
		if err != nil {
			return nil, err
		}
		request.Body.Close()
	}
	if request.Method != http.MethodPost || len(body) > model.MaxTopologyRequestBytes {
		return nil, errors.New("unsupported experiment process HTTP mutation")
	}
	var reply runWorkerHTTPReply
	if err := t.peer.Call(request.Context(), "peer.http", runWorkerHTTP{NodeID: request.Header.Get("X-KPL-Node-ID"), Path: request.URL.Path, Body: body}, &reply); err != nil {
		return nil, err
	}
	return &http.Response{StatusCode: reply.Status, Status: fmt.Sprintf("%d %s", reply.Status, http.StatusText(reply.Status)), Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(reply.Body)), Request: request}, nil
}
