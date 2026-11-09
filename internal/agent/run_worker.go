package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"sync"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

// RunWorkerConfig is passed only over the inherited private worker channel.
// Account credentials, Docker access and host-management keys stay in the Agent.
type RunWorkerConfig struct {
	AgentID string `json:"agentId"`
	RunID   string `json:"runId"`
}

type RunWorkerFactory func(context.Context, RunWorkerConfig, workerproc.Handler) (*workerproc.Process, error)

type agentRunWorker struct {
	runID     string
	process   *workerproc.Process
	ready     chan struct{}
	cancel    context.CancelFunc
	failure   error
	closing   bool
	transport *http.Transport
	closeMu   sync.Mutex
}

func (w *agentRunWorker) call(ctx context.Context, method string, in, out any) error {
	if _, bounded := ctx.Deadline(); !bounded {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
	}
	for {
		err := w.process.Call(ctx, method, in, out)
		if !errors.Is(err, workerproc.ErrBusy) {
			return err
		}
		// ErrBusy is an explicit refusal before handler admission. All other
		// failures can have unknown mutation outcomes and are never replayed.
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

type runWorkerPendingError struct{ cause error }
type runWorkerFailedError struct{ cause error }

func (e *runWorkerFailedError) Error() string {
	return "Run experiment process failed before reset completed: " + e.cause.Error()
}
func (e *runWorkerFailedError) Unwrap() error { return e.cause }

func (e *runWorkerPendingError) Error() string {
	return "Run experiment process has not exited: " + e.cause.Error()
}
func (e *runWorkerPendingError) Unwrap() error { return e.cause }

type runWorkerNode struct {
	Node        model.Node `json:"node"`
	APIURL      string     `json:"apiUrl,omitempty"`
	Exited      bool       `json:"exited,omitempty"`
	CleanupDone bool       `json:"cleanupDone,omitempty"`
}
type runWorkerTimer struct {
	NodeID    string    `json:"nodeId"`
	Lifetime  string    `json:"lifetime"`
	CreatedAt time.Time `json:"createdAt"`
}
type runWorkerControl struct {
	NodeID string          `json:"nodeId"`
	Action string          `json:"action"`
	Body   json.RawMessage `json:"body"`
}
type runWorkerReply struct {
	Node   model.Node      `json:"node"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
	Status int             `json:"status,omitempty"`
}
type runWorkerHTTP struct {
	NodeID string `json:"nodeId"`
	Path   string `json:"path"`
	Body   []byte `json:"body"`
}
type runWorkerHTTPReply struct {
	Status int    `json:"status"`
	Body   []byte `json:"body"`
}
type runWorkerSnapshots struct {
	Nodes []model.Node `json:"nodes"`
	Next  string       `json:"next,omitempty"`
	Done  bool         `json:"done"`
}

func (s *Server) ensureRunWorker(ctx context.Context, runID string) (*agentRunWorker, error) {
	s.runWorkersMu.Lock()
	if existing := s.runWorkers[runID]; existing != nil {
		s.runWorkersMu.Unlock()
		select {
		case <-existing.ready:
			s.runWorkersMu.Lock()
			err := existing.failure
			s.runWorkersMu.Unlock()
			return existing, err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	for id := range s.runWorkers {
		if id != runID {
			s.runWorkersMu.Unlock()
			return nil, errors.New("previous Run experiment process still requires finalization")
		}
	}
	workerCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	transport := &http.Transport{MaxIdleConns: 100, IdleConnTimeout: 90 * time.Second}
	if base, ok := http.DefaultTransport.(*http.Transport); ok {
		transport = base.Clone()
	}
	w := &agentRunWorker{runID: runID, ready: make(chan struct{}), cancel: cancel, transport: transport}
	w.transport.Proxy = nil
	if s.runWorkers == nil {
		s.runWorkers = make(map[string]*agentRunWorker)
	}
	s.runWorkers[runID] = w
	s.runWorkersMu.Unlock()
	process, err := s.config.RunWorkerFactory(workerCtx, RunWorkerConfig{AgentID: s.config.ID, RunID: runID}, s.runWorkerBroker(w))
	if process == nil && err == nil {
		err = errors.New("Run experiment process factory returned no process")
	}
	s.runWorkersMu.Lock()
	w.process, w.failure = process, err
	close(w.ready)
	s.runWorkersMu.Unlock()
	if err != nil {
		cancel()
		return w, err
	}
	s.logger.Info("Agent Run experiment process started", "id", s.config.ID, "run", runID, "pid", process.PID())
	go func() {
		<-process.Done()
		err := process.Wait()
		s.runWorkersMu.Lock()
		unexpected := !w.closing
		if unexpected {
			w.failure = fmt.Errorf("Run experiment process exited unexpectedly: %w", errors.Join(err, errors.New("worker stopped")))
		}
		s.runWorkersMu.Unlock()
		if unexpected {
			s.failRunWorker(w.runID, fmt.Errorf("Run experiment process exited unexpectedly: %w", errors.Join(err, errors.New("worker stopped"))))
		}
	}()
	return w, nil
}

func (s *Server) runWorker(runID string) (*agentRunWorker, error) {
	s.runWorkersMu.Lock()
	w := s.runWorkers[runID]
	if w == nil {
		s.runWorkersMu.Unlock()
		return nil, errors.New("Run experiment process is unavailable")
	}
	select {
	case <-w.ready:
	default:
		s.runWorkersMu.Unlock()
		return nil, errors.New("Run experiment process is starting")
	}
	err := w.failure
	s.runWorkersMu.Unlock()
	if err != nil {
		return nil, err
	}
	return w, nil
}

func (s *Server) failRunWorker(runID string, cause error) {
	s.mu.Lock()
	for _, proc := range s.processes {
		if proc.node.RunID == runID && !proc.exited {
			proc.exitErr = cause
		}
	}
	s.mu.Unlock()
	s.stopRunGeneration(runID, ^uint64(0))
	s.logger.Error("Run experiment process failed; preserving Peer cleanup ownership", "run", runID, "error", cause)
}

func compactRunWorkerNode(node model.Node) model.Node {
	node.Addresses, node.ConnectedPeers, node.RoutingPeers = nil, nil, nil
	node.MeshPeers, node.TopicPeers, node.PeerScores, node.ScoreSample = nil, nil, nil, nil
	return node
}

func (s *Server) applyRunWorkerNode(proc *process, epoch string, node model.Node) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.processes[node.ID] != proc || proc.workerEpoch != epoch || proc.exited || proc.node.State == model.NodeStopping {
		return
	}
	for _, key := range []string{"createRequestHash", "createAdmission", "containerId", "containerCreatedAt", "containerStartedAt", "cleanupComplete", "stoppedAt", "stopRequestedAt"} {
		if value, ok := proc.node.Metadata[key]; ok {
			if node.Metadata == nil {
				node.Metadata = make(map[string]string)
			}
			node.Metadata[key] = value
		}
	}
	proc.node = compactRunWorkerNode(node)
}

func (s *Server) observeRunWorkerNode(update model.Node) error {
	s.mu.RLock()
	proc := s.processes[update.ID]
	if proc == nil {
		s.mu.RUnlock()
		return fmt.Errorf("node %q not found", update.ID)
	}
	id, epoch, terminal := proc.node.RunID, proc.workerEpoch, proc.exited || proc.node.State == model.NodeStopping
	s.mu.RUnlock()
	if terminal {
		return nil
	}
	w, err := s.runWorker(id)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var node model.Node
	if err := w.call(ctx, "node.observe", update, &node); err != nil {
		return err
	}
	s.applyRunWorkerNode(proc, epoch, node)
	return nil
}

func (s *Server) syncRunWorkerNode(proc *process) {
	s.mu.RLock()
	node := runWorkerNode{Node: cloneNodeStatus(proc.node), APIURL: proc.apiURL, Exited: proc.exited, CleanupDone: processCleanupComplete(proc)}
	s.mu.RUnlock()
	w, err := s.runWorker(node.Node.RunID)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.call(ctx, "node.lifecycle", node, nil); err != nil {
		s.failRunWorker(node.Node.RunID, err)
	}
}

func (s *Server) runWorkerControl(ctx context.Context, proc *process, action string, input, output any) error {
	if _, bounded := ctx.Deadline(); !bounded {
		timeout := 10 * time.Second
		if action == "topology" {
			timeout = model.TopologyProxyTimeout
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	s.mu.RLock()
	id, runID, epoch := proc.node.ID, proc.node.RunID, proc.workerEpoch
	s.mu.RUnlock()
	w, err := s.runWorker(runID)
	if err != nil {
		return err
	}
	body, err := json.Marshal(input)
	if err != nil {
		return err
	}
	var reply runWorkerReply
	if err := w.call(ctx, "peer.control", runWorkerControl{NodeID: id, Action: action, Body: body}, &reply); err != nil {
		return err
	}
	s.applyRunWorkerNode(proc, epoch, reply.Node)
	if reply.Error != "" {
		if reply.Status == http.StatusServiceUnavailable || reply.Status == http.StatusGatewayTimeout {
			return &topologyPeerResponseError{statusCode: reply.Status, status: http.StatusText(reply.Status), message: reply.Error}
		}
		return errors.New(reply.Error)
	}
	if output != nil && len(reply.Result) > 0 {
		return json.Unmarshal(reply.Result, output)
	}
	return nil
}

func (s *Server) runWorkerBroker(w *agentRunWorker) workerproc.Handler {
	return func(ctx context.Context, method string, raw json.RawMessage) (any, error) {
		s.runWorkersMu.Lock()
		process := w.process
		closing := w.closing
		s.runWorkersMu.Unlock()
		if process == nil || closing {
			return nil, errors.New("Run process broker is not active")
		}
		if method == "peer.stop" {
			var id string
			if err := json.Unmarshal(raw, &id); err != nil {
				return nil, err
			}
			s.mu.RLock()
			proc := s.processes[id]
			owned := proc != nil && proc.node.RunID == w.runID && proc.workerEpoch == process.Epoch()
			s.mu.RUnlock()
			if !owned {
				return nil, errors.New("stale Run process stop request")
			}
			return nil, s.stopNode(id)
		}
		if method != "peer.http" {
			return nil, errors.New("unsupported Run process broker operation")
		}
		var request runWorkerHTTP
		if err := json.Unmarshal(raw, &request); err != nil {
			return nil, err
		}
		limit := int64(4097)
		switch request.Path {
		case "/publish", "/mesh-freeze":
		case "/profile":
			limit = model.MaxProfileUpdateBytes + 1
		case "/topology":
			limit = model.MaxTopologyRequestBytes + 1
		default:
			return nil, errors.New("unsupported Peer control path")
		}
		if len(request.Body) > model.MaxTopologyRequestBytes {
			return nil, errors.New("Peer control request is too large")
		}
		s.mu.RLock()
		proc := s.processes[request.NodeID]
		valid := proc != nil && proc.node.RunID == w.runID && proc.workerEpoch == process.Epoch() && !proc.exited && proc.node.State == model.NodeReady
		if valid {
			fence, fenced := s.runFences[w.runID]
			valid = !s.fencingAll && (!fenced || proc.node.Generation > fence)
		}
		apiURL := ""
		if valid {
			apiURL = proc.apiURL
		}
		s.mu.RUnlock()
		if !valid || apiURL == "" {
			return nil, errors.New("stale or inactive Peer control target")
		}
		ctx, cancel := context.WithTimeout(ctx, model.TopologyProxyTimeout)
		defer cancel()
		r, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL+request.Path, bytes.NewReader(request.Body))
		if err != nil {
			return nil, err
		}
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-KPL-Node-ID", request.NodeID)
		r.Header.Set("Authorization", "Bearer "+s.config.Token)
		response, err := (&http.Client{Transport: w.transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}).Do(r)
		if err != nil {
			return runWorkerHTTPReply{Status: http.StatusServiceUnavailable, Body: []byte("Peer control transport is unavailable")}, nil
		}
		defer response.Body.Close()
		body, err := io.ReadAll(io.LimitReader(response.Body, limit))
		if err != nil {
			return nil, err
		}
		s.mu.RLock()
		valid = s.processes[request.NodeID] == proc && !proc.exited && proc.node.State == model.NodeReady
		s.mu.RUnlock()
		if !valid {
			return nil, errors.New("Peer control target changed before acknowledgement")
		}
		return runWorkerHTTPReply{Status: response.StatusCode, Body: body}, nil
	}
}

func (s *Server) finishRunWorker(ctx context.Context, id string) error {
	if s.history != nil {
		s.history.mu.Lock()
		err := s.history.saveFence(id, ^uint64(0))
		s.history.mu.Unlock()
		if err != nil {
			return &runWorkerPendingError{err}
		}
	}
	if err := s.prepareRunReset(ctx, id); err != nil {
		return err
	}
	if _, err := s.inspectRunReset(id, false); err != nil {
		return err
	}
	// Peer wait calls own a separate Docker HTTP pool. Once every wait has
	// returned, release its idle sockets at this Run boundary as well.
	if s.docker != nil && s.docker.waitClient != nil {
		s.docker.waitClient.CloseIdleConnections()
	}
	s.runWorkersMu.Lock()
	w := s.runWorkers[id]
	s.runWorkersMu.Unlock()
	if w == nil {
		s.clearRunNetworkState(id)
		proof, err := s.proveRunReset(id)
		if err == nil {
			s.logger.Info("Agent Run reset verified", "id", s.config.ID, "run", id, "reset", proof)
		}
		return err
	}
	select {
	case <-w.ready:
	case <-ctx.Done():
		return &runWorkerPendingError{ctx.Err()}
	}
	w.closeMu.Lock()
	defer w.closeMu.Unlock()
	s.runWorkersMu.Lock()
	failure := w.failure
	w.closing = true
	s.runWorkersMu.Unlock()
	if w.process != nil {
		select {
		case <-w.process.Done():
			if failure == nil {
				failure = errors.Join(w.process.Wait(), errors.New("process exited before requested finalization"))
			}
		default:
			if err := w.call(ctx, "run.finish", nil, nil); err != nil {
				if ctx.Err() != nil {
					return &runWorkerPendingError{err}
				}
				failure = errors.Join(failure, err)
			}
			if err := w.process.CloseContext(ctx); err != nil {
				select {
				case <-w.process.Done():
					failure = errors.Join(failure, err)
				default:
					return &runWorkerPendingError{err}
				}
			}
		}
		if err := w.process.Wait(); err != nil {
			var exit *exec.ExitError
			if errors.As(err, &exit) && exit.ExitCode() > 0 {
				failure = errors.Join(failure, err)
			}
		}
	}
	w.cancel()
	w.transport.CloseIdleConnections()
	if _, err := s.inspectRunReset(id, false); err != nil {
		return err
	}
	s.clearRunNetworkState(id)
	proof, err := s.proveRunReset(id)
	if err != nil {
		return err
	}
	s.runWorkersMu.Lock()
	delete(s.runWorkers, id)
	s.runWorkersMu.Unlock()
	if w.process != nil {
		s.logger.Info("Agent Run experiment process reaped", "id", s.config.ID, "run", id, "pid", w.process.PID())
	}
	s.logger.Info("Agent Run reset verified", "id", s.config.ID, "run", id, "reset", proof)
	if failure != nil {
		return &runWorkerFailedError{failure}
	}
	return nil
}

func (s *Server) closeRunWorkers() error {
	s.runWorkersMu.Lock()
	workers := make([]*agentRunWorker, 0, len(s.runWorkers))
	for _, w := range s.runWorkers {
		w.closing = true
		workers = append(workers, w)
	}
	s.runWorkersMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var failures []error
	for _, w := range workers {
		// Destruction is not a successful Run reset: never recurse into
		// heartbeat/finalization while releasing local ownership.
		w.cancel()
		select {
		case <-w.ready:
		case <-ctx.Done():
			failures = append(failures, ctx.Err())
			continue
		}
		if w.process != nil {
			if err := w.process.CloseContext(ctx); err != nil {
				failures = append(failures, err)
			}
		}
		w.transport.CloseIdleConnections()
	}
	return errors.Join(failures...)
}

// Heavy snapshots remain in the experiment process. Heartbeat/status responses
// receive temporary copies, never another retained topology cache in the Agent.
func (s *Server) enrichRunWorkerNodes(ctx context.Context, nodes []model.Node) ([]model.Node, error) {
	if s.config.RunWorkerFactory == nil {
		return nodes, nil
	}
	byRun := make(map[string][]model.Node)
	for _, node := range nodes {
		if node.State == model.NodeStarting || node.State == model.NodeReady {
			byRun[node.RunID] = nil
		}
	}
	for id := range byRun {
		worker, err := s.runWorker(id)
		if err != nil {
			return nil, err
		}
		budget := s.runWorkerSnapshotBudget
		if budget <= 0 {
			budget = 8 * time.Second
		}
		readCtx, cancel := context.WithTimeout(ctx, budget)
		var snapshots []model.Node
		after := ""
		for {
			var page runWorkerSnapshots
			err = worker.call(readCtx, "node.snapshots", after, &page)
			if err != nil {
				break
			}
			snapshots = append(snapshots, page.Nodes...)
			if page.Done {
				break
			}
			if page.Next == "" || page.Next == after {
				err = errors.New("Run process snapshot made no progress")
				break
			}
			after = page.Next
		}
		cancel()
		if err != nil {
			return nil, fmt.Errorf("read complete Run process snapshot: %w", err)
		}
		byRun[id] = snapshots
	}
	for i, node := range nodes {
		for _, snapshot := range byRun[node.RunID] {
			if snapshot.ID != node.ID {
				continue
			}
			s.mu.RLock()
			proc := s.processes[node.ID]
			active := proc != nil && !proc.exited && proc.node.State != model.NodeStopping
			s.mu.RUnlock()
			if active {
				nodes[i].Addresses, nodes[i].ConnectedPeers, nodes[i].RoutingPeers = snapshot.Addresses, snapshot.ConnectedPeers, snapshot.RoutingPeers
				nodes[i].MeshPeers, nodes[i].TopicPeers, nodes[i].PeerScores, nodes[i].ScoreSample = snapshot.MeshPeers, snapshot.TopicPeers, snapshot.PeerScores, snapshot.ScoreSample
				nodes[i].OverlayObservedAt = snapshot.OverlayObservedAt
			}
			break
		}
	}
	return nodes, nil
}
