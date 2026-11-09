package controller

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

type workerRunStart struct {
	RunID           string           `json:"runId"`
	Experiment      model.Experiment `json:"experiment"`
	Scenario        []byte           `json:"scenario"`
	DataDir         string           `json:"dataDir"`
	NetworkManager  bool             `json:"networkManager"`
	RunMinFreeBytes uint64           `json:"runMinFreeBytes"`
}

type workerInventory struct {
	Experiment  model.Experiment     `json:"experiment"`
	Agents      []model.Agent        `json:"agents"`
	Nodes       []model.Node         `json:"nodes"`
	ReportTimes map[string]time.Time `json:"reportTimes"`
}

type workerGeneration struct {
	RunID      string `json:"runId"`
	Generation uint64 `json:"generation"`
}
type workerReservation struct {
	NodeID, AgentID string
	Random          bool
	Seed            int64
}
type workerCreated struct {
	Request  model.CreateNodeRequest
	AgentID  string
	Node     model.Node
	Expected []time.Time
}
type workerCreatedResult struct {
	Accepted   bool
	Node       model.Node
	ReportedAt time.Time
}

type workerAdmission struct {
	Agent  model.Agent
	After  time.Duration
	Reason string
}
type workerPhaseTiming struct {
	Phase    int
	Finished bool
	At       time.Time
}
type workerAgentCall struct {
	BaseURL, Method, Path string
	Input                 json.RawMessage
	Output                bool
}
type workerDomainError struct {
	Message       string        `json:"message"`
	Kind          string        `json:"kind,omitempty"`
	Remaining     int           `json:"remaining,omitempty"`
	WorkerPending bool          `json:"workerPending,omitempty"`
	Canceled      bool          `json:"canceled,omitempty"`
	Deadline      bool          `json:"deadline,omitempty"`
	After         time.Duration `json:"after,omitempty"`
	Status        int           `json:"status,omitempty"`
}
type workerReply struct {
	Value json.RawMessage    `json:"value,omitempty"`
	Error *workerDomainError `json:"error,omitempty"`
}
type workerJournalCursor struct {
	Size int64 `json:"size"`
}
type workerHistogram struct {
	AgentID, Topic string
	Count          uint64
	Sum            float64
	Buckets        []uint64
}
type runWorkerSummary struct {
	RunID            string                `json:"runId"`
	Metrics          model.Metrics         `json:"metrics"`
	Histograms       []workerHistogram     `json:"histograms"`
	At               time.Time             `json:"at"`
	NextRefreshAt    time.Time             `json:"nextRefreshAt,omitempty"`
	Cursor           int64                 `json:"cursor"`
	SourceRevision   string                `json:"sourceRevision,omitempty"`
	Retired          bool                  `json:"retired,omitempty"`
	Counters         []workerCounterSeries `json:"counters,omitempty"`
	CountersComplete bool                  `json:"countersComplete,omitempty"`
	Nodes            []workerNodeCount     `json:"nodes,omitempty"`
}

type runWorkerClient struct {
	peer              *workerproc.Peer
	server            *Server
	runID             string
	ctx               context.Context
	syncMu            sync.Mutex
	inventoryRevision uint64
	journalMu         sync.Mutex
	journal           *os.File
	journalInfo       os.FileInfo
	offset            int64
	accumulator       *runMetricAccumulator
	placementMu       sync.Mutex
	placementRNG      map[string]*rand.Rand
	observationOnce   sync.Once
	observationSlot   chan struct{}
}

// WorkerFactory is reached only through workerproc's inherited private pipes.
// Public Controller and Agent listeners never expose these methods.
func WorkerFactory(ctx context.Context, kind string, bootstrap json.RawMessage, peer *workerproc.Peer) (workerproc.Handler, error) {
	if kind == "controller-journal" {
		return journalIndexWorkerFactory(ctx, bootstrap, peer)
	}
	if kind == "controller-analysis" {
		return analysisWorkerFactory(ctx, bootstrap, peer)
	}
	if kind == "controller-summary" {
		return summaryWorkerFactory(ctx, bootstrap, peer)
	}
	if kind == "controller-export" {
		return exportWorkerFactory(ctx, bootstrap, peer)
	}
	if kind != "controller-run" {
		return nil, errors.New("unknown Controller worker kind")
	}
	var start workerRunStart
	if err := json.Unmarshal(bootstrap, &start); err != nil {
		return nil, err
	}
	if !validResultID(start.RunID) || start.RunID != peer.RunID() || start.Experiment.ID != start.RunID {
		return nil, errors.New("worker Run identity mismatch")
	}
	spec, err := scenario.Parse(start.Scenario)
	if err != nil {
		return nil, err
	}
	config := ServerConfig{DataDir: start.DataDir, RunMinFreeBytes: start.RunMinFreeBytes}
	if start.NetworkManager {
		config.NetworkManagerURL = "parent-broker"
	}
	s := New(config, slog.Default())
	client := &runWorkerClient{peer: peer, server: s, runID: start.RunID, ctx: ctx, accumulator: newLiveRunMetricAccumulator()}
	s.worker, s.state.worker = client, client
	s.state.experiments[start.RunID] = start.Experiment
	s.state.runMetrics[start.RunID] = client.accumulator
	var runMu sync.Mutex
	var cancel context.CancelFunc
	started := false
	return func(callCtx context.Context, method string, input json.RawMessage) (any, error) {
		switch method {
		case "placement-draw":
			var id string
			if err := json.Unmarshal(input, &id); err != nil {
				return nil, err
			}
			client.placementMu.Lock()
			defer client.placementMu.Unlock()
			rng := client.placementRNG[id]
			if rng == nil {
				return nil, errors.New("unknown placement RNG lease")
			}
			return rng.Int63(), nil
		case "cancel":
			runMu.Lock()
			if cancel != nil {
				cancel()
			}
			runMu.Unlock()
			return nil, nil
		case "execute":
			runMu.Lock()
			if started {
				runMu.Unlock()
				return nil, errors.New("worker already executed its Run")
			}
			started = true
			runCtx, stop := context.WithCancel(callCtx)
			cancel = stop
			client.ctx = callCtx
			runMu.Unlock()
			defer stop()
			if err := client.syncInventory(callCtx); err != nil {
				return nil, err
			}
			loopCtx, loopCancel := context.WithCancel(callCtx)
			loopDone := make(chan struct{})
			go func() { defer close(loopDone); client.observeLoop(loopCtx, stop) }()
			s.runScenario(runCtx, start.Experiment, spec)
			loopCancel()
			<-loopDone
			if err := client.syncInventory(callCtx); err != nil {
				return nil, err
			}
			if err := client.syncJournal(callCtx); err != nil {
				return nil, err
			}
			if err := client.publishSummary(callCtx); err != nil {
				return nil, err
			}
			if client.journal != nil {
				_ = client.journal.Close()
				client.journal = nil
			}
			return nil, nil
		default:
			return nil, errors.New("unsupported Controller worker operation")
		}
	}, nil
}

func encodeWorkerError(err error) *workerDomainError {
	if err == nil {
		return nil
	}
	e := &workerDomainError{Message: err.Error(), Canceled: errors.Is(err, context.Canceled), Deadline: errors.Is(err, context.DeadlineExceeded)}
	if len(e.Message) > 4096 {
		e.Message = e.Message[:4096]
	}
	var pending *agentRunDrainPendingError
	if errors.As(err, &pending) && pending.remaining > 0 {
		e.Remaining = pending.remaining
	}
	e.WorkerPending = hasPendingRunWorker(err)
	var uncertain *uncertainNodeCreateError
	var deferred *agentAdmissionDeferredError
	var topology *topologyAgentError
	switch {
	case errors.As(err, &uncertain):
		e.Kind = "uncertain"
	case errors.As(err, &deferred):
		e.Kind = "deferred"
		e.After = deferred.after
	case errors.Is(err, errAgentCapacityReached):
		e.Kind = "capacity"
	case errors.As(err, &topology):
		e.Kind = "topology"
		e.Status = topology.status
	}
	return e
}

func (e *workerDomainError) decode() error {
	if e == nil {
		return nil
	}
	var causes []error
	causes = append(causes, errors.New(e.Message))
	if e.Canceled {
		causes = append(causes, context.Canceled)
	}
	if e.Deadline {
		causes = append(causes, context.DeadlineExceeded)
	}
	if e.Remaining > 0 {
		causes = append(causes, &agentRunDrainPendingError{remaining: e.Remaining, message: e.Message})
	}
	if e.WorkerPending {
		causes = append(causes, &agentRunWorkerPendingError{message: e.Message})
	}
	err := errors.Join(causes...)
	switch e.Kind {
	case "uncertain":
		return &uncertainNodeCreateError{err}
	case "deferred":
		return &agentAdmissionDeferredError{after: e.After, reason: e.Message}
	case "capacity":
		return errAgentCapacityReached
	case "topology":
		return &topologyAgentError{status: e.Status, message: e.Message}
	}
	return err
}

func (c *runWorkerClient) call(ctx context.Context, method string, input, output any) error {
	if ctx == context.Background() {
		ctx = c.ctx
	}
	var reply workerReply
	if err := c.peer.Call(ctx, method, input, &reply); err != nil {
		return &workerCallTransportError{err}
	}
	if reply.Error != nil {
		return reply.Error.decode()
	}
	if output != nil && len(reply.Value) > 0 {
		return json.Unmarshal(reply.Value, output)
	}
	return nil
}

func (c *runWorkerClient) command(ctx context.Context, method string, input any) error {
	err := c.call(ctx, method, input, nil)
	_ = c.syncInventory(context.WithoutCancel(ctx))
	return err
}

func (c *runWorkerClient) syncInventory(ctx context.Context) error {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	snapshot, err := c.readInventory(ctx)
	if err != nil {
		return err
	}
	s := c.server.state
	s.mu.Lock()
	s.agents = make(map[string]model.Agent, len(snapshot.Agents))
	for _, agent := range snapshot.Agents {
		s.agents[agent.ID] = agent
	}
	if snapshot.full {
		s.nodes = make(map[string]model.Node, len(snapshot.Nodes))
		s.activeNodeIDs = nil
		s.nodeVersions = nil
		s.nodeReportTimes = make(map[string]time.Time, len(snapshot.ReportTimes))
	}
	for _, node := range snapshot.Nodes {
		s.setNodeLocked(node)
		s.nodeReportTimes[node.ID] = snapshot.ReportTimes[node.ID]
	}
	s.experiments[c.runID] = snapshot.Experiment
	s.mu.Unlock()
	c.inventoryRevision = snapshot.revision
	return nil
}

func (c *runWorkerClient) update(runID string, update func(*model.Experiment)) {
	if runID != c.runID {
		return
	}
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	s := c.server.state
	s.mu.Lock()
	experiment := s.experiments[runID]
	update(&experiment)
	s.experiments[runID] = experiment
	s.mu.Unlock()
	if err := c.call(c.ctx, "update", experiment, nil); err != nil {
		s.recordRunWriteError(runID, err)
	}
}

func (c *runWorkerClient) callAgent(ctx context.Context, baseURL, method, path string, input, output any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var raw json.RawMessage
	if input != nil {
		data, err := json.Marshal(input)
		if err != nil {
			return err
		}
		raw = data
	}
	var result json.RawMessage
	if err := c.call(ctx, "agent", workerAgentCall{BaseURL: baseURL, Method: method, Path: path, Input: raw, Output: output != nil}, &result); err != nil {
		if method == http.MethodPost && path == "/api/v1/nodes" {
			return classifyWorkerCreateTransport(err)
		}
		return err
	}
	if output != nil && len(result) > 0 {
		return json.Unmarshal(result, output)
	}
	return nil
}

func (c *runWorkerClient) syncJournal(ctx context.Context) error {
	c.journalMu.Lock()
	defer c.journalMu.Unlock()
	var cursor workerJournalCursor
	if err := c.call(ctx, "journal", nil, &cursor); err != nil {
		return err
	}
	if cursor.Size == c.offset {
		return nil
	}
	path := filepath.Join(c.server.config.DataDir, currentRunsDirectory, c.runID, "events.jsonl")
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) && cursor.Size == 0 {
		return nil
	}
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	if c.journal == nil || !os.SameFile(info, c.journalInfo) {
		if c.journal != nil {
			_ = c.journal.Close()
		}
		c.journal, c.journalInfo, c.offset = f, info, 0
	} else {
		f.Close()
	}
	if cursor.Size < c.offset {
		return errors.New("committed journal moved backwards")
	}
	reader := io.NewSectionReader(c.journal, c.offset, cursor.Size-c.offset)
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64<<10), 16<<20)
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var event model.TraceEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return err
		}
		if event.RunID != c.runID {
			return errors.New("foreign Run in worker journal")
		}
		c.accumulator.observe(event)
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	c.offset = cursor.Size
	return nil
}

func (c *runWorkerClient) publishSummary(ctx context.Context) error {
	c.journalMu.Lock()
	defer c.journalMu.Unlock()
	now := time.Now().UTC()
	metrics, histograms := c.accumulator.livePrometheusSummary(c.runID, now)
	summary := runWorkerSummary{RunID: c.runID, Metrics: metrics, At: now, Cursor: c.offset}
	for key, histogram := range histograms {
		row := workerHistogram{AgentID: key.agentID, Topic: key.topic, Count: histogram.count, Sum: histogram.sum}
		for _, bound := range propagationBounds {
			row.Buckets = append(row.Buckets, histogram.buckets[bound])
		}
		summary.Histograms = append(summary.Histograms, row)
	}
	c.accumulator.dataMu.RLock()
	for _, publication := range c.accumulator.window.publications {
		if publication.deadline.After(now) && (summary.NextRefreshAt.IsZero() || publication.deadline.Before(summary.NextRefreshAt)) {
			summary.NextRefreshAt = publication.deadline
		}
	}
	c.accumulator.dataMu.RUnlock()
	return c.call(ctx, "summary", summary, nil)
}

func (c *runWorkerClient) observeLoop(ctx context.Context, cancelRun context.CancelFunc) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	lastSummary := time.Time{}
	lastObservation := time.Time{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if err := c.syncInventory(ctx); err != nil {
			if ctx.Err() == nil {
				c.server.state.recordRunWriteError(c.runID, err)
				cancelRun()
			}
			return
		}
		if err := c.syncJournal(ctx); err != nil {
			if ctx.Err() == nil {
				c.server.state.recordRunWriteError(c.runID, err)
				cancelRun()
			}
			return
		}
		if time.Since(lastObservation) >= 5*time.Second {
			if err := c.server.state.recordAnalysisObservation(c.runID, time.Now().UTC(), ctx); err != nil {
				if ctx.Err() == nil {
					c.server.state.recordRunWriteError(c.runID, err)
					cancelRun()
				}
				return
			}
			lastObservation = time.Now()
		}
		if time.Since(lastSummary) >= time.Second {
			if err := c.publishSummary(ctx); err != nil {
				if ctx.Err() == nil {
					c.server.state.recordRunWriteError(c.runID, err)
					cancelRun()
				}
				return
			}
			lastSummary = time.Now()
		}
	}
}

func (s *Server) runScenarioWorker(parent context.Context, experiment model.Experiment, spec scenario.Scenario) {
	lifecycle, cancel := context.WithCancel(context.WithoutCancel(parent))
	defer cancel()
	dataDir, err := filepath.Abs(s.config.DataDir)
	if err != nil {
		s.failRunWorker(experiment.ID, err)
		return
	}
	start := workerRunStart{RunID: experiment.ID, Experiment: experiment, Scenario: []byte(experiment.ScenarioYAML), DataDir: dataDir, NetworkManager: s.config.NetworkManagerURL != "", RunMinFreeBytes: s.config.RunMinFreeBytes}
	if len(start.Scenario) == 0 {
		data, e := os.ReadFile(filepath.Join(dataDir, currentRunsDirectory, experiment.ID, "scenario.yaml"))
		if e != nil {
			s.failRunWorker(experiment.ID, e)
			return
		}
		start.Scenario = data
	}
	broker := &runWorkerBroker{server: s, runID: experiment.ID, executionID: experiment.ExecutionID, generation: 1}
	options := workerproc.Options{RunID: experiment.ID, Command: s.workerCommand}
	process, err := workerproc.Start(lifecycle, "controller-run", start, broker.handle, options)
	if err != nil {
		err = errors.Join(err, broker.close())
		if process != nil {
			s.rememberUnreapedWorker(experiment.ID, process)
		}
		s.failRunWorker(experiment.ID, err)
		return
	}
	broker.process = process
	s.logger.Info("experiment worker started", "run", experiment.ID, "pid", process.PID(), "epoch", process.Epoch())
	finished := make(chan error, 1)
	go func() { finished <- process.Call(lifecycle, "execute", nil, nil) }()
	select {
	case err = <-finished:
	case <-parent.Done():
		requestCtx, stop := context.WithTimeout(lifecycle, 5*time.Second)
		cancelErr := process.Call(requestCtx, "cancel", nil, nil)
		stop()
		if cancelErr != nil {
			err = cancelErr
		} else {
			budget, _ := time.ParseDuration(spec.JobShutdownTimeout)
			if budget <= 0 {
				budget = 3 * time.Minute
			}
			timer := time.NewTimer(3*budget + 10*time.Second)
			select {
			case err = <-finished:
			case <-timer.C:
				err = errors.New("experiment worker cleanup deadline exceeded")
			}
			timer.Stop()
		}
	}
	err = errors.Join(err, broker.close())
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	closeErr := process.CloseContext(closeCtx)
	closeCancel()
	select {
	case <-process.Done():
	default:
		s.rememberUnreapedWorker(experiment.ID, process)
	}
	if err = errors.Join(err, closeErr); err != nil {
		s.failRunWorker(experiment.ID, err)
	} else {
		broker.mu.Lock()
		sealed := broker.fenced && broker.drained
		broker.mu.Unlock()
		if !sealed {
			err = errors.New("worker did not confirm Peer cleanup and final delivery")
			s.failRunWorker(experiment.ID, err)
		} else {
			resetCtx, stop := context.WithTimeout(lifecycle, 30*time.Second)
			broker.mu.Lock()
			final := broker.final
			broker.mu.Unlock()
			resetErr := s.waitRunTerminalEvidence(resetCtx, experiment.ID)
			if final == nil {
				resetErr = errors.New("worker omitted final Run status")
			}
			if resetErr == nil {
				resetErr = s.retireRunRuntime(resetCtx, experiment.ID, final)
			}
			stop()
			if resetErr != nil {
				err = resetErr
				s.failRunWorker(experiment.ID, resetErr)
			}
		}
	}
	s.cancelMu.Lock()
	delete(s.cancels, experiment.ID)
	s.cancelMu.Unlock()
	s.logger.Info("experiment worker exited", "run", experiment.ID, "pid", process.PID(), "error", err)
}

func (s *Server) failRunWorker(id string, cause error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	stopErr := s.stopRunGeneration(ctx, id, ^uint64(0))
	refreshErr := s.refreshAgentState(ctx, id)
	drainErr := s.drainRunTelemetry(ctx, id)
	cleanupErr := errors.Join(stopErr, refreshErr)
	if hasPendingRunWorker(drainErr) {
		cleanupErr = errors.Join(cleanupErr, drainErr)
	}
	s.updateExperiment(id, func(run *model.Experiment) {
		run.State = "failed"
		run.FinishedAt = time.Now().UTC()
		run.Error = strings.Join(nonemptyStrings(run.Error, "Experiment worker failed: "+cause.Error()), "; ")
		run.DataState = "incomplete"
		run.IntegrityError = strings.Join(nonemptyStrings(run.IntegrityError, run.Error), "; ")
		run.TelemetryPending = hasPendingRunTelemetry(drainErr)
		run.CleanupState = "failed"
		run.CleanupError = errors.Join(cause, cleanupErr).Error()
	})
	s.cancelMu.Lock()
	s.rememberQueueCleanupLocked(id)
	if err := s.persistQueueCleanupLocked(); err != nil {
		s.logger.Error("persist worker cleanup obligation", "run", id, "error", err)
	}
	s.cancelMu.Unlock()
}

type runWorkerBroker struct {
	server             *Server
	process            *workerproc.Process
	runID, executionID string
	mu                 sync.Mutex
	closed             bool
	generation         uint64
	fenced             bool
	drained            bool
	final              *model.Experiment
	inventoryLease     *workerInventoryLease
	createLeases       map[string]*workerCreateLease
	createFenced       bool
	observationMu      sync.Mutex
	observationStage   *workerObservationStage
}

func (b *runWorkerBroker) close() error {
	b.mu.Lock()
	b.closed = true
	b.createLeases = nil
	b.clearInventoryLeaseLocked()
	b.mu.Unlock()
	return b.closeObservationStage()
}
func (b *runWorkerBroker) handle(ctx context.Context, method string, raw json.RawMessage) (any, error) {
	b.mu.Lock()
	closed := b.closed
	b.mu.Unlock()
	if closed {
		return nil, errors.New("Run worker lease is closed")
	}
	value, err := b.dispatch(ctx, method, raw)
	reply := workerReply{Error: encodeWorkerError(err)}
	if err == nil && value != nil {
		reply.Value, err = json.Marshal(value)
		if err != nil {
			reply.Error = encodeWorkerError(err)
		}
	}
	return reply, nil
}
func (b *runWorkerBroker) inventory() workerInventory {
	snapshot, _ := b.inventorySince(0)
	return snapshot.workerInventory
}

func (b *runWorkerBroker) inventorySince(since uint64) (workerInventoryUpdate, error) {
	s := b.server.state
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reconcileNodeVersionsLocked()
	if since > s.nodeRevision {
		return workerInventoryUpdate{}, errors.New("inventory revision is ahead of the current state")
	}
	full := since == 0 || s.nodeRemovalRevision > since
	result := workerInventoryUpdate{workerInventory: workerInventory{Experiment: s.experiments[b.runID], ReportTimes: make(map[string]time.Time)}, revision: s.nodeRevision, full: full}
	for _, agent := range s.agents {
		result.Agents = append(result.Agents, agent)
	}
	for _, node := range s.nodes {
		if node.RunID == b.runID && (full || s.nodeVersions[node.ID] > since) {
			result.Nodes = append(result.Nodes, node)
			result.ReportTimes[node.ID] = s.nodeReportTimes[node.ID]
		}
	}
	return result, nil
}
func (b *runWorkerBroker) dispatch(ctx context.Context, method string, raw json.RawMessage) (any, error) {
	s := b.server
	switch method {
	case "inventory":
		var request workerInventoryRequest
		if len(raw) > 0 && string(raw) != "null" {
			if err := json.Unmarshal(raw, &request); err != nil {
				return nil, err
			}
		}
		return b.inventoryPage(ctx, request)
	case "reserve":
		var r workerReservation
		if err := json.Unmarshal(raw, &r); err != nil {
			return nil, err
		}
		if !strings.HasPrefix(r.NodeID, b.runID+"-") {
			return nil, errors.New("foreign reservation")
		}
		if err := b.checkCreateReservation(r.NodeID); err != nil {
			return nil, err
		}
		var rng *rand.Rand
		source := &workerPlacementSource{ctx: ctx, process: b.process, nodeID: r.NodeID}
		if r.Random {
			rng = rand.New(source)
		}
		agent, err := s.acquireAgentWithPlacement(ctx, r.NodeID, r.AgentID, rng)
		if source.err != nil {
			s.releaseReservation(r.NodeID)
			return nil, errors.Join(err, source.err)
		}
		if err == nil {
			if err = b.bindCreateReservation(r.NodeID, agent); err != nil {
				s.releaseReservation(r.NodeID)
				return nil, err
			}
		}
		return agent, err
	case "release":
		var id string
		if err := json.Unmarshal(raw, &id); err != nil {
			return nil, err
		}
		if !strings.HasPrefix(id, b.runID+"-") {
			return nil, errors.New("foreign reservation")
		}
		s.releaseReservation(id)
		b.releaseCreateLease(id)
		return nil, nil
	case "uncertain":
		var r workerCreated
		if err := json.Unmarshal(raw, &r); err != nil {
			return nil, err
		}
		if r.Request.RunID != b.runID {
			return nil, errors.New("foreign uncertain creation")
		}
		s.retainUncertainCreate(r.Request, r.AgentID)
		return nil, nil
	case "defer-admission":
		var r workerAdmission
		if err := json.Unmarshal(raw, &r); err != nil {
			return nil, err
		}
		s.deferAgentAdmission(r.Agent, &agentAdmissionDeferredError{after: r.After, reason: r.Reason}, b.runID)
		return nil, nil
	case "mark-stopping":
		var id string
		if err := json.Unmarshal(raw, &id); err != nil {
			return nil, err
		}
		s.state.mu.RLock()
		node, ok := s.state.nodes[id]
		s.state.mu.RUnlock()
		if !ok || node.RunID != b.runID {
			return nil, errors.New("foreign Peer stop")
		}
		s.markNodeStoppingAndReleaseCapacity(id)
		return nil, nil
	case "remember":
		var agent model.Agent
		if err := json.Unmarshal(raw, &agent); err != nil {
			return nil, err
		}
		current, ok := s.agent(agent.ID)
		if !ok || current.URL != agent.URL || !current.StartedAt.Equal(agent.StartedAt) || !current.RunProcesses {
			return nil, errors.New("Agent does not support the current Run worker lease")
		}
		return nil, s.rememberRunAgent(b.runID, current)
	case "created":
		var r workerCreated
		if err := json.Unmarshal(raw, &r); err != nil {
			return nil, err
		}
		if r.Request.RunID != b.runID {
			return nil, errors.New("foreign create acknowledgement")
		}
		accepted := s.recordCreatedNode(r.Request, r.AgentID, r.Node, r.Expected...)
		if accepted {
			b.releaseCreateLease(r.Request.ID)
		}
		s.state.mu.RLock()
		node := s.state.nodes[r.Request.ID]
		at := s.state.nodeReportTimes[r.Request.ID]
		s.state.mu.RUnlock()
		return workerCreatedResult{Accepted: accepted, Node: node, ReportedAt: at}, nil
	case "update":
		var incoming model.Experiment
		if err := json.Unmarshal(raw, &incoming); err != nil {
			return nil, err
		}
		if incoming.ID != b.runID || incoming.ExecutionID != b.executionID {
			return nil, errors.New("foreign experiment update")
		}
		terminal := incoming.State == "completed" || incoming.State == "failed" || incoming.State == "canceled"
		if terminal {
			b.mu.Lock()
			copy := incoming
			b.final = &copy
			b.mu.Unlock()
		}
		s.updateExperiment(b.runID, func(current *model.Experiment) {
			mergeWorkerExperiment(current, incoming)
			if terminal {
				current.State = "running"
				current.FinishedAt = time.Time{}
				current.PhaseName = "Finalizing experiment runtime"
			}
		})
		return nil, nil
	case "phase-timing":
		var request workerPhaseTiming
		if err := json.Unmarshal(raw, &request); err != nil {
			return nil, err
		}
		s.state.recordPhaseTiming(b.runID, request.Phase, request.Finished, request.At)
		return nil, nil
	case "prepare-network":
		s.state.mu.RLock()
		run := s.state.experiments[b.runID]
		s.state.mu.RUnlock()
		return nil, s.prepareExperimentNetwork(ctx, run)
	case "stop-generation":
		var r workerGeneration
		if err := json.Unmarshal(raw, &r); err != nil {
			return nil, err
		}
		if r.RunID != b.runID {
			return nil, errors.New("foreign generation fence")
		}
		err := s.stopRunGeneration(ctx, b.runID, r.Generation)
		if err == nil {
			b.mu.Lock()
			b.generation = max(b.generation, r.Generation+1)
			b.fenced = true
			b.createFenced = b.createFenced || r.Generation == ^uint64(0)
			b.createLeases = nil
			b.mu.Unlock()
		}
		return nil, err
	case "refresh":
		return nil, s.refreshAgentState(ctx, b.runID)
	case "drain":
		err := s.drainRunTelemetry(ctx, b.runID)
		b.mu.Lock()
		b.drained = err == nil
		if err == nil {
			b.createLeases = nil
		}
		b.mu.Unlock()
		return nil, err
	case "flush":
		return nil, s.flushRunSources(b.runID)
	case "append":
		var events []model.TraceEvent
		if err := json.Unmarshal(raw, &events); err != nil {
			return nil, err
		}
		for _, event := range events {
			if event.RunID != b.runID {
				return nil, errors.New("foreign worker event")
			}
		}
		return nil, s.state.appendRunEvents(b.runID, events)
	case "observation.begin", "observation.chunk", "observation.commit", "observation.abort":
		return b.observationOperation(ctx, method, raw)
	case "journal":
		s.state.persistMu.Lock()
		defer s.state.persistMu.Unlock()
		if err := s.state.recoverPendingJournalLocked(b.runID); err != nil {
			return nil, err
		}
		path := filepath.Join(s.config.DataDir, currentRunsDirectory, b.runID, "events.jsonl")
		info, err := os.Stat(path)
		if errors.Is(err, os.ErrNotExist) {
			return workerJournalCursor{}, nil
		}
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() {
			return nil, errors.New("journal is not a regular file")
		}
		return workerJournalCursor{Size: info.Size()}, nil
	case "summary":
		var summary runWorkerSummary
		if err := json.Unmarshal(raw, &summary); err != nil {
			return nil, err
		}
		if summary.RunID != b.runID || summary.Metrics.RunID != b.runID {
			return nil, errors.New("foreign worker summary")
		}
		return nil, s.publishWorkerSummary(summary)
	case "agent":
		var call workerAgentCall
		if err := json.Unmarshal(raw, &call); err != nil {
			return nil, err
		}
		return b.dispatchAgent(ctx, call)
	default:
		return nil, errors.New("unknown Run broker method")
	}
}

func (b *runWorkerBroker) authorizeAgent(call workerAgentCall) error {
	route, _, _ := strings.Cut(call.Path, "?")
	if call.Method == http.MethodPost && route == "/api/v1/nodes" {
		return b.authorizeCreateAgent(call)
	}
	s := b.server.state
	s.mu.RLock()
	defer s.mu.RUnlock()
	var owner model.Agent
	found := false
	for _, agent := range s.agents {
		if agent.URL == call.BaseURL {
			owner = agent
			found = true
			break
		}
	}
	if !found {
		return errors.New("unregistered Agent target")
	}
	if call.Method == http.MethodGet && route == "/api/v1/status" {
		return nil
	}
	prefix := "/api/v1/nodes/"
	if !strings.HasPrefix(route, prefix) {
		return errors.New("worker cannot call this Agent route")
	}
	part := strings.TrimPrefix(route, prefix)
	id, _, _ := strings.Cut(part, "/")
	id, err := url.PathUnescape(id)
	if err != nil {
		return err
	}
	node, ok := s.nodes[id]
	if !ok || node.RunID != b.runID || node.AgentID != owner.ID {
		return errors.New("worker cannot control a foreign Peer")
	}
	if call.Method != http.MethodPost && call.Method != http.MethodDelete && call.Method != http.MethodGet {
		return errors.New("worker Agent method is not allowed")
	}
	return nil
}

func mergeWorkerExperiment(current *model.Experiment, incoming model.Experiment) {
	original := *current
	*current = incoming
	current.ScenarioYAML = original.ScenarioYAML
	current.Seed = original.Seed
	current.BatchID = original.BatchID
	current.Iteration = original.Iteration
	current.Repetitions = original.Repetitions
	current.Name = original.Name
	current.QueuePosition = original.QueuePosition
	current.TotalPhases = original.TotalPhases
	current.PreviousRunIDs = original.PreviousRunIDs
	current.Superseded = original.Superseded
	current.Timing = original.Timing
	current.Agents = original.Agents
	current.StopRequested = original.StopRequested || incoming.StopRequested
	if original.IntegrityError != "" {
		current.IntegrityError = original.IntegrityError
		current.DataState = "incomplete"
		if current.State == "completed" {
			current.State = "failed"
			current.Error = original.IntegrityError
		}
	}
}

// Preserve the exact caller-owned random stream, including Intn rejection
// draws and retries, while only the parent can reserve Agent capacity.
type workerPlacementSource struct {
	ctx     context.Context
	process *workerproc.Process
	nodeID  string
	err     error
}

func (s *workerPlacementSource) Seed(int64) {}
func (s *workerPlacementSource) Int63() int64 {
	if s.err != nil {
		return 0
	}
	if s.process == nil {
		s.err = errors.New("placement worker is unavailable")
		return 0
	}
	var value int64
	s.err = s.process.Call(s.ctx, "placement-draw", s.nodeID, &value)
	return value
}
func nonemptyStrings(values ...string) []string {
	var result []string
	for _, value := range values {
		if value != "" {
			result = append(result, value)
		}
	}
	return result
}
