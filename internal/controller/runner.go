package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/auth"
	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

type ServerConfig struct {
	RunMinFreeBytes uint64
	User            string
	Password        string
	Listen          string
	DataDir         string
	Token           string
	MetricsURL      string
	PrometheusPort  int
	GrafanaPort     int
}

type Server struct {
	submissionMu           sync.Mutex
	archiveQueueLoaded     bool
	archiveLastScan        time.Time
	archiveChecking        bool
	archiveCheckStartedAt  time.Time
	archivePhase           string
	archiveLastProgressAt  time.Time
	webLogs                *webLogs
	auth                   *browserAuth
	config                 ServerConfig
	state                  *state
	client                 *http.Client
	logger                 *slog.Logger
	cancelMu               sync.Mutex
	cancels                map[string]context.CancelFunc
	shuttingDown           bool
	runs                   sync.WaitGroup
	nodeSeq                atomic.Uint64
	repeatBatches          map[string]*repeatBatch
	resultDownloads        map[string]int
	resultReadPins         map[string]int
	archiveReadSlots       chan struct{}
	archiveStatusMu        sync.RWMutex
	archiveCheckedAt       time.Time
	archiveError           string
	archiveIOCheck         func()
	resultArchiveMu        sync.Mutex
	resultArchives         map[string]resultArchiveInfo
	resultArchiveFlights   map[string]*resultArchiveFlight
	resultArchiveSlots     chan struct{}
	analysisSlots          chan struct{}
	analysisJobMu          sync.Mutex
	analysisJobs           map[string]*analysisJob
	batchAnalysisJobs      map[string]*batchAnalysisJob
	analysisWorkers        sync.WaitGroup
	scenarioMu             sync.Mutex
	scenarioCacheMu        sync.Mutex
	scenarioSummaries      map[string][]scenarioSummaryCacheEntry
	scenarioSummaryFlights map[string][]*scenarioSummaryFlight
	scenarioRecordCheck    func([]byte) error
	snapshotMu             sync.Mutex
	dashboardFrame         *dashboardFrame
	snapshotData           []byte
	snapshotAt             time.Time
}

func New(config ServerConfig, logger *slog.Logger) *Server {
	// Hash the executable once before experiment persistence locks are used.
	model.BuildVersion()
	if config.User != "" && config.Password != "" {
		config.Token = auth.InternalToken(config.User, config.Password)
	}
	if config.Listen == "" {
		config.Listen = ":8080"
	}
	if config.DataDir == "" {
		config.DataDir = "data"
	}
	if config.PrometheusPort == 0 {
		config.PrometheusPort = 9090
	}
	if config.GrafanaPort == 0 {
		config.GrafanaPort = 3000
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		webLogs:                newWebLogs(config.DataDir),
		auth:                   newBrowserAuth(),
		config:                 config,
		state:                  newState(config.DataDir),
		client:                 &http.Client{Timeout: 10 * time.Second},
		logger:                 logger,
		cancels:                make(map[string]context.CancelFunc),
		resultArchives:         make(map[string]resultArchiveInfo),
		resultReadPins:         make(map[string]int),
		archiveReadSlots:       make(chan struct{}, 2),
		resultArchiveFlights:   make(map[string]*resultArchiveFlight),
		resultArchiveSlots:     make(chan struct{}, resultArchiveMeasureLimit),
		analysisSlots:          make(chan struct{}, 1),
		analysisJobs:           make(map[string]*analysisJob),
		batchAnalysisJobs:      make(map[string]*batchAnalysisJob),
		scenarioSummaries:      make(map[string][]scenarioSummaryCacheEntry),
		scenarioSummaryFlights: make(map[string][]*scenarioSummaryFlight),
		scenarioRecordCheck: func(data []byte) error {
			_, err := scenario.Parse(data)
			return err
		},
	}
}

func (s *Server) StartScenario(parent context.Context, raw []byte) (model.Experiment, error) {
	return s.StartScenarioRepeated(parent, raw, 1)
}

func (s *Server) StopScenario(runID string) error {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	return s.stopScenarioLocked(runID)
}

func (s *Server) stopScenarioRequest(runID, executionID string) error {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	s.state.mu.RLock()
	run := s.state.experiments[runID]
	s.state.mu.RUnlock()
	if run.ExecutionID != executionID {
		return errExecutionChanged
	}
	if run.StopRequested {
		return nil
	}
	return s.stopScenarioLocked(runID)
}

var errExecutionChanged = errors.New("execution changed or executionId missing; refresh before stopping")

func (s *Server) stopScenarioLocked(runID string) error {
	if s.cancelRepeatLocked(runID) {
		return nil
	}
	if s.repeatBatches[runID] != nil {
		return errExecutionChanged
	}
	cancel, ok := s.cancels[runID]
	if !ok {
		return fmt.Errorf("running experiment %q not found", runID)
	}
	s.markStopRequestedLocked(runID, "")
	cancel()
	return nil
}

func (s *Server) runScenario(parentCtx context.Context, experiment model.Experiment, spec scenario.Scenario) {
	ctx, cancelScenario := context.WithCancel(parentCtx)
	defer cancelScenario()
	s.updateExperiment(experiment.ID, func(run *model.Experiment) {
		run.ControllerVersion = model.BuildVersion()
		run.CleanupState = "pending"
		run.DataState = "collecting"
	})
	monitorDone := make(chan struct{})
	defer close(monitorDone)
	go s.watchRunStorage(ctx, monitorDone, experiment.ID, cancelScenario)

	rng := rand.New(rand.NewSource(experiment.Seed))
	jobs := newPhaseJobs()
	shutdownTimeout, _ := time.ParseDuration(spec.JobShutdownTimeout)
	if shutdownTimeout <= 0 {
		shutdownTimeout = 3 * time.Minute
	}

	var asyncMu sync.Mutex
	var asyncErr error
	recordAsyncFailure := func(jobID string, err error) {
		if err == nil || errors.Is(err, context.Canceled) {
			return
		}
		asyncMu.Lock()
		if asyncErr == nil {
			asyncErr = fmt.Errorf("job %q: %w", jobID, err)
			cancelScenario()
		}
		asyncMu.Unlock()
	}

	var runErr error
	generation := uint64(1)
	for index, phase := range spec.Phases {
		if err := ctx.Err(); err != nil {
			runErr = err
			break
		}
		s.state.recordPhaseTiming(experiment.ID, index, false, time.Now().UTC())
		s.updateExperiment(experiment.ID, func(current *model.Experiment) {
			current.Phase = index + 1
			current.PhaseName = phase.Name
		})
		phaseSeed := rng.Int63()
		phaseGeneration := generation
		execute := func(executionCtx context.Context) error {
			defer func() { s.state.recordPhaseTiming(experiment.ID, index, true, time.Now().UTC()) }()
			phaseRNG := rand.New(rand.NewSource(phaseSeed))
			for repetition := 0; repetition < phase.Repeat; repetition++ {
				if err := executionCtx.Err(); err != nil {
					return err
				}
				if err := s.runPhase(executionCtx, experiment.ID, phaseGeneration, phase, phaseRNG, jobs, shutdownTimeout); err != nil {
					return err
				}
			}
			return nil
		}
		s.logger.Info("scenario phase started", "run", experiment.ID, "phase", phase.Name, "job", phase.Job, "action", phase.Action, "parallel", phase.Parallel, "await", phase.ShouldAwait())
		if phase.ShouldAwait() {
			runErr = execute(ctx)
		} else {
			jobID := phase.Job
			s.updateExperiment(experiment.ID, func(current *model.Experiment) { current.ActiveJobs++ })
			runErr = jobs.startContext(ctx, jobID, execute, func(err error) {
				s.updateExperiment(experiment.ID, func(current *model.Experiment) {
					if current.ActiveJobs > 0 {
						current.ActiveJobs--
					}
					switch {
					case err == nil:
						current.CompletedJobs++
					case errors.Is(err, context.Canceled):
						current.CanceledJobs++
					default:
						current.FailedJobs++
					}
				})
				recordAsyncFailure(jobID, err)
			})
			if runErr != nil {
				s.updateExperiment(experiment.ID, func(current *model.Experiment) {
					if current.ActiveJobs > 0 {
						current.ActiveJobs--
					}
				})
			}
		}
		if runErr != nil {
			break
		}
		if phase.Action == "stop-all" {
			generation++
		}
	}
	if runErr == nil && spec.OnExit == "drain" {
		runErr = jobs.wait(ctx, nil, 0)
	}

	var cleanupErr, dataErr error
	if runErr != nil || spec.OnExit == "cancel" {
		cancelScenario()
		jobs.cancelAll()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), shutdownTimeout)
		jobCleanupErr := jobs.waitDone(cleanupCtx, nil, 0)
		cleanupCancel()
		if jobCleanupErr != nil {
			jobCleanupErr = fmt.Errorf("stop background jobs: %w", jobCleanupErr)
			cleanupErr = errors.Join(cleanupErr, jobCleanupErr)
			s.logger.Error("scenario background-job cleanup failed", "run", experiment.ID, "error", jobCleanupErr)
		}
	} else {
		cancelScenario()
	}

	asyncMu.Lock()
	backgroundErr := asyncErr
	asyncMu.Unlock()
	// Retire the cancellation handle and sample its context while holding the
	// same lock used by StopScenario. Thus a stop either wins here and triggers
	// cleanup, or observes that the run has already entered finalization.
	s.cancelMu.Lock()
	delete(s.cancels, experiment.ID)
	parentErr := parentCtx.Err()
	s.cancelMu.Unlock()
	if parentErr != nil {
		runErr = parentErr
	} else if backgroundErr != nil && (runErr == nil || errors.Is(runErr, context.Canceled)) {
		runErr = backgroundErr
	}

	closedSingleRun := experiment.Repetitions <= 1 && generation > 1 && len(s.matchingNodes(experiment.ID, "", "", "")) == 0
	// For a single run, natural onExit only governs background jobs. Repeated,
	// canceled, or failed runs also fence and remove Peers after producers stop,
	// so no in-flight creation can escape cleanup or overlap the next iteration.
	if runErr != nil || cleanupErr != nil || experiment.Repetitions > 1 {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), shutdownTimeout)
		s.updateExperiment(experiment.ID, func(run *model.Experiment) { run.CleanupState = "running" })
		stopErr := s.stopRunGeneration(cleanupCtx, experiment.ID, generation)
		if stopErr == nil {
			dataErr = s.drainRunTelemetry(cleanupCtx, experiment.ID)
		}
		refreshErr := s.refreshAgentState(cleanupCtx)
		cleanupCancel()
		if err := errors.Join(stopErr, refreshErr); err != nil {
			err = fmt.Errorf("stop scenario peers through generation %d: %w", generation, err)
			cleanupErr = errors.Join(cleanupErr, err)
			s.logger.Error("scenario peer cleanup failed", "run", experiment.ID, "generation", generation, "error", err)
		}
	}
	if runErr == nil && cleanupErr == nil && closedSingleRun {
		drainCtx, drainCancel := context.WithTimeout(context.Background(), shutdownTimeout)
		dataErr = s.drainRunTelemetry(drainCtx, experiment.ID)
		drainCancel()
	}
	resultErr := errors.Join(runErr, cleanupErr, dataErr)

	if err := s.state.recordAnalysisObservation(experiment.ID, time.Now().UTC()); err != nil {
		s.logger.Warn("record final analysis observation", "run", experiment.ID, "error", err)
		dataErr = errors.Join(dataErr, err)
		resultErr = errors.Join(resultErr, err)
		s.state.recordRunWriteError(experiment.ID, err)
	}

	if err := s.flushRunSources(experiment.ID); err != nil {
		s.state.recordRunWriteError(experiment.ID, err)
		resultErr = errors.Join(resultErr, err)
	}
	s.state.mu.RLock()
	integrity := s.state.experiments[experiment.ID].IntegrityError
	s.state.mu.RUnlock()
	if integrity != "" {
		resultErr = errors.Join(resultErr, errors.New(integrity))
	}
	finished := time.Now().UTC()
	s.updateExperiment(experiment.ID, func(current *model.Experiment) {
		current.FinishedAt = finished
		current.PhaseName = ""
		current.CleanupState = "complete"
		if cleanupErr != nil {
			current.CleanupState = "failed"
			current.CleanupError = cleanupErr.Error()
		}
		current.DataState = "complete"
		for _, agent := range current.Agents {
			if !agent.RunDrain {
				current.DataState = "unverified"
			}
		}
		if dataErr != nil || current.IntegrityError != "" || cleanupErr != nil {
			current.DataState = "incomplete"
			if dataErr != nil {
				current.IntegrityError = dataErr.Error()
			}
		}
		if resultErr == nil && experiment.Repetitions <= 1 && !closedSingleRun && len(current.Agents) > 0 {
			current.CleanupState = "retained"
			current.DataState = "collecting"
		}
		switch {
		case resultErr == nil:
			current.State = "completed"
		case errors.Is(runErr, context.Canceled) && integrity == "":
			current.State = "canceled"
			if cleanupErr != nil {
				current.Error = resultErr.Error()
			}
		default:
			current.State = "failed"
			current.Error = resultErr.Error()
		}
	})
	if resultErr != nil && (!errors.Is(runErr, context.Canceled) || cleanupErr != nil) {
		s.logger.Error("scenario failed", "run", experiment.ID, "error", resultErr)
	} else {
		s.logger.Info("scenario finished", "run", experiment.ID, "result", resultErr)
	}
}

func (s *Server) runPhase(ctx context.Context, runID string, generation uint64, phase scenario.Phase, rng *rand.Rand, jobs *phaseJobs, shutdownTimeout time.Duration) error {
	switch phase.Action {
	case "join":
		return s.runJoin(ctx, runID, generation, phase, rng)
	case "wait":
		duration, _ := time.ParseDuration(phase.Duration)
		return sleepContext(ctx, duration)
	case "wait-ready":
		timeout, _ := time.ParseDuration(phase.Timeout)
		waitCtx, waitCancel := context.WithTimeout(ctx, timeout)
		defer waitCancel()
		if len(phase.Jobs) > 0 {
			if err := jobs.wait(waitCtx, phase.Jobs, 0); err != nil {
				return err
			}
		}
		return s.waitReadyUntil(waitCtx, runID, generation, phase)
	case "wait-jobs":
		timeout, _ := time.ParseDuration(phase.Timeout)
		return jobs.wait(ctx, phase.Jobs, timeout)
	case "publish":
		return s.runPublish(ctx, runID, phase, rng)
	case "leave":
		return s.runLeave(ctx, runID, phase, rng)
	case "stop-all":
		jobs.cancelAll()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), shutdownTimeout)
		err := jobs.waitDone(cleanupCtx, nil, 0)
		cleanupCancel()
		if err != nil {
			return fmt.Errorf("stop background jobs before stop-all: %w", err)
		}
		if err := jobs.reset(); err != nil {
			return fmt.Errorf("reset background jobs before stop-all: %w", err)
		}
		cleanupCtx, cleanupCancel = context.WithTimeout(context.Background(), shutdownTimeout)
		defer cleanupCancel()
		stopErr := s.stopRunGeneration(cleanupCtx, runID, generation)
		refreshErr := s.refreshAgentState(cleanupCtx)
		return errors.Join(refreshErr, stopErr)
	case "log":
		s.logger.Info("scenario message", "run", runID, "message", phase.Message)
		return nil
	default:
		return fmt.Errorf("unsupported action %q", phase.Action)
	}
}

func (s *Server) runJoin(ctx context.Context, runID string, generation uint64, phase scenario.Phase, rng *rand.Rand) error {
	requests := make([]model.CreateNodeRequest, 0, phase.Count)
	for i := 0; i < phase.Count; i++ {
		seq := s.nodeSeq.Add(1)
		nodeID := fmt.Sprintf("%s-%s-%05d", runID, safeName(phase.Group), seq)
		lifetime := ""
		if phase.Lifetime.Model != "" {
			lifetime = phase.Lifetime.Sample(rng).String()
		}
		requests = append(requests, model.CreateNodeRequest{
			ID:         nodeID,
			RunID:      runID,
			Generation: generation,
			Group:      phase.Group,
			Role:       phase.Role,
			Type:       phase.NodeType,
			Profile:    phase.Profile,
			Seed:       rng.Int63(),
			Config:     phase.Node.WithDefaults(),
			Lifetime:   lifetime,
		})
	}
	delays := sampleDelays(rng, phase.Interval, phase.Count)
	agentID := phase.AgentID
	if agentID == "" && phase.Placement == "single-agent" {
		var err error
		agentID, err = s.selectBatchAgent(ctx, rng)
		if err != nil {
			return err
		}
	}
	scheduled := time.Now()
	var lastScheduleWarning time.Time
	return runOperations(ctx, phase.Count, phase.Parallel, phase.Parallelism, delays, false, func(operationCtx context.Context, i int) error {
		request := requests[i]
		var placementRNG *rand.Rand
		if phase.Placement == "random" && agentID == "" {
			placementRNG = rand.New(rand.NewSource(request.Seed))
		}
		agent, err := s.acquireAgentWithPlacement(operationCtx, request.ID, agentID, placementRNG)
		if err != nil {
			return err
		}
		if !phase.Parallel {
			if i > 0 {
				scheduled = scheduled.Add(delays[i-1])
			}
			now := time.Now()
			if lag := now.Sub(scheduled); lag >= time.Second && (lastScheduleWarning.IsZero() || now.Sub(lastScheduleWarning) >= 10*time.Second) {
				s.logger.Warn("churn join schedule delayed", "runId", runID, "generation", generation, "group", phase.Group, "job", phase.Job, "operation", i+1, "count", phase.Count, "agentId", agent.ID, "scheduledAt", scheduled.UTC(), "delay", lag)
				lastScheduleWarning = now
			}
		}
		return s.createReservedNode(operationCtx, request, agent, agentID, placementRNG)
	})
}

func (s *Server) waitReady(ctx context.Context, runID string, generation uint64, phase scenario.Phase) error {
	timeout, _ := time.ParseDuration(phase.Timeout)
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return s.waitReadyUntil(waitCtx, runID, generation, phase)
}

func (s *Server) waitReadyUntil(ctx context.Context, runID string, generation uint64, phase scenario.Phase) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		ready, total, failed := s.readyCount(runID, generation, phase.Group, phase.NodeType)
		enoughNodes := total > 0
		if phase.MinCount > 0 {
			enoughNodes = total >= phase.MinCount
		}
		if failed == 0 && enoughNodes && float64(ready)/float64(total) >= phase.ReadyRatio {
			return nil
		}
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return fmt.Errorf("ready barrier timed out for group %q in generation %d: %d/%d ready, %d failed: %w", phase.Group, generation, ready, total, failed, ctx.Err())
			}
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Server) runPublish(ctx context.Context, runID string, phase scenario.Phase, rng *rand.Rand) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	candidates := s.matchingNodes(runID, phase.Group, model.NodeReady, phase.NodeType)
	nodes := make([]model.Node, 0, len(candidates))
	for _, node := range candidates {
		if !s.nodeAgentOnline(node) {
			continue
		}
		if len(nodePublishTopics(node, phase.Topic)) > 0 {
			nodes = append(nodes, node)
		}
	}
	if len(nodes) == 0 {
		if phase.OnError == "continue" {
			return nil
		}
		return fmt.Errorf("no publish-capable ready nodes in group %q for topic %q", phase.Group, phase.Topic)
	}
	rng.Shuffle(len(nodes), func(i, j int) { nodes[i], nodes[j] = nodes[j], nodes[i] })
	count := phase.Count
	if count > len(nodes) {
		count = len(nodes)
	}
	delays := publishDelays(rng, phase.Interval, count, phase.Parallel)
	return runOperations(ctx, count, phase.Parallel, phase.Parallelism, delays, true, func(operationCtx context.Context, i int) error {
		node := nodes[i]
		agent, ok := s.agent(node.AgentID)
		if !ok || !agentIsOnline(agent, time.Now()) {
			return s.phaseOperationError(operationCtx, runID, phase, node, "", fmt.Errorf("agent %q for node %q is unavailable", node.AgentID, node.ID))
		}
		topic := phase.Topic
		if topic == "" {
			topic = nodeDefaultTopic(node)
		}
		request := model.PublishRequest{RunID: runID, Topic: topic, PayloadSize: phase.PayloadSize, PayloadEncoding: phase.PayloadEncoding, DeliveryWindow: phase.DeliveryWindow}
		if !s.capturePublishCohort(&request, node, nodePublishTopics(node, topic)) {
			return s.phaseOperationError(operationCtx, runID, phase, node, topic, fmt.Errorf("publisher %q is no longer ready", node.ID))
		}
		path := "/api/v1/nodes/" + node.ID + "/publish"
		if err := s.callAgent(operationCtx, agent.URL, http.MethodPost, path, request, nil); err != nil {
			return s.phaseOperationError(operationCtx, runID, phase, node, topic, fmt.Errorf("publish through node %s on topic %q: %w", node.ID, topic, err))
		}
		return nil
	})
}

func (s *Server) runLeave(ctx context.Context, runID string, phase scenario.Phase, rng *rand.Rand) error {
	return s.stopNodesWithPolicy(ctx, runID, phase, rng)
}

func (s *Server) stopNodes(ctx context.Context, runID, group, nodeType string, count int, rng *rand.Rand, interval scenario.Distribution, parallel bool, parallelism int) error {
	return s.stopNodesWithPolicy(ctx, runID, scenario.Phase{Action: "leave", Group: group, NodeType: nodeType, Count: count, Interval: interval, Parallel: parallel, Parallelism: parallelism}, rng)
}

func (s *Server) stopNodesWithPolicy(ctx context.Context, runID string, phase scenario.Phase, rng *rand.Rand) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	nodes := s.matchingNodes(runID, phase.Group, "", phase.NodeType)
	rng.Shuffle(len(nodes), func(i, j int) { nodes[i], nodes[j] = nodes[j], nodes[i] })
	count := phase.Count
	if count <= 0 || count > len(nodes) {
		count = len(nodes)
	}
	delays := sampleDelays(rng, phase.Interval, count)
	var stopErrorsMu sync.Mutex
	var stopErrors []error
	recordFailure := func(operationCtx context.Context, node model.Node, err error) error {
		if phase.OnError == "continue" || operationCtx.Err() != nil {
			return s.phaseOperationError(operationCtx, runID, phase, node, "", err)
		}
		// The default policy still attempts every selected delete before failing.
		stopErrorsMu.Lock()
		stopErrors = append(stopErrors, err)
		stopErrorsMu.Unlock()
		return nil
	}
	operationErr := runOperations(ctx, count, phase.Parallel, phase.Parallelism, delays, false, func(operationCtx context.Context, i int) error {
		node := nodes[i]
		agent, ok := s.agent(node.AgentID)
		if !ok {
			return recordFailure(operationCtx, node, fmt.Errorf("stop node %s: agent %q is unavailable", node.ID, node.AgentID))
		}
		if err := s.callAgent(operationCtx, agent.URL, http.MethodDelete, "/api/v1/nodes/"+node.ID, nil, nil); err != nil {
			return recordFailure(operationCtx, node, fmt.Errorf("stop node %s: %w", node.ID, err))
		}
		s.markNodeStoppingAndReleaseCapacity(node.ID)
		return nil
	})
	if operationErr != nil {
		stopErrors = append(stopErrors, operationErr)
	}
	return errors.Join(stopErrors...)
}

func (s *Server) phaseOperationError(ctx context.Context, runID string, phase scenario.Phase, node model.Node, topic string, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if phase.OnError != "continue" {
		return err
	}
	event := model.TraceEvent{
		RunID: runID, NodeID: node.ID, AgentID: node.AgentID, Topic: topic,
		Type:   "phase-operation-failed",
		Fields: map[string]any{"phase": phase.Name, "job": phase.Job, "action": phase.Action, "error": err.Error()},
	}
	if persistErr := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{event}}); persistErr != nil {
		return fmt.Errorf("record phase operation failure: %w", errors.Join(err, persistErr))
	}
	return nil
}

// stopRunGeneration is the stop-all transaction boundary. Each Agent records
// a monotonically increasing fence for the run before stopping matching peers,
// so a create that was already in flight cannot commit after cleanup. A later
// scenario generation is still allowed to create peers with the same run ID.
func (s *Server) stopRunGeneration(ctx context.Context, runID string, generation uint64) error {
	agents, err := s.cleanupParticipants(runID)
	if err != nil {
		return err
	}

	type result struct {
		agentID string
		err     error
	}
	results := make(chan result, len(agents))
	var group sync.WaitGroup
	for _, agent := range agents {
		agent := agent
		group.Add(1)
		go func() {
			defer group.Done()
			path := "/api/v1/runs/" + url.PathEscape(runID) + "/nodes?generation=" + strconv.FormatUint(generation, 10)
			err := s.callAgent(ctx, agent.URL, http.MethodDelete, path, nil, nil)
			if err != nil {
				err = fmt.Errorf("stop run generation on agent %s: %w", agent.ID, err)
			}
			results <- result{agentID: agent.ID, err: err}
		}()
	}
	group.Wait()
	close(results)

	succeeded := make(map[string]struct{}, len(agents))
	var stopErrors []error
	for result := range results {
		if result.err != nil {
			stopErrors = append(stopErrors, result.err)
		} else {
			succeeded[result.agentID] = struct{}{}
		}
	}
	for _, node := range s.matchingNodes(runID, "", "", "") {
		if node.Generation <= generation {
			if _, ok := succeeded[node.AgentID]; ok {
				s.markNodeStoppingAndReleaseCapacity(node.ID)
			}
		}
	}
	return errors.Join(stopErrors...)
}

func (s *Server) refreshAgentState(ctx context.Context) error {
	s.state.mu.RLock()
	agents := make([]model.Agent, 0, len(s.state.agents))
	for _, agent := range s.state.agents {
		if agent.State == model.AgentOnline {
			agents = append(agents, agent)
		}
	}
	s.state.mu.RUnlock()
	sort.Slice(agents, func(i, j int) bool { return agents[i].ID < agents[j].ID })

	var group sync.WaitGroup
	errorsByAgent := make(chan error, len(agents))
	for _, agent := range agents {
		agent := agent
		group.Add(1)
		go func() {
			defer group.Done()
			var heartbeat model.AgentHeartbeat
			if err := s.callAgent(ctx, agent.URL, http.MethodGet, "/api/v1/status", nil, &heartbeat); err != nil {
				errorsByAgent <- fmt.Errorf("refresh agent %s: %w", agent.ID, err)
				return
			}
			if err := s.state.heartbeat(heartbeat); err != nil {
				errorsByAgent <- fmt.Errorf("apply agent %s status: %w", agent.ID, err)
			}
		}()
	}
	group.Wait()
	close(errorsByAgent)
	var refreshErrors []error
	for err := range errorsByAgent {
		refreshErrors = append(refreshErrors, err)
	}
	return errors.Join(refreshErrors...)
}

func (s *Server) acquireAgent(ctx context.Context, nodeID string) (model.Agent, error) {
	return s.acquireAgentWithPlacement(ctx, nodeID, "", nil)
}

func (s *Server) acquireAgentWithPlacement(ctx context.Context, nodeID, agentID string, rng *rand.Rand) (model.Agent, error) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return model.Agent{}, err
		}
		if agent, ok := s.tryReserveAgentWithPlacement(nodeID, agentID, rng); ok {
			return agent, nil
		}
		select {
		case <-ctx.Done():
			return model.Agent{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Server) tryReserveAgent(nodeID string) (model.Agent, bool) {
	return s.tryReserveAgentWithPlacement(nodeID, "", nil)
}

func (s *Server) tryReserveAgentWithPlacement(nodeID, targetAgentID string, rng *rand.Rand) (model.Agent, bool) {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	if agentID, exists := s.state.reservations[nodeID]; exists {
		agent, ok := s.state.agents[agentID]
		return agent, ok && agentIsOnline(agent, time.Now()) && (targetAgentID == "" || agentID == targetAgentID)
	}
	var selected model.Agent
	var candidates []model.Agent
	found := false
	now := time.Now()
	for _, agent := range s.state.agents {
		if targetAgentID != "" && agent.ID != targetAgentID {
			continue
		}
		if !agentIsOnline(agent, now) || agent.Capacity > 0 && agent.ActiveNodes >= agent.Capacity {
			continue
		}
		if rng != nil {
			candidates = append(candidates, agent)
		} else if !found || balancedAgentBefore(agent, selected) {
			// Balanced placement needs only the best candidate, not a sort
			// of the entire fleet while holding the reservation lock.
			selected = agent
		}
		found = true
	}
	if !found {
		return model.Agent{}, false
	}
	if rng != nil {
		// Keep seeded random placement independent of map iteration order.
		sort.Slice(candidates, func(i, j int) bool { return candidates[i].ID < candidates[j].ID })
		selected = candidates[rng.Intn(len(candidates))]
	}
	selected.ActiveNodes++
	s.state.agents[selected.ID] = selected
	s.state.reservations[nodeID] = selected.ID
	return selected, true
}

// A single-agent join picks its target once, then waits for that Agent's
// capacity for every replica rather than spilling the batch to other Agents.
func (s *Server) selectBatchAgent(ctx context.Context, rng *rand.Rand) (string, error) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		s.state.mu.RLock()
		var candidates []string
		for _, agent := range s.state.agents {
			if agentIsOnline(agent, time.Now()) && (agent.Capacity <= 0 || agent.ActiveNodes < agent.Capacity) {
				candidates = append(candidates, agent.ID)
			}
		}
		s.state.mu.RUnlock()
		if len(candidates) > 0 {
			sort.Strings(candidates)
			return candidates[rng.Intn(len(candidates))], nil
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
		}
	}
}

// Use the effective capacity, including Dashboard overrides. Looking ahead
// one Peer avoids filling a small Agent just because both Agents are idle.
// ActiveNodes also includes reservations and containers still being removed.
func balancedAgentBefore(a, b model.Agent) bool {
	left, right := utilization(a, 1), utilization(b, 1)
	if left != right {
		return left < right
	}
	left, right = utilization(a, 0), utilization(b, 0)
	if left != right {
		return left < right
	}
	return a.ID < b.ID
}

func utilization(agent model.Agent, additional int) float64 {
	occupied := float64(agent.ActiveNodes) + float64(additional)
	if agent.Capacity <= 0 {
		return occupied
	}
	return occupied / float64(agent.Capacity)
}

func (s *Server) releaseReservation(nodeID string) {
	s.state.mu.Lock()
	agentID, reserved := s.state.reservations[nodeID]
	if reserved {
		delete(s.state.reservations, nodeID)
	}
	agent, ok := s.state.agents[agentID]
	if ok && agent.ActiveNodes > 0 {
		agent.ActiveNodes--
		s.state.agents[agentID] = agent
	}
	s.state.mu.Unlock()
	if reserved {
		s.state.notify()
	}
}

func (s *Server) recordCreatedNode(request model.CreateNodeRequest, agentID string, node model.Node, expectedInstance ...time.Time) bool {
	now := time.Now().UTC()
	config := request.Config.WithDefaults()
	if node.ID == "" {
		node.ID = request.ID
	}
	if node.RunID == "" {
		node.RunID = request.RunID
	}
	if node.Generation == 0 {
		node.Generation = request.Generation
	}
	if node.AgentID == "" {
		node.AgentID = agentID
	}
	if node.Group == "" {
		node.Group = request.Group
	}
	if node.Role == "" {
		node.Role = request.Role
	}
	if node.Type == "" {
		node.Type = request.Type
	}
	if node.Profile == "" {
		node.Profile = request.Profile
	}
	if node.State == "" {
		node.State = model.NodeStarting
	}
	if node.Metadata == nil {
		node.Metadata = map[string]string{
			"profile":       node.Profile,
			"pubsubRouter":  config.GossipSub.Router,
			"pubsubEnabled": strconv.FormatBool(config.GossipSub.Enabled != nil && *config.GossipSub.Enabled),
			"scoreEnabled":  strconv.FormatBool(config.GossipSub.Enabled != nil && *config.GossipSub.Enabled && config.GossipSub.Router == "gossipsub" && config.GossipSub.Score != nil && config.GossipSub.Score.IsEnabled()),
			"allowPublish":  strconv.FormatBool(config.PublishAllowed()),
			"topicMode":     config.GossipSub.TopicMode,
			"topics":        strings.Join(config.GossipSub.Topics, ","),
			"topicsJSON":    encodeTopics(config.GossipSub.Topics),
			"dhtEnabled":    strconv.FormatBool(config.Kademlia.Enabled != nil && *config.Kademlia.Enabled),
			"dhtMode":       config.Kademlia.Mode,
		}
	}
	if node.StartedAt.IsZero() {
		node.StartedAt = now
	}
	if node.LastSeen.IsZero() {
		node.LastSeen = now
	}
	s.state.mu.Lock()
	if len(expectedInstance) > 0 {
		current, exists := s.state.agents[agentID]
		if !exists || !current.StartedAt.Equal(expectedInstance[0]) {
			s.state.mu.Unlock()
			return false
		}
	}
	if current, exists := s.state.nodes[node.ID]; !exists ||
		current.State != model.NodeStopping && current.State != model.NodeStopped && current.State != model.NodeFailed && current.LastSeen.Before(node.LastSeen) {
		// A delayed create response can carry a later Agent clock than a
		// Controller stop request. Timestamps must not undo that lifecycle.
		s.state.setNodeLocked(node)
	}
	s.state.mu.Unlock()
	s.state.notify()
	return true
}

func (s *Server) markNodeStoppingAndReleaseCapacity(nodeID string) {
	s.state.mu.Lock()
	node, exists := s.state.nodes[nodeID]
	releaseCapacity := false
	if exists && node.State != model.NodeStopping && node.State != model.NodeStopped && node.State != model.NodeFailed {
		releaseCapacity = true
		node.State = model.NodeStopping
		node.LastSeen = time.Now().UTC()
		s.state.setNodeLocked(node)
	}
	if _, reserved := s.state.reservations[nodeID]; reserved {
		delete(s.state.reservations, nodeID)
		releaseCapacity = true
	}
	// A successful DELETE marks logical state only. Reuse capacity after the
	// Agent reports completed cleanup in its next heartbeat/status snapshot.
	s.state.mu.Unlock()
	if exists || releaseCapacity {
		s.state.notify()
	}
}

func (s *Server) readyCount(runID string, generation uint64, group, nodeType string) (ready, total, failed int) {
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	for _, node := range s.state.nodes {
		if node.RunID != runID || node.Generation != generation || group != "" && node.Group != group || nodeType != "" && node.Type != nodeType {
			continue
		}
		total++
		switch node.State {
		case model.NodeReady:
			if agent, ok := s.state.agents[node.AgentID]; ok && agentIsOnline(agent, time.Now()) {
				ready++
			}
		case model.NodeFailed:
			failed++
		}
	}
	return ready, total, failed
}

func (s *Server) matchingNodes(runID, group, state, nodeType string) []model.Node {
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	var nodes []model.Node
	for _, node := range s.state.nodes {
		if node.RunID != runID || group != "" && node.Group != group || nodeType != "" && node.Type != nodeType {
			continue
		}
		if state != "" && node.State != state {
			continue
		}
		if node.State == model.NodeStopping || node.State == model.NodeStopped || node.State == model.NodeFailed {
			continue
		}
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	return nodes
}

func (s *Server) topicSubscriberCount(runID, topic string) int {
	nodes := s.matchingNodes(runID, "", model.NodeReady, "")
	count := 0
	for _, node := range nodes {
		if s.nodeAgentOnline(node) && nodeSubscribesToTopic(node, topic) {
			count++
		}
	}
	return count
}

func (s *Server) topicReachTargetCount(runID, topic, publisherID string) int {
	count := s.topicSubscriberCount(runID, topic)
	for _, node := range s.matchingNodes(runID, "", model.NodeReady, "") {
		if node.ID == publisherID && s.nodeAgentOnline(node) {
			if !nodeSubscribesToTopic(node, topic) {
				count++
			}
			break
		}
	}
	if count == 0 {
		return 1
	}
	return count
}

// Capture all topic cohorts under one inventory lock immediately before the
// publish RPC. This is a Controller observation, not an atomic cluster barrier:
// a receiver leaving after this snapshot remains an intended recipient.
func (s *Server) capturePublishCohort(request *model.PublishRequest, publisher model.Node, topics []string) bool {
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	now := time.Now().UTC()
	current, exists := s.state.nodes[publisher.ID]
	if !exists || current.State != model.NodeReady || !agentIsOnline(s.state.agents[current.AgentID], now) {
		return false
	}
	request.CohortCapturedAt = now
	request.TargetNodeIDsByTopic = make(map[string][]string, len(topics))
	request.TargetNodesByTopic = make(map[string]int, len(topics))
	for _, topic := range topics {
		targets := make([]string, 0)
		for _, node := range s.state.nodes {
			if node.ID != publisher.ID && node.RunID == request.RunID && node.State == model.NodeReady &&
				agentIsOnline(s.state.agents[node.AgentID], now) && nodeSubscribesToTopic(node, topic) {
				targets = append(targets, node.ID)
			}
		}
		sort.Strings(targets)
		request.TargetNodeIDsByTopic[topic] = targets
		// Retain the old count for older peers. New metrics only use exact IDs.
		request.TargetNodesByTopic[topic] = len(targets) + 1
		if request.Topic != "*" {
			request.TargetNodeIDs = targets
			request.TargetNodes = len(targets) + 1
		}
	}
	return true
}

func (s *Server) nodeAgentOnline(node model.Node) bool {
	agent, ok := s.agent(node.AgentID)
	return ok && agentIsOnline(agent, time.Now())
}

func nodeCanPublishTopic(node model.Node, topic string) bool {
	if !nodeFeatureEnabled(node, "pubsubEnabled", node.Type != "boot" && node.Type != "dht-only") {
		return false
	}
	fallback := node.Type != "subscriber" && node.Type != "observer" && node.Type != "relay"
	if !nodeFeatureEnabled(node, "allowPublish", fallback) {
		return false
	}
	return nodeHasTopic(node, topic)
}

func nodePublishTopics(node model.Node, requested string) []string {
	if requested != "*" {
		if requested == "" {
			requested = nodeDefaultTopic(node)
		}
		if nodeCanPublishTopic(node, requested) {
			return []string{requested}
		}
		return nil
	}
	configured := nodeTopics(node)
	if len(configured) == 0 {
		configured = []string{"kpl/default"}
	}
	unique := make(map[string]struct{}, len(configured))
	for _, topic := range configured {
		if topic != "" && nodeCanPublishTopic(node, topic) {
			unique[topic] = struct{}{}
		}
	}
	topics := make([]string, 0, len(unique))
	for topic := range unique {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	return topics
}

func nodeSubscribesToTopic(node model.Node, topic string) bool {
	if !nodeFeatureEnabled(node, "pubsubEnabled", node.Type != "boot" && node.Type != "dht-only") {
		return false
	}
	mode := node.Metadata["topicMode"]
	if mode == "" {
		if node.Type == "publisher" || node.Type == "relay" {
			mode = node.Type
		} else {
			mode = "subscribe"
		}
	}
	if mode != "subscribe" {
		return false
	}
	return nodeHasTopic(node, topic)
}

func nodeFeatureEnabled(node model.Node, key string, fallback bool) bool {
	raw, exists := node.Metadata[key]
	if !exists {
		return fallback
	}
	value, err := strconv.ParseBool(raw)
	return err == nil && value
}

func nodeDefaultTopic(node model.Node) string {
	for _, topic := range nodeTopics(node) {
		if topic != "" {
			return topic
		}
	}
	return "kpl/default"
}

func nodeHasTopic(node model.Node, topic string) bool {
	if topic == "" {
		return true
	}
	topics := nodeTopics(node)
	if len(topics) == 0 {
		return topic == "kpl/default"
	}
	for _, candidate := range topics {
		if candidate == topic {
			return true
		}
	}
	return false
}

func nodeTopics(node model.Node) []string {
	if raw := strings.TrimSpace(node.Metadata["topicsJSON"]); raw != "" {
		var topics []string
		if json.Unmarshal([]byte(raw), &topics) == nil {
			return topics
		}
	}
	if raw := strings.TrimSpace(node.Metadata["topics"]); raw != "" {
		return strings.Split(raw, ",")
	}
	return nil
}

func encodeTopics(topics []string) string {
	data, err := json.Marshal(topics)
	if err != nil {
		return "[]"
	}
	return string(data)
}

func sampleDelays(rng *rand.Rand, distribution scenario.Distribution, count int) []time.Duration {
	delays := make([]time.Duration, count)
	for i := range delays {
		delays[i] = distribution.Sample(rng)
	}
	return delays
}

func publishDelays(rng *rand.Rand, distribution scenario.Distribution, count int, parallel bool) []time.Duration {
	delays := sampleDelays(rng, distribution, count)
	if parallel && distribution.Model == "" {
		for i := range delays {
			delays[i] = time.Second
		}
	}
	return delays
}

func runOperations(ctx context.Context, count int, parallel bool, parallelism int, delays []time.Duration, delayParallel bool, operation func(context.Context, int) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// A batch with one dispatch slot has stable request order. Parallel join
	// and leave ignore interval; their serial dispatch must do so as well.
	if !parallel || parallelism == 1 && !delayParallel {
		// Intervals describe arrival times, not idle time after each request.
		// Keep a monotonic deadline so admission/RPC latency does not accumulate
		// across a long churn stream. Late operations still execute in order;
		// they catch up without dropping arrivals or bypassing capacity checks.
		next := time.Now()
		for i := 0; i < count; i++ {
			if !parallel {
				if err := sleepContext(ctx, time.Until(next)); err != nil {
					return err
				}
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := operation(ctx, i); err != nil {
				return err
			}
			if !parallel && i < len(delays) {
				next = next.Add(delays[i])
			}
		}
		return nil
	}
	if parallelism <= 0 || parallelism > count {
		parallelism = count
	}
	operationCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	started := time.Now()
	// Only workers consume goroutines. Queued operations and publication
	// timers must not create one goroutine per item in a large churn batch.
	jobs := make(chan int)
	failures := make(chan error, parallelism)
	var group sync.WaitGroup
	for i := 0; i < parallelism; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for index := range jobs {
				if operationCtx.Err() != nil {
					return
				}
				if err := operation(operationCtx, index); err != nil {
					failures <- err
					cancel()
					return
				}
			}
		}()
	}
	// Parallel publication delays are independent offsets from batch start.
	// Dispatch by due time so a late first item cannot hold up an earlier one.
	// Waiting for a worker does not reset any remaining publication deadline.
	var order []int
	delay := func(index int) time.Duration {
		if index < len(delays) && delays[index] > 0 {
			return delays[index]
		}
		return 0
	}
	if delayParallel && len(delays) > 0 {
		order = make([]int, count)
		for i := range order {
			order[i] = i
		}
		sort.SliceStable(order, func(i, j int) bool { return delay(order[i]) < delay(order[j]) })
	}
dispatch:
	for i := 0; i < count; i++ {
		index := i
		if order != nil {
			index = order[i]
			if err := sleepContext(operationCtx, time.Until(started.Add(delay(index)))); err != nil {
				break
			}
		}
		select {
		case <-operationCtx.Done():
			break dispatch
		case jobs <- index:
		}
	}
	close(jobs)
	group.Wait()
	close(failures)
	var canceled error
	for err := range failures {
		if !errors.Is(err, context.Canceled) {
			return err
		}
		if canceled == nil {
			canceled = err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// A canceled operation is still a failure when its parent is alive.
	return canceled
}

func (s *Server) agent(id string) (model.Agent, bool) {
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	agent, ok := s.state.agents[id]
	if ok && !agentIsOnline(agent, time.Now()) {
		agent.State = model.AgentOffline
	}
	return agent, ok
}

func (s *Server) callAgent(ctx context.Context, baseURL, method, path string, input, output any) error {
	var body io.Reader
	if input != nil {
		data, err := json.Marshal(input)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(baseURL, "/")+path, body)
	if err != nil {
		return err
	}
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if s.config.Token != "" {
		req.Header.Set("Authorization", "Bearer "+s.config.Token)
	}
	client := s.client
	if (method == http.MethodDelete && (strings.HasPrefix(path, "/api/v1/runs/") || path == "/api/v1/nodes")) || strings.HasSuffix(path, "/drain") {
		// Removing Docker namespaces/filesystems can exceed the ordinary 10s
		// control request timeout. The scenario context still bounds cleanup.
		cleanupClient := *s.client
		cleanupClient.Timeout = 190 * time.Second
		client = &cleanupClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if method == http.MethodPost && path == "/api/v1/nodes" && resp.StatusCode == http.StatusTooManyRequests {
			return errAgentCapacityReached
		}
		return fmt.Errorf("agent returned %s: %s", resp.Status, strings.TrimSpace(string(message)))
	}
	if output != nil {
		return json.NewDecoder(resp.Body).Decode(output)
	}
	return nil
}

func (s *Server) updateExperiment(runID string, update func(*model.Experiment)) {
	s.state.persistMu.Lock()
	defer s.state.persistMu.Unlock()
	if deleted, err := s.state.resultDeletedLocked(runID); err != nil || deleted {
		if err != nil {
			s.logger.Warn("check deleted experiment", "run", runID, "error", err)
		}
		return
	}
	s.state.mu.Lock()
	experiment, exists := s.state.experiments[runID]
	if !exists {
		s.state.mu.Unlock()
		return
	}
	update(&experiment)
	s.state.experiments[runID] = experiment
	s.state.mu.Unlock()
	s.state.notify()
	if err := s.persistExperiment(experiment); err != nil {
		s.logger.Warn("persist experiment state", "run", runID, "error", err)
		s.state.recordRunWriteError(runID, err)
	}
}

func (s *Server) persistManifest(experiment model.Experiment, raw []byte) error {
	dir := filepath.Join(s.config.DataDir, currentRunsDirectory, safeName(experiment.ID))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create run directory: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "scenario.yaml"), raw, 0o644); err != nil {
		return fmt.Errorf("write scenario manifest: %w", err)
	}
	return s.persistExperiment(experiment)
}

func (s *Server) persistExperiment(experiment model.Experiment) error {
	dir := filepath.Join(s.config.DataDir, currentRunsDirectory, safeName(experiment.ID))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create run directory: %w", err)
	}
	metadata, err := json.MarshalIndent(experiment, "", "  ")
	if err != nil {
		return err
	}
	if err := writeFileAtomic(filepath.Join(dir, "experiment.json"), metadata, 0644); err != nil {
		return err
	}
	s.state.markRunArchiveDirty(experiment.ID)
	return nil
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
