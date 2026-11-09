package controller

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

const workerCreateLeaseLimit = 4096

type workerCallTransportError struct{ error }

func (e *workerCallTransportError) Unwrap() error { return e.error }

func classifyWorkerCreateTransport(err error) error {
	var transport *workerCallTransportError
	if !errors.As(err, &transport) {
		return err
	}
	// A connection can itself close with ErrBusy after admitting other RPCs.
	// Only a request-specific remote refusal establishes no handler admission.
	var refused *workerproc.RemoteError
	if errors.As(transport, &refused) && refused.Code == "busy" {
		return &agentAdmissionDeferredError{after: time.Second, reason: "Run broker deferred this create before admission"}
	}
	return &uncertainNodeCreateError{err}
}

// This is a per-pending-create capability, not a cache of past Node IDs. The
// reservation pins an Agent instance; a possible HTTP dispatch additionally
// pins the complete original body. Never expire a possibly admitted creation:
// only its acknowledgement, explicit release, generation fence or Run close
// retires this evidence. The hard limit bounds abandoned worker requests.
type workerCreateLease struct {
	agentID, baseURL  string
	startedAt         time.Time
	generation        uint64
	digest            [sha256.Size]byte
	attempted         bool
	possibleAdmission bool
	inFlight          int
}

type workerCreateAuthorizationError struct {
	reason                        string
	nodeID, agentID, reservedBy   string
	generation, currentGeneration uint64
	cause                         error
}

func (e *workerCreateAuthorizationError) Error() string {
	return fmt.Sprintf("Run create authorization %s (run generation %d, request generation %d, node %s, Agent %s)", e.reason, e.currentGeneration, e.generation, e.nodeID, e.agentID)
}
func (e *workerCreateAuthorizationError) Unwrap() error { return e.cause }

func (b *runWorkerBroker) checkCreateReservation(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || b.drained || b.createFenced {
		return errors.New("cannot reserve after terminal Run generation fence or finalization")
	}
	if lease := b.createLeases[id]; lease != nil && lease.attempted {
		return &uncertainNodeCreateError{errors.New("cannot reserve again while the original create acknowledgement is unresolved")}
	}
	return nil
}

func (b *runWorkerBroker) bindCreateReservation(id string, owner model.Agent) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || b.drained || b.createFenced {
		return errors.New("cannot bind a create reservation after Run finalization")
	}
	if previous := b.createLeases[id]; previous != nil {
		if previous.agentID == owner.ID && previous.baseURL == owner.URL && previous.startedAt.Equal(owner.StartedAt) && previous.generation == b.generation {
			return nil
		}
		return errors.New("cannot replace an unresolved create reservation identity")
	}
	if len(b.createLeases) >= workerCreateLeaseLimit {
		return errors.New("Run pending create lease limit reached")
	}
	if b.createLeases == nil {
		b.createLeases = make(map[string]*workerCreateLease)
	}
	b.createLeases[id] = &workerCreateLease{agentID: owner.ID, baseURL: owner.URL, startedAt: owner.StartedAt, generation: b.generation}
	return nil
}

func (b *runWorkerBroker) releaseCreateLease(id string) {
	b.mu.Lock()
	delete(b.createLeases, id)
	b.mu.Unlock()
}

func (b *runWorkerBroker) authorizeCreateAgent(call workerAgentCall) error {
	var request model.CreateNodeRequest
	if err := json.Unmarshal(call.Input, &request); err != nil {
		return err
	}
	s := b.server.state
	s.mu.RLock()
	defer s.mu.RUnlock()
	b.mu.Lock()
	defer b.mu.Unlock()
	lease := b.createLeases[request.ID]
	denied := func(reason string, cause error) error {
		agentID := s.reservations[request.ID]
		if lease != nil {
			agentID = lease.agentID
		}
		return &workerCreateAuthorizationError{reason: reason, nodeID: request.ID, agentID: agentID, reservedBy: s.reservations[request.ID], generation: request.Generation, currentGeneration: b.generation, cause: cause}
	}
	if call.Path != "/api/v1/nodes" {
		return denied("create-route-changed", nil)
	}
	if b.closed {
		return denied("lease-closed", nil)
	}
	run, exists := s.experiments[b.runID]
	if !exists || run.ID != b.runID || run.ExecutionID != b.executionID {
		return denied("run-identity-changed", nil)
	}
	if run.StopRequested || run.State == "canceled" {
		return denied("stop-requested", context.Canceled)
	}
	if run.State != "running" {
		return denied("run-not-running", nil)
	}
	if b.drained {
		return denied("run-drained", nil)
	}
	if b.createFenced {
		return denied("run-create-fenced", nil)
	}
	if request.RunID != b.runID || !strings.HasPrefix(request.ID, b.runID+"-") {
		return denied("foreign-run", nil)
	}
	if request.Generation != b.generation {
		return denied("generation-mismatch", nil)
	}
	if lease == nil {
		return denied("reservation-lease-missing", nil)
	}
	if lease.generation != b.generation {
		return denied("reservation-generation-changed", nil)
	}
	owner, exists := s.agents[lease.agentID]
	if !exists {
		return denied("agent-unregistered", nil)
	}
	if owner.URL != call.BaseURL || lease.baseURL != call.BaseURL || !owner.StartedAt.Equal(lease.startedAt) || !request.AgentStartedAt.IsZero() && !request.AgentStartedAt.Equal(owner.StartedAt) {
		return denied("agent-instance-changed", nil)
	}
	if !owner.RunProcesses || !owner.RunDrain {
		return denied("agent-capability-changed", nil)
	}
	digest := sha256.Sum256(call.Input)
	if lease.attempted && lease.digest != digest {
		return denied("create-request-changed", nil)
	}
	if lease.attempted && (!owner.CreateReplay || owner.StartedAt.IsZero() || !request.AgentStartedAt.Equal(owner.StartedAt)) {
		return denied("create-replay-not-supported", nil)
	}
	reserved := s.reservations[request.ID] == owner.ID
	if reservedBy := s.reservations[request.ID]; reservedBy != "" && reservedBy != owner.ID {
		return denied("reservation-owner-changed", nil)
	}
	if !reserved {
		// A heartbeat can consume the reservation before a lost HTTP response
		// is replayed. Existing Node metadata alone is insufficient: require
		// this broker's earlier reservation and possible dispatch of these
		// exact bytes, plus the Agent's instance-fenced replay contract.
		node, known := s.nodes[request.ID]
		if !lease.attempted {
			return denied("reservation-missing", nil)
		}
		if !known {
			return denied("replay-peer-unconfirmed", nil)
		}
		if node.ID != request.ID || node.RunID != b.runID || node.Generation != request.Generation || node.AgentID != owner.ID || node.Group != request.Group {
			return denied("replay-peer-identity-changed", nil)
		}
		if node.State != model.NodeStarting && node.State != model.NodeReady {
			return denied("replay-peer-stopping-or-terminal", nil)
		}
	} else if !lease.attempted {
		// The worker checks a periodically refreshed inventory. This check is
		// authoritative, but a changed lease/Disabled/cooldown state before the
		// first HTTP attempt proves no mutation occurred and allows placement
		// to retry through its existing typed pre-admission path.
		reason := ""
		switch {
		case owner.Disabled:
			reason = "agent-disabled"
		case !agentIsOnline(owner, time.Now()):
			reason = "agent-offline"
		case time.Now().Before(s.agentAdmissionUntil[owner.ID]):
			reason = "agent-admission-cooldown"
		}
		if reason != "" {
			return denied(reason, &agentAdmissionDeferredError{after: time.Second, reason: reason})
		}
	}
	lease.digest, lease.attempted = digest, true
	lease.inFlight++
	b.fenced = false
	return nil
}

func (b *runWorkerBroker) uncertainCreateAuthorization(call workerAgentCall, err error) error {
	if err == nil || call.Method != http.MethodPost || call.Path != "/api/v1/nodes" {
		return err
	}
	var request model.CreateNodeRequest
	if json.Unmarshal(call.Input, &request) != nil {
		return err
	}
	b.mu.Lock()
	lease := b.createLeases[request.ID]
	attempted := lease != nil && lease.attempted
	b.mu.Unlock()
	if attempted {
		var uncertain *uncertainNodeCreateError
		if !errors.As(err, &uncertain) {
			return &uncertainNodeCreateError{err}
		}
	}
	return err
}

func (b *runWorkerBroker) dispatchAgent(ctx context.Context, call workerAgentCall) (json.RawMessage, error) {
	err := ctx.Err()
	if err == nil {
		err = b.authorizeAgent(call)
	}
	if err != nil {
		err = b.uncertainCreateAuthorization(call, err)
		var rejected *workerCreateAuthorizationError
		if errors.As(err, &rejected) {
			var deferred *agentAdmissionDeferredError
			var uncertain *uncertainNodeCreateError
			b.server.logger.Warn("Run worker create authorization rejected", "runId", b.runID, "executionId", b.executionID, "nodeId", rejected.nodeID, "agentId", rejected.agentID, "reason", rejected.reason, "generation", rejected.generation, "currentGeneration", rejected.currentGeneration, "reservedBy", rejected.reservedBy, "canceled", errors.Is(err, context.Canceled), "deferred", errors.As(err, &deferred), "uncertainAdmission", errors.As(err, &uncertain))
		}
		return nil, err
	}
	var output json.RawMessage
	var target any
	if call.Output {
		target = &output
	}
	var input any
	if len(call.Input) > 0 {
		input = call.Input
	}
	err = b.server.callAgent(ctx, call.BaseURL, call.Method, call.Path, input, target)
	if call.Method == http.MethodPost && call.Path == "/api/v1/nodes" {
		err = b.finishCreateAgentCall(call, err)
	}
	return output, err
}

func (b *runWorkerBroker) finishCreateAgentCall(call workerAgentCall, err error) error {
	// A refusal of a replay says nothing about an earlier interrupted POST.
	// Only a first definite no-create response, with no other dispatch in
	// flight, permits discarding the original admission capability.
	var request model.CreateNodeRequest
	if json.Unmarshal(call.Input, &request) != nil {
		return err
	}
	var deferred *agentAdmissionDeferredError
	notCreated := errors.As(err, &deferred) || errors.Is(err, errAgentCapacityReached)
	b.mu.Lock()
	lease := b.createLeases[request.ID]
	priorUnknown := false
	if lease != nil && lease.digest == sha256.Sum256(call.Input) {
		lease.inFlight = max(0, lease.inFlight-1)
		priorUnknown = lease.possibleAdmission || lease.inFlight > 0
		if notCreated && !priorUnknown {
			lease.attempted = false
		} else {
			lease.possibleAdmission = true
		}
	}
	b.mu.Unlock()
	if err != nil && (!notCreated || priorUnknown) {
		var uncertain *uncertainNodeCreateError
		if !errors.As(err, &uncertain) {
			return &uncertainNodeCreateError{err}
		}
	}
	return err
}
