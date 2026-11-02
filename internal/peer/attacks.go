package peer

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	corepeer "github.com/libp2p/go-libp2p/core/peer"
)

const attackRegistryMaxAge = 5 * time.Second

var errAmbiguousAttackPublisher = errors.New("publisher selector cannot distinguish nodes sharing a signing identity")

type attackPolicyState struct {
	config   model.NodeConfig
	revision uint64
}
type attackRegistry struct {
	targets     []model.AttackTarget
	observedAt  time.Time
	byTransport map[string][]int
	byAuthor    map[string][]int
}

// Build immutable indexes on the registry fetch worker, before publication to
// routing/validation callbacks. Keep all aliases so partial shared-ID selections
// remain unknown instead of silently broadening to another node.
func newAttackRegistry(targets []model.AttackTarget, observedAt time.Time) *attackRegistry {
	r := &attackRegistry{targets: targets, observedAt: observedAt, byTransport: make(map[string][]int, len(targets)), byAuthor: make(map[string][]int, len(targets))}
	for i, target := range targets {
		r.byTransport[target.PeerID] = append(r.byTransport[target.PeerID], i)
		r.byAuthor[target.AuthorPeerID] = append(r.byAuthor[target.AuthorPeerID], i)
	}
	return r
}

type attackRuntime struct {
	server       *Server
	policy       atomic.Pointer[attackPolicyState]
	registry     atomic.Pointer[attackRegistry]
	mu           sync.Mutex
	counters     map[string]map[string]uint64
	dirty        bool
	detailSecond int64
	detailCount  int
}

func newAttackRuntime(s *Server) *attackRuntime {
	if s.telemetry != nil {
		s.telemetry.reserveFinalAttackStats()
	}
	r := &attackRuntime{server: s, counters: make(map[string]map[string]uint64)}
	r.update(s.config.NodeConfig, s.config.ProfileRevision)
	return r
}
func (r *attackRuntime) update(config model.NodeConfig, revision uint64) {
	r.policy.Store(&attackPolicyState{config: config.WithDefaults(), revision: revision})
	if config.HasAttackOptions() {
		fields := map[string][]string{
			"forwarding":       {"publishConsidered", "publishWithheld", "ihaveConsidered", "ihaveWithheld", "unknown"},
			"invalidMessage":   {"generatedMessages", "queuedRPCs", "droppedRPCs", "skippedRequests", "sendUncertain", "unknown", "generationErrors"},
			"sink":             {"considered", "dropped"},
			"censorship":       {"considered", "dropped", "unknown"},
			"messageDelay":     {"considered", "selected", "queued", "released", "overflow", "canceled", "dropped"},
			"iwantWithholding": {"considered", "withheld", "unknown"},
			"connectionAttack": {"rounds", "candidateObservations", "connectedObservations", "dialAttempts", "dialSucceeded", "dialFailures", "unknown"},
			"graftFlood":       {"generatedGrafts", "queuedRPCs", "droppedRPCs", "suppressedGrafts", "skippedRequests", "sendUncertain", "unknown"},
			"ihaveSpam":        {"generatedRPCs", "generatedIDs", "queuedRPCs", "droppedRPCs", "skippedRequests", "sendUncertain", "unknown"},
		}
		r.mu.Lock()
		for kind := range attackOptions(config) {
			if r.counters[kind] == nil {
				r.counters[kind] = map[string]uint64{}
			}
			for _, field := range fields[kind] {
				if _, exists := r.counters[kind][field]; !exists {
					r.counters[kind][field] = 0
				}
			}
		}
		r.dirty = true
		r.mu.Unlock()
	}
}
func (r *attackRuntime) count(kind, field string, n uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.counters[kind] == nil {
		r.counters[kind] = make(map[string]uint64)
	}
	r.counters[kind][field] += n
	r.dirty = true
}
func (r *attackRuntime) snapshot() map[string]map[string]uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]map[string]uint64, len(r.counters))
	for k, v := range r.counters {
		out[k] = maps.Clone(v)
	}
	return out
}
func attackOptions(c model.NodeConfig) map[string]any {
	out := map[string]any{}
	if c.Forwarding != nil {
		out["forwarding"] = c.Forwarding
	}
	if c.InvalidMessage != nil {
		out["invalidMessage"] = c.InvalidMessage
	}
	if c.Sink != nil {
		out["sink"] = c.Sink
	}
	if c.Censorship != nil {
		out["censorship"] = c.Censorship
	}
	if c.MessageDelay != nil {
		out["messageDelay"] = c.MessageDelay
	}
	if c.ConnectionAttack != nil {
		out["connectionAttack"] = c.ConnectionAttack
	}
	if c.GraftFlood != nil {
		out["graftFlood"] = c.GraftFlood
	}
	if c.IHaveSpam != nil {
		out["ihaveSpam"] = c.IHaveSpam
	}
	if c.IWantWithholding != nil {
		out["iwantWithholding"] = c.IWantWithholding
	}
	return out
}
func attackSelected(seed int64, actor, kind, topic, messageID, requester string, p float64) bool {
	if p <= 0 {
		return false
	}
	if p >= 1 {
		return true
	}
	h := sha256.New()
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(seed))
	h.Write(b[:])
	for _, v := range []string{actor, kind, topic, messageID, requester} {
		binary.BigEndian.PutUint64(b[:], uint64(len(v)))
		h.Write(b[:])
		h.Write([]byte(v))
	}
	sum := h.Sum(nil)
	sample := binary.BigEndian.Uint64(sum[:8]) >> 11
	return float64(sample)/float64(uint64(1)<<53) < p
}
func (r *attackRuntime) selected(kind string, msg *pubsub.Message, p float64, requester string) bool {
	return attackSelected(r.server.config.Seed, r.server.config.Node.ID, kind, msg.GetTopic(), msg.ID, requester, p)
}
func matchesAttackTopic(topics []string, topic string) bool {
	return len(topics) == 0 || slices.Contains(topics, topic)
}
func (r *attackRuntime) matchTransport(selector *model.AttackSelector, id string) (bool, bool) {
	return matchAttackTransport(r.registry.Load(), selector, id)
}
func matchAttackTransport(registry *attackRegistry, selector *model.AttackSelector, id string) (bool, bool) {
	if selector.Empty() {
		return true, true
	}
	if len(selector.Groups)+len(selector.NodeIDs) == 0 {
		return selector.Matches("", "", id), true
	}
	if registry == nil || time.Since(registry.observedAt) > attackRegistryMaxAge {
		return false, false
	}
	known, matched, unmatched := false, false, false
	for _, index := range registry.byTransport[id] {
		target := registry.targets[index]
		known = true
		if selector.Matches(target.Group, target.NodeID, id) {
			matched = true
		} else {
			unmatched = true
		}
	}
	if matched && unmatched {
		return false, false
	}
	return matched, known
}
func (r *attackRuntime) matchPublisher(selector *model.AttackSelector, id string) (bool, bool) {
	if selector.Empty() {
		return true, true
	}
	if len(selector.Groups)+len(selector.NodeIDs) == 0 {
		return selector.Matches("", "", id), true
	}
	registry := r.registry.Load()
	if registry == nil || time.Since(registry.observedAt) > attackRegistryMaxAge {
		return false, false
	}
	known, matched, unmatched := false, false, false
	for _, index := range registry.byAuthor[id] {
		target := registry.targets[index]
		known = true
		if selector.Matches(target.Group, target.NodeID, id) {
			matched = true
		} else {
			unmatched = true
		}
	}
	if matched && unmatched {
		return false, false
	}
	return matched, known
}
func (r *attackRuntime) decide(msg *pubsub.Message) pubsub.InboundMessageDecision {
	state := r.policy.Load()
	c := state.config
	if c.Censorship != nil && c.Censorship.Active() && matchesAttackTopic(c.Censorship.Topics, msg.GetTopic()) {
		matched, known := r.matchPublisher(c.Censorship.Publishers, msg.GetFrom().String())
		if !known {
			r.count("censorship", "unknown", 1)
		}
		if matched {
			r.count("censorship", "considered", 1)
			if r.selected("censorship", msg, *c.Censorship.Probability, "") {
				r.count("censorship", "dropped", 1)
				r.detail("censorship", "drop", msg, state.revision)
				return pubsub.InboundMessageDecision{Drop: true}
			}
		}
	}
	if c.Sink.Active() {
		r.count("sink", "considered", 1)
		if r.selected("sink", msg, *c.Sink.Probability, "") {
			r.count("sink", "dropped", 1)
			r.detail("sink", "drop", msg, state.revision)
			return pubsub.InboundMessageDecision{Drop: true}
		}
	}
	if c.MessageDelay != nil && c.MessageDelay.Active() {
		r.count("messageDelay", "considered", 1)
		if r.selected("messageDelay", msg, *c.MessageDelay.Probability, "") {
			r.count("messageDelay", "selected", 1)
			delay, _ := time.ParseDuration(c.MessageDelay.Delay)
			r.detail("messageDelay", "delay", msg, state.revision)
			return pubsub.InboundMessageDecision{Delay: delay, Tag: state.revision}
		}
	}
	return pubsub.InboundMessageDecision{}
}
func (r *attackRuntime) observeDelay(msg *pubsub.Message, event string, decision pubsub.InboundMessageDecision) {
	r.count("messageDelay", event, 1)
	if event == "overflow" || event == "canceled" {
		r.count("messageDelay", "dropped", 1)
		r.detail("messageDelay", event, msg, decision.Tag)
	}
}
func (r *attackRuntime) allowIWant(requester corepeer.ID, msg *pubsub.Message) bool {
	state := r.policy.Load()
	c := state.config.IWantWithholding
	if c == nil || !c.Active() || !matchesAttackTopic(c.Topics, msg.GetTopic()) {
		return true
	}
	matched, known := r.matchTransport(c.Targets, requester.String())
	if !known {
		r.count("iwantWithholding", "unknown", 1)
	}
	if !matched {
		return true
	}
	r.count("iwantWithholding", "considered", 1)
	if r.selected("iwantWithholding", msg, *c.Probability, requester.String()) {
		r.count("iwantWithholding", "withheld", 1)
		r.detail("iwantWithholding", "withhold", msg, state.revision, requester.String())
		return false
	}
	return true
}
func (r *attackRuntime) detail(kind, decision string, msg *pubsub.Message, revision uint64, remote ...string) {
	remoteID := msg.ReceivedFrom.String()
	if len(remote) > 0 {
		remoteID = remote[0]
	}
	if r.server.telemetry == nil {
		return
	}
	now := time.Now().Unix()
	r.mu.Lock()
	if r.detailSecond != now {
		r.detailSecond = now
		r.detailCount = 0
	}
	if r.detailCount >= 8 {
		r.mu.Unlock()
		return
	}
	r.detailCount++
	r.mu.Unlock()
	r.server.telemetry.emit(model.TraceEvent{Type: "attack_decision", Topic: msg.GetTopic(), RemotePeerID: remoteID, Fields: map[string]any{"attack": kind, "decision": decision, "pubsubMessageId": hex.EncodeToString([]byte(msg.ID)), "authorPeerId": msg.GetFrom().String(), "revision": revision, "sampled": true}})
}
func (r *attackRuntime) flush(final bool) {
	r.mu.Lock()
	dirty := r.dirty
	r.dirty = false
	r.mu.Unlock()
	state := r.policy.Load()
	if !dirty && (!final || !state.config.HasAttackOptions()) {
		return
	}
	if r.server.telemetry != nil {
		r.server.telemetry.emitObservedPriority(func(controllerClockReading) (model.TraceEvent, bool) {
			return model.TraceEvent{Type: "attack_stats", Fields: map[string]any{"counters": r.snapshot(), "options": attackOptions(state.config), "revision": state.revision, "seed": r.server.config.Seed, "final": final, "cumulative": true}}, true
		}, final)
	}
}
func (s *Server) addAttackStatus(node *model.Node) {
	node.Metadata = maps.Clone(node.Metadata)
	if node.Metadata == nil {
		node.Metadata = map[string]string{}
	}
	authorID, mode := s.host.ID().String(), "host"
	author := s.config.NodeConfig.GossipSub.Author
	if publishing := s.config.NodeConfig.GossipSub.Publish; publishing != nil && publishing.Author != nil {
		author = publishing.Author
	}
	if author != nil {
		if author.NoAuthor {
			authorID, mode = "", "anonymous"
		} else {
			_, id, err := author.Identity()
			if err == nil && id != "" {
				authorID, mode = id.String(), "explicit"
			}
		}
	}
	node.Metadata["pubsubAuthorId"], node.Metadata["pubsubAuthorMode"] = authorID, mode
	if s.attacks == nil {
		return
	}
	state := s.attacks.policy.Load()
	if !state.config.HasAttackOptions() {
		return
	}
	raw, _ := json.Marshal(attackOptions(state.config))
	node.Metadata["attackOptions"] = string(raw)
	raw, _ = json.Marshal(s.attacks.snapshot())
	node.Metadata["attackStats"] = string(raw)
	node.Metadata["attackStatsObservedAt"] = time.Now().UTC().Format(time.RFC3339Nano)
	node.Metadata["attackRevision"] = strconv.FormatUint(state.revision, 10)
	if registry := s.attacks.registry.Load(); registry != nil {
		node.Metadata["attackRegistryObservedAt"] = registry.observedAt.Format(time.RFC3339Nano)
	}
}
func (r *attackRuntime) needsRegistry() bool {
	for _, selector := range r.policy.Load().config.AttackSelectors() {
		if selector != nil && len(selector.Groups)+len(selector.NodeIDs) > 0 {
			return true
		}
	}
	c := r.policy.Load().config
	return c.ConnectionAttack != nil && c.ConnectionAttack.Enabled != nil && *c.ConnectionAttack.Enabled
}
func (r *attackRuntime) fetchTargets(ctx context.Context) error {
	endpoint := strings.TrimRight(r.server.config.ControllerURL, "/") + "/api/v1/attack-targets"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	query := req.URL.Query()
	query.Set("runId", r.server.config.Node.RunID)
	query.Set("requesterNodeId", r.server.config.Node.ID)
	query.Set("generation", strconv.FormatUint(r.server.config.Node.Generation, 10))
	req.URL.RawQuery = query.Encode()
	if r.server.config.Token != "" {
		req.Header.Set("Authorization", "Bearer "+r.server.config.Token)
	}
	response, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("attack registry returned %s", response.Status)
	}
	var targets []model.AttackTarget
	decoder := json.NewDecoder(io.LimitReader(response.Body, 8<<20))
	if err := decoder.Decode(&targets); err != nil {
		return err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return fmt.Errorf("attack registry expected one response")
	}
	if len(targets) > 50000 {
		return fmt.Errorf("attack registry exceeds 50000 identity records")
	}
	if err := validateAttackPublisherTargets(r.policy.Load().config, targets); err != nil {
		return err
	}
	r.registry.Store(newAttackRegistry(targets, time.Now()))
	return nil
}
func (r *attackRuntime) run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	defer func() { cancel(); workers.Wait() }()
	spawn := func(fn func(context.Context)) { workers.Add(1); go func() { defer workers.Done(); fn(ctx) }() }
	fatal := make(chan error, 1)
	if r.needsRegistry() {
		spawn(func(ctx context.Context) {
			r.periodic(ctx, time.Second, func() {
				requestCtx, stop := context.WithTimeout(ctx, 3*time.Second)
				err := r.fetchTargets(requestCtx)
				stop()
				if err != nil && ctx.Err() == nil {
					r.count("registry", "failures", 1)
					if errors.Is(err, errAmbiguousAttackPublisher) {
						select {
						case fatal <- err:
						default:
						}
					}
				}
			})
		})
	}
	c := r.policy.Load().config
	if c.InvalidMessage != nil && c.InvalidMessage.Enabled != nil && *c.InvalidMessage.Enabled {
		spawn(r.invalidMessageLoop)
	}
	if c.ConnectionAttack != nil && c.ConnectionAttack.Enabled != nil && *c.ConnectionAttack.Enabled {
		spawn(r.connectionLoop)
	}
	if c.GraftFlood != nil && c.GraftFlood.Enabled != nil && *c.GraftFlood.Enabled {
		spawn(r.graftLoop)
	}
	if c.IHaveSpam != nil && c.IHaveSpam.Enabled != nil && *c.IHaveSpam.Enabled {
		spawn(r.ihaveLoop)
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-fatal:
			return err
		case <-ticker.C:
			r.flush(false)
		}
	}
}
func (r *attackRuntime) periodic(ctx context.Context, interval time.Duration, round func()) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		round()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func validateAttackPublisherTargets(config model.NodeConfig, targets []model.AttackTarget) error {
	selector := config.Censorship
	if selector == nil || !selector.Active() || selector.Publishers.Empty() || len(selector.Publishers.Groups)+len(selector.Publishers.NodeIDs) == 0 {
		return nil
	}
	byAuthor := map[string]int{}
	for _, target := range targets {
		if target.AuthorPeerID == "" {
			if selector.Publishers.Matches(target.Group, target.NodeID, "") {
				return fmt.Errorf("%w: anonymous node %s", errAmbiguousAttackPublisher, target.NodeID)
			}
			continue
		}
		bit := 1
		if selector.Publishers.Matches(target.Group, target.NodeID, target.AuthorPeerID) {
			bit = 2
		}
		byAuthor[target.AuthorPeerID] |= bit
		if byAuthor[target.AuthorPeerID] == 3 {
			return errAmbiguousAttackPublisher
		}
	}
	return nil
}

func (r *attackRuntime) receiveActive() bool {
	c := r.policy.Load().config
	return c.Sink.Active() || c.Censorship != nil && c.Censorship.Active() || c.MessageDelay != nil && c.MessageDelay.Active()
}
