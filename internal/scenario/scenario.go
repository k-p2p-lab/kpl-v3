package scenario

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/k-p2p-lab/kpl-v3/internal/distribution"
	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/libp2p/go-libp2p/core/peer"
	"gopkg.in/yaml.v3"
)

type Scenario struct {
	Version            int                         `json:"version" yaml:"version"`
	Name               string                      `json:"name" yaml:"name"`
	Seed               int64                       `json:"seed" yaml:"seed"`
	OnExit             string                      `json:"onExit,omitempty" yaml:"onExit,omitempty"`
	JobShutdownTimeout string                      `json:"jobShutdownTimeout,omitempty" yaml:"jobShutdownTimeout,omitempty"`
	Profiles           map[string]model.NodeConfig `json:"profiles,omitempty" yaml:"profiles,omitempty"`
	Phases             []Phase                     `json:"phases" yaml:"phases"`
}

type Phase struct {
	Name            string                 `json:"name" yaml:"name"`
	Job             string                 `json:"job,omitempty" yaml:"job,omitempty"`
	Jobs            []string               `json:"jobs,omitempty" yaml:"jobs,omitempty"`
	Action          string                 `json:"action" yaml:"action"`
	OnError         string                 `json:"onError,omitempty" yaml:"onError,omitempty"`
	AgentID         string                 `json:"agentId,omitempty" yaml:"agentId,omitempty"`
	Placement       string                 `json:"placement,omitempty" yaml:"placement,omitempty"`
	Group           string                 `json:"group" yaml:"group"`
	NodeIDs         []string               `json:"nodeIds,omitempty" yaml:"nodeIds,omitempty"`
	PeerIDs         []string               `json:"peerIds,omitempty" yaml:"peerIds,omitempty"`
	Role            string                 `json:"role" yaml:"role"`
	Profile         string                 `json:"profile,omitempty" yaml:"profile,omitempty"`
	NodeType        string                 `json:"type,omitempty" yaml:"type,omitempty"`
	Count           int                    `json:"count" yaml:"count"`
	Repeat          int                    `json:"repeat,omitempty" yaml:"repeat,omitempty"`
	Parallel        bool                   `json:"parallel,omitempty" yaml:"parallel,omitempty"`
	Parallelism     int                    `json:"parallelism,omitempty" yaml:"parallelism,omitempty"`
	Await           *bool                  `json:"await,omitempty" yaml:"await,omitempty"`
	Duration        string                 `json:"duration" yaml:"duration"`
	Timeout         string                 `json:"timeout" yaml:"timeout"`
	Message         string                 `json:"message,omitempty" yaml:"message,omitempty"`
	ReadyRatio      float64                `json:"readyRatio" yaml:"readyRatio"`
	MinCount        int                    `json:"minCount,omitempty" yaml:"minCount,omitempty"`
	PayloadSize     int                    `json:"payloadSize" yaml:"payloadSize"`
	PayloadEncoding string                 `json:"payloadEncoding,omitempty" yaml:"payloadEncoding,omitempty"`
	DeliveryWindow  string                 `json:"deliveryWindow,omitempty" yaml:"deliveryWindow,omitempty"`
	Topic           string                 `json:"topic" yaml:"topic"`
	Topology        *model.TopologyConfig  `json:"topology,omitempty" yaml:"topology,omitempty"`
	Schedule        *model.ProfileSchedule `json:"schedule,omitempty" yaml:"schedule,omitempty"`
	NetworkMutable  bool                   `json:"-" yaml:"-"`
	Interval        Distribution           `json:"interval" yaml:"interval"`
	Lifetime        Distribution           `json:"lifetime" yaml:"lifetime"`
	Node            model.NodeConfig       `json:"node" yaml:"node"`
}

type Distribution = distribution.Distribution

func Parse(data []byte) (Scenario, error) {
	var s Scenario
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&s); err != nil {
		return s, fmt.Errorf("decode scenario: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err != nil {
			return s, fmt.Errorf("decode scenario: %w", err)
		}
		return s, fmt.Errorf("scenario must contain exactly one YAML document")
	}
	if err := s.Validate(); err != nil {
		return s, err
	}
	return s, nil
}

func (s *Scenario) Validate() error {
	if s.Version == 0 {
		s.Version = 1
	}
	if s.Version != 1 && s.Version != 2 && s.Version != 3 {
		return fmt.Errorf("unsupported scenario version %d", s.Version)
	}
	if strings.TrimSpace(s.Name) == "" {
		return fmt.Errorf("scenario name is required")
	}
	if len(s.Phases) == 0 {
		return fmt.Errorf("scenario must contain at least one phase")
	}
	if s.OnExit == "" {
		s.OnExit = "cancel"
	}
	if s.OnExit != "cancel" && s.OnExit != "drain" {
		return fmt.Errorf("onExit must be cancel or drain")
	}
	if s.JobShutdownTimeout == "" {
		s.JobShutdownTimeout = "3m"
	}
	shutdownTimeout, err := time.ParseDuration(s.JobShutdownTimeout)
	if err != nil {
		return fmt.Errorf("invalid jobShutdownTimeout: %w", err)
	}
	if shutdownTimeout <= 0 {
		return fmt.Errorf("jobShutdownTimeout must be positive")
	}
	backgroundJoins := make(map[string]map[string]struct{})
	knownJobs := make(map[string]struct{})
	joinedGroups := make(map[string][]*Phase)
	for i := range s.Phases {
		p := &s.Phases[i]
		if p.Action == "sleep" {
			p.Action = "wait"
		}
		if p.Action == "reset" {
			p.Action = "stop-all"
		}
		if p.Name == "" {
			p.Name = fmt.Sprintf("%s-%d", p.Action, i+1)
		}
		if p.NodeType != "" {
			if preset, ok := model.BuiltInNodeConfig(p.NodeType); ok {
				p.NodeType = preset.Type
			}
		}
		if p.Job == "" && p.Action != "topology" {
			p.Job = fmt.Sprintf("phase-%d", i+1)
		}
		if p.Repeat == 0 {
			p.Repeat = 1
		}
		if p.Repeat < 1 {
			return fmt.Errorf("phase %q: repeat must be positive", p.Name)
		}
		if p.Parallelism < 0 {
			return fmt.Errorf("phase %q: parallelism cannot be negative", p.Name)
		}
		if len(p.Jobs) > 0 && p.Action != "wait-ready" && p.Action != "wait-jobs" {
			return fmt.Errorf("phase %q: jobs is only supported for wait-ready and wait-jobs", p.Name)
		}
		if p.Action == "publish" || p.Action == "leave" {
			if p.OnError == "" {
				p.OnError = "fail"
			}
			if p.OnError != "fail" && p.OnError != "continue" {
				return fmt.Errorf("phase %q: onError must be fail or continue", p.Name)
			}
		} else if p.OnError != "" {
			return fmt.Errorf("phase %q: onError is only supported for publish and leave", p.Name)
		}
		if p.Action != "join" && (p.AgentID != "" || p.Placement != "") {
			return fmt.Errorf("phase %q: agentId and placement are only supported for join", p.Name)
		}
		if p.Action != "publish" && p.PayloadEncoding != "" {
			return fmt.Errorf("phase %q: payloadEncoding is only supported for publish", p.Name)
		}
		if p.Action != "publish" && p.DeliveryWindow != "" {
			return fmt.Errorf("phase %q: deliveryWindow is only supported for publish", p.Name)
		}
		if (p.NodeIDs != nil || p.PeerIDs != nil) && (s.Version != 3 || p.Action != "mesh-freeze" && p.Action != "topology" && p.Action != "schedule") {
			return fmt.Errorf("phase %q: nodeIds and peerIds require version 3 mesh-freeze, topology, or schedule", p.Name)
		}
		if p.Schedule != nil && p.Action != "schedule" {
			return fmt.Errorf("phase %q: schedule configuration requires the schedule action", p.Name)
		}
		if p.Topology != nil && p.Action != "topology" {
			return fmt.Errorf("phase %q: topology configuration is only supported for the topology action", p.Name)
		}
		switch p.Action {
		case "schedule":
			if s.Version != 3 {
				return fmt.Errorf("phase %q: schedule requires version 3", p.Name)
			}
			if err := validateSchedulePhase(p); err != nil {
				return fmt.Errorf("phase %q: %w", p.Name, err)
			}
		case "topology":
			if s.Version != 3 {
				return fmt.Errorf("phase %q: topology requires version 3", p.Name)
			}
			if err := validateTopologyPhase(p); err != nil {
				return fmt.Errorf("phase %q: %w", p.Name, err)
			}
			if err := validateTopologyGroup(p, joinedGroups[p.Group]); err != nil {
				return fmt.Errorf("phase %q: %w", p.Name, err)
			}
		case "mesh-freeze":
			if s.Version != 3 {
				return fmt.Errorf("phase %q: mesh-freeze requires version 3", p.Name)
			}
			if err := validateMeshFreezePhase(p); err != nil {
				return fmt.Errorf("phase %q: %w", p.Name, err)
			}
		case "join":
			if p.Placement == "" {
				p.Placement = "balanced"
			}
			if p.Placement != "balanced" && p.Placement != "single-agent" && p.Placement != "random" {
				return fmt.Errorf("phase %q: placement must be balanced, random, or single-agent", p.Name)
			}
			if p.AgentID != "" && strings.TrimSpace(p.AgentID) != p.AgentID {
				return fmt.Errorf("phase %q: agentId must not contain surrounding whitespace", p.Name)
			}
			if p.Group == "" || p.Count < 1 {
				return fmt.Errorf("phase %q: join requires group and positive count", p.Name)
			}
			if p.Role == "" {
				p.Role = "worker"
			}
			if p.Role != "boot" && p.Role != "worker" {
				return fmt.Errorf("phase %q: role must be boot or worker", p.Name)
			}
			if err := p.Interval.Validate(true); err != nil {
				return fmt.Errorf("phase %q interval: %w", p.Name, err)
			}
			if err := p.Lifetime.Validate(true); err != nil {
				return fmt.Errorf("phase %q lifetime: %w", p.Name, err)
			}
			if err := s.resolveNodeConfig(p); err != nil {
				return fmt.Errorf("phase %q: %w", p.Name, err)
			}
			joinedGroups[p.Group] = append(joinedGroups[p.Group], p)
			if !p.ShouldAwait() {
				if backgroundJoins[p.Group] == nil {
					backgroundJoins[p.Group] = make(map[string]struct{})
				}
				backgroundJoins[p.Group][p.Job] = struct{}{}
			}
		case "wait":
			duration, err := time.ParseDuration(p.Duration)
			if err != nil {
				return fmt.Errorf("phase %q: invalid duration: %w", p.Name, err)
			}
			if duration <= 0 {
				return fmt.Errorf("phase %q: duration must be positive", p.Name)
			}
		case "wait-ready":
			if p.MinCount < 0 {
				return fmt.Errorf("phase %q: minCount cannot be negative", p.Name)
			}
			for _, job := range p.Jobs {
				if _, exists := knownJobs[job]; !exists {
					return fmt.Errorf("phase %q: job %q has not been started", p.Name, job)
				}
			}
			if job := firstUnwaitedBackgroundJoin(backgroundJoins, p.Group, p.Jobs); job != "" && p.MinCount == 0 {
				return fmt.Errorf("phase %q: wait-ready after background join %q requires minCount or all relevant join jobs", p.Name, job)
			}
			removeBackgroundJoinJobs(backgroundJoins, p.Jobs)
			if p.ReadyRatio == 0 {
				p.ReadyRatio = 1
			}
			if math.IsNaN(p.ReadyRatio) || p.ReadyRatio <= 0 || p.ReadyRatio > 1 {
				return fmt.Errorf("phase %q: readyRatio must be in (0, 1]", p.Name)
			}
			if p.Timeout == "" {
				p.Timeout = "1m"
			}
			timeout, err := time.ParseDuration(p.Timeout)
			if err != nil {
				return fmt.Errorf("phase %q: invalid timeout: %w", p.Name, err)
			}
			if timeout <= 0 {
				return fmt.Errorf("phase %q: timeout must be positive", p.Name)
			}
		case "wait-jobs":
			if !p.ShouldAwait() {
				return fmt.Errorf("phase %q: wait-jobs must use await: true", p.Name)
			}
			if p.Timeout == "" {
				p.Timeout = "5m"
			}
			timeout, err := time.ParseDuration(p.Timeout)
			if err != nil {
				return fmt.Errorf("phase %q: invalid timeout: %w", p.Name, err)
			}
			if timeout <= 0 {
				return fmt.Errorf("phase %q: timeout must be positive", p.Name)
			}
			if len(p.Jobs) == 0 {
				clear(backgroundJoins)
			} else {
				for _, job := range p.Jobs {
					if _, exists := knownJobs[job]; !exists {
						return fmt.Errorf("phase %q: job %q has not been started", p.Name, job)
					}
				}
				removeBackgroundJoinJobs(backgroundJoins, p.Jobs)
			}
		case "publish":
			if p.DeliveryWindow == "" {
				p.DeliveryWindow = "10s"
			}
			window, err := time.ParseDuration(p.DeliveryWindow)
			if err != nil || window <= 0 || window > time.Hour {
				return fmt.Errorf("phase %q: deliveryWindow must be a positive duration no longer than 1h", p.Name)
			}
			if p.PayloadEncoding == "" {
				p.PayloadEncoding = "envelope"
			}
			if p.PayloadEncoding != "envelope" && p.PayloadEncoding != "raw" {
				return fmt.Errorf("phase %q: payloadEncoding must be envelope or raw", p.Name)
			}
			if p.Group == "" || p.Count < 1 {
				return fmt.Errorf("phase %q: publish requires group and positive count", p.Name)
			}
			if p.PayloadSize <= 0 {
				p.PayloadSize = 32
			}
			if p.PayloadSize > model.MaxPublishPayloadBytes {
				return fmt.Errorf("phase %q: payloadSize exceeds 16 MiB", p.Name)
			}
			if err := p.Interval.Validate(true); err != nil {
				return fmt.Errorf("phase %q interval: %w", p.Name, err)
			}
		case "leave":
			if p.Group == "" || p.Count < 1 {
				return fmt.Errorf("phase %q: leave requires group and positive count", p.Name)
			}
			if err := p.Interval.Validate(true); err != nil {
				return fmt.Errorf("phase %q interval: %w", p.Name, err)
			}
		case "stop-all", "log":
			if p.Action == "stop-all" {
				clear(backgroundJoins)
				clear(knownJobs)
				clear(joinedGroups)
			}
		default:
			return fmt.Errorf("phase %q: unknown action %q", p.Name, p.Action)
		}
		if !p.ShouldAwait() && p.Action != "join" && p.Action != "publish" && p.Action != "leave" && p.Action != "schedule" {
			return fmt.Errorf("phase %q: await:false is only supported for join, publish, leave, and schedule", p.Name)
		}
		if p.Parallel && p.Action != "join" && p.Action != "publish" && p.Action != "leave" {
			return fmt.Errorf("phase %q: parallel is only supported for join, publish, and leave", p.Name)
		}
		if !p.ShouldAwait() {
			if _, exists := knownJobs[p.Job]; exists {
				return fmt.Errorf("phase %q: duplicate background job %q", p.Name, p.Job)
			}
			knownJobs[p.Job] = struct{}{}
		}
	}
	return s.validateProfileSchedules()
}

func validateMeshFreezePhase(p *Phase) error {
	if err := validatePeerSelector(p); err != nil {
		return err
	}
	if p.Count != 0 || p.Parallelism != 0 || p.Interval != (Distribution{}) || p.Lifetime != (Distribution{}) || p.Topic != "" {
		return fmt.Errorf("mesh-freeze does not support count, parallelism, interval, lifetime, or topic; it freezes all selected peers and their joined topics")
	}
	if p.Timeout == "" {
		p.Timeout = "30s"
	}
	timeout, err := time.ParseDuration(p.Timeout)
	if err != nil || timeout <= 0 {
		return fmt.Errorf("mesh-freeze timeout must be a positive duration")
	}
	return nil
}

func validatePeerSelector(p *Phase) error {
	if p.Group == "" && len(p.NodeIDs) == 0 && len(p.PeerIDs) == 0 {
		return fmt.Errorf("%s requires group, nodeIds, or peerIds", p.Action)
	}
	if strings.TrimSpace(p.Group) != p.Group {
		return fmt.Errorf("%s group must not contain surrounding whitespace", p.Action)
	}
	for _, selector := range []struct {
		name string
		ids  []string
	}{{"nodeIds", p.NodeIDs}, {"peerIds", p.PeerIDs}} {
		if selector.ids != nil && len(selector.ids) == 0 {
			return fmt.Errorf("%s %s must not be empty when specified", p.Action, selector.name)
		}
		seen := make(map[string]bool, len(selector.ids))
		for _, id := range selector.ids {
			if id == "" || id == "." || id == ".." || strings.IndexFunc(id, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 || strings.ContainsAny(id, "/\\?#") {
				return fmt.Errorf("%s %s contains an invalid ID %q", p.Action, selector.name, id)
			}
			if selector.name == "peerIds" {
				if _, err := peer.Decode(id); err != nil {
					return fmt.Errorf("%s peerIds contains an invalid libp2p peer ID %q", p.Action, id)
				}
			}
			if seen[id] {
				return fmt.Errorf("%s %s contains duplicate ID %q", p.Action, selector.name, id)
			}
			seen[id] = true
		}
	}
	if p.Role != "" && p.Role != "boot" && p.Role != "worker" {
		return fmt.Errorf("%s role must be boot or worker", p.Action)
	}
	return nil
}

func validateTopologyPhase(p *Phase) error {
	if err := validatePeerSelector(p); err != nil {
		return err
	}
	if p.Topology == nil {
		return fmt.Errorf("topology action requires topology configuration")
	}
	if p.Topic == "" || strings.TrimSpace(p.Topic) != p.Topic || p.Topic == "*" || len(p.Topic) > 1024 {
		return fmt.Errorf("topology requires one explicit topic of at most 1024 bytes")
	}
	if p.Repeat != 1 || !p.ShouldAwait() || p.Parallel || p.Parallelism != 0 {
		return fmt.Errorf("topology requires repeat: 1, await: true, and no parallel or parallelism")
	}
	if p.Job != "" || p.Profile != "" || p.Duration != "" || p.Message != "" || p.ReadyRatio != 0 || p.MinCount != 0 || p.PayloadSize != 0 || p.Interval != (Distribution{}) || p.Lifetime != (Distribution{}) || !reflect.DeepEqual(p.Node, model.NodeConfig{}) {
		return fmt.Errorf("topology does not support job, profile, node, duration, message, readiness, payload, interval, or lifetime settings")
	}
	if err := p.Topology.Validate(p.Count); err != nil {
		return err
	}
	if p.Timeout == "" {
		// The Controller chooses the default from the actual selected cohort.
		// Keep omission distinct from an explicitly configured hard deadline.
		return nil
	}
	timeout, err := time.ParseDuration(p.Timeout)
	if err != nil || timeout <= 0 {
		return fmt.Errorf("topology timeout must be a positive duration")
	}
	return nil
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func firstUnwaitedBackgroundJoin(background map[string]map[string]struct{}, group string, waited []string) string {
	var candidates []string
	for candidateGroup, jobs := range background {
		if group != "" && candidateGroup != group {
			continue
		}
		for job := range jobs {
			if !contains(waited, job) {
				candidates = append(candidates, job)
			}
		}
	}
	sort.Strings(candidates)
	if len(candidates) == 0 {
		return ""
	}
	return candidates[0]
}

func removeBackgroundJoinJobs(background map[string]map[string]struct{}, waited []string) {
	for group, jobs := range background {
		for _, job := range waited {
			delete(jobs, job)
		}
		if len(jobs) == 0 {
			delete(background, group)
		}
	}
}

func (s *Scenario) resolveNodeConfig(phase *Phase) error {
	kind := phase.NodeType
	profile, hasProfile := s.Profiles[phase.Profile]
	if kind == "" && hasProfile && profile.Type != "" {
		kind = profile.Type
	}
	if kind == "" && phase.Profile != "" && !hasProfile {
		kind = phase.Profile
	}
	if kind == "" {
		if phase.Role == "boot" {
			kind = "boot"
		} else {
			kind = "full"
		}
	}
	base, builtIn := model.BuiltInNodeConfig(kind)
	if !builtIn {
		base, _ = model.BuiltInNodeConfig("full")
		base.Type = kind
	}
	if phase.Profile != "" {
		if hasProfile {
			base = base.Merge(profile)
		} else if _, known := model.BuiltInNodeConfig(phase.Profile); !known || !builtIn {
			return fmt.Errorf("unknown profile %q", phase.Profile)
		}
	}
	base = base.Merge(phase.Node)
	if phase.NodeType != "" {
		base.Type = phase.NodeType
	}
	// Built-in aliases are accepted at every overlay level, but the resolved
	// lifecycle type must remain canonical. Otherwise a custom profile with
	// `type: worker` creates a `full` peer at the Agent while later phase
	// filters keep looking for the non-canonical `worker` type.
	if preset, ok := model.BuiltInNodeConfig(base.Type); ok {
		base.Type = preset.Type
	}
	base = base.WithDefaults()
	if err := base.Validate(); err != nil {
		return err
	}
	phase.Node = base
	phase.NodeType = base.Type
	if phase.Profile == "" {
		phase.Profile = base.Type
	}
	return nil
}

func (p Phase) ShouldAwait() bool {
	return p.Await == nil || *p.Await
}

// Keep the scenario sampler name for existing package-local callers.
var samplePoisson = distribution.SamplePoisson
