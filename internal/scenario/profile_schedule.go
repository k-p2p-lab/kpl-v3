package scenario

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func validateSchedulePhase(p *Phase) error {
	// A profile alone can select all its current/future users in this run.
	if p.Profile == "" {
		if err := validatePeerSelector(p); err != nil {
			return err
		}
	} else {
		if strings.TrimSpace(p.Profile) != p.Profile {
			return fmt.Errorf("profile must not contain surrounding whitespace")
		}
		selector := *p
		if selector.Group == "" && len(selector.NodeIDs) == 0 && len(selector.PeerIDs) == 0 {
			selector.Group = "profile-selector"
		}
		if err := validatePeerSelector(&selector); err != nil {
			return err
		}
	}
	if p.Schedule == nil {
		return fmt.Errorf("schedule action requires schedule.changes")
	}
	if err := p.Schedule.Validate(); err != nil {
		return err
	}
	if p.Repeat != 1 || p.Parallel || p.Parallelism != 0 || p.Count != 0 || p.Interval != (Distribution{}) || p.Lifetime != (Distribution{}) || p.Duration != "" || p.Topic != "" || p.Message != "" || p.ReadyRatio != 0 || p.MinCount != 0 || p.PayloadSize != 0 || !reflect.DeepEqual(p.Node, model.NodeConfig{}) {
		return fmt.Errorf("schedule supports selectors, job, await, timeout, and schedule; repeat must be 1")
	}
	if p.Timeout == "" {
		p.Timeout = "2m"
	}
	if timeout, err := time.ParseDuration(p.Timeout); err != nil || timeout <= 0 {
		return fmt.Errorf("schedule timeout must be positive")
	}
	return nil
}

// Profiles are immutable templates. Runtime overlays belong to the run and
// selector, so changing one group cannot alter another group using that profile.
func (p Phase) MatchesProfile(group, profile, nodeType, role string) bool {
	return (p.Group == "" || p.Group == group) && (p.Profile == "" || p.Profile == profile) &&
		(p.NodeType == "" || p.NodeType == nodeType) && (p.Role == "" || p.Role == role)
}

func (s *Scenario) validateProfileSchedules() error {
	for index, schedule := range s.Phases {
		if schedule.Action != "schedule" {
			continue
		}
		begin, end := index, index+1
		for begin > 0 && s.Phases[begin-1].Action != "stop-all" {
			begin--
		}
		for end < len(s.Phases) && s.Phases[end].Action != "stop-all" {
			end++
		}
		matched := false
		for i := begin; i < end; i++ {
			join := &s.Phases[i]
			if join.Action != "join" || !schedule.MatchesProfile(join.Group, join.Profile, join.NodeType, join.Role) {
				continue
			}
			matched = true
			config := join.Node
			for step, change := range schedule.Schedule.Changes {
				if change.Set.Network != nil {
					join.NetworkMutable = true
				}
				// Explicit IDs may select only some joins. Runtime prepare checks
				// their actual configuration; grant potential network capability.
				if len(schedule.NodeIDs) > 0 || len(schedule.PeerIDs) > 0 {
					continue
				}
				var err error
				config, err = change.Set.Apply(config)
				if err != nil {
					return fmt.Errorf("phase %q change %d, join %q: %w", schedule.Name, step+1, join.Name, err)
				}
			}
		}
		if !matched && len(schedule.NodeIDs) == 0 && len(schedule.PeerIDs) == 0 {
			return fmt.Errorf("phase %q: schedule selectors do not match any join in this generation", schedule.Name)
		}
	}
	return nil
}
