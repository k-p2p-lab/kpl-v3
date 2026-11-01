package scenario

import "fmt"

func (s *Scenario) validateAttackVersion() error {
	for name, p := range s.Profiles {
		if p.HasAttackOptions() && s.Version < 4 {
			return fmt.Errorf("profile %q: attack options require version 4", name)
		}
		if err := p.ValidateAttackSyntax(); err != nil {
			return fmt.Errorf("profile %q: %w", name, err)
		}
	}
	for _, p := range s.Phases {
		if (p.Node.HasAttackOptions() || p.Schedule != nil && scheduleHasAttacks(p)) && s.Version < 4 {
			return fmt.Errorf("attack options require version 4")
		}
		if err := p.Node.ValidateAttackSyntax(); err != nil {
			return fmt.Errorf("phase %q: %w", p.Name, err)
		}
	}
	return nil
}

func scheduleHasAttacks(p Phase) bool {
	for _, change := range p.Schedule.Changes {
		if change.Set.HasAttacks() {
			return true
		}
	}
	return false
}
func (s *Scenario) validateAttackSelectors() error {
	// Resolve declared groups within each generation, including future joins.
	for start := 0; start < len(s.Phases); {
		end := start
		groups := map[string]bool{}
		for end < len(s.Phases) && s.Phases[end].Action != "stop-all" {
			if s.Phases[end].Action == "join" {
				groups[s.Phases[end].Group] = true
			}
			end++
		}
		for _, p := range s.Phases[start:end] {
			if p.Action != "join" {
				continue
			}
			for _, selector := range p.Node.AttackSelectors() {
				if selector == nil {
					continue
				}
				for _, group := range selector.Groups {
					if !groups[group] {
						return fmt.Errorf("phase %q: unknown attack target group %q in this generation", p.Name, group)
					}
				}
			}
		}
		start = end + 1
	}
	return nil
}
