package scenario

import "fmt"

// Check resolved join configurations in the current stop-all generation.
// Churn can remove any subset, so every matching join must support freezing.
// Explicit IDs cannot be mapped to join phases until runtime; the Controller
// checks every selected Peer before sending topology commands in all cases.
func validateTopologyGroup(p *Phase, joins []*Phase) error {
	if p.Group == "" || len(p.NodeIDs) > 0 || len(p.PeerIDs) > 0 {
		return nil
	}
	matched := false
	for _, joined := range joins {
		if p.Role != "" && joined.Role != p.Role || p.NodeType != "" && joined.NodeType != p.NodeType {
			continue
		}
		matched = true
		if enabled := joined.Node.GossipSub.MeshFreeze; enabled == nil || !*enabled {
			return fmt.Errorf("topology group %q requires gossipsub.meshFreeze: true; matching join phase %q does not enable it", p.Group, joined.Name)
		}
	}
	if !matched {
		return fmt.Errorf("topology group %q requires a preceding join matching its type and role filters with gossipsub.meshFreeze: true after the last stop-all", p.Group)
	}
	return nil
}
