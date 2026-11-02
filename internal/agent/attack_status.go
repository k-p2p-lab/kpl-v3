package agent

import (
	"maps"
	"strconv"
	"time"
)

// Caller holds Server.mu and has checked the enclosing Peer source time. Attack
// values are captured before Node.LastSeen, so they need their own revision/time
// check to prevent a crossed status from relabeling an older policy as current.
func updateProcessAttackStatus(proc *process, metadata map[string]string) {
	keys := []string{"pubsubAuthorId", "pubsubAuthorMode"}
	_, hasOptions := metadata["attackOptions"]
	_, hasStats := metadata["attackStats"]
	if revisionText, present := metadata["attackRevision"]; present && hasOptions && hasStats {
		revision, revisionErr := strconv.ParseUint(revisionText, 10, 64)
		observed, observedErr := time.Parse(time.RFC3339Nano, metadata["attackStatsObservedAt"])
		previous, previousErr := strconv.ParseUint(proc.node.Metadata["attackRevision"], 10, 64)
		previousTime, previousTimeErr := time.Parse(time.RFC3339Nano, proc.node.Metadata["attackStatsObservedAt"])
		if revisionErr == nil && observedErr == nil && !observed.IsZero() && (previousErr != nil || revision > previous || revision == previous && (previousTimeErr != nil || observed.After(previousTime))) {
			keys = append(keys, "attackOptions", "attackStats", "attackRevision", "attackStatsObservedAt", "attackRegistryObservedAt")
		}
	}
	var updated map[string]string
	for _, key := range keys {
		if value, exists := metadata[key]; exists {
			if updated == nil {
				updated = maps.Clone(proc.node.Metadata)
				if updated == nil {
					updated = make(map[string]string)
				}
			}
			updated[key] = value
		}
	}
	if updated != nil {
		proc.node.Metadata = updated
	}
}
