package agent

import (
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// Called under the Agent state lock. Only scheduled peers may update these
// fields, and an older network revision cannot undo a later tc transition.
func updateProcessNetwork(proc *process, metadata map[string]string) {
	if proc.node.Metadata["networkSchedule"] == "" || metadata["networkPending"] != "false" {
		return
	}
	revision, err := strconv.Atoi(metadata["networkRevision"])
	if err != nil || revision < 0 || revision > 128 {
		return
	}
	if previous, err := strconv.Atoi(proc.node.Metadata["networkRevision"]); err == nil && revision <= previous {
		return
	}
	raw := metadata["network"]
	if len(raw) > 16<<10 {
		return
	}
	var config *model.NetworkConfig
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&config) != nil || config == nil || config.Schedule != nil || config.DelayDistribution != nil || config.Validate() != nil {
		return
	}
	if decoder.Decode(new(any)) != io.EOF {
		return
	}
	appliedAt, err := time.Parse(time.RFC3339Nano, metadata["networkAppliedAt"])
	if err != nil {
		return
	}
	setProcessMetadata(proc, "network", raw)
	setProcessMetadata(proc, "networkPending", "false")
	setProcessMetadata(proc, "networkRevision", strconv.Itoa(revision))
	setProcessMetadata(proc, "networkAppliedAt", appliedAt.UTC().Format(time.RFC3339Nano))
}
