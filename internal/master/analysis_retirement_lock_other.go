//go:build !linux

package master

import (
	"context"
	"errors"
	"os"
)

// Swarm operation is supported on Linux. Do not claim durable serialized
// retirement publication on platforms without the production lock primitive.
func lockAnalysisRetirementJournal(context.Context, string) (*os.File, error) {
	return nil, errors.New("analysis retirement migration requires Linux")
}
