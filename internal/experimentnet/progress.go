package experimentnet

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// Record only preparation identity and stage names. In particular, a Gateway
// config and the Docker cp input contain credentials and must never be logged.
type preparationProgress struct {
	logger                *slog.Logger
	operation, stage      string
	started, stageStarted time.Time
}

func newPreparationProgress(logger *slog.Logger, operation, run, epoch string) *preparationProgress {
	return &preparationProgress{logger: logger.With("run", run, "epoch", epoch), operation: operation, started: time.Now()}
}

func (p *preparationProgress) step(stage string) {
	if p.stage != "" {
		p.logger.Info(p.operation+" stage finished", "stage", p.stage, "elapsed", time.Since(p.started), "stageElapsed", time.Since(p.stageStarted))
	}
	p.stage, p.stageStarted = stage, time.Now()
	p.logger.Info(p.operation+" stage started", "stage", stage, "elapsed", time.Since(p.started))
}

func (p *preparationProgress) finish(err error) {
	outcome := "ready"
	if err != nil {
		outcome = "failed"
		if errors.Is(err, context.Canceled) {
			outcome = "canceled"
		} else if errors.Is(err, context.DeadlineExceeded) {
			outcome = "timeout"
		}
	}
	// Detailed errors remain in the API response. Avoid logging daemon error
	// output here: it is not a trusted source of credential-free diagnostics.
	p.logger.Info(p.operation+" finished", "stage", p.stage, "elapsed", time.Since(p.started), "stageElapsed", time.Since(p.stageStarted), "outcome", outcome)
}
