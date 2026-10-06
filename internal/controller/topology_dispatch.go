package controller

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const topologyCommandParallelism = 16
const topologyPrepareAttempts = 3

// Allow the bounded worker pool to visit larger domains without increasing its
// connection fanout. An explicit scenario timeout always overrides this budget.
func defaultTopologyPhaseTimeout(targets int) time.Duration {
	waves := max(1, (targets+topologyCommandParallelism-1)/topologyCommandParallelism)
	return 2*time.Minute + time.Duration(waves-1)*5*time.Second
}

type topologyAgentError struct {
	status  int
	message string
}

func (e *topologyAgentError) Error() string { return e.message }

func topologyCommandTimedOut(err error) bool {
	var remote *topologyAgentError
	var networkErr net.Error
	return errors.Is(err, context.DeadlineExceeded) ||
		errors.As(err, &remote) && remote.status == http.StatusGatewayTimeout ||
		errors.As(err, &networkErr) && networkErr.Timeout()
}

func retryTopologyPrepare(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	var remote *topologyAgentError
	if errors.As(err, &remote) {
		return remote.status == http.StatusServiceUnavailable || remote.status == http.StatusGatewayTimeout
	}
	var networkErr net.Error
	var requestErr *url.Error
	return errors.Is(err, context.DeadlineExceeded) || errors.As(err, &networkErr) || errors.As(err, &requestErr)
}

type topologyDispatchStats struct {
	total   atomic.Int64
	retries atomic.Int64
}

func (s *Server) dispatchTopologyCommand(ctx context.Context, runID string, generation uint64, target model.Node, request model.TopologyRequest, stats *topologyDispatchStats) error {
	limit := 1
	if request.Stage == "prepare" {
		limit = topologyPrepareAttempts
	}
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		agent, err := s.currentTopologyAgent(runID, generation, target)
		if err != nil {
			return err
		}
		commandCtx, stop := context.WithTimeout(ctx, model.TopologyRequestTimeout)
		var response model.TopologyResponse
		path := "/api/v1/nodes/" + url.PathEscape(target.ID) + "/topology"
		stats.total.Add(1)
		if attempt > 1 {
			stats.retries.Add(1)
		}
		err = s.callAgent(commandCtx, agent.URL, http.MethodPost, path, request, &response)
		commandErr := commandCtx.Err()
		stop()
		if err == nil {
			if err := response.Validate(request, target.ID, target.PeerID); err != nil {
				return fmt.Errorf("node %q: %w", target.ID, err)
			}
			if _, err := s.currentTopologyAgent(runID, generation, target); err != nil {
				return err
			}
			return ctx.Err()
		}
		failure := fmt.Errorf("%s topology on node %q (attempt %d/%d): %w", request.Stage, target.ID, attempt, limit, err)
		if ctx.Err() != nil {
			return errors.Join(ctx.Err(), failure)
		}
		if errors.Is(commandErr, context.DeadlineExceeded) {
			failure = fmt.Errorf("topology command exceeded its %s request timeout: %w", model.TopologyRequestTimeout, failure)
		}
		if attempt == limit || !retryTopologyPrepare(err) {
			return failure
		}
		// Only prepare is repeated: it may establish connections, but cannot
		// install the graph. Successful peers are not asked to prepare again.
		if err := sleepContext(ctx, time.Duration(attempt)*250*time.Millisecond); err != nil {
			return errors.Join(err, failure)
		}
	}
}
