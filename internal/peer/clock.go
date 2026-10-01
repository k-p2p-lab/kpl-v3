package peer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	controllerClockBasis                = "controller-offset-v1"
	controllerClockProbes               = 7
	controllerClockStartupBudget        = 5 * time.Second
	controllerClockStartupRetryInterval = 250 * time.Millisecond
	controllerClockUnsyncedInterval     = 5 * time.Second
	controllerClockResyncInterval       = 30 * time.Second
	controllerClockMaxAge               = 2 * time.Minute
)

type controllerClockEstimate struct {
	offset      time.Duration
	uncertainty time.Duration
}

// controllerClockSample is immutable after publication through telemetry.clock.
// Keeping the values together prevents readers from combining different refresh
// generations.
type controllerClockSample struct {
	offset      time.Duration
	uncertainty time.Duration
	sampledAt   time.Time
}

type controllerClockReading struct {
	timestamp    time.Time
	offset       time.Duration
	uncertainty  time.Duration
	synchronized bool
}

// estimateControllerClock uses the minimum-RTT sample from several Controller
// health requests. The midpoint estimate removes constant host clock offsets;
// half the round trip is retained as an explicit error bound.
func estimateControllerClock(parent context.Context, controllerURL string, client *http.Client) (controllerClockEstimate, error) {
	return estimateControllerClockWithRetry(parent, controllerURL, client, controllerClockStartupBudget, controllerClockStartupRetryInterval)
}

func estimateControllerClockWithRetry(parent context.Context, controllerURL string, client *http.Client, budget, retryInterval time.Duration) (controllerClockEstimate, error) {
	if client == nil {
		client = http.DefaultClient
	}
	if budget <= 0 {
		budget = controllerClockStartupBudget
	}
	ctx, cancel := context.WithTimeout(parent, budget)
	defer cancel()
	endpoint := strings.TrimRight(controllerURL, "/") + "/api/v1/health"
	var best controllerClockEstimate
	bestRoundTrip := time.Duration(1<<63 - 1)
	var lastErr error
	for attempt := range controllerClockProbes {
		probeCtx, stopProbe := context.WithTimeout(ctx, time.Second)
		started := time.Now()
		req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, endpoint, nil)
		if err == nil {
			var response struct {
				Time time.Time `json:"time"`
			}
			resp, requestErr := client.Do(req)
			if requestErr != nil {
				err = requestErr
			} else {
				body := io.LimitReader(resp.Body, 64<<10)
				if resp.StatusCode != http.StatusOK {
					err = fmt.Errorf("Controller clock endpoint returned %s", resp.Status)
				} else if decodeErr := json.NewDecoder(body).Decode(&response); decodeErr != nil {
					err = fmt.Errorf("decode Controller clock: %w", decodeErr)
				} else if response.Time.IsZero() {
					err = fmt.Errorf("Controller clock endpoint returned an empty time")
				} else {
					// Do returns after headers; include receipt and decoding of the
					// timestamp body in the midpoint and uncertainty interval.
					finished := time.Now()
					roundTrip := finished.Sub(started)
					if roundTrip >= 0 && roundTrip < bestRoundTrip {
						midpoint := started.Add(roundTrip / 2)
						best = controllerClockEstimate{offset: response.Time.Sub(midpoint), uncertainty: roundTrip / 2}
						bestRoundTrip = roundTrip
					}
				}
				_ = resp.Body.Close()
			}
		}
		stopProbe()
		if err != nil {
			lastErr = err
		}
		if ctx.Err() != nil {
			break
		}
		// Space fast failures across the startup budget. Without this delay a
		// brief 503 or connection refusal can consume all probes immediately.
		if err != nil && bestRoundTrip == time.Duration(1<<63-1) && attempt+1 < controllerClockProbes && retryInterval > 0 {
			timer := time.NewTimer(retryInterval)
			select {
			case <-ctx.Done():
				timer.Stop()
			case <-timer.C:
			}
		}
	}
	if bestRoundTrip == time.Duration(1<<63-1) {
		if ctx.Err() != nil {
			lastErr = ctx.Err()
		}
		if lastErr == nil {
			lastErr = fmt.Errorf("no valid Controller clock sample")
		}
		return controllerClockEstimate{}, fmt.Errorf("synchronize with Controller clock: %w", lastErr)
	}
	return best, nil
}
