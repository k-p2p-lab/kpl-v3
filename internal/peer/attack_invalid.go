package peer

import (
	"context"
	"errors"
	"time"
)

func (r *attackRuntime) invalidMessageLoop(ctx context.Context) {
	c := r.policy.Load().config.InvalidMessage
	interval, _ := time.ParseDuration(c.Interval)
	var sequence uint64
	r.periodic(ctx, interval, func() {
		roundCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		for _, topic := range c.Topics {
			for _, id := range r.controlTargets("invalidMessage", c.Targets, topic) {
				for range *c.MessagesPerPeer {
					if roundCtx.Err() != nil {
						return
					}
					sequence++
					r.count("invalidMessage", "generatedMessages", 1)
					result, err := r.server.pubsub.SendExperimentalInvalidMessage(roundCtx, id, topic, c.Mode, *c.PayloadSize, sequence)
					if err != nil {
						if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
							r.count("invalidMessage", "sendUncertain", 1)
						} else {
							r.count("invalidMessage", "generationErrors", 1)
						}
						continue
					}
					r.recordControlResult("invalidMessage", result)
				}
			}
		}
	})
}
