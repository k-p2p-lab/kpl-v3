package agent

import (
	"context"
	"testing"
	"testing/synctest"
	"time"
)

func TestScheduleLifetimeStopTreatsZeroAsImmediate(t *testing.T) {
	stopped := make(chan struct{})
	if scheduled := scheduleLifetimeStop(context.Background(), "0s", time.Now(), func() { close(stopped) }); !scheduled {
		t.Fatal("explicit zero lifetime was not scheduled")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("explicit zero lifetime did not stop immediately")
	}
}

func TestScheduleLifetimeStopDistinguishesOmittedAndInvalidLifetime(t *testing.T) {
	for _, value := range []string{"", "-1s", "not-a-duration"} {
		called := make(chan struct{}, 1)
		if scheduled := scheduleLifetimeStop(context.Background(), value, time.Now(), func() { called <- struct{}{} }); scheduled {
			t.Errorf("lifetime %q was unexpectedly scheduled", value)
		}
		select {
		case <-called:
			t.Errorf("lifetime %q unexpectedly invoked stop", value)
		default:
		}
	}
}

func TestScheduleLifetimeStopHonorsProcessCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	called := make(chan struct{}, 1)
	if scheduled := scheduleLifetimeStop(ctx, "1h", time.Now(), func() { called <- struct{}{} }); !scheduled {
		t.Fatal("positive lifetime was not scheduled")
	}
	cancel()
	select {
	case <-called:
		t.Fatal("canceled process invoked lifetime stop")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestScheduleLifetimeStopDoesNotStopAnAlreadyCanceledProcess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		called := make(chan struct{}, 100)
		for range 100 {
			scheduleLifetimeStop(ctx, "0s", time.Now(), func() { called <- struct{}{} })
		}
		synctest.Wait()
		if len(called) != 0 {
			t.Fatalf("expired timers invoked stop %d times after cancellation", len(called))
		}
	})
}

func TestScheduleLifetimeStopUsesContainerCreationDeadline(t *testing.T) {
	for _, admissionDelay := range []time.Duration{3 * time.Second, 7 * time.Second} {
		t.Run(admissionDelay.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				createdAt := time.Now()
				// Model time spent waiting for Agent metadata locks after create.
				time.Sleep(admissionDelay)
				stopped := make(chan struct{}, 1)
				scheduleLifetimeStop(context.Background(), "5s", createdAt, func() { stopped <- struct{}{} })
				synctest.Wait()
				if admissionDelay < 5*time.Second {
					select {
					case <-stopped:
						t.Fatal("stopped before the creation-relative deadline")
					default:
					}
					time.Sleep(5*time.Second - admissionDelay)
					synctest.Wait()
				}
				select {
				case <-stopped:
				default:
					t.Fatal("metadata/scheduler delay extended the peer lifetime")
				}
			})
		})
	}
}
