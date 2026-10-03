package scenario

import (
	"math/rand"
	"strings"
	"testing"
	"time"
)

func TestParseAndDefaults(t *testing.T) {
	s, err := Parse([]byte(`
version: 1
name: smoke
seed: 42
phases:
  - action: join
    group: boot
    role: boot
    count: 1
  - action: wait-ready
    group: boot
  - action: publish
    group: boot
    count: 1
`))
	if err != nil {
		t.Fatal(err)
	}
	if s.Phases[0].Node.D != 6 || s.Phases[1].ReadyRatio != 1 {
		t.Fatalf("defaults were not applied: %+v", s.Phases)
	}
}

func TestDistributionIsDeterministicAndClamped(t *testing.T) {
	d := Distribution{Model: "normal", Mean: "10ms", Sigma: "100ms", Min: "2ms", Max: "20ms"}
	a := d.Sample(rand.New(rand.NewSource(7)))
	b := d.Sample(rand.New(rand.NewSource(7)))
	if a != b {
		t.Fatalf("same seed produced %v and %v", a, b)
	}
	if a < 2*time.Millisecond || a > 20*time.Millisecond {
		t.Fatalf("sample outside clamp: %v", a)
	}
}

func TestV2CompatibleDistributions(t *testing.T) {
	tests := []struct {
		name         string
		distribution Distribution
		check        func(*testing.T, time.Duration)
	}{
		{
			name:         "poisson integer seconds",
			distribution: Distribution{Model: "poisson", Mean: "3s"},
			check: func(t *testing.T, value time.Duration) {
				t.Helper()
				if value%time.Second != 0 {
					t.Fatalf("poisson value %s is not quantized to seconds", value)
				}
			},
		},
		{
			name:         "gamma beta rate",
			distribution: Distribution{Model: "gamma", Alpha: 2, Beta: 0.5},
			check: func(t *testing.T, value time.Duration) {
				t.Helper()
				if value <= 0 {
					t.Fatalf("gamma value = %s, want positive", value)
				}
			},
		},
		{
			name:         "lognormal v2 sigma",
			distribution: Distribution{Model: "lognormal", Mu: 0, Sigma: "0"},
			check: func(t *testing.T, value time.Duration) {
				t.Helper()
				if value != time.Second {
					t.Fatalf("lognormal(mu=0,sigma=0) = %s, want 1s", value)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.distribution.Validate(false); err != nil {
				t.Fatal(err)
			}
			a := test.distribution.Sample(rand.New(rand.NewSource(99)))
			b := test.distribution.Sample(rand.New(rand.NewSource(99)))
			if a != b {
				t.Fatalf("same seed produced %s and %s", a, b)
			}
			test.check(t, a)
		})
	}
}

func TestDistributionRejectsInvalidBoundsAndGammaAmbiguity(t *testing.T) {
	invalid := []Distribution{
		{Model: "fixed", Value: "1s", Min: "2s", Max: "1s"},
		{Model: "gamma", Alpha: 2, Beta: 1, Scale: "1s"},
		{Model: "lognormal", Mu: 0, Sigma: "not-a-number"},
		{Model: "lognormal", Mu: 0, Sigma: "0.5", LogSigma: 0.25},
	}
	for _, distribution := range invalid {
		if err := distribution.Validate(false); err == nil {
			t.Fatalf("invalid distribution was accepted: %+v", distribution)
		}
	}
}

func TestLargePoissonSamplerRemainsPoisson(t *testing.T) {
	const (
		lambda  = 80.0
		samples = 20000
	)
	rng := rand.New(rand.NewSource(1234))
	var sum float64
	for i := 0; i < samples; i++ {
		value := samplePoisson(rng, lambda)
		if value < 0 {
			t.Fatalf("poisson sample was negative: %d", value)
		}
		sum += float64(value)
	}
	mean := sum / samples
	if mean < 79 || mean > 81 {
		t.Fatalf("sample mean %.3f is too far from lambda %.1f", mean, lambda)
	}
}

func TestProfilesTypesAndV2JobControls(t *testing.T) {
	s, err := Parse([]byte(`
version: 2
name: mixed-workers
profiles:
  high-fanout:
    type: full
    gossipsub:
      params:
        d: 8
        dLow: 5
        dHigh: 14
phases:
  - action: join
    job: churn-a
    group: fast
    role: worker
    profile: high-fanout
    count: 20
    parallel: true
    parallelism: 4
    await: false
    node:
      gossipsub:
        params:
          historyGossip: 0
          gossipFactor: 0
  - action: wait-jobs
    jobs: [churn-a]
    timeout: 1m
`))
	if err != nil {
		t.Fatal(err)
	}
	join := s.Phases[0]
	if join.ShouldAwait() {
		t.Fatal("explicit await:false was not preserved")
	}
	if !join.Parallel || join.Parallelism != 4 {
		t.Fatalf("parallel controls were not preserved: %+v", join)
	}
	params := join.Node.GossipSub.Params
	if *params.D != 8 || *params.HistoryGossip != 0 || *params.GossipFactor != 0 {
		t.Fatalf("profile/inline merge failed: %+v", params)
	}
	if join.Profile != "high-fanout" || join.NodeType != "full" {
		t.Fatalf("profile and type were not resolved: %+v", join)
	}
	if !s.Phases[1].ShouldAwait() {
		t.Fatal("await must default to true for v3 YAML compatibility")
	}
}

func TestBuiltInWorkerTypes(t *testing.T) {
	for _, kind := range []string{"full", "light", "publisher", "subscriber", "relay", "flood", "random", "dht-only", "gossip-only", "non-gossip"} {
		_, err := Parse([]byte("version: 2\nname: " + kind + "\nphases:\n  - action: join\n    group: nodes\n    role: worker\n    profile: " + kind + "\n    count: 1\n"))
		if err != nil {
			t.Errorf("built-in profile %s failed: %v", kind, err)
		}
	}
}

func TestExplicitTypeDoesNotHideUnknownProfile(t *testing.T) {
	for _, kind := range []string{"full", "worker", "publisher", "boot", "custom"} {
		t.Run(kind, func(t *testing.T) {
			_, err := Parse([]byte("version: 2\nname: typo\nphases:\n  - action: join\n    group: nodes\n    count: 1\n    type: " + kind + "\n    profile: missing-profile\n"))
			if err == nil || !strings.Contains(err.Error(), `unknown profile "missing-profile"`) {
				t.Fatalf("unknown profile with explicit type %q: %v", kind, err)
			}
		})
	}
}

func TestJoinRejectsTimeDistributionWithoutModel(t *testing.T) {
	for _, field := range []string{"interval", "lifetime"} {
		t.Run(field, func(t *testing.T) {
			_, err := Parse([]byte("version: 2\nname: missing-model\nphases:\n  - action: join\n    group: nodes\n    count: 1\n    " + field + ": {value: 5s}\n"))
			if err == nil || !strings.Contains(err.Error(), field+": model is required") {
				t.Fatalf("configured %s silently ignored: %v", field, err)
			}
		})
	}
}

func TestWorkerTypeAliasIsCanonicalAcrossLifecycleActions(t *testing.T) {
	s, err := Parse([]byte(`
version: 2
name: worker-alias
phases:
  - action: join
    group: workers
    role: worker
    type: worker
    count: 1
  - action: wait-ready
    group: workers
    type: worker
  - action: leave
    group: workers
    type: worker
    count: 1
`))
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range s.Phases {
		if phase.NodeType != "full" {
			t.Fatalf("phase %q resolved type %q, want canonical full", phase.Name, phase.NodeType)
		}
	}
}

func TestWorkerTypeAliasInCustomProfileIsCanonical(t *testing.T) {
	s, err := Parse([]byte(`
version: 2
name: worker-profile-alias
profiles:
  custom-worker:
    type: worker
phases:
  - action: join
    group: workers
    role: worker
    profile: custom-worker
    count: 1
  - action: wait-ready
    group: workers
    type: full
`))
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Phases[0].NodeType; got != "full" {
		t.Fatalf("custom profile worker alias resolved type %q, want full", got)
	}
	if got := s.Phases[0].Profile; got != "custom-worker" {
		t.Fatalf("custom profile name changed to %q", got)
	}
}

func TestBackgroundJobReferencesAndShutdownDefaults(t *testing.T) {
	s, err := Parse([]byte(`
version: 2
name: job-barrier
phases:
  - action: join
    job: churn
    await: false
    group: workers
    count: 2
  - action: wait-ready
    group: workers
    jobs: [churn]
`))
	if err != nil {
		t.Fatal(err)
	}
	if s.OnExit != "cancel" || s.JobShutdownTimeout != "3m" {
		t.Fatalf("scenario shutdown defaults were not applied: onExit=%q timeout=%q", s.OnExit, s.JobShutdownTimeout)
	}

	invalid := []string{
		"version: 2\nname: unknown-job\nphases:\n  - action: wait-jobs\n    jobs: [missing]\n",
		"version: 2\nname: invalid-timeout\njobShutdownTimeout: 0s\nphases:\n  - action: log\n",
		"version: 2\nname: early-ready\nphases:\n  - action: join\n    job: churn\n    await: false\n    group: workers\n    count: 2\n  - action: wait-ready\n    group: workers\n",
	}
	for _, input := range invalid {
		if _, err := Parse([]byte(input)); err == nil {
			t.Fatalf("invalid scenario was accepted:\n%s", input)
		}
	}
}

func TestWaitReadyRequiresEveryRelevantBackgroundJoin(t *testing.T) {
	tests := []struct {
		name    string
		barrier string
		wantErr bool
	}{
		{
			name:    "one of two same-group jobs is insufficient",
			barrier: "    group: workers\n    jobs: [first]\n",
			wantErr: true,
		},
		{
			name:    "unrelated job is insufficient",
			barrier: "    group: workers\n    jobs: [other]\n",
			wantErr: true,
		},
		{
			name:    "all same-group jobs are sufficient",
			barrier: "    group: workers\n    jobs: [first, second]\n",
		},
		{
			name:    "global barrier covers joins in every group",
			barrier: "    jobs: [first, second, third]\n",
		},
		{
			name:    "global barrier cannot omit another group",
			barrier: "    jobs: [first, second]\n",
			wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw := "version: 2\nname: join-barrier\nphases:\n" +
				"  - action: join\n    job: first\n    await: false\n    group: workers\n    count: 1\n" +
				"  - action: join\n    job: second\n    await: false\n    group: workers\n    count: 1\n" +
				"  - action: join\n    job: third\n    await: false\n    group: observers\n    count: 1\n" +
				"  - action: publish\n    job: other\n    await: false\n    group: workers\n    count: 1\n" +
				"  - action: wait-ready\n" + test.barrier
			_, err := Parse([]byte(raw))
			if test.wantErr && err == nil {
				t.Fatal("wait-ready accepted without every relevant background join job")
			}
			if !test.wantErr && err != nil {
				t.Fatalf("wait-ready rejected complete job barrier: %v", err)
			}
		})
	}
}

func TestStopAllStartsANewJobGeneration(t *testing.T) {
	_, err := Parse([]byte(`
version: 2
name: reset-jobs
phases:
  - action: join
    job: churn
    await: false
    group: workers
    count: 1
  - action: stop-all
  - action: join
    job: churn
    await: false
    group: workers
    count: 1
  - action: wait-jobs
    jobs: [churn]
`))
	if err != nil {
		t.Fatalf("job id should be reusable after stop-all: %v", err)
	}
}

func TestWaitDurationsAndTimeoutsMustBePositive(t *testing.T) {
	tests := []string{
		"version: 2\nname: zero-wait\nphases:\n  - action: wait\n    duration: 0s\n",
		"version: 2\nname: negative-wait\nphases:\n  - action: wait\n    duration: -1s\n",
		"version: 2\nname: zero-ready-timeout\nphases:\n  - action: wait-ready\n    group: workers\n    timeout: 0s\n",
		"version: 2\nname: negative-ready-timeout\nphases:\n  - action: wait-ready\n    group: workers\n    timeout: -1s\n",
		"version: 2\nname: zero-job-timeout\nphases:\n  - action: wait-jobs\n    timeout: 0s\n",
		"version: 2\nname: negative-job-timeout\nphases:\n  - action: wait-jobs\n    timeout: -1s\n",
	}
	for _, input := range tests {
		if _, err := Parse([]byte(input)); err == nil {
			t.Fatalf("non-positive wait duration or timeout was accepted:\n%s", input)
		}
	}
}

func TestParseRejectsUnknownConfigurationFields(t *testing.T) {
	_, err := Parse([]byte(`
version: 2
name: typo
phases:
  - action: join
    group: workers
    count: 1
    node:
      gossipsub:
        randomNetworkSise: 100
`))
	if err == nil || !strings.Contains(err.Error(), "randomNetworkSise") {
		t.Fatalf("unknown field was not reported: %v", err)
	}
}
