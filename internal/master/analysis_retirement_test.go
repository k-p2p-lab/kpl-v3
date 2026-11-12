package master

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"
)

type retirementMigrationModel struct {
	t              *testing.T
	base           *republishLifecycleModel
	journal        string
	workerID       string
	workerPresent  bool
	workerVerified bool
	failDeploy     bool
	failStop       bool
}

func (m *retirementMigrationModel) handle(ctx context.Context, c Command) (string, error) {
	args := c.Args
	last := args[len(args)-1]
	if args[0] == "service" {
		switch args[1] {
		case "ls":
			if !m.base.base.removed && m.workerPresent {
				return "controller agent monitor " + m.workerID, nil
			}
		case "inspect":
			if last == m.workerID {
				return "kpl_analysis-worker-1|" + swarmApplication, nil
			}
		case "ps":
			if last == m.workerID || last == "kpl_analysis-worker-1" {
				return "retiredtask", nil
			}
		case "rm":
			if slices.Contains(args, m.workerID) {
				if !m.workerVerified {
					m.t.Fatal("legacy service removal preceded task cleanup verification")
				}
				data, err := os.ReadFile(m.journal)
				if err != nil {
					m.t.Fatalf("destructive removal preceded durable retirement evidence: %v", err)
				}
				var journal analysisRetirementJournal
				if err := json.Unmarshal(data, &journal); err != nil || len(journal.Services) != 1 || journal.Services[0].ServiceID != m.workerID {
					m.t.Fatalf("removal journal lost exact immutable service identity: %v", err)
				}
				if len(args) == 3 {
					m.workerPresent = false
					return "", nil
				}
			}
		}
	}
	if args[0] == "inspect" && last == "retiredtask" {
		if m.base.base.paused[m.workerID] || m.base.base.paused["kpl_analysis-worker-1"] {
			if m.failStop {
				return "analysisnode|failed|retiredcontainer|0|1|error", nil
			}
			if !m.base.base.paused["kpl_controller"] {
				m.t.Fatal("analysis tasks stopped before Controller admission stopped")
			}
			m.workerVerified = true
			return "analysisnode|shutdown|retiredcontainer|0|0|ok", nil
		}
		return "analysisnode|running|retiredcontainer|123|0|ok", nil
	}
	if args[0] == "stack" && args[1] == "deploy" && m.failDeploy {
		return "", errors.New("fixture stack deploy failed")
	}
	return m.base.handle(ctx, c)
}

func newRetirementMigrationFixture(t *testing.T) (*swarm, *swarmTestRunner, *retirementMigrationModel) {
	t.Helper()
	s, runner, _ := newSwarmFixture(t, "deploy")
	model := &retirementMigrationModel{t: t, base: newRepublishLifecycleModel(), journal: s.analysisRetirementPath(), workerID: "oldanalysisservice", workerPresent: true}
	runner.handle = model.handle
	return s, runner, model
}

func TestAnalysisRetirementJournalSurvivesNewProcessAfterPublishOrDeployFailure(t *testing.T) {
	for _, phase := range []string{"publish", "deploy"} {
		t.Run(phase, func(t *testing.T) {
			s, runner, model := newRetirementMigrationFixture(t)
			command, args := "deploy", []string(nil)
			if phase == "publish" {
				command, args = "restart", []string{"--republish"}
				model.base.failure = "push"
			} else {
				model.failDeploy = true
			}
			if err := s.a.RunSwarm(t.Context(), command, args); err == nil {
				t.Fatal("fixture failure was not returned")
			}
			info, err := os.Stat(s.analysisRetirementPath())
			if err != nil || info.Mode().Perm() != 0600 {
				t.Fatalf("retirement journal was not privately persisted before failure: %v", err)
			}
			if swarmCallIndex(runner.calls, "service", "rm") < 0 {
				t.Fatal("failure fixture never crossed service removal")
			}
			// Simulate a fresh CLI process after services have disappeared. No
			// Controller environment or in-memory endpoint list survives.
			next := *s.a
			nextRunner := &swarmTestRunner{}
			nextModel := newRepublishLifecycleModel()
			nextModel.base.removed = true
			nextRunner.handle = nextModel.handle
			next.Runner = nextRunner
			next.Stdout, next.Stderr = &bytes.Buffer{}, &bytes.Buffer{}
			if err := next.RunSwarm(t.Context(), "deploy", []string{"workerone"}); err != nil {
				t.Fatal(err)
			}
			deployed := swarmCallIndex(nextRunner.calls, "stack", "deploy")
			if deployed < 0 || swarmCommandEnv(nextRunner.calls[deployed], retiredAnalysisEnvironment) != "http://analysis-worker-1:8093" {
				t.Fatal("a new CLI process lost durable verified retirement evidence")
			}
		})
	}
}

func TestAnalysisRetirementJournalNeverBypassesExistingServiceCleanup(t *testing.T) {
	s, runner, model := newRetirementMigrationFixture(t)
	previous := analysisRetirementJournal{Version: 1, Stack: s.stack, ControlNodeID: "control", ClusterID: "fixturecluster", Services: []analysisRetirementRecord{{Endpoint: "http://analysis-worker-1:8093", ServiceID: "previousservicegeneration", TaskIDs: []string{"previoustask"}, VerifiedAt: time.Unix(1, 0).UTC()}}}
	writeRetirementJournalFixture(t, s.analysisRetirementPath(), previous)
	if err := s.a.RunSwarm(t.Context(), "deploy", nil); err != nil {
		t.Fatal(err)
	}
	if !model.workerVerified || swarmCallIndex(runner.calls, "service", "rm", model.workerID) < 0 {
		t.Fatal("journal existence bypassed cleanup of a current service generation")
	}
	data, err := os.ReadFile(s.analysisRetirementPath())
	if err != nil {
		t.Fatal(err)
	}
	var current analysisRetirementJournal
	if err := json.Unmarshal(data, &current); err != nil {
		t.Fatal(err)
	}
	if len(current.Services) != 1 || current.Services[0].ServiceID != model.workerID {
		t.Fatal("journal was not updated with the currently verified immutable service")
	}
}

func writeRetirementJournalFixture(t *testing.T, path string, journal analysisRetirementJournal) {
	t.Helper()
	data, err := json.Marshal(journal)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestAnalysisRetirementJournalRejectsScopeCorruptionAndUnsafeFiles(t *testing.T) {
	for _, mode := range []string{"stack", "control", "cluster", "malformed", "trailing", "dual-provenance", "missing-provenance", "symlink", "writable"} {
		t.Run(mode, func(t *testing.T) {
			s, runner, _ := newSwarmFixture(t, "deploy")
			model := newRepublishLifecycleModel()
			runner.handle = model.handle
			journal := analysisRetirementJournal{Version: 1, Stack: s.stack, ControlNodeID: "control", ClusterID: "fixturecluster", Services: []analysisRetirementRecord{{Endpoint: "http://analysis-worker-1:8093", ServiceID: "oldservice", VerifiedAt: time.Unix(1, 0).UTC()}}}
			switch mode {
			case "stack":
				journal.Stack = "another"
			case "control":
				journal.ControlNodeID = "othernode"
			case "cluster":
				journal.ClusterID = "anothercluster"
			case "dual-provenance":
				journal.Services[0].SourceControllerID = "controller"
				journal.Services[0].ObservedAt = time.Unix(1, 0).UTC()
			case "missing-provenance":
				journal.Services[0].ServiceID = ""
			}
			path := s.analysisRetirementPath()
			writeRetirementJournalFixture(t, path, journal)
			switch mode {
			case "malformed":
				if err := os.WriteFile(path, []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			case "trailing":
				file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				_, err = file.WriteString("{}")
				file.Close()
				if err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Rename(path, path+".source"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(path+".source", path); err != nil {
					t.Fatal(err)
				}
			case "writable":
				if err := os.Chmod(path, 0660); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.a.RunSwarm(t.Context(), "deploy", nil); err == nil {
				t.Fatal("untrusted retirement journal was accepted")
			}
			for _, prefix := range [][]string{{"service", "rm"}, {"service", "update"}, {"stack", "deploy"}, {"node", "update"}, {"pull"}} {
				if swarmCallIndex(runner.calls, prefix...) >= 0 {
					t.Fatalf("invalid journal changed deployment before rejection: %v", prefix)
				}
			}
		})
	}
}

func TestAnalysisRetirementJournalDoesNotMintProofForUnverifiedTasks(t *testing.T) {
	s, runner, model := newRetirementMigrationFixture(t)
	model.failStop = true
	if err := s.a.RunSwarm(t.Context(), "deploy", nil); err == nil {
		t.Fatal("unclean legacy task was accepted")
	}
	if _, err := os.Stat(s.analysisRetirementPath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unclean shutdown minted a durable proof: %v", err)
	}
	if swarmCallIndex(runner.calls, "service", "rm") >= 0 || swarmCallIndex(runner.calls, "stack", "deploy") >= 0 {
		t.Fatal("unclean shutdown crossed destructive removal/deployment")
	}
}

func TestAnalysisRetirementPersistsImportedControllerProvenance(t *testing.T) {
	s, runner, _ := newSwarmFixture(t, "deploy")
	model := newRepublishLifecycleModel()
	runner.handle = func(ctx context.Context, c Command) (string, error) {
		if len(c.Args) > 3 && c.Args[0] == "service" && c.Args[1] == "inspect" && c.Args[3] == retiredAnalysisEnvironmentFormat {
			return retiredAnalysisEnvironment + "=http://analysis-worker-2:8093", nil
		}
		return model.handle(ctx, c)
	}
	if err := s.a.RunSwarm(t.Context(), "deploy", nil); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(s.analysisRetirementPath())
	if err != nil {
		t.Fatal(err)
	}
	var journal analysisRetirementJournal
	if err := json.Unmarshal(data, &journal); err != nil {
		t.Fatal(err)
	}
	if len(journal.Services) != 1 || journal.Services[0].ServiceID != "" || !journal.Services[0].VerifiedAt.IsZero() || journal.Services[0].SourceControllerID != "controller" || journal.Services[0].ObservedAt.IsZero() {
		t.Fatal("imported Controller provenance was lost or misrepresented as verified task evidence")
	}
	next := *s.a
	nextRunner := &swarmTestRunner{}
	nextModel := newRepublishLifecycleModel()
	nextModel.base.removed = true
	nextRunner.handle = nextModel.handle
	next.Runner = nextRunner
	if err := next.RunSwarm(t.Context(), "deploy", []string{"workerone"}); err != nil {
		t.Fatal(err)
	}
	deployed := swarmCallIndex(nextRunner.calls, "stack", "deploy")
	if deployed < 0 || swarmCommandEnv(nextRunner.calls[deployed], retiredAnalysisEnvironment) != "http://analysis-worker-2:8093" {
		t.Fatal("imported Controller proof was not durable after its service disappeared")
	}
}

func TestAnalysisRetirementJournalCrashReleasedLockAndConcurrentWriters(t *testing.T) {
	first, _, _ := newSwarmFixture(t, "deploy")
	second, _, _ := newSwarmFixture(t, "deploy")
	second.a.EnvFile = first.a.EnvFile
	for _, s := range []*swarm{first, second} {
		s.a.Runner = &swarmTestRunner{handle: func(_ context.Context, c Command) (string, error) {
			if slices.Equal(c.Args, []string{"info", "--format", "{{.Swarm.Cluster.ID}}"}) {
				return "fixturecluster", nil
			}
			return "", fmt.Errorf("unexpected metadata request: %v", c.Args)
		}}
	}
	// A prior process may leave this inode after a crash; without a kernel
	// lock it must never prevent the next writer from making progress.
	if err := os.WriteFile(first.analysisRetirementPath()+".lock", nil, 0600); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	done := make(chan error, 2)
	for i, s := range []*swarm{first, second} {
		go func(index int, s *swarm) {
			<-start
			done <- s.persistAnalysisRetirements(t.Context(), []analysisRetirementRecord{{Endpoint: fmt.Sprintf("http://analysis-worker-%d:8093", index+1), ServiceID: fmt.Sprintf("service%d", index+1), VerifiedAt: time.Now().UTC()}})
		}(i, s)
	}
	close(start)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	journal, _, _, err := first.readAnalysisRetirementJournal(t.Context())
	if err != nil || len(journal.Services) != 2 {
		t.Fatalf("concurrent durable proofs lost their union: records=%d error=%v", len(journal.Services), err)
	}
	info, err := os.Stat(first.analysisRetirementPath() + ".lock")
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("journal lock is not private: %v", err)
	}
}

func TestAnalysisRetirementJournalLockWaitHonorsCancellation(t *testing.T) {
	s, _, _ := newSwarmFixture(t, "deploy")
	lock, err := lockAnalysisRetirementJournal(t.Context(), s.analysisRetirementPath()+".lock")
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	err = s.persistAnalysisRetirements(ctx, []analysisRetirementRecord{{Endpoint: "http://analysis-worker-1:8093", ServiceID: "oldservice", VerifiedAt: time.Now().UTC()}})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("held kernel lock did not honor cancellation: %v", err)
	}
	if _, err := os.Stat(s.analysisRetirementPath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("waiting writer published without owning the journal lock")
	}
}
