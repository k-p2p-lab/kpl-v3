package master

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/template"
)

const maxAnalysisWorkers = 16
const analysisWorkerPrefix = "analysis-worker-"

//go:embed swarm_analysis.tmpl
var analysisComposeSource string

var analysisCompose = template.Must(template.New("analysis-compose").Parse(analysisComposeSource))

func validateAnalysisNodeIDs(value string) error {
	nodes := strings.Fields(value)
	if len(nodes) > maxAnalysisWorkers {
		return fmt.Errorf("KPL_ANALYSIS_NODE_IDS supports up to %d nodes", maxAnalysisWorkers)
	}
	seen := make(map[string]bool, len(nodes))
	for _, node := range nodes {
		if !nodeIDPattern.MatchString(node) || seen[node] {
			return errors.New("KPL_ANALYSIS_NODE_IDS must contain distinct exact Swarm node IDs separated by spaces")
		}
		seen[node] = true
	}
	return nil
}

func analysisWorkerIndex(component string) (int, bool) {
	number, ok := strings.CutPrefix(component, analysisWorkerPrefix)
	if !ok {
		return 0, false
	}
	index, err := strconv.Atoi(number)
	if err != nil || index < 1 || index > maxAnalysisWorkers || strconv.Itoa(index) != number {
		return 0, false
	}
	return index - 1, true
}

func writeAnalysisCompose(writer io.Writer, value string) error {
	if err := validateAnalysisNodeIDs(value); err != nil {
		return err
	}
	type worker struct{ Name, NodeID string }
	nodes := strings.Fields(value)
	workers := make([]worker, 0, len(nodes))
	origins := make([]string, 0, len(nodes))
	for i, node := range nodes {
		name := analysisWorkerPrefix + strconv.Itoa(i+1)
		workers = append(workers, worker{Name: name, NodeID: node})
		origins = append(origins, "http://"+name+":8093")
	}
	return analysisCompose.Execute(writer, struct {
		Origins string
		Workers []worker
	}{Origins: strings.Join(origins, " "), Workers: workers})
}

func checkAnalysisSwarm(c Config, docker func(...string) (string, error)) error {
	nodes := strings.Fields(c["KPL_ANALYSIS_NODE_IDS"])
	if len(nodes) == 0 {
		return nil
	}
	format := "{{.ID}} {{.Description.Platform.OS}} {{.Status.State}} {{.Spec.Availability}}"
	metadata, err := docker(append([]string{"node", "inspect", "--format", format}, nodes...)...)
	if err != nil {
		return fmt.Errorf("inspect analysis nodes: %w", err)
	}
	expected := make(map[string]bool, len(nodes))
	for _, node := range nodes {
		expected[node+" linux ready active"] = true
	}
	for _, record := range strings.Split(metadata, "\n") {
		if !expected[record] {
			return errors.New("KPL_ANALYSIS_NODE_IDS must identify exact Linux, Ready, Active Swarm nodes")
		}
		delete(expected, record)
	}
	if len(expected) != 0 {
		return errors.New("Docker returned an incomplete analysis node inventory")
	}
	secret := c["KPL_ANALYSIS_SECRET"]
	name, err := docker("secret", "inspect", "--format", "{{.Spec.Name}}", secret)
	if err != nil {
		return fmt.Errorf("inspect analysis Docker secret: %w", err)
	}
	if name != secret {
		return errors.New("KPL_ANALYSIS_SECRET must identify an existing Docker secret by name")
	}
	return nil
}

func (s *swarm) checkAnalysisSwarm(ctx context.Context) error {
	return checkAnalysisSwarm(s.config, func(args ...string) (string, error) { return s.output(ctx, args...) })
}

type deployedAnalysisWorker struct {
	name, id string
	index    int
}

func (s *swarm) analysisWorkers() []deployedAnalysisWorker {
	var workers []deployedAnalysisWorker
	for i, name := range s.services {
		index, ok := analysisWorkerIndex(strings.TrimPrefix(name, s.stack+"_"))
		if ok {
			workers = append(workers, deployedAnalysisWorker{name: name, id: s.serviceIDs[i], index: index})
		}
	}
	return workers
}

// A move or removal requires the verified service shutdown performed by restart.
func (s *swarm) checkAnalysisPlacementUnchanged(ctx context.Context) error {
	nodes := strings.Fields(s.config["KPL_ANALYSIS_NODE_IDS"])
	for _, worker := range s.analysisWorkers() {
		if worker.index >= len(nodes) {
			return errors.New("use kpl swarm restart to remove analysis workers")
		}
		constraints, err := s.output(ctx, "service", "inspect", "--format", "{{range .Spec.TaskTemplate.Placement.Constraints}}{{println .}}{{end}}", worker.id)
		if err != nil {
			return fmt.Errorf("inspect analysis worker placement: %w", err)
		}
		var pinnedNode string
		for _, constraint := range strings.Split(constraints, "\n") {
			left, right, ok := strings.Cut(constraint, "==")
			if ok && strings.TrimSpace(left) == "node.id" {
				if pinnedNode != "" {
					return errors.New("analysis worker has conflicting node constraints")
				}
				pinnedNode = strings.TrimSpace(right)
			}
		}
		if pinnedNode != nodes[worker.index] {
			return errors.New("use kpl swarm restart to move analysis workers")
		}
	}
	return nil
}
