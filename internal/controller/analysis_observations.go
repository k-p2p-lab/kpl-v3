package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

type analysisLayer struct {
	Metrics       map[string]*float64 `json:"metrics,omitempty"`
	Protocol      string              `json:"protocol"`
	Nodes         int                 `json:"nodes"`
	AverageDegree *float64            `json:"averageDegree"`
	Clustering    *float64            `json:"clustering"`
	Degrees       []analysisPoint     `json:"degrees"`
}

type analysisGroup struct {
	Group              string          `json:"group"`
	Ready              int             `json:"ready"`
	Starting           int             `json:"starting"`
	Stopping           int             `json:"stopping"`
	Failed             int             `json:"failed"`
	Reporting          int             `json:"reporting"`
	ScoreObservers     int             `json:"scoreObservers"`
	ScoreCount         int             `json:"scoreCount"`
	ScoreMean          *float64        `json:"scoreMean"`
	ScoreMin           *float64        `json:"scoreMin"`
	ScoreMax           *float64        `json:"scoreMax"`
	NegativeScoreRatio *float64        `json:"negativeScoreRatio"`
	Layers             []analysisLayer `json:"layers"`
}

type analysisGraph struct {
	Protocol string   `json:"protocol"`
	Nodes    []string `json:"nodes"`
	Groups   []string `json:"groups"`
	Edges    [][2]int `json:"edges"`
}

type analysisObservation struct {
	Graphs []analysisGraph `json:"graphs,omitempty"`
	RunID  string          `json:"runId"`
	At     time.Time       `json:"at"`
	Groups []analysisGroup `json:"groups"`
}

// Sampling is owned by the Controller, independent of browser/SSE clients.
func (s *Server) recordAnalysisLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.state.mu.RLock()
			runs := make([]string, 0)
			for id, run := range s.state.experiments {
				if run.State == "running" {
					runs = append(runs, id)
				}
			}
			s.state.mu.RUnlock()
			for _, id := range runs {
				if ctx.Err() != nil {
					return
				}
				if err := s.state.recordAnalysisObservation(id, now.UTC()); err != nil {
					s.logger.Warn("record analysis observation", "run", id, "error", err)
				}
			}
		}
	}
}

func (s *state) recordAnalysisObservation(runID string, now time.Time) error {
	s.mu.RLock()
	nodes := make([]model.Node, 0)
	agents := make(map[string]model.Agent, len(s.agents))
	for id, agent := range s.agents {
		agents[id] = agent
	}
	for _, node := range s.nodes {
		if node.RunID == runID && node.State != model.NodeStopped {
			node.LastSeen = s.nodeReportTimes[node.ID]
			nodes = append(nodes, node)
		}
	}
	s.mu.RUnlock()
	observation := makeAnalysisObservation(runID, now, nodes, agents)
	data, err := json.Marshal(observation)
	if err != nil {
		return err
	}
	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	deleted, err := s.resultDeletedLocked(runID)
	if err != nil || deleted {
		return err
	}
	// Do not recreate a deleted/missing result or keep completed runs growing.
	// The final sample is explicitly recorded before the terminal metadata.
	s.mu.RLock()
	run, exists := s.experiments[runID]
	s.mu.RUnlock()
	if !exists || run.State != "running" {
		return nil
	}
	path := filepath.Join(s.dataDir, "runs", safeName(runID), "observations.jsonl")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open observations: %w", err)
	}
	_, writeErr := file.Write(append(data, '\n'))
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

func makeAnalysisObservation(runID string, now time.Time, nodes []model.Node, agents map[string]model.Agent) analysisObservation {
	result := analysisObservation{RunID: runID, At: now, Groups: []analysisGroup{}}
	groups := map[string][]model.Node{"": {}}
	fresh := make(map[string]bool)
	for _, node := range nodes {
		groups[""] = append(groups[""], node)
		if node.Group != "" {
			groups[node.Group] = append(groups[node.Group], node)
		}
		fresh[node.ID] = node.State == model.NodeReady && node.PeerID != "" && agentIsOnline(agents[node.AgentID], now) &&
			!node.LastSeen.IsZero() && now.Sub(node.LastSeen) <= agentStaleAfter
	}
	edges := networkEdgesAt(nodes, agents, now)
	graphs := make(map[string]map[string]map[string]struct{})
	coefficients := make(map[string]map[string]float64)
	for _, protocol := range []string{"transport", "kademlia", "gossipsub"} {
		adj := make(map[string]map[string]struct{})
		for _, node := range nodes {
			if !fresh[node.ID] {
				continue
			}
			if protocol != "transport" && node.OverlayObservedAt.IsZero() {
				continue
			}
			if protocol == "gossipsub" && node.Metadata["pubsubEnabled"] == "false" {
				continue
			}
			if protocol == "kademlia" && node.Metadata["dhtEnabled"] == "false" {
				continue
			}
			adj[node.ID] = make(map[string]struct{})
		}
		for _, edge := range edges {
			if edge.Protocol != protocol || adj[edge.Source] == nil || adj[edge.Target] == nil {
				continue
			}
			adj[edge.Source][edge.Target] = struct{}{}
			adj[edge.Target][edge.Source] = struct{}{}
		}
		graph := analysisGraph{Protocol: protocol, Nodes: []string{}, Groups: []string{}, Edges: [][2]int{}}
		nodeGroups := make(map[string]string)
		for _, node := range nodes {
			nodeGroups[node.ID] = node.Group
		}
		for id := range adj {
			graph.Nodes = append(graph.Nodes, id)
		}
		sort.Strings(graph.Nodes)
		index := make(map[string]int)
		for i, id := range graph.Nodes {
			index[id] = i
			graph.Groups = append(graph.Groups, nodeGroups[id])
		}
		for i, id := range graph.Nodes {
			for other := range adj[id] {
				if i < index[other] {
					graph.Edges = append(graph.Edges, [2]int{i, index[other]})
				}
			}
		}
		sort.Slice(graph.Edges, func(i, j int) bool {
			if graph.Edges[i][0] != graph.Edges[j][0] {
				return graph.Edges[i][0] < graph.Edges[j][0]
			}
			return graph.Edges[i][1] < graph.Edges[j][1]
		})
		result.Graphs = append(result.Graphs, graph)
		graphs[protocol] = adj
		coefficients[protocol] = analysisClustering(adj)
	}
	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		group := analysisGroup{Group: name, Layers: []analysisLayer{}}
		mean, negative, low, high := 0.0, 0, math.Inf(1), math.Inf(-1)
		for _, node := range groups[name] {
			switch node.State {
			case model.NodeReady:
				group.Ready++
			case model.NodeStarting:
				group.Starting++
			case model.NodeStopping:
				group.Stopping++
			case model.NodeFailed:
				group.Failed++
			}
			if !fresh[node.ID] {
				continue
			}
			group.Reporting++
			observed := false
			for _, score := range node.PeerScores {
				if math.IsNaN(score) || math.IsInf(score, 0) {
					continue
				}
				observed = true
				group.ScoreCount++
				n := float64(group.ScoreCount)
				if math.Signbit(mean) == math.Signbit(score) {
					mean += (score - mean) / n
				} else {
					// Opposite signs can overflow score-mean even though
					// both inputs and their average are finite.
					mean = mean*((n-1)/n) + score/n
				}
				low = math.Min(low, score)
				high = math.Max(high, score)
				if score < 0 {
					negative++
				}
			}
			if observed {
				group.ScoreObservers++
			}
		}
		if group.ScoreCount > 0 {
			ratio := float64(negative) / float64(group.ScoreCount)
			group.ScoreMean, group.ScoreMin, group.ScoreMax, group.NegativeScoreRatio = &mean, &low, &high, &ratio
		}
		for _, protocol := range []string{"transport", "kademlia", "gossipsub"} {
			layer := analysisLayer{Protocol: protocol, Degrees: []analysisPoint{}}
			degreeSum, clusteringSum, clusteringCount := 0, 0.0, 0
			histogram := make(map[int]int)
			for _, node := range groups[name] {
				adj, ok := graphs[protocol][node.ID]
				if !ok {
					continue
				}
				layer.Nodes++
				degreeSum += len(adj)
				histogram[len(adj)]++
				if coefficient, ok := coefficients[protocol][node.ID]; ok {
					clusteringSum += coefficient
					clusteringCount++
				}
			}
			if layer.Nodes > 0 {
				average := float64(degreeSum) / float64(layer.Nodes)
				layer.AverageDegree = &average
				if clusteringCount == layer.Nodes {
					clustering := clusteringSum / float64(layer.Nodes)
					layer.Clustering = &clustering
				}
			}
			for degree, count := range histogram {
				layer.Degrees = append(layer.Degrees, analysisPoint{float64(degree), float64(count)})
			}
			sort.Slice(layer.Degrees, func(i, j int) bool { return layer.Degrees[i].X < layer.Degrees[j].X })
			group.Layers = append(group.Layers, layer)
		}
		result.Groups = append(result.Groups, group)
	}
	return result
}

func analysisClustering(adj map[string]map[string]struct{}) map[string]float64 {
	result := make(map[string]float64)
	// Bound triangle work so observation cannot monopolize the Controller on
	// dense graphs. If the exact calculation exceeds the budget, report N/A.
	pairs := 0
	for _, neighbors := range adj {
		pairs += len(neighbors) * (len(neighbors) - 1) / 2
	}
	if pairs > 250000 {
		return result
	}
	for id, neighbors := range adj {
		if len(neighbors) < 2 {
			result[id] = 0
			continue
		}
		links := 0
		for a := range neighbors {
			for b := range neighbors {
				if a < b {
					if _, linked := adj[a][b]; linked {
						links++
					}
				}
			}
		}
		result[id] = float64(links) / float64(len(neighbors)*(len(neighbors)-1)/2)
	}
	return result
}
