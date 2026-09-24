package controller

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"sync"

	"golang.org/x/exp/rand"
	"gonum.org/v1/gonum/graph/community"
	"gonum.org/v1/gonum/graph/simple"
)

// Graph inputs are undirected unique-neighbor observations, including isolates.
// Distances exclude self pairs and unreachable pairs; connected-pair coverage
// accompanies every distance metric. Missing/undefined statistics remain null.
type graphStatistics struct {
	Values  map[string]*float64
	PerNode map[string][]float64
}

func numberPointer(x float64) *float64 {
	if math.IsNaN(x) || math.IsInf(x, 0) {
		return nil
	}
	return &x
}

func calculateGraph(ctx context.Context, raw analysisGraph) (graphStatistics, error) {
	n := len(raw.Nodes)
	out := graphStatistics{Values: map[string]*float64{}, PerNode: map[string][]float64{}}
	for _, key := range []string{"node_count", "average_degree", "average_degree_excluding_leaves", "diameter", "shortest_path_length", "clustering_coefficient", "betweenness_centrality", "page_rank", "degree_centrality", "closeness_centrality", "eigenvector_centrality", "assortativity", "modularity", "connected_pair_fraction"} {
		out.Values[key] = nil
	}
	out.Values["node_count"] = numberPointer(float64(n))
	if n == 0 {
		return out, nil
	}
	adj := make([][]int, n)
	edgeSet := make(map[[2]int]bool)
	for _, edge := range raw.Edges {
		a, b := edge[0], edge[1]
		if a < 0 || b < 0 || a >= n || b >= n || a == b {
			return out, fmt.Errorf("invalid %s graph edge", raw.Protocol)
		}
		if a > b {
			a, b = b, a
		}
		key := [2]int{a, b}
		if edgeSet[key] {
			continue
		}
		edgeSet[key] = true
		adj[a] = append(adj[a], b)
		adj[b] = append(adj[b], a)
	}
	degrees, clustering, between, close, rank, eigen := make([]float64, n), make([]float64, n), make([]float64, n), make([]float64, n), make([]float64, n), make([]float64, n)
	degreeSum, coreNodes := 0., 0
	for i := range adj {
		sort.Ints(adj[i])
		degreeSum += float64(len(adj[i]))
		if len(adj[i]) > 1 {
			coreNodes++
		}
		if n > 1 {
			degrees[i] = float64(len(adj[i])) / float64(n-1)
		}
		links := 0
		for j, a := range adj[i] {
			for _, b := range adj[i][j+1:] {
				if edgeSet[[2]int{a, b}] {
					links++
				}
			}
		}
		if len(adj[i]) > 1 {
			clustering[i] = float64(2*links) / float64(len(adj[i])*(len(adj[i])-1))
		}
		rank[i] = 1 / float64(n)
		eigen[i] = 1 / math.Sqrt(float64(n))
	}
	out.Values["average_degree"] = numberPointer(degreeSum / float64(n))
	if coreNodes > 0 {
		out.Values["average_degree_excluding_leaves"] = numberPointer(degreeSum / float64(coreNodes))
	}
	// Brandes with one BFS per source computes distance, closeness and betweenness
	// together, using O(V+E) scratch memory rather than storing all paths.
	distanceSum, reachable, diameter := 0., 0., 0
	for source := 0; source < n; source++ {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		dist, sigma, delta := make([]int, n), make([]float64, n), make([]float64, n)
		parents := make([][]int, n)
		for i := range dist {
			dist[i] = -1
		}
		dist[source] = 0
		sigma[source] = 1
		queue := make([]int, 1, n)
		queue[0] = source
		for head := 0; head < len(queue); head++ {
			v := queue[head]
			for _, w := range adj[v] {
				if dist[w] < 0 {
					dist[w] = dist[v] + 1
					queue = append(queue, w)
				}
				if dist[w] == dist[v]+1 {
					sigma[w] += sigma[v]
					parents[w] = append(parents[w], v)
				}
			}
		}
		sum := 0.
		for _, v := range queue {
			if v != source {
				sum += float64(dist[v])
				if dist[v] > diameter {
					diameter = dist[v]
				}
			}
		}
		reached := float64(len(queue) - 1)
		distanceSum += sum
		reachable += reached
		if sum > 0 && n > 1 {
			close[source] = reached * reached / (float64(n-1) * sum)
		}
		for i := len(queue) - 1; i >= 0; i-- {
			w := queue[i]
			for _, v := range parents[w] {
				delta[v] += (sigma[v] / sigma[w]) * (1 + delta[w])
			}
			if w != source {
				between[w] += delta[w]
			}
		}
	}
	if reachable > 0 {
		out.Values["diameter"] = numberPointer(float64(diameter))
		out.Values["shortest_path_length"] = numberPointer(distanceSum / reachable)
	}
	if n > 1 {
		out.Values["connected_pair_fraction"] = numberPointer(reachable / (float64(n) * float64(n-1)))
	}
	for i := range between {
		if n > 2 {
			between[i] /= float64(n-1) * float64(n-2)
		}
	}
	rankOK, eigenOK := false, false
	for iteration := 0; iteration < 2000; iteration++ {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		r, e := make([]float64, n), make([]float64, n)
		dangling := 0.
		for i := range adj {
			if len(adj[i]) == 0 {
				dangling += rank[i]
			}
		}
		for i := range adj {
			r[i] = (1-.85)/float64(n) + .85*dangling/float64(n)
			e[i] = eigen[i] // A+I avoids bipartite sign oscillation.
			for _, j := range adj[i] {
				r[i] += .85 * rank[j] / float64(len(adj[j]))
				e[i] += eigen[j]
			}
		}
		norm := 0.
		for _, v := range e {
			norm = math.Hypot(norm, v)
		}
		rd, ed := 0., 0.
		for i := range e {
			e[i] /= norm
			rd += math.Abs(r[i] - rank[i])
			ed += math.Abs(e[i] - eigen[i])
		}
		rank, eigen = r, e
		rankOK = rd < 1e-10
		eigenOK = ed < 1e-9*float64(n)
		if rankOK && eigenOK {
			break
		}
	}
	out.PerNode["degree_centrality"], out.PerNode["clustering_coefficient"], out.PerNode["betweenness_centrality"], out.PerNode["closeness_centrality"] = degrees, clustering, between, close
	if rankOK {
		out.PerNode["page_rank"] = rank
	}
	if eigenOK && len(edgeSet) > 0 {
		out.PerNode["eigenvector_centrality"] = eigen
	}
	for name, values := range out.PerNode {
		mean := 0.
		for i, v := range values {
			mean += (v - mean) / float64(i+1)
		}
		out.Values[name] = numberPointer(mean)
	}
	if len(edgeSet) > 0 {
		product, mean, square := 0., 0., 0.
		graph := simple.NewUndirectedGraph()
		for i := 0; i < n; i++ {
			graph.AddNode(simple.Node(i))
		}
		for edge := range edgeSet {
			a, b := float64(len(adj[edge[0]])), float64(len(adj[edge[1]]))
			product += a * b
			mean += (a + b) / 2
			square += (a*a + b*b) / 2
			graph.SetEdge(graph.NewEdge(simple.Node(edge[0]), simple.Node(edge[1])))
		}
		count := float64(len(edgeSet))
		mean /= count
		variance := square/count - mean*mean
		if variance > 1e-14 {
			out.Values["assortativity"] = numberPointer((product/count - mean*mean) / variance)
		}
		if err := ctx.Err(); err != nil {
			return out, err
		}
		partition := community.Modularize(graph, 1, rand.NewSource(1)).Communities()
		out.Values["modularity"] = numberPointer(community.Q(graph, partition, 1))
	}
	return out, ctx.Err()
}

// Cache both completed and in-flight calculations. Repeated snapshots share one
// immutable result, and the existing 256-entry limit bounds retained statistics.
type analysisGraphCalculation struct {
	done  chan struct{}
	stats graphStatistics
	err   error
}

type analysisGraphCache struct {
	mu      sync.Mutex
	entries map[[32]byte]*analysisGraphCalculation
}

func (cache *analysisGraphCache) calculate(ctx context.Context, raw analysisGraph) (graphStatistics, error) {
	if err := ctx.Err(); err != nil {
		return graphStatistics{}, err
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return graphStatistics{}, err
	}
	key := sha256.Sum256(data)
	cache.mu.Lock()
	if cached := cache.entries[key]; cached != nil {
		cache.mu.Unlock()
		select {
		case <-cached.done:
			return cached.stats, cached.err
		case <-ctx.Done():
			return graphStatistics{}, ctx.Err()
		}
	}
	if len(cache.entries) >= 256 {
		cache.mu.Unlock()
		return calculateGraph(ctx, raw)
	}
	if cache.entries == nil {
		cache.entries = make(map[[32]byte]*analysisGraphCalculation)
	}
	calculation := &analysisGraphCalculation{done: make(chan struct{})}
	cache.entries[key] = calculation
	cache.mu.Unlock()
	calculation.stats, calculation.err = calculateGraph(ctx, raw)
	close(calculation.done)
	return calculation.stats, calculation.err
}

// Expensive graph calculations run in bounded workers. Each observation owns
// its output, so completion order never changes the saved timeline or groups.
func enrichAnalysisGraphs(ctx context.Context, observations []analysisObservation) error {
	return enrichAnalysisGraphsWithWorkers(ctx, observations, analysisParallelism())
}

func enrichAnalysisGraphsWithWorkers(ctx context.Context, observations []analysisObservation, workers int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var cache analysisGraphCache
	var progressMu sync.Mutex
	completed := 0
	reportAnalysisProgress(ctx, fmt.Sprintf("graph metrics 0/%d", len(observations)), 0)
	return parallelAnalysis(ctx, len(observations), workers, func(ctx context.Context, index int) error {
		if err := enrichAnalysisObservation(ctx, &observations[index], &cache); err != nil {
			return err
		}
		// Existing progress callbacks need serialized calls and monotonically
		// increasing completed counts even when workers finish out of order.
		progressMu.Lock()
		defer progressMu.Unlock()
		if err := ctx.Err(); err != nil {
			return err
		}
		completed++
		reportAnalysisProgress(ctx, fmt.Sprintf("graph metrics %d/%d", completed, len(observations)), 0)
		return nil
	})
}

func enrichAnalysisObservation(ctx context.Context, observation *analysisObservation, cache *analysisGraphCache) error {
	for _, raw := range observation.Graphs {
		if err := ctx.Err(); err != nil {
			return err
		}
		calculated, err := cache.calculate(ctx, raw)
		if err != nil {
			return err
		}
		for gi := range observation.Groups {
			group := &observation.Groups[gi]
			for li := range group.Layers {
				layer := &group.Layers[li]
				if layer.Protocol != raw.Protocol {
					continue
				}
				layer.Metrics = make(map[string]*float64)
				if group.Group == "" {
					for key, value := range calculated.Values {
						layer.Metrics[key] = value
					}
					layer.Clustering = calculated.Values["clustering_coefficient"]
					continue
				}
				for key, values := range calculated.PerNode {
					count, mean := 0, 0.
					for i, v := range values {
						if i < len(raw.Groups) && raw.Groups[i] == group.Group {
							count++
							mean += (v - mean) / float64(count)
						}
					}
					if count > 0 {
						layer.Metrics[key] = numberPointer(mean)
						if key == "clustering_coefficient" {
							layer.Clustering = layer.Metrics[key]
						}
					}
				}
			}
		}
	}
	for gi := range observation.Groups {
		for li := range observation.Groups[gi].Layers {
			layer := &observation.Groups[gi].Layers[li]
			if layer.Metrics == nil {
				layer.Metrics = map[string]*float64{}
			}
			if _, ok := layer.Metrics["average_degree_excluding_leaves"]; !ok {
				total, core := 0., 0.
				for _, p := range layer.Degrees {
					total += p.X * p.Y
					if p.X > 1 {
						core += p.Y
					}
				}
				layer.Metrics["average_degree_excluding_leaves"] = nil
				if core > 0 {
					layer.Metrics["average_degree_excluding_leaves"] = numberPointer(total / core)
				}
			}
		}
	}
	observation.Graphs = nil
	return ctx.Err()
}
