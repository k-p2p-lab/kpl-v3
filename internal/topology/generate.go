// Package topology generates seeded simple undirected graphs for ordered Peer
// cohorts. It does not open network connections or mutate a live experiment.
package topology

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const (
	MaxNodes = model.MaxTopologyNodes
	MaxEdges = model.MaxTopologyEdges
)

type Position struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

// Graph indices match the caller's cohort order. Every neighbor list is sorted;
// each undirected edge occurs in both lists. Positions is nil for nongeometric
// models, and all returned memory belongs to the caller.
type Graph struct {
	Neighbors [][]int    `json:"neighbors"`
	Positions []Position `json:"positions,omitempty"`
}

func (g Graph) EdgeCount() int {
	count := 0
	for _, neighbors := range g.Neighbors {
		count += len(neighbors)
	}
	return count / 2
}

// Generate uses the explicit config seed when present, including zero. Otherwise
// seed supplies the caller's reproducible run/phase seed. Errors never return a
// partial graph. Connectivity is a model outcome, not an implicit repair step.
func Generate(ctx context.Context, config model.TopologyConfig, n int, seed int64) (Graph, error) {
	if err := ctx.Err(); err != nil {
		return Graph{}, err
	}
	if n < 1 {
		return Graph{}, fmt.Errorf("topology requires at least one node")
	}
	if err := config.Validate(n); err != nil {
		return Graph{}, err
	}
	if config.Seed != nil {
		seed = *config.Seed
	}
	rng := rand.New(rand.NewSource(seed))
	b := &builder{ctx: ctx, neighbors: make([]map[int]struct{}, n)}
	var positions []Position
	var err error
	switch config.Model {
	case "er":
		err = b.pairs(func(_, _ int) bool { return rng.Float64() < *config.P })
	case "ws":
		err = b.wattsStrogatz(rng, *config.K, *config.P)
	case "ba":
		err = b.barabasiAlbert(rng, *config.M)
	case "rgg", "waxman":
		positions = make([]Position, n)
		for i := range positions {
			positions[i] = Position{X: rng.Float64(), Y: rng.Float64()}
		}
		if config.Model == "rgg" {
			err = b.pairs(func(u, v int) bool { return distance(positions[u], positions[v]) <= *config.Radius })
		} else {
			var longest float64
			err = b.pairs(func(u, v int) bool {
				longest = max(longest, distance(positions[u], positions[v]))
				return false
			})
			if err == nil {
				err = b.pairs(func(u, v int) bool {
					chance := *config.Beta
					if longest > 0 {
						chance *= math.Exp(-distance(positions[u], positions[v]) / (*config.Alpha * longest))
					}
					return rng.Float64() < chance
				})
			}
		}
	case "grid", "triangular":
		positions, err = b.lattice(*config.Rows, *config.Columns, config.Model == "triangular")
	}
	if err != nil {
		return Graph{}, err
	}
	graph := Graph{Neighbors: make([][]int, n), Positions: positions}
	for i, neighbors := range b.neighbors {
		if err := ctx.Err(); err != nil {
			return Graph{}, err
		}
		graph.Neighbors[i] = make([]int, 0, len(neighbors))
		for neighbor := range neighbors {
			graph.Neighbors[i] = append(graph.Neighbors[i], neighbor)
		}
		sort.Ints(graph.Neighbors[i])
	}
	return graph, nil
}

type builder struct {
	ctx       context.Context
	neighbors []map[int]struct{}
	edges     int
	steps     uint64
}

func (b *builder) step() error {
	b.steps++
	if b.steps&1023 == 0 {
		return b.ctx.Err()
	}
	return nil
}

func (b *builder) add(u, v int) error {
	if u == v {
		return fmt.Errorf("topology cannot contain a self edge")
	}
	if _, exists := b.neighbors[u][v]; exists {
		return nil
	}
	if b.edges >= MaxEdges {
		return fmt.Errorf("topology exceeds the limit of %d undirected edges", MaxEdges)
	}
	if b.neighbors[u] == nil {
		b.neighbors[u] = make(map[int]struct{})
	}
	if b.neighbors[v] == nil {
		b.neighbors[v] = make(map[int]struct{})
	}
	b.neighbors[u][v], b.neighbors[v][u] = struct{}{}, struct{}{}
	b.edges++
	return nil
}

func (b *builder) pairs(include func(int, int) bool) error {
	for u := range b.neighbors {
		for v := u + 1; v < len(b.neighbors); v++ {
			if err := b.step(); err != nil {
				return err
			}
			if include(u, v) {
				if err := b.add(u, v); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Rewire each original clockwise ring edge once. Choosing only currently
// unconnected targets preserves a simple graph and the original edge count.
func (b *builder) wattsStrogatz(rng *rand.Rand, k int, probability float64) error {
	n := len(b.neighbors)
	for offset := 1; offset <= k/2; offset++ {
		for u := 0; u < n; u++ {
			if err := b.step(); err != nil {
				return err
			}
			if err := b.add(u, (u+offset)%n); err != nil {
				return err
			}
		}
	}
	for offset := 1; offset <= k/2; offset++ {
		for u := 0; u < n; u++ {
			if err := b.step(); err != nil {
				return err
			}
			if rng.Float64() >= probability || len(b.neighbors[u]) == n-1 {
				continue
			}
			w := -1
			// Sparse rings usually succeed on the first draw. Cap rejection
			// attempts, then choose an exact uniform rank among valid targets.
			for attempt := 0; attempt < 16; attempt++ {
				candidate := rng.Intn(n)
				if _, linked := b.neighbors[u][candidate]; candidate != u && !linked {
					w = candidate
					break
				}
			}
			if w < 0 {
				rank := rng.Intn(n - 1 - len(b.neighbors[u]))
				for candidate := 0; candidate < n; candidate++ {
					if err := b.step(); err != nil {
						return err
					}
					if _, linked := b.neighbors[u][candidate]; candidate != u && !linked {
						if rank == 0 {
							w = candidate
							break
						}
						rank--
					}
				}
			}
			v := (u + offset) % n
			delete(b.neighbors[u], v)
			delete(b.neighbors[v], u)
			b.edges--
			if err := b.add(u, w); err != nil {
				return err
			}
		}
	}
	return nil
}

// Start from the m+1-node star used by NetworkX. Each subsequent node selects
// m distinct existing nodes with probability proportional to their degrees.
func (b *builder) barabasiAlbert(rng *rand.Rand, m int) error {
	n := len(b.neighbors)
	weights := degreeTree{tree: make([]int, n+1)}
	for v := 1; v <= m; v++ {
		if err := b.step(); err != nil {
			return err
		}
		if err := b.add(0, v); err != nil {
			return err
		}
	}
	for v := 0; v <= m; v++ {
		weights.add(v, len(b.neighbors[v]))
	}
	targets := make([]int, m)
	for v := m + 1; v < n; v++ {
		for i := range targets {
			if err := b.step(); err != nil {
				return err
			}
			target := weights.choose(rng.Intn(weights.total))
			targets[i] = target
			weights.add(target, -len(b.neighbors[target]))
		}
		for _, target := range targets {
			if err := b.add(v, target); err != nil {
				return err
			}
			weights.add(target, len(b.neighbors[target]))
		}
		weights.add(v, m)
	}
	return nil
}

// A Fenwick tree permits exact integer degree-weighted sampling without the
// unbounded rejection loops of repeated-node sampling near a dense seed graph.
type degreeTree struct {
	tree  []int
	total int
}

func (t *degreeTree) add(index, delta int) {
	t.total += delta
	for i := index + 1; i < len(t.tree); i += i & -i {
		t.tree[i] += delta
	}
}

func (t *degreeTree) choose(ticket int) int {
	index, step := 0, 1
	for step < len(t.tree) {
		step <<= 1
	}
	for step >>= 1; step > 0; step >>= 1 {
		if next := index + step; next < len(t.tree) && t.tree[next] <= ticket {
			index = next
			ticket -= t.tree[next]
		}
	}
	return index
}

func (b *builder) lattice(rows, columns int, triangular bool) ([]Position, error) {
	positions := make([]Position, len(b.neighbors))
	for row := 0; row < rows; row++ {
		for column := 0; column < columns; column++ {
			if err := b.step(); err != nil {
				return nil, err
			}
			u := row*columns + column
			positions[u] = Position{X: float64(column), Y: float64(row)}
			if triangular {
				positions[u].X += float64(row%2) / 2
				positions[u].Y *= math.Sqrt(3) / 2
			}
			if column+1 < columns {
				if err := b.add(u, u+1); err != nil {
					return nil, err
				}
			}
			if row+1 < rows {
				if err := b.add(u, u+columns); err != nil {
					return nil, err
				}
				if triangular {
					next := column - 1
					if row%2 == 1 {
						next = column + 1
					}
					if next >= 0 && next < columns {
						if err := b.add(u, (row+1)*columns+next); err != nil {
							return nil, err
						}
					}
				}
			}
		}
	}
	return positions, nil
}

func distance(a, b Position) float64 { return math.Hypot(a.X-b.X, a.Y-b.Y) }
