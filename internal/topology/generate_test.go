package topology

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"reflect"
	"slices"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func ptr[T any](v T) *T { return &v }

func generated(t *testing.T, config model.TopologyConfig, n int, seed int64) Graph {
	t.Helper()
	g, err := Generate(context.Background(), config, n, seed)
	if err != nil {
		t.Fatal(err)
	}
	if len(g.Neighbors) != n {
		t.Fatalf("got %d nodes, want %d", len(g.Neighbors), n)
	}
	for u, neighbors := range g.Neighbors {
		if !slices.IsSorted(neighbors) {
			t.Fatalf("node %d has unsorted neighbors %v", u, neighbors)
		}
		for i, v := range neighbors {
			if v < 0 || v >= n || v == u || i > 0 && v == neighbors[i-1] {
				t.Fatalf("invalid edge %d-%d in %v", u, v, neighbors)
			}
			if !slices.Contains(g.Neighbors[v], u) {
				t.Fatalf("asymmetric edge %d-%d", u, v)
			}
		}
	}
	return g
}

func TestModelsDeterministicAndOwnResults(t *testing.T) {
	configs := []model.TopologyConfig{
		{Model: "er", P: ptr(0.2)},
		{Model: "ws", P: ptr(0.7), K: ptr(4)},
		{Model: "ba", M: ptr(3)},
		{Model: "rgg", Radius: ptr(0.3)},
		{Model: "waxman", Alpha: ptr(0.4), Beta: ptr(0.9)},
		{Model: "grid", Rows: ptr(5), Columns: ptr(6)},
		{Model: "triangular", Rows: ptr(5), Columns: ptr(6)},
	}
	for _, config := range configs {
		t.Run(config.Model, func(t *testing.T) {
			first := generated(t, config, 30, 819)
			second := generated(t, config, 30, 819)
			if !reflect.DeepEqual(first, second) {
				t.Fatal("same seed produced different graphs")
			}
			config.Seed = ptr(int64(0))
			explicit := generated(t, config, 30, 42)
			if !reflect.DeepEqual(explicit, generated(t, config, 30, 91)) {
				t.Fatal("explicit zero seed did not override the fallback seed")
			}
			for _, neighbors := range first.Neighbors {
				if len(neighbors) > 0 {
					neighbors[0] = -1
					break
				}
			}
			if len(first.Positions) > 0 {
				first.Positions[0].X = -1
			}
			config.Seed = nil
			if !reflect.DeepEqual(second, generated(t, config, 30, 819)) {
				t.Fatal("mutating an earlier result changed later generation")
			}
		})
	}
}

func TestErdosRenyiExtremesAndSeed(t *testing.T) {
	for _, probability := range []float64{0, 1} {
		g := generated(t, model.TopologyConfig{Model: "er", P: ptr(probability)}, 12, 2)
		if want := int(probability * 66); g.EdgeCount() != want {
			t.Fatalf("p=%v edges=%d, want %d", probability, g.EdgeCount(), want)
		}
	}
	config := model.TopologyConfig{Model: "er", P: ptr(0.4)}
	if reflect.DeepEqual(generated(t, config, 30, 1), generated(t, config, 30, 2)) {
		t.Fatal("different seeds produced the same nontrivial random graph")
	}
	config.P = ptr(0.05)
	cohort := generated(t, config, 500, 1001)
	t.Logf("ER(500, 0.05), seed=1001: %d undirected edges", cohort.EdgeCount())
}

func TestWattsStrogatzRingAndRewiring(t *testing.T) {
	config := model.TopologyConfig{Model: "ws", P: ptr(0.0), K: ptr(4)}
	ring := generated(t, config, 12, 5)
	if ring.EdgeCount() != 24 || !slices.Equal(ring.Neighbors[0], []int{1, 2, 10, 11}) {
		t.Fatalf("unexpected ring: %+v", ring)
	}
	for _, neighbors := range ring.Neighbors {
		if len(neighbors) != 4 {
			t.Fatal("ring vertex degree differs from k")
		}
	}
	config.P = ptr(1.0)
	rewired := generated(t, config, 12, 5)
	if rewired.EdgeCount() != ring.EdgeCount() || reflect.DeepEqual(rewired, ring) {
		t.Fatal("rewiring must alter the graph while preserving edge count")
	}
	config.K = ptr(2)
	complete := generated(t, config, 3, 5)
	if complete.EdgeCount() != 3 {
		t.Fatal("complete ring lost edges while searching for rewiring targets")
	}
}

func TestBarabasiAlbertStarGrowthAndConnectivity(t *testing.T) {
	config := model.TopologyConfig{Model: "ba", M: ptr(3)}
	star := generated(t, config, 4, 29)
	if !reflect.DeepEqual(star.Neighbors, [][]int{{1, 2, 3}, {0}, {0}, {0}}) {
		t.Fatalf("initial graph is not the m+1 star: %v", star.Neighbors)
	}
	for _, m := range []int{1, 3, 15} {
		config.M = ptr(m)
		g := generated(t, config, 50, 29)
		if g.EdgeCount() != m*(50-m) || len(g.Neighbors[49]) != m {
			t.Fatalf("m=%d wrong growth edge count or last-node degree", m)
		}
		for v := m + 1; v < 50; v++ {
			if len(g.Neighbors[v]) < m {
				t.Fatalf("node %d lost its original m edges", v)
			}
		}
		seen := map[int]bool{0: true}
		queue := []int{0}
		for len(queue) > 0 {
			u := queue[0]
			queue = queue[1:]
			for _, v := range g.Neighbors[u] {
				if !seen[v] {
					seen[v] = true
					queue = append(queue, v)
				}
			}
		}
		if len(seen) != 50 {
			t.Fatal("preferential attachment disconnected its initial connected graph")
		}
	}
}

func TestDegreeSamplingWeightsAndRemoval(t *testing.T) {
	tree := degreeTree{tree: make([]int, 9)}
	for i, weight := range []int{1, 0, 4, 0, 2} {
		tree.add(i, weight)
	}
	counts := make([]int, 8)
	for ticket := 0; ticket < tree.total; ticket++ {
		counts[tree.choose(ticket)]++
	}
	if !slices.Equal(counts, []int{1, 0, 4, 0, 2, 0, 0, 0}) {
		t.Fatalf("ticket weights = %v", counts)
	}
	tree.add(2, -4)
	for ticket := 0; ticket < tree.total; ticket++ {
		if tree.choose(ticket) == 2 {
			t.Fatal("sampling without replacement selected a removed node")
		}
	}
}

func TestGeometricDistancesAndWaxmanConvention(t *testing.T) {
	for _, radius := range []float64{0, 0.3, 2} {
		g := generated(t, model.TopologyConfig{Model: "rgg", Radius: ptr(radius)}, 25, 181)
		for u, p := range g.Positions {
			if p.X < 0 || p.X >= 1 || p.Y < 0 || p.Y >= 1 {
				t.Fatalf("position outside the unit square: %v", p)
			}
			for v := u + 1; v < len(g.Positions); v++ {
				q := g.Positions[v]
				want := math.Hypot(p.X-q.X, p.Y-q.Y) <= radius
				if slices.Contains(g.Neighbors[u], v) != want {
					t.Fatalf("edge %d-%d disagrees with geometric radius %v", u, v, radius)
				}
			}
		}
	}
	// For two nodes L equals their distance, so Waxman-1 must use
	// beta*exp(-1/alpha), independent of how far apart the points landed.
	for _, params := range [][2]float64{{0.8, 0.3}, {0.3, 0.8}, {1, 1}} {
		config := model.TopologyConfig{Model: "waxman", Alpha: ptr(params[0]), Beta: ptr(params[1])}
		for seed := int64(0); seed < 128; seed++ {
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < 4; i++ {
				rng.Float64() // Two coordinates per node precede the edge draw.
			}
			want := rng.Float64() < params[1]*math.Exp(-1/params[0])
			g := generated(t, config, 2, seed)
			if (g.EdgeCount() == 1) != want {
				t.Fatalf("seed=%d alpha=%v beta=%v violated Waxman-1 probability", seed, params[0], params[1])
			}
		}
		if g := generated(t, config, 1, 0); g.EdgeCount() != 0 || len(g.Positions) != 1 {
			t.Fatal("single-node Waxman graph is invalid")
		}
	}
}

func TestGridAndTriangularLattices(t *testing.T) {
	cases := []struct {
		model string
		want  [][]int
	}{
		{"grid", [][]int{{1, 3}, {0, 2, 4}, {1, 5}, {0, 4}, {1, 3, 5}, {2, 4}}},
		{"triangular", [][]int{{1, 3}, {0, 2, 3, 4}, {1, 4, 5}, {0, 1, 4}, {1, 2, 3, 5}, {2, 4}}},
	}
	for _, tc := range cases {
		t.Run(tc.model, func(t *testing.T) {
			config := model.TopologyConfig{Model: tc.model, Rows: ptr(2), Columns: ptr(3)}
			g := generated(t, config, 6, 0)
			if !reflect.DeepEqual(g.Neighbors, tc.want) {
				t.Fatalf("neighbors = %v, want %v", g.Neighbors, tc.want)
			}
			config.Rows, config.Columns = ptr(5), ptr(5)
			g = generated(t, config, 25, 0)
			wantDegree := 4
			if tc.model == "triangular" {
				wantDegree = 6
			}
			if len(g.Neighbors[12]) != wantDegree {
				t.Fatalf("interior degree = %d, want %d", len(g.Neighbors[12]), wantDegree)
			}
			for u, neighbors := range g.Neighbors {
				for _, v := range neighbors {
					if math.Abs(distance(g.Positions[u], g.Positions[v])-1) > 1e-12 {
						t.Fatalf("non-unit lattice edge %d-%d", u, v)
					}
				}
			}
		})
	}
}

type cancelDuringGeneration struct {
	context.Context
	cancel context.CancelFunc
	checks int
}

func (c *cancelDuringGeneration) Err() error {
	c.checks++
	if c.checks == 2 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestGenerationCancellationAndLimits(t *testing.T) {
	config := model.TopologyConfig{Model: "er", P: ptr(0.0)}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	g, err := Generate(ctx, config, 100, 0)
	if !errors.Is(err, context.Canceled) || !reflect.DeepEqual(g, Graph{}) {
		t.Fatalf("already canceled: graph=%+v err=%v", g, err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	midway := &cancelDuringGeneration{Context: ctx, cancel: cancel}
	g, err = Generate(midway, config, MaxNodes, 0)
	if !errors.Is(err, context.Canceled) || !reflect.DeepEqual(g, Graph{}) || midway.checks != 2 {
		t.Fatalf("mid-generation cancellation: graph=%+v err=%v checks=%d", g, err, midway.checks)
	}
	for _, count := range []int{0, -1, MaxNodes + 1} {
		g, err = Generate(context.Background(), config, count, 0)
		if err == nil || !reflect.DeepEqual(g, Graph{}) {
			t.Fatalf("node limit %d: graph=%+v err=%v", count, g, err)
		}
	}
	config.P = ptr(1.0)
	g, err = Generate(context.Background(), config, MaxNodes, 0)
	if err == nil || !reflect.DeepEqual(g, Graph{}) {
		t.Fatal("complete graph above the edge budget was accepted")
	}
	// A random model can cross its budget while sampling rather than during
	// parameter validation. A new edge must fail without allocating adjacency.
	b := builder{ctx: context.Background(), neighbors: make([]map[int]struct{}, 2), edges: MaxEdges}
	if err := b.add(0, 1); err == nil || b.edges != MaxEdges || b.neighbors[0] != nil || b.neighbors[1] != nil {
		t.Fatal("edge budget was not checked before mutation")
	}
}
