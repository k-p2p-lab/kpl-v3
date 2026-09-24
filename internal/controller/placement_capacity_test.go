package controller

import (
	"fmt"
	"sync"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestBalancedPlacementDoesNotFillSmallIdleAgentFirst(t *testing.T) {
	for _, smallCapacity := range []int{1, 50} {
		t.Run(fmt.Sprint(smallCapacity), func(t *testing.T) {
			server := newLifecycleTestController(t)
			// The smaller Agent sorts first by ID, but its first Peer would consume
			// a larger fraction of capacity than the larger Agent's first Peer.
			server.state.agents["a-small"] = model.Agent{ID: "a-small", State: model.AgentOnline, Capacity: smallCapacity}
			server.state.agents["z-large"] = model.Agent{ID: "z-large", State: model.AgentOnline, Capacity: 100}
			agent, ok := server.tryReserveAgent("first")
			if !ok || agent.ID != "z-large" {
				t.Fatalf("first placement = %+v, want larger Agent", agent)
			}
		})
	}
}

func TestBalancedPlacementUsesCapacityOverridesForConcurrentJoins(t *testing.T) {
	for _, capacities := range [][]int{{100, 50}, {120, 60, 30}} {
		t.Run(fmt.Sprint(capacities), func(t *testing.T) {
			server := newLifecycleTestController(t)
			total := 0
			for i, capacity := range capacities {
				id := fmt.Sprintf("agent-%d", i)
				capacityTestAgent(t, server.state, id, 200)
				capacityTestSet(t, server.state, id, &capacity)
				total += capacity / 2
			}
			var workers sync.WaitGroup
			for i := 0; i < total; i++ {
				workers.Add(1)
				go func(i int) {
					defer workers.Done()
					if _, ok := server.tryReserveAgent(fmt.Sprintf("node-%d", i)); !ok {
						t.Errorf("reservation %d failed", i)
					}
				}(i)
			}
			workers.Wait()
			for i, capacity := range capacities {
				agent := server.state.agents[fmt.Sprintf("agent-%d", i)]
				if agent.ActiveNodes != capacity/2 || agent.Capacity != capacity {
					t.Fatalf("capacity %d: got %+v, want %d Peers", capacity, agent, capacity/2)
				}
			}
			if len(server.state.reservations) != total {
				t.Fatal("lost concurrent reservations")
			}
		})
	}
}

func TestBalancedPlacementTracksCapacityReductionAndChurnWithoutMovingPeers(t *testing.T) {
	server := newLifecycleTestController(t)
	for _, id := range []string{"large", "reduced"} {
		capacityTestAgent(t, server.state, id, 100)
		for i := 0; i < 40; i++ {
			if _, ok := server.tryReserveAgentWithPlacement(fmt.Sprintf("%s-%d", id, i), id, nil); !ok {
				t.Fatal("initial reservation failed")
			}
		}
	}
	limit := 50
	capacityTestSet(t, server.state, "reduced", &limit)
	for i := 0; i < 40; i++ {
		agent, ok := server.tryReserveAgent(fmt.Sprintf("new-%d", i))
		if !ok || agent.ID != "large" {
			t.Fatalf("smaller Agent got more load before ratios equalized: %+v", agent)
		}
	}
	if a, b := server.state.agents["large"].ActiveNodes, server.state.agents["reduced"].ActiveNodes; a != 80 || b != 40 {
		t.Fatalf("after reduction: %d/%d, want 80/40", a, b)
	}
	// Churn frees slots on the reduced Agent; subsequent joins restore the
	// same capacity ratio instead of continually preferring the larger host.
	for i := 0; i < 20; i++ {
		server.releaseReservation(fmt.Sprintf("reduced-%d", i))
	}
	for i := 0; i < 20; i++ {
		agent, ok := server.tryReserveAgent(fmt.Sprintf("replacement-%d", i))
		if !ok || agent.ID != "reduced" {
			t.Fatalf("did not refill reduced Agent proportionally: %+v", agent)
		}
	}
	if a, b := server.state.agents["large"].ActiveNodes, server.state.agents["reduced"].ActiveNodes; a != 80 || b != 40 {
		t.Fatalf("after churn: %d/%d, want 80/40", a, b)
	}
	for i := 20; i < 40; i++ {
		if server.state.reservations[fmt.Sprintf("reduced-%d", i)] != "reduced" {
			t.Fatal("existing Peer moved")
		}
	}
}

func TestBalancedPlacementUsesAcknowledgedCapacityAfterIncrease(t *testing.T) {
	server := newLifecycleTestController(t)
	a := capacityTestAgent(t, server.state, "large", 100)
	b := capacityTestAgent(t, server.state, "small", 100)
	limit := 50
	b = capacityTestSet(t, server.state, "small", &limit)
	b = capacityTestHeartbeat(t, server.state, b, 50, server.state.agentCapacityRevisions[b.ID])
	// Occupancy is reported, not pending, so the acknowledgment cannot double
	// count the test's previously placed Peers as unobserved reservations.
	a.ActiveNodes = 50
	capacityTestHeartbeat(t, server.state, a, 100, server.state.agentCapacityRevisions[a.ID])
	b.ActiveNodes = 25
	capacityTestHeartbeat(t, server.state, b, 50, server.state.agentCapacityRevisions[b.ID])
	capacityTestSet(t, server.state, "small", nil)
	if agent, ok := server.tryReserveAgent("before-ack"); !ok || agent.ID != "large" {
		t.Fatalf("unacknowledged increase changed weights: %+v", agent)
	}
	server.releaseReservation("before-ack")
	capacityTestHeartbeat(t, server.state, b, 100, server.state.agentCapacityRevisions[b.ID])
	for i := 0; i < 25; i++ {
		if agent, ok := server.tryReserveAgent(fmt.Sprintf("after-ack-%d", i)); !ok || agent.ID != "small" {
			t.Fatalf("acknowledged increase not reflected: %+v", agent)
		}
	}
	if server.state.agents["large"].ActiveNodes != 50 || server.state.agents["small"].ActiveNodes != 50 {
		t.Fatal("equal capacities did not reach equal occupancy")
	}
}
