package controller

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

const (
	workerInventoryPageBytes     = 4 << 20
	workerInventoryFrameBytes    = workerproc.MaxMessageBytes - (64 << 10)
	workerInventoryLeaseDuration = 15 * time.Second
)

type workerInventoryRequest struct {
	Lease   string `json:"lease,omitempty"`
	Next    int    `json:"next,omitempty"`
	Release bool   `json:"release,omitempty"`
}
type workerInventoryPage struct {
	Lease string            `json:"lease"`
	Next  int               `json:"next"`
	Done  bool              `json:"done"`
	Items []json.RawMessage `json:"items"`
}
type workerInventoryItem struct {
	Experiment *model.Experiment `json:"experiment,omitempty"`
	Agent      *model.Agent      `json:"agent,omitempty"`
	Node       *model.Node       `json:"node,omitempty"`
	ReportedAt time.Time         `json:"reportedAt,omitempty"`
}

// Only one short borrowed snapshot may exist per Run broker. The state already
// owns these immutable values; the lease adds shallow references, never a
// second history-sized encoded snapshot. A timer also releases an abandoned
// page sequence while its Run remains alive.
type workerInventoryLease struct {
	token    string
	snapshot workerInventory
	next     int
	timer    *time.Timer
}

func (b *runWorkerBroker) clearInventoryLeaseLocked() {
	if b.inventoryLease != nil {
		b.inventoryLease.timer.Stop()
		b.inventoryLease = nil
	}
}
func (b *runWorkerBroker) inventoryPage(ctx context.Context, request workerInventoryRequest) (workerInventoryPage, error) {
	// authorizeAgent takes state.mu before b.mu. Capture the borrowed values
	// before acquiring b.mu as well, avoiding an RWMutex writer inversion.
	var candidate workerInventory
	if !request.Release && request.Lease == "" && request.Next == 0 {
		if err := ctx.Err(); err != nil {
			return workerInventoryPage{}, err
		}
		candidate = b.inventory()
		sort.Slice(candidate.Agents, func(i, j int) bool { return candidate.Agents[i].ID < candidate.Agents[j].ID })
		sort.Slice(candidate.Nodes, func(i, j int) bool { return candidate.Nodes[i].ID < candidate.Nodes[j].ID })
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return workerInventoryPage{}, errors.New("Run worker lease is closed")
	}
	if request.Release {
		if b.inventoryLease != nil && b.inventoryLease.token == request.Lease {
			b.clearInventoryLeaseLocked()
		}
		return workerInventoryPage{Done: true}, nil
	}
	if err := ctx.Err(); err != nil {
		return workerInventoryPage{}, err
	}
	if request.Lease == "" {
		if request.Next != 0 || b.inventoryLease != nil {
			return workerInventoryPage{}, errors.New("inventory lease is already active or cursor is invalid")
		}
		if candidate.Experiment.ID != b.runID {
			return workerInventoryPage{}, errors.New("inventory Run identity mismatch")
		}
		lease := &workerInventoryLease{token: rand.Text(), snapshot: candidate}
		b.inventoryLease = lease
		lease.timer = time.AfterFunc(workerInventoryLeaseDuration, func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			if b.inventoryLease == lease {
				b.inventoryLease = nil
			}
		})
	} else if b.inventoryLease == nil || b.inventoryLease.token != request.Lease || b.inventoryLease.next != request.Next {
		return workerInventoryPage{}, errors.New("inventory lease expired or cursor does not match")
	}
	lease := b.inventoryLease
	page := workerInventoryPage{Lease: lease.token, Next: lease.next}
	total := 1 + len(lease.snapshot.Agents) + len(lease.snapshot.Nodes)
	bytes := 0
	for page.Next < total {
		if err := ctx.Err(); err != nil {
			b.clearInventoryLeaseLocked()
			return workerInventoryPage{}, err
		}
		item := workerInventoryItem{}
		index := page.Next
		switch {
		case index == 0:
			item.Experiment = &lease.snapshot.Experiment
		case index <= len(lease.snapshot.Agents):
			item.Agent = &lease.snapshot.Agents[index-1]
		default:
			node := &lease.snapshot.Nodes[index-1-len(lease.snapshot.Agents)]
			item.Node = node
			item.ReportedAt = lease.snapshot.ReportTimes[node.ID]
		}
		encoded, err := json.Marshal(item)
		if err != nil {
			b.clearInventoryLeaseLocked()
			return workerInventoryPage{}, err
		}
		if len(encoded) > workerInventoryFrameBytes {
			b.clearInventoryLeaseLocked()
			return workerInventoryPage{}, fmt.Errorf("inventory record exceeds private frame budget: %d bytes", len(encoded))
		}
		if len(page.Items) > 0 && bytes+len(encoded)+1 > workerInventoryPageBytes {
			break
		}
		page.Items = append(page.Items, encoded)
		bytes += len(encoded) + 1
		page.Next++
		if bytes >= workerInventoryPageBytes {
			break
		}
	}
	lease.next = page.Next
	page.Done = page.Next == total
	if page.Done {
		b.clearInventoryLeaseLocked()
	}
	return page, nil
}

func (c *runWorkerClient) readInventory(ctx context.Context) (workerInventory, error) {
	snapshot := workerInventory{ReportTimes: make(map[string]time.Time)}
	request := workerInventoryRequest{}
	complete := false
	defer func() {
		if !complete && request.Lease != "" {
			releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
			defer cancel()
			_ = c.call(releaseCtx, "inventory", workerInventoryRequest{Lease: request.Lease, Release: true}, nil)
		}
	}()
	seenNodes := make(map[string]bool)
	seenAgents := make(map[string]bool)
	for {
		var page workerInventoryPage
		if err := c.call(ctx, "inventory", request, &page); err != nil {
			return workerInventory{}, err
		}
		if page.Lease == "" || request.Lease != "" && request.Lease != page.Lease || page.Next != request.Next+len(page.Items) || len(page.Items) == 0 {
			return workerInventory{}, errors.New("invalid inventory page identity or progress")
		}
		request.Lease = page.Lease
		for _, raw := range page.Items {
			var item workerInventoryItem
			if err := json.Unmarshal(raw, &item); err != nil {
				return workerInventory{}, err
			}
			switch {
			case item.Experiment != nil && item.Agent == nil && item.Node == nil:
				if snapshot.Experiment.ID != "" || item.Experiment.ID != c.runID {
					return workerInventory{}, errors.New("inventory experiment identity mismatch")
				}
				snapshot.Experiment = *item.Experiment
			case item.Agent != nil && item.Experiment == nil && item.Node == nil:
				if item.Agent.ID == "" || seenAgents[item.Agent.ID] {
					return workerInventory{}, errors.New("duplicate inventory Agent")
				}
				seenAgents[item.Agent.ID] = true
				snapshot.Agents = append(snapshot.Agents, *item.Agent)
			case item.Node != nil && item.Experiment == nil && item.Agent == nil:
				if item.Node.RunID != c.runID || item.Node.ID == "" || seenNodes[item.Node.ID] {
					return workerInventory{}, errors.New("inventory Peer identity mismatch")
				}
				seenNodes[item.Node.ID] = true
				snapshot.Nodes = append(snapshot.Nodes, *item.Node)
				snapshot.ReportTimes[item.Node.ID] = item.ReportedAt
			default:
				return workerInventory{}, errors.New("invalid inventory record")
			}
		}
		request.Next = page.Next
		if page.Done {
			if snapshot.Experiment.ID != c.runID {
				return workerInventory{}, errors.New("inventory omitted its experiment")
			}
			complete = true
			return snapshot, nil
		}
	}
}
