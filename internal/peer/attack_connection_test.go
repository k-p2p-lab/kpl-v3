package peer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/libp2p/go-libp2p/core/host"
	corepeer "github.com/libp2p/go-libp2p/core/peer"
)

type attackDialTestHost struct {
	host.Host
	connect func(context.Context, corepeer.AddrInfo) error
}

func (h *attackDialTestHost) Connect(ctx context.Context, info corepeer.AddrInfo) error {
	return h.connect(ctx, info)
}

func TestConnectionAttackFailedCandidateDoesNotStarveLaterTargets(t *testing.T) {
	local, failed, healthy := newConfigTestHost(t), newConfigTestHost(t), newConfigTestHost(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	attempts := make(chan corepeer.ID, 1000)
	h := &attackDialTestHost{Host: local, connect: func(_ context.Context, target corepeer.AddrInfo) error {
		attempts <- target.ID
		if target.ID == healthy.ID() {
			cancel()
			return nil
		}
		return errors.New("target is still listed ready but its address is unreachable")
	}}
	config := model.NodeConfig{ConnectionAttack: &model.ConnectionAttackConfig{Enabled: attackBool(true), Targets: &model.AttackSelector{Groups: []string{"victims"}}, TargetConnections: attackInt(1), RetryInterval: "10ms"}}.WithDefaults()
	runtime := newAttackRuntime(&Server{host: h, config: model.PeerProcessConfig{NodeConfig: config}})
	rows := []model.AttackTarget{
		{NodeID: "first-unreachable", Group: "victims", PeerID: failed.ID().String(), Addresses: []string{"/ip4/127.0.0.1/tcp/1"}, Ready: true},
		{NodeID: "later-healthy", Group: "victims", PeerID: healthy.ID().String(), Addresses: []string{"/ip4/127.0.0.1/tcp/2"}, Ready: true},
	}
	runtime.registry.Store(newAttackRegistry(rows, time.Now()))
	runtime.connectionLoop(ctx)
	close(attempts)
	for id := range attempts {
		if id == healthy.ID() {
			return
		}
	}
	t.Fatalf("failed first candidate consumed every round's dial budget: %v", runtime.snapshot())
}

func TestConnectionAttackDoesNotBroadenPartiallySelectedSharedIdentity(t *testing.T) {
	local, remote := newConfigTestHost(t), newConfigTestHost(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	attempted := make(chan corepeer.ID, 1)
	h := &attackDialTestHost{Host: local, connect: func(_ context.Context, target corepeer.AddrInfo) error {
		attempted <- target.ID
		cancel()
		return nil
	}}
	config := model.NodeConfig{ConnectionAttack: &model.ConnectionAttackConfig{Enabled: attackBool(true), Targets: &model.AttackSelector{Groups: []string{"victims"}}, TargetConnections: attackInt(1), RetryInterval: "10ms"}}.WithDefaults()
	runtime := newAttackRuntime(&Server{host: h, config: model.PeerProcessConfig{NodeConfig: config}})
	runtime.registry.Store(newAttackRegistry([]model.AttackTarget{
		{NodeID: "selected", Group: "victims", PeerID: remote.ID().String(), Addresses: []string{"/ip4/127.0.0.1/tcp/1"}, Ready: true},
		{NodeID: "excluded", Group: "others", PeerID: remote.ID().String(), Addresses: []string{"/ip4/127.0.0.1/tcp/2"}, Ready: true},
	}, time.Now()))
	done := make(chan struct{})
	go func() { defer close(done); runtime.connectionLoop(ctx) }()
	defer func() { cancel(); <-done }()
	waitAttackCondition(t, func() bool { return runtime.snapshot()["connectionAttack"]["rounds"] > 0 })
	cancel()
	<-done
	select {
	case id := <-attempted:
		t.Errorf("partially selected shared transport was dialed: %s", id)
	default:
	}
	if runtime.snapshot()["connectionAttack"]["unknown"] == 0 {
		t.Error("ambiguous target was not recorded as unknown")
	}
}

func TestConnectionAttackCandidateRotationSurvivesRegistryChanges(t *testing.T) {
	local := newConfigTestHost(t)
	var rows []model.AttackTarget
	for _, name := range []string{"a", "b", "c"} {
		remote := newConfigTestHost(t)
		rows = append(rows, model.AttackTarget{NodeID: name, Group: "victims", PeerID: remote.ID().String(), Addresses: []string{"/ip4/127.0.0.1/tcp/1"}, Ready: true})
	}
	type dial struct {
		id     string
		resume chan struct{}
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	attempts := make(chan dial)
	h := &attackDialTestHost{Host: local, connect: func(ctx context.Context, target corepeer.AddrInfo) error {
		request := dial{id: target.ID.String(), resume: make(chan struct{})}
		select {
		case attempts <- request:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-request.resume:
			return errors.New("unreachable")
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	config := model.NodeConfig{ConnectionAttack: &model.ConnectionAttackConfig{Enabled: attackBool(true), Targets: &model.AttackSelector{Groups: []string{"victims"}}, TargetConnections: attackInt(1), RetryInterval: "1ms"}}.WithDefaults()
	runtime := newAttackRuntime(&Server{host: h, config: model.PeerProcessConfig{NodeConfig: config}})
	store := func(rows []model.AttackTarget) { runtime.registry.Store(newAttackRegistry(rows, time.Now())) }
	store(rows)
	done := make(chan struct{})
	go func() { defer close(done); runtime.connectionLoop(ctx) }()
	defer func() { cancel(); <-done }()
	next := func() dial {
		t.Helper()
		select {
		case request := <-attempts:
			return request
		case <-time.After(time.Second):
			t.Fatal("candidate rotation stopped making progress")
			return dial{}
		}
	}
	first := next()
	if first.id != rows[0].PeerID {
		t.Fatal("unexpected initial candidate")
	}
	store(rows[1:2]) // The cursor now exceeds the shortened candidate list.
	close(first.resume)
	second := next()
	if second.id != rows[1].PeerID {
		t.Fatal("shortened registry lost its only eligible candidate")
	}
	store(nil)
	close(second.resume)
	waitAttackCondition(t, func() bool { return runtime.snapshot()["connectionAttack"]["rounds"] >= 3 })
	store(rows)
	seen := map[string]bool{}
	for range len(rows) {
		request := next()
		seen[request.id] = true
		close(request.resume)
	}
	if len(seen) != len(rows) {
		t.Fatalf("stable registry did not give every candidate a turn: %v", seen)
	}
}

func TestConnectionAttackCombinesReadyAddressesForSelectedSharedIdentity(t *testing.T) {
	for _, selectorKind := range []string{"group", "peer-id"} {
		t.Run(selectorKind, func(t *testing.T) {
			local, remote := newConfigTestHost(t), newConfigTestHost(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var received []string
			h := &attackDialTestHost{Host: local, connect: func(_ context.Context, target corepeer.AddrInfo) error {
				defer cancel()
				for _, address := range target.Addrs {
					received = append(received, address.String())
				}
				for _, address := range received {
					if address == "/ip4/127.0.0.1/tcp/2" {
						return nil
					}
				}
				return errors.New("first alias address is unreachable; healthy alias was omitted")
			}}
			selector := &model.AttackSelector{Groups: []string{"victims"}}
			if selectorKind == "peer-id" {
				selector = &model.AttackSelector{PeerIDs: []string{remote.ID().String()}}
			}
			config := model.NodeConfig{ConnectionAttack: &model.ConnectionAttackConfig{Enabled: attackBool(true), Targets: selector, TargetConnections: attackInt(1), RetryInterval: "10ms"}}.WithDefaults()
			runtime := newAttackRuntime(&Server{host: h, config: model.PeerProcessConfig{NodeConfig: config}})
			runtime.registry.Store(newAttackRegistry([]model.AttackTarget{
				{NodeID: "old-address", Group: "victims", PeerID: remote.ID().String(), Addresses: []string{"/ip4/127.0.0.1/tcp/1", "invalid-multiaddr"}, Ready: true},
				{NodeID: "reachable-address", Group: "victims", PeerID: remote.ID().String(), Addresses: []string{"/ip4/127.0.0.1/tcp/1", "/ip4/127.0.0.1/tcp/2"}, Ready: true},
				{NodeID: "not-ready", Group: "victims", PeerID: remote.ID().String(), Addresses: []string{"/ip4/127.0.0.1/tcp/3"}, Ready: false},
			}, time.Now()))
			runtime.connectionLoop(ctx)
			counts := runtime.snapshot()["connectionAttack"]
			if counts["dialAttempts"] != 1 || counts["dialSucceeded"] != 1 || len(received) != 2 || received[0] != "/ip4/127.0.0.1/tcp/1" || received[1] != "/ip4/127.0.0.1/tcp/2" {
				t.Fatalf("selected aliases lost ready address fallback or duplicated the dial: addresses=%v counters=%v", received, counts)
			}
		})
	}
}
