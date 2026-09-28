package netem

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestNetworkUpdateKeepsDelayTreeAndRemovesDisabledConditions(t *testing.T) {
	base := model.NetworkConfig{Delay: "10ms"}
	next := model.NetworkConfig{Delay: "100ms"}
	var calls []string
	run := func(_ context.Context, args ...string) ([]byte, error) {
		calls = append(calls, strings.Join(args, " "))
		return nil, nil
	}
	interfaces := []networkInterface{ipv4Interface("eth0")}
	if err := update(context.Background(), base, next, 20000, interfaces, run); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 1 || calls[0] != "qdisc change dev eth0 parent 1:3 handle 30: netem delay 100000000ns" {
		t.Fatalf("delay change rebuilt filters/tree: %v", calls)
	}
	calls = nil
	if err := update(context.Background(), next, model.NetworkConfig{Delay: "0s"}, 20000, interfaces, run); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 1 || calls[0] != "qdisc del dev eth0 root handle 1:" {
		t.Fatalf("disabled settings survived: %v", calls)
	}
	calls = nil
	shaped := model.NetworkConfig{Delay: "100ms", TBF: &model.TBFConfig{RateMbps: 1, BurstKbit: 32, Latency: "1s"}}
	if err := update(context.Background(), shaped, next, 20000, interfaces, run); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 5 || calls[0] != "qdisc del dev eth0 root handle 1:" || strings.Contains(strings.Join(calls, "\n"), " tbf ") {
		t.Fatalf("old TBF was retained: %v", calls)
	}
	calls = nil
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := update(ctx, base, next, 20000, interfaces, run); !errors.Is(err, context.Canceled) || len(calls) != 0 {
		t.Fatalf("canceled update ran tc: %v %v", calls, err)
	}
}

func TestNetworkUpdateFailuresAreNotReportedAsApplied(t *testing.T) {
	sentinel := errors.New("tc unavailable")
	err := update(context.Background(), model.NetworkConfig{Delay: "10ms"}, model.NetworkConfig{Delay: "100ms"}, 20000, []networkInterface{ipv4Interface("eth0")}, func(context.Context, ...string) ([]byte, error) { return nil, sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("update error lost: %v", err)
	}
	if err := Update(context.Background(), model.NetworkConfig{}, model.NetworkConfig{Schedule: &model.NetworkSchedule{}}, 20000); err == nil {
		t.Fatal("unresolved schedule accepted")
	}
}
