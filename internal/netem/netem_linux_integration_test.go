//go:build linux

package netem

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// This opt-in test needs two disposable Linux containers on an attachable Swarm overlay.
// Run this test binary in the remote container with KPL_NETEM_ECHO_SERVER=1 and
// -test.run=TestLinuxNetworkEchoServer. Run the client with NET_ADMIN,
// KPL_NETEM_INTEGRATION=1, KPL_NETEM_REMOTE_IP=<remote container IPv4>, and
// -test.run=TestLinuxNetworkImpairment. Never use host/container network sharing:
// the test changes every active non-loopback IPv4 interface's root qdisc.
func TestLinuxNetworkImpairment(t *testing.T) {
	if os.Getenv("KPL_NETEM_INTEGRATION") != "1" {
		t.Skip("requires an explicitly enabled disposable Linux container")
	}
	if _, err := os.Stat("/.dockerenv"); err != nil {
		t.Fatal("integration test requires a disposable Docker container")
	}
	remote := net.ParseIP(os.Getenv("KPL_NETEM_REMOTE_IP"))
	if remote.To4() == nil || remote.IsLoopback() || remote.IsUnspecified() {
		t.Fatal("KPL_NETEM_REMOTE_IP must identify a separate container's IPv4 address")
	}
	interfaces, err := systemInterfaces()
	if err != nil {
		t.Fatal(err)
	}
	for _, device := range interfaces {
		for _, address := range device.addresses {
			if network, ok := address.(*net.IPNet); ok && network.IP.Equal(remote) {
				t.Fatal("the remote must be a separate container; self traffic bypasses the egress qdisc")
			}
		}
	}
	devices := activeIPv4Interfaces(interfaces)
	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, device := range devices {
			// A test can fail before installing a qdisc, so absence is harmless.
			_ = exec.CommandContext(ctx, "tc", "qdisc", "del", "dev", device, "root").Run()
		}
	}
	t.Cleanup(cleanup)
	applyConfig := func(t *testing.T, config model.NetworkConfig) {
		t.Helper()
		cleanup()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := Apply(ctx, config, DefaultP2PPort); err != nil {
			t.Fatal(err)
		}
	}
	dial := func(port, sourcePort int, timeout time.Duration) (net.Conn, error) {
		dialer := net.Dialer{Timeout: timeout}
		if sourcePort != 0 {
			dialer.LocalAddr = &net.TCPAddr{Port: sourcePort}
		}
		return dialer.Dial("tcp4", net.JoinHostPort(remote.String(), fmt.Sprint(port)))
	}
	echo := func(t *testing.T, port int, size int) time.Duration {
		t.Helper()
		conn, err := dial(port, 0, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
		payload := bytes.Repeat([]byte("n"), size)
		written := make(chan error, 1)
		start := time.Now()
		go func() { _, err := io.Copy(conn, bytes.NewReader(payload)); written <- err }()
		received := make([]byte, len(payload))
		if _, err := io.ReadFull(conn, received); err != nil {
			t.Fatal(err)
		}
		if err := <-written; err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(payload, received) {
			t.Fatal("TCP echo payload changed")
		}
		return time.Since(start)
	}
	t.Run("P2PTotalLossPreservesControl", func(t *testing.T) {
		applyConfig(t, model.NetworkConfig{LossPercent: floatPtr(100)})
		for _, ports := range [][2]int{{20000, 0}, {18000, 20000}} {
			conn, err := dial(ports[0], ports[1], 400*time.Millisecond)
			if err == nil {
				conn.Close()
				t.Fatalf("P2P TCP destination/source port was not impaired: %v", ports)
			}
			if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
				t.Fatalf("expected dropped SYN timeout, got %v", err)
			}
		}
		echo(t, 18000, 32)
	})
	t.Run("Delay", func(t *testing.T) {
		applyConfig(t, model.NetworkConfig{Delay: "100ms"})
		if elapsed := echo(t, 20000, 32); elapsed < 80*time.Millisecond {
			t.Fatalf("100ms outbound delay was not applied: %s", elapsed)
		}
	})
	t.Run("NetemWithTBF", func(t *testing.T) {
		applyConfig(t, model.NetworkConfig{Delay: "10ms", TBF: &model.TBFConfig{RateMbps: 1, BurstKbit: 32, Latency: "2s"}})
		if elapsed := echo(t, 20000, 256*1024); elapsed < time.Second {
			t.Fatalf("1Mbit TBF did not limit 256KiB transfer: %s", elapsed)
		}
	})
	t.Run("AllScopeTotalLossIncludesControl", func(t *testing.T) {
		applyConfig(t, model.NetworkConfig{Scope: "all", LossPercent: floatPtr(100)})
		conn, err := dial(18000, 0, 400*time.Millisecond)
		if err == nil {
			conn.Close()
			t.Fatal("whole-interface impairment bypassed the control port")
		}
		if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
			t.Fatalf("expected dropped SYN timeout, got %v", err)
		}
	})
	t.Run("KernelAcceptsSupportedOptions", func(t *testing.T) {
		for _, distribution := range []string{"normal", "uniform", "pareto", "paretonormal"} {
			applyConfig(t, model.NetworkConfig{Delay: "20ms", Jitter: "2ms", JitterDistribution: distribution,
				LossPercent: floatPtr(1), DuplicatePercent: floatPtr(1), CorruptPercent: floatPtr(1),
				ReorderPercent: floatPtr(1), ReorderCorrelationPercent: floatPtr(10), RateMbps: floatPtr(10), QueueLimit: intPtr(1000)})
		}
		applyConfig(t, model.NetworkConfig{Scope: "all", TBF: &model.TBFConfig{RateMbps: 1, BurstKbit: 32, Latency: "2s"}})
		output, err := exec.Command("tc", "qdisc", "show").CombinedOutput()
		if err != nil || !strings.Contains(string(output), "netem") || !strings.Contains(string(output), "tbf") {
			t.Fatalf("missing installed netem/TBF: %s; %v", output, err)
		}
	})
}

func TestLinuxNetworkEchoServer(t *testing.T) {
	if os.Getenv("KPL_NETEM_ECHO_SERVER") != "1" {
		t.Skip("helper for disposable two-container integration test")
	}
	for _, port := range []string{"18000", "20000"} {
		listener, err := net.Listen("tcp4", "0.0.0.0:"+port)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = listener.Close() })
		go func() {
			for {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				go func() { defer conn.Close(); _, _ = io.Copy(conn, conn) }()
			}
		}()
	}
	// Docker removes this helper after the client completes. The finite upper
	// bound also prevents a forgotten helper from remaining live indefinitely.
	<-time.After(5 * time.Minute)
}
