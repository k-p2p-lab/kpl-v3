package peer

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

func TestBandwidthReporterImmediateExactConcurrentCounters(t *testing.T) {
	b := newBandwidthReporter()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 100; n++ {
				b.LogSentMessage(17)
				b.LogSentMessageStream(17, "/test", "remote")
				b.LogRecvMessage(11)
				b.LogRecvMessageStream(11, "/test", "remote")
			}
		}()
	}
	wg.Wait()
	s := b.snapshot(true)
	if s.SentBytes != 34000 || s.ReceivedBytes != 22000 || !s.Final || len(s.Protocols) != 1 || s.Protocols[0].SentBytes != 34000 || s.ElapsedNS <= 0 {
		t.Fatalf("not exact immediately after hooks: %+v", s)
	}
	if p := b.GetBandwidthForPeer("remote"); p.TotalOut != 34000 || p.TotalIn != 22000 || p.RateOut <= 0 {
		t.Fatalf("peer attribution: %+v", p)
	}
	protocols := b.GetBandwidthByProtocol()
	delete(protocols, "/test")
	peers := b.GetBandwidthByPeer()
	delete(peers, "remote")
	if b.GetBandwidthForProtocol("/test").TotalOut != 34000 || b.GetBandwidthTotals().TotalIn != 22000 || len(b.GetBandwidthByPeer()) != 1 {
		t.Fatal("getters leaked mutable state")
	}
}

func TestBandwidthReporterMeasuresRealLibp2pStreamBytes(t *testing.T) {
	sender, receiver := newBandwidthReporter(), newBandwidthReporter()
	a, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), libp2p.BandwidthReporter(sender), libp2p.DisableMetrics())
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), libp2p.BandwidthReporter(receiver), libp2p.DisableMetrics())
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	const id protocol.ID = "/kpl/bandwidth-test/1"
	payload := bytes.Repeat([]byte("bandwidth"), 8192)
	received := make(chan error, 2)
	b.SetStreamHandler(id, func(stream network.Stream) {
		defer stream.Close()
		for i := 0; i < 2; i++ {
			data := make([]byte, len(payload))
			_, err := io.ReadFull(stream, data)
			if err == nil && !bytes.Equal(data, payload) {
				err = io.ErrUnexpectedEOF
			}
			if err == nil {
				_, err = stream.Write([]byte("ack"))
			}
			received <- err
			if err != nil {
				return
			}
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.Connect(ctx, peer.AddrInfo{ID: b.ID(), Addrs: b.Addrs()}); err != nil {
		t.Fatal(err)
	}
	stream, err := a.NewStream(ctx, b.ID(), id)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(5 * time.Second))
	exchange := func() {
		t.Helper()
		if _, err := stream.Write(payload); err != nil {
			t.Fatal(err)
		}
		ack := make([]byte, 3)
		if _, err := io.ReadFull(stream, ack); err != nil {
			t.Fatal(err)
		}
		if err := <-received; err != nil {
			t.Fatal(err)
		}
	}
	exchange()
	beforeSent, beforeRecv := sender.GetBandwidthForProtocol(id), receiver.GetBandwidthForProtocol(id)
	// Initial protocol negotiation can be attributed to this ID by the initiator
	// and to the empty ID by the responder. Reusing the stream isolates data.
	exchange()
	sent, recv := sender.GetBandwidthForProtocol(id), receiver.GetBandwidthForProtocol(id)
	if sent.TotalOut-beforeSent.TotalOut != int64(len(payload)) || recv.TotalIn-beforeRecv.TotalIn != int64(len(payload)) || sent.TotalIn-beforeSent.TotalIn != 3 || recv.TotalOut-beforeRecv.TotalOut != 3 {
		t.Fatalf("stream byte deltas: sent=%+v recv=%+v beforeSent=%+v beforeRecv=%+v", sent, recv, beforeSent, beforeRecv)
	}
	if sender.snapshot(false).SentBytes < sent.TotalOut || receiver.snapshot(false).ReceivedBytes < recv.TotalIn {
		t.Fatal("total does not include stream bytes")
	}
}

func TestFinalBandwidthSurvivesSaturatedTelemetry(t *testing.T) {
	server := newRunTestServer(t, model.PeerProcessConfig{Node: model.Node{ID: "node", RunID: "run"}})
	server.bandwidth = newBandwidthReporter()
	server.telemetry.events = make(chan model.TraceEvent, 8)
	server.telemetry.startMeasurement([]string{"topic"})
	for i := 0; i < 20; i++ {
		server.telemetry.emit(model.TraceEvent{Type: "deliver"})
	}
	server.telemetry.stopMeasurement()
	server.bandwidth.LogSentMessage(321)
	server.emitBandwidth(true)
	var stop, final model.TraceEvent
	for len(server.telemetry.events) > 0 {
		e := <-server.telemetry.events
		if e.Type == "measurement_stop" {
			stop = e
		}
		if e.Type == "bandwidth" {
			final = e
		}
	}
	if stop.Sequence == 0 || final.Sequence <= stop.Sequence || final.Bandwidth == nil || !final.Bandwidth.Final || final.Bandwidth.SentBytes != 321 {
		t.Fatalf("saturated queue lost final counters: stop=%+v final=%+v", stop, final)
	}
}
