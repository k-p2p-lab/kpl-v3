package pubsub

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// SendExperimentalInvalidMessage queues one deliberately invalid wire RPC on an
// existing transport. It bypasses local application validation/publication and
// never enters the local cache. Queued means admission, not remote receipt.
// malformed-protobuf sends one illegal wire-type byte; payloadSize applies only
// to invalid-signature. Cancellation may race admission.
func (p *PubSub) SendExperimentalInvalidMessage(ctx context.Context, id peer.ID, topic, mode string, payloadSize int, sequence uint64) (ExperimentalControlResult, error) {
	if _, ok := p.rt.(*GossipSubRouter); !ok {
		return ExperimentalControlResult{}, errors.New("invalid message injection requires GossipSub")
	}
	if topic == "" || payloadSize < 1 || payloadSize > 1<<20 || mode != "invalid-signature" && mode != "malformed-protobuf" {
		return ExperimentalControlResult{}, errors.New("invalid experimental message parameters")
	}
	if err := ctx.Err(); err != nil {
		return ExperimentalControlResult{}, err
	}
	rpc := &RPC{}
	if mode == "malformed-protobuf" {
		rpc.experimentalMalformed = true
	} else {
		// Native author/sequence IDs are unique across this attack loop. A
		// domain-separated digest avoids collision with normal uint64 seqnos.
		var seq [8]byte
		binary.BigEndian.PutUint64(seq[:], sequence)
		digest := sha256.Sum256(append([]byte("kpl-invalid-message:"), seq[:]...))
		msg := &pb.Message{From: []byte(p.host.ID()), Seqno: digest[:], Topic: &topic, Data: make([]byte, payloadSize)}
		key := p.host.Peerstore().PrivKey(p.host.ID())
		if key == nil {
			return ExperimentalControlResult{}, errors.New("host signing key unavailable")
		}
		if err := signMessage(p.host.ID(), key, msg); err != nil {
			return ExperimentalControlResult{}, err
		}
		msg.Signature[0] ^= 0xff
		rpc.Publish = []*pb.Message{msg}
	}
	done := make(chan ExperimentalControlResult, 1)
	_, err := p.evalMeshFreeze(ctx, func() MeshFreezeSnapshot {
		result := ExperimentalControlResult{}
		defer func() { done <- result }()
		q, ok := p.peers[id]
		if !ok {
			result.Skipped++
			return MeshFreezeSnapshot{}
		}
		gs := p.rt.(*GossipSubRouter)
		if rpc.Size() > p.maxMessageSize {
			gs.tracer.DropRPC(rpc, id)
			result.Dropped++
		} else if err := q.Push(rpc, false); err != nil {
			gs.tracer.DropRPC(rpc, id)
			result.Dropped++
		} else {
			gs.tracer.SendRPC(rpc, id)
			result.Queued++
		}
		return MeshFreezeSnapshot{}
	})
	if err != nil {
		return ExperimentalControlResult{}, err
	}
	return <-done, nil
}
