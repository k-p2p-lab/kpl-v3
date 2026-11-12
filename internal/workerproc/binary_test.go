package workerproc

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestBinaryRPCKeepsMetadataAndPayloadIndependent(t *testing.T) {
	input := bytes.Repeat([]byte{0, 255, 128, '"', '\\', '\n', 'a', 1}, MaxBinaryBytes/8)
	l, _ := peerPair(t, nil, func(*Peer) Handler {
		return func(ctx context.Context, method string, raw json.RawMessage) (any, error) {
			var count int
			if err := json.Unmarshal(raw, &count); err != nil {
				return nil, err
			}
			if method == "binary" || method == "binary-response" {
				if method == "binary" && !bytes.Equal(RequestData(ctx), input) {
					return nil, errors.New("binary input changed")
				}
				return Binary{Body: count + 1, Data: append([]byte(nil), input...)}, nil
			}
			return count, nil
		}
	})
	var count int
	output, err := l.CallBinary(t.Context(), "binary", 17, input, &count)
	if err != nil || count != 18 || !bytes.Equal(input, output) {
		t.Fatalf("binary RPC: metadata=%d payload=%d error=%v", count, len(output), err)
	}
	if err := l.Call(t.Context(), "binary-response", 17, &count); !errors.Is(err, ErrProtocol) {
		t.Fatalf("JSON-only call accepted an unexpected raw response: %v", err)
	}
	if err := l.Call(t.Context(), "echo", 23, &count); err != nil || count != 23 {
		t.Fatalf("following JSON RPC failed: count=%d error=%v", count, err)
	}
}

func TestBinaryFrameOwnsPayloadWithoutBase64Expansion(t *testing.T) {
	payload := bytes.Repeat([]byte{0, 255, 128, 'x'}, MaxBinaryBytes/4)
	encoded, err := encodeFrame(frame{identity: identity{Version: ProtocolVersion}, Type: "request", ID: 1, Method: "blob", Body: json.RawMessage(`{"part":7}`), data: payload})
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > len(payload)+1024 {
		t.Fatalf("raw payload expanded: packet=%d payload=%d", len(encoded), len(payload))
	}
	// Queueing owns a copy even if a canceled caller reuses its original bytes
	// before the writer reaches this packet.
	clear(payload)
	var wire bytes.Buffer
	if err := writeFrame(&wire, encoded); err != nil {
		t.Fatal(err)
	}
	decoded, err := readFrame(&wire)
	if err != nil || len(decoded.data) != MaxBinaryBytes || decoded.data[1] != 255 || string(decoded.Body) != `{"part":7}` {
		t.Fatalf("queued frame borrowed mutable caller bytes: bytes=%d error=%v", len(decoded.data), err)
	}
}

func TestBinaryLimitsDoNotPoisonFollowingCalls(t *testing.T) {
	var calls atomic.Int32
	l, _ := peerPair(t, nil, func(*Peer) Handler {
		return func(_ context.Context, method string, _ json.RawMessage) (any, error) {
			calls.Add(1)
			if method == "large" {
				return Binary{Data: make([]byte, MaxBinaryBytes+1)}, nil
			}
			return true, nil
		}
	})
	if _, err := l.CallBinary(t.Context(), "too-large", nil, make([]byte, MaxBinaryBytes+1), nil); !errors.Is(err, ErrTooLarge) || calls.Load() != 0 {
		t.Fatalf("oversized request reached handler: calls=%d error=%v", calls.Load(), err)
	}
	if _, err := l.CallBinary(t.Context(), "large", nil, nil, nil); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized binary response: %v", err)
	}
	var ok bool
	if err := l.Call(t.Context(), "normal", nil, &ok); err != nil || !ok {
		t.Fatalf("following call lost framing: value=%t error=%v", ok, err)
	}
}

func TestBinaryFrameBudgetIncludesMetadataAndPayload(t *testing.T) {
	body, err := marshalBody(strings.Repeat("m", MaxMessageBytes-MaxBinaryBytes))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := encodeFrame(frame{identity: identity{Version: ProtocolVersion}, Type: "request", ID: 1, Method: "blob", Body: body, data: make([]byte, MaxBinaryBytes)}); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("separate metadata and payload caps bypassed the total message limit: %v", err)
	}
}

func TestBinaryCancellationPreservesHandlerLifetime(t *testing.T) {
	entered, exited := make(chan struct{}), make(chan struct{})
	l, _ := peerPair(t, nil, func(*Peer) Handler {
		return func(ctx context.Context, _ string, _ json.RawMessage) (any, error) {
			if len(RequestData(ctx)) != MaxBinaryBytes {
				return nil, errors.New("missing binary input")
			}
			close(entered)
			<-ctx.Done()
			close(exited)
			return nil, ctx.Err()
		}
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := l.CallBinary(ctx, "wait", nil, make([]byte, MaxBinaryBytes), nil); done <- err }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("binary handler did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("binary cancellation lost its error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("binary caller did not cancel")
	}
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("binary cancellation did not reach its handler")
	}
}

func TestBinaryFrameRejectsInvalidLengthsAndControlPayloads(t *testing.T) {
	for _, check := range []struct {
		name string
		meta string
		data []byte
	}{
		{"negative", `{"type":"request","bytes":-1}`, nil},
		{"oversized", `{"type":"request","bytes":1048577}`, nil},
		{"missing", `{"type":"request","bytes":2}`, []byte{1}},
		{"extra", `{"type":"response"}`, []byte{1}},
		{"control", `{"type":"cancel","bytes":1}`, []byte{1}},
		{"error", `{"type":"response","bytes":1,"error":{"code":"remote"}}`, []byte{1}},
	} {
		t.Run(check.name, func(t *testing.T) {
			packet := make([]byte, 4+len(check.meta)+len(check.data))
			binary.BigEndian.PutUint32(packet, uint32(len(check.meta)))
			copy(packet[4:], check.meta)
			copy(packet[4+len(check.meta):], check.data)
			var wire bytes.Buffer
			if err := writeFrame(&wire, packet); err != nil {
				t.Fatal(err)
			}
			if _, err := readFrame(&wire); !errors.Is(err, ErrProtocol) {
				t.Fatalf("invalid binary frame was accepted: %v", err)
			}
		})
	}
	var short bytes.Buffer
	if err := writeFrame(&short, []byte{0, 0, 0, 10, '{'}); err != nil {
		t.Fatal(err)
	}
	if _, err := readFrame(&short); !errors.Is(err, ErrProtocol) {
		t.Fatalf("invalid metadata boundary: %v", err)
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], 64)
	if _, err := readFrame(bytes.NewReader(append(header[:], byte(1)))); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated packet: %v", err)
	}
}
