package workerproc

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"reflect"
	"testing"
)

// Exercise malformed/truncated IPC framing and lossless accepted messages.
// Application method/identity authorization remains the Peer handler's job.
func FuzzPrivateFrameDecoderBoundaries(f *testing.F) {
	for _, message := range []frame{
		{Type: "cancel", ID: 1},
		{Type: "request", ID: 2, Method: "run.finish", Body: json.RawMessage(`{"runId":"fixture-run"}`), Deadline: 1794747260000000000},
		{Type: "response", ID: 3, Body: json.RawMessage(`{"error":"<retained>&\"original\"","count":9223372036854775807}`)},
		{Type: "request", ID: 4, Method: "observation.chunk", Body: json.RawMessage(`{"offset":0}`), data: []byte{0, 1, 255, '{', '}'}, Depth: maxCallDepth},
		{Type: "response", ID: 5, Error: &wireError{Code: "deadline", Message: "context deadline exceeded"}},
	} {
		message.identity = identity{Version: ProtocolVersion, Kind: "controller-run", RunID: "fixture-run", Epoch: "fixture-epoch", Build: "fixture-build"}
		encoded, err := encodeFrame(message)
		if err != nil {
			f.Fatal(err)
		}
		var packet bytes.Buffer
		if err := writeFrame(&packet, encoded); err != nil {
			f.Fatal(err)
		}
		f.Add(packet.Bytes())
	}
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{0, 0, 0, 4, 0, 0, 0, 0})
	var oversized [4]byte
	binary.BigEndian.PutUint32(oversized[:], MaxMessageBytes+1)
	f.Add(oversized[:])
	f.Fuzz(func(t *testing.T, data []byte) {
		// Fuzz common metadata/raw-payload sizes; deterministic size-limit
		// tests cover full 16 MiB messages without retaining giant corpus files.
		if len(data) > 2<<20 {
			return
		}
		original, err := readFrame(bytes.NewReader(data))
		if err != nil {
			return
		}
		encoded, err := encodeFrame(original)
		if err != nil {
			t.Fatalf("accepted frame cannot be re-encoded: %v", err)
		}
		var packet bytes.Buffer
		if err := writeFrame(&packet, encoded); err != nil {
			t.Fatal(err)
		}
		repeated, err := readFrame(&packet)
		if err != nil || packet.Len() != 0 {
			t.Fatalf("encoded frame lost its exact boundary: %v, remaining=%d", err, packet.Len())
		}
		body := func(raw json.RawMessage) any {
			if len(raw) == 0 {
				return nil
			}
			decoder := json.NewDecoder(bytes.NewReader(raw))
			decoder.UseNumber()
			var value any
			if err := decoder.Decode(&value); err != nil {
				t.Fatalf("accepted frame contains invalid JSON body: %v", err)
			}
			return value
		}
		if !reflect.DeepEqual(body(original.Body), body(repeated.Body)) || !bytes.Equal(original.data, repeated.data) {
			t.Fatal("frame round trip changed its application body or binary payload")
		}
		original.Body, repeated.Body, original.data, repeated.data = nil, nil, nil, nil
		if !reflect.DeepEqual(original, repeated) {
			t.Fatal("frame round trip changed lifecycle, error, deadline or identity metadata")
		}
	})
}
