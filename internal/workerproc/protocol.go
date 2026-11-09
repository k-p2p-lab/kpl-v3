// Package workerproc supervises private, short-lived roles of the current kpl
// executable. Application data and credentials never travel in argv or env.
package workerproc

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	ProtocolVersion = 1
	MaxMessageBytes = 16 << 20
	privateMarker   = "KPL_PRIVATE_WORKER"
	maxCallDepth    = 4
	callsPerDepth   = 16
	workersPerDepth = 4
	maxErrorBytes   = 2048
)

var (
	ErrClosed       = errors.New("private worker connection is closed")
	ErrTooLarge     = errors.New("private worker message exceeds 16 MiB")
	ErrProtocol     = errors.New("invalid private worker protocol")
	ErrBusy         = errors.New("private worker RPC admission is full")
	ErrDepth        = errors.New("private worker nested RPC depth exceeded")
	ErrNotReaped    = errors.New("private worker has not exited after termination")
	ErrNotQuiescent = errors.New("private worker RPC handlers have not finished")
	ErrPrivateRole  = errors.New("internal-worker requires inherited private pipes")
)

// Handler must honor ctx. It may call the remote peer using that same context;
// nested calls have separate bounded dispatch pools and never block responses.
type Handler func(context.Context, string, json.RawMessage) (any, error)

type Factory func(context.Context, string, json.RawMessage, *Peer) (Handler, error)

// RemoteError preserves cancellation semantics without treating display text as
// structured application evidence. Domain error details belong in RPC payloads.
type RemoteError struct {
	Code    string
	Message string
}

func (e *RemoteError) Error() string { return e.Message }
func (e *RemoteError) Unwrap() error {
	switch e.Code {
	case "canceled":
		return context.Canceled
	case "deadline":
		return context.DeadlineExceeded
	case "busy":
		return ErrBusy
	case "depth":
		return ErrDepth
	case "too_large":
		return ErrTooLarge
	}
	return nil
}

// Identity identifies the private worker lease. It is copied into handler
// contexts only after the incoming frame has matched this connection.
type Identity struct {
	Version int    `json:"protocol"`
	Kind    string `json:"kind"`
	RunID   string `json:"runId"`
	Epoch   string `json:"epoch"`
	Build   string `json:"build"`
}

type identity = Identity
type identityKey struct{}

func IdentityFromContext(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(identityKey{}).(Identity)
	return id, ok
}

type wireError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type frame struct {
	identity
	Type     string          `json:"type"`
	ID       uint64          `json:"id,omitempty"`
	Method   string          `json:"method,omitempty"`
	Body     json.RawMessage `json:"body,omitempty"`
	Error    *wireError      `json:"error,omitempty"`
	Deadline int64           `json:"deadline,omitempty"`
	Depth    int             `json:"depth,omitempty"`
}

func encodeFrame(f frame) ([]byte, error) {
	data, err := json.Marshal(f)
	if err != nil {
		return nil, err
	}
	if len(data) > MaxMessageBytes {
		return nil, ErrTooLarge
	}
	return data, nil
}

func readFrame(r io.Reader) (frame, error) {
	var f frame
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return f, err
	}
	n := binary.BigEndian.Uint32(header[:])
	if n == 0 || n > MaxMessageBytes {
		return f, ErrTooLarge
	}
	data := make([]byte, int(n))
	if _, err := io.ReadFull(r, data); err != nil {
		return f, err
	}
	if err := json.Unmarshal(data, &f); err != nil {
		return f, fmt.Errorf("%w: malformed JSON", ErrProtocol)
	}
	return f, nil
}

func writeFrame(w io.Writer, data []byte) error {
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(data)))
	for _, part := range [][]byte{header[:], data} {
		for len(part) != 0 {
			n, err := w.Write(part)
			if err != nil {
				return err
			}
			if n <= 0 {
				return io.ErrShortWrite
			}
			part = part[n:]
		}
	}
	return nil
}

func remoteFailure(err error) *wireError {
	if err == nil {
		return nil
	}
	code := "remote"
	switch {
	case errors.Is(err, context.Canceled):
		code = "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		code = "deadline"
	case errors.Is(err, ErrBusy):
		code = "busy"
	case errors.Is(err, ErrDepth):
		code = "depth"
	case errors.Is(err, ErrTooLarge):
		code = "too_large"
	}
	message := err.Error()
	if len(message) > maxErrorBytes {
		message = strings.ToValidUTF8(message[:maxErrorBytes], "") + "..."
	}
	return &wireError{Code: code, Message: message}
}

func marshalBody(value any) (json.RawMessage, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(data) > MaxMessageBytes {
		return nil, ErrTooLarge
	}
	return data, nil
}
