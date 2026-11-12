package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

const workerSourceChunk = 1 << 20

func (snapshot *resultSnapshot) releaseArchiveReader() {
	if snapshot.releaseArchiveRead != nil {
		snapshot.releaseArchiveRead()
	}
}

// A captured NAS snapshot originally owns a streaming read slot. Workers keep
// the pinned file handles but acquire the I/O slot per block instead: retaining
// one slot per batch member would prevent capturing a third archived Run.
func (s *Server) workerSnapshotSource(snapshots []*resultSnapshot) workerproc.Handler {
	for _, snapshot := range snapshots {
		snapshot.releaseArchiveReader()
	}
	source := workerSourceHandler(snapshots)
	return func(ctx context.Context, method string, data json.RawMessage) (any, error) {
		select {
		case s.archiveReadSlots <- struct{}{}:
			defer func() { <-s.archiveReadSlots }()
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return source(ctx, method, data)
	}
}

type workerSourceDescriptor struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

type workerSnapshotDescriptor struct {
	Result            savedResult              `json:"result"`
	Files             []workerSourceDescriptor `json:"files"`
	ExportedAt        time.Time                `json:"exportedAt"`
	Active            bool                     `json:"active"`
	StoredState       string                   `json:"storedState"`
	AnalysisCacheKind string                   `json:"analysisCacheKind,omitempty"`
}

func describeWorkerSnapshot(snapshot *resultSnapshot) workerSnapshotDescriptor {
	d := workerSnapshotDescriptor{Result: snapshot.result, ExportedAt: snapshot.exportedAt, Active: snapshot.active, StoredState: snapshot.storedState, AnalysisCacheKind: snapshot.analysisCacheKind}
	for _, f := range snapshot.files {
		d.Files = append(d.Files, workerSourceDescriptor{Name: f.name, Size: f.size})
	}
	return d
}

type workerSourceRead struct {
	Snapshot int   `json:"snapshot"`
	File     int   `json:"file"`
	Offset   int64 `json:"offset"`
	Length   int   `json:"length"`
}

type workerSourceBytes struct {
	Data []byte `json:"data,omitempty"`
	EOF  bool   `json:"eof"`
}

// The closure holds only already-pinned source handles. It never resolves a
// path supplied by the child or takes Controller state/persistence locks.
func workerSourceHandler(snapshots []*resultSnapshot) workerproc.Handler {
	return func(ctx context.Context, method string, data json.RawMessage) (any, error) {
		if method != "source.read" {
			return nil, errors.New("unknown source operation")
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var request workerSourceRead
		if err := json.Unmarshal(data, &request); err != nil {
			return nil, err
		}
		if request.Snapshot < 0 || request.Snapshot >= len(snapshots) || request.File < 0 || request.File >= len(snapshots[request.Snapshot].files) || request.Offset < 0 || request.Length < 0 || request.Length > workerSourceChunk {
			return nil, errors.New("invalid source read boundary")
		}
		file := snapshots[request.Snapshot].files[request.File]
		if request.Offset >= file.size {
			return workerSourceBytes{EOF: true}, nil
		}
		length := min(int64(request.Length), file.size-request.Offset)
		buffer := make([]byte, int(length))
		n, err := file.reader().ReadAt(buffer, request.Offset)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return workerSourceBytes{Data: buffer[:n], EOF: errors.Is(err, io.EOF) || request.Offset+int64(n) >= file.size}, nil
	}
}

type workerSourceReader struct {
	ctx      context.Context
	peer     *workerproc.Peer
	snapshot int
	file     int
}

func (r workerSourceReader) ReadAt(p []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, errors.New("negative source offset")
	}
	total := 0
	for total < len(p) {
		var response workerSourceBytes
		request := workerSourceRead{Snapshot: r.snapshot, File: r.file, Offset: offset + int64(total), Length: min(len(p)-total, workerSourceChunk)}
		data, err := r.peer.CallBinary(r.ctx, "source.read", request, nil, &response)
		if err != nil {
			return total, err
		}
		if len(response.Data) != 0 {
			return total, errors.New("source bytes must use the binary payload")
		}
		response.Data = data
		if len(response.Data) > request.Length {
			return total, errors.New("source response exceeds requested boundary")
		}
		total += copy(p[total:], response.Data)
		if response.EOF {
			if total == len(p) {
				return total, nil
			}
			return total, io.EOF
		}
		if len(response.Data) == 0 {
			return total, io.ErrNoProgress
		}
	}
	return total, nil
}

func workerSnapshotFromDescriptor(ctx context.Context, peer *workerproc.Peer, index int, d workerSnapshotDescriptor) *resultSnapshot {
	snapshot := &resultSnapshot{result: d.Result, exportedAt: d.ExportedAt, active: d.Active, storedState: d.StoredState, analysisCacheKind: d.AnalysisCacheKind}
	for i, f := range d.Files {
		snapshot.files = append(snapshot.files, resultFile{name: f.Name, size: f.Size, readAt: workerSourceReader{ctx: ctx, peer: peer, snapshot: index, file: i}})
	}
	return snapshot
}

// Filled by the analysis worker below; the factory is kept in the Controller
// package so its calculations use the same definitions as inline unit engines.
func analysisWorkerFactory(ctx context.Context, bootstrap json.RawMessage, peer *workerproc.Peer) (workerproc.Handler, error) {
	return newAnalysisProcessHandler(ctx, bootstrap, peer)
}

func invalidAnalysisOperation(method string) error {
	return fmt.Errorf("unsupported analysis operation %q", method)
}
