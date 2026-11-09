package controller

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"strings"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

const workerObservationChunkBytes = 512 << 10

type workerObservationInfo struct {
	RunID  string    `json:"runId"`
	Epoch  string    `json:"epoch"`
	At     time.Time `json:"at"`
	Bytes  int64     `json:"bytes"`
	SHA256 string    `json:"sha256"`
}

type workerObservationChunk struct {
	Offset int64  `json:"offset"`
	Data   []byte `json:"data"`
}

// The full encoded graph exists only in the calculation process. One staging
// file and fixed-size transfer/copy buffers are sufficient in the supervisor.
type workerObservationStage struct {
	info  workerObservationInfo
	root  *os.Root
	file  *os.File
	name  string
	hash  hash.Hash
	bytes int64
}

func (c *runWorkerClient) streamObservation(ctx context.Context, at time.Time, data []byte) (resultErr error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	c.observationOnce.Do(func() { c.observationSlot = make(chan struct{}, 1) })
	select {
	case c.observationSlot <- struct{}{}:
		defer func() { <-c.observationSlot }()
	case <-ctx.Done():
		return ctx.Err()
	}
	sum := sha256.Sum256(data)
	info := workerObservationInfo{RunID: c.runID, Epoch: c.peer.Epoch(), At: at, Bytes: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}
	defer func() {
		if resultErr != nil {
			cleanupCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer stop()
			if err := c.call(cleanupCtx, "observation.abort", nil, nil); err != nil {
				resultErr = errors.Join(resultErr, err)
			}
		}
	}()
	if err := c.call(ctx, "observation.begin", info, nil); err != nil {
		return err
	}
	for offset := 0; offset < len(data); {
		end := min(len(data), offset+workerObservationChunkBytes)
		if err := c.call(ctx, "observation.chunk", workerObservationChunk{Offset: int64(offset), Data: data[offset:end]}, nil); err != nil {
			return err
		}
		offset = end
	}
	return c.call(ctx, "observation.commit", info, nil)
}

func (b *runWorkerBroker) observationOperation(ctx context.Context, method string, raw json.RawMessage) (any, error) {
	b.observationMu.Lock()
	defer b.observationMu.Unlock()
	b.mu.Lock()
	closed := b.closed
	b.mu.Unlock()
	if closed {
		return nil, errors.New("Run worker lease is closed")
	}
	identity, ok := workerproc.IdentityFromContext(ctx)
	if !ok || identity.Kind != "controller-run" || identity.RunID != b.runID {
		return nil, errors.New("observation has no matching private worker identity")
	}
	if method == "observation.abort" {
		return nil, b.clearObservationStageLocked()
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, b.clearObservationStageLocked())
	}
	var operationErr error
	switch method {
	case "observation.begin":
		var info workerObservationInfo
		if err := json.Unmarshal(raw, &info); err != nil {
			return nil, err
		}
		digest, err := hex.DecodeString(info.SHA256)
		if info.RunID != b.runID || info.Epoch != identity.Epoch || info.At.IsZero() || info.Bytes <= 0 || len(digest) != sha256.Size || err != nil {
			return nil, errors.New("observation header does not match its Run, epoch, timestamp or byte digest")
		}
		if b.observationStage != nil {
			return nil, errors.New("a Run observation transfer is already active")
		}
		stage, err := b.newObservationStage(info)
		if err != nil {
			return nil, err
		}
		b.observationStage = stage
		return nil, nil
	case "observation.chunk":
		var chunk workerObservationChunk
		if err := json.Unmarshal(raw, &chunk); err != nil {
			operationErr = err
			break
		}
		stage := b.observationStage
		if stage == nil || stage.file == nil || stage.info.Epoch != identity.Epoch || len(chunk.Data) == 0 || len(chunk.Data) > workerObservationChunkBytes || chunk.Offset != stage.bytes || int64(len(chunk.Data)) > stage.info.Bytes-stage.bytes {
			operationErr = errors.New("observation chunk exceeds its bound or committed cursor")
			break
		}
		n, err := stage.file.Write(chunk.Data)
		if n > 0 {
			stage.bytes += int64(n)
			_, _ = stage.hash.Write(chunk.Data[:n])
		}
		if n != len(chunk.Data) && err == nil {
			err = io.ErrShortWrite
		}
		operationErr = errors.Join(err, ctx.Err())
	case "observation.commit":
		var info workerObservationInfo
		if err := json.Unmarshal(raw, &info); err != nil {
			operationErr = err
			break
		}
		stage := b.observationStage
		if stage == nil || stage.file == nil || stage.info != info || info.Epoch != identity.Epoch || stage.bytes != info.Bytes || hex.EncodeToString(stage.hash.Sum(nil)) != info.SHA256 {
			operationErr = errors.New("observation length, checksum or header does not match the completed transfer")
			break
		}
		operationErr = b.commitObservationStage(ctx, stage)
		return nil, errors.Join(operationErr, b.clearObservationStageLocked())
	default:
		return nil, errors.New("unknown observation transfer operation")
	}
	if operationErr != nil {
		operationErr = errors.Join(operationErr, b.clearObservationStageLocked())
	}
	return nil, operationErr
}

func (b *runWorkerBroker) newObservationStage(info workerObservationInfo) (*workerObservationStage, error) {
	s := b.server
	s.state.persistMu.Lock()
	defer s.state.persistMu.Unlock()
	if deleted, err := s.state.resultDeletedLocked(b.runID); err != nil || deleted {
		return nil, errors.Join(err, errors.New("observation Run is deleted"))
	}
	parent, err := s.openResultRuns()
	if err != nil {
		return nil, err
	}
	root, err := openResultDirectory(parent, b.runID)
	parentCloseErr := parent.Close()
	if err != nil {
		return nil, errors.Join(err, parentCloseErr)
	}
	if parentCloseErr != nil {
		return nil, errors.Join(parentCloseErr, root.Close())
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, errors.Join(err, root.Close())
	}
	name := ".observation-" + hex.EncodeToString(nonce[:]) + ".tmp"
	file, err := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return nil, errors.Join(err, root.Close())
	}
	return &workerObservationStage{info: info, root: root, file: file, name: name, hash: sha256.New()}, nil
}

// Caller holds observationMu. A failed unlink remains owned for the close path
// to retry; neither its handle nor its cleanup error is silently discarded.
func (b *runWorkerBroker) clearObservationStageLocked() error {
	stage := b.observationStage
	if stage == nil {
		return nil
	}
	var closeErr error
	if stage.file != nil {
		closeErr = stage.file.Close()
		stage.file = nil
	}
	err := stage.root.Remove(stage.name)
	if errors.Is(err, os.ErrNotExist) {
		err = nil
	}
	if err != nil {
		return errors.Join(closeErr, err)
	}
	err = syncRunDirectory(stage.root)
	err = errors.Join(closeErr, err, stage.root.Close())
	b.observationStage = nil
	return err
}

func (b *runWorkerBroker) closeObservationStage() error {
	b.observationMu.Lock()
	defer b.observationMu.Unlock()
	return b.clearObservationStageLocked()
}

func (b *runWorkerBroker) commitObservationStage(ctx context.Context, stage *workerObservationStage) error {
	if err := stage.file.Sync(); err != nil {
		return err
	}
	if _, err := stage.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	s := b.server
	s.state.persistMu.Lock()
	defer s.state.persistMu.Unlock()
	if deleted, err := s.state.resultDeletedLocked(b.runID); err != nil || deleted {
		return errors.Join(err, errors.New("observation Run is deleted"))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if info, err := stage.root.Lstat("observations.jsonl"); err == nil && !info.Mode().IsRegular() {
		return errors.New("observations are not a regular file")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	f, err := stage.root.OpenFile("observations.jsonl", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err == nil && !info.Mode().IsRegular() {
		err = errors.New("observations are not a regular file")
	}
	if err == nil {
		err = appendObservationBytes(ctx, f, stage.file, info.Size(), stage.info.Bytes)
	}
	err = errors.Join(err, f.Close())
	if err == nil {
		err = syncRunDirectory(stage.root)
	}
	if err == nil {
		s.state.markRunArchiveDirty(b.runID)
	}
	return err
}

type observationAppendFile interface {
	io.Writer
	io.Seeker
	Sync() error
	Truncate(int64) error
}

func appendObservationBytes(ctx context.Context, target observationAppendFile, source io.Reader, offset, size int64) (resultErr error) {
	if _, err := target.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	defer func() {
		if resultErr != nil {
			// Readers pin their source prefix under persistMu. Roll back before
			// releasing it so a failed or canceled transfer exposes no partial row.
			resultErr = errors.Join(resultErr, target.Truncate(offset), target.Sync())
		}
	}()
	buffer := make([]byte, 64<<10)
	remaining := size
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := io.ReadFull(source, buffer[:min(int64(len(buffer)), remaining)])
		if n > 0 {
			written, writeErr := target.Write(buffer[:n])
			if writeErr != nil {
				return writeErr
			}
			if written != n {
				return io.ErrShortWrite
			}
			remaining -= int64(n)
		}
		if err != nil {
			return fmt.Errorf("observation stage is shorter than its declared size: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if n, err := target.Write([]byte{'\n'}); err != nil {
		return err
	} else if n != 1 {
		return io.ErrShortWrite
	}
	return errors.Join(target.Sync(), ctx.Err())
}

func isRunWorkerStageTemporary(name string) bool {
	if isArchiveManifestTemporary(name) {
		return true
	}
	const prefix = ".observation-"
	return strings.HasPrefix(name, prefix) && isArchiveManifestTemporary(".analysis-"+strings.TrimPrefix(name, prefix))
}

// Called only after the owning worker and its RPC handlers are quiescent, in
// the FIFO cleanup turn. A previous failed unlink or parent crash must not let
// an abandoned transfer survive a later claim of a clean Run boundary.
func (s *Server) cleanupRunWorkerStages(ctx context.Context, id string) (resultErr error) {
	s.state.persistMu.Lock()
	defer s.state.persistMu.Unlock()
	if deleted, err := s.state.resultDeletedLocked(id); err != nil || deleted {
		return err
	}
	parent, err := s.openResultRuns()
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, parent.Close()) }()
	root, err := openResultDirectory(parent, id)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, root.Close()) }()
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, dir.Close()) }()
	removed := false
	defer func() {
		if removed {
			resultErr = errors.Join(resultErr, syncRunDirectory(root))
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries, readErr := dir.ReadDir(128)
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !isRunWorkerStageTemporary(entry.Name()) {
				continue
			}
			info, err := root.Lstat(entry.Name())
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return errors.New("Run worker temporary is not a regular file")
			}
			if err := root.Remove(entry.Name()); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			removed = true
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}
