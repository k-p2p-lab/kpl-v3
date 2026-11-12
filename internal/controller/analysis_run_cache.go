package controller

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const (
	batchRunCacheDirectory  = "analysis-run-cache"
	batchRunCacheIndexFile  = "index.json"
	batchRunCacheSourceName = "batch-run-analysis.json"
	batchRunCacheMaxBytes   = int64(64 << 20)
	batchRunCacheVersion    = 1
)

var errBatchRunCacheTooLarge = errors.New("per-Run analysis cache exceeds its size limit")

type batchRunCacheIndex struct {
	Version         int       `json:"version"`
	AnalysisVersion int       `json:"analysisVersion"`
	RunID           string    `json:"runId"`
	SourceRevision  string    `json:"sourceRevision"`
	SourceHash      string    `json:"sourceHash"`
	ArtifactSlot    int       `json:"artifactSlot"`
	Bytes           int64     `json:"bytes"`
	SHA256          string    `json:"sha256"`
	AsOf            time.Time `json:"asOf"`
}

func batchRunCacheEligible(result savedResult) bool {
	return result.State == "completed" && !result.Active && !result.TelemetryPending && result.DataState != "collecting" &&
		result.CleanupState != "running" && result.CleanupState != "pending" && result.CleanupState != "retained" && result.CleanupState != "failed"
}

func (s *Server) batchRunCacheDirectory(id string, create bool) (*os.Root, error) {
	if !validResultID(id) {
		return nil, errResultNotFound
	}
	data, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		return nil, err
	}
	defer data.Close()
	if create {
		if err := data.Mkdir(batchRunCacheDirectory, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, err
		}
	}
	parent, err := openResultDirectory(data, batchRunCacheDirectory)
	if err != nil {
		return nil, err
	}
	defer parent.Close()
	if create {
		if err := parent.Mkdir(id, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, err
		}
	}
	return openResultDirectory(parent, id)
}

func readBatchRunCacheIndex(root *os.Root) (batchRunCacheIndex, error) {
	var index batchRunCacheIndex
	file, err := openResultFile(root, batchRunCacheIndexFile)
	if err != nil {
		return index, err
	}
	defer file.close()
	if file.size <= 0 || file.size > resultMetadataLimit {
		return index, errors.New("invalid per-Run analysis cache index size")
	}
	decoder := json.NewDecoder(io.NewSectionReader(file.file, 0, file.size))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&index); err != nil {
		return index, err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return index, errors.New("trailing per-Run analysis cache index data")
	}
	digest, err := hex.DecodeString(index.SHA256)
	if index.Version != batchRunCacheVersion || index.AnalysisVersion != currentAnalysisVersion || !validResultID(index.RunID) ||
		index.SourceRevision == "" || len(index.SourceRevision) > 128 || len(index.SourceHash) > 128 || !currentSourceHash(index.SourceHash) ||
		index.ArtifactSlot < 0 || index.ArtifactSlot > 1 || index.Bytes <= 0 || index.Bytes > batchRunCacheMaxBytes ||
		index.AsOf.IsZero() || err != nil || len(digest) != sha256.Size {
		return index, errors.New("invalid per-Run analysis cache identity")
	}
	return index, nil
}

// Caller holds persistMu. Only local manifests and archive descriptors are read;
// no archived log is opened or sampled to establish this cheap identity.
func (s *Server) batchRunCacheMemberLocked(id string) (savedResult, error) {
	root, err := s.analysisDirectory(id)
	if err != nil {
		return savedResult{}, err
	}
	defer root.Close()
	file, err := openResultFile(root, "experiment.json")
	if err != nil {
		return savedResult{}, err
	}
	member, err := readResultMetadata(file, id, s.resultActive(id))
	file.close()
	if err == nil {
		err = s.applyBatchExtension(&member)
	}
	if err == nil {
		member.SourceRevision, err = sourceRevisionAt(root, id)
	}
	return member, err
}

func (s *Server) pinBatchRunCacheMember(ctx context.Context, member savedResult) (func(), bool, error) {
	s.state.persistMu.Lock()
	defer s.state.persistMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	current, err := s.batchRunCacheMemberLocked(member.ID)
	if err != nil {
		return nil, false, err
	}
	if !batchRunCacheEligible(current) || !batchRunCacheEligible(member) || member.SourceRevision == "" || current.SourceRevision != member.SourceRevision {
		return nil, false, nil
	}
	if s.resultReadPins == nil {
		s.resultReadPins = make(map[string]int)
	}
	s.resultReadPins[member.ID]++
	release := sync.OnceFunc(func() {
		s.state.persistMu.Lock()
		s.resultReadPins[member.ID]--
		if s.resultReadPins[member.ID] == 0 {
			delete(s.resultReadPins, member.ID)
		}
		s.state.persistMu.Unlock()
	})
	return release, true, nil
}

func validateBatchRunCachePayload(ctx context.Context, file resultFile, index batchRunCacheIndex) error {
	if file.size != index.Bytes {
		return errors.New("per-Run analysis cache length changed")
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, &resultContextReader{ctx: ctx, reader: file.reader()}); err != nil {
		return err
	}
	if hex.EncodeToString(hash.Sum(nil)) != index.SHA256 {
		return errors.New("per-Run analysis cache checksum changed")
	}
	// Avoid materializing large chart arrays merely to verify a cache reference.
	var proof struct {
		Version         int           `json:"version"`
		AnalysisVersion int           `json:"analysisVersion"`
		Result          savedResult   `json:"result"`
		AsOf            time.Time     `json:"asOf"`
		Metrics         model.Metrics `json:"metrics"`
	}
	decoder := json.NewDecoder(&resultContextReader{ctx: ctx, reader: file.reader()})
	if err := decoder.Decode(&proof); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("trailing per-Run analysis cache payload data")
	}
	if proof.Version != 1 || proof.AnalysisVersion != currentAnalysisVersion || proof.Result.ID != index.RunID ||
		proof.Result.SourceRevision != index.SourceRevision || proof.Result.SourceHash != index.SourceHash ||
		!proof.AsOf.Equal(index.AsOf) || proof.Metrics.RunID != index.RunID || proof.Metrics.PendingPublications != 0 || !batchRunCacheEligible(proof.Result) {
		return errors.New("per-Run analysis cache payload identity changed")
	}
	return ctx.Err()
}

func (s *Server) captureBatchRunAnalysisCache(ctx context.Context, member savedResult) (*resultSnapshot, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if !batchRunCacheEligible(member) || !validResultID(member.ID) || member.SourceRevision == "" {
		return nil, false, nil
	}
	release, pinned, err := s.pinBatchRunCacheMember(ctx, member)
	if err != nil || !pinned {
		return nil, false, err
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()
	root, err := s.batchRunCacheDirectory(member.ID, false)
	if err != nil {
		return nil, false, nil // Derived caches never make saved source data unavailable.
	}
	defer root.Close()
	index, err := readBatchRunCacheIndex(root)
	if err != nil || index.RunID != member.ID || index.SourceRevision != member.SourceRevision {
		return nil, false, nil
	}
	file, err := openResultFile(root, analysisArtifactFile(batchResultFile, index.ArtifactSlot))
	if err != nil {
		return nil, false, nil
	}
	if err := validateBatchRunCachePayload(ctx, file, index); err != nil {
		file.close()
		if ctx.Err() != nil {
			return nil, false, ctx.Err()
		}
		s.logger.Warn("discard invalid per-Run analysis cache", "run", member.ID, "error", err)
		return nil, false, nil
	}
	file.name = batchRunCacheSourceName
	member.SourceHash = index.SourceHash
	snapshot := &resultSnapshot{analysisCacheKind: "batch", result: member, files: []resultFile{file}, exportedAt: index.AsOf, storedState: member.storedState, release: release}
	transferred = true
	return snapshot, true, nil
}

type batchRunCacheWriter struct {
	writer io.Writer
	left   int64
}

// Small reads bound the decoder's look-ahead beyond one Run. Its current Run
// budget is separate from the surrounding batch's potentially much larger file.
type batchRunCacheSeedReader struct {
	ctx     context.Context
	reader  io.Reader
	limited bool
	left    int64
	offset  int64
}

func (reader *batchRunCacheSeedReader) Read(data []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	length := min(len(data), 64<<10)
	if reader.limited {
		if reader.left <= 0 {
			return 0, errBatchRunCacheTooLarge
		}
		length = int(min(int64(length), reader.left))
	}
	n, err := reader.reader.Read(data[:length])
	reader.offset += int64(n)
	if reader.limited {
		reader.left -= int64(n)
	}
	return n, err
}

func (reader *batchRunCacheSeedReader) limitValue(decoder *json.Decoder) {
	reader.limited = true
	reader.left = batchRunCacheMaxBytes - (reader.offset - decoder.InputOffset())
}

func (writer *batchRunCacheWriter) Write(data []byte) (int, error) {
	if int64(len(data)) > writer.left {
		return 0, errBatchRunCacheTooLarge
	}
	n, err := writer.writer.Write(data)
	writer.left -= int64(n)
	return n, err
}

func stageBatchRunCache(ctx context.Context, root *os.Root, analysis resultAnalysis) (*stagedAnalysisJSON, int64, string, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, 0, "", err
	}
	staged := &stagedAnalysisJSON{root: root, temp: ".analysis-" + hex.EncodeToString(nonce[:]) + ".tmp"}
	file, err := root.OpenFile(staged.temp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, 0, "", err
	}
	hash := sha256.New()
	writer := &batchRunCacheWriter{writer: io.MultiWriter(file, hash), left: batchRunCacheMaxBytes}
	err = json.NewEncoder(resultContextWriter{ctx: ctx, writer: writer}).Encode(analysis)
	if err == nil {
		err = file.Sync()
	}
	err = errors.Join(err, file.Close(), ctx.Err())
	if err != nil {
		staged.discard()
		return nil, 0, "", err
	}
	return staged, batchRunCacheMaxBytes - writer.left, hex.EncodeToString(hash.Sum(nil)), nil
}

// Preserve the prior cache slot until both the new payload and index commit.
// Chart encoding and hashing happen outside Controller persistence locks.
func (s *Server) saveBatchRunAnalysisCache(ctx context.Context, analysis resultAnalysis) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if analysis.Version != 1 || analysis.AnalysisVersion != currentAnalysisVersion || analysis.AsOf.IsZero() ||
		analysis.Metrics.RunID != analysis.Result.ID || analysis.Metrics.PendingPublications != 0 ||
		!batchRunCacheEligible(analysis.Result) || len(analysis.Result.SourceHash) > 128 || !currentSourceHash(analysis.Result.SourceHash) {
		return nil
	}
	release, pinned, err := s.pinBatchRunCacheMember(ctx, analysis.Result)
	if err != nil || !pinned {
		return err
	}
	defer release()
	root, err := s.batchRunCacheDirectory(analysis.Result.ID, true)
	if err != nil {
		return err
	}
	defer root.Close()
	staged, size, digest, err := stageBatchRunCache(ctx, root, analysis)
	if err != nil {
		return err
	}
	defer staged.discard()
	s.state.persistMu.Lock()
	defer s.state.persistMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	current, err := s.batchRunCacheMemberLocked(analysis.Result.ID)
	if err != nil {
		return err
	}
	if !batchRunCacheEligible(current) || current.SourceRevision != analysis.Result.SourceRevision {
		return errors.New("Run changed before publishing its analysis cache")
	}
	slot := 0
	if previous, err := readBatchRunCacheIndex(root); err == nil {
		slot = 1 - previous.ArtifactSlot
	}
	if err := staged.publish(analysisArtifactFile(batchResultFile, slot)); err != nil {
		return err
	}
	return writeAnalysisJSON(root, batchRunCacheIndexFile, batchRunCacheIndex{Version: batchRunCacheVersion, AnalysisVersion: currentAnalysisVersion,
		RunID: analysis.Result.ID, SourceRevision: analysis.Result.SourceRevision, SourceHash: analysis.Result.SourceHash,
		ArtifactSlot: slot, Bytes: size, SHA256: digest, AsOf: analysis.AsOf})
}

// Pull one compact Run at a time from an already-published batch. This avoids
// loading a whole batch or transferring its old source logs to an analysis host.
func (s *Server) seedBatchRunAnalysisCache(ctx context.Context, batchID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.analysisJobMu.Lock()
	job, err := s.loadBatchAnalysis(batchID)
	if err != nil {
		s.analysisJobMu.Unlock()
		return err
	}
	status := job.status
	identity := status.savedAnalysisID()
	if identity == "" {
		s.analysisJobMu.Unlock()
		return nil
	}
	root, err := s.batchAnalysisDirectory(batchID, false)
	if err != nil {
		s.analysisJobMu.Unlock()
		return err
	}
	file, err := openResultFile(root, analysisArtifactFile(batchResultFile, status.ArtifactSlot))
	root.Close()
	s.analysisJobMu.Unlock()
	if err != nil {
		return err
	}
	defer file.close()
	return s.seedBatchRunAnalysisCacheFile(ctx, batchID, identity, file, false)
}

// A staged, complete batch may seed independently valid Run caches before its
// public pointer is committed. The caller retains file ownership throughout.
func (s *Server) seedBatchRunAnalysisCacheFile(ctx context.Context, batchID, identity string, file resultFile, replace bool) error {
	reader := &batchRunCacheSeedReader{ctx: ctx, reader: file.reader()}
	decoder := json.NewDecoder(reader)
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return errors.New("invalid saved batch artifact")
	}
	version, analysisVersion, artifactBatch, artifactID := 0, 0, "", ""
	for decoder.More() {
		reader.limitValue(decoder)
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		key, ok := token.(string)
		if !ok {
			return errors.New("invalid saved batch artifact field")
		}
		switch key {
		case "version":
			err = decoder.Decode(&version)
		case "analysisVersion":
			err = decoder.Decode(&analysisVersion)
		case "batchId":
			err = decoder.Decode(&artifactBatch)
		case "analysisId":
			err = decoder.Decode(&artifactID)
		case "runs":
			reader.limited = false
			if version != batchAnalysisVersion || analysisVersion != currentAnalysisVersion || artifactBatch != batchID || artifactID != identity {
				return errors.New("saved batch artifact cannot seed current Run analyses")
			}
			start, err := decoder.Token()
			if err != nil || start != json.Delim('[') {
				return errors.New("invalid saved batch Run collection")
			}
			count := 0
			for decoder.More() {
				if err := ctx.Err(); err != nil {
					return err
				}
				if count >= maxScenarioRepetitions {
					return errors.New("saved batch artifact contains too many Run analyses")
				}
				var analysis resultAnalysis
				before := decoder.InputOffset()
				reader.limitValue(decoder)
				if err := decoder.Decode(&analysis); err != nil {
					return err
				}
				reader.limited = false
				if decoder.InputOffset()-before > batchRunCacheMaxBytes {
					return errBatchRunCacheTooLarge
				}
				count++
				if analysis.Result.BatchID != batchID {
					return errors.New("saved batch artifact contains a foreign Run")
				}
				hit := false
				if !replace {
					cache, found, err := s.captureBatchRunAnalysisCache(ctx, analysis.Result)
					if err != nil && !errors.Is(err, errResultNotFound) {
						return err
					}
					hit = found
					if cache != nil {
						cache.close()
					}
				}
				if !hit {
					if err := s.saveBatchRunAnalysisCache(ctx, analysis); err != nil && !errors.Is(err, errResultNotFound) {
						return fmt.Errorf("cache Run %s: %w", analysis.Result.ID, err)
					}
				}
			}
			_, err = decoder.Token()
		default:
			err = skipBatchRunCacheJSON(decoder)
		}
		if err != nil {
			return err
		}
		reader.limited = false
	}
	if _, err := decoder.Token(); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("trailing saved batch artifact data")
	}
	return ctx.Err()
}

func skipBatchRunCacheJSON(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, compound := token.(json.Delim)
	if !compound {
		return nil
	}
	if delim != '[' && delim != '{' {
		return errors.New("invalid saved batch artifact value")
	}
	for decoder.More() {
		if delim == '{' {
			if _, err := decoder.Token(); err != nil {
				return err
			}
		}
		if err := skipBatchRunCacheJSON(decoder); err != nil {
			return err
		}
	}
	_, err = decoder.Token()
	return err
}
