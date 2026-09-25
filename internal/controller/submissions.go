package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

var errSubmissionConflict = errors.New("submission key already belongs to a different request")
var errSubmissionDeleted = errors.New("this submission was already accepted and its result was deleted")

type submissionReceipt struct {
	Version int    `json:"version"`
	Digest  string `json:"digest"`
	BatchID string `json:"batchId"`
}

func (s *Server) submitScenario(ctx context.Context, raw []byte, count int, key string) (model.Experiment, error) {
	if key == "" {
		return s.StartScenarioRepeated(ctx, raw, count)
	}
	if len(key) > 128 || strings.ContainsAny(key, "\r\n\x00") {
		return model.Experiment{}, errors.New("invalid submission key")
	}
	sum := sha256.Sum256([]byte(key))
	name := hex.EncodeToString(sum[:])
	id := "run-request-" + name
	hash := sha256.Sum256(append([]byte(fmt.Sprintf("%d\n", count)), raw...))
	digest := hex.EncodeToString(hash[:])
	s.submissionMu.Lock()
	defer s.submissionMu.Unlock()
	path := filepath.Join(s.config.DataDir, "run-submissions")
	if err := os.MkdirAll(path, 0700); err != nil {
		return model.Experiment{}, err
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return model.Experiment{}, err
	}
	defer root.Close()
	var receipt submissionReceipt
	f, e := openResultFile(root, name+".json")
	if e == nil {
		data, e := readAllSmallSubmission(f)
		f.close()
		if e != nil {
			return model.Experiment{}, e
		}
		if e = json.Unmarshal(data, &receipt); e != nil {
			return model.Experiment{}, e
		}
		if receipt.Version != 1 || receipt.BatchID != id || receipt.Digest != digest {
			return model.Experiment{}, errSubmissionConflict
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return model.Experiment{}, e
	} else {
		receipt = submissionReceipt{Version: 1, Digest: digest, BatchID: id}
		if e = writeAnalysisJSON(root, name+".json", receipt); e != nil {
			return model.Experiment{}, e
		}
		if e = syncRunDirectory(root); e != nil {
			return model.Experiment{}, e
		}
		data, e := os.OpenRoot(s.config.DataDir)
		if e != nil {
			return model.Experiment{}, e
		}
		e = syncRunDirectory(data)
		data.Close()
		if e != nil {
			return model.Experiment{}, e
		}
	}
	s.cancelMu.Lock()
	s.state.persistMu.Lock()
	err = s.rollbackBatchExtensionLocked(id)
	if err == nil {
		var deleted bool
		deleted, err = s.state.resultDeletedLocked(id)
		if deleted {
			err = errSubmissionDeleted
		}
	}
	if err == nil {
		record, e := s.readBatchExtension(id)
		err = e
		if record != nil && len(record.Current) > 0 && err == nil {
			data, e := os.ReadFile(filepath.Join(s.config.DataDir, currentRunsDirectory, id, "experiment.json"))
			var run model.Experiment
			if e == nil {
				e = json.Unmarshal(data, &run)
			}
			s.state.persistMu.Unlock()
			s.cancelMu.Unlock()
			return run, e
		}
	}
	s.state.persistMu.Unlock()
	s.cancelMu.Unlock()
	if err != nil {
		return model.Experiment{}, err
	}
	return s.startScenarioRepeated(ctx, raw, count, id)
}
func readAllSmallSubmission(f resultFile) ([]byte, error) {
	if f.size > 4096 {
		return nil, errors.New("invalid submission receipt")
	}
	data := make([]byte, f.size)
	_, err := f.reader().ReadAt(data, 0)
	return data, err
}
