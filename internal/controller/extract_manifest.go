package controller

import (
	"bytes"
	"crypto/sha256"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

var extractRunID = regexp.MustCompile(`^run-[A-Za-z0-9-]+$`)
var unsafeExtractLabel = regexp.MustCompile(`[^A-Za-z0-9_.-]+`)

type extractParameters struct{ payload, degree string }

// A Run with several configured values has no single numeric label. Leaving
// it empty also lets the extractor use the recorded size of each message.
func scenarioExtractParameters(raw []byte) (extractParameters, error) {
	spec, err := scenario.Parse(raw)
	if err != nil {
		return extractParameters{}, err
	}
	payloads, degrees := make(map[int]bool), make(map[int]bool)
	for _, phase := range spec.Phases {
		switch phase.Action {
		case "publish":
			payloads[phase.PayloadSize] = true
		case "join":
			g := phase.Node.GossipSub
			if g.Enabled != nil && *g.Enabled && g.Router == "gossipsub" && g.Params.D != nil {
				degrees[*g.Params.D] = true
			}
		case "schedule":
			for _, change := range phase.Schedule.Changes {
				if g := change.Set.GossipSub; g != nil && g.Params.D != nil {
					degrees[*g.Params.D] = true
				}
			}
		}
	}
	single := func(values map[int]bool) string {
		if len(values) == 1 {
			for value := range values {
				return strconv.Itoa(value)
			}
		}
		return ""
	}
	return extractParameters{single(payloads), single(degrees)}, nil
}

func (s *Server) extractManifestRow(runs *os.Root, id string, cache map[[32]byte]extractParameters) ([]string, error) {
	s.state.persistMu.Lock()
	deleted, err := s.state.resultDeletedLocked(id)
	var dir *os.Root
	if err == nil && !deleted {
		dir, err = openResultDirectory(runs, id)
	}
	s.state.persistMu.Unlock()
	if err != nil {
		return nil, err
	}
	if deleted {
		return nil, errResultNotFound
	}
	defer dir.Close()
	meta, err := readResumeFile(dir, "experiment.json", resultMetadataLimit)
	if err != nil {
		return nil, err
	}
	var run struct {
		ID, Name, State string
		Seed            *int64
	}
	if err := json.Unmarshal(meta, &run); err != nil || run.ID != id || run.State == "" {
		return nil, errors.New("invalid experiment.json")
	}
	var params extractParameters
	raw, err := readResumeFile(dir, "scenario.yaml", scenarioYAMLLimit)
	if err == nil {
		key := sha256.Sum256(raw)
		var known bool
		if params, known = cache[key]; !known {
			params, err = scenarioExtractParameters(raw)
			if err != nil {
				return nil, fmt.Errorf("invalid scenario.yaml: %w", err)
			}
			cache[key] = params
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	label := downloadFilePart(strings.Trim(unsafeExtractLabel.ReplaceAllString(run.Name, "-"), ".-"), id)
	seed := ""
	if run.Seed != nil {
		seed = strconv.FormatInt(*run.Seed, 10)
	}
	return []string{params.payload, params.degree, seed, id, label}, nil
}

func (s *Server) writeExtractManifest(w http.ResponseWriter, r *http.Request, ids []string) {
	select {
	case s.libraryDownloadSlots <- struct{}{}:
		defer func() { <-s.libraryDownloadSlots }()
	default:
		writeError(w, http.StatusTooManyRequests, "two bulk downloads are already in progress")
		return
	}
	runs, err := s.openResultRuns()
	if errors.Is(err, os.ErrNotExist) {
		writeError(w, http.StatusNotFound, "no saved Runs are available")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot open saved results")
		return
	}
	defer runs.Close()
	var content bytes.Buffer
	writer := csv.NewWriter(&content)
	writer.UseCRLF = true
	_ = writer.Write([]string{"payload_bytes", "degree", "seed", "run_id", "label"})
	cache := make(map[[32]byte]extractParameters)
	for _, id := range ids {
		if r.Context().Err() != nil {
			return
		}
		row, err := s.extractManifestRow(runs, id, cache)
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, errResultNotFound) {
				status = http.StatusNotFound
			}
			s.logger.Warn("read extract manifest input", "run", id, "error", err)
			writeError(w, status, "cannot read saved inputs for "+id+"; refresh results and check the Controller log")
			return
		}
		_ = writer.Write(row)
	}
	writer.Flush()
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="kpl_extract_manifest.csv"`)
	_, _ = w.Write(content.Bytes())
}
