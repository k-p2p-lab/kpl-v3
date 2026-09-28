package controller

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

const resourceMeasurementLimit = 100
const resourceMeasurementDuration = 24 * time.Hour
const resourceMeasurementFile = "agent-resource-measurements.json"

type resourceMeasurement struct {
	ID        string    `json:"id"`
	StartedAt time.Time `json:"startedAt"`
	EndedAt   time.Time `json:"endedAt,omitzero"`
	EndReason string    `json:"endReason,omitempty"`
}

type resourceMeasurementList struct {
	GeneratedAt        time.Time             `json:"generatedAt"`
	MaxDurationSeconds int                   `json:"maxDurationSeconds"`
	Measurements       []resourceMeasurement `json:"measurements"`
}

func (m resourceMeasurement) effective(now time.Time) resourceMeasurement {
	if m.EndedAt.IsZero() && !now.Before(m.StartedAt.Add(resourceMeasurementDuration)) {
		m.EndedAt = m.StartedAt.Add(resourceMeasurementDuration)
		m.EndReason = "time_limit"
	}
	return m
}

// The small interval index is local Controller metadata. Samples stay in the
// existing Prometheus TSDB; no new collector, experiment lock or NAS I/O is used.
// All access to the index is serialized independently of export queries.
func (s *Server) loadResourceMeasurementsLocked() error {
	if s.resourceMeasurementsLoaded {
		return nil
	}
	file, err := os.Open(filepath.Join(s.config.DataDir, resourceMeasurementFile))
	if errors.Is(err, os.ErrNotExist) {
		s.resourceMeasurements = []resourceMeasurement{}
		s.resourceMeasurementsLoaded = true
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()
	const limit = 256 << 10
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return err
	}
	var stored struct {
		Version      int                   `json:"version"`
		Measurements []resourceMeasurement `json:"measurements"`
	}
	if len(data) > limit || json.Unmarshal(data, &stored) != nil || stored.Version != 1 || len(stored.Measurements) > resourceMeasurementLimit {
		return errors.New("invalid resource measurement index")
	}
	seen := map[string]bool{}
	for i, m := range stored.Measurements {
		id, err := hex.DecodeString(m.ID)
		if err != nil || len(id) != 16 || seen[m.ID] || m.StartedAt.IsZero() ||
			(m.EndedAt.IsZero() && (i != 0 || m.EndReason != "")) ||
			(!m.EndedAt.IsZero() && (m.EndedAt.Before(m.StartedAt) || m.EndedAt.Sub(m.StartedAt) > resourceMeasurementDuration || (m.EndReason != "stopped" && m.EndReason != "time_limit"))) {
			return errors.New("invalid resource measurement interval")
		}
		seen[m.ID] = true
	}
	s.resourceMeasurements = stored.Measurements
	s.resourceMeasurementsLoaded = true
	return nil
}

func (s *Server) saveResourceMeasurementsLocked(items []resourceMeasurement) error {
	data, err := json.Marshal(struct {
		Version      int                   `json:"version"`
		Measurements []resourceMeasurement `json:"measurements"`
	}{1, items})
	if err != nil {
		return err
	}
	if err := os.MkdirAll(s.config.DataDir, 0o755); err != nil {
		return err
	}
	if err := writeFileAtomic(filepath.Join(s.config.DataDir, resourceMeasurementFile), data, 0o600); err != nil {
		return err
	}
	// Publish only after a successful atomic replacement; a failed stop is retryable.
	s.resourceMeasurements = items
	return nil
}

func (s *Server) resourceMeasurementListLocked(now time.Time) resourceMeasurementList {
	result := resourceMeasurementList{GeneratedAt: now, MaxDurationSeconds: int(resourceMeasurementDuration.Seconds()), Measurements: make([]resourceMeasurement, len(s.resourceMeasurements))}
	for i, m := range s.resourceMeasurements {
		result.Measurements[i] = m.effective(now)
	}
	return result
}

func (s *Server) resourceMeasurementAction(action, id string) (resourceMeasurementList, int, string) {
	s.resourceMeasurementsMu.Lock()
	defer s.resourceMeasurementsMu.Unlock()
	if err := s.loadResourceMeasurementsLocked(); err != nil {
		return resourceMeasurementList{}, http.StatusServiceUnavailable, "Cannot read the local resource measurement index"
	}
	now := time.Now().UTC()
	result := s.resourceMeasurementListLocked(now)
	switch action {
	case "start":
		// One shared active interval. Concurrent/retried starts return that interval.
		if len(result.Measurements) == 0 || !result.Measurements[0].EndedAt.IsZero() {
			var random [16]byte
			if _, err := rand.Read(random[:]); err != nil {
				return result, http.StatusInternalServerError, "Cannot create measurement ID"
			}
			m := resourceMeasurement{ID: hex.EncodeToString(random[:]), StartedAt: now}
			items := append([]resourceMeasurement{m}, result.Measurements...)
			if len(items) > resourceMeasurementLimit {
				items = items[:resourceMeasurementLimit]
			}
			if err := s.saveResourceMeasurementsLocked(items); err != nil {
				return result, http.StatusServiceUnavailable, "Cannot save measurement start; check local Controller storage"
			}
			result.Measurements = items
			return result, http.StatusCreated, ""
		}
	case "stop":
		for i, m := range result.Measurements {
			if m.ID != id {
				continue
			}
			// Stop is tied to an ID: a delayed request cannot stop a newer measurement.
			if m.EndedAt.IsZero() {
				m.EndedAt = now
				if m.EndedAt.Before(m.StartedAt) {
					m.EndedAt = m.StartedAt
				}
				m.EndReason = "stopped"
				result.Measurements[i] = m
				if err := s.saveResourceMeasurementsLocked(result.Measurements); err != nil {
					return result, http.StatusServiceUnavailable, "Cannot save measurement stop; retry after checking local Controller storage"
				}
			}
			return result, http.StatusOK, ""
		}
		return result, http.StatusNotFound, "Resource measurement not found"
	}
	return result, http.StatusOK, ""
}

func (s *Server) writeResourceMeasurementAction(w http.ResponseWriter, action, id string) {
	w.Header().Set("Cache-Control", "no-store")
	result, status, message := s.resourceMeasurementAction(action, id)
	// A slow browser must not hold the measurement lock while writing a response.
	if message != "" {
		writeError(w, status, message)
		return
	}
	writeJSON(w, status, result)
}

func (s *Server) handleResourceMeasurements(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	action := "list"
	if r.Method == http.MethodPost {
		action = "start"
	}
	s.writeResourceMeasurementAction(w, action, "")
}

func (s *Server) handleStopResourceMeasurement(w http.ResponseWriter, r *http.Request) {
	s.writeResourceMeasurementAction(w, "stop", r.PathValue("measurementID"))
}

func (s *Server) handleExportResourceMeasurement(w http.ResponseWriter, r *http.Request) {
	s.resourceMeasurementsMu.Lock()
	err := s.loadResourceMeasurementsLocked()
	var found *resourceMeasurement
	if err == nil {
		for _, m := range s.resourceMeasurements {
			if m.ID == r.PathValue("measurementID") {
				value := m.effective(time.Now().UTC())
				found = &value
				break
			}
		}
	}
	s.resourceMeasurementsMu.Unlock()
	w.Header().Set("Cache-Control", "no-store")
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "Cannot read the local resource measurement index")
		return
	}
	if found == nil {
		writeError(w, http.StatusNotFound, "Resource measurement not found")
		return
	}
	if found.EndedAt.IsZero() {
		writeError(w, http.StatusConflict, "Stop the measurement before downloading its fixed interval")
		return
	}
	s.serveResourceHistory(w, r, found.StartedAt, found.EndedAt, "kpl-agent-measurement-"+found.ID)
}
