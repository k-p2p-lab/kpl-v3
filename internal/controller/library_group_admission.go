package controller

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

var errScenarioResultGroup = errors.New("cannot assign scenario results to their library group")

const libraryGroupAdmissionFile = "library-group-admission.json"
const libraryGroupAdmissionLimit = 4096

type libraryGroupAdmission struct {
	Version       int    `json:"version"`
	BatchID       string `json:"batchId"`
	GroupID       string `json:"groupId"`
	PreviousGroup string `json:"previousGroup,omitempty"`
}

func (s *Server) readLibraryGroupAdmission() (*libraryGroupAdmission, error) {
	root, err := os.OpenRoot(s.config.DataDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer root.Close()
	file, err := openResultFile(root, libraryGroupAdmissionFile)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.close()
	if file.size < 0 || file.size > libraryGroupAdmissionLimit {
		return nil, errors.New("invalid library group admission size")
	}
	var record libraryGroupAdmission
	decoder := json.NewDecoder(io.LimitReader(file.file, libraryGroupAdmissionLimit+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, errors.New("invalid library group admission JSON")
	}
	if record.Version != 1 || !validResultID(record.BatchID) || !validScenarioID(record.GroupID) || record.PreviousGroup != "" && !validScenarioID(record.PreviousGroup) {
		return nil, errors.New("invalid library group admission identity")
	}
	return &record, nil
}

func (s *Server) writeLibraryGroupAdmission(record libraryGroupAdmission) error {
	root, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		return err
	}
	defer root.Close()
	return writeAnalysisJSON(root, libraryGroupAdmissionFile, record)
}

func (s *Server) clearLibraryGroupAdmission() error {
	root, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		return err
	}
	defer root.Close()
	if err := root.Remove(libraryGroupAdmissionFile); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncRunDirectory(root)
}

// Startup calls this after batch recovery. The same pending intent is recovered
// before the next library admission, so a failed cleanup cannot be overwritten.
func (s *Server) recoverLibraryGroupAdmissions(ctx context.Context) error {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	s.state.persistMu.Lock()
	defer s.state.persistMu.Unlock()
	s.libraryGroupsMu.Lock()
	defer s.libraryGroupsMu.Unlock()
	return s.recoverLibraryGroupAdmissionLocked(ctx)
}

func (s *Server) recoverLibraryGroupAdmissionLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	intent, err := s.readLibraryGroupAdmission()
	if err != nil || intent == nil {
		return err
	}
	batch, err := s.readBatchExtension(intent.BatchID)
	if err != nil {
		return fmt.Errorf("check result group admission: %w", err)
	}
	// A published batch owns the membership. In particular, do not undo a
	// later manual move or recreate a group that the user has deleted.
	if batch == nil || len(batch.Current) == 0 {
		snapshot, err := s.readLibraryGroups()
		if err != nil {
			return err
		}
		key := "batch:" + intent.BatchID
		if snapshot.Memberships[key] == intent.GroupID {
			delete(snapshot.Memberships, key)
			for _, group := range snapshot.Groups {
				if group.ID == intent.PreviousGroup {
					snapshot.Memberships[key] = group.ID
					break
				}
			}
			snapshot.Revision = rand.Text()
			if err := s.writeLibraryGroups(snapshot); err != nil {
				return fmt.Errorf("restore result group after failed admission: %w", err)
			}
		}
	}
	return s.clearLibraryGroupAdmission()
}

// The caller holds cancelMu and persistMu. Group endpoints never acquire those
// locks. Keep the library snapshot stable only across this local admission; no
// network or archive I/O and no experiment execution occurs under this lock.
func (s *Server) admitScenarioResultGroup(ctx context.Context, scenarioID, batchID string, admit func() error) error {
	if scenarioID == "" {
		return admit()
	}
	s.libraryGroupsMu.Lock()
	defer s.libraryGroupsMu.Unlock()
	if err := s.recoverLibraryGroupAdmissionLocked(ctx); err != nil {
		return errors.Join(errScenarioResultGroup, err)
	}
	if _, err := s.getSavedScenario(scenarioID); err != nil {
		return errors.Join(errScenarioResultGroup, err)
	}
	snapshot, err := s.readLibraryGroups()
	if err != nil {
		return errors.Join(errScenarioResultGroup, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	group := snapshot.Memberships["scenario:"+scenarioID]
	if group == "" {
		return admit()
	}
	key := "batch:" + batchID
	previous := snapshot.Memberships[key]
	if previous == "" && len(snapshot.Memberships) >= libraryMembershipLimit {
		return errors.Join(errScenarioResultGroup, errLibraryGroupsLimit)
	}
	// cancelMu serializes initial admissions; a single durable intent is enough.
	intent := libraryGroupAdmission{Version: 1, BatchID: batchID, GroupID: group, PreviousGroup: previous}
	if err := s.writeLibraryGroupAdmission(intent); err != nil {
		return errors.Join(errScenarioResultGroup, err)
	}
	snapshot.Memberships[key] = group
	snapshot.Revision = rand.Text()
	// Save before admitting any run. A group storage failure must never report
	// submission failure after its experiment has already started.
	if err := s.writeLibraryGroups(snapshot); err != nil {
		// A rename may have succeeded before its directory sync failed. Recover
		// from the actual file, rather than assuming the write changed nothing.
		return errors.Join(errScenarioResultGroup, err, s.recoverLibraryGroupAdmissionLocked(context.Background()))
	}
	if err = admit(); err == nil {
		if cleanupErr := s.clearLibraryGroupAdmission(); cleanupErr != nil {
			s.logger.Warn("clean committed library group admission", "batch", batchID, "error", cleanupErr)
		}
		return nil
	}
	// The batch commit can report an uncertain final directory sync. Preserve
	// its membership if publication occurred, just as admission keeps its runs.
	if recoveryErr := s.recoverLibraryGroupAdmissionLocked(context.Background()); recoveryErr != nil {
		return errors.Join(err, errScenarioResultGroup, recoveryErr)
	}
	return err
}
