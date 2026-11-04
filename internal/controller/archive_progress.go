package controller

import (
	"errors"
	"time"
)

const archiveStallAfter = 15 * time.Second

// Store access failures are distinct from a damaged result or failed local
// maintenance operation. Neither a bad run nor a local disk error proves that
// the NAS itself is unavailable.
type archiveAccessError struct {
	kind string
	err  error
}

func (e *archiveAccessError) Error() string { return e.err.Error() }
func (e *archiveAccessError) Unwrap() error { return e.err }

func archiveFailureKind(err error) string {
	if err == nil {
		return ""
	}
	var access *archiveAccessError
	if errors.As(err, &access) {
		return access.kind
	}
	return "maintenance"
}

// Only the single archive worker updates these fields. Reads use cached state;
// dashboard requests never issue a NAS probe. Total cycle duration includes
// transfer throttling and deliberate experiment pauses, so it is not a timeout.
func (s *Server) setArchivePhase(phase string) {
	s.archiveStatusMu.Lock()
	s.archivePhase = phase
	s.archiveLastProgressAt = time.Now()
	s.archiveStatusMu.Unlock()
}

func (s *Server) archiveProgress() {
	s.archiveStatusMu.Lock()
	s.archiveLastProgressAt = time.Now()
	// Successful filesystem work is also evidence that a previous failure is
	// recovering; do not show the old error throughout a large new transfer.
	s.archiveError = ""
	s.archiveErrorKind = ""
	s.archiveStatusMu.Unlock()
}

func archivePhaseCanStall(phase string) bool {
	switch phase {
	case "discovering", "copying", "verifying", "publishing", "deleting":
		return true
	default:
		return false
	}
}
