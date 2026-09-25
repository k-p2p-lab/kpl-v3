package controller

import "time"

const archiveStallAfter = 15 * time.Second

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
