package controller

// Lost sources and explicit deletions both reject late telemetry and runtime
// writes. A lost-source fence alone does not hide restored files from readers
// and is never evidence that Agent cleanup/drain has completed.
const lostResultWritesDirectory = ".lost-result-writes"

func (s *state) resultWriteFencedLocked(id string) (bool, error) {
	deleted, err := s.resultDeletedLocked(id)
	if err != nil || deleted {
		return deleted, err
	}
	return s.resultMarkerLocked(id, lostResultWritesDirectory)
}

func (s *Server) markResultWriteFencedLocked(id string) error {
	return s.markResultMarkerLocked(id, lostResultWritesDirectory, "fencedAt")
}
