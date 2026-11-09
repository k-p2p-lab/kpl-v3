package controller

// Callers hold state.mu. Node values and their nested fields are immutable
// after publication; every replacement goes through setNodeLocked.
func (s *state) markNodeVersionLocked(id string) {
	if s.nodeVersions == nil {
		s.nodeVersions = make(map[string]uint64, len(s.nodes)+1)
		for existing := range s.nodes {
			s.nodeRevision++
			s.nodeVersions[existing] = s.nodeRevision
		}
	}
	s.nodeRevision++
	s.nodeVersions[id] = s.nodeRevision
}

func (s *state) deleteNodeLocked(id string) {
	if _, exists := s.nodes[id]; !exists {
		return
	}
	s.nodeRevision++
	s.nodeRemovalRevision = s.nodeRevision
	delete(s.nodes, id)
	delete(s.nodeVersions, id)
	if len(s.nodeVersions) == 0 {
		s.nodeVersions = nil
	}
	delete(s.nodeReportTimes, id)
	delete(s.activeNodeIDs, id)
}

// Legacy/test states can be assembled before the version index exists. Also
// detect direct insertion/deletion so an omitted removal cannot leave a stale
// Peer in a worker. Production replacements always use setNodeLocked.
func (s *state) reconcileNodeVersionsLocked() {
	changed := len(s.nodeVersions) != len(s.nodes)
	if !changed {
		for id := range s.nodes {
			if s.nodeVersions[id] == 0 {
				changed = true
				break
			}
		}
	}
	if !changed {
		return
	}
	if s.nodeVersions == nil {
		s.nodeVersions = make(map[string]uint64, len(s.nodes))
	}
	s.nodeRevision++
	s.nodeRemovalRevision = s.nodeRevision
	for id := range s.nodeVersions {
		if _, exists := s.nodes[id]; !exists {
			delete(s.nodeVersions, id)
		}
	}
	for id := range s.nodes {
		if s.nodeVersions[id] == 0 {
			s.nodeVersions[id] = s.nodeRevision
		}
	}
}
