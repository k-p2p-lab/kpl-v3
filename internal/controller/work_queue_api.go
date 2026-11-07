package controller

import (
	"errors"
	"net/http"
	"strings"
)

func (s *Server) handleWorkQueueAction(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v1/work-queue/"), "/")
	if len(parts) != 2 || parts[0] == "" || len(parts[0]) > 256 || parts[1] != "cancel" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	if err := s.cancelWork(parts[0]); err != nil {
		status := http.StatusConflict
		if errors.Is(err, errWorkNotFound) {
			status = http.StatusNotFound
		}
		writeError(w, status, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"id": parts[0], "state": "canceling"})
}
