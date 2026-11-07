package controller

import (
	"net/http"
	"regexp"
	"strings"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

var runtimeImageDigest = regexp.MustCompile(`^([^@\s]+)@(sha256:[0-9a-f]{64})$`)

type runtimeVersion struct {
	Build       model.BuildMetadata `json:"build"`
	Image       string              `json:"image"`
	ImageDigest string              `json:"imageDigest"`
	ImageSource string              `json:"imageSource"`
}

func (s *Server) handleVersion(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	image := strings.TrimSpace(s.config.RuntimeImage)
	response := runtimeVersion{Build: model.BuildDetails(), Image: image, ImageSource: "unknown"}
	if image != "" {
		response.ImageSource = "deployment"
		if match := runtimeImageDigest.FindStringSubmatch(image); match != nil {
			response.ImageDigest = match[2]
		}
	}
	writeJSON(w, http.StatusOK, response)
}
