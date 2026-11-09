package agent

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// Stream Nodes individually so encoding/json's shared pool never retains a
// buffer sized to the complete multi-Peer topology after the Run has ended.
func encodeNodeView(w io.Writer, nodes []model.Node) error {
	if _, err := io.WriteString(w, "["); err != nil {
		return err
	}
	encoder := json.NewEncoder(w)
	for i, node := range nodes {
		if i > 0 {
			if _, err := io.WriteString(w, ","); err != nil {
				return err
			}
		}
		if err := encoder.Encode(node); err != nil {
			return err
		}
	}
	_, err := io.WriteString(w, "]")
	return err
}

func writeNodeView(w http.ResponseWriter, nodes []model.Node) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = encodeNodeView(w, nodes)
}

func writeHeartbeatView(w http.ResponseWriter, report model.AgentHeartbeat) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if _, err := io.WriteString(w, `{"agent":`); err != nil {
		return
	}
	if err := json.NewEncoder(w).Encode(report.Agent); err != nil {
		return
	}
	if _, err := io.WriteString(w, `,"nodes":`); err != nil {
		return
	}
	if err := encodeNodeView(w, report.Nodes); err != nil {
		return
	}
	if report.Partial {
		_, _ = io.WriteString(w, `,"partial":true`)
	}
	_, _ = io.WriteString(w, "}\n")
}
