package peer

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// Lifecycle tests provide a host without P2P listeners after the container startup boundary.
// Actual startup always resolves its overlay address and prepares its namespace.
func newRunTestServer(t *testing.T, config model.PeerProcessConfig) *Server {
	t.Helper()
	config.NodeConfig = config.NodeConfig.WithDefaults()
	if err := config.NodeConfig.Validate(); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &Server{
		config:     config,
		host:       newConfigTestHost(t),
		topics:     make(map[string]*pubsub.Topic),
		peerScores: make(map[string]float64),
		telemetry:  newTelemetry(config.Node, config.AgentURL, config.Token, logger),
		logger:     logger,
		startedAt:  time.Now().UTC(),
	}
}
