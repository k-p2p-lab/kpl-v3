package model

import "time"

const (
	AgentOnline  = "online"
	AgentOffline = "offline"

	NodeStarting = "starting"
	NodeReady    = "ready"
	NodeStopping = "stopping"
	NodeStopped  = "stopped"
	NodeFailed   = "failed"
)

type Agent struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	URL         string            `json:"url"`
	MetricsURL  string            `json:"metricsUrl,omitempty"`
	Hostname    string            `json:"hostname"`
	Version     string            `json:"version"`
	Capacity    int               `json:"capacity"`
	ActiveNodes int               `json:"activeNodes"`
	State       string            `json:"state"`
	Labels      map[string]string `json:"labels,omitempty"`
	StartedAt   time.Time         `json:"startedAt"`
	LastSeen    time.Time         `json:"lastSeen"`
}

type Node struct {
	ID                string              `json:"id"`
	RunID             string              `json:"runId"`
	Generation        uint64              `json:"generation,omitempty"`
	AgentID           string              `json:"agentId"`
	Group             string              `json:"group"`
	Role              string              `json:"role"`
	Type              string              `json:"type"`
	Profile           string              `json:"profile,omitempty"`
	PeerID            string              `json:"peerId,omitempty"`
	State             string              `json:"state"`
	Addresses         []string            `json:"addresses,omitempty"`
	ConnectedPeers    []string            `json:"connectedPeers,omitempty"`
	RoutingPeers      []string            `json:"routingPeers,omitempty"`
	MeshPeers         map[string][]string `json:"meshPeers,omitempty"`
	OverlayObservedAt time.Time           `json:"overlayObservedAt,omitempty"`
	TopicPeers        map[string]int      `json:"topicPeers,omitempty"`
	PeerScores        map[string]float64  `json:"peerScores,omitempty"`
	Metadata          map[string]string   `json:"metadata,omitempty"`
	StartedAt         time.Time           `json:"startedAt"`
	LastSeen          time.Time           `json:"lastSeen"`
	Error             string              `json:"error,omitempty"`
}

type TraceEvent struct {
	Bandwidth    *BandwidthSample `json:"bandwidth,omitempty"`
	SessionID    string           `json:"sessionId,omitempty"`
	Sequence     uint64           `json:"sequence,omitempty"`
	EventID      string           `json:"eventId,omitempty"`
	RunID        string           `json:"runId"`
	AgentID      string           `json:"agentId,omitempty"`
	NodeID       string           `json:"nodeId"`
	PeerID       string           `json:"peerId,omitempty"`
	Type         string           `json:"type"`
	MessageID    string           `json:"messageId,omitempty"`
	RemotePeerID string           `json:"remotePeerId,omitempty"`
	Topic        string           `json:"topic,omitempty"`
	Timestamp    time.Time        `json:"timestamp"`
	LatencyMS    float64          `json:"latencyMs,omitempty"`
	Fields       map[string]any   `json:"fields,omitempty"`
}

type EventBatch struct {
	AgentID string       `json:"agentId"`
	Events  []TraceEvent `json:"events"`
}

type AgentHeartbeat struct {
	Agent Agent  `json:"agent"`
	Nodes []Node `json:"nodes"`
	// Partial reports update only the included nodes; omission is not an exit.
	Partial bool `json:"partial,omitempty"`
}

type CreateNodeRequest struct {
	ID         string     `json:"id"`
	RunID      string     `json:"runId"`
	Generation uint64     `json:"generation,omitempty"`
	Group      string     `json:"group"`
	Role       string     `json:"role"`
	Type       string     `json:"type"`
	Profile    string     `json:"profile,omitempty"`
	Seed       int64      `json:"seed"`
	Config     NodeConfig `json:"config"`
	Lifetime   string     `json:"lifetime,omitempty"`
}

// MaxPublishPayloadBytes is shared by scenario validation and Peer admission.
const MaxPublishPayloadBytes = 16 << 20

type PublishRequest struct {
	RunID       string `json:"runId"`
	Topic       string `json:"topic,omitempty"`
	PayloadSize int    `json:"payloadSize"`
	// PayloadEncoding defaults to envelope. Raw publishes exactly PayloadSize
	// bytes as PubSub data and carries no run, sender, or timestamp metadata.
	PayloadEncoding    string         `json:"payloadEncoding,omitempty"`
	DeliveryWindow     string         `json:"deliveryWindow,omitempty"`
	TargetNodes        int            `json:"targetNodes,omitempty"`
	TargetNodesByTopic map[string]int `json:"targetNodesByTopic,omitempty"`
	// The Controller freezes remote subscribers immediately before dispatch.
	// Nil means unknown (legacy publication); an empty array means no targets.
	TargetNodeIDs        []string            `json:"targetNodeIds"`
	TargetNodeIDsByTopic map[string][]string `json:"targetNodeIdsByTopic,omitempty"`
	CohortCapturedAt     time.Time           `json:"cohortCapturedAt,omitempty"`
}

type Experiment struct {
	PreviousRunIDs []string          `json:"previousRunIds,omitempty"`
	ID             string            `json:"id"`
	BatchID        string            `json:"batchId,omitempty"`
	Iteration      int               `json:"iteration,omitempty"`
	Repetitions    int               `json:"repetitions,omitempty"`
	Name           string            `json:"name"`
	State          string            `json:"state"`
	Seed           int64             `json:"seed"`
	Phase          int               `json:"phase"`
	TotalPhases    int               `json:"totalPhases"`
	PhaseName      string            `json:"phaseName,omitempty"`
	ActiveJobs     int               `json:"activeJobs"`
	CompletedJobs  int               `json:"completedJobs"`
	FailedJobs     int               `json:"failedJobs"`
	CanceledJobs   int               `json:"canceledJobs"`
	StartedAt      time.Time         `json:"startedAt"`
	FinishedAt     time.Time         `json:"finishedAt,omitempty"`
	Error          string            `json:"error,omitempty"`
	ScenarioYAML   string            `json:"-"`
	Timing         *ExperimentTiming `json:"timing,omitempty"`
}

type Edge struct {
	Source   string `json:"source"`
	Target   string `json:"target"`
	Protocol string `json:"protocol,omitempty"`
	Topic    string `json:"topic,omitempty"`
	// An undirected display edge can represent one or both endpoints' reports.
	ReportedBy []string `json:"reportedBy,omitempty"`
}

type Metrics struct {
	DeliveryWindows                     []string `json:"deliveryWindows,omitempty"`
	Definition                          string   `json:"definition"`
	RunID                               string   `json:"runId,omitempty"`
	Published                           int      `json:"published"`
	Delivered                           int      `json:"delivered"`
	Duplicates                          int      `json:"duplicates"`
	AverageLatencyMS                    float64  `json:"averageLatencyMs"`
	P95LatencyMS                        float64  `json:"p95LatencyMs"`
	Reachability                        float64  `json:"reachability"`
	EligibleDeliveries                  int      `json:"eligibleDeliveries"`
	ExpectedDeliveries                  int      `json:"expectedDeliveries"`
	DeliveryRatioAvailable              bool     `json:"deliveryRatioAvailable"`
	LatencySamples                      int      `json:"latencySamples"`
	InvalidLatencySamples               int      `json:"invalidLatencySamples"`
	AverageDuplicates                   float64  `json:"averageDuplicates"`
	EligibleDuplicates                  int      `json:"eligibleDuplicates"`
	DuplicateSamples                    int      `json:"duplicateSamples"`
	UnscopedPublications                int      `json:"unscopedPublications"`
	DeliveryRatioUpperBound             float64  `json:"deliveryRatioUpperBound"`
	UnknownDeliveries                   int      `json:"unknownDeliveries"`
	MissedDeliveries                    int      `json:"missedDeliveries"`
	LateDeliveries                      int      `json:"lateDeliveries"`
	PendingPublications                 int      `json:"pendingPublications"`
	FinalizedPublications               int      `json:"finalizedPublications"`
	InitialExpectedDeliveries           int      `json:"initialExpectedDeliveries"`
	InitialEligibleDeliveries           int      `json:"initialEligibleDeliveries"`
	InitialUnknownDeliveries            int      `json:"initialUnknownDeliveries"`
	InitialDeliveryRatio                float64  `json:"initialDeliveryRatio"`
	InitialDeliveryRatioUpperBound      float64  `json:"initialDeliveryRatioUpperBound"`
	InitialDeliveryRatioAvailable       bool     `json:"initialDeliveryRatioAvailable"`
	DepartedPairs                       int      `json:"departedPairs"`
	PublicationAvailabilityUnknownPairs int      `json:"publicationAvailabilityUnknownPairs"`
	ContinuityUnknownPairs              int      `json:"continuityUnknownPairs"`
	AvailabilityUnknownPairs            int      `json:"availabilityUnknownPairs"`
	StableCoverage                      float64  `json:"stableCoverage"`
	StableCoverageUpperBound            float64  `json:"stableCoverageUpperBound"`
	StableCoverageAvailable             bool     `json:"stableCoverageAvailable"`
	LegacyPublications                  int      `json:"legacyPublications"`
	MeasurementIncomplete               bool     `json:"measurementIncomplete"`

	Bandwidth *BandwidthSummary `json:"bandwidth,omitempty"`

	GossipSubControl []GossipSubControlMetric `json:"gossipsubControl,omitempty"`
}

// GossipSubControlMetric separates wire RPC occurrences from the protobuf
// control entries and logical message-ID references carried by those RPCs.
type GossipSubControlMetric struct {
	AgentID             string `json:"agentId,omitempty"`
	Direction           string `json:"direction"`
	ControlType         string `json:"controlType"`
	RPCs                int    `json:"rpcs"`
	Entries             int    `json:"entries"`
	MessageIDs          int    `json:"messageIds"`
	PeerExchangeRecords int    `json:"peerExchangeRecords"`
}

type Snapshot struct {
	GeneratedAt time.Time    `json:"generatedAt"`
	Agents      []Agent      `json:"agents"`
	Nodes       []Node       `json:"nodes"`
	Experiments []Experiment `json:"experiments"`
	Edges       []Edge       `json:"edges"`
	Events      []TraceEvent `json:"events"`
	Metrics     Metrics      `json:"metrics"`
}

type PeerProcessConfig struct {
	Node          Node       `json:"node"`
	NodeConfig    NodeConfig `json:"nodeConfig"`
	Seed          int64      `json:"seed"`
	ControllerURL string     `json:"controllerUrl"`
	AgentURL      string     `json:"agentUrl"`
	APListen      string     `json:"apiListen"`
	P2PListen     string     `json:"p2pListen"`
	Token         string     `json:"token,omitempty"`
}

// ExperimentTiming is a live projection, not a persisted completion promise.
type ExperimentTiming struct {
	EstimatedFinishAt      time.Time  `json:"estimatedFinishAt"`
	RemainingSeconds       float64    `json:"remainingSeconds"`
	Basis                  string     `json:"basis"`
	ObservedRuns           int        `json:"observedRuns"`
	Overdue                bool       `json:"overdue"`
	BatchEstimatedFinishAt *time.Time `json:"batchEstimatedFinishAt,omitempty"`
	BatchRemainingSeconds  float64    `json:"batchRemainingSeconds,omitempty"`
	BatchOverdue           bool       `json:"batchOverdue,omitempty"`
}
