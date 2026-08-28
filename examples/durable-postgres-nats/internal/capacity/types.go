package capacity

import (
	"time"

	"example.com/gomessenger-durable-postgres-nats/internal/demo"
	"example.com/gomessenger-durable-postgres-nats/internal/pgtelemetry"
)

const reportSpecVersion = "1.3"

// BusinessSnapshot is the committed PostgreSQL truth for one run stage.
type BusinessSnapshot struct {
	Accepted       int64 `json:"accepted"`
	Staged         int64 `json:"staged"`
	Published      int64 `json:"published"`
	Committed      int64 `json:"committed"`
	CommittedBytes int64 `json:"committedBytes"`
}

// BrokerSnapshot is the current file-backed JetStream state.
type BrokerSnapshot struct {
	StreamMessages  uint64 `json:"streamMessages"`
	StreamBytes     uint64 `json:"streamBytes"`
	ConsumerPending uint64 `json:"consumerPending"`
	AckPending      int    `json:"ackPending"`
	Redelivered     int    `json:"redelivered"`
	DLQMessages     uint64 `json:"dlqMessages"`
}

// Sample is one point in the load or drain timeline.
type Sample struct {
	ObservedAt      time.Time               `json:"observedAt"`
	RunID           string                  `json:"runId"`
	StageID         string                  `json:"stageId"`
	Phase           string                  `json:"phase"`
	ElapsedSeconds  float64                 `json:"elapsedSeconds"`
	Business        BusinessSnapshot        `json:"business"`
	Broker          BrokerSnapshot          `json:"broker"`
	Application     demo.AppStats           `json:"application"`
	PostgreSQLWaits []pgtelemetry.WaitEvent `json:"postgresqlWaits,omitempty"`
}

// K6Result is the normalized subset of one k6 summary used by classification.
type K6Result struct {
	ExitCode             int     `json:"exitCode"`
	Iterations           int64   `json:"iterations"`
	DroppedIterations    int64   `json:"droppedIterations"`
	OfferedIterations    int64   `json:"offeredIterations"`
	AcceptedOrders       int64   `json:"acceptedOrders"`
	HTTPRequests         int64   `json:"httpRequests"`
	HTTPFailureRate      float64 `json:"httpFailureRate"`
	AcceptedRate         float64 `json:"acceptedRate"`
	HTTPRequestP95Millis float64 `json:"httpRequestP95Millis"`
}

// LatencyStats reports generator-offered-to-handler-write business latency.
type LatencyStats struct {
	P50Millis float64 `json:"p50Millis"`
	P95Millis float64 `json:"p95Millis"`
	P99Millis float64 `json:"p99Millis"`
}

// EnvelopeStats reports exact canonical envelope sizes for one stage.
type EnvelopeStats struct {
	Count      int64   `json:"count"`
	TotalBytes int64   `json:"totalBytes"`
	P50Bytes   float64 `json:"p50Bytes"`
	P95Bytes   float64 `json:"p95Bytes"`
	MaxBytes   int64   `json:"maxBytes"`
}

// StageCounts compares the offered boundary with each durable business boundary.
type StageCounts struct {
	Offered          int64 `json:"offered"`
	HTTPAccepted     int64 `json:"httpAccepted"`
	BusinessAccepted int64 `json:"businessAccepted"`
	Staged           int64 `json:"staged"`
	Published        int64 `json:"published"`
	StreamPublished  int64 `json:"streamPublished"`
	Committed        int64 `json:"committed"`
	CommittedBytes   int64 `json:"committedBytes"`
}

// IntegrityResult is the exact post-drain business reconciliation.
type IntegrityResult struct {
	Passed                 bool     `json:"passed"`
	Orders                 int64    `json:"orders"`
	DistinctOrderMessages  int64    `json:"distinctOrderMessages"`
	Measurements           int64    `json:"measurements"`
	BrokerConfirmed        int64    `json:"brokerConfirmed"`
	Projections            int64    `json:"projections"`
	DistinctProjectionMsgs int64    `json:"distinctProjectionMessages"`
	InvalidMeasurements    int64    `json:"invalidMeasurements"`
	MissingOrderLinks      int64    `json:"missingOrderLinks"`
	MissingProjectionLinks int64    `json:"missingProjectionLinks"`
	Reasons                []string `json:"reasons,omitempty"`
}

// StageReport is the durable result for one warm-up or measured rate.
type StageReport struct {
	StageID                 string               `json:"stageId"`
	Warmup                  bool                 `json:"warmup"`
	TargetRate              int                  `json:"targetRate"`
	LoadStartedAt           time.Time            `json:"loadStartedAt"`
	LoadEndedAt             time.Time            `json:"loadEndedAt"`
	LoadWindowSeconds       float64              `json:"loadWindowSeconds"`
	DrainSeconds            float64              `json:"drainSeconds"`
	DrainCompleted          bool                 `json:"drainCompleted"`
	LoadWindow              StageCounts          `json:"loadWindow"`
	AfterDrain              StageCounts          `json:"afterDrain"`
	RelayMessagesPerSec     float64              `json:"relayMessagesPerSecond"`
	ConsumerMessagesPerSec  float64              `json:"consumerMessagesPerSecond"`
	ConsumerMiBPerSec       float64              `json:"consumerMiBPerSecond"`
	AcceptedMessagesPerSec  float64              `json:"acceptedMessagesPerSecond"`
	OutboxLag               int64                `json:"outboxLag"`
	ConsumerLag             int64                `json:"consumerLag"`
	OutboxLagGrowthPerSec   float64              `json:"outboxLagGrowthPerSecond"`
	ConsumerLagGrowthPerSec float64              `json:"consumerLagGrowthPerSecond"`
	BacklogSlopePerSec      float64              `json:"backlogSlopePerSecond"`
	MaxBusinessBacklog      int64                `json:"maxBusinessBacklog"`
	MaxOutboxBacklog        int64                `json:"maxOutboxBacklog"`
	MaxConsumerPending      uint64               `json:"maxConsumerPending"`
	MaxBrokerRedelivered    int                  `json:"maxBrokerRedelivered"`
	InboxDuplicates         int64                `json:"inboxDuplicates"`
	DLQMessages             int64                `json:"dlqMessages"`
	Latency                 LatencyStats         `json:"latency"`
	InboxHandle             demo.OperationStats  `json:"inboxHandle"`
	BrokerAck               demo.OperationStats  `json:"brokerAck"`
	Envelopes               EnvelopeStats        `json:"envelopes"`
	PostgreSQL              pgtelemetry.Timeline `json:"postgresql"`
	K6                      K6Result             `json:"k6"`
	Sustainable             bool                 `json:"sustainable"`
	UnsustainableReasons    []string             `json:"unsustainableReasons,omitempty"`
	Integrity               IntegrityResult      `json:"integrity"`
}

// Environment records the exact checkout and local execution context.
type Environment struct {
	GoVersion                  string            `json:"goVersion"`
	OutboxVersion              string            `json:"outboxVersion"`
	ContainerOS                string            `json:"containerOs"`
	ContainerArch              string            `json:"containerArch"`
	ContainerCPUs              int               `json:"containerLogicalCpus"`
	HostOS                     string            `json:"hostOs"`
	HostArch                   string            `json:"hostArch"`
	HostCPUs                   string            `json:"hostLogicalCpus"`
	GitCommit                  string            `json:"gitCommit"`
	GitDirty                   string            `json:"gitDirty"`
	PostgreSQLVersion          string            `json:"postgresqlVersion"`
	NATSServerVersion          string            `json:"natsServerVersion"`
	K6Version                  string            `json:"k6Version"`
	OutboxWorkers              int               `json:"outboxWorkers"`
	OutboxReservationBatchSize int               `json:"outboxReservationBatchSize"`
	OutboxProducerMaxConns     int               `json:"outboxProducerMaxConnections"`
	OutboxRelayMaxConns        int               `json:"outboxRelayMaxConnections"`
	OutboxPGXConnectionBudget  int               `json:"outboxPgxConnectionBudget"`
	ConsumerConcurrency        int               `json:"consumerConcurrency"`
	DBMaxOpenConns             int               `json:"dbMaxOpenConnections"`
	JetStreamStorage           string            `json:"jetStreamStorage"`
	PostgreSQLSettings         map[string]string `json:"postgresqlSettings"`
}

// ReportConfig is the stable, serializable experiment configuration.
type ReportConfig struct {
	Profile               string  `json:"profile"`
	Rates                 []int   `json:"rates"`
	WarmupSeconds         float64 `json:"warmupSeconds"`
	StageSeconds          float64 `json:"stageSeconds"`
	DrainTimeoutSeconds   float64 `json:"drainTimeoutSeconds"`
	SampleIntervalSeconds float64 `json:"sampleIntervalSeconds"`
	E2EP95SLOMillis       float64 `json:"e2eP95SloMillis"`
	MinimumRate           int     `json:"minimumRate"`
	PayloadProfile        string  `json:"payloadProfile"`
}

// RunReport is the complete machine-readable capacity artifact.
type RunReport struct {
	SpecVersion           string        `json:"specVersion"`
	RunID                 string        `json:"runId"`
	StartedAt             time.Time     `json:"startedAt"`
	CompletedAt           time.Time     `json:"completedAt"`
	Config                ReportConfig  `json:"config"`
	Environment           Environment   `json:"environment"`
	Warmup                *StageReport  `json:"warmup,omitempty"`
	Stages                []StageReport `json:"stages"`
	IntegrityPassed       bool          `json:"integrityPassed"`
	MaxSustainableRate    int           `json:"maxSustainableRate"`
	CapacityAtLeastTested bool          `json:"capacityAtLeastTested"`
	CapacityStatement     string        `json:"capacityStatement"`
	Failure               string        `json:"failure,omitempty"`
}
