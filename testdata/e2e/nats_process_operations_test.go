package e2e_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	inboxsqlite "github.com/assurrussa/gomessenger/adapters/inbox/sqlite"
	natsadapter "github.com/assurrussa/gomessenger/adapters/nats"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	opsSteadyRate      = 125 // Conservative configuration, not a measured capacity claim.
	opsFaultRate       = opsSteadyRate / 2
	opsLoadDuration    = 120 * time.Second
	opsPollInterval    = 100 * time.Millisecond
	opsDrainSLO        = 60 * time.Second
	opsCampaignTimeout = 15 * time.Minute
	opsEffectSchema    = `CREATE TABLE process_effects (
		sequence INTEGER PRIMARY KEY AUTOINCREMENT,
		message_id TEXT NOT NULL UNIQUE,
		job_id INTEGER NOT NULL UNIQUE,
		worker_pid INTEGER NOT NULL,
		executions INTEGER NOT NULL
	)`
)

type opsCampaignReport struct {
	Scope      string              `json:"scope"`
	StartedAt  time.Time           `json:"startedAt"`
	FinishedAt time.Time           `json:"finishedAt"`
	GoVersion  string              `json:"goVersion"`
	CPUCount   int                 `json:"cpuCount"`
	BinaryHash string              `json:"testBinarySha256"`
	BuildInfo  string              `json:"buildInfo"`
	Stages     []opsStageReport    `json:"stages"`
	Events     []opsLifecycleEvent `json:"events"`
	Processes  []opsProcessEvent   `json:"processes"`
	Passed     bool                `json:"passed"`
	Failure    string              `json:"failure,omitempty"`
}

type opsLifecycleEvent struct {
	Action     string    `json:"action"`
	At         time.Time `json:"at"`
	OldPID     int       `json:"oldPid,omitempty"`
	NewPID     int       `json:"newPid,omitempty"`
	Sequence   uint64    `json:"sequence,omitempty"`
	RecordHash string    `json:"recordSha256,omitempty"`
}

type opsStageReport struct {
	Name               string      `json:"name"`
	Replicas           int         `json:"replicas"`
	Rate               int         `json:"rate"`
	StartedAt          time.Time   `json:"startedAt"`
	LoadEndedAt        time.Time   `json:"loadEndedAt"`
	DurationSeconds    float64     `json:"durationSeconds"`
	Expected           int         `json:"expected"`
	Attempted          int         `json:"attempted"`
	GeneratorDropped   int         `json:"generatorDropped"`
	Confirmed          int         `json:"confirmed"`
	ConfirmedInWindow  int         `json:"confirmedInWindow"`
	Committed          int         `json:"committed"`
	ObservedInWindow   int         `json:"observedInWindow"`
	ObservedP95Millis  float64     `json:"observedCommitP95UpperBoundMillis"`
	PubAckP95Millis    float64     `json:"offeredToPubAckP95Millis"`
	DrainSeconds       float64     `json:"drainSeconds"`
	LagGrowthPerSecond float64     `json:"secondHalfLagGrowthPerSecond"`
	MaxAckPending      int         `json:"maximumSampledAckPending"`
	MaxRedelivered     int         `json:"maximumSampledOutstandingRedeliveries"`
	ObservedDuplicates int64       `json:"observedDuplicateDeliveries"`
	HandleErrors       int64       `json:"handleErrors"`
	AckErrors          int64       `json:"ackErrors"`
	IntegrityPassed    bool        `json:"integrityPassed"`
	Sustainable        bool        `json:"sustainable"`
	Reasons            []string    `json:"reasons,omitempty"`
	Failure            string      `json:"failure,omitempty"`
	Ledger             string      `json:"ledger,omitempty"`
	EffectsByProcess   map[int]int `json:"effectsByProcess"`
}

type opsFixture struct {
	database     *sql.DB
	databasePath string
	broker       *opsChild
	brokerConfig opsProcessConfig
	consumers    []*opsChild
	children     []*opsChild
	js           jetstream.JetStream
	route        *natsadapter.Route
	jobID        int64
	report       *opsCampaignReport
	retired      opsProcessEvent
	evidence     *opsEvidence
	expected     map[string]opsExpectedEffect
}

type opsEvidence struct {
	report     *os.File
	ledgers    *os.Root
	ledgerPath string
}

type opsExpectedEffect struct {
	JobID     int64
	WorkerPID int
}

type opsDelivery struct {
	JobID      int64
	Envelope   []byte
	OfferedAt  time.Time
	AckAt      time.Time
	ObservedAt time.Time
	WorkerPID  int
}

type opsLoadState struct {
	mu           sync.Mutex
	byID         map[string]*opsDelivery
	firstError   error
	lastSequence int64
	committed    int
}

// TestNATSProcessOperations is an explicit, bounded development screen. Normal
// repository gates only compile it; they never start the load campaign.
func TestNATSProcessOperations(t *testing.T) {
	if os.Getenv("GOMESSENGER_NATS_OPERATIONS") != "1" {
		t.Skip("set GOMESSENGER_NATS_OPERATIONS=1 for the opt-in process screen")
	}
	reportPath := os.Getenv("GOMESSENGER_NATS_OPERATIONS_REPORT")
	if reportPath == "" {
		t.Fatal("GOMESSENGER_NATS_OPERATIONS_REPORT is required to retain evidence")
	}
	evidence, err := opsPrepareEvidence(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	report := opsCampaignReport{
		Scope:     "checkout/direct-NATS/shared-SQLite/fixed-rate-operations; not PostgreSQL, Outbox, production or capacity",
		StartedAt: time.Now().UTC(), GoVersion: runtime.Version(), CPUCount: runtime.NumCPU(),
	}
	t.Cleanup(func() {
		if err := evidence.ledgers.Close(); err != nil {
			t.Errorf("close evidence directory: %v", err)
		}
		report.FinishedAt = time.Now().UTC()
		if t.Failed() {
			report.Passed = false
			if report.Failure == "" {
				report.Failure = "test or owned-resource cleanup failed"
			}
		}
		if err := opsWriteReport(evidence.report, report); err != nil {
			t.Errorf("retain process screen report: %v", err)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), opsCampaignTimeout)
	defer cancel()
	if info, ok := debug.ReadBuildInfo(); ok {
		report.BuildInfo = info.String()
	}
	hash, err := opsExecutableHash()
	if err != nil {
		report.Failure = err.Error()
		t.Fatal(err)
	}
	report.BinaryHash = hash
	fixture, err := opsNewFixture(t, ctx, &report, evidence)
	if err == nil {
		err = fixture.campaign(ctx)
	}
	if err != nil {
		report.Failure = err.Error()
		t.Fatal(err)
	}
	report.Passed = true
	t.Logf("bounded NATS process screen passed; evidence: %s", reportPath)
}

func opsNewFixture(t *testing.T, ctx context.Context, report *opsCampaignReport, evidence *opsEvidence) (*opsFixture, error) {
	t.Helper()
	root := t.TempDir()
	f := &opsFixture{
		databasePath: filepath.Join(root, "inbox.db"), report: report, evidence: evidence,
		expected: make(map[string]opsExpectedEffect),
	}
	t.Cleanup(func() {
		for index := len(f.children) - 1; index >= 0; index-- {
			if err := f.children[index].kill(); err != nil {
				t.Errorf("clean child process: %v", err)
			}
		}
	})
	database, err := opsOpenDatabase(ctx, f.databasePath)
	if err != nil {
		return nil, err
	}
	f.database = database
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := database.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
		return nil, err
	}
	if err := inboxsqlite.Migrate(ctx, database); err != nil {
		return nil, err
	}
	if _, err := database.ExecContext(ctx, opsEffectSchema); err != nil {
		return nil, err
	}
	f.brokerConfig = opsProcessConfig{Role: opsBrokerRole, Port: -1, Store: filepath.Join(root, "jetstream")}
	f.broker, err = f.start(ctx, f.brokerConfig)
	if err != nil {
		return nil, err
	}
	parsedURL, err := url.Parse(f.broker.ready.URL)
	if err != nil {
		return nil, err
	}
	f.brokerConfig.Port, err = strconv.Atoi(parsedURL.Port())
	if err != nil {
		return nil, err
	}
	connection := connectNATS(t, f.broker.ready.URL)
	f.js, err = jetstream.New(connection)
	if err != nil {
		return nil, err
	}
	stream := natsadapter.DevStream(opsStream, opsNamespace+".command.>")
	stream.Storage = jetstream.FileStorage
	dlq := natsadapter.DevDLQStream(opsStream+"_DLQ", opsNamespace+".dlq")
	dlq.Storage = jetstream.FileStorage
	if _, err := natsadapter.ApplyTopology(ctx, connection, natsadapter.Topology{
		SpecVersion: natsadapter.TopologySpecVersion, Streams: []natsadapter.StreamSpec{stream, dlq},
	}); err != nil {
		return nil, err
	}
	f.route, err = natsadapter.NewRoute(connection, natsadapter.RouteConfig{
		Name: "process-screen", Namespace: opsNamespace, WireMode: natsadapter.WireNative,
	})
	return f, err
}

func (f *opsFixture) start(ctx context.Context, config opsProcessConfig) (*opsChild, error) {
	child, err := opsStartChild(ctx, config)
	if child != nil {
		f.children = append(f.children, child)
	}
	if err == nil {
		f.report.Processes = append(f.report.Processes, child.ready)
	}
	return child, err
}

func (f *opsFixture) addConsumer(ctx context.Context, concurrency int) error {
	child, err := f.start(ctx, opsProcessConfig{
		Role: opsConsumerRole, URL: f.broker.ready.URL, Database: f.databasePath, Concurrency: concurrency,
	})
	if err == nil {
		f.consumers = append(f.consumers, child)
	}
	return err
}

func (f *opsFixture) campaign(ctx context.Context) error {
	for range 2 {
		if err := f.addConsumer(ctx, 1); err != nil {
			return err
		}
	}
	stage, err := f.runStage(ctx, "two-consumer-steady", opsSteadyRate, false)
	f.report.Stages = append(f.report.Stages, stage)
	if err != nil {
		return err
	}
	if err := opsRequireSteadyAcceptance(stage); err != nil {
		return err
	}
	// Recovery keeps headroom without searching for a capacity frontier.
	stage, err = f.runStage(ctx, "rolling-and-persistent-restart", opsFaultRate, true)
	f.report.Stages = append(f.report.Stages, stage)
	return err
}

func opsRequireSteadyAcceptance(stage opsStageReport) error {
	if !stage.IntegrityPassed || !stage.Sustainable {
		return fmt.Errorf("fixed-rate steady acceptance failed at %d/s: %v; no lower-rate retry", stage.Rate, stage.Reasons)
	}
	return nil
}

func (f *opsFixture) runStage(ctx context.Context, name string, rate int, faults bool) (result opsStageReport, runErr error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	result = opsStageReport{Name: name, Replicas: len(f.consumers), Rate: rate, DurationSeconds: opsLoadDuration.Seconds()}
	defer func() {
		if runErr != nil {
			result.Failure = runErr.Error()
		}
	}()
	before, err := f.health(ctx)
	if err != nil {
		return result, err
	}
	state := &opsLoadState{byID: make(map[string]*opsDelivery)}
	defer func() {
		// This runs after the join defer, including on an early failure.
		if err := f.retainStageLedger(&result, state); err != nil {
			runErr = errors.Join(runErr, err)
		}
	}()
	if err := f.database.QueryRowContext(ctx,
		"SELECT COALESCE(MAX(sequence), 0) FROM process_effects").Scan(&state.lastSequence); err != nil {
		return result, err
	}
	var anchor *jetstream.RawStreamMsg
	if faults {
		// Previous-stage publishers joined with PubAck and reconciliation.
		// Capture the anchor before any fault-stage publication can start.
		anchor, err = f.capturePersistenceAnchor(ctx)
		if err != nil {
			return result, err
		}
	}
	result.Expected = int(opsLoadDuration.Seconds()) * rate
	result.StartedAt = time.Now()
	result.LoadEndedAt = result.StartedAt.Add(opsLoadDuration)
	loadDone := make(chan struct{})
	go func(startedAt, endedAt time.Time) {
		defer close(loadDone)
		f.generate(ctx, state, rate, startedAt, endedAt, faults)
	}(result.StartedAt, result.LoadEndedAt)
	faultDone := make(chan error, 1)
	if faults {
		go func(startedAt time.Time) { faultDone <- f.injectFaults(ctx, startedAt, anchor) }(result.StartedAt)
	} else {
		faultDone <- nil
	}
	joinedFault := false
	defer func() {
		cancel()
		<-loadDone // Closed channels can be observed again; always join.
		if !joinedFault {
			<-faultDone
		}
	}()
	if err := f.waitForStageDrain(ctx, state, &result, faults, loadDone); err != nil {
		return result, err
	}
	if err := <-faultDone; err != nil {
		joinedFault = true
		return result, err
	}
	joinedFault = true
	if err := f.reconcile(ctx, state); err != nil {
		return result, err
	}
	after, err := f.health(ctx)
	if err != nil {
		return result, err
	}
	result.HandleErrors = after.HandleErrors - before.HandleErrors
	result.AckErrors = after.AckErrors - before.AckErrors
	result.ObservedDuplicates = after.Duplicates - before.Duplicates
	if err := opsSummarizeTiming(&result, state); err != nil {
		return result, err
	}
	var requiredPIDs []int
	for _, child := range f.consumers {
		requiredPIDs = append(requiredPIDs, child.ready.PID)
	}
	var restartedAt time.Time
	if faults {
		restartedAt = f.report.Events[len(f.report.Events)-1].At
	}
	if err := opsVerifyWorkerProgress(state.byID, requiredPIDs, restartedAt); err != nil {
		return result, err
	}
	result.IntegrityPassed = true
	if faults {
		result.Reasons = []string{"fault-recovery phase; steady-state capacity criteria do not apply"}
		return result, nil
	}
	result.Reasons = opsSaturationReasons(result)
	result.Sustainable = len(result.Reasons) == 0
	return result, nil
}

func (f *opsFixture) retainStageLedger(result *opsStageReport, state *opsLoadState) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	result.Attempted, result.Committed = len(state.byID), state.committed
	result.GeneratorDropped = max(0, result.Expected-result.Attempted)
	result.Confirmed = 0
	result.EffectsByProcess = make(map[int]int)
	for _, delivery := range state.byID {
		if !delivery.AckAt.IsZero() {
			result.Confirmed++
		}
		if delivery.WorkerPID != 0 {
			result.EffectsByProcess[delivery.WorkerPID]++
		}
	}
	name := result.Name + ".jsonl"
	result.Ledger = filepath.Join("tmp", f.evidence.ledgerPath, name)
	if err := opsWriteLedger(f.evidence.ledgers, name, state.byID); err != nil {
		return fmt.Errorf("retain message ledger: %w", err)
	}
	return nil
}

func (f *opsFixture) capturePersistenceAnchor(ctx context.Context) (*jetstream.RawStreamMsg, error) {
	stream, err := f.js.Stream(ctx, opsStream)
	if err != nil {
		return nil, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, err
	}
	return stream.GetMsg(ctx, info.State.LastSeq)
}

func (f *opsFixture) waitForStageDrain(
	ctx context.Context, state *opsLoadState, result *opsStageReport, faults bool, loadDone <-chan struct{},
) error {
	ticker := time.NewTicker(opsPollInterval)
	defer ticker.Stop()
	var middleLag, finalLag int
	var middleAt, lastLoadSample time.Time
	for now := range ticker.C {
		if err := f.observe(ctx, state, now); err != nil {
			return err
		}
		state.mu.Lock()
		lag := len(state.byID) - state.committed
		state.mu.Unlock()
		if now.Before(result.LoadEndedAt) {
			if middleAt.IsZero() && !now.Before(result.StartedAt.Add(opsLoadDuration/2)) {
				middleLag, middleAt = lag, now
			}
			finalLag, lastLoadSample = lag, now
		}
		pending, waiting, redelivered, err := f.brokerSnapshot(ctx)
		if err != nil && !faults {
			return err
		}
		if err == nil {
			result.MaxAckPending = max(result.MaxAckPending, pending)
			result.MaxRedelivered = max(result.MaxRedelivered, redelivered)
			if pending > opsAckWindow {
				return fmt.Errorf("global ACK window exceeded: %d", pending)
			}
		}
		if err == nil && pending == 0 && waiting == 0 && !now.Before(result.LoadEndedAt) && opsLoadSettled(state, loadDone) {
			result.DrainSeconds = max(0, now.Sub(result.LoadEndedAt).Seconds())
			break
		}
		if now.After(result.LoadEndedAt.Add(3 * opsDrainSLO)) {
			return errors.New("pipeline did not reconcile within bounded recovery; do not lower load to mask missing effects")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if lastLoadSample.After(middleAt) && !middleAt.IsZero() {
		result.LagGrowthPerSecond = float64(finalLag-middleLag) / lastLoadSample.Sub(middleAt).Seconds()
	}
	return nil
}

func opsLoadSettled(state *opsLoadState, loadDone <-chan struct{}) bool {
	select {
	case <-loadDone:
		state.mu.Lock()
		defer state.mu.Unlock()
		return state.committed == len(state.byID)
	default:
		return false
	}
}

func opsSummarizeTiming(result *opsStageReport, state *opsLoadState) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.firstError != nil {
		return state.firstError
	}
	result.Attempted, result.Committed = len(state.byID), state.committed
	result.GeneratorDropped = max(0, result.Expected-result.Attempted)
	var observed, puback []time.Duration
	for _, delivery := range state.byID {
		if delivery.AckAt.IsZero() || delivery.ObservedAt.IsZero() {
			return errors.New("missing broker confirmation or observed durable effect")
		}
		if delivery.AckAt.Before(delivery.OfferedAt) || delivery.ObservedAt.Before(delivery.OfferedAt) {
			return errors.New("parent clock moved backwards; timing sample is invalid")
		}
		result.Confirmed++
		if delivery.AckAt.Before(result.LoadEndedAt) {
			result.ConfirmedInWindow++
		}
		if delivery.ObservedAt.Before(result.LoadEndedAt) {
			result.ObservedInWindow++
		}
		observed = append(observed, delivery.ObservedAt.Sub(delivery.OfferedAt))
		puback = append(puback, delivery.AckAt.Sub(delivery.OfferedAt))
	}
	result.ObservedP95Millis, result.PubAckP95Millis = opsP95(observed), opsP95(puback)
	return nil
}

func (f *opsFixture) generate(ctx context.Context, state *opsLoadState, rate int, started, ended time.Time, retry bool) {
	jobs := make(chan *opsDelivery, 64)
	var workers sync.WaitGroup
	for range 16 {
		workers.Go(func() {
			for delivery := range jobs {
				ackAt, err := f.publish(ctx, delivery, retry)
				state.mu.Lock()
				delivery.AckAt = ackAt
				if err != nil && state.firstError == nil {
					state.firstError = err
				}
				state.mu.Unlock()
			}
		})
	}
	defer func() { close(jobs); workers.Wait() }()
	expected := int(ended.Sub(started).Seconds()) * rate
	for index := range expected {
		offered := started.Add(time.Duration(index) * time.Second / time.Duration(rate))
		if err := opsWaitUntil(ctx, offered); err != nil {
			state.mu.Lock()
			state.firstError = err
			state.mu.Unlock()
			return
		}
		// Short scheduler jitter may catch up, but a stalled generator never
		// converts an arbitrarily long pause into an unbounded burst.
		if time.Since(offered) > opsPollInterval {
			continue
		}
		f.jobID++
		id, err := messenger.UUIDv7Generator().New()
		if err != nil {
			state.mu.Lock()
			state.firstError = err
			state.mu.Unlock()
			return
		}
		info := opsDescriptor().Info()
		data, err := messenger.EncodeCommandEnvelope(opsDescriptor(), messenger.Metadata{
			ID: id, Kind: info.Kind, Name: info.Name, SchemaVersion: info.SchemaVersion,
			ContentType: info.ContentType, Schema: info.Schema, Source: opsSource,
			Time: offered.UTC(), CorrelationID: id,
		}, processPayload{JobID: f.jobID})
		if err != nil {
			state.mu.Lock()
			state.firstError = err
			state.mu.Unlock()
			return
		}
		delivery := &opsDelivery{JobID: f.jobID, Envelope: data, OfferedAt: offered}
		state.mu.Lock()
		state.byID[id.String()] = delivery
		select {
		case jobs <- delivery:
		default:
			delete(state.byID, id.String())
		}
		state.mu.Unlock()
	}
}

func (f *opsFixture) publish(ctx context.Context, delivery *opsDelivery, retry bool) (time.Time, error) {
	deadline := time.Now().Add(30 * time.Second)
	for {
		attemptContext, cancel := context.WithTimeout(ctx, 3*time.Second)
		_, err := f.route.PublishEnvelope(attemptContext, delivery.Envelope)
		cancel()
		if err == nil {
			return time.Now(), nil
		}
		if !retry || time.Now().After(deadline) || ctx.Err() != nil {
			return time.Time{}, fmt.Errorf("unconfirmed publication: %w", err)
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return time.Time{}, ctx.Err()
		}
	}
}

func (f *opsFixture) observe(ctx context.Context, state *opsLoadState, observedAt time.Time) error {
	rows, err := f.database.QueryContext(ctx,
		`SELECT sequence, message_id, job_id, executions, worker_pid
		FROM process_effects WHERE sequence > ? ORDER BY sequence`, state.lastSequence)
	if err != nil {
		return err
	}
	defer rows.Close()
	state.mu.Lock()
	defer state.mu.Unlock()
	for rows.Next() {
		var sequence, jobID int64
		var id string
		var executions int
		var workerPID int
		if err := rows.Scan(&sequence, &id, &jobID, &executions, &workerPID); err != nil {
			return err
		}
		delivery := state.byID[id]
		if !opsValidEffect(delivery, jobID, executions) || !delivery.ObservedAt.IsZero() {
			return fmt.Errorf("unexpected durable effect: id=%s job=%d executions=%d", id, jobID, executions)
		}
		delivery.ObservedAt = maxTime(observedAt, time.Now())
		delivery.WorkerPID = workerPID
		state.lastSequence, state.committed = sequence, state.committed+1
	}
	return rows.Err()
}

func (f *opsFixture) reconcile(ctx context.Context, state *opsLoadState) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	for id, delivery := range state.byID {
		if _, exists := f.expected[id]; exists {
			return fmt.Errorf("reused campaign message ID %s", id)
		}
		if !slices.ContainsFunc(f.report.Processes, func(process opsProcessEvent) bool {
			return process.Role == opsConsumerRole && process.PID == delivery.WorkerPID
		}) {
			return fmt.Errorf("effect %s has an unverified consumer process %d", id, delivery.WorkerPID)
		}
		f.expected[id] = opsExpectedEffect{JobID: delivery.JobID, WorkerPID: delivery.WorkerPID}
	}
	// Recheck the entire campaign, including old rows whose execution counter
	// could change without increasing the sequence used by the timing poller.
	if err := opsReconcileEffects(ctx, f.database, f.expected); err != nil {
		return err
	}
	stream, err := f.js.Stream(ctx, opsStream+"_DLQ")
	if err != nil {
		return err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return err
	}
	if info.State.Msgs != 0 {
		return fmt.Errorf("unexpected DLQ records: %d", info.State.Msgs)
	}
	return nil
}

func opsReconcileEffects(ctx context.Context, database *sql.DB, expected map[string]opsExpectedEffect) error {
	rows, err := database.QueryContext(ctx, `SELECT e.message_id, e.job_id, e.executions, i.message_id, e.worker_pid
		FROM process_effects e LEFT JOIN gomessenger_inbox i ON i.message_id=e.message_id
		AND i.consumer_id=? AND i.source=? AND i.completed_at IS NOT NULL
		ORDER BY e.job_id`, opsConsumer, opsSource)
	if err != nil {
		return err
	}
	defer rows.Close()
	seen := make(map[string]bool)
	for rows.Next() {
		var id string
		var inboxID sql.NullString
		var jobID int64
		var executions int
		var workerPID int
		if err := rows.Scan(&id, &jobID, &executions, &inboxID, &workerPID); err != nil {
			return err
		}
		want, exists := expected[id]
		if !exists || want.JobID != jobID || executions != 1 || !inboxID.Valid || seen[id] {
			return fmt.Errorf("identity reconciliation failed for %s", id)
		}
		if want.WorkerPID != workerPID {
			return fmt.Errorf("effect %s changed consumer process to %d", id, workerPID)
		}
		seen[id] = true
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(seen) != len(expected) {
		return fmt.Errorf("missing effects: have %d, want %d", len(seen), len(expected))
	}
	return nil
}

func opsVerifyWorkerProgress(deliveries map[string]*opsDelivery, requiredPIDs []int, restartedAt time.Time) error {
	seen := make(map[int]bool)
	resumed := restartedAt.IsZero()
	for _, delivery := range deliveries {
		if delivery.AckAt.IsZero() || delivery.ObservedAt.IsZero() {
			continue
		}
		seen[delivery.WorkerPID] = true
		if delivery.OfferedAt.After(restartedAt) {
			resumed = true
		}
	}
	for _, pid := range requiredPIDs {
		if !seen[pid] {
			return fmt.Errorf("ready consumer process %d committed no confirmed work", pid)
		}
	}
	if !resumed {
		return errors.New("no newly offered work confirmed and committed after broker restart")
	}
	return nil
}

func opsValidEffect(delivery *opsDelivery, jobID int64, executions int) bool {
	return delivery != nil && delivery.JobID == jobID && executions == 1
}

func (f *opsFixture) brokerSnapshot(ctx context.Context) (int, uint64, int, error) {
	probeContext, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	consumer, err := f.js.Consumer(probeContext, opsStream, opsConsumer)
	if err != nil {
		return 0, 0, 0, err
	}
	info, err := consumer.Info(probeContext)
	if err != nil {
		return 0, 0, 0, err
	}
	if info.Config.MaxAckPending != opsAckWindow {
		return 0, 0, 0, errors.New("durable ACK-window configuration changed")
	}
	return info.NumAckPending, info.NumPending, info.NumRedelivered, nil
}

func (f *opsFixture) health(ctx context.Context) (opsProcessEvent, error) {
	total := f.retired
	for _, child := range f.consumers {
		probeContext, cancel := context.WithTimeout(ctx, 5*time.Second)
		value, err := child.health(probeContext)
		cancel()
		if err != nil {
			return total, err
		}
		total.HandleErrors += value.HandleErrors
		total.AckErrors += value.AckErrors
		total.Duplicates += value.Duplicates
	}
	return total, nil
}

func (f *opsFixture) injectFaults(ctx context.Context, started time.Time, anchor *jetstream.RawStreamMsg) error {
	if err := opsWaitUntil(ctx, started.Add(opsLoadDuration/4)); err != nil {
		return err
	}
	old := f.consumers[0]
	stopContext, stopCancel := context.WithTimeout(ctx, 15*time.Second)
	err := old.stop(stopContext)
	stopCancel()
	if err != nil {
		return err
	}
	stopped, err := old.await(ctx, "stopped")
	if err != nil {
		return err
	}
	f.retired.HandleErrors += stopped.HandleErrors
	f.retired.AckErrors += stopped.AckErrors
	f.retired.Duplicates += stopped.Duplicates
	replacement, err := f.start(ctx, opsProcessConfig{
		Role: opsConsumerRole, URL: f.broker.ready.URL, Database: f.databasePath, Concurrency: 2,
	})
	if err != nil {
		return err
	}
	if replacement.ready.PID == old.ready.PID {
		return errors.New("rolling replacement reused the old PID; process identity is ambiguous")
	}
	f.consumers[0] = replacement
	f.report.Events = append(f.report.Events, opsLifecycleEvent{
		Action: "rolling-replacement-local-concurrency-1-to-2-global-window-32", At: time.Now(),
		OldPID: old.ready.PID, NewPID: replacement.ready.PID,
	})
	if err := opsWaitUntil(ctx, started.Add(opsLoadDuration/2)); err != nil {
		return err
	}
	hash := sha256.Sum256(anchor.Data)
	oldBroker := f.broker
	if err := oldBroker.kill(); err != nil {
		return err
	}
	f.broker, err = f.start(ctx, f.brokerConfig)
	if err != nil {
		return err
	}
	if f.broker.ready.PID == oldBroker.ready.PID {
		return errors.New("broker restart reused the old PID; process identity is ambiguous")
	}
	if f.broker.ready.URL != oldBroker.ready.URL {
		return errors.New("persistent broker restart changed its address")
	}
	recoveryContext, recoveryCancel := context.WithTimeout(ctx, 20*time.Second)
	defer recoveryCancel()
	for {
		stream, err := f.js.Stream(recoveryContext, opsStream)
		if err == nil {
			var retained *jetstream.RawStreamMsg
			retained, err = stream.GetMsg(recoveryContext, anchor.Sequence)
			if err == nil && sha256.Sum256(retained.Data) != hash {
				return errors.New("persisted message changed across broker restart")
			}
		}
		if err == nil {
			break
		}
		if err := opsWaitUntil(recoveryContext, time.Now().Add(100*time.Millisecond)); err != nil {
			return err
		}
	}
	f.report.Events = append(f.report.Events, opsLifecycleEvent{
		Action: "broker-process-kill-and-restart-same-store", At: time.Now(),
		OldPID: oldBroker.ready.PID, NewPID: f.broker.ready.PID,
		Sequence: anchor.Sequence, RecordHash: hex.EncodeToString(hash[:]),
	})
	return nil
}

func opsWaitUntil(ctx context.Context, at time.Time) error {
	timer := time.NewTimer(max(0, time.Until(at)))
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func maxTime(first, second time.Time) time.Time {
	if first.After(second) {
		return first
	}
	return second
}

func opsP95(values []time.Duration) float64 {
	if len(values) == 0 {
		return 0
	}
	slices.Sort(values)
	return float64(values[int(math.Ceil(float64(len(values))*0.95))-1]) / float64(time.Millisecond)
}

func opsSaturationReasons(stage opsStageReport) []string {
	var reasons []string
	if stage.GeneratorDropped != 0 {
		reasons = append(reasons, "generator missed scheduled load")
	}
	if float64(stage.ConfirmedInWindow) < 0.99*float64(stage.Expected) {
		reasons = append(reasons, "PubAck throughput below 99%")
	}
	if float64(stage.ObservedInWindow) < 0.99*float64(stage.Expected) {
		reasons = append(reasons, "observed durable throughput below 99%")
	}
	if stage.ObservedP95Millis > 2000 {
		reasons = append(reasons, "observed commit p95 upper bound above 2s")
	}
	if stage.DrainSeconds > opsDrainSLO.Seconds() {
		reasons = append(reasons, "drain exceeded 60s")
	}
	if stage.LagGrowthPerSecond > 0.01*float64(stage.Rate) {
		reasons = append(reasons, "second-half lag growth above 1%")
	}
	if stage.HandleErrors != 0 || stage.AckErrors != 0 || stage.MaxRedelivered != 0 || stage.ObservedDuplicates != 0 {
		reasons = append(reasons, "steady-state delivery errors or redelivery")
	}
	return reasons
}

func opsPrepareEvidence(path string) (evidence *opsEvidence, prepareErr error) {
	base, err := filepath.Abs("../../tmp")
	if err != nil {
		return nil, err
	}
	relative, err := opsReportPath(base, path)
	if err != nil {
		return nil, err
	}
	checkout, err := os.OpenRoot("../..")
	if err != nil {
		return nil, err
	}
	var root *os.Root
	defer func() {
		if root != nil {
			prepareErr = errors.Join(prepareErr, root.Close())
		}
		prepareErr = errors.Join(prepareErr, checkout.Close())
		if prepareErr != nil && evidence != nil {
			prepareErr = errors.Join(prepareErr, evidence.report.Close(), evidence.ledgers.Close())
			evidence = nil
		}
	}()
	root, err = checkout.OpenRoot("tmp")
	if err != nil {
		return nil, err
	}
	return opsReserveReport(root, relative)
}

func opsReportPath(base, path string) (string, error) {
	if !filepath.IsAbs(base) || !filepath.IsAbs(path) {
		return "", errors.New("report path and evidence root must be absolute")
	}
	relative, err := filepath.Rel(base, path)
	if err != nil {
		return "", err
	}
	if !filepath.IsLocal(relative) || relative == "." {
		return "", errors.New("report path must name a file beneath checkout/tmp")
	}
	return relative, nil
}

func opsReserveReport(root *os.Root, path string) (*opsEvidence, error) {
	if !filepath.IsLocal(path) || path == "." {
		return nil, errors.New("report path must remain beneath the evidence root")
	}
	if err := root.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, err
	}
	if _, err := root.Lstat(path + ".records"); err == nil || !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("evidence ledger path must be new: %s: %w", path+".records", errors.Join(os.ErrExist, err))
	}
	file, err := root.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	if err := root.Mkdir(path+".records", 0o750); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	ledgers, err := root.OpenRoot(path + ".records")
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}
	return &opsEvidence{report: file, ledgers: ledgers, ledgerPath: path + ".records"}, nil
}

func opsWriteReport(file *os.File, report opsCampaignReport) (writeErr error) {
	defer func() { writeErr = errors.Join(writeErr, file.Close()) }()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		return err
	}
	return file.Sync()
}

func opsWriteLedger(root *os.Root, name string, deliveries map[string]*opsDelivery) (writeErr error) {
	if !filepath.IsLocal(name) || filepath.Base(name) != name || name == "." {
		return errors.New("ledger name must be a single local filename")
	}
	file, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer func() { writeErr = errors.Join(writeErr, file.Close()) }()
	encoder := json.NewEncoder(file)
	ids := make([]string, 0, len(deliveries))
	for id := range deliveries {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		delivery := deliveries[id]
		hash := sha256.Sum256(delivery.Envelope)
		record := struct {
			MessageID        string    `json:"messageId"`
			JobID            int64     `json:"jobId"`
			EnvelopeSHA256   string    `json:"envelopeSha256"`
			OfferedAt        time.Time `json:"offeredAt"`
			PubAckAt         time.Time `json:"pubAckAt"`
			ObservedCommitAt time.Time `json:"observedCommitAt"`
			WorkerPID        int       `json:"workerPid"`
		}{id, delivery.JobID, hex.EncodeToString(hash[:]), delivery.OfferedAt, delivery.AckAt, delivery.ObservedAt, delivery.WorkerPID}
		if err := encoder.Encode(record); err != nil {
			return err
		}
	}
	return file.Sync()
}

func opsExecutableHash() (string, error) {
	path, err := os.Executable()
	if err != nil {
		return "", err
	}
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	_, copyErr := io.Copy(hash, file)
	if err := errors.Join(copyErr, file.Close()); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
