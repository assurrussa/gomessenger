package e2e_test

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	inboxsqlite "github.com/assurrussa/gomessenger/adapters/inbox/sqlite"
)

func TestNATSProcessFixedAcceptancePolicy(t *testing.T) {
	if got := [2]int{opsSteadyRate, opsFaultRate}; got != [2]int{125, 62} {
		t.Fatalf("fixed acceptance rates = %v", got)
	}
	if opsLoadDuration != 2*time.Minute || opsCampaignTimeout != 15*time.Minute {
		t.Fatal("fixed acceptance time bounds changed")
	}
	for _, test := range []struct {
		name        string
		integrity   bool
		sustainable bool
		wantError   bool
	}{
		{name: "accepted", integrity: true, sustainable: true},
		{name: "overload rejected", integrity: true, wantError: true},
		{name: "integrity failure rejected", sustainable: true, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			stage := opsStageReport{Rate: opsSteadyRate, IntegrityPassed: test.integrity, Sustainable: test.sustainable}
			if err := opsRequireSteadyAcceptance(stage); (err != nil) != test.wantError {
				t.Fatalf("steady acceptance error = %v, wantError %t", err, test.wantError)
			}
		})
	}
}

func TestNATSProcessChildEnvironmentReplacesPrivateSettings(t *testing.T) {
	input := []string{"PATH=/test", "GOMAXPROCS=8", opsHelperEnvironment + "=old", "OTHER=kept"}
	got := opsChildEnvironment(input, "candidate")
	want := []string{"PATH=/test", "OTHER=kept", opsHelperEnvironment + "=candidate", "GOMAXPROCS=1"}
	if !slices.Equal(got, want) {
		t.Fatalf("environment = %v, want %v", got, want)
	}
	if input[1] != "GOMAXPROCS=8" {
		t.Fatal("source environment was mutated")
	}
}

func TestNATSProcessEffectIdentityRejectsMissingDuplicateAndWrongJob(t *testing.T) {
	for _, test := range []struct {
		name       string
		delivery   *opsDelivery
		jobID      int64
		executions int
		want       bool
	}{
		{name: "missing", jobID: 7, executions: 1},
		{name: "duplicate", delivery: &opsDelivery{JobID: 7}, jobID: 7, executions: 2},
		{name: "wrong job", delivery: &opsDelivery{JobID: 7}, jobID: 8, executions: 1},
		{name: "one expected effect", delivery: &opsDelivery{JobID: 7}, jobID: 7, executions: 1, want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := opsValidEffect(test.delivery, test.jobID, test.executions); got != test.want {
				t.Fatalf("valid effect = %t, want %t", got, test.want)
			}
		})
	}
}

func TestNATSProcessSaturationCriteria(t *testing.T) {
	baseline := opsStageReport{
		Rate: 1000, Expected: 120000, ConfirmedInWindow: 120000, ObservedInWindow: 120000,
		ObservedP95Millis: 100, DrainSeconds: 1, IntegrityPassed: true,
	}
	if reasons := opsSaturationReasons(baseline); len(reasons) != 0 {
		t.Fatalf("baseline reasons: %v", reasons)
	}
	for _, test := range []struct {
		name   string
		change func(*opsStageReport)
	}{
		{name: "generator", change: func(stage *opsStageReport) { stage.GeneratorDropped = 1 }},
		{name: "PubAck rate", change: func(stage *opsStageReport) { stage.ConfirmedInWindow = 118799 }},
		{name: "observed rate", change: func(stage *opsStageReport) { stage.ObservedInWindow = 118799 }},
		{name: "latency", change: func(stage *opsStageReport) { stage.ObservedP95Millis = 2001 }},
		{name: "drain", change: func(stage *opsStageReport) { stage.DrainSeconds = 61 }},
		{name: "lag", change: func(stage *opsStageReport) { stage.LagGrowthPerSecond = 11 }},
		{name: "handler error", change: func(stage *opsStageReport) { stage.HandleErrors = 1 }},
		{name: "ACK error", change: func(stage *opsStageReport) { stage.AckErrors = 1 }},
		{name: "redelivery", change: func(stage *opsStageReport) { stage.MaxRedelivered = 1 }},
		{name: "duplicate delivery", change: func(stage *opsStageReport) { stage.ObservedDuplicates = 1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := baseline
			test.change(&candidate)
			if len(opsSaturationReasons(candidate)) == 0 {
				t.Fatal("unsustainable sample was accepted")
			}
		})
	}
}

func TestNATSProcessP95AndObservedTimestamp(t *testing.T) {
	if got := opsP95(nil); got != 0 {
		t.Fatalf("empty p95 = %v", got)
	}
	values := []time.Duration{3 * time.Millisecond, time.Millisecond, 2 * time.Millisecond}
	if got := opsP95(values); got != 3 {
		t.Fatalf("p95 = %v, want 3ms", got)
	}
	before := time.Unix(1, 0)
	after := before.Add(time.Second)
	if !maxTime(before, after).Equal(after) || !maxTime(after, before).Equal(after) {
		t.Fatal("observed timestamp precedes SQL visibility")
	}
}

func TestNATSProcessLoadSettledRequiresProducerJoinAndAllEffects(t *testing.T) {
	state := &opsLoadState{byID: map[string]*opsDelivery{"expected": {JobID: 1}}}
	done := make(chan struct{})
	state.committed = 1
	if opsLoadSettled(state, done) {
		t.Fatal("unjoined producer was treated as settled")
	}
	state.committed = 0
	close(done)
	if opsLoadSettled(state, done) {
		t.Fatal("missing durable effect was treated as settled")
	}
	state.committed = 1
	for range 2 {
		if !opsLoadSettled(state, done) {
			t.Fatal("closed completion channel cannot be observed repeatedly")
		}
	}
}

func TestNATSProcessExitedChildRetainsError(t *testing.T) {
	cause := errors.New("child failed")
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "failed exit", err: cause},
		{name: "clean exit without response"},
	} {
		t.Run(test.name, func(t *testing.T) {
			child := &opsChild{events: make(chan opsProcessEvent), done: make(chan struct{}), err: test.err}
			close(child.done)
			if _, err := child.await(t.Context(), "ready"); err == nil || (test.err != nil && !errors.Is(err, test.err)) {
				t.Fatalf("exit error = %v, want non-nil error retaining %v", err, test.err)
			}
		})
	}
}

func TestNATSProcessCampaignReconciliationRejectsPriorStageMutation(t *testing.T) {
	database, err := opsOpenDatabase(t.Context(), filepath.Join(t.TempDir(), "effects.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := inboxsqlite.Migrate(t.Context(), database); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), opsEffectSchema); err != nil {
		t.Fatal(err)
	}
	expected := map[string]opsExpectedEffect{
		"prior-stage":   {JobID: 1, WorkerPID: 101},
		"current-stage": {JobID: 2, WorkerPID: 102},
	}
	for id, effect := range expected {
		if _, err := database.ExecContext(t.Context(), `INSERT INTO process_effects
			(message_id, job_id, worker_pid, executions) VALUES (?, ?, ?, 1)`, id, effect.JobID, effect.WorkerPID); err != nil {
			t.Fatal(err)
		}
		if _, err := database.ExecContext(t.Context(), `INSERT INTO gomessenger_inbox
			(consumer_id, source, message_id, fingerprint, created_at, completed_at)
			VALUES (?, ?, ?, x'01', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, opsConsumer, opsSource, id); err != nil {
			t.Fatal(err)
		}
	}
	if err := opsReconcileEffects(t.Context(), database, expected); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(),
		"UPDATE process_effects SET executions=2 WHERE message_id='prior-stage'"); err != nil {
		t.Fatal(err)
	}
	if err := opsReconcileEffects(t.Context(), database, expected); err == nil {
		t.Fatal("duplicate effect from a prior stage was accepted")
	}
}

func TestNATSProcessWorkerProgressRequiresBothReplicasAndPostRestartWork(t *testing.T) {
	restarted := time.Now()
	deliveries := map[string]*opsDelivery{
		"first": {WorkerPID: 101, OfferedAt: restarted.Add(-time.Second), AckAt: restarted, ObservedAt: restarted},
	}
	if err := opsVerifyWorkerProgress(deliveries, []int{101, 102}, time.Time{}); err == nil {
		t.Fatal("idle second consumer was accepted")
	}
	deliveries["second"] = &opsDelivery{
		WorkerPID: 102, OfferedAt: restarted.Add(-time.Second), AckAt: restarted, ObservedAt: restarted,
	}
	if err := opsVerifyWorkerProgress(deliveries, []int{101, 102}, restarted); err == nil {
		t.Fatal("only pre-restart offers were accepted as resumed work")
	}
	deliveries["second"].OfferedAt = restarted.Add(time.Second)
	deliveries["second"].AckAt = restarted.Add(2 * time.Second)
	deliveries["second"].ObservedAt = restarted.Add(2 * time.Second)
	if err := opsVerifyWorkerProgress(deliveries, []int{101, 102}, restarted); err != nil {
		t.Fatal(err)
	}
}

func TestNATSProcessReportReservationPreservesExistingEvidence(t *testing.T) {
	root := opsTestEvidenceRoot(t)
	const path = "report.json"
	evidence, err := opsReserveReport(root, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := evidence.ledgers.Close(); err != nil {
		t.Fatal(err)
	}
	if err := opsWriteReport(evidence.report, opsCampaignReport{Scope: "original"}); err != nil {
		t.Fatal(err)
	}
	original, err := root.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate, err := opsReserveReport(root, path); err == nil {
		_ = duplicate.report.Close()
		_ = duplicate.ledgers.Close()
		t.Fatal("existing report path was accepted")
	}
	after, err := root.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(original) {
		t.Fatal("existing evidence changed")
	}
	const existing = "existing.json"
	if err := root.WriteFile(existing, original, 0o600); err != nil {
		t.Fatal(err)
	}
	if duplicate, err := opsReserveReport(root, existing); err == nil {
		_ = duplicate.report.Close()
		_ = duplicate.ledgers.Close()
		t.Fatal("existing report without ledger directory was accepted")
	}
	after, err = root.ReadFile(existing)
	if err != nil || string(after) != string(original) {
		t.Fatalf("existing report-only evidence changed: %v", err)
	}
	const other = "other.json"
	if err := root.Mkdir(other+".records", 0o750); err != nil {
		t.Fatal(err)
	}
	if duplicate, err := opsReserveReport(root, other); err == nil {
		_ = duplicate.report.Close()
		_ = duplicate.ledgers.Close()
		t.Fatal("existing ledger path was accepted")
	}
	if _, err := root.Lstat(other); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected ledger reservation created report: %v", err)
	}
}

func TestNATSProcessReportPathsRejectTraversalAndSymlinkEscape(t *testing.T) {
	root := opsTestEvidenceRoot(t)
	base := root.Name()
	for _, path := range []string{"relative.json", base, filepath.Join(base, "..", "outside.json")} {
		if _, err := opsReportPath(base, path); err == nil {
			t.Fatalf("unsafe report path accepted: %s", path)
		}
	}
	path, err := opsReportPath(base, filepath.Join(base, "nested", "valid.json"))
	if err != nil || path != filepath.Join("nested", "valid.json") {
		t.Fatalf("valid report path = %q, error %v", path, err)
	}
	if evidence, err := opsReserveReport(root, "../outside.json"); err == nil {
		_ = evidence.report.Close()
		_ = evidence.ledgers.Close()
		t.Fatal("traversing evidence reservation accepted")
	}
	t.Run("symlink escape", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("Windows symlink creation requires separate system privileges")
		}
		outside := t.TempDir()
		if err := root.Symlink(outside, "escape"); err != nil {
			t.Fatal(err)
		}
		if err := root.Symlink(filepath.Join(outside, "missing"), "dangling.json.records"); err != nil {
			t.Fatal(err)
		}
		for _, path := range []string{"escape/report.json", "dangling.json"} {
			if evidence, err := opsReserveReport(root, path); err == nil {
				_ = evidence.report.Close()
				_ = evidence.ledgers.Close()
				t.Fatalf("unsafe evidence reservation accepted: %s", path)
			}
		}
		entries, err := os.ReadDir(outside)
		if err != nil || len(entries) != 0 {
			t.Fatalf("outside directory changed: entries=%v error=%v", entries, err)
		}
	})
}

func TestNATSProcessLedgerRootRetainsIdentityAndRejectsOverwrite(t *testing.T) {
	root := opsTestEvidenceRoot(t)
	evidence, err := opsReserveReport(root, "ledger-report.json")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := errors.Join(evidence.report.Close(), evidence.ledgers.Close()); err != nil {
			t.Error(err)
		}
	})
	deliveries := map[string]*opsDelivery{"expected": {JobID: 1}}
	t.Run("directory identity", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("Windows prevents renaming a directory retained by os.Root")
		}
		if err := root.Rename(evidence.ledgerPath, "retained-records"); err != nil {
			t.Fatal(err)
		}
		outside := t.TempDir()
		if err := root.Symlink(outside, evidence.ledgerPath); err != nil {
			t.Fatal(err)
		}
		if err := opsWriteLedger(evidence.ledgers, "identity.jsonl", deliveries); err != nil {
			t.Fatal(err)
		}
		if _, err := root.Stat("retained-records/identity.jsonl"); err != nil {
			t.Fatalf("owned directory lost: %v", err)
		}
		entries, err := os.ReadDir(outside)
		if err != nil || len(entries) != 0 {
			t.Fatalf("outside directory changed: entries=%v error=%v", entries, err)
		}
	})
	const name = "stage.jsonl"
	if err := opsWriteLedger(evidence.ledgers, name, deliveries); err != nil {
		t.Fatal(err)
	}
	original, err := evidence.ledgers.ReadFile(name)
	if err != nil || len(original) == 0 {
		t.Fatalf("owned directory lost: bytes=%d error=%v", len(original), err)
	}
	for _, path := range []string{name, "../outside.jsonl", "nested/stage.jsonl"} {
		if err := opsWriteLedger(evidence.ledgers, path, deliveries); err == nil {
			t.Fatalf("unsafe ledger write accepted: %s", path)
		}
	}
	after, err := evidence.ledgers.ReadFile(name)
	if err != nil || string(after) != string(original) {
		t.Fatalf("existing ledger changed: %v", err)
	}
}

func opsTestEvidenceRoot(t *testing.T) *os.Root {
	t.Helper()
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	})
	return root
}
