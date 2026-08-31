package capacity

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"example.com/gomessenger-durable-postgres-nats/internal/demo"
)

type artifacts struct {
	directory string
	samples   *os.File
	encoder   *json.Encoder
}

func newArtifacts(directory string) (*artifacts, error) {
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return nil, fmt.Errorf("create capacity artifact directory: %w", err)
	}
	samples, err := os.OpenFile(
		filepath.Join(directory, "samples.jsonl"),
		os.O_CREATE|os.O_TRUNC|os.O_WRONLY,
		0o640,
	)
	if err != nil {
		return nil, fmt.Errorf("create capacity samples: %w", err)
	}
	return &artifacts{directory: directory, samples: samples, encoder: json.NewEncoder(samples)}, nil
}

func (a *artifacts) appendSample(sample Sample) error {
	if err := a.encoder.Encode(sample); err != nil {
		return fmt.Errorf("append capacity sample: %w", err)
	}
	return nil
}

func (a *artifacts) writeEnvironment(environment Environment) error {
	return writeJSONFile(filepath.Join(a.directory, "environment.json"), environment)
}

func (a *artifacts) writeReport(report RunReport) error {
	if err := writeJSONFile(filepath.Join(a.directory, "report.json"), report); err != nil {
		return err
	}
	if err := writeFileAtomic(
		filepath.Join(a.directory, "report.md"), []byte(renderMarkdown(report)),
	); err != nil {
		return fmt.Errorf("write Markdown capacity report: %w", err)
	}
	return nil
}

func (a *artifacts) close() error {
	if a == nil || a.samples == nil {
		return nil
	}
	if err := a.samples.Close(); err != nil {
		return fmt.Errorf("close capacity samples: %w", err)
	}
	return nil
}

func writeJSONFile(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode %s: %w", filepath.Base(path), err)
	}
	data = append(data, '\n')
	if err := writeFileAtomic(path, data); err != nil {
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}
	return nil
}

func writeFileAtomic(path string, data []byte) error {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".capacity-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	cleanup := func() error {
		return errors.Join(temporary.Close(), os.Remove(temporaryPath))
	}
	if err := temporary.Chmod(0o640); err != nil {
		return errors.Join(err, cleanup())
	}
	if _, err := temporary.Write(data); err != nil {
		return errors.Join(err, cleanup())
	}
	if err := temporary.Sync(); err != nil {
		return errors.Join(err, cleanup())
	}
	if err := temporary.Close(); err != nil {
		_ = os.Remove(temporaryPath)
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		_ = os.Remove(temporaryPath)
		return err
	}
	return nil
}

func renderMarkdown(report RunReport) string {
	var builder strings.Builder
	_, _ = builder.WriteString("# GoMessenger capacity report\n\n")
	_, _ = builder.WriteString("Run: `" + report.RunID + "`\n\n")
	_, _ = builder.WriteString("Report spec: `" + report.SpecVersion + "`\n\n")
	statement := report.CapacityStatement
	if statement == "" {
		statement = "run is incomplete"
	}
	_, _ = builder.WriteString("**Result:** " + statement + ".\n\n")
	if report.Failure != "" {
		_, _ = builder.WriteString("**Failure:** " + report.Failure + "\n\n")
	}
	_, _ = builder.WriteString(
		"This is a checkout-local measurement on the recorded machine and topology;" +
			" it is not a production-capacity claim.\n\n",
	)
	if report.Warmup != nil {
		_, _ = fmt.Fprintf(&builder,
			"Warm-up: target %d msg/s; HTTP accepted %d; unique projections %d; integrity `%t`.\n\n",
			report.Warmup.TargetRate,
			report.Warmup.AfterDrain.HTTPAccepted,
			report.Warmup.Integrity.DistinctProjectionMsgs,
			report.Warmup.Integrity.Passed,
		)
	}
	_, _ = builder.WriteString("## Measured stages\n\n")
	_, _ = builder.WriteString(
		"| Rate | Sustainable | Offered | HTTP 202 total | DB accepted | Staged | Published | Committed |" +
			" Relay msg/s | Consumer msg/s | Consumer MiB/s | Outbox lag | Consumer lag |" +
			" Business p95 | Inbox p95 | ACK p95 | Batch calls | Avg batch | Max batch |" +
			" Batch handler p95 | Drain |\n",
	)
	_, _ = builder.WriteString(
		"|---:|:---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n",
	)
	for _, stage := range report.Stages {
		_, _ = fmt.Fprintf(&builder,
			"| %d | %t | %d | %d | %d | %d | %d | %d | %.2f | %.2f | %.3f | %d | %d |"+
				" %.2f ms | %.2f ms | %.2f ms | %d | %.2f | %d | %.2f ms | %.2f s |\n",
			stage.TargetRate,
			stage.Sustainable,
			stage.LoadWindow.Offered,
			stage.LoadWindow.HTTPAccepted,
			stage.LoadWindow.BusinessAccepted,
			stage.LoadWindow.Staged,
			stage.LoadWindow.Published,
			stage.LoadWindow.Committed,
			stage.RelayMessagesPerSec,
			stage.ConsumerMessagesPerSec,
			stage.ConsumerMiBPerSec,
			stage.OutboxLag,
			stage.ConsumerLag,
			stage.Latency.P95Millis,
			stage.InboxHandle.P95Millis,
			stage.BrokerAck.P95Millis,
			stage.ConsumerBatch.Invocations,
			stage.ConsumerBatch.AverageMessages,
			stage.ConsumerBatch.MaxMessages,
			stage.ConsumerBatch.Handler.P95Millis,
			stage.DrainSeconds,
		)
	}
	for _, stage := range report.Stages {
		_, _ = fmt.Fprintf(&builder,
			"\nPost-drain `%s`: HTTP accepted %d; business orders %d; measurements %d;"+
				" broker-confirmed %d; JetStream messages %d; unique projections %d; integrity `%t`.\n",
			stage.StageID,
			stage.AfterDrain.HTTPAccepted,
			stage.Integrity.Orders,
			stage.Integrity.Measurements,
			stage.Integrity.BrokerConfirmed,
			stage.AfterDrain.StreamPublished,
			stage.Integrity.DistinctProjectionMsgs,
			stage.Integrity.Passed,
		)
		if len(stage.UnsustainableReasons) != 0 {
			_, _ = builder.WriteString(
				"\nReasons for `" + stage.StageID + "`: " + strings.Join(stage.UnsustainableReasons, "; ") + ".\n",
			)
		}
		if !stage.Integrity.Passed {
			_, _ = builder.WriteString(
				"\nIntegrity failure for `" + stage.StageID + "`: " + strings.Join(stage.Integrity.Reasons, "; ") + ".\n",
			)
		}
		_, _ = builder.WriteString("\nBatch execution and normalized PostgreSQL cost:\n\n")
		_, _ = builder.WriteString("| Operation | Calls | Messages | Avg batch | Max batch | Errors | p95 |\n")
		_, _ = builder.WriteString("|---|---:|---:|---:|---:|---:|---:|\n")
		writeBatchExecutionRow(&builder, "Outbox handler", stage.OutboxExecution.Handler)
		writeBatchExecutionRow(&builder, "Outbox publish", stage.OutboxExecution.Publish)
		writeBatchExecutionRow(&builder, "Outbox finalization", stage.OutboxExecution.Finalization)
		writeBatchExecutionRow(&builder, "Consumer handler", stage.ConsumerBatch)
		_, _ = fmt.Fprintf(&builder,
			"\nOutbox outcomes: success `%d`, retry `%d`, defer `%d`, DLQ `%d`. "+
				"PostgreSQL: SQL calls `%d`, transactions/message `%.4f`, WAL/message `%.2f B`, checkpoints `%d`.\n"+
				"\nOutbox pool health: producer new/replaced/canceled/unusable/max-acquired `%d/%d/%d/%d/%d`; "+
				"relay `%d/%d/%d/%d/%d`.\n",
			stage.OutboxExecution.Outcomes.Success,
			stage.OutboxExecution.Outcomes.Retry,
			stage.OutboxExecution.Outcomes.Defer,
			stage.OutboxExecution.Outcomes.DLQ,
			stage.PostgreSQLNormalized.SQLCalls,
			stage.PostgreSQLNormalized.TransactionsPerMessage,
			stage.PostgreSQLNormalized.WALBytesPerMessage,
			stage.PostgreSQLNormalized.CompletedCheckpoints,
			stage.OutboxDatabase.Producer.NewConnections,
			stage.OutboxDatabase.Producer.ReplacementConnections,
			stage.OutboxDatabase.Producer.CanceledAcquires,
			stage.OutboxDatabase.Producer.UnusableReleases,
			stage.OutboxDatabase.Producer.MaxAcquiredConnections,
			stage.OutboxDatabase.Relay.NewConnections,
			stage.OutboxDatabase.Relay.ReplacementConnections,
			stage.OutboxDatabase.Relay.CanceledAcquires,
			stage.OutboxDatabase.Relay.UnusableReleases,
			stage.OutboxDatabase.Relay.MaxAcquiredConnections,
		)
		_, _ = builder.WriteString("\nPostgreSQL load-window statements for `" + stage.StageID + "`:\n\n")
		_, _ = builder.WriteString("| Class | Calls | Total time | WAL bytes | Query |\n")
		_, _ = builder.WriteString("|---|---:|---:|---:|---|\n")
		for _, statement := range stage.PostgreSQL.LoadDelta.Statements {
			_, _ = fmt.Fprintf(&builder, "| %s | %d | %.2f ms | %.0f | `%s` |\n",
				statement.Classification, statement.Calls, statement.TotalExecTimeMillis,
				statement.WALBytes, strings.ReplaceAll(statement.Query, "|", "\\|"))
		}
	}
	_, _ = builder.WriteString("\n## Metric boundary\n\n")
	_, _ = builder.WriteString("- `relay msg/s = published delta during the offered window / offered-window seconds`\n")
	_, _ = builder.WriteString(
		"- `consumer msg/s = committed projection delta during the offered window / offered-window seconds`\n",
	)
	_, _ = builder.WriteString("- `Outbox lag = staged delta - published delta`\n")
	_, _ = builder.WriteString("- `consumer lag = published delta - committed projection delta`\n")
	_, _ = builder.WriteString(
		"- `consumer MiB/s = exact canonical envelope bytes joined to those committed message IDs" +
			" / offered-window seconds / 1,048,576`\n",
	)
	_, _ = builder.WriteString(
		"- Drain time and post-drain reconciliation are reported separately" +
			" and never enter either throughput denominator.\n",
	)
	_, _ = builder.WriteString("\n## Environment\n\n")
	_, _ = fmt.Fprintf(&builder,
		"- Checkout: `%s` (dirty: `%s`)\n"+
			"- Host: `%s/%s`, logical CPUs `%s`\n"+
			"- Container: `%s/%s`, logical CPUs `%d`\n"+
			"- Go: `%s`\n"+
			"- Outbox module: `%s`; checkout `%s` (dirty: `%s`)\n"+
			"- PostgreSQL: `%s`, profile `%s`, image `%s`, digest `%s`\n"+
			"- NATS: `%s`, image `%s`, digest `%s`\n"+
			"- k6: `%s`\n"+
			"- Topology: file-backed JetStream, Outbox ingress `%s`, relay `%s`, workers `%d`, reservation batch `%d`, "+
			"producer/relay pgx `%d + %d = %d`, consumer mode `%s`, concurrency `%d`, "+
			"Outbox batch max messages/bytes/wait `%d` / `%d` / `%.3f ms`, "+
			"consumer batch max messages/bytes/wait `%d` / `%d` / `%.3f ms`, business DB max-open `%d`\n"+
			"- Limits: shared cpuset `%s`; PostgreSQL/API/NATS memory `%d` / `%d` / `%d` bytes; swap disabled `%t`\n"+
			"- PostgreSQL telemetry: compute_query_id `%s`, statement track `%s`, utility track `%s`,"+
			" I/O timing `%s`, WAL I/O timing `%s`\n",
		report.Environment.GitCommit,
		report.Environment.GitDirty,
		report.Environment.HostOS,
		report.Environment.HostArch,
		report.Environment.HostCPUs,
		report.Environment.ContainerOS,
		report.Environment.ContainerArch,
		report.Environment.ContainerCPUs,
		report.Environment.GoVersion,
		report.Environment.OutboxVersion,
		report.Environment.OutboxGitCommit,
		report.Environment.OutboxGitDirty,
		report.Environment.PostgreSQLVersion,
		report.Environment.PostgreSQLProfile,
		report.Environment.PostgreSQLImage,
		report.Environment.PostgreSQLImageDigest,
		report.Environment.NATSServerVersion,
		report.Environment.NATSImage,
		report.Environment.NATSImageDigest,
		report.Environment.K6Version,
		report.Environment.OutboxIngressMode,
		report.Environment.OutboxRelayMode,
		report.Environment.OutboxWorkers,
		report.Environment.OutboxReservationBatchSize,
		report.Environment.OutboxProducerMaxConns,
		report.Environment.OutboxRelayMaxConns,
		report.Environment.OutboxPGXConnectionBudget,
		report.Environment.ConsumerMode,
		report.Environment.ConsumerConcurrency,
		report.Environment.OutboxBatchMaxMessages,
		report.Environment.OutboxBatchMaxBytes,
		report.Environment.OutboxBatchMaxWaitMillis,
		report.Environment.ConsumerBatchMaxMessages,
		report.Environment.ConsumerBatchMaxBytes,
		report.Environment.ConsumerBatchMaxWaitMillis,
		report.Environment.DBMaxOpenConns,
		report.Environment.SUTCPUSet,
		report.Environment.PostgreSQLMemoryBytes,
		report.Environment.APIMemoryBytes,
		report.Environment.NATSMemoryBytes,
		report.Environment.SwapDisabled,
		report.Environment.PostgreSQLSettings["compute_query_id"],
		report.Environment.PostgreSQLSettings["pg_stat_statements.track"],
		report.Environment.PostgreSQLSettings["pg_stat_statements.track_utility"],
		report.Environment.PostgreSQLSettings["track_io_timing"],
		report.Environment.PostgreSQLSettings["track_wal_io_timing"],
	)
	return builder.String()
}

func writeBatchExecutionRow(builder *strings.Builder, name string, stats demo.BatchHandlerStats) {
	_, _ = fmt.Fprintf(builder, "| %s | %d | %d | %.2f | %d | %d | %.2f ms |\n",
		name,
		stats.Invocations,
		stats.Messages,
		stats.AverageMessages,
		stats.MaxMessages,
		stats.Handler.Errors,
		stats.Handler.P95Millis,
	)
}
