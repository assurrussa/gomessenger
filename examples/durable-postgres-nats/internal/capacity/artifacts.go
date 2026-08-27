package capacity

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	builder.WriteString("# GoMessenger capacity report\n\n")
	builder.WriteString("Run: `" + report.RunID + "`\n\n")
	statement := report.CapacityStatement
	if statement == "" {
		statement = "run is incomplete"
	}
	builder.WriteString("**Result:** " + statement + ".\n\n")
	if report.Failure != "" {
		builder.WriteString("**Failure:** " + report.Failure + "\n\n")
	}
	builder.WriteString(
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
	builder.WriteString("## Measured stages\n\n")
	builder.WriteString(
		"| Rate | Sustainable | Offered | HTTP 202 total | DB accepted | Staged | JetStream | Committed |" +
			" Effective msg/s | Effective MiB/s | Business p95 | Drain |\n",
	)
	builder.WriteString("|---:|:---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, stage := range report.Stages {
		_, _ = fmt.Fprintf(&builder,
			"| %d | %t | %d | %d | %d | %d | %d | %d | %.2f | %.3f | %.2f ms | %.2f s |\n",
			stage.TargetRate,
			stage.Sustainable,
			stage.LoadWindow.Offered,
			stage.LoadWindow.HTTPAccepted,
			stage.LoadWindow.BusinessAccepted,
			stage.LoadWindow.Staged,
			stage.LoadWindow.StreamPublished,
			stage.LoadWindow.Committed,
			stage.EffectiveMessagesPerSec,
			stage.EffectiveMiBPerSec,
			stage.Latency.P95Millis,
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
			builder.WriteString("\nReasons for `" + stage.StageID + "`: " + strings.Join(stage.UnsustainableReasons, "; ") + ".\n")
		}
		if !stage.Integrity.Passed {
			builder.WriteString("\nIntegrity failure for `" + stage.StageID + "`: " + strings.Join(stage.Integrity.Reasons, "; ") + ".\n")
		}
	}
	builder.WriteString("\n## Metric boundary\n\n")
	builder.WriteString("- `effective msg/s = committed projection delta during the offered window / offered-window seconds`\n")
	builder.WriteString(
		"- `effective MiB/s = exact canonical envelope bytes joined to those committed message IDs" +
			" / offered-window seconds / 1,048,576`\n",
	)
	builder.WriteString(
		"- Drain time and post-drain reconciliation are reported separately" +
			" and never enter either throughput denominator.\n",
	)
	builder.WriteString("\n## Environment\n\n")
	_, _ = fmt.Fprintf(&builder,
		"- Checkout: `%s` (dirty: `%s`)\n"+
			"- Host: `%s/%s`, logical CPUs `%s`\n"+
			"- Container: `%s/%s`, logical CPUs `%d`\n"+
			"- Go: `%s`\n"+
			"- PostgreSQL: `%s`\n"+
			"- NATS: `%s`\n"+
			"- k6: `%s`\n"+
			"- Topology: file-backed JetStream, Outbox workers `%d`, consumer concurrency `%d`\n",
		report.Environment.GitCommit,
		report.Environment.GitDirty,
		report.Environment.HostOS,
		report.Environment.HostArch,
		report.Environment.HostCPUs,
		report.Environment.ContainerOS,
		report.Environment.ContainerArch,
		report.Environment.ContainerCPUs,
		report.Environment.GoVersion,
		report.Environment.PostgreSQLVersion,
		report.Environment.NATSServerVersion,
		report.Environment.K6Version,
		report.Environment.OutboxWorkers,
		report.Environment.ConsumerConcurrency,
	)
	return builder.String()
}
