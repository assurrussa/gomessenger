package inboxcapacity

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type artifacts struct{ directory string }

func newArtifacts(directory string) (*artifacts, error) {
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return nil, fmt.Errorf("create PostgreSQL Inbox capacity artifact directory: %w", err)
	}
	return &artifacts{directory: directory}, nil
}

func (a *artifacts) writeEnvironment(environment Environment) error {
	return writeJSONFile(filepath.Join(a.directory, "environment.json"), environment)
}

func (a *artifacts) writeReport(report RunReport) error {
	if err := writeJSONFile(filepath.Join(a.directory, "report.json"), report); err != nil {
		return err
	}
	if err := writeFileAtomic(filepath.Join(a.directory, "report.md"), []byte(renderMarkdown(report))); err != nil {
		return fmt.Errorf("write PostgreSQL Inbox Markdown report: %w", err)
	}
	return nil
}

func writeJSONFile(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode %s: %w", filepath.Base(path), err)
	}
	return writeFileAtomic(path, append(data, '\n'))
}

func writeFileAtomic(path string, data []byte) error {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".inbox-capacity-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	cleanup := func() error { return errors.Join(temporary.Close(), os.Remove(temporaryPath)) }
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
	_, _ = builder.WriteString("# PostgreSQL Inbox capacity report\n\n")
	_, _ = builder.WriteString("Run: `" + report.RunID + "`\n\n")
	_, _ = builder.WriteString("This is a checkout-local `ProcessAttempt` measurement without Outbox or NATS;" +
		" it is not a production-capacity claim.\n\n")
	_, _ = builder.WriteString("| Concurrency | Repetition | Operations | Throughput | p50 | p95 | p99 | Integrity |\n")
	_, _ = builder.WriteString("|---:|---:|---:|---:|---:|---:|---:|:---:|\n")
	for _, item := range report.Cases {
		_, _ = fmt.Fprintf(&builder, "| %d | %d | %d | %.2f op/s | %.3f ms | %.3f ms | %.3f ms | %t |\n",
			item.Concurrency, item.Repetition, item.Operations, item.ThroughputPerSec,
			item.Latency.P50Millis, item.Latency.P95Millis, item.Latency.P99Millis, item.Integrity.Passed)
	}
	for _, item := range report.Cases {
		_, _ = fmt.Fprintf(&builder, "\n## C%d repetition %d statement delta\n\n", item.Concurrency, item.Repetition)
		_, _ = builder.WriteString("| Class | Calls | Total time | WAL bytes | Query |\n")
		_, _ = builder.WriteString("|---|---:|---:|---:|---|\n")
		for _, statement := range item.PostgreSQL.LoadDelta.Statements {
			_, _ = fmt.Fprintf(&builder, "| %s | %d | %.2f ms | %.0f | `%s` |\n",
				statement.Classification, statement.Calls, statement.TotalExecTimeMillis,
				statement.WALBytes, strings.ReplaceAll(statement.Query, "|", "\\|"))
		}
	}
	_, _ = builder.WriteString("\n## Environment\n\n")
	_, _ = fmt.Fprintf(&builder,
		"- Checkout: `%s` (dirty: `%s`)\n"+
			"- Host: `%s/%s`, logical CPUs `%s`\n"+
			"- Container: `%s/%s`, logical CPUs `%d`\n"+
			"- Go: `%s`\n"+
			"- PostgreSQL: `%s`\n"+
			"- Pool max-open: `%d`\n",
		report.Environment.GitCommit, report.Environment.GitDirty,
		report.Environment.HostOS, report.Environment.HostArch, report.Environment.HostCPUs,
		report.Environment.ContainerOS, report.Environment.ContainerArch, report.Environment.ContainerCPUs,
		report.Environment.GoVersion, report.Environment.PostgreSQLVersion, report.Environment.DBMaxOpenConns,
	)
	return builder.String()
}
