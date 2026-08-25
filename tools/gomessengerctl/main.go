package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	natsadapter "github.com/assurrussa/gomessenger/adapters/nats"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	exitOK               = 0
	exitFailure          = 1
	exitUsage            = 2
	exitConflict         = 3
	dlqReplayUnavailable = "DLQ record is not replayable"
	commandValidate      = "validate"
	commandPlan          = "plan"
	commandApply         = "apply"
	commandInspect       = "inspect"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) < 2 {
		usage(stderr)
		return exitUsage
	}
	switch args[0] {
	case "kafka":
		return runKafka(args[1:], stdout, stderr)
	case "manifest":
		if args[1] != commandValidate {
			usage(stderr)
			return exitUsage
		}
		return validateManifest(args[2:], stdout, stderr)
	case "topology":
		switch args[1] {
		case commandValidate:
			return validateTopology(args[2:], stdout, stderr)
		case commandPlan, commandApply:
			return manageTopology(args[1], args[2:], stdout, stderr)
		default:
			usage(stderr)
			return exitUsage
		}
	case "dlq":
		switch args[1] {
		case commandInspect:
			return inspectDLQ(args[2:], stdout, stderr)
		case "replay":
			return replayDLQ(args[2:], stdout, stderr)
		default:
			usage(stderr)
			return exitUsage
		}
	default:
		usage(stderr)
		return exitUsage
	}
}

func validateManifest(args []string, stdout, stderr io.Writer) int {
	set := flag.NewFlagSet("manifest validate", flag.ContinueOnError)
	set.SetOutput(stderr)
	file := set.String("file", "", "manifest JSON file")
	if err := set.Parse(args); err != nil || *file == "" || set.NArg() != 0 {
		return exitUsage
	}
	var manifest messenger.Manifest
	if err := readStrictJSON(*file, &manifest); err != nil {
		return report(stderr, err)
	}
	if err := manifest.Validate(); err != nil {
		return report(stderr, err)
	}
	if _, err := fmt.Fprintln(stdout, "manifest valid"); err != nil {
		return report(stderr, err)
	}
	return exitOK
}

func validateTopology(args []string, stdout, stderr io.Writer) int {
	topology, code := readTopologyArgs("topology validate", args, stderr)
	if code != exitOK {
		return code
	}
	if err := natsadapter.ValidateTopology(topology); err != nil {
		return report(stderr, err)
	}
	if _, err := fmt.Fprintln(stdout, "topology valid"); err != nil {
		return report(stderr, err)
	}
	return exitOK
}

func manageTopology(action string, args []string, stdout, stderr io.Writer) int {
	set := flag.NewFlagSet("topology "+action, flag.ContinueOnError)
	set.SetOutput(stderr)
	file := set.String("file", "", "topology JSON file")
	server := set.String("server", natsio.DefaultURL, "NATS server URL")
	timeout := set.Duration("timeout", 10*time.Second, "operation timeout")
	if err := set.Parse(args); err != nil || *file == "" || *server == "" || *timeout <= 0 || set.NArg() != 0 {
		return exitUsage
	}
	var topology natsadapter.Topology
	if err := readStrictJSON(*file, &topology); err != nil {
		return report(stderr, err)
	}
	if err := natsadapter.ValidateTopology(topology); err != nil {
		return report(stderr, err)
	}
	connection, err := natsio.Connect(*server, natsio.Timeout(*timeout))
	if err != nil {
		return report(stderr, fmt.Errorf("connect NATS: %w", err))
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	var changes []natsadapter.Change
	if action == commandPlan {
		changes, err = natsadapter.PlanTopology(ctx, connection, topology)
	} else {
		changes, err = natsadapter.ApplyTopology(ctx, connection, topology)
	}
	if encodeErr := writeJSON(stdout, changes); encodeErr != nil && err == nil {
		err = encodeErr
	}
	if err != nil {
		return report(stderr, err)
	}
	for _, change := range changes {
		if change.Action == natsadapter.ChangeConflict {
			return exitConflict
		}
	}
	return exitOK
}

func inspectDLQ(args []string, stdout, stderr io.Writer) int {
	set := flag.NewFlagSet("dlq inspect", flag.ContinueOnError)
	set.SetOutput(stderr)
	file := set.String("file", "", "DLQ record JSON file")
	if err := set.Parse(args); err != nil || *file == "" || set.NArg() != 0 {
		return exitUsage
	}
	record, err := readDLQRecord(*file)
	if err != nil {
		return report(stderr, err)
	}
	if err := writeJSON(stdout, inspectDLQRecord(record)); err != nil {
		return report(stderr, err)
	}
	return exitOK
}

type dlqInspection struct {
	SpecVersion         string                  `json:"specVersion"`
	ConsumerID          string                  `json:"consumerId"`
	Subject             string                  `json:"subject"`
	Attempt             uint64                  `json:"attempt"`
	FailureKind         string                  `json:"failureKind"`
	FailedAt            time.Time               `json:"failedAt"`
	WireMode            natsadapter.WireMode    `json:"wireMode"`
	OriginalBytes       int                     `json:"originalBytes"`
	OriginalHeaderCount int                     `json:"originalHeaderCount"`
	Replayable          bool                    `json:"replayable"`
	ReplayPlan          *natsadapter.ReplayPlan `json:"replayPlan,omitempty"`
	ReplayError         string                  `json:"replayError,omitempty"`
}

func inspectDLQRecord(record natsadapter.DLQRecord) dlqInspection {
	original, _ := base64.StdEncoding.DecodeString(record.OriginalBase64)
	inspection := dlqInspection{
		SpecVersion: record.SpecVersion, ConsumerID: record.ConsumerID, Subject: record.Subject,
		Attempt: record.Attempt, FailureKind: record.FailureKind, FailedAt: record.FailedAt,
		WireMode: record.WireMode, OriginalBytes: len(original), OriginalHeaderCount: len(record.OriginalHeaders),
	}
	plan, err := natsadapter.PlanDLQReplay(record)
	if err != nil {
		inspection.ReplayError = dlqReplayUnavailable
		return inspection
	}
	inspection.Replayable = true
	inspection.ReplayPlan = &plan
	return inspection
}

func replayDLQ(args []string, stdout, stderr io.Writer) int {
	set := flag.NewFlagSet("dlq replay", flag.ContinueOnError)
	set.SetOutput(stderr)
	file := set.String("file", "", "DLQ record JSON file")
	server := set.String("server", natsio.DefaultURL, "NATS server URL")
	timeout := set.Duration("timeout", 10*time.Second, "operation timeout")
	confirm := set.Bool("confirm", false, "publish the replay after showing the same deterministic plan")
	if err := set.Parse(args); err != nil || *file == "" || *server == "" || *timeout <= 0 || set.NArg() != 0 {
		return exitUsage
	}
	record, err := readDLQRecord(*file)
	if err != nil {
		return report(stderr, err)
	}
	plan, planErr := natsadapter.PlanDLQReplay(record)
	if planErr != nil {
		return report(stderr, errors.New(dlqReplayUnavailable))
	}
	if !*confirm {
		if err := writeJSON(stdout, plan); err != nil {
			return report(stderr, err)
		}
		return exitOK
	}
	connection, err := natsio.Connect(*server, natsio.Timeout(*timeout))
	if err != nil {
		return report(stderr, fmt.Errorf("connect NATS: %w", err))
	}
	defer connection.Close()
	publisher, err := jetstream.New(connection)
	if err != nil {
		return report(stderr, fmt.Errorf("create JetStream context: %w", err))
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	result, err := natsadapter.ReplayDLQ(ctx, publisher, record)
	if err != nil {
		return report(stderr, err)
	}
	if err := writeJSON(stdout, result); err != nil {
		return report(stderr, err)
	}
	return exitOK
}

func readDLQRecord(path string) (natsadapter.DLQRecord, error) {
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return natsadapter.DLQRecord{}, fmt.Errorf("read %s: %w", path, err)
	}
	record, err := natsadapter.DecodeDLQRecord(data)
	if err != nil {
		return natsadapter.DLQRecord{}, err
	}
	return record, nil
}

func readTopologyArgs(command string, args []string, stderr io.Writer) (natsadapter.Topology, int) {
	set := flag.NewFlagSet(command, flag.ContinueOnError)
	set.SetOutput(stderr)
	file := set.String("file", "", "topology JSON file")
	if err := set.Parse(args); err != nil || *file == "" || set.NArg() != 0 {
		return natsadapter.Topology{}, exitUsage
	}
	var topology natsadapter.Topology
	if err := readStrictJSON(*file, &topology); err != nil {
		return natsadapter.Topology{}, report(stderr, err)
	}
	return topology, exitOK
}

func readStrictJSON(path string, target any) (err error) {
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer func() {
		if closeErr := file.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("close %s: %w", path, closeErr)
		}
	}()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return fmt.Errorf("decode %s: trailing JSON value", path)
	}
	return nil
}

func writeJSON(writer io.Writer, value any) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func report(stderr io.Writer, err error) int {
	_, _ = fmt.Fprintln(stderr, err)
	return exitFailure
}

func usage(writer io.Writer) {
	_, _ = fmt.Fprintln(writer, "usage: gomessengerctl manifest validate --file manifest.json")
	_, _ = fmt.Fprintln(writer, "       gomessengerctl topology validate --file topology.json")
	_, _ = fmt.Fprintln(writer,
		"       gomessengerctl topology plan|apply --file topology.json [--server nats://localhost:4222]")
	_, _ = fmt.Fprintln(writer, "       gomessengerctl dlq inspect --file record.json")
	_, _ = fmt.Fprintln(writer,
		"       gomessengerctl dlq replay --file record.json [--confirm] [--server nats://localhost:4222]")
	_, _ = fmt.Fprintln(writer, "       gomessengerctl kafka topology validate --file topology.json")
	_, _ = fmt.Fprintln(writer,
		"       gomessengerctl kafka topology plan|apply --file topology.json --instance-id INSTANCE [--brokers localhost:9092]")
	_, _ = fmt.Fprintln(writer, "       gomessengerctl kafka dlq inspect --file record.json")
	_, _ = fmt.Fprintln(writer,
		"       gomessengerctl kafka dlq replay --file record.json [--confirm --instance-id INSTANCE] [--brokers localhost:9092]")
}
