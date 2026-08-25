package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	kafkaadapter "github.com/assurrussa/gomessenger/adapters/kafka"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

const kafkaDLQReplayUnavailable = "kafka DLQ record is not replayable"

func runKafka(args []string, stdout, stderr io.Writer) int {
	if len(args) < 2 {
		usage(stderr)
		return exitUsage
	}
	switch args[0] {
	case "topology":
		switch args[1] {
		case commandValidate:
			return validateKafkaTopology(args[2:], stdout, stderr)
		case commandPlan, commandApply:
			return manageKafkaTopology(args[1], args[2:], stdout, stderr)
		}
	case "dlq":
		switch args[1] {
		case commandInspect:
			return inspectKafkaDLQ(args[2:], stdout, stderr)
		case "replay":
			return replayKafkaDLQ(args[2:], stdout, stderr)
		}
	}
	usage(stderr)
	return exitUsage
}

func validateKafkaTopology(args []string, stdout, stderr io.Writer) int {
	set := flag.NewFlagSet("kafka topology validate", flag.ContinueOnError)
	set.SetOutput(stderr)
	file := set.String("file", "", "Kafka topology JSON file")
	if err := set.Parse(args); err != nil || *file == "" || set.NArg() != 0 {
		return exitUsage
	}
	var topology kafkaadapter.Topology
	if err := readStrictJSON(*file, &topology); err != nil {
		return report(stderr, err)
	}
	if err := kafkaadapter.ValidateTopology(topology); err != nil {
		return report(stderr, err)
	}
	if _, err := fmt.Fprintln(stdout, "Kafka topology valid"); err != nil {
		return report(stderr, err)
	}
	return exitOK
}

func manageKafkaTopology(action string, args []string, stdout, stderr io.Writer) int {
	set := flag.NewFlagSet("kafka topology "+action, flag.ContinueOnError)
	set.SetOutput(stderr)
	file := set.String("file", "", "Kafka topology JSON file")
	var connection kafkaConnectionFlags
	connection.add(set)
	if err := set.Parse(args); err != nil || *file == "" || set.NArg() != 0 {
		return exitUsage
	}
	var topology kafkaadapter.Topology
	if err := readStrictJSON(*file, &topology); err != nil {
		return report(stderr, err)
	}
	if err := kafkaadapter.ValidateTopology(topology); err != nil {
		return report(stderr, err)
	}
	transport, err := connection.newTransport("gomessengerctl-kafka-topology", true)
	if err != nil {
		return report(stderr, err)
	}
	defer func() { _ = transport.Shutdown(context.Background()) }()
	ctx, cancel := context.WithTimeout(context.Background(), connection.timeout)
	defer cancel()
	var plan kafkaadapter.TopologyPlan
	if action == commandPlan {
		plan, err = kafkaadapter.PlanTopology(ctx, transport, topology)
	} else {
		plan, err = kafkaadapter.ApplyTopology(ctx, transport, topology)
	}
	if encodeErr := writeJSON(stdout, plan); encodeErr != nil && err == nil {
		err = encodeErr
	}
	if err != nil && !errors.Is(err, kafkaadapter.ErrTopologyDrift) {
		return report(stderr, err)
	}
	if plan.HasConflicts() {
		if err != nil {
			_, _ = fmt.Fprintln(stderr, err)
		}
		return exitConflict
	}
	return exitOK
}

type kafkaDLQInspection struct {
	SpecVersion       string                   `json:"specVersion"`
	ConsumerID        string                   `json:"consumerId"`
	SourceTopic       string                   `json:"sourceTopic"`
	SourcePartition   int32                    `json:"sourcePartition"`
	SourceOffset      int64                    `json:"sourceOffset"`
	MessageID         string                   `json:"messageId,omitempty"`
	Attempt           uint64                   `json:"attempt"`
	AttemptGeneration string                   `json:"attemptGeneration,omitempty"`
	FailureKind       string                   `json:"failureKind"`
	FailedAt          time.Time                `json:"failedAt"`
	OriginalBytes     int                      `json:"originalBytes"`
	RecordKeyBytes    int                      `json:"recordKeyBytes"`
	Replayable        bool                     `json:"replayable"`
	ReplayPlan        *kafkaadapter.ReplayPlan `json:"replayPlan,omitempty"`
	ReplayError       string                   `json:"replayError,omitempty"`
}

func inspectKafkaDLQ(args []string, stdout, stderr io.Writer) int {
	set := flag.NewFlagSet("kafka dlq inspect", flag.ContinueOnError)
	set.SetOutput(stderr)
	file := set.String("file", "", "Kafka DLQ record JSON file")
	if err := set.Parse(args); err != nil || *file == "" || set.NArg() != 0 {
		return exitUsage
	}
	record, err := readKafkaDLQRecord(*file)
	if err != nil {
		return report(stderr, err)
	}
	original, _ := base64.StdEncoding.DecodeString(record.OriginalBase64)
	key, _ := base64.StdEncoding.DecodeString(record.RecordKeyBase64)
	inspection := kafkaDLQInspection{
		SpecVersion: record.SpecVersion, ConsumerID: record.ConsumerID,
		SourceTopic: record.SourceTopic, SourcePartition: record.SourcePartition, SourceOffset: record.SourceOffset,
		MessageID: record.MessageID, Attempt: record.Attempt, AttemptGeneration: record.AttemptGeneration,
		FailureKind: record.FailureKind, FailedAt: record.FailedAt,
		OriginalBytes: len(original), RecordKeyBytes: len(key),
	}
	plan, planErr := kafkaadapter.PlanDLQReplay(record)
	if planErr != nil {
		inspection.ReplayError = kafkaDLQReplayUnavailable
	} else {
		inspection.Replayable = true
		inspection.ReplayPlan = &plan
	}
	if err := writeJSON(stdout, inspection); err != nil {
		return report(stderr, err)
	}
	return exitOK
}

func replayKafkaDLQ(args []string, stdout, stderr io.Writer) int {
	set := flag.NewFlagSet("kafka dlq replay", flag.ContinueOnError)
	set.SetOutput(stderr)
	file := set.String("file", "", "Kafka DLQ record JSON file")
	confirm := set.Bool("confirm", false, "publish the replay after showing the same deterministic plan")
	var connection kafkaConnectionFlags
	connection.add(set)
	if err := set.Parse(args); err != nil || *file == "" || set.NArg() != 0 {
		return exitUsage
	}
	record, err := readKafkaDLQRecord(*file)
	if err != nil {
		return report(stderr, err)
	}
	plan, err := kafkaadapter.PlanDLQReplay(record)
	if err != nil {
		return report(stderr, errors.New(kafkaDLQReplayUnavailable))
	}
	if !*confirm {
		if err := writeJSON(stdout, plan); err != nil {
			return report(stderr, err)
		}
		return exitOK
	}
	transport, err := connection.newTransport("gomessengerctl-kafka-replay", true)
	if err != nil {
		return report(stderr, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), connection.timeout)
	defer cancel()
	runDone, err := startKafkaTransport(ctx, transport)
	if err != nil {
		_ = transport.Shutdown(context.Background())
		return report(stderr, err)
	}
	result, replayErr := kafkaadapter.ReplayDLQ(ctx, transport, record)
	transport.BeginDrain()
	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), connection.timeout)
	shutdownErr := transport.Shutdown(shutdownContext)
	cancelShutdown()
	runErr := <-runDone
	if errors.Is(runErr, context.Canceled) {
		runErr = nil
	}
	if err := errors.Join(replayErr, shutdownErr, runErr); err != nil {
		return report(stderr, err)
	}
	if err := writeJSON(stdout, result); err != nil {
		return report(stderr, err)
	}
	return exitOK
}

func readKafkaDLQRecord(path string) (kafkaadapter.DLQRecord, error) {
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return kafkaadapter.DLQRecord{}, fmt.Errorf("read %s: %w", path, err)
	}
	record, err := kafkaadapter.DecodeDLQRecord(data)
	if err != nil {
		return kafkaadapter.DLQRecord{}, err
	}
	return record, nil
}

type kafkaConnectionFlags struct {
	brokers         string
	clientID        string
	instanceID      string
	timeout         time.Duration
	tlsEnabled      bool
	tlsCA           string
	tlsCert         string
	tlsKey          string
	tlsServerName   string
	saslMechanism   string
	saslUser        string
	saslPasswordEnv string
}

func (flags *kafkaConnectionFlags) add(set *flag.FlagSet) {
	set.StringVar(&flags.brokers, "brokers", "localhost:9092", "comma-separated Kafka seed brokers")
	set.StringVar(&flags.clientID, "client-id", "gomessengerctl", "Kafka client ID")
	set.StringVar(&flags.instanceID, "instance-id", "", "stable unique Kafka transaction instance ID")
	set.DurationVar(&flags.timeout, "timeout", 30*time.Second, "operation timeout")
	set.BoolVar(&flags.tlsEnabled, "tls", false, "enable TLS using system roots")
	set.StringVar(&flags.tlsCA, "tls-ca", "", "PEM CA bundle (also enables TLS)")
	set.StringVar(&flags.tlsCert, "tls-cert", "", "PEM client certificate")
	set.StringVar(&flags.tlsKey, "tls-key", "", "PEM client private key")
	set.StringVar(&flags.tlsServerName, "tls-server-name", "", "TLS server name override")
	set.StringVar(&flags.saslMechanism, "sasl-mechanism", "", "PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512")
	set.StringVar(&flags.saslUser, "sasl-user", "", "SASL username")
	set.StringVar(&flags.saslPasswordEnv, "sasl-password-env", "GOMESSENGER_KAFKA_SASL_PASSWORD",
		"environment variable containing the SASL password")
}

func (flags kafkaConnectionFlags) newTransport(name string, requireInstance bool) (*kafkaadapter.Transport, error) {
	brokers, err := splitKafkaBrokers(flags.brokers)
	if err != nil || flags.timeout <= 0 || flags.clientID == "" || requireInstance && flags.instanceID == "" {
		return nil, fmt.Errorf("%w: Kafka connection flags", kafkaadapter.ErrInvalidConfig)
	}
	options, err := flags.options()
	if err != nil {
		return nil, err
	}
	return kafkaadapter.NewTransport(kafkaadapter.TransportConfig{
		Name: name, Brokers: brokers, ClientID: flags.clientID, InstanceID: flags.instanceID,
		ConnectionOptions: options, OperationTimeout: flags.timeout,
	})
}

func splitKafkaBrokers(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		broker := strings.TrimSpace(part)
		if broker == "" {
			return nil, fmt.Errorf("%w: empty Kafka broker", kafkaadapter.ErrInvalidConfig)
		}
		brokers = append(brokers, broker)
	}
	return brokers, nil
}

func (flags kafkaConnectionFlags) options() ([]kafkaadapter.ConnectionOption, error) {
	options := make([]kafkaadapter.ConnectionOption, 0, 2)
	useTLS := flags.tlsEnabled || flags.tlsCA != "" || flags.tlsCert != "" || flags.tlsKey != "" || flags.tlsServerName != ""
	if useTLS {
		tlsConfig, err := flags.loadTLSConfig()
		if err != nil {
			return nil, err
		}
		options = append(options, kafkaadapter.DialTLSConfig(tlsConfig))
	}
	mechanism := strings.ToUpper(flags.saslMechanism)
	if mechanism == "" {
		if flags.saslUser != "" {
			return nil, fmt.Errorf("%w: SASL user without mechanism", kafkaadapter.ErrInvalidConfig)
		}
		return options, nil
	}
	if flags.saslUser == "" || flags.saslPasswordEnv == "" {
		return nil, fmt.Errorf("%w: incomplete SASL configuration", kafkaadapter.ErrInvalidConfig)
	}
	password := os.Getenv(flags.saslPasswordEnv)
	if password == "" {
		return nil, fmt.Errorf("%w: SASL password environment variable %s is empty",
			kafkaadapter.ErrInvalidConfig, flags.saslPasswordEnv)
	}
	switch mechanism {
	case "PLAIN":
		options = append(options, kafkaadapter.SASL(plain.Auth{User: flags.saslUser, Pass: password}.AsMechanism()))
	case "SCRAM-SHA-256":
		options = append(options, kafkaadapter.SASL(
			scram.Auth{User: flags.saslUser, Pass: password}.AsSha256Mechanism(),
		))
	case "SCRAM-SHA-512":
		options = append(options, kafkaadapter.SASL(
			scram.Auth{User: flags.saslUser, Pass: password}.AsSha512Mechanism(),
		))
	default:
		return nil, fmt.Errorf("%w: unsupported SASL mechanism %q", kafkaadapter.ErrInvalidConfig, flags.saslMechanism)
	}
	return options, nil
}

func (flags kafkaConnectionFlags) loadTLSConfig() (*tls.Config, error) {
	if (flags.tlsCert == "") != (flags.tlsKey == "") {
		return nil, fmt.Errorf("%w: TLS certificate and key must be provided together", kafkaadapter.ErrInvalidConfig)
	}
	roots, err := x509.SystemCertPool()
	if err != nil {
		return nil, fmt.Errorf("load system certificate roots: %w", err)
	}
	if roots == nil {
		roots = x509.NewCertPool()
	}
	if flags.tlsCA != "" {
		data, err := os.ReadFile(filepath.Clean(flags.tlsCA))
		if err != nil {
			return nil, fmt.Errorf("read Kafka TLS CA: %w", err)
		}
		if !roots.AppendCertsFromPEM(data) {
			return nil, errors.New("kafka TLS CA contains no certificates")
		}
	}
	config := &tls.Config{
		MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: flags.tlsServerName,
	}
	if flags.tlsCert != "" {
		certificate, err := tls.LoadX509KeyPair(filepath.Clean(flags.tlsCert), filepath.Clean(flags.tlsKey))
		if err != nil {
			return nil, fmt.Errorf("load Kafka TLS client certificate: %w", err)
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	return config, nil
}

func startKafkaTransport(ctx context.Context, transport *kafkaadapter.Transport) (<-chan error, error) {
	runDone := make(chan error, 1)
	go func() { runDone <- transport.Run(ctx) }()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		readinessContext, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		err := transport.Readiness(readinessContext)
		cancel()
		if err == nil {
			return runDone, nil
		}
		select {
		case runErr := <-runDone:
			if runErr == nil {
				runErr = messenger.ErrRuntimeNotRunning
			}
			return nil, runErr
		case <-ctx.Done():
			transport.BeginDrain()
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}
