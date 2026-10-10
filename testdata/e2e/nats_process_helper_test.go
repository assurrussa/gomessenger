package e2e_test

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/assurrussa/gomessenger/adapters/inbox"
	inboxsqlite "github.com/assurrussa/gomessenger/adapters/inbox/sqlite"
	natsadapter "github.com/assurrussa/gomessenger/adapters/nats"
	"github.com/nats-io/nats-server/v2/server"
	natsio "github.com/nats-io/nats.go"
)

const (
	opsHelperEnvironment = "GOMESSENGER_NATS_PROCESS_HELPER"
	opsProtocolPrefix    = "GOMESSENGER_NATS_PROCESS "
	opsNamespace         = "processcheck"
	opsStream            = "GOMESSENGER_PROCESS_CHECK"
	opsConsumer          = "process-check-v1"
	opsSource            = "urn:gomessenger:process-check"
	opsAckWindow         = 32
	opsBrokerRole        = "broker"
	opsConsumerRole      = "consumer"
)

type opsProcessConfig struct {
	Role        string `json:"role"`
	Store       string `json:"store,omitempty"`
	Database    string `json:"database,omitempty"`
	URL         string `json:"url,omitempty"`
	Port        int    `json:"port,omitempty"`
	Concurrency int    `json:"concurrency,omitempty"`
}

type opsProcessEvent struct {
	Kind         string `json:"kind"`
	PID          int    `json:"pid"`
	URL          string `json:"url,omitempty"`
	Error        string `json:"error,omitempty"`
	HandleErrors int64  `json:"handleErrors,omitempty"`
	AckErrors    int64  `json:"ackErrors,omitempty"`
	Duplicates   int64  `json:"duplicates,omitempty"`
	Role         string `json:"role,omitempty"`
	Version      string `json:"serverVersion,omitempty"`
	Concurrency  int    `json:"localConcurrency,omitempty"`
	AckWindow    int    `json:"globalAckWindow,omitempty"`
}

type opsProcessObserver struct {
	mu     sync.Mutex
	counts opsProcessEvent
}

func (o *opsProcessObserver) Observe(_ context.Context, observation messenger.Observation) {
	o.mu.Lock()
	defer o.mu.Unlock()
	switch observation.Operation {
	case messenger.OperationHandle:
		if observation.Err != nil {
			o.counts.HandleErrors++
		}
		if observation.Duplicate {
			o.counts.Duplicates++
		}
	case messenger.OperationBrokerAck:
		if observation.Err != nil {
			o.counts.AckErrors++
		}
	default:
		// Only these outcome counters qualify the bounded load sample.
	}
}

func (o *opsProcessObserver) snapshot(kind string, health error) opsProcessEvent {
	o.mu.Lock()
	defer o.mu.Unlock()
	event := o.counts
	event.Kind, event.PID = kind, os.Getpid()
	if health != nil {
		event.Error = health.Error()
	}
	return event
}

// TestNATSProcessHelper is re-executed only by the opt-in parent test. It is
// intentionally a real process, not another Consumer in the parent's runtime.
func TestNATSProcessHelper(t *testing.T) {
	value := os.Getenv(opsHelperEnvironment)
	if value == "" {
		t.Skip("private helper for TestNATSProcessOperations")
	}
	var config opsProcessConfig
	if err := json.Unmarshal([]byte(value), &config); err != nil {
		t.Fatal(err)
	}
	runtime.GOMAXPROCS(1)
	var err error
	switch config.Role {
	case opsBrokerRole:
		err = opsRunBroker(config)
	case opsConsumerRole:
		err = opsRunConsumer(config)
	default:
		err = fmt.Errorf("unknown process role %q", config.Role)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func opsEmit(event opsProcessEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(os.Stdout, opsProtocolPrefix+string(data))
	return err
}

func opsRunBroker(config opsProcessConfig) error {
	instance, err := server.NewServer(&server.Options{
		Host: "127.0.0.1", Port: config.Port, JetStream: true, StoreDir: config.Store,
		MaxPayload: natsadapter.DefaultMaxDLQMessageBytes, NoLog: true, NoSigs: true,
	})
	if err != nil {
		return err
	}
	instance.Start()
	defer func() { instance.Shutdown(); instance.WaitForShutdown() }()
	if !instance.ReadyForConnections(10 * time.Second) {
		return errors.New("broker did not become ready")
	}
	if err := opsEmit(opsProcessEvent{
		Kind: "ready", PID: os.Getpid(), Role: opsBrokerRole, URL: instance.ClientURL(), Version: server.VERSION,
	}); err != nil {
		return err
	}
	scanner := bufio.NewScanner(os.Stdin)
	if scanner.Scan() {
		if scanner.Text() == "stop" {
			return nil
		}
		return fmt.Errorf("unknown broker command %q", scanner.Text())
	}
	return scanner.Err()
}

func opsOpenDatabase(ctx context.Context, path string) (*sql.DB, error) {
	database, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	for _, statement := range []string{"PRAGMA busy_timeout=5000", "PRAGMA synchronous=FULL"} {
		if _, err := database.ExecContext(ctx, statement); err != nil {
			return nil, errors.Join(err, database.Close())
		}
	}
	return database, nil
}

func opsRunConsumer(config opsProcessConfig) (runErr error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	database, err := opsOpenDatabase(ctx, config.Database)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, database.Close()) }()
	store, err := inboxsqlite.New(database)
	if err != nil {
		return err
	}
	connection, err := natsio.Connect(config.URL, natsio.Timeout(3*time.Second),
		natsio.MaxReconnects(-1), natsio.ReconnectWait(100*time.Millisecond),
		natsio.Name(fmt.Sprintf("gomessenger-process-check-%d", os.Getpid())))
	if err != nil {
		return err
	}
	defer connection.Close()
	observer := &opsProcessObserver{}
	descriptor := opsDescriptor()
	consumer, err := natsadapter.NewCommandConsumer(connection, store, descriptor,
		func(handlerContext context.Context, message messenger.Message[processPayload]) error {
			tx, ok := inbox.SQLTxFromContext(handlerContext)
			if !ok {
				return errors.New("missing Inbox transaction")
			}
			// A second handler execution increments the counter instead of hiding
			// duplicate effects behind ON CONFLICT DO NOTHING.
			_, err := tx.ExecContext(handlerContext, `INSERT INTO process_effects
				(message_id, job_id, worker_pid, executions) VALUES (?, ?, ?, 1)
				ON CONFLICT(message_id) DO UPDATE SET executions = executions + 1`,
				message.Metadata.ID.String(), message.Payload.JobID, os.Getpid())
			return err
		}, natsadapter.HandlerConfig{
			Stream: opsStream, Namespace: opsNamespace, ConsumerID: opsConsumer,
			WireMode: natsadapter.WireNative, Concurrency: config.Concurrency,
			MaxAckPending: opsAckWindow, Timeout: 10 * time.Second,
			FinalizationTimeout: 5 * time.Second, MaxAttempts: 3,
			BaseRetry: 100 * time.Millisecond, MaxRetry: time.Second,
			AckWait: 3 * time.Second, DLQSubject: opsNamespace + ".dlq",
			Replicas: 1, MemoryStorage: false, Observers: []messenger.Observer{observer},
		})
	if err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()
	readyContext, readyCancel := context.WithTimeout(ctx, 10*time.Second)
	defer readyCancel()
	for consumer.Readiness(readyContext) != nil {
		select {
		case err := <-done:
			return errors.Join(errors.New("consumer stopped before ready"), err)
		case <-readyContext.Done():
			return readyContext.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	ready := observer.snapshot("ready", consumer.DeepHealth(readyContext))
	ready.Role, ready.Concurrency, ready.AckWindow = opsConsumerRole, config.Concurrency, opsAckWindow
	if err := opsEmit(ready); err != nil {
		return err
	}
	return opsServeConsumerCommands(ctx, consumer, observer, done)
}

func opsServeConsumerCommands(
	ctx context.Context, consumer *natsadapter.Consumer, observer *opsProcessObserver, done <-chan error,
) error {
	commands := make(chan string)
	go func() {
		defer close(commands)
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			select {
			case commands <- scanner.Text():
			case <-ctx.Done():
				return
			}
		}
	}()
	for {
		select {
		case err := <-done:
			return errors.Join(errors.New("consumer stopped before drain"), err)
		case command, ok := <-commands:
			if !ok || command == "stop" {
				return opsDrainConsumer(ctx, consumer, observer, done)
			}
			if command != "health" {
				return fmt.Errorf("unknown consumer command %q", command)
			}
			healthContext, healthCancel := context.WithTimeout(ctx, 3*time.Second)
			health := consumer.DeepHealth(healthContext)
			healthCancel()
			if err := opsEmit(observer.snapshot("health", health)); err != nil {
				return err
			}
		}
	}
}

func opsDrainConsumer(
	ctx context.Context, consumer *natsadapter.Consumer, observer *opsProcessObserver, done <-chan error,
) error {
	consumer.BeginDrain()
	stopContext, stopCancel := context.WithTimeout(ctx, 10*time.Second)
	defer stopCancel()
	if err := consumer.Shutdown(stopContext); err != nil {
		return err
	}
	select {
	case err := <-done:
		return errors.Join(err, opsEmit(observer.snapshot("stopped", nil)))
	case <-stopContext.Done():
		return stopContext.Err()
	}
}

func opsDescriptor() messenger.Command[processPayload] {
	return messenger.MustCommand("operations.process", 1, messenger.JSON[processPayload]())
}

type opsChild struct {
	command *exec.Cmd
	input   io.WriteCloser
	events  chan opsProcessEvent
	done    chan struct{}
	err     error
	log     opsTail
	ready   opsProcessEvent
}

type opsTail struct {
	mu   sync.Mutex
	data []byte
}

func (b *opsTail) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data = append(b.data, data...)
	if len(b.data) > 64<<10 {
		b.data = b.data[len(b.data)-(64<<10):]
	}
	return len(data), nil
}

func (b *opsTail) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.data)
}

func opsStartChild(ctx context.Context, config opsProcessConfig) (*opsChild, error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	child := &opsChild{events: make(chan opsProcessEvent, 16), done: make(chan struct{})}
	// The current test binary is the only executable admitted by this helper.
	child.command = exec.CommandContext(ctx, executable,
		"-test.run=^TestNATSProcessHelper$", "-test.count=1", "-test.timeout=20m")
	child.command.Env = opsChildEnvironment(os.Environ(), string(data))
	child.command.Stderr = &child.log
	output, err := child.command.StdoutPipe()
	if err != nil {
		return nil, err
	}
	child.input, err = child.command.StdinPipe()
	if err != nil {
		return nil, err
	}
	if err := child.command.Start(); err != nil {
		return nil, err
	}
	scanDone := make(chan struct{})
	go func() {
		defer close(scanDone)
		scanner := bufio.NewScanner(output)
		for scanner.Scan() {
			line := scanner.Text()
			if !strings.HasPrefix(line, opsProtocolPrefix) {
				_, _ = child.log.Write([]byte(line + "\n"))
				continue
			}
			var event opsProcessEvent
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, opsProtocolPrefix)), &event); err != nil {
				_, _ = child.log.Write([]byte(err.Error()))
				continue
			}
			child.events <- event
		}
	}()
	go func() {
		<-scanDone
		child.err = child.command.Wait()
		close(child.done)
	}()
	readyContext, readyCancel := context.WithTimeout(ctx, 15*time.Second)
	defer readyCancel()
	event, err := child.await(readyContext, "ready")
	if err != nil {
		// Retain ownership even when readiness or the first cleanup fails.
		return child, errors.Join(err, child.kill())
	}
	child.ready = event
	return child, nil
}

func opsChildEnvironment(environment []string, config string) []string {
	result := make([]string, 0, len(environment)+2)
	for _, value := range environment {
		if strings.HasPrefix(value, opsHelperEnvironment+"=") || strings.HasPrefix(value, "GOMAXPROCS=") {
			continue
		}
		result = append(result, value)
	}
	return append(result, opsHelperEnvironment+"="+config, "GOMAXPROCS=1")
}

func (p *opsChild) await(ctx context.Context, kind string) (opsProcessEvent, error) {
	check := func(event opsProcessEvent) (opsProcessEvent, error) {
		if event.Kind != kind || event.PID != p.command.Process.Pid || event.Error != "" {
			return event, fmt.Errorf("unexpected %s response: %+v", kind, event)
		}
		return event, nil
	}
	// A final status is queued before stdout closes. Prefer it to the process's
	// done signal when both are ready after a graceful exit.
	select {
	case event := <-p.events:
		return check(event)
	default:
	}
	select {
	case event := <-p.events:
		return check(event)
	case <-p.done:
		return opsProcessEvent{}, errors.Join(fmt.Errorf("process exited waiting for %s; %s", kind, p.log.String()), p.err)
	case <-ctx.Done():
		return opsProcessEvent{}, fmt.Errorf("waiting for %s: %w", kind, ctx.Err())
	}
}

func (p *opsChild) health(ctx context.Context) (opsProcessEvent, error) {
	if _, err := io.WriteString(p.input, "health\n"); err != nil {
		return opsProcessEvent{}, err
	}
	return p.await(ctx, "health")
}

func (p *opsChild) stop(ctx context.Context) error {
	if _, err := io.WriteString(p.input, "stop\n"); err != nil {
		return err
	}
	select {
	case <-p.done:
		return p.err
	case <-ctx.Done():
		_ = p.kill()
		return ctx.Err()
	}
}

func (p *opsChild) kill() error {
	select {
	case <-p.done:
		return nil
	default:
	}
	err := p.command.Process.Kill()
	if errors.Is(err, os.ErrProcessDone) {
		err = nil
	}
	select {
	case <-p.done:
		return err
	case <-time.After(5 * time.Second):
		return errors.Join(err, errors.New("child did not exit after kill"))
	}
}
