package demo

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	natsadapter "github.com/assurrussa/gomessenger/adapters/nats"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// RunCorrectness executes the deterministic retry, Inbox, DLQ, and replay proof.
func RunCorrectness(ctx context.Context, log *slog.Logger) (runErr error) {
	application, err := Open(ctx, CorrectnessConfig(log))
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		joinError(&runErr, "close demo application", application.Close(shutdownCtx))
	}()

	sourceSubscription, err := application.NATS().SubscribeSync(Namespace + ".event.>")
	if err != nil {
		return fmt.Errorf("subscribe to demo source: %w", err)
	}
	dlqSubscription, err := application.NATS().SubscribeSync(DLQSubject)
	if err != nil {
		return fmt.Errorf("subscribe to demo DLQ: %w", err)
	}
	if err := application.NATS().FlushTimeout(2 * time.Second); err != nil {
		return fmt.Errorf("flush demo subscriptions: %w", err)
	}
	js, err := jetstream.New(application.NATS())
	if err != nil {
		return fmt.Errorf("create JetStream context: %w", err)
	}
	return runScenarios(
		ctx, log, application.DB(), application, js, sourceSubscription, dlqSubscription,
	)
}

func runScenarios(
	ctx context.Context,
	log *slog.Logger,
	db *sql.DB,
	application *Application,
	js jetstream.JetStream,
	sourceSubscription *natsio.Subscription,
	dlqSubscription *natsio.Subscription,
) error {
	runID, err := randomID()
	if err != nil {
		return err
	}
	retryOrderID := runID + "-retry"
	dlqOrderID := runID + "-dlq"

	retryReceipt, err := application.StageOrder(
		ctx, correctnessOrder(retryOrderID, 4_200, ScenarioRetry), BenchmarkLabels{}, time.Time{},
	)
	if err != nil {
		return err
	}
	retryWire, err := waitForSource(ctx, sourceSubscription, retryReceipt.MessageID)
	if err != nil {
		return err
	}
	if err := waitFor(ctx, "retried projection", func() (bool, error) {
		exists, existsErr := projectionExists(ctx, db, retryOrderID)
		return exists && application.attempts.get(retryOrderID) == 2, existsErr
	}); err != nil {
		return err
	}
	log.Info("intentional retry rolled back the first write and committed the second",
		"order_id", retryOrderID, "handler_attempts", application.attempts.get(retryOrderID))

	if err := publishDistinctDuplicate(ctx, js, retryWire, retryReceipt.MessageID); err != nil {
		return err
	}
	if err := waitFor(ctx, "inbox duplicate", func() (bool, error) {
		return application.duplicates.Load() >= 1, nil
	}); err != nil {
		return err
	}
	if attempts := application.attempts.get(retryOrderID); attempts != 2 {
		return fmt.Errorf("duplicate invoked retry handler: attempts=%d", attempts)
	}
	log.Info("inbox suppressed a distinct broker delivery", "order_id", retryOrderID)

	dlqReceipt, err := application.StageOrder(
		ctx, correctnessOrder(dlqOrderID, 9_900, ScenarioDLQ), BenchmarkLabels{}, time.Time{},
	)
	if err != nil {
		return err
	}
	if _, err := waitForSource(ctx, sourceSubscription, dlqReceipt.MessageID); err != nil {
		return err
	}
	record, err := waitForDLQ(ctx, dlqSubscription, dlqReceipt.MessageID)
	if err != nil {
		return err
	}
	if exists, err := projectionExists(ctx, db, dlqOrderID); err != nil {
		return err
	} else if exists {
		return errors.New("permanent handler write was not rolled back before DLQ hand-off")
	}
	log.Info("permanent failure rolled back business state and reached DLQ", "order_id", dlqOrderID)

	replay, err := natsadapter.ReplayDLQ(ctx, js, record)
	if err != nil {
		return fmt.Errorf("replay DLQ record: %w", err)
	}
	if replay.Duplicate {
		return errors.New("first DLQ replay was unexpectedly broker-deduplicated")
	}
	if err := waitFor(ctx, "replayed projection", func() (bool, error) {
		exists, existsErr := projectionExists(ctx, db, dlqOrderID)
		return exists && application.attempts.get(dlqOrderID) == 2, existsErr
	}); err != nil {
		return err
	}
	secondReplay, err := natsadapter.ReplayDLQ(ctx, js, record)
	if err != nil {
		return fmt.Errorf("repeat DLQ replay: %w", err)
	}
	if !secondReplay.Duplicate || secondReplay.Plan.ReplayID != replay.Plan.ReplayID {
		return errors.New("repeated DLQ replay did not use deterministic broker deduplication")
	}
	log.Info("DLQ replay committed once and repeated replay was broker-deduplicated",
		"order_id", dlqOrderID, "replay_id", replay.Plan.ReplayID)

	if err := waitFor(ctx, "empty outbox", func() (bool, error) {
		total, statsErr := application.OutboxTotal(ctx)
		return statsErr == nil && total == 0, statsErr
	}); err != nil {
		return err
	}
	log.Info("durable scenarios passed; draining runtimes",
		"business_orders", 2,
		"committed_projections", 2,
		"inbox_duplicates", application.duplicates.Load(),
		"retry_attempts", application.attempts.get(retryOrderID),
		"dlq_replay_attempts", application.attempts.get(dlqOrderID))
	return nil
}

func correctnessOrder(orderID string, amount int64, scenario string) OrderCreated {
	return OrderCreated{
		OrderID: orderID, CustomerID: "correctness-customer", Currency: orderCurrencyUSD,
		Items:  []LineItem{{SKU: "CORRECTNESS", Quantity: 1, UnitPrice: amount}},
		Amount: amount, Scenario: scenario,
	}
}

func waitForSource(
	ctx context.Context,
	subscription *natsio.Subscription,
	messageID messenger.MessageID,
) (*natsio.Msg, error) {
	for {
		message, err := subscription.NextMsg(250 * time.Millisecond)
		if errors.Is(err, natsio.ErrTimeout) {
			if ctx.Err() != nil {
				return nil, fmt.Errorf("wait for source message %s: %w", messageID, ctx.Err())
			}
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read source message: %w", err)
		}
		envelope, err := messenger.UnmarshalEnvelope(message.Data)
		if err == nil && envelope.ID == messageID {
			return message, nil
		}
	}
}

func waitForDLQ(
	ctx context.Context,
	subscription *natsio.Subscription,
	messageID messenger.MessageID,
) (natsadapter.DLQRecord, error) {
	for {
		message, err := subscription.NextMsg(250 * time.Millisecond)
		if errors.Is(err, natsio.ErrTimeout) {
			if ctx.Err() != nil {
				return natsadapter.DLQRecord{}, fmt.Errorf("wait for DLQ message %s: %w", messageID, ctx.Err())
			}
			continue
		}
		if err != nil {
			return natsadapter.DLQRecord{}, fmt.Errorf("read DLQ message: %w", err)
		}
		record, err := natsadapter.DecodeDLQRecord(message.Data)
		if err != nil {
			return natsadapter.DLQRecord{}, fmt.Errorf("decode DLQ message: %w", err)
		}
		envelope, err := messenger.UnmarshalEnvelope(record.Envelope)
		if err == nil && envelope.ID == messageID {
			return record, nil
		}
	}
}

func publishDistinctDuplicate(
	ctx context.Context,
	js jetstream.JetStream,
	original *natsio.Msg,
	messageID messenger.MessageID,
) error {
	header := cloneHeader(original.Header)
	header.Del(natsio.MsgIdHdr)
	_, err := js.PublishMsg(ctx, &natsio.Msg{
		Subject: original.Subject,
		Header:  header,
		Data:    bytes.Clone(original.Data),
	}, jetstream.WithMsgID("demo-distinct-delivery-"+messageID.String()))
	if err != nil {
		return fmt.Errorf("publish distinct duplicate: %w", err)
	}
	return nil
}

func cloneHeader(source natsio.Header) natsio.Header {
	result := make(natsio.Header, len(source))
	for key, values := range source {
		result[key] = append([]string(nil), values...)
	}
	return result
}

func projectionExists(ctx context.Context, db *sql.DB, orderID string) (bool, error) {
	var count int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM demo.order_projection WHERE order_id = $1`, orderID).Scan(&count); err != nil {
		return false, fmt.Errorf("query order projection: %w", err)
	}
	return count == 1, nil
}
