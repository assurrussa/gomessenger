package demo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	messenger "github.com/assurrussa/gomessenger"
	outboxstorage "github.com/assurrussa/outbox/backends/pgsql/storage"
)

type envelopeMeasurement struct {
	MessageID     string
	Labels        BenchmarkLabels
	EnvelopeBytes int64
	SHA256        string
}

type measurementRecorder func(context.Context, envelopeMeasurement) error

type measurementRoute struct {
	delegate messenger.Route
	record   measurementRecorder
}

func newMeasurementRoute(delegate messenger.Route) (*measurementRoute, error) {
	return newMeasurementRouteWithRecorder(delegate, recordEnvelopeMeasurement)
}

func newMeasurementRouteWithRecorder(
	delegate messenger.Route,
	record measurementRecorder,
) (*measurementRoute, error) {
	if delegate == nil || record == nil {
		return nil, errors.New("capacity measurement route requires a delegate and recorder")
	}
	return &measurementRoute{delegate: delegate, record: record}, nil
}

func (r *measurementRoute) Name() string { return r.delegate.Name() }

func (r *measurementRoute) Deliver(
	ctx context.Context,
	delivery messenger.Delivery,
) (messenger.Receipt, error) {
	if delivery == nil {
		return messenger.Receipt{}, errors.New("capacity measurement route received nil delivery")
	}
	metadata := delivery.Metadata()
	labels, measured, err := benchmarkLabels(metadata.Headers)
	if err != nil {
		return messenger.Receipt{}, err
	}
	if !measured {
		return r.delegate.Deliver(ctx, delivery)
	}
	envelope, err := delivery.MarshalEnvelope()
	if err != nil {
		return messenger.Receipt{}, fmt.Errorf("marshal measured envelope: %w", err)
	}
	digest := sha256.Sum256(envelope)
	if err := r.record(ctx, envelopeMeasurement{
		MessageID: metadata.ID.String(), Labels: labels, EnvelopeBytes: int64(len(envelope)),
		SHA256: hex.EncodeToString(digest[:]),
	}); err != nil {
		return messenger.Receipt{}, fmt.Errorf("record envelope measurement: %w", err)
	}
	receipt, err := r.delegate.Deliver(ctx, delivery)
	if err != nil {
		return messenger.Receipt{}, fmt.Errorf("stage measured envelope: %w", err)
	}
	return receipt, nil
}

func recordEnvelopeMeasurement(ctx context.Context, measurement envelopeMeasurement) error {
	tx := outboxstorage.GetTx(ctx)
	if tx == nil {
		return errors.New("missing Outbox business transaction")
	}
	tag, err := tx.Exec(ctx, `INSERT INTO demo.envelope_measurements (
		message_id, run_id, stage_id, envelope_bytes, envelope_sha256
	) VALUES ($1, $2, $3, $4, $5)
	ON CONFLICT (message_id) DO UPDATE SET message_id = EXCLUDED.message_id
	WHERE demo.envelope_measurements.run_id = EXCLUDED.run_id
	  AND demo.envelope_measurements.stage_id = EXCLUDED.stage_id
	  AND demo.envelope_measurements.envelope_bytes = EXCLUDED.envelope_bytes
	  AND demo.envelope_measurements.envelope_sha256 = EXCLUDED.envelope_sha256`,
		measurement.MessageID,
		measurement.Labels.RunID,
		measurement.Labels.StageID,
		measurement.EnvelopeBytes,
		measurement.SHA256,
	)
	if err != nil {
		return fmt.Errorf("insert envelope measurement: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("message %s conflicts with a different envelope measurement", measurement.MessageID)
	}
	return nil
}

type confirmedEnvelopePublisher interface {
	PublishEnvelope(ctx context.Context, payload []byte) (messenger.Receipt, error)
}

type measurementPublisher struct {
	delegate confirmedEnvelopePublisher
	mark     func(context.Context, envelopeMeasurement) error
}

func newMeasurementPublisher(
	delegate confirmedEnvelopePublisher,
	mark func(context.Context, envelopeMeasurement) error,
) (*measurementPublisher, error) {
	if delegate == nil || mark == nil {
		return nil, errors.New("capacity measurement publisher requires a delegate and marker")
	}
	return &measurementPublisher{delegate: delegate, mark: mark}, nil
}

func (p *measurementPublisher) PublishEnvelope(
	ctx context.Context,
	payload []byte,
) (messenger.Receipt, error) {
	receipt, err := p.delegate.PublishEnvelope(ctx, payload)
	if err != nil {
		return messenger.Receipt{}, err
	}
	envelope, err := messenger.UnmarshalEnvelope(payload)
	if err != nil {
		return messenger.Receipt{}, fmt.Errorf("decode broker-confirmed envelope: %w", err)
	}
	labels, measured, err := benchmarkLabels(envelope.Headers)
	if err != nil {
		return messenger.Receipt{}, err
	}
	if !measured {
		return receipt, nil
	}
	digest := sha256.Sum256(payload)
	if err := p.mark(ctx, envelopeMeasurement{
		MessageID: envelope.ID.String(), Labels: labels, EnvelopeBytes: int64(len(payload)),
		SHA256: hex.EncodeToString(digest[:]),
	}); err != nil {
		return messenger.Receipt{}, fmt.Errorf("mark broker-confirmed envelope: %w", err)
	}
	return receipt, nil
}

func benchmarkLabels(headers map[string]string) (BenchmarkLabels, bool, error) {
	runID, hasRun := headers[BenchmarkRunHeader]
	stageID, hasStage := headers[BenchmarkStageHeader]
	if !hasRun && !hasStage {
		return BenchmarkLabels{}, false, nil
	}
	if !hasRun || !hasStage {
		return BenchmarkLabels{}, false, errors.New("capacity envelope must contain both run and stage headers")
	}
	labels := BenchmarkLabels{RunID: runID, StageID: stageID}
	if err := labels.Validate(); err != nil {
		return BenchmarkLabels{}, false, fmt.Errorf("validate capacity envelope labels: %w", err)
	}
	return labels, true, nil
}
