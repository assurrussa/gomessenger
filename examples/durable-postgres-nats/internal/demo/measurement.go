package demo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	messenger "github.com/assurrussa/gomessenger"
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

func newAsyncMeasurementRoute(
	delegate messenger.Route,
	record func(envelopeMeasurement),
) (*measurementRoute, error) {
	if record == nil {
		return nil, errors.New("capacity async measurement recorder is required")
	}
	return newMeasurementRouteWithRecorder(delegate, func(_ context.Context, measurement envelopeMeasurement) error {
		record(measurement)
		return nil
	})
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

func (r *measurementRoute) DeliverBatch(
	ctx context.Context,
	deliveries []messenger.Delivery,
) ([]messenger.Receipt, error) {
	delegate, ok := r.delegate.(messenger.BatchRoute)
	if !ok {
		return nil, fmt.Errorf("%w: route %s does not support measured batches", messenger.ErrUnsupportedCapability, r.delegate.Name())
	}
	for index, delivery := range deliveries {
		if delivery == nil {
			return nil, fmt.Errorf("capacity measurement batch received nil delivery at index %d", index)
		}
		metadata := delivery.Metadata()
		labels, measured, err := benchmarkLabels(metadata.Headers)
		if err != nil {
			return nil, err
		}
		if !measured {
			continue
		}
		envelope, err := delivery.MarshalEnvelope()
		if err != nil {
			return nil, fmt.Errorf("marshal measured envelope %d: %w", index, err)
		}
		digest := sha256.Sum256(envelope)
		// Recorder failures invalidate the run out of band and never alter the
		// transactional delivery result.
		_ = r.record(ctx, envelopeMeasurement{
			MessageID: metadata.ID.String(), Labels: labels, EnvelopeBytes: int64(len(envelope)),
			SHA256: hex.EncodeToString(digest[:]),
		})
	}
	return delegate.DeliverBatch(ctx, deliveries)
}

type confirmedEnvelopePublisher interface {
	PublishEnvelope(ctx context.Context, payload []byte) (messenger.Receipt, error)
}

type confirmedBatchEnvelopePublisher interface {
	PublishEnvelopeBatch(ctx context.Context, payloads [][]byte) ([]messenger.Receipt, []error, error)
}

type measurementPublisher struct {
	delegate     confirmedEnvelopePublisher
	record       func(publicationConfirmation)
	now          func() time.Time
	observations *outboxObservationRecorder
}

func newMeasurementPublisher(
	delegate confirmedEnvelopePublisher,
	record func(publicationConfirmation),
) (*measurementPublisher, error) {
	return newMeasurementPublisherWithClock(delegate, record, time.Now)
}

func newMeasurementPublisherWithClock(
	delegate confirmedEnvelopePublisher,
	record func(publicationConfirmation),
	now func() time.Time,
) (*measurementPublisher, error) {
	if delegate == nil || record == nil || now == nil {
		return nil, errors.New("capacity measurement publisher requires a delegate and marker")
	}
	return &measurementPublisher{delegate: delegate, record: record, now: now}, nil
}

func (p *measurementPublisher) PublishEnvelope(
	ctx context.Context,
	payload []byte,
) (messenger.Receipt, error) {
	envelope, err := messenger.UnmarshalEnvelope(payload)
	if err != nil {
		return messenger.Receipt{}, fmt.Errorf("decode envelope before broker publish: %w", err)
	}
	labels, measured, err := benchmarkLabels(envelope.Headers)
	if err != nil {
		return messenger.Receipt{}, err
	}
	var confirmation publicationConfirmation
	if measured {
		digest := sha256.Sum256(payload)
		confirmation.envelopeMeasurement = envelopeMeasurement{
			MessageID: envelope.ID.String(), Labels: labels, EnvelopeBytes: int64(len(payload)),
			SHA256: hex.EncodeToString(digest[:]),
		}
	}
	started := time.Now()
	receipt, err := p.delegate.PublishEnvelope(ctx, payload)
	p.observations.recordPublish([][]byte{payload}, time.Since(started), []error{err}, nil)
	if err != nil {
		return messenger.Receipt{}, err
	}
	if measured {
		// PubAck is complete. Recorder health is observed out of band and must
		// never turn a confirmed broker publication into a relay retry.
		confirmation.PublishedAt = p.now().UTC()
		p.record(confirmation)
	}
	return receipt, nil
}

func (p *measurementPublisher) PublishEnvelopeBatch(
	ctx context.Context,
	payloads [][]byte,
) ([]messenger.Receipt, []error, error) {
	delegate, ok := p.delegate.(confirmedBatchEnvelopePublisher)
	if !ok {
		return nil, nil, fmt.Errorf("%w: measured publisher does not support batches", messenger.ErrUnsupportedCapability)
	}
	confirmations := make([]publicationConfirmation, len(payloads))
	measured := make([]bool, len(payloads))
	for index, payload := range payloads {
		envelope, err := messenger.UnmarshalEnvelope(payload)
		if err != nil {
			return nil, nil, fmt.Errorf("decode batch envelope %d before broker publish: %w", index, err)
		}
		labels, isMeasured, err := benchmarkLabels(envelope.Headers)
		if err != nil {
			return nil, nil, err
		}
		if !isMeasured {
			continue
		}
		digest := sha256.Sum256(payload)
		confirmations[index].envelopeMeasurement = envelopeMeasurement{
			MessageID: envelope.ID.String(), Labels: labels, EnvelopeBytes: int64(len(payload)),
			SHA256: hex.EncodeToString(digest[:]),
		}
		measured[index] = true
	}
	started := time.Now()
	receipts, errs, err := delegate.PublishEnvelopeBatch(ctx, payloads)
	p.observations.recordPublish(payloads, time.Since(started), errs, err)
	if err != nil {
		return receipts, errs, err
	}
	if len(receipts) != len(payloads) || len(errs) != len(payloads) {
		return receipts, errs, errors.New("capacity measured batch publisher returned an invalid result length")
	}
	now := p.now().UTC()
	for index := range payloads {
		if measured[index] && errs[index] == nil {
			confirmations[index].PublishedAt = now
			p.record(confirmations[index])
		}
	}
	return receipts, errs, nil
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
