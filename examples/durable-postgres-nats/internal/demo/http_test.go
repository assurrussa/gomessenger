//nolint:testpackage // Tests exercise the handler's transaction boundary through a narrow package-local stub.
package demo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

type httpServiceStub struct {
	receipt     messenger.Receipt
	stageErr    error
	readyErr    error
	stageCalled bool
	labels      BenchmarkLabels
	payload     OrderCreated
	offeredAt   time.Time
}

func (s *httpServiceStub) StageOrder(
	_ context.Context,
	payload OrderCreated,
	labels BenchmarkLabels,
	offeredAt time.Time,
) (messenger.Receipt, error) {
	s.stageCalled = true
	s.labels = labels
	s.payload = payload
	s.offeredAt = offeredAt
	return s.receipt, s.stageErr
}
func (s *httpServiceStub) Readiness(context.Context) error { return s.readyErr }
func (s *httpServiceStub) Stats(context.Context) (AppStats, error) {
	return AppStats{Ready: s.readyErr == nil}, nil
}

func TestHTTPCreateOrderReturnsAcceptedReceipt(t *testing.T) {
	t.Parallel()
	messageID := mustMessageID(t)
	service := &httpServiceStub{receipt: messenger.Receipt{
		MessageID: messageID, State: messenger.ReceiptStaged,
	}}
	handler, err := NewHTTPHandler(service, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewHTTPHandler() error = %v", err)
	}
	body, err := json.Marshal(CapacityRequest(95, "order-95"))
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/orders", bytes.NewReader(body))
	request.Header.Set("X-GoMessenger-Capacity-Run", testRunID)
	request.Header.Set("X-GoMessenger-Capacity-Stage", "r0050")
	request.Header.Set(CapacityOfferedAtHeader, "2026-08-27T08:00:00.123Z")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	if !service.stageCalled || service.labels != (BenchmarkLabels{RunID: testRunID, StageID: "r0050"}) {
		t.Fatalf("stage call = %v labels=%#v", service.stageCalled, service.labels)
	}
	if service.payload.Scenario != ScenarioSuccess || len(service.payload.Items) != 50 {
		t.Fatalf("payload = %#v", service.payload)
	}
	if service.offeredAt != time.Date(2026, 8, 27, 8, 0, 0, 123_000_000, time.UTC) {
		t.Fatalf("offeredAt = %s", service.offeredAt)
	}
}

func TestHTTPCreateOrderRejectsBeforeStaging(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		headers bool
		body    string
	}{
		{name: "missing labels", body: `{}`},
		{name: "invalid JSON", headers: true, body: `{`},
		{name: "invalid order", headers: true, body: `{}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			service := &httpServiceStub{}
			handler, err := NewHTTPHandler(service, slog.New(slog.DiscardHandler))
			if err != nil {
				t.Fatalf("NewHTTPHandler() error = %v", err)
			}
			request := httptest.NewRequestWithContext(
				t.Context(), http.MethodPost, "/orders", bytes.NewBufferString(test.body),
			)
			if test.headers {
				request.Header.Set("X-GoMessenger-Capacity-Run", testRunID)
				request.Header.Set("X-GoMessenger-Capacity-Stage", testStageID)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest || service.stageCalled {
				t.Fatalf("status=%d stageCalled=%v body=%s", response.Code, service.stageCalled, response.Body.String())
			}
		})
	}
}

func TestHTTPHealthReflectsStartupAndDrainReadiness(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		readyErr error
		status   int
	}{
		{name: "ready", status: http.StatusOK},
		{name: "startup", readyErr: errors.New("consumer starting"), status: http.StatusServiceUnavailable},
		{name: "drain", readyErr: errors.New("application draining"), status: http.StatusServiceUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			service := &httpServiceStub{readyErr: test.readyErr}
			handler, err := NewHTTPHandler(service, slog.New(slog.DiscardHandler))
			if err != nil {
				t.Fatalf("NewHTTPHandler() error = %v", err)
			}
			request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/healthz", nil)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
			}
		})
	}
}

func TestHTTPCreateOrderDoesNotReturnAcceptedOnStageFailure(t *testing.T) {
	t.Parallel()
	service := &httpServiceStub{stageErr: context.DeadlineExceeded}
	handler, err := NewHTTPHandler(service, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewHTTPHandler() error = %v", err)
	}
	body, err := json.Marshal(CapacityRequest(1, "order-1"))
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/orders", bytes.NewReader(body))
	request.Header.Set("X-GoMessenger-Capacity-Run", testRunID)
	request.Header.Set("X-GoMessenger-Capacity-Stage", testStageID)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
}
