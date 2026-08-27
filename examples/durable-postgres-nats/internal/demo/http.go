package demo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

const maxCreateOrderBodyBytes = 96 << 10

type capacityHTTPService interface {
	StageOrder(
		ctx context.Context,
		order OrderCreated,
		labels BenchmarkLabels,
		offeredAt time.Time,
	) (messenger.Receipt, error)
	Readiness(ctx context.Context) error
	Stats(ctx context.Context, labels BenchmarkLabels) (AppStats, error)
}

type capacityHTTPHandler struct {
	service capacityHTTPService
	log     *slog.Logger
}

// NewHTTPHandler returns the capacity service's isolated HTTP contract.
func NewHTTPHandler(service capacityHTTPService, log *slog.Logger) (http.Handler, error) {
	if service == nil {
		return nil, errors.New("capacity HTTP service is required")
	}
	if log == nil {
		log = slog.Default()
	}
	handler := &capacityHTTPHandler{service: service, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", handler.createOrder)
	mux.HandleFunc("GET /healthz", handler.health)
	mux.HandleFunc("GET /benchmark/stats", handler.stats)
	return mux, nil
}

// RunHTTPServer serves until cancellation, then closes admission and performs
// a bounded HTTP shutdown. Application resources remain owned by the caller.
func RunHTTPServer(
	ctx context.Context,
	address string,
	application *Application,
	log *slog.Logger,
) error {
	if address == "" {
		return errors.New("HTTP listen address is required")
	}
	handler, err := NewHTTPHandler(application, log)
	if err != nil {
		return err
	}
	server := &http.Server{
		Addr: address, Handler: handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.ListenAndServe()
	}()
	log.Info("capacity HTTP service ready", "address", address)

	select {
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve capacity HTTP: %w", err)
	case <-ctx.Done():
		return shutdownHTTPServer(ctx, application, server, serveErr, nil)
	case <-application.runtimeDone():
		return shutdownHTTPServer(
			ctx, application, server, serveErr,
			fmt.Errorf("capacity application runtime failed: %w", application.runtimeFailure()),
		)
	}
}

func shutdownHTTPServer(
	ctx context.Context,
	application *Application,
	server *http.Server,
	serveErr <-chan error,
	triggerErr error,
) error {
	application.BeginDrain()
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return errors.Join(triggerErr, fmt.Errorf("shutdown capacity HTTP: %w", err))
	}
	err := <-serveErr
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return errors.Join(triggerErr, fmt.Errorf("serve capacity HTTP during shutdown: %w", err))
	}
	return triggerErr
}

func (h *capacityHTTPHandler) createOrder(writer http.ResponseWriter, request *http.Request) {
	labels := BenchmarkLabels{
		RunID:   request.Header.Get("X-GoMessenger-Capacity-Run"),
		StageID: request.Header.Get("X-GoMessenger-Capacity-Stage"),
	}
	if err := labels.Validate(); err != nil {
		writeJSONError(writer, http.StatusBadRequest, err.Error())
		return
	}
	request.Body = http.MaxBytesReader(writer, request.Body, maxCreateOrderBodyBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var input CreateOrderRequest
	if err := decoder.Decode(&input); err != nil {
		writeJSONError(writer, http.StatusBadRequest, "decode order: "+err.Error())
		return
	}
	if err := ensureJSONEOF(decoder); err != nil {
		writeJSONError(writer, http.StatusBadRequest, err.Error())
		return
	}
	payload, err := NewOrder(input)
	if err != nil {
		writeJSONError(writer, http.StatusBadRequest, err.Error())
		return
	}
	offeredAt := time.Now().UTC()
	if rawOfferedAt := request.Header.Get(CapacityOfferedAtHeader); rawOfferedAt != "" {
		offeredAt, err = time.Parse(time.RFC3339Nano, rawOfferedAt)
		if err != nil {
			writeJSONError(writer, http.StatusBadRequest, "invalid offered-at timestamp")
			return
		}
	}
	receipt, err := h.service.StageOrder(request.Context(), payload, labels, offeredAt)
	if err != nil {
		h.log.Error("stage capacity order", "error", err)
		status := http.StatusInternalServerError
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			status = http.StatusServiceUnavailable
		}
		writeJSONError(writer, status, "order was not staged")
		return
	}
	writeJSON(writer, http.StatusAccepted, struct {
		OrderID   string                 `json:"orderId"`
		MessageID string                 `json:"messageId"`
		State     messenger.ReceiptState `json:"state"`
	}{OrderID: payload.OrderID, MessageID: receipt.MessageID.String(), State: receipt.State})
}

func (h *capacityHTTPHandler) health(writer http.ResponseWriter, request *http.Request) {
	probeCtx, cancel := context.WithTimeout(request.Context(), 2*time.Second)
	defer cancel()
	if err := h.service.Readiness(probeCtx); err != nil {
		writeJSONError(writer, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, struct {
		Status string `json:"status"`
	}{Status: "ready"})
}

func (h *capacityHTTPHandler) stats(writer http.ResponseWriter, request *http.Request) {
	probeCtx, cancel := context.WithTimeout(request.Context(), 2*time.Second)
	defer cancel()
	labels := BenchmarkLabels{
		RunID: request.URL.Query().Get("runId"), StageID: request.URL.Query().Get("stageId"),
	}
	if labels != (BenchmarkLabels{}) {
		if err := labels.Validate(); err != nil {
			writeJSONError(writer, http.StatusBadRequest, err.Error())
			return
		}
	}
	stats, err := h.service.Stats(probeCtx, labels)
	if err != nil {
		writeJSONError(writer, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, stats)
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request must contain one JSON object")
		}
		return fmt.Errorf("decode trailing request data: %w", err)
	}
	return nil
}

func writeJSONError(writer http.ResponseWriter, status int, message string) {
	writeJSON(writer, status, struct {
		Error string `json:"error"`
	}{Error: message})
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		return
	}
}
