//nolint:testpackage // Tests exercise deterministic package-local payload construction and validation.
package demo

import (
	"strings"
	"testing"
)

func TestCapacityRequestMixIsDeterministicAndBounded(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		sequence  uint64
		wantItems int
		wantNote  int
	}{
		{name: "small lower", sequence: 0, wantItems: 1, wantNote: 256},
		{name: "small upper", sequence: 79, wantItems: 1, wantNote: 256},
		{name: "medium lower", sequence: 80, wantItems: 10, wantNote: 4 << 10},
		{name: "medium upper", sequence: 94, wantItems: 10, wantNote: 4 << 10},
		{name: "large", sequence: 95, wantItems: 50, wantNote: 64 << 10},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := CapacityRequest(test.sequence, "order-1")
			if len(request.Items) != test.wantItems || len(request.Note) != test.wantNote {
				t.Fatalf("unexpected payload class: items=%d note=%d", len(request.Items), len(request.Note))
			}
			order, err := NewOrder(request)
			if err != nil {
				t.Fatalf("NewOrder() error = %v", err)
			}
			if order.Amount <= 0 || order.Scenario != ScenarioSuccess {
				t.Fatalf("unexpected derived order: %#v", order)
			}
		})
	}
}

func TestNewOrderRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	valid := CapacityRequest(1, "order-1")
	tests := []struct {
		name   string
		mutate func(*CreateOrderRequest)
		match  string
	}{
		{name: "order ID", mutate: func(request *CreateOrderRequest) { request.OrderID = "" }, match: "orderId"},
		{name: "currency", mutate: func(request *CreateOrderRequest) { request.Currency = "usd" }, match: "currency"},
		{name: "items", mutate: func(request *CreateOrderRequest) { request.Items = nil }, match: "items"},
		{name: "note", mutate: func(request *CreateOrderRequest) { request.Note = strings.Repeat("x", maxNoteBytes+1) }, match: "note"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := valid
			request.Items = append([]LineItem(nil), valid.Items...)
			test.mutate(&request)
			_, err := NewOrder(request)
			if err == nil || !strings.Contains(err.Error(), test.match) {
				t.Fatalf("NewOrder() error = %v, want substring %q", err, test.match)
			}
		})
	}
}

func TestBenchmarkLabels(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		labels  BenchmarkLabels
		wantErr bool
	}{
		{name: "valid", labels: BenchmarkLabels{RunID: testRunID, StageID: "r0050"}},
		{name: "missing", labels: BenchmarkLabels{RunID: testRunID}, wantErr: true},
		{name: "unsafe", labels: BenchmarkLabels{RunID: "run/1", StageID: "stage"}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := test.labels.Validate()
			if (err != nil) != test.wantErr {
				t.Fatalf("Validate() error = %v, wantErr=%v", err, test.wantErr)
			}
		})
	}
}
