// Package demo contains the shared business flow used by the runnable
// correctness demonstration and the local capacity stack.
package demo

import (
	"errors"
	"fmt"
	"math"
	"strings"
)

const (
	// Namespace is the PostgreSQL schema and NATS subject namespace used by the example.
	Namespace = "demo"
	// Stream is the source JetStream stream used by the example.
	Stream = "GOMESSENGER_DEMO"
	// DLQStream is the dead-letter JetStream stream used by the example.
	DLQStream = "GOMESSENGER_DEMO_DLQ"
	// DLQSubject is the dead-letter subject used by the example.
	DLQSubject = "demo.dlq"
	// ConsumerID is the stable durable consumer identity used by the example.
	ConsumerID = "demo-order-projection-v1"

	// BenchmarkRunHeader carries the capacity run identity in the canonical envelope.
	BenchmarkRunHeader = "benchmark-run-id"
	// BenchmarkStageHeader carries the capacity stage identity in the canonical envelope.
	BenchmarkStageHeader = "benchmark-stage-id"
	// CapacityOfferedAtHeader carries the generator-side request dispatch time.
	CapacityOfferedAtHeader = "X-GoMessenger-Capacity-Offered-At"

	ScenarioSuccess = "success"
	ScenarioRetry   = "retry"
	ScenarioDLQ     = "dlq"

	maxOrderIDBytes    = 160
	maxCustomerIDBytes = 128
	maxSKUBytes        = 64
	maxOrderItems      = 50
	maxNoteBytes       = 64 << 10
)

// LineItem is one immutable order line carried by the business event.
type LineItem struct {
	SKU       string `json:"sku"`
	Quantity  int64  `json:"quantity"`
	UnitPrice int64  `json:"unitPrice"`
}

// CreateOrderRequest is the HTTP business input accepted by the capacity service.
type CreateOrderRequest struct {
	OrderID    string     `json:"orderId"`
	CustomerID string     `json:"customerId"`
	Currency   string     `json:"currency"`
	Items      []LineItem `json:"items"`
	Note       string     `json:"note,omitempty"`
}

// OrderCreated is the typed durable event produced by the example.
type OrderCreated struct {
	OrderID    string     `json:"orderId"`
	CustomerID string     `json:"customerId"`
	Currency   string     `json:"currency"`
	Items      []LineItem `json:"items"`
	Amount     int64      `json:"amount"`
	Note       string     `json:"note,omitempty"`
	Scenario   string     `json:"scenario"`
}

// BenchmarkLabels identify one isolated capacity stage.
type BenchmarkLabels struct {
	RunID   string
	StageID string
}

// Validate checks that both benchmark labels are present and bounded.
func (l BenchmarkLabels) Validate() error {
	if err := validateLabel("run ID", l.RunID); err != nil {
		return err
	}
	return validateLabel("stage ID", l.StageID)
}

// NewOrder validates one HTTP request and derives its authoritative total.
func NewOrder(request CreateOrderRequest) (OrderCreated, error) {
	request.OrderID = strings.TrimSpace(request.OrderID)
	request.CustomerID = strings.TrimSpace(request.CustomerID)
	request.Currency = strings.TrimSpace(request.Currency)
	if request.OrderID == "" || len(request.OrderID) > maxOrderIDBytes {
		return OrderCreated{}, errors.New("orderId must contain 1..160 bytes")
	}
	if request.CustomerID == "" || len(request.CustomerID) > maxCustomerIDBytes {
		return OrderCreated{}, errors.New("customerId must contain 1..128 bytes")
	}
	if len(request.Currency) != 3 || request.Currency != strings.ToUpper(request.Currency) {
		return OrderCreated{}, errors.New("currency must be an uppercase three-letter code")
	}
	if len(request.Items) == 0 || len(request.Items) > maxOrderItems {
		return OrderCreated{}, errors.New("items must contain 1..50 entries")
	}
	if len(request.Note) > maxNoteBytes {
		return OrderCreated{}, errors.New("note exceeds 64 KiB")
	}

	items := make([]LineItem, len(request.Items))
	var amount int64
	for index, item := range request.Items {
		item.SKU = strings.TrimSpace(item.SKU)
		if item.SKU == "" || len(item.SKU) > maxSKUBytes {
			return OrderCreated{}, fmt.Errorf("items[%d].sku must contain 1..64 bytes", index)
		}
		if item.Quantity < 1 || item.Quantity > 1_000 {
			return OrderCreated{}, fmt.Errorf("items[%d].quantity must be in 1..1000", index)
		}
		if item.UnitPrice < 1 || item.UnitPrice > 1_000_000_000 {
			return OrderCreated{}, fmt.Errorf("items[%d].unitPrice must be in 1..1000000000", index)
		}
		if item.Quantity > math.MaxInt64/item.UnitPrice {
			return OrderCreated{}, fmt.Errorf("items[%d] amount overflows int64", index)
		}
		lineAmount := item.Quantity * item.UnitPrice
		if amount > math.MaxInt64-lineAmount {
			return OrderCreated{}, errors.New("order amount overflows int64")
		}
		amount += lineAmount
		items[index] = item
	}

	return OrderCreated{
		OrderID: request.OrderID, CustomerID: request.CustomerID, Currency: request.Currency,
		Items: items, Amount: amount, Note: request.Note, Scenario: ScenarioSuccess,
	}, nil
}

// CapacityRequest returns the deterministic 80/15/5 small, medium, and large
// business payload mix used by tests and mirrored by the k6 script.
func CapacityRequest(sequence uint64, orderID string) CreateOrderRequest {
	itemCount := 1
	noteBytes := 256
	switch bucket := sequence % 100; {
	case bucket >= 95:
		itemCount = 50
		noteBytes = 64 << 10
	case bucket >= 80:
		itemCount = 10
		noteBytes = 4 << 10
	}
	items := make([]LineItem, itemCount)
	for index := range items {
		items[index] = LineItem{
			SKU:       fmt.Sprintf("SKU-%03d-%02d", sequence%1_000, index),
			Quantity:  int64(index%3 + 1),
			UnitPrice: 100 + int64((sequence+uint64(index))%5_000),
		}
	}
	return CreateOrderRequest{
		OrderID: orderID, CustomerID: fmt.Sprintf("customer-%04d", sequence%10_000),
		Currency: "USD", Items: items, Note: strings.Repeat("n", noteBytes),
	}
}

func validateLabel(name, value string) error {
	if value == "" || len(value) > 64 {
		return fmt.Errorf("%s must contain 1..64 characters", name)
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || character == '-' || character == '_' || character == '.' {
			continue
		}
		return fmt.Errorf("%s contains unsupported character %q", name, character)
	}
	return nil
}
