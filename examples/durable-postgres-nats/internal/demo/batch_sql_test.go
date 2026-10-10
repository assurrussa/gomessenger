//nolint:testpackage // Exercise the demo's package-local SQL batch boundary.
package demo

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/assurrussa/gomessenger/adapters/inbox"
)

func TestHandleOrderBatchPartialFailureAndRetry(t *testing.T) {
	t.Parallel()
	application := &handlerApplication{attempts: newAttemptTracker()}
	messages := sqlBatchMessages(t, ScenarioSuccess, ScenarioRetry, ScenarioSuccess, ScenarioDLQ)
	tx := &batchProjectionTx{t: t, beforeExec: func() {
		// Classification must finish before the first business statement.
		if application.attempts.get(messages[1].Payload.OrderID) != 1 ||
			application.attempts.get(messages[3].Payload.OrderID) != 1 {
			t.Fatal("business SQL ran before all items were classified")
		}
	}}
	result, err := application.handleOrderBatch(inbox.ContextWithSQLTx(t.Context(), tx), messages)
	if err != nil {
		t.Fatal(err)
	}
	assertSQLBatchKeys(t, result, messages)
	if result.Items[0].Err != nil || result.Items[2].Err != nil {
		t.Fatalf("successful results = %#v", result.Items)
	}
	if delay, ok := messenger.RetryDelay(result.Items[1].Err); !ok || delay != 300*time.Millisecond {
		t.Fatalf("retry result = %v", result.Items[1].Err)
	}
	if !messenger.IsPermanent(result.Items[3].Err) {
		t.Fatalf("terminal result = %v", result.Items[3].Err)
	}
	wantArgs := []any{
		[]string{messages[0].Payload.OrderID, messages[2].Payload.OrderID},
		[]string{messages[0].Metadata.ID.String(), messages[2].Metadata.ID.String()},
		[]int64{messages[0].Payload.Amount, messages[2].Payload.Amount},
		[]int32{1, 1},
		[]string{"", ""},
		[]string{"", ""},
	}
	if tx.calls != 1 || !reflect.DeepEqual(tx.args, wantArgs) {
		t.Fatalf("SQL calls=%d args=%#v, want one successful-subset insert %#v", tx.calls, tx.args, wantArgs)
	}

	// The handler does not sleep or retry internally. Simulate the consumer
	// invoking only the retryable item later, with the same immutable identity.
	retryTx := &batchProjectionTx{t: t}
	retried := messages[1:2]
	result, err = application.handleOrderBatch(inbox.ContextWithSQLTx(t.Context(), retryTx), retried)
	if err != nil {
		t.Fatal(err)
	}
	assertSQLBatchKeys(t, result, retried)
	wantArgs = []any{
		[]string{retried[0].Payload.OrderID},
		[]string{retried[0].Metadata.ID.String()},
		[]int64{retried[0].Payload.Amount},
		[]int32{2},
		[]string{""},
		[]string{""},
	}
	if result.Items[0].Err != nil || retryTx.calls != 1 || !reflect.DeepEqual(retryTx.args, wantArgs) {
		t.Fatalf("retry result=%#v calls=%d args=%#v", result, retryTx.calls, retryTx.args)
	}
}

func TestHandleOrderBatchSQLFailureReturnsOnlyTopLevelError(t *testing.T) {
	t.Parallel()
	application := &handlerApplication{attempts: newAttemptTracker()}
	messages := sqlBatchMessages(t, ScenarioSuccess, ScenarioRetry, ScenarioSuccess)
	sqlErr := errors.New("injected projection constraint failure")
	tx := &batchProjectionTx{t: t, execErr: sqlErr}
	result, err := application.handleOrderBatch(inbox.ContextWithSQLTx(t.Context(), tx), messages)
	if !errors.Is(err, sqlErr) || len(result.Items) != 0 || tx.calls != 1 {
		t.Fatalf("result=%#v error=%v SQL calls=%d", result, err, tx.calls)
	}
	// No success/item-error result may escape a failed shared SQL statement.
	// The real Inbox backend owns rollback; this recorder is not a database.
}

func TestHandleOrderBatchAllItemsFailWithoutSQL(t *testing.T) {
	t.Parallel()
	application := &handlerApplication{attempts: newAttemptTracker()}
	messages := sqlBatchMessages(t, ScenarioRetry, ScenarioDLQ)
	tx := &batchProjectionTx{t: t}
	result, err := application.handleOrderBatch(inbox.ContextWithSQLTx(t.Context(), tx), messages)
	if err != nil {
		t.Fatal(err)
	}
	assertSQLBatchKeys(t, result, messages)
	if result.Items[0].Err == nil || result.Items[1].Err == nil || tx.calls != 0 {
		t.Fatalf("result=%#v SQL calls=%d", result, tx.calls)
	}
}

func TestHandleOrderBatchRequiresInboxTransaction(t *testing.T) {
	t.Parallel()
	application := &handlerApplication{attempts: newAttemptTracker()}
	messages := sqlBatchMessages(t, ScenarioRetry)
	result, err := application.handleOrderBatch(t.Context(), messages)
	if err == nil || len(result.Items) != 0 || application.attempts.get(messages[0].Payload.OrderID) != 0 {
		t.Fatalf("result=%#v error=%v: missing transaction must fail before classification", result, err)
	}
}

func sqlBatchMessages(t *testing.T, scenarios ...string) []messenger.Message[OrderCreated] {
	t.Helper()
	ids := []string{
		"018f4f2c-4a00-7000-8000-000000000081",
		"018f4f2c-4a00-7000-8000-000000000082",
		"018f4f2c-4a00-7000-8000-000000000083",
		"018f4f2c-4a00-7000-8000-000000000084",
	}
	if len(scenarios) > len(ids) {
		t.Fatal("too many SQL batch fixtures")
	}
	messages := make([]messenger.Message[OrderCreated], len(scenarios))
	for index, scenario := range scenarios {
		id, err := messenger.ParseMessageID(ids[index])
		if err != nil {
			t.Fatal(err)
		}
		messages[index] = messenger.Message[OrderCreated]{
			Metadata: messenger.Metadata{Source: "urn:example:batch-sql", ID: id},
			Payload: OrderCreated{
				OrderID: ids[index], Amount: int64(index + 1), Scenario: scenario,
			},
		}
	}
	return messages
}

func assertSQLBatchKeys(t *testing.T, result messenger.BatchResult, messages []messenger.Message[OrderCreated]) {
	t.Helper()
	if len(result.Items) != len(messages) {
		t.Fatalf("result length=%d, want %d", len(result.Items), len(messages))
	}
	for index, message := range messages {
		want := messenger.BatchItemKey{Source: message.Metadata.Source, MessageID: message.Metadata.ID}
		if result.Items[index].Key != want {
			t.Fatalf("result key %d=%#v, want %#v", index, result.Items[index].Key, want)
		}
	}
}

// batchProjectionTx records the existing SQLTx seam. It deliberately models
// neither transaction commit/rollback nor broker acknowledgement.
type batchProjectionTx struct {
	t          *testing.T
	beforeExec func()
	execErr    error
	calls      int
	args       []any
}

var _ inbox.SQLTx = (*batchProjectionTx)(nil)

func (tx *batchProjectionTx) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	tx.t.Helper()
	tx.calls++
	if tx.beforeExec != nil {
		tx.beforeExec()
	}
	if !strings.Contains(query, "INSERT INTO demo.order_projection") || !strings.Contains(query, "FROM unnest(") {
		tx.t.Fatalf("unexpected business SQL: %s", query)
	}
	tx.args = append([]any(nil), args...)
	if tx.execErr != nil {
		return nil, tx.execErr
	}
	return driver.RowsAffected(1), nil
}

func (tx *batchProjectionTx) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	tx.t.Helper()
	tx.t.Fatal("unexpected QueryContext")
	return nil, errors.New("unexpected QueryContext")
}

func (tx *batchProjectionTx) QueryRowContext(context.Context, string, ...any) *sql.Row {
	tx.t.Helper()
	tx.t.Fatal("unexpected QueryRowContext")
	return nil
}
