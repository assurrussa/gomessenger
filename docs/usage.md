# Practical usage guide

This guide shows how a host application composes GoMessenger from a typed contract through transactional delivery,
durable consumption, and graceful shutdown. GoMessenger supplies messaging contracts and adapters; the host still owns
database connections, broker endpoints and credentials, migrations, topology policy, transaction boundaries, process
supervision, and deployment. The Kafka adapter creates franz-go clients from that host input.

The snippets below belong in an application's composition root. Variables such as `database`, `natsConnection`,
`outboxRuntime`, and business repositories are deliberately host-owned dependencies.

## Choose the delivery path

| Need                                             | Route                               | Success means                                       |
|--------------------------------------------------|-------------------------------------|-----------------------------------------------------|
| Call a command/event handler in the same process | `messenger.NewLocalSyncRoute()`     | the handler completed                               |
| Execute a local request/reply query              | `messenger.NewLocalSyncRoute()`     | the handler returned one typed result               |
| Isolate a local query behind bounded workers     | `messenger.NewLocalAsyncRoute(...)` | `Query` waited for the queued handler result        |
| Publish directly to JetStream                    | `natsadapter.NewRoute(...)`         | JetStream returned `PubAck`                         |
| Publish directly to Kafka                       | `kafkaadapter.NewRoute(...)`        | the Kafka producer transaction committed            |
| Commit a business write and message together     | `outboxadapter.NewProducer(...)`    | the envelope was staged in the caller's transaction |

## Local typed queries

Use `Query[Q,R]` for process-local request/reply while keeping the same builder, middleware, metadata, runtime, and DI
facade as commands and events:

```go
type FindOrder struct{ OrderID string }
type OrderView struct {
OrderID string
Status  string
}

findOrder := messenger.MustQuery[FindOrder, OrderView](
"orders.find", 1, messenger.JSON[FindOrder](),
)
builder := messenger.NewBuilder(messenger.WithSource("urn:service:orders"))
builder.HandleQueryFunc(findOrder, "orders.reader", func(ctx context.Context, query FindOrder) (OrderView, error) {
return orderRepository.Find(ctx, query.OrderID)
})
builder.RouteQuery(findOrder, messenger.NewLocalSyncRoute())

bus, _, err := builder.Build()
if err != nil {
return err
}
reader := messenger.BindQuerier(bus, findOrder)
view, err := reader.Query(ctx, FindOrder{OrderID: "order-42"})
```

Every registered query requires exactly one handler and one `LocalQueryRoute`. Only `LocalSyncRoute` and
`LocalAsyncRoute` implement that sealed contract, so an Outbox or NATS route cannot be registered accidentally. The
request codec and schema participate in descriptor identity; result `R` participates only in compile-time identity and
is absent from the manifest.

`Messenger.Query` always waits for one result. Selecting the async route adds bounded admission, backpressure, and
worker isolation; it does not add a public `QueryAsync`. Its runtime must be running, and the caller context controls
admission, handler execution, and waiting. Queries have generated local metadata and trace lineage but no `Outgoing`,
receipt, scheduling, expiry, or arbitrary-header API.

Global middleware may replace context or return an error, but successful completion without a result is
`ErrQueryResultMissing`. Use `ChainQueryHandler` for typed caching or synthetic results. Queries never serialize, enter
`Delivery`, or use Outbox, Inbox, JetStream, Kafka, retry, receipts, DLQ, or replay. Distributed request/reply is not
implemented; see [ADR-0003](decisions/0003-distributed-queries.md).

## Durable end-to-end flow

```text
producer host
  business transaction
    |-- business database write
    `-- bus.Publish / bus.Send
          `-- Outbox producer stages the canonical envelope
                         |
                  Outbox relay job
                         |
                  JetStream/Kafka <-- PubAck or transaction commit
                         |
consumer host      pull consumer
                         |
                  Inbox transaction
                    |-- typed handler database writes
                    `-- Inbox completion marker
                         |
                     commit succeeds
                         |
                    broker DoubleAck/offset transaction
```

Delivery is at-least-once. A crash after the Inbox commit but before broker acknowledgement causes redelivery; the
stable inbox identity suppresses a second committed handler transaction. An HTTP call, email, or other external side
effect outside that SQL transaction still needs its own idempotency key.

## 1. Declare a stable contract

Define payloads and descriptors in a package shared by producers and consumers. The wire name and schema version are
explicit; Go type and package names are not inferred as protocol identity.

```go
package contracts

import messenger "github.com/assurrussa/gomessenger"

type OrderCreatedPayload struct {
	OrderID string `json:"orderId"`
	Amount  int64  `json:"amount"`
}

var OrderCreated = messenger.MustEvent(
	"orders.order-created",
	1,
	messenger.JSON[OrderCreatedPayload](),
)
```

Keep a published descriptor version immutable. An incompatible payload change requires a new schema version and an
explicit consumer migration. Use `MustCommand` for a command with one logical handler, `MustQuery[Q,R]` for one local
request/reply handler, and `MustEvent` for an event that may have independently named consumers.

## 2. Compose a transactional producer

Provision the source and separately sized DLQ resources before starting traffic. Production hosts should plan and apply
declarative topology with `gomessengerctl`; `DevStream` and `DevDLQStream` are convenient NATS local-test defaults. For
Kafka topic policy and composition, use the [Kafka adapter guide](kafka.md).

Create the broker-confirmed route, register its relay capability on the Outbox service, and only then create the staging
route. The default relay job name and schema used by `NewRelayJob` match the defaults used by `NewProducer`.

```go
natsRoute, err := natsadapter.NewRoute(natsConnection, natsadapter.RouteConfig{
Name:      "nats.integration-events",
Namespace: "prod",
WireMode:  natsadapter.WireNative,
})
if err != nil {
return err
}

relayJob, err := outboxadapter.NewRelayJob(
natsRoute,
outboxadapter.RelayJobConfig{},
)
if err != nil {
return err
}
if err := outboxRuntime.Service().RegisterJob(relayJob); err != nil {
return err
}

outboxRoute, err := outboxadapter.NewProducer(
outboxRuntime.Service(),
outboxadapter.ProducerConfig{Name: "outbox.integration-events"},
)
if err != nil {
return err
}

builder := messenger.NewBuilder(
messenger.WithSource("urn:service:order-service"),
)
builder.RouteEvent(contracts.OrderCreated, outboxRoute)

bus, _, err := builder.Build()
if err != nil {
return err
}
```

Stage the message inside the same host transaction used by the business repository:

```go
var receipt messenger.Receipt

err = outboxRuntime.Transactor().RunInTx(ctx, func (txCtx context.Context) error {
if err := orderRepository.Create(txCtx, order); err != nil {
return err
}

var publishErr error
receipt, publishErr = bus.Publish(
txCtx,
contracts.OrderCreated,
contracts.OrderCreatedPayload{
OrderID: order.ID,
Amount:  order.Amount,
},
)
return publishErr
})
if err != nil {
return err
}
if receipt.State != messenger.ReceiptStaged {
return fmt.Errorf("unexpected receipt state: %s", receipt.State)
}
```

`ReceiptStaged` is provisional until `RunInTx` returns successfully. If the callback rolls back, neither the business
write nor the staged envelope remains. The relay later republishes the exact canonical bytes and completes only after
JetStream returns `PubAck`; it does not decode and rebuild message metadata.

Do not replace this flow with a direct NATS publish inside a database transaction: a database commit and broker publish
cannot be made atomic that way.

## 3. Compose a durable consumer

Apply the additive Inbox migrations before constructing the store. Size a shared connection pool for the sum of all
active consumer concurrency values plus application and maintenance headroom.

```go
const consumerConcurrency = 8

database.SetMaxOpenConns(consumerConcurrency + 4)
if err := inboxpgsql.Migrate(ctx, database); err != nil {
return err
}
inboxStore, err := inboxpgsql.New(database)
if err != nil {
return err
}
```

Construct one consumer with a stable `ConsumerID`. Renaming it creates a different durable consumer and normally
reprocesses retained messages.

```go
consumer, err := natsadapter.NewEventConsumer(
natsConnection,
inboxStore,
contracts.OrderCreated,
func (ctx context.Context, message messenger.Message[contracts.OrderCreatedPayload]) error {
tx, ok := inbox.SQLTxFromContext(ctx)
if !ok {
return errors.New("missing inbox transaction")
}

return billingRepository.RecordOrder(
ctx,
tx,
message.Payload.OrderID,
message.Payload.Amount,
)
},
natsadapter.HandlerConfig{
Stream:              "MESSAGES",
Namespace:           "prod",
ConsumerID:          "billing-order-projector",
WireMode:            natsadapter.WireNative,
Concurrency:         consumerConcurrency,
Timeout:             30 * time.Second,
FinalizationTimeout: 5 * time.Second,
AckWait:             30 * time.Second,
MaxAttempts:         5,
DLQSubject:          "prod.dlq",
},
)
if err != nil {
return err
}

consumerBuilder := messenger.NewBuilder(
messenger.WithSource("urn:service:billing-service"),
)
consumerBuilder.Use("consumer.billing-order-projector", consumer)

_, runtime, err := consumerBuilder.Build()
if err != nil {
return err
}
```

The Inbox backend begins the transaction and attaches it to the handler context. Database writes made through that
transaction and the Inbox completion marker commit together. Broker acknowledgement happens only after that commit.
When the handler returns an error, its business writes roll back while bounded attempt state is retained for retry and
terminal handling.

For commands use `NewCommandConsumer`; commands require the native wire mode. Events may use the native envelope or
CloudEvents structured/binary mode, but producer and consumer configuration must agree.

### Kafka alternative

For Kafka, create one `kafkaadapter.Transport` from host brokers, TLS/SASL options, and a stable unique `InstanceID`.
Give it to `kafkaadapter.NewRoute` and `NewEventConsumer`/`NewCommandConsumer`; attach the route and consumer to the same
runtime. The route is also an `outboxadapter.EnvelopePublisher`, so the Outbox relay wiring is unchanged. Kafka supports
native envelopes only. Its consumer atomically commits success offsets or produces retry/DLQ records with the consumed
offset, while Inbox still owns the handler database transaction. The complete example, topology JSON, CLI flags, and
ordering boundary are in [Kafka adapter](kafka.md).

## 4. Run and drain managed services

`Builder.Use` adds consumers and workers to the returned `Runtime`. The host must supervise `Runtime.Run`, expose
`Runtime.Readiness`, stop admission on shutdown, and give accepted work a bounded drain window.

```go
signalCtx, stopSignals := signal.NotifyContext(
context.Background(),
os.Interrupt,
syscall.SIGTERM,
)
defer stopSignals()

runErr := make(chan error, 1)
go func() {
runErr <- runtime.Run(context.Background())
}()

select {
case err := <-runErr:
return err // a managed service stopped unexpectedly
case <-signalCtx.Done():
}

runtime.BeginDrain()
shutdownCtx, cancelShutdown := context.WithTimeout(
context.Background(),
15*time.Second,
)
defer cancelShutdown()

if err := runtime.Shutdown(shutdownCtx); err != nil {
return err
}
return <-runErr
```

`BeginDrain` makes the runtime unready and closes admission. `Shutdown` waits for accepted work; if its context expires,
it force-cancels the shared run context. Coordinate shutdown outside handlers running on that same runtime. The selected
Outbox backend runtime has its own host-owned `Run`, readiness, and close lifecycle and is not implicitly supervised by
the GoMessenger runtime.

## 5. Classify handler failures

```go
// Transient failure: use bounded exponential retry with full jitter.
return err

// Retry no earlier than the requested delay.
return messenger.RetryAfter(err, 5*time.Minute)

// Terminal application failure: persist the outcome and hand off to DLQ.
return messenger.Permanent(err)
```

`MaxAttempts` bounds application handler invocations, not raw broker deliveries. A `RetryAfter` returned by the handler
uses one invocation; an envelope deferred by `NotBefore` does not invoke the handler and consumes no attempt. After a
permanent outcome or attempt exhaustion, the adapter keeps retrying the confirmed DLQ hand-off without invoking the
handler again. The source message is acknowledged only after the DLQ record is published successfully.

On Kafka, retry/DLQ publication and the consumed offset are one transaction. Retry uses consumer-specific topics with
unlimited retention and an exact due-time header; later source records may overtake a failed record. On NATS, the
current delivery remains unacknowledged until confirmed hand-off and active handlers use progress acknowledgements.

`AckWait` is the broker redelivery deadline, while `Timeout` bounds the handler invocation. The Inbox transaction has
an additional `FinalizationTimeout`, which defaults to 5 seconds, for commit or rollback; increase it for a remote or
otherwise slow database without extending handler execution. Active handlers send progress acknowledgements every
`AckWait / 3`, so `AckWait` does not need to exceed the longest handler duration.

## Startup checklist

1. Provision compatible source/retry/replay/DLQ topology and validate broker message limits.
2. Apply host-owned Outbox and Inbox migrations.
3. Register the relay job before any producer can stage its job name/schema.
4. Build consumers with stable IDs, bounded concurrency, timeouts, retries, and DLQ targets; Kafka also requires a
   stable unique process `InstanceID`.
5. Start and supervise Outbox and GoMessenger runtimes; expose their readiness independently.
6. Enable producer traffic only after the relay and consumers are ready.
7. On shutdown, stop admission, drain within a deadline, then close host-owned connections.

The complete executable reference is the Docker-free
[durable pipeline E2E](e2e.md), backed by `testdata/e2e`. It covers producer rollback, relay `PubAck`, lost-ACK
redelivery, Inbox suppression, retry, permanent DLQ, replay, trace propagation, and drain/redelivery through public
APIs. The same module contains an opt-in real Kafka pipeline run by `make test-kafka` against Kafka 4.1.2 and 4.3.1.
