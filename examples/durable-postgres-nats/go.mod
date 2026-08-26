module example.com/gomessenger-durable-postgres-nats

go 1.27.0

require (
	github.com/assurrussa/gomessenger v0.2.1
	github.com/assurrussa/gomessenger/adapters/inbox v0.2.1
	github.com/assurrussa/gomessenger/adapters/nats v0.0.0
	github.com/assurrussa/gomessenger/adapters/outbox v0.0.0
	github.com/assurrussa/outbox v0.11.0
	github.com/assurrussa/outbox/backends/pgsql v0.11.0
	github.com/jackc/pgx/v5 v5.10.0
	github.com/nats-io/nats.go v1.53.1
)

require (
	github.com/Masterminds/squirrel v1.5.4 // indirect
	github.com/assurrussa/gobus v1.1.0 // indirect
	github.com/cloudevents/sdk-go/v2 v2.16.2 // indirect
	github.com/georgysavva/scany/v2 v2.1.4 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.19.2 // indirect
	github.com/lann/builder v0.0.0-20180802200727-47ae307949d0 // indirect
	github.com/lann/ps v0.0.0-20150810152359-62de8c46ede0 // indirect
	github.com/mfridman/interpolate v0.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/nats-io/nkeys v0.4.16 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pressly/goose/v3 v3.26.0 // indirect
	github.com/sethvargo/go-retry v0.3.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/vgarvardt/pgx-google-uuid/v5 v5.6.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	golang.org/x/crypto v0.55.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
)

replace github.com/assurrussa/gomessenger => ../..

replace github.com/assurrussa/gomessenger/adapters/inbox => ../../adapters/inbox

replace github.com/assurrussa/gomessenger/adapters/nats => ../../adapters/nats

replace github.com/assurrussa/gomessenger/adapters/outbox => ../../adapters/outbox
