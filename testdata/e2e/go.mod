module example.com/gomessenger-e2e

go 1.27.0

require (
	github.com/assurrussa/gomessenger v0.2.2
	github.com/assurrussa/gomessenger/adapters/inbox v0.2.2
	github.com/assurrussa/gomessenger/adapters/kafka v0.0.0
	github.com/assurrussa/gomessenger/adapters/nats v0.0.0
	github.com/assurrussa/gomessenger/adapters/outbox v0.0.0
	github.com/assurrussa/outbox v0.12.0
	github.com/assurrussa/outbox/backends/sqlite v0.12.0
	github.com/nats-io/nats-server/v2 v2.14.5
	github.com/nats-io/nats.go v1.53.1
	github.com/twmb/franz-go v1.21.6
	modernc.org/sqlite v1.57.0
)

require (
	github.com/antithesishq/antithesis-sdk-go v0.7.2-default-no-op // indirect
	github.com/assurrussa/gobus v1.1.0 // indirect
	github.com/cloudevents/sdk-go/v2 v2.16.2 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/google/go-tpm v0.9.8 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.19.2 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/mfridman/interpolate v0.0.2 // indirect
	github.com/minio/highwayhash v1.0.4 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/nats-io/jwt/v2 v2.8.2 // indirect
	github.com/nats-io/nkeys v0.4.16 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/pressly/goose/v3 v3.26.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/sethvargo/go-retry v0.3.0 // indirect
	github.com/twmb/franz-go/pkg/kadm v1.18.0 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	golang.org/x/crypto v0.55.0 // indirect
	golang.org/x/mod v0.38.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/time v0.15.0 // indirect
	modernc.org/libc v1.74.4 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
)

replace github.com/assurrussa/gomessenger => ../..

replace github.com/assurrussa/gomessenger/adapters/inbox => ../../adapters/inbox

replace github.com/assurrussa/gomessenger/adapters/kafka => ../../adapters/kafka

replace github.com/assurrussa/gomessenger/adapters/nats => ../../adapters/nats

replace github.com/assurrussa/gomessenger/adapters/outbox => ../../adapters/outbox
