GO ?= go
GOLANGCI_LINT ?= golangci-lint
CHECK_GOWORK ?= $(abspath go.work)
MODULES := . adapters/inbox adapters/kafka adapters/nats adapters/outbox observability tools/gomessengerctl examples/durable-postgres-nats
LINT_MODULES := adapters/inbox adapters/kafka adapters/nats adapters/outbox observability tools/gomessengerctl examples/durable-postgres-nats
E2E_MODULE := testdata/e2e
DEMO_COMPOSE := examples/durable-postgres-nats/compose.yaml
CAPACITY_COMPOSE := examples/durable-postgres-nats/compose.capacity.yaml
CAPACITY_PROJECT := gomessenger-capacity-nats
INBOX_CAPACITY_COMPOSE := examples/durable-postgres-nats/compose.inbox-capacity.yaml
INBOX_CAPACITY_PROJECT := gomessenger-capacity-inbox-postgres

.PHONY: prepare fmt-check build vet lint lint-core lint-root lint-modules lint-fix lint-fix-core test test-race test-checkptr cover test-consumer test-consumer-release test-e2e test-integration test-batch-integration test-kafka test-postgres check check-workspace check-published bench-all release-ready release-readiness demo-durable-postgres-nats demo-durable-postgres-nats-down capacity-nats capacity-nats-full capacity-nats-site capacity-nats-site-single capacity-nats-site-batch-1 capacity-nats-site-batch-100 capacity-frontier capacity-frontier-matrix capacity-outbox-batch-screen capacity-batch-proof capacity-batch-proof-verdict capacity-inbox-postgres capacity-nats-down capacity-inbox-postgres-down

prepare:
	@$(GO) work sync
	@for module in $(MODULES); do (cd $$module && GOWORK=off $(GO) mod tidy) || exit 1; done
	@cd $(E2E_MODULE) && GOWORK=off $(GO) mod tidy
	@cd testdata/consumer && GOWORK=off $(GO) mod tidy
	@files="$$(find . -name '*.go' -not -path './tmp/*')"; test -z "$$files" || gofmt -w $$files

fmt-check:
	@test -z "$$(gofmt -l $$(find . -name '*.go' -not -path './tmp/*'))"

build:
	@for module in $(MODULES); do \
		if [ "$$module" = "tools/gomessengerctl" ]; then \
			(cd $$module && GOWORK=$(CHECK_GOWORK) $(GO) build -o /dev/null .) || exit 1; \
		else \
			(cd $$module && GOWORK=$(CHECK_GOWORK) $(GO) build ./...) || exit 1; \
		fi; \
	done
	@cd $(E2E_MODULE) && GOWORK=$(CHECK_GOWORK) $(GO) build ./...

vet:
	@for module in $(MODULES); do (cd $$module && GOWORK=$(CHECK_GOWORK) $(GO) vet ./...) || exit 1; done
	@cd $(E2E_MODULE) && GOWORK=$(CHECK_GOWORK) $(GO) vet ./...

lint: lint-core

lint-fix: lint-fix-core

lint-core: lint-root lint-modules

lint-root:
	@GOWORK=$(CHECK_GOWORK) $(GOLANGCI_LINT) run --timeout=5m ./...

lint-modules:
	@for module in $(LINT_MODULES); do (cd $$module && GOWORK=$(CHECK_GOWORK) $(GOLANGCI_LINT) run --timeout=5m ./...) || exit 1; done
	@cd $(E2E_MODULE) && GOWORK=$(CHECK_GOWORK) $(GOLANGCI_LINT) run --timeout=5m ./...

lint-fix-core:
	@for module in $(MODULES); do (cd $$module && GOWORK=off $(GOLANGCI_LINT) run -v --fix --timeout=5m ./...) || exit 1; done
	@cd $(E2E_MODULE) && GOWORK=off $(GOLANGCI_LINT) run -v --fix --timeout=5m ./...

test:
	@for module in $(MODULES); do (cd $$module && GOWORK=$(CHECK_GOWORK) $(GO) test ./...) || exit 1; done

test-race:
	@for module in $(MODULES); do (cd $$module && GOWORK=$(CHECK_GOWORK) $(GO) test -race ./...) || exit 1; done

test-checkptr:
	@for module in $(MODULES); do (cd $$module && GOWORK=$(CHECK_GOWORK) $(GO) test -gcflags=all=-d=checkptr=2 ./...) || exit 1; done

cover:
	@mkdir -p coverage
	@GOWORK=$(CHECK_GOWORK) $(GO) test -coverprofile=coverage/root.out ./...
	@coverage="$$(go tool cover -func=coverage/root.out | awk '/^total:/ {gsub("%", "", $$3); print $$3}')"; \
	awk -v coverage="$$coverage" 'BEGIN {if (coverage + 0 < 90) {printf "coverage %s%% is below 90%%\n", coverage; exit 1}}'

test-consumer:
	@cd testdata/consumer && GOWORK=$(CHECK_GOWORK) $(GO) test ./...

test-consumer-release:
	@test -n "$(VERSION)" || (echo "VERSION=vX.Y.Z is required" >&2; exit 2)
	@sh ./scripts/test-release-consumer.sh "$(VERSION)"

test-e2e:
	@cd $(E2E_MODULE) && GOWORK=$(CHECK_GOWORK) $(GO) test -race -count=1 ./...

test-integration: test-e2e
	@cd adapters/inbox && GOWORK=off $(GO) test -race ./...
	@cd adapters/kafka && GOWORK=off $(GO) test -race ./...
	@cd adapters/nats && GOWORK=off $(GO) test -race ./...
	@cd adapters/outbox && GOWORK=off $(GO) test -race ./...

test-batch-integration:
	@$(GO) test -race -count=1 ./internal/batchruntime
	@cd adapters/inbox && $(GO) test -race -count=1 ./pgsql ./sqlite
	@cd adapters/nats && $(GO) test -race -count=1 -run 'Batch' ./...
	@cd adapters/kafka && $(GO) test -race -count=1 -run 'Batch' ./...
	@cd $(E2E_MODULE) && $(GO) test -race -count=1 -run 'Batch' ./...

test-kafka:
	@sh ./scripts/test-kafka.sh

test-postgres:
	@test -n "$(GOMESSENGER_POSTGRES_DSN)" || (echo "GOMESSENGER_POSTGRES_DSN is required" >&2; exit 2)
	@cd adapters/inbox && GOWORK=off $(GO) test -race -count=1 -run '^TestPostgresInboxIntegration$$' ./pgsql

check: fmt-check build vet lint test test-race test-checkptr cover test-consumer test-e2e

check-workspace:
	@$(MAKE) check CHECK_GOWORK="$(abspath go.work)"

check-published:
	@$(MAKE) check CHECK_GOWORK=off

release-ready:
	@test -n "$(VERSION)" || (echo "VERSION=vX.Y.Z is required" >&2; exit 2)
	@test -n "$(OUTBOX_VERSION)" || (echo "OUTBOX_VERSION=vX.Y.Z is required" >&2; exit 2)
	@sh ./scripts/prepare-release-modules.sh "$(VERSION)" "$(OUTBOX_VERSION)"
	@files="$$(find . -name '*.go' -not -path './tmp/*')"; test -z "$$files" || gofmt -w $$files

release-readiness:
	@test -n "$(VERSION)" || (echo "VERSION=vX.Y.Z is required" >&2; exit 2)
	@test -n "$(OUTBOX_VERSION)" || (echo "OUTBOX_VERSION=vX.Y.Z is required" >&2; exit 2)
	@sh ./scripts/check-release-modules.sh "$(VERSION)" "$(OUTBOX_VERSION)"

bench-all:
	@GOWORK=off $(GO) test -run '^$$' -bench . -benchmem ./...

demo-durable-postgres-nats:
	@docker compose -f $(DEMO_COMPOSE) up --build --abort-on-container-exit --exit-code-from demo

demo-durable-postgres-nats-down:
	@docker compose -f $(DEMO_COMPOSE) down --volumes --remove-orphans

capacity-nats:
	@bash ./scripts/run-capacity-nats.sh

capacity-nats-full:
	@CAPACITY_PROFILE=full bash ./scripts/run-capacity-nats.sh

capacity-nats-site:
	@CAPACITY_PROFILE=site \
	POSTGRES_IMAGE="$${POSTGRES_IMAGE:-postgres:17-alpine}" \
	OUTBOX_WORKERS="$${OUTBOX_WORKERS:-2}" \
	OUTBOX_RESERVATION_BATCH_SIZE="$${OUTBOX_RESERVATION_BATCH_SIZE:-1}" \
	OUTBOX_PRODUCER_MAX_CONNS="$${OUTBOX_PRODUCER_MAX_CONNS:-9}" \
	OUTBOX_RELAY_MAX_CONNS="$${OUTBOX_RELAY_MAX_CONNS:-1}" \
	NATS_CONSUMER_CONCURRENCY="$${NATS_CONSUMER_CONCURRENCY:-1}" \
	CONSUMER_MODE="$${CONSUMER_MODE:-single}" \
	CONSUMER_BATCH_MAX_MESSAGES="$${CONSUMER_BATCH_MAX_MESSAGES:-100}" \
	CONSUMER_BATCH_MAX_BYTES="$${CONSUMER_BATCH_MAX_BYTES:-4194304}" \
	CONSUMER_BATCH_MAX_WAIT="$${CONSUMER_BATCH_MAX_WAIT:-25ms}" \
	DB_MAX_OPEN_CONNS="$${DB_MAX_OPEN_CONNS:-10}" \
	bash ./scripts/run-capacity-nats.sh

capacity-nats-site-single:
	@CONSUMER_MODE=single $(MAKE) capacity-nats-site

capacity-nats-site-batch-1:
	@CONSUMER_MODE=batch CONSUMER_BATCH_MAX_MESSAGES=1 $(MAKE) capacity-nats-site

capacity-nats-site-batch-100:
	@CONSUMER_MODE=batch CONSUMER_BATCH_MAX_MESSAGES=100 $(MAKE) capacity-nats-site

capacity-frontier:
	@bash ./scripts/run-capacity-frontier.sh

capacity-frontier-matrix:
	@bash ./scripts/run-capacity-frontier-matrix.sh

capacity-outbox-batch-screen:
	@bash ./scripts/run-outbox-batch-screen.sh

capacity-batch-proof:
	@bash ./scripts/run-capacity-batch-proof.sh

capacity-batch-proof-verdict:
	@test -n "$(PROOF_DIR)" || (echo "PROOF_DIR=tmp/capacity/frontiers/<proof-id> is required" >&2; exit 2)
	@cd examples/durable-postgres-nats && $(GO) run ./cmd/batch-proof -dir "$(abspath $(PROOF_DIR))"

capacity-inbox-postgres:
	@bash ./scripts/run-capacity-inbox-postgres.sh

capacity-nats-down:
	@docker compose -p $(CAPACITY_PROJECT) -f $(CAPACITY_COMPOSE) down --volumes --remove-orphans

capacity-inbox-postgres-down:
	@docker compose -p $(INBOX_CAPACITY_PROJECT) -f $(INBOX_CAPACITY_COMPOSE) down --volumes --remove-orphans
