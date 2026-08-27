GO ?= go
GOLANGCI_LINT ?= golangci-lint
MODULES := . adapters/inbox adapters/kafka adapters/nats adapters/outbox observability tools/gomessengerctl examples/durable-postgres-nats
LINT_MODULES := adapters/inbox adapters/kafka adapters/nats adapters/outbox observability tools/gomessengerctl examples/durable-postgres-nats
E2E_MODULE := testdata/e2e
DEMO_COMPOSE := examples/durable-postgres-nats/compose.yaml
CAPACITY_COMPOSE := examples/durable-postgres-nats/compose.capacity.yaml
CAPACITY_PROJECT := gomessenger-capacity-nats

.PHONY: prepare fmt-check build vet lint lint-core lint-root lint-modules lint-fix lint-fix-core test test-race test-checkptr cover test-consumer test-consumer-release test-e2e test-integration test-kafka test-postgres check bench-all release-ready release-readiness demo-durable-postgres-nats demo-durable-postgres-nats-down capacity-nats capacity-nats-full capacity-nats-down

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
			(cd $$module && GOWORK=off $(GO) build -o /dev/null .) || exit 1; \
		else \
			(cd $$module && GOWORK=off $(GO) build ./...) || exit 1; \
		fi; \
	done
	@cd $(E2E_MODULE) && GOWORK=off $(GO) build ./...

vet:
	@for module in $(MODULES); do (cd $$module && GOWORK=off $(GO) vet ./...) || exit 1; done
	@cd $(E2E_MODULE) && GOWORK=off $(GO) vet ./...

lint: lint-core

lint-fix: lint-fix-core

lint-core: lint-root lint-modules

lint-root:
	@GOWORK=off $(GOLANGCI_LINT) run --timeout=5m ./...

lint-modules:
	@for module in $(LINT_MODULES); do (cd $$module && GOWORK=off $(GOLANGCI_LINT) run --timeout=5m ./...) || exit 1; done
	@cd $(E2E_MODULE) && GOWORK=off $(GOLANGCI_LINT) run --timeout=5m ./...

lint-fix-core:
	@for module in $(MODULES); do (cd $$module && GOWORK=off $(GOLANGCI_LINT) run -v --fix --timeout=5m ./...) || exit 1; done
	@cd $(E2E_MODULE) && GOWORK=off $(GOLANGCI_LINT) run -v --fix --timeout=5m ./...

test:
	@for module in $(MODULES); do (cd $$module && GOWORK=off $(GO) test ./...) || exit 1; done

test-race:
	@for module in $(MODULES); do (cd $$module && GOWORK=off $(GO) test -race ./...) || exit 1; done

test-checkptr:
	@for module in $(MODULES); do (cd $$module && GOWORK=off $(GO) test -gcflags=all=-d=checkptr=2 ./...) || exit 1; done

cover:
	@mkdir -p coverage
	@GOWORK=off $(GO) test -coverprofile=coverage/root.out ./...
	@coverage="$$(go tool cover -func=coverage/root.out | awk '/^total:/ {gsub("%", "", $$3); print $$3}')"; \
	awk -v coverage="$$coverage" 'BEGIN {if (coverage + 0 < 90) {printf "coverage %s%% is below 90%%\n", coverage; exit 1}}'

test-consumer:
	@cd testdata/consumer && GOWORK=off $(GO) test ./...

test-consumer-release:
	@test -n "$(VERSION)" || (echo "VERSION=vX.Y.Z is required" >&2; exit 2)
	@sh ./scripts/test-release-consumer.sh "$(VERSION)"

test-e2e:
	@cd $(E2E_MODULE) && GOWORK=off $(GO) test -race -count=1 ./...

test-integration: test-e2e
	@cd adapters/inbox && GOWORK=off $(GO) test -race ./...
	@cd adapters/kafka && GOWORK=off $(GO) test -race ./...
	@cd adapters/nats && GOWORK=off $(GO) test -race ./...
	@cd adapters/outbox && GOWORK=off $(GO) test -race ./...

test-kafka:
	@sh ./scripts/test-kafka.sh

test-postgres:
	@test -n "$(GOMESSENGER_POSTGRES_DSN)" || (echo "GOMESSENGER_POSTGRES_DSN is required" >&2; exit 2)
	@cd adapters/inbox && GOWORK=off $(GO) test -race -count=1 -run '^TestPostgresInboxIntegration$$' ./pgsql

check: fmt-check build vet lint test test-race test-checkptr cover test-consumer test-e2e

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

capacity-nats-down:
	@docker compose -p $(CAPACITY_PROJECT) -f $(CAPACITY_COMPOSE) down --volumes --remove-orphans
