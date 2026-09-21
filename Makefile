SHELL := /bin/bash
GO ?= go
GOOSE_VERSION := v3.24.3
SQLC_VERSION := v1.30.0

.PHONY: help build run fmt vet test unit race generate tools migrate rollback integration concurrency recovery auth load conformance profile audit chaos soak up replicas down preflight
help:
	@printf '%s\n' 'make up          Compose infrastructure + API' 'make replicas    Three independent API processes' 'make test race   Unit tests and race detector' 'make integration Full real-infrastructure suite (see README)' 'make load      Constant-rate load with invariant verdict' 'make conformance Generated adversarial workload, many workers, audited' 'make profile   Hardware and Docker capability gate' 'make audit     Global ledger invariant auditor (read-only)' 'make soak      Load + chaos + audit, one report' 'make generate    SQLC generation (pinned tool)' 'make migrate     Apply Goose migrations'
build:
	$(GO) build ./...
run:
	$(GO) run ./cmd/api
fmt:
	gofmt -w cmd internal tests
vet:
	$(GO) vet ./...
	cd tests/load && $(GO) vet -tags=load ./...
test unit:
	$(GO) test ./...
race:
	$(GO) test -race ./...
generate:
	@test "$$(sqlc version)" = "$(SQLC_VERSION)" || { printf 'sqlc %s required\n' '$(SQLC_VERSION)'; exit 1; }
	sqlc generate
# Installing public dependencies is explicit; these targets never run from tests.
tools:
	$(GO) install github.com/sqlc-dev/sqlc/cmd/sqlc@$(SQLC_VERSION)
	$(GO) install github.com/pressly/goose/v3/cmd/goose@$(GOOSE_VERSION)
migrate:
	$(GO) run ./cmd/migrate up
rollback:
	$(GO) run ./cmd/migrate down
integration:
	$(GO) test -tags=integration -count=1 ./...
concurrency:
	$(GO) test -tags=integration -count=1 ./... -run 'Concurrent|Concurrency|Independent|Duplicate|Eighty'
recovery:
	$(GO) test -tags=integration -count=1 ./... -run 'Recovery|Restart|Crash|Reference|Outbox|Redelivery'
auth:
	$(GO) test -tags=integration -count=1 ./tests/auth/...
# Load runs against a stack already up (make replicas). It is excluded from the
# default and integration suites on purpose: it is slow and it is a measurement.
# Tune with LOAD_RATE, LOAD_DURATION, LOAD_WALLETS, LOAD_TARGETS.
load:
	cd tests/load && $(GO) test -tags=load -count=1 -v -timeout 30m ./...
# profile gates the others: on a laptop the binding constraint is how much of
# the machine Docker was actually given, not how much the machine has.
# Generated adversarial operations applied concurrently, then audited. Tune with
# MODEL_OPS, MODEL_WORKERS, MODEL_WALLETS; MODEL_SEED replays a past failure.
conformance:
	$(GO) test -tags=integration -count=1 -v -timeout 30m ./tests/model/...
profile:
	@./scripts/profile.sh || test $$? -le 1
audit:
	./scripts/audit.sh manual
chaos:
	./scripts/chaos.sh $(SCENARIO) $(SECONDS)
soak:
	./scripts/soak.sh $(SCENARIOS)
up:
	docker compose up --build -d
replicas:
	docker compose --profile replicas up --build -d
down:
	docker compose --profile replicas --profile observability down
preflight:
	$(GO) version
	docker compose config --quiet
	test -n "$(DATABASE_URL)"
