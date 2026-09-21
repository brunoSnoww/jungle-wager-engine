package config

import (
	"strings"
	"testing"
	"time"

	"jungle/internal/application"
)

// keys is every variable Load reads. Each test clears all of them first: an
// inherited value would otherwise decide the outcome, so a rejection case could
// pass because of the ambient environment rather than because of its override.
var keys = []string{
	"DATABASE_URL", "HTTP_ADDR", "INSTANCE_ID", "OIDC_ISSUER", "OIDC_FETCH_ISSUER",
	"OIDC_AUDIENCE", "OIDC_ALLOW_INSECURE_HTTP", "AWS_REGION", "SQS_ENDPOINT",
	"SQS_INPUT_QUEUE", "SQS_OUTPUT_QUEUE", "SQS_INPUT_DLQ", "SQS_TRUSTED_PROVIDERS",
	"WORKER_CONCURRENCY", "OUTBOX_PUBLISH_BATCH", "DB_MAX_CONNS", "REFERENCE_MAX_ATTEMPTS",
	"PROCESS_TIMEOUT", "SQS_VISIBILITY_TIMEOUT", "SHUTDOWN_TIMEOUT", "CLAIM_LEASE",
	"POLL_INTERVAL", "REFERENCE_TTL",
}

// environment isolates the process, then applies the minimum Load accepts plus
// this case's override.
func environment(t *testing.T, override map[string]string) {
	t.Helper()
	for _, k := range keys {
		t.Setenv(k, "")
	}
	t.Setenv("DATABASE_URL", "postgres://u:p@localhost:5432/jungle?sslmode=disable")
	t.Setenv("OIDC_ISSUER", "http://localhost:8081/realms/jungle")
	for k, v := range override {
		t.Setenv(k, v)
	}
}

func TestLoadRejectsIncoherentBounds(t *testing.T) {
	for name, override := range map[string]map[string]string{
		"missing database":             {"DATABASE_URL": ""},
		"missing issuer":               {"OIDC_ISSUER": ""},
		"concurrency over receive cap": {"WORKER_CONCURRENCY": "99"},
		"zero concurrency":             {"WORKER_CONCURRENCY": "0"},
		"process over visible":         {"PROCESS_TIMEOUT": "55s"},
		"shutdown over visible":        {"SHUTDOWN_TIMEOUT": "90s"},
		"shutdown under process":       {"SHUTDOWN_TIMEOUT": "10s"},
		"lease under process":          {"CLAIM_LEASE": "31s"},
		"fractional visibility":        {"SQS_VISIBILITY_TIMEOUT": "60500ms"},
		"unparsable duration":          {"POLL_INTERVAL": "soon"},
		"publish batch zero":           {"OUTBOX_PUBLISH_BATCH": "0"},
		"publish batch oversize":       {"OUTBOX_PUBLISH_BATCH": "101"},
		"pool too small":               {"DB_MAX_CONNS": "1"},
		"pool oversize":                {"DB_MAX_CONNS": "51"},
		"unparsable bool":              {"OIDC_ALLOW_INSECURE_HTTP": "maybe"},
		"zero reference attempts":      {"REFERENCE_MAX_ATTEMPTS": "0"},
		"zero reference ttl":           {"REFERENCE_TTL": "0s"},
		"zero process timeout":         {"PROCESS_TIMEOUT": "0s"},
		"visibility beyond 12h":        {"SQS_VISIBILITY_TIMEOUT": "13h"},
		"padded provider":              {"SQS_TRUSTED_PROVIDERS": "provider-a, provider-b"},
		"empty provider":               {"SQS_TRUSTED_PROVIDERS": "provider-a,"},
	} {
		t.Run(name, func(t *testing.T) {
			environment(t, override)
			if _, err := Load(); err == nil {
				t.Fatalf("accepted invalid configuration: %v", override)
			}
		})
	}
}

func TestLoadDefaultsSatisfyTheirOwnInvariants(t *testing.T) {
	environment(t, nil)
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.InstanceID == "" || c.HTTPAddr == "" {
		t.Fatal("identity defaults missing")
	}
	// Restating the guard would be a dead assertion. Pin the intent instead: the
	// shipped defaults must leave real slack, not merely clear the inequality by
	// a nanosecond, so a redelivery cannot race a commit still in flight.
	if c.VisibilityTimeout-c.ProcessTimeout < 20*time.Second {
		t.Fatalf("defaults leave only %s between processing and redelivery", c.VisibilityTimeout-c.ProcessTimeout)
	}
	if c.Lease-c.ProcessTimeout < 20*time.Second {
		t.Fatalf("defaults leave only %s of lease headroom over a publish wave", c.Lease-c.ProcessTimeout)
	}
	if strings.Join(c.TrustedProviders, ",") != "provider-a,provider-b" {
		t.Fatalf("unexpected provider allowlist: %v", c.TrustedProviders)
	}
	// The SQS ReceiveMessage cap of 10 must not reach the outbox: coupling them
	// throttles publishing to a fraction of what the API can ingest.
	if c.PublishBatch <= c.Concurrency {
		t.Fatalf("outbox batch %d is still bounded by the SQS receive cap %d", c.PublishBatch, c.Concurrency)
	}
	// A batch the store would reject makes the publisher fail every claim and go
	// silent, so the accepted range must not exceed what ClaimOutbox allows.
	if c.PublishBatch > application.MaxOutboxBatch {
		t.Fatalf("default PublishBatch %d exceeds the store limit %d", c.PublishBatch, application.MaxOutboxBatch)
	}
}
