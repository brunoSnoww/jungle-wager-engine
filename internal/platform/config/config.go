package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"jungle/internal/application"
)

type Config struct {
	DatabaseURL, HTTPAddr, InstanceID                                    string
	OIDCIssuer, OIDCFetchIssuer, OIDCAudience                            string
	OIDCAllowHTTP                                                        bool
	AWSRegion, SQSEndpoint, InputQueue, OutputQueue, InputDLQ, OutputDLQ string
	TrustedProviders                                                     []string
	Concurrency                                                          int
	// PublishBatch is deliberately separate from Concurrency: the latter is
	// capped at 10 by the SQS ReceiveMessage API, and letting that inbound
	// limit also throttle the outbox caps publishing far below ingest.
	// PublishBatch pairs with MaxConns as operator guidance rather than a hard
	// rule: a wave holds a pool connection only for its single-row mark, but a
	// wide wave on a small pool can still briefly queue the financial path.
	PublishBatch int
	// MaxConns is per process, so the deployment budget is
	//
	//     max_replicas * DB_MAX_CONNS + migrations + operators <= max_connections
	//
	// Note max_replicas, not the steady-state count: under an autoscaler the
	// multiplier is whatever the ceiling allows, and a rolling update briefly
	// adds maxSurge on top of it. Budgeting against the replicas you usually run
	// is how a scale-out event turns every write into a 503. This process cannot
	// see the fleet, so the ceiling below is the only guardrail it can enforce.
	MaxConns                                                                int
	ProcessTimeout, VisibilityTimeout, ShutdownTimeout, Lease, PollInterval time.Duration
	ReferenceTTL                                                            time.Duration
	ReferenceMaxAttempts                                                    int
}

func Load() (Config, error) {
	host, err := os.Hostname()
	if err != nil {
		return Config{}, err
	}
	c := Config{
		DatabaseURL: os.Getenv("DATABASE_URL"), HTTPAddr: env("HTTP_ADDR", ":8080"), InstanceID: env("INSTANCE_ID", fmt.Sprintf("%s-%d", host, os.Getpid())),
		OIDCIssuer: os.Getenv("OIDC_ISSUER"), OIDCFetchIssuer: os.Getenv("OIDC_FETCH_ISSUER"), OIDCAudience: env("OIDC_AUDIENCE", "jungle-api"),
		AWSRegion: env("AWS_REGION", "us-east-1"), SQSEndpoint: os.Getenv("SQS_ENDPOINT"),
		InputQueue: env("SQS_INPUT_QUEUE", "wager-transactions.fifo"), OutputQueue: env("SQS_OUTPUT_QUEUE", "wager-events.fifo"), InputDLQ: env("SQS_INPUT_DLQ", "wager-transactions-dlq.fifo"), OutputDLQ: env("SQS_OUTPUT_DLQ", "wager-events-dlq.fifo"),
		TrustedProviders: strings.Split(env("SQS_TRUSTED_PROVIDERS", "provider-a,provider-b"), ","),
		Concurrency:      4, PublishBatch: 100, MaxConns: 20, ProcessTimeout: 30 * time.Second, VisibilityTimeout: 60 * time.Second, ShutdownTimeout: 45 * time.Second, Lease: 60 * time.Second, PollInterval: time.Second, ReferenceTTL: 24 * time.Hour, ReferenceMaxAttempts: 12,
	}
	if raw := os.Getenv("OIDC_ALLOW_INSECURE_HTTP"); raw != "" {
		c.OIDCAllowHTTP, err = strconv.ParseBool(raw)
		if err != nil {
			return c, fmt.Errorf("OIDC_ALLOW_INSECURE_HTTP: %w", err)
		}
	}
	for key, p := range map[string]*int{"WORKER_CONCURRENCY": &c.Concurrency, "OUTBOX_PUBLISH_BATCH": &c.PublishBatch, "DB_MAX_CONNS": &c.MaxConns, "REFERENCE_MAX_ATTEMPTS": &c.ReferenceMaxAttempts} {
		if raw := os.Getenv(key); raw != "" {
			*p, err = strconv.Atoi(raw)
			if err != nil {
				return c, fmt.Errorf("%s: %w", key, err)
			}
		}
	}
	for key, p := range map[string]*time.Duration{"PROCESS_TIMEOUT": &c.ProcessTimeout, "SQS_VISIBILITY_TIMEOUT": &c.VisibilityTimeout, "SHUTDOWN_TIMEOUT": &c.ShutdownTimeout, "CLAIM_LEASE": &c.Lease, "POLL_INTERVAL": &c.PollInterval, "REFERENCE_TTL": &c.ReferenceTTL} {
		if raw := os.Getenv(key); raw != "" {
			*p, err = time.ParseDuration(raw)
			if err != nil {
				return c, fmt.Errorf("%s: %w", key, err)
			}
		}
	}
	if c.DatabaseURL == "" || c.OIDCIssuer == "" {
		return c, errors.New("DATABASE_URL and OIDC_ISSUER are required")
	}
	if c.Concurrency < 1 || c.Concurrency > 10 || c.ReferenceMaxAttempts < 1 || c.ProcessTimeout <= 0 || c.PollInterval <= 0 || c.ReferenceTTL <= 0 {
		return c, errors.New("invalid worker bounds")
	}
	// The ceiling mirrors the store's own ClaimOutbox guard. Accepting a larger
	// value here would pass startup and then make every claim fail forever, so
	// the publisher would go permanently silent while the ledger kept committing.
	if c.PublishBatch < 1 || c.PublishBatch > application.MaxOutboxBatch {
		return c, fmt.Errorf("OUTBOX_PUBLISH_BATCH must be between 1 and %d", application.MaxOutboxBatch)
	}
	if c.MaxConns < 2 || c.MaxConns > 50 {
		return c, errors.New("DB_MAX_CONNS must be between 2 and 50; max_replicas * DB_MAX_CONNS must still fit max_connections")
	}
	// The orchestrator's own kill deadline has to sit above ShutdownTimeout or
	// the drain is cut short: Compose uses stop_grace_period, Kubernetes uses
	// terminationGracePeriodSeconds, whose default of 30s is BELOW the 45s
	// default here and would SIGKILL mid-drain. Nothing in this process can read
	// that deadline, so it is a deployment obligation, not a validation.
	if c.ProcessTimeout+10*time.Second >= c.VisibilityTimeout || c.ShutdownTimeout >= c.VisibilityTimeout || c.ShutdownTimeout <= c.ProcessTimeout || c.Lease < c.ProcessTimeout+10*time.Second {
		return c, errors.New("require process+10s < visibility, process < shutdown < visibility and lease >= process+10s")
	}
	if c.VisibilityTimeout%time.Second != 0 || c.VisibilityTimeout > 12*time.Hour {
		return c, errors.New("visibility must be integral seconds and <= 12h")
	}
	for _, p := range c.TrustedProviders {
		if p == "" || strings.TrimSpace(p) != p {
			return c, errors.New("invalid trusted provider allowlist")
		}
	}
	return c, nil
}
func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
