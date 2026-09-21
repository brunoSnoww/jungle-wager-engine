-- +goose Up
CREATE TABLE inbox (
 consumer_name text NOT NULL CHECK(length(consumer_name)>0), message_id text NOT NULL CHECK(length(message_id)>0),
 payload_hash bytea NOT NULL CHECK(octet_length(payload_hash)=32), wager_transaction_id uuid REFERENCES wager_transaction(id),
 received_at timestamptz NOT NULL DEFAULT now(), completed_at timestamptz,
 PRIMARY KEY(consumer_name,message_id)
);
CREATE TABLE outbox (
 event_id uuid PRIMARY KEY DEFAULT uuidv7(), event_type text NOT NULL,
 event_version integer NOT NULL CHECK(event_version=1),
 aggregate_type text NOT NULL CHECK(aggregate_type IN ('Wallet','WagerTransaction')),
 aggregate_id uuid NOT NULL, correlation_id text NOT NULL, causation_id text,
 payload jsonb NOT NULL, occurred_at timestamptz NOT NULL,
 attempts integer NOT NULL DEFAULT 0 CHECK(attempts>=0), next_attempt_at timestamptz NOT NULL DEFAULT now(),
 claimed_by text, claim_token uuid, claimed_until timestamptz, published_at timestamptz, last_error text,
 CHECK(payload->>'eventId'=event_id::text), CHECK(payload->>'eventType'=event_type),
 CHECK((payload->>'version')::integer=event_version), CHECK(payload->>'aggregateId'=aggregate_id::text),
 CHECK((claim_token IS NULL AND claimed_by IS NULL AND claimed_until IS NULL)
  OR (claim_token IS NOT NULL AND claimed_by IS NOT NULL AND claimed_until IS NOT NULL))
);
-- +goose Down
DROP TABLE outbox;
DROP TABLE inbox;
