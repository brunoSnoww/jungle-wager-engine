-- +goose Up
-- +goose StatementBegin
DO $$ BEGIN
 IF current_setting('server_version_num')::integer < 180000 OR to_regprocedure('uuidv7()') IS NULL THEN
  RAISE EXCEPTION 'Jungle requires PostgreSQL 18+ with native uuidv7()';
 END IF;
END $$;
-- +goose StatementEnd
CREATE TABLE wallet (
 id uuid PRIMARY KEY DEFAULT uuidv7(),
 player_id uuid NOT NULL CHECK(player_id<>'00000000-0000-0000-0000-000000000000'),
 currency text NOT NULL CHECK(currency IN ('BRL','USD','EUR')),
 balance_minor bigint NOT NULL CHECK(balance_minor>=0),
 version bigint NOT NULL DEFAULT 1 CHECK(version>=1),
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 CONSTRAINT wallet_player_currency_key UNIQUE(player_id,currency),
 UNIQUE(id,player_id), UNIQUE(id,currency)
);
CREATE TABLE wager_transaction (
 id uuid PRIMARY KEY DEFAULT uuidv7(),
 origin text NOT NULL CHECK(origin IN ('INTERNAL','EXTERNAL')),
 provider_id text, external_transaction_id text, idempotency_key text, payload_hash bytea,
 wallet_id uuid NOT NULL REFERENCES wallet(id), player_id uuid NOT NULL,
 round_id text, game_id text,
 kind text NOT NULL CHECK(kind IN ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
 amount_minor bigint NOT NULL CHECK(amount_minor>=0),
 currency text NOT NULL CHECK(currency IN ('BRL','USD','EUR')),
 reference_external_transaction_id text, reference_transaction_id uuid REFERENCES wager_transaction(id),
 status text NOT NULL CHECK(status IN ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
 failure_code text, result_balance_minor bigint CHECK(result_balance_minor>=0), result_currency text,
 result_wallet_version bigint CHECK(result_wallet_version>=1),
 correlation_id text NOT NULL, causation_id text,
 reference_attempts integer NOT NULL DEFAULT 0 CHECK(reference_attempts>=0),
 next_reference_attempt_at timestamptz, reference_claimed_by text,
 reference_claim_token uuid, reference_claimed_until timestamptz,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(), completed_at timestamptz,
 FOREIGN KEY(wallet_id,player_id) REFERENCES wallet(id,player_id),
 CONSTRAINT wager_idempotency_key UNIQUE(provider_id,idempotency_key),
 CONSTRAINT wager_external_id_key UNIQUE(provider_id,external_transaction_id),
 UNIQUE(id,wallet_id,currency), CHECK(reference_transaction_id IS DISTINCT FROM id),
 CONSTRAINT wager_origin_shape CHECK (
  (origin='INTERNAL' AND kind='OPENING' AND provider_id IS NULL AND external_transaction_id IS NULL
   AND idempotency_key IS NULL AND payload_hash IS NULL AND round_id IS NULL AND game_id IS NULL
   AND reference_external_transaction_id IS NULL AND reference_transaction_id IS NULL)
  OR (origin='EXTERNAL' AND kind<>'OPENING' AND length(provider_id)>0 AND length(external_transaction_id)>0
   AND length(idempotency_key)>0 AND octet_length(payload_hash)=32 AND length(round_id)>0 AND length(game_id)>0
   AND provider_id IS NOT NULL AND external_transaction_id IS NOT NULL AND idempotency_key IS NOT NULL
   AND payload_hash IS NOT NULL AND round_id IS NOT NULL AND game_id IS NOT NULL)),
 -- Invalid amounts remain auditable as rejected requests, never financial effects.
 CHECK(status<>'PROCESSED' OR (kind='LOSS' AND amount_minor=0) OR (kind<>'LOSS' AND amount_minor>0)),
 CHECK((status IN ('PROCESSED','REJECTED','FAILED') AND completed_at IS NOT NULL
   AND result_balance_minor IS NOT NULL AND result_wallet_version IS NOT NULL)
  OR (status IN ('PENDING','PENDING_REFERENCE') AND completed_at IS NULL)),
 CHECK((status IN ('REJECTED','FAILED') AND length(failure_code)>0 AND failure_code IS NOT NULL)
   OR (status NOT IN ('REJECTED','FAILED') AND failure_code IS NULL)),
 CHECK(status<>'PENDING_REFERENCE' OR (reference_external_transaction_id IS NOT NULL AND next_reference_attempt_at IS NOT NULL)),
 CHECK((reference_claim_token IS NULL AND reference_claimed_by IS NULL AND reference_claimed_until IS NULL)
  OR (reference_claim_token IS NOT NULL AND reference_claimed_by IS NOT NULL AND reference_claimed_until IS NOT NULL))
);
CREATE UNIQUE INDEX wager_opening_once ON wager_transaction(wallet_id) WHERE kind='OPENING';
CREATE UNIQUE INDEX wager_single_reversal ON wager_transaction(reference_transaction_id)
 WHERE kind IN ('REFUND','ROLLBACK') AND status='PROCESSED';
CREATE TABLE wallet_ledger_entry (
 id uuid PRIMARY KEY DEFAULT uuidv7(), wallet_id uuid NOT NULL REFERENCES wallet(id), transaction_id uuid NOT NULL,
 direction text NOT NULL CHECK(direction IN ('DEBIT','CREDIT')),
 amount_minor bigint NOT NULL CHECK(amount_minor>0), currency text NOT NULL,
 balance_before_minor bigint NOT NULL CHECK(balance_before_minor>=0),
 balance_after_minor bigint NOT NULL CHECK(balance_after_minor>=0), created_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(wallet_id,transaction_id), FOREIGN KEY(wallet_id,currency) REFERENCES wallet(id,currency),
 FOREIGN KEY(transaction_id,wallet_id,currency) REFERENCES wager_transaction(id,wallet_id,currency),
 CHECK(balance_after_minor::numeric=balance_before_minor::numeric+
  CASE direction WHEN 'CREDIT' THEN amount_minor::numeric ELSE -amount_minor::numeric END)
);
CREATE INDEX ledger_cursor ON wallet_ledger_entry(wallet_id,created_at DESC,id DESC);
-- +goose Down
DROP TABLE wallet_ledger_entry;
DROP TABLE wager_transaction;
DROP TABLE wallet;
