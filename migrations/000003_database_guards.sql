-- +goose Up
-- +goose StatementBegin
CREATE FUNCTION protect_ledger() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 RAISE EXCEPTION 'wallet ledger is append-only' USING ERRCODE='23514';
END $$;
-- +goose StatementEnd
CREATE TRIGGER ledger_append_only BEFORE UPDATE OR DELETE ON wallet_ledger_entry FOR EACH ROW EXECUTE FUNCTION protect_ledger();
-- +goose StatementBegin
CREATE FUNCTION protect_outbox_snapshot() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF ROW(NEW.event_id,NEW.event_type,NEW.event_version,NEW.aggregate_type,NEW.aggregate_id,NEW.correlation_id,NEW.causation_id,NEW.payload,NEW.occurred_at)
 IS DISTINCT FROM ROW(OLD.event_id,OLD.event_type,OLD.event_version,OLD.aggregate_type,OLD.aggregate_id,OLD.correlation_id,OLD.causation_id,OLD.payload,OLD.occurred_at) THEN
  RAISE EXCEPTION 'outbox snapshot is immutable' USING ERRCODE='23514';
 END IF; RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER outbox_snapshot BEFORE UPDATE ON outbox FOR EACH ROW EXECUTE FUNCTION protect_outbox_snapshot();
-- +goose StatementBegin
CREATE FUNCTION protect_wager() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF OLD.status IN ('PROCESSED','REJECTED','FAILED') THEN
  RAISE EXCEPTION 'terminal wager is immutable' USING ERRCODE='23514';
 END IF;
 IF ROW(NEW.id,NEW.origin,NEW.provider_id,NEW.external_transaction_id,NEW.idempotency_key,NEW.payload_hash,NEW.wallet_id,NEW.player_id,NEW.round_id,NEW.game_id,NEW.kind,NEW.amount_minor,NEW.currency,NEW.reference_external_transaction_id,NEW.created_at)
 IS DISTINCT FROM ROW(OLD.id,OLD.origin,OLD.provider_id,OLD.external_transaction_id,OLD.idempotency_key,OLD.payload_hash,OLD.wallet_id,OLD.player_id,OLD.round_id,OLD.game_id,OLD.kind,OLD.amount_minor,OLD.currency,OLD.reference_external_transaction_id,OLD.created_at) THEN
  RAISE EXCEPTION 'wager identity is immutable' USING ERRCODE='23514';
 END IF; RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER wager_guard BEFORE UPDATE ON wager_transaction FOR EACH ROW EXECUTE FUNCTION protect_wager();
-- +goose StatementBegin
CREATE FUNCTION validate_wager_commit() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE w wager_transaction; e wallet_ledger_entry;
BEGIN
 SELECT * INTO w FROM wager_transaction WHERE id=NEW.id;
 IF w.status='PENDING' THEN RAISE EXCEPTION 'PENDING cannot commit without durable recovery' USING ERRCODE='23514'; END IF;
 SELECT * INTO e FROM wallet_ledger_entry WHERE transaction_id=w.id AND wallet_id=w.wallet_id;
 IF w.status='PROCESSED' AND w.kind<>'LOSS' THEN
  IF e.id IS NULL OR e.amount_minor<>w.amount_minor OR e.balance_after_minor<>w.result_balance_minor THEN
   RAISE EXCEPTION 'processed wager requires matching ledger' USING ERRCODE='23514';
  END IF;
 ELSIF e.id IS NOT NULL THEN RAISE EXCEPTION 'nonfinancial wager cannot have ledger' USING ERRCODE='23514';
 END IF; RETURN NULL;
END $$;
-- +goose StatementEnd
CREATE CONSTRAINT TRIGGER wager_commit_guard AFTER INSERT OR UPDATE ON wager_transaction DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION validate_wager_commit();
-- +goose StatementBegin
CREATE FUNCTION validate_inbox_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF EXISTS(SELECT 1 FROM inbox WHERE consumer_name=NEW.consumer_name AND message_id=NEW.message_id AND (completed_at IS NULL OR wager_transaction_id IS NULL)) THEN
  RAISE EXCEPTION 'inbox must complete with its durable wager' USING ERRCODE='23514';
 END IF; RETURN NULL;
END $$;
-- +goose StatementEnd
CREATE CONSTRAINT TRIGGER inbox_commit_guard AFTER INSERT ON inbox DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION validate_inbox_commit();
CREATE INDEX outbox_poll ON outbox(next_attempt_at,occurred_at) WHERE published_at IS NULL;
CREATE INDEX reference_poll ON wager_transaction(next_reference_attempt_at,created_at) WHERE status='PENDING_REFERENCE';
-- +goose Down
DROP INDEX reference_poll;
DROP INDEX outbox_poll;
DROP TRIGGER inbox_commit_guard ON inbox;
DROP FUNCTION validate_inbox_commit();
DROP TRIGGER wager_commit_guard ON wager_transaction;
DROP FUNCTION validate_wager_commit();
DROP TRIGGER wager_guard ON wager_transaction;
DROP FUNCTION protect_wager();
DROP TRIGGER outbox_snapshot ON outbox;
DROP FUNCTION protect_outbox_snapshot();
DROP TRIGGER ledger_append_only ON wallet_ledger_entry;
DROP FUNCTION protect_ledger();
