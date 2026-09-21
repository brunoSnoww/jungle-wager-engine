-- +goose Up
-- Validate both insertion paths. A wager-only deferred trigger is insufficient:
-- someone could append a ledger row after a LOSS/rejection had already committed.
-- +goose StatementBegin
CREATE FUNCTION verify_wager_ledger(transaction_id uuid) RETURNS void LANGUAGE plpgsql AS $$
DECLARE w wager_transaction; e wallet_ledger_entry; reference_kind text; expected_direction text;
BEGIN
 SELECT * INTO w FROM wager_transaction WHERE id=transaction_id;
 IF NOT FOUND THEN RAISE EXCEPTION 'ledger wager does not exist' USING ERRCODE='23514'; END IF;
 IF w.status='PENDING' THEN RAISE EXCEPTION 'PENDING cannot commit without durable recovery' USING ERRCODE='23514'; END IF;
 SELECT * INTO e FROM wallet_ledger_entry WHERE wallet_id=w.wallet_id AND wallet_ledger_entry.transaction_id=w.id;
 IF w.status='PROCESSED' AND w.kind<>'LOSS' THEN
  IF e.id IS NULL OR e.amount_minor<>w.amount_minor OR e.balance_after_minor<>w.result_balance_minor THEN
   RAISE EXCEPTION 'processed wager requires matching ledger' USING ERRCODE='23514';
  END IF;
  CASE w.kind
   WHEN 'BET' THEN expected_direction := 'DEBIT';
   WHEN 'OPENING' THEN expected_direction := 'CREDIT';
   WHEN 'WIN' THEN expected_direction := 'CREDIT';
   WHEN 'REFUND' THEN expected_direction := 'CREDIT';
   WHEN 'ROLLBACK' THEN
    SELECT kind INTO reference_kind FROM wager_transaction WHERE id=w.reference_transaction_id AND status='PROCESSED';
    IF reference_kind='BET' THEN expected_direction := 'CREDIT';
    ELSIF reference_kind IN ('WIN','REFUND') THEN expected_direction := 'DEBIT';
    ELSE RAISE EXCEPTION 'rollback requires a processed reversible reference' USING ERRCODE='23514';
    END IF;
  END CASE;
  IF e.direction IS DISTINCT FROM expected_direction THEN
   RAISE EXCEPTION 'ledger direction disagrees with wager kind' USING ERRCODE='23514';
  END IF;
 ELSIF e.id IS NOT NULL THEN
  RAISE EXCEPTION 'nonfinancial wager cannot have ledger' USING ERRCODE='23514';
 END IF;
END $$;
-- +goose StatementEnd
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION validate_wager_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 PERFORM verify_wager_ledger(NEW.id);
 RETURN NULL;
END $$;
-- +goose StatementEnd
-- +goose StatementBegin
CREATE FUNCTION validate_ledger_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 PERFORM verify_wager_ledger(NEW.transaction_id);
 RETURN NULL;
END $$;
-- +goose StatementEnd
CREATE CONSTRAINT TRIGGER ledger_commit_guard AFTER INSERT ON wallet_ledger_entry
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION validate_ledger_commit();

-- +goose Down
DROP TRIGGER ledger_commit_guard ON wallet_ledger_entry;
DROP FUNCTION validate_ledger_commit();
-- Restore version 3 exactly before removing the shared validator.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION validate_wager_commit() RETURNS trigger LANGUAGE plpgsql AS $$
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
DROP FUNCTION verify_wager_ledger(uuid);
