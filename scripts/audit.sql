-- Global financial invariant auditor. Read-only, and deliberately independent of
-- the application: after a chaos scenario the API may be down, and that is
-- precisely when the ledger most needs checking.
--
-- Emits one row per finding as (severity, check_name, detail). FAIL means money or a
-- durable guarantee broke. WARN means an operational threshold was crossed but
-- the system is still allowed to recover on its own. No rows means a clean pass.
--
-- Sums run in NUMERIC because historical ledger totals can exceed int64 even
-- when the net balance is perfectly valid; only the comparison is exact.
--
-- Parameters (psql -v): stale_event_age, stale_reference_age.
--
-- Coverage note: several checks duplicate a constraint or trigger and can only
-- fire if a migration dropped one -- they are the dropped-guard detector. The
-- checks that cover something the schema does NOT enforce are conservation,
-- ledger_chain, result_snapshot, reversal_mismatch, result_currency,
-- duplicate_event and event_not_emitted. A clean run means those held too.
--
-- The single-statement shape is load-bearing: one statement takes one READ
-- COMMITTED snapshot, so every check sees the same instant. Splitting this into
-- several statements would reintroduce the skew it exists to avoid.

WITH
-- A wallet's balance must equal everything ever credited minus everything
-- debited. A lost update shows up here and almost nowhere else.
conservation AS (
  SELECT w.id, w.balance_minor,
         COALESCE(SUM(CASE l.direction WHEN 'CREDIT' THEN l.amount_minor::numeric
                                       ELSE -l.amount_minor::numeric END), 0) AS rebuilt
  FROM wallet w
  LEFT JOIN wallet_ledger_entry l ON l.wallet_id = w.id
  GROUP BY w.id, w.balance_minor
),
entries AS (
  SELECT t.id AS transaction_id, t.kind, t.status, count(l.id) AS n
  FROM wager_transaction t
  LEFT JOIN wallet_ledger_entry l ON l.transaction_id = t.id
  GROUP BY t.id, t.kind, t.status
),
findings AS (
  SELECT 'FAIL' AS severity, 'conservation' AS check_name,
         format('wallet=%s stored=%s rebuilt=%s', id, balance_minor, rebuilt) AS detail
  FROM conservation WHERE balance_minor::numeric <> rebuilt

  UNION ALL
  SELECT 'FAIL', 'negative_balance', format('wallet=%s balance=%s', id, balance_minor)
  FROM wallet WHERE balance_minor < 0

  UNION ALL
  SELECT 'FAIL', 'ledger_cardinality',
         format('transaction=%s kind=%s status=%s entries=%s', transaction_id, kind, status, n)
  FROM entries
  WHERE (status = 'PROCESSED' AND kind <> 'LOSS' AND n <> 1)
     OR ((status <> 'PROCESSED' OR kind = 'LOSS') AND n <> 0)

  UNION ALL
  SELECT 'FAIL', 'ledger_arithmetic',
         format('entry=%s transaction=%s amount=%s before=%s after=%s direction=%s',
                l.id, l.transaction_id, l.amount_minor, l.balance_before_minor, l.balance_after_minor, l.direction)
  FROM wallet_ledger_entry l
  JOIN wager_transaction t ON t.id = l.transaction_id
  WHERE l.amount_minor <> t.amount_minor
     OR l.currency <> t.currency
     OR l.wallet_id <> t.wallet_id
     OR (l.direction = 'DEBIT'  AND l.balance_after_minor <> l.balance_before_minor - l.amount_minor)
     OR (l.direction = 'CREDIT' AND l.balance_after_minor <> l.balance_before_minor + l.amount_minor)

  -- The database trigger already enforces this; auditing it again is what
  -- catches a trigger that was dropped by a bad migration or a manual fix.
  UNION ALL
  SELECT 'FAIL', 'ledger_direction',
         format('entry=%s kind=%s direction=%s reference_kind=%s', l.id, t.kind, l.direction, ref.kind)
  FROM wallet_ledger_entry l
  JOIN wager_transaction t ON t.id = l.transaction_id
  LEFT JOIN wager_transaction ref ON ref.id = t.reference_transaction_id
  WHERE (t.kind IN ('OPENING','WIN','REFUND') AND l.direction <> 'CREDIT')
     OR (t.kind = 'BET' AND l.direction <> 'DEBIT')
     OR (t.kind = 'ROLLBACK' AND ref.status = 'PROCESSED' AND ref.kind = 'BET'             AND l.direction <> 'CREDIT')
     OR (t.kind = 'ROLLBACK' AND ref.status = 'PROCESSED' AND ref.kind IN ('WIN','REFUND') AND l.direction <> 'DEBIT')

  -- A replay must be able to return the original result, so the snapshot stored
  -- on the newest processed wager has to still describe the wallet.
  UNION ALL
  SELECT 'FAIL', 'result_snapshot',
         format('wallet=%s balance=%s version=%s last=%s snapshot=%s/%s',
                w.id, w.balance_minor, w.version, t.id, t.result_balance_minor, t.result_wallet_version)
  FROM wallet w
  JOIN LATERAL (
    SELECT id, result_balance_minor, result_wallet_version
    FROM wager_transaction
    WHERE wallet_id = w.id AND status = 'PROCESSED' AND result_wallet_version IS NOT NULL
    ORDER BY result_wallet_version DESC LIMIT 1
  ) t ON TRUE
  WHERE t.result_balance_minor <> w.balance_minor OR t.result_wallet_version <> w.version

  UNION ALL
  SELECT 'FAIL', 'double_reversal',
         format('reference=%s reversals=%s', reference_transaction_id, count(*))
  FROM wager_transaction
  WHERE status = 'PROCESSED' AND kind IN ('REFUND','ROLLBACK') AND reference_transaction_id IS NOT NULL
  GROUP BY reference_transaction_id HAVING count(*) > 1

  UNION ALL
  SELECT 'FAIL', 'identity_uniqueness',
         format('provider=%s idempotency_key=%s rows=%s', provider_id, idempotency_key, count(*))
  FROM wager_transaction
  WHERE provider_id IS NOT NULL AND idempotency_key IS NOT NULL
  GROUP BY provider_id, idempotency_key HAVING count(*) > 1

  UNION ALL
  SELECT 'FAIL', 'identity_uniqueness',
         format('provider=%s external_transaction_id=%s rows=%s', provider_id, external_transaction_id, count(*))
  FROM wager_transaction
  WHERE provider_id IS NOT NULL AND external_transaction_id IS NOT NULL
  GROUP BY provider_id, external_transaction_id HAVING count(*) > 1

  -- PENDING is an in-transaction state. Finding one committed means a crash
  -- left a durable row the recovery path does not know how to finish.
  UNION ALL
  SELECT 'FAIL', 'incomplete_terminal',
         format('transaction=%s status=%s failure_code=%s completed_at=%s', id, status, failure_code, completed_at)
  FROM wager_transaction
  WHERE status = 'PENDING'
     OR (status IN ('PROCESSED','REJECTED','FAILED') AND completed_at IS NULL)
     OR (status IN ('REJECTED','FAILED') AND failure_code IS NULL)

  -- Emission is audited by count equality per (wallet, event type), which needs
  -- no JSON at all. Three schema facts make that exact: aggregate_id is always
  -- the wallet id, event_type is a 1:1 function of the terminal status, and a
  -- terminal status is immutable, so exactly one such event exists per wager.
  -- The previous form extracted payload->'data'->>'transactionId' from every
  -- outbox row; at three million rows that was 60s and 69s and it was what
  -- exhausted the parallel workers' shared memory. Too few events is a lost
  -- emission, too many is a duplicate, and one check now covers both.
  UNION ALL
  SELECT 'FAIL', 'event_count_mismatch',
         format('wallet=%s type=%s wagers=%s events=%s',
                coalesce(w.wallet_id::text, e.aggregate_id::text),
                coalesce(w.event_type, e.event_type),
                coalesce(w.total, 0), coalesce(e.total, 0))
  FROM (
    SELECT wallet_id, 'WagerTransaction' || initcap(lower(status)) AS event_type, count(*) AS total
    FROM wager_transaction WHERE status IN ('PROCESSED','REJECTED','FAILED')
    GROUP BY 1, 2
  ) w
  FULL JOIN (
    SELECT aggregate_id, event_type, count(*) AS total FROM outbox
    WHERE event_type IN ('WagerTransactionProcessed','WagerTransactionRejected','WagerTransactionFailed')
    GROUP BY 1, 2
  ) e ON e.aggregate_id = w.wallet_id AND e.event_type = w.event_type
  WHERE coalesce(w.total, 0) <> coalesce(e.total, 0)

  UNION ALL
  SELECT 'FAIL', 'incomplete_inbox',
         format('consumer=%s message=%s transaction=%s completed=%s',
                consumer_name, message_id, coalesce(wager_transaction_id::text,'-'), coalesce(completed_at::text,'-'))
  FROM inbox WHERE completed_at IS NULL OR wager_transaction_id IS NULL

  -- Reversal amount and identity are enforced in Go and nowhere in the schema.
  -- A rollback crediting more than its reference satisfies conservation,
  -- cardinality, arithmetic and direction: this is where money can be invented.
  UNION ALL
  SELECT 'FAIL', 'reversal_mismatch',
         format('transaction=%s kind=%s amount=%s reference=%s reference_amount=%s reference_status=%s',
                t.id, t.kind, t.amount_minor, r.id, r.amount_minor, r.status)
  FROM wager_transaction t JOIN wager_transaction r ON r.id = t.reference_transaction_id
  WHERE t.status = 'PROCESSED' AND t.kind IN ('REFUND','ROLLBACK')
    AND (r.amount_minor <> t.amount_minor OR r.wallet_id <> t.wallet_id
         OR r.currency <> t.currency OR r.status <> 'PROCESSED')

  -- Conservation is a sum, so an interleaving that nets out slips past it. The
  -- chain does not: it is what tears if the per-wallet lock is ever bypassed.
  UNION ALL
  SELECT 'FAIL', 'ledger_chain',
         format('wallet=%s entry=%s before=%s previous_after=%s',
                wallet_id, id, balance_before_minor, coalesce(previous_after, 0))
  FROM (
    SELECT wallet_id, id, balance_before_minor,
           lag(balance_after_minor) OVER (PARTITION BY wallet_id ORDER BY created_at, id) AS previous_after
    FROM wallet_ledger_entry
  ) chain
  WHERE balance_before_minor <> COALESCE(previous_after, 0)

  -- result_currency carries no constraint at all, and a NULL makes the read
  -- path return a zero balance without erroring: a silent wrong answer.
  UNION ALL
  SELECT 'FAIL', 'result_currency',
         format('transaction=%s status=%s result_currency=%s expected=%s',
                id, status, coalesce(result_currency,'-'), currency)
  FROM wager_transaction
  WHERE status IN ('PROCESSED','REJECTED','FAILED') AND result_currency IS DISTINCT FROM currency

  -- Warnings: the system is still permitted to recover from these on its own,
  -- so they are operational signals, not verdicts about correctness.
  -- Every WARN branch is bounded at the source. A chaos run legitimately leaves
  -- a backlog in the millions, and the outer LIMIT cannot help: a UNION ALL
  -- materializes every branch before it sorts. Twenty examples say as much as a
  -- million, and the gauge already carries the count.
  UNION ALL
  SELECT 'WARN', 'stale_unpublished_event',
         format('event=%s type=%s attempts=%s age=%s error=%s',
                event_id, event_type, attempts, age(now(), occurred_at), coalesce(last_error,'-'))
  FROM (SELECT event_id, event_type, attempts, occurred_at, last_error FROM outbox
        WHERE published_at IS NULL AND occurred_at < now() - :'stale_event_age'::interval
        LIMIT 20) stale

  UNION ALL
  SELECT 'WARN', 'leaked_claim', format('surface=outbox event=%s owner=%s until=%s', event_id, claimed_by, claimed_until)
  FROM (SELECT event_id, claimed_by, claimed_until FROM outbox
        WHERE published_at IS NULL AND claimed_until IS NOT NULL AND claimed_until < now()
        LIMIT 20) leaked

  UNION ALL
  SELECT 'WARN', 'leaked_claim', format('surface=reference transaction=%s owner=%s until=%s', id, reference_claimed_by, reference_claimed_until)
  FROM (SELECT id, reference_claimed_by, reference_claimed_until FROM wager_transaction
        WHERE status = 'PENDING_REFERENCE' AND reference_claimed_until IS NOT NULL AND reference_claimed_until < now()
        LIMIT 20) leaked_reference

  UNION ALL
  SELECT 'WARN', 'stale_pending_reference',
         format('transaction=%s attempts=%s waiting=%s', id, reference_attempts, age(now(), created_at))
  FROM (SELECT id, reference_attempts, created_at FROM wager_transaction
        WHERE status = 'PENDING_REFERENCE' AND created_at < now() - :'stale_reference_age'::interval
        LIMIT 20) stale_reference
)
SELECT severity, check_name, detail
FROM findings
ORDER BY severity, check_name, detail
LIMIT 500;
