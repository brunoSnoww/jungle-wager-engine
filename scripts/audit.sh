#!/usr/bin/env bash
# Runs the global invariant auditor against the live database and exits non-zero
# on any FAIL. It touches nothing: chaos scenarios use it as their verdict, and
# it works with the API completely down.
#
# Usage: scripts/audit.sh [label]
#   STALE_EVENT_AGE     warn above this unpublished-event age (default 2 minutes)
#   STALE_REFERENCE_AGE warn above this pending-reference age (default 1 hour)

# shellcheck source=scripts/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

need psql

LABEL="${1:-audit}"
STALE_EVENT_AGE="${STALE_EVENT_AGE:-2 minutes}"
STALE_REFERENCE_AGE="${STALE_REFERENCE_AGE:-1 hour}"

mkdir -p "$REPORT_DIR" || inconclusive "$LABEL: cannot create the report directory"
OUT="$REPORT_DIR/audit-$(date +%Y%m%dT%H%M%S)-${LABEL//[^A-Za-z0-9_-]/_}.txt"

# Unreachable is inconclusive for the same reason a failed scan is: no finding
# was made either way, and exiting 1 here would accuse the ledger.
if ! psql "$DATABASE_URL" -X -q -c 'SELECT 1' >/dev/null 2>&1; then
  inconclusive "$LABEL: cannot reach PostgreSQL, so nothing was verified"
fi

# Exit 1 means the ledger is wrong. Exit 3 means the auditor could not answer.
# Collapsing the two lets an environment failure be reported as a financial
# violation, which is the worst possible confusion for a chaos run: it accuses
# the system of losing money when the truth is that nobody looked.
if ! psql "$DATABASE_URL" -X -A -t -q \
  -v ON_ERROR_STOP=1 \
  -v stale_event_age="$STALE_EVENT_AGE" \
  -v stale_reference_age="$STALE_REFERENCE_AGE" \
  -F '|' -f "$ROOT/scripts/audit.sql" > "$OUT" 2> "$OUT.err"; then
  sed 's/^/          /' "$OUT.err" >&2
  inconclusive "$LABEL: the auditor could not run, so nothing was verified"
fi
rm -f "$OUT.err"

FAILS="$(grep -c '^FAIL|' "$OUT" 2>/dev/null || true)"; FAILS="${FAILS:-0}"
WARNS="$(grep -c '^WARN|' "$OUT" 2>/dev/null || true)"; WARNS="${WARNS:-0}"

if [ "$WARNS" -gt 0 ]; then
  warn "$LABEL: $WARNS operational warning(s)"
  grep -m 10 '^WARN|' "$OUT" | sed 's/^/          /'
fi

if [ "$FAILS" -gt 0 ]; then
  printf '%s[FAIL]%s %s: %s financial invariant violation(s) -- full report: %s\n' "$C_RED" "$C_OFF" "$LABEL" "$FAILS" "$OUT" >&2
  grep -m 20 '^FAIL|' "$OUT" | sed 's/^/          /' >&2
  exit 1
fi

ok "$LABEL: every financial invariant holds ($WARNS warning(s)) -- $OUT"
