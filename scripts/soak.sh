#!/usr/bin/env bash
# Runs steady load while injecting failure modes underneath it, auditing after
# each one, and writes a single report.
#
# This is the script that answers the real question. A load run alone says how
# fast; a chaos run alone says whether it crashed. Only the two together, with
# the ledger audited between faults, say whether the service is safe to put in
# front of money.
#
# Usage: scripts/soak.sh [scenario ...]
#   RATE      offered requests per second (default: budget from profile.sh)
#   WALLETS   distinct wallets (default 32)
#   SETTLE    seconds between scenarios (default 25)
#   DRY_RUN=1 print the plan and exit without touching anything

# shellcheck source=scripts/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

need docker
need psql
need go
# Without jq the profile verdict parses as 'unknown' and the stop gate silently
# disappears, which is worse than refusing to run.
need jq

DEFAULT_SCENARIOS=(kill-app pause-db drop-db-conns partition-sqs pause-sqs flap-sqs partition-idp squeeze-app lease-steal compound kill-db rolling-restart)
if [ "$#" -gt 0 ]; then SCENARIOS=("$@"); else SCENARIOS=("${DEFAULT_SCENARIOS[@]}"); fi

SETTLE="${SETTLE:-25}"
WALLETS="${WALLETS:-32}"
# Each scenario gets its fault window plus a settle gap; the load must outlive
# all of them or the last faults land after the generator already stopped.
# Each scenario costs its fault window plus chaos.sh's own settle and audit,
# plus the recovery waits (pg_isready loops, readiness budgets). Budgeting the
# fault window alone let the load stop before the last scenarios were injected.
PER_SCENARIO=$(( 20 + SETTLE + 240 ))
DURATION=$(( PER_SCENARIO * ${#SCENARIOS[@]} + 60 ))

PROFILE_JSON="$("$ROOT/scripts/profile.sh" --json || true)"
VERDICT="$(echo "$PROFILE_JSON" | jq -r .verdict 2>/dev/null || echo unknown)"
RATE="${RATE:-$(echo "$PROFILE_JSON" | jq -r .budget.suggested_rate 2>/dev/null || echo 200)}"

mkdir -p "$REPORT_DIR"
STAMP="$(date +%Y%m%dT%H%M%S)"
REPORT="$REPORT_DIR/soak-$STAMP.md"

cat <<PLAN
soak plan
  scenarios : ${SCENARIOS[*]}
  rate      : ${RATE} rps
  wallets   : ${WALLETS}
  duration  : ${DURATION}s (${#SCENARIOS[@]} scenarios x ${PER_SCENARIO}s + 60s tail)
  report    : ${REPORT}
  profile   : ${VERDICT}
PLAN

if [ "${DRY_RUN:-0}" = 1 ]; then
  echo "DRY_RUN=1: nothing was executed"
  exit 0
fi

case "$VERDICT" in
  stop|unknown) fail "profile verdict is '$VERDICT'; free Docker memory or raise its limits before soaking" ;;
  caution) warn "profile verdict is 'caution'; results will be indicative, not reproducible" ;;
esac

await_all_ready 120 || fail "the stack is not ready; bring it up with 'make replicas' first"

log "baseline audit before any load"
"$ROOT/scripts/audit.sh" baseline || fail "the ledger is already inconsistent; fix that before soaking"

{
  echo "# Soak report $STAMP"
  echo
  echo '## Environment'
  echo '```'
  "$ROOT/scripts/profile.sh" || true
  echo '```'
  echo
  echo "## Plan"
  echo "- rate: ${RATE} rps across three replicas"
  echo "- wallets: ${WALLETS}"
  echo "- scenarios: ${SCENARIOS[*]}"
  echo
  echo "## Timeline"
} > "$REPORT"

LOAD_LOG="$REPORT_DIR/soak-$STAMP-load.log"
log "starting steady load for ${DURATION}s (log: $LOAD_LOG)"
LOAD_PID=""
DRAIN_PID=""
stop_load() {
  if [ -n "$DRAIN_PID" ]; then
    kill -- -"$DRAIN_PID" 2>/dev/null || kill "$DRAIN_PID" 2>/dev/null || true
    DRAIN_PID=""
  fi
  [ -n "$LOAD_PID" ] || return 0
  # Kill the process group: $! is the subshell wrapper, and killing only that
  # leaves `go test` reparented and still driving traffic.
  kill -- -"$LOAD_PID" 2>/dev/null || kill "$LOAD_PID" 2>/dev/null || true
}
trap 'stop_load; exit 130' INT
trap 'stop_load; exit 143' TERM
trap stop_load EXIT
set -m
(
  cd "$ROOT/tests/load" && \
  TEST_DATABASE_URL="$DATABASE_URL" LOAD_RATE="$RATE" LOAD_WALLETS="$WALLETS" LOAD_DURATION="${DURATION}s" \
  go test -tags=load -count=1 -v -timeout $(( DURATION + 900 ))s -run TestSteadyLoadUnderChaos ./...
) > "$LOAD_LOG" 2>&1 &
LOAD_PID=$!

# Nothing in the demo consumes the output queue: it is the provider-facing
# stream, and the provider is not part of the stack. Left alone the emulator
# retains every event it was ever sent -- a measured 2,111,642 messages and
# 5.5GiB after one hour at 400rps -- and its cost per send climbs with the
# retained set. A queue drained into a fresh one sent 1255/s; the same emulator
# holding two million sent about 250/s. Without this the soak measures the
# emulator decaying, not the engine, and the number it reports is worthless.
#
# Discarding is the right disposal here. The engine's contract is that a
# committed event reaches the broker, and outbox.published_at in PostgreSQL is
# what the drain assertion reads; what a downstream would do with the message
# afterwards is not under test. Purge is one API call a minute, which is also
# the rate AWS allows, so it stays honest against the real service.
(
  while :; do
    docker compose exec -T localstack awslocal sqs purge-queue \
      --queue-url "$(docker compose exec -T localstack awslocal sqs get-queue-url \
        --queue-name wager-events.fifo --query QueueUrl --output text 2>/dev/null)" >/dev/null 2>&1 || true
    sleep 60
  done
) >/dev/null 2>&1 &
DRAIN_PID=$!
set +m

log "letting the load reach steady state"
sleep 30

FAILED_SCENARIOS=()
INCONCLUSIVE=()
INJECTED=0
for scenario in "${SCENARIOS[@]}"; do
  if ! kill -0 "$LOAD_PID" 2>/dev/null; then
    warn "the load generator exited early; stopping scenario injection"
    break
  fi
  log "injecting $scenario"
  echo "### $scenario ($(date +%H:%M:%S))" >> "$REPORT"
  rc=0
  "$ROOT/scripts/chaos.sh" "$scenario" 20 >> "$REPORT" 2>&1 || rc=$?
  INJECTED=$(( INJECTED + 1 ))
  if [ "$rc" -eq 0 ]; then
    echo "- verdict: invariants held" >> "$REPORT"
    ok "$scenario: invariants held"
  elif [ "$rc" -eq 3 ]; then
    # The auditor failed to run. That is a hole in the evidence, not a finding
    # about the ledger, and calling it a violation would blame the service for
    # the environment.
    echo "- verdict: **INCONCLUSIVE** (auditor could not run)" >> "$REPORT"
    INCONCLUSIVE+=("$scenario")
    warn "$scenario: inconclusive; the auditor could not run"
  elif [ "$rc" -ge 128 ]; then
    # Signalled, not falsified: recording this as a violation would put a
    # financial verdict on a keyboard interrupt.
    echo "- verdict: aborted by signal ($rc)" >> "$REPORT"
    warn "$scenario: aborted by signal; stopping"
    break
  else
    echo "- verdict: **VIOLATION**" >> "$REPORT"
    FAILED_SCENARIOS+=("$scenario")
    warn "$scenario: invariant violation recorded; continuing to gather the full picture"
  fi
  sleep "$SETTLE"
done

log "waiting for the load profile to finish and assert its own contract"
LOAD_STATUS=0
wait "$LOAD_PID" || LOAD_STATUS=$?
LOAD_PID=""
trap - EXIT INT TERM
case "$LOAD_STATUS" in
  130|143) fail "the load generator was signalled; this run is aborted, not a verdict" ;;
esac

{
  echo
  echo "## Load profile"
  echo '```'
  grep -E '^\s+(chaos_test|load_test)\.go:|^(---|ok|FAIL)' "$LOAD_LOG" | tail -40 || true
  echo '```'
  echo
  echo "## Final audit"
  echo '```'
} >> "$REPORT"

FINAL_STATUS=0
"$ROOT/scripts/audit.sh" final >> "$REPORT" 2>&1 || FINAL_STATUS=$?
echo '```' >> "$REPORT"

{
  echo
  echo "## Verdict"
  echo "- load profile exit: $LOAD_STATUS"
  echo "- final audit exit: $FINAL_STATUS"
  if [ "${#FAILED_SCENARIOS[@]}" -gt 0 ]; then
    echo "- scenarios that broke an invariant: ${FAILED_SCENARIOS[*]}"
  else
    echo "- every scenario left the ledger consistent"
  fi
  echo "- scenarios injected: $INJECTED of ${#SCENARIOS[@]}"
  echo "- the output queue was purged once a minute: the demo has no provider consuming it, and an emulator holding every event it was ever sent slows down until the run measures the emulator instead of the engine. Delivery is judged by outbox.published_at, not by queue depth."
  if [ "${#INCONCLUSIVE[@]}" -gt 0 ]; then
    echo "- **inconclusive** (auditor could not run): ${INCONCLUSIVE[*]}"
  fi
  if [ "$INJECTED" -lt "${#SCENARIOS[@]}" ]; then
    echo "- **incomplete**: the load ended before every scenario was injected"
  fi
} >> "$REPORT"

log "report written: $REPORT"
if [ "${#FAILED_SCENARIOS[@]}" -gt 0 ] || [ "$LOAD_STATUS" -ne 0 ] || [ "$FINAL_STATUS" -ne 0 ]; then
  fail "soak found problems; read $REPORT"
fi
if [ "$INJECTED" -lt "${#SCENARIOS[@]}" ]; then
  fail "only $INJECTED of ${#SCENARIOS[@]} scenarios were injected; the run is incomplete, not clean"
fi
if [ "${#INCONCLUSIVE[@]}" -gt 0 ]; then
  fail "${#INCONCLUSIVE[@]} scenario(s) could not be audited: ${INCONCLUSIVE[*]} -- unverified is not the same as clean"
fi
ok "soak clean: load contract and every invariant held across $INJECTED scenarios"
