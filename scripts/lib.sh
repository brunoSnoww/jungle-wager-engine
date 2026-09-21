#!/usr/bin/env bash
# Shared helpers. Sourced, never executed.
#
# Targets bash 3.2, which is what macOS ships and what /usr/bin/env bash
# resolves to here: no associative arrays, no mapfile, and every empty-array
# expansion guarded by a length test because 3.2 aborts on them under `set -u`.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_PROJECT="${COMPOSE_PROJECT:-jungle-gaming}"
DATABASE_URL="${TEST_DATABASE_URL:-postgres://jungle:jungle-local@localhost:5433/jungle?sslmode=disable}"
REPORT_DIR="${REPORT_DIR:-$ROOT/.chaos-reports}"

# Docker Desktop keeps its credential helper outside the default PATH; without
# it every compose call fails on an unrelated credential error.
if [ -d "$HOME/.docker/bin" ]; then
  PATH="$HOME/.docker/bin:$PATH"
fi
export PATH

if [ -t 1 ]; then
  C_RED=$'\033[31m'; C_YEL=$'\033[33m'; C_GRN=$'\033[32m'; C_DIM=$'\033[2m'; C_OFF=$'\033[0m'
else
  C_RED=''; C_YEL=''; C_GRN=''; C_DIM=''; C_OFF=''
fi

log()  { printf '%s[%s]%s %s\n' "$C_DIM" "$(date +%H:%M:%S)" "$C_OFF" "$*"; }
warn() { printf '%s[warn]%s %s\n' "$C_YEL" "$C_OFF" "$*" >&2; }
fail() { printf '%s[fail]%s %s\n' "$C_RED" "$C_OFF" "$*" >&2; exit 1; }
# Exit 3 is reserved for "this could not be carried out or verified". Only a real
# finding about the ledger may exit 1, or an environment problem gets reported as
# money being lost -- which is the one mistake a chaos suite must never make.
inconclusive() { printf '%s[inconclusive]%s %s\n' "$C_YEL" "$C_OFF" "$*" >&2; exit 3; }
ok()   { printf '%s[ok]%s %s\n'   "$C_GRN" "$C_OFF" "$*"; }

need() { command -v "$1" >/dev/null 2>&1 || fail "$1 is required but not installed"; }

container() { printf '%s-%s-1' "$COMPOSE_PROJECT" "$1"; }

container_up() {
  [ "$(docker inspect -f '{{.State.Running}}' "$(container "$1")" 2>/dev/null || echo false)" = true ]
}

await_ready() {
  local port="$1" budget="${2:-90}" deadline=$(( SECONDS + ${2:-90} ))
  [ "$budget" -le 0 ] && return 1
  while [ "$SECONDS" -lt "$deadline" ]; do
    if [ "$(curl -s -o /dev/null -w '%{http_code}' -m 3 "http://localhost:$port/health/ready" 2>/dev/null)" = 200 ]; then
      return 0
    fi
    sleep 1
  done
  return 1
}

# One budget for the whole set, not one per port: three ports each given the
# full timeout would block three times as long as the caller asked for.
await_all_ready() {
  local deadline=$(( SECONDS + ${1:-90} )) port
  for port in 8080 8082 8083; do
    await_ready "$port" $(( deadline - SECONDS )) || return 1
  done
}

psql_q() { psql "$DATABASE_URL" -X -A -t -q "$@"; }
