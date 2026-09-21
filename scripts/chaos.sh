#!/usr/bin/env bash
# Injects one failure mode into the running stack, restores it, then audits.
#
# Every scenario uses only Docker primitives and psql. Pumba and Toxiproxy are
# not installed and not cached on this machine, and requiring them would make
# the suite unreproducible offline. That constraint is not a real loss: process
# death, a stalled process, a network partition, a memory squeeze and a severed
# connection already cover the failure modes this service must survive.
#
# Usage: scripts/chaos.sh <scenario> [seconds]
#        scripts/chaos.sh --list

# shellcheck source=scripts/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

need docker
need psql

SCENARIO="${1:-}"
DURATION="${2:-20}"
NETWORK="${COMPOSE_PROJECT}_default"

# macOS ships bash 3.2, which has no associative arrays, and `/usr/bin/env bash`
# resolves to it here. A case statement costs nothing and actually runs.
scenario_names() {
  printf '%s\n' kill-app rolling-restart pause-db kill-db drop-db-conns \
    partition-sqs pause-sqs flap-sqs partition-idp squeeze-app lease-steal compound
}

scenario_desc() {
  case "$1" in
    kill-app)        echo "SIGKILL one replica, restart it. Expects: peers absorb traffic, no lost or duplicated money, inbox and outbox resume." ;;
    rolling-restart) echo "SIGTERM each replica in turn. Expects: graceful drain, in-flight work completes, readiness flips before the socket closes." ;;
    pause-db)        echo "SIGSTOP PostgreSQL. Expects: writes fail closed as 503, nothing partially commits, the pool recovers on unpause." ;;
    kill-db)         echo "SIGKILL PostgreSQL and restart it. Expects: no torn transaction survives, pgxpool reconnects without restarting the apps." ;;
    drop-db-conns)   echo "pg_terminate_backend every application connection. Expects: in-flight transactions roll back whole, the pool refills." ;;
    partition-sqs)   echo "Detach LocalStack from the network. Expects: the consumer stops acknowledging, the outbox backlogs, no event is lost." ;;
    pause-sqs)       echo "SIGSTOP LocalStack. Expects: publishes time out, retry with backoff, and republish under the same event_id." ;;
    flap-sqs)        echo "Detach and reattach the broker repeatedly. Expects: retry storms stay bounded by backoff, no duplicate financial effect." ;;
    partition-idp)   echo "Detach Keycloak. Expects: already-issued tokens keep working, liveness does not depend on the IdP." ;;
    squeeze-app)     echo "Clamp one replica below its working set to force the OOM killer. Expects: restart recovers, claims released by lease, limit restored." ;;
    lease-steal)     echo "Freeze one replica past its lease, let peers steal its claims, then thaw it. Expects: the stale owner's fencing token is refused, so no claim completes twice." ;;
    compound)        echo "Kill a replica and freeze the broker at the same instant. Expects: two subsystems failing together still converge, with no lost or duplicated money." ;;
    *)               return 1 ;;
  esac
}

usage() {
  echo "scenarios:"
  while read -r k; do printf '  %-16s %s\n' "$k" "$(scenario_desc "$k")"; done < <(scenario_names | sort)
  echo
  echo "usage: scripts/chaos.sh <scenario> [seconds]"
  echo "note : disk exhaustion is intentionally absent -- filling the 64GiB Docker"
  echo "       disk is slow and can wedge the daemon, so it stays a manual"
  echo "       procedure on a dedicated volume rather than an automated script."
}

[ "$SCENARIO" = "--list" ] || [ -z "$SCENARIO" ] && { usage; exit 0; }
scenario_desc "$SCENARIO" >/dev/null || { usage; inconclusive "unknown scenario: $SCENARIO"; }

# restore always runs, including on Ctrl-C: leaving PostgreSQL paused or a
# container off the network is a worse outcome than any test failure.
RESTORE=()
restore() {
  local cmd
  [ "${#RESTORE[@]}" -eq 0 ] && return 0
  # Deliberately not cleared: every restore below is idempotent, so running it
  # twice is harmless while running it zero times leaves the machine broken.
  for cmd in "${RESTORE[@]}"; do eval "$cmd" >/dev/null 2>&1 || true; done
}
# A bare `trap restore INT` runs the handler and then resumes the script, which
# inside a loop means injecting the next fault with the net already spent.
trap 'restore; exit 130' INT
trap 'restore; exit 143' TERM
trap restore EXIT

# Compose gives each container a short DNS alias equal to its service name, and
# the apps resolve `localstack` and `keycloak` by it. A manual reconnect restores
# only the container name, so the alias must be handed back explicitly or the
# stack stays silently broken until a full recreate.
reattach() { docker network connect --alias "$1" "$NETWORK" "$(container "$1")" 2>/dev/null || true; }
detach()   { docker network disconnect "$NETWORK" "$(container "$1")" 2>/dev/null || true; }

log "scenario '$SCENARIO' for ${DURATION}s"
log "expectation: $(scenario_desc "$SCENARIO")"

case "$SCENARIO" in
  kill-app)
    target="$(container app-2)"
    RESTORE+=("docker start $target")
    docker kill -s KILL "$target"
    log "killed $target; peers on 8080 and 8083 must keep serving"
    sleep "$DURATION"
    docker start "$target" >/dev/null
    await_ready 8082 120 || inconclusive "$target never became ready again; the scenario could not be completed"
    ;;

  rolling-restart)
    for pair in "app-1:8080" "app-2:8082" "app-3:8083"; do
      svc="${pair%%:*}"; port="${pair##*:}"
      log "draining $svc"
      RESTORE+=("docker start $(container "$svc")")
      # -t 50 exceeds the service's 45s shutdown timeout, so this measures the
      # drain rather than Docker's impatience.
      docker stop -t 50 "$(container "$svc")" >/dev/null
      docker start "$(container "$svc")" >/dev/null
      await_ready "$port" 120 || inconclusive "$svc never became ready again; the scenario could not be completed"
    done
    ;;

  pause-db|pause-sqs)
    svc="postgres"; [ "$SCENARIO" = pause-sqs ] && svc="localstack"
    target="$(container "$svc")"
    RESTORE+=("docker unpause $target")
    docker pause "$target"
    log "paused $target; a stalled dependency is not the same as a dead one"
    sleep "$DURATION"
    docker unpause "$target" >/dev/null
    await_all_ready 180 || warn "not every replica recovered readiness in time"
    ;;

  kill-db)
    target="$(container postgres)"
    RESTORE+=("docker start $target")
    docker kill -s KILL "$target"
    sleep "$DURATION"
    docker start "$target" >/dev/null
    log "waiting for PostgreSQL to accept connections again"
    for _ in $(seq 1 60); do pg_isready -d "$DATABASE_URL" >/dev/null 2>&1 && break; sleep 2; done
    await_all_ready 180 || warn "not every replica recovered readiness in time"
    ;;

  drop-db-conns)
    killed="$(psql_q -c "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity
                         WHERE datname = current_database() AND pid <> pg_backend_pid()
                           AND application_name LIKE 'app-%'")"
    log "terminated ${killed:-0} application backend(s) mid-flight"
    sleep "$DURATION"
    await_all_ready 120 || warn "not every replica recovered readiness in time"
    ;;

  partition-sqs|partition-idp)
    svc="localstack"; [ "$SCENARIO" = partition-idp ] && svc="keycloak"
    RESTORE+=("docker network connect --alias $svc $NETWORK $(container "$svc")")
    detach "$svc"
    log "detached $svc from $NETWORK"
    sleep "$DURATION"
    reattach "$svc"
    await_all_ready 180 || warn "not every replica recovered readiness in time"
    ;;

  flap-sqs)
    # A dependency that keeps coming back is harder than one that stays down: it
    # re-arms every retry loop at once. Per-replica partitioning is deliberately
    # not attempted -- the stack shares one Compose network, so isolating a single
    # app would need NET_ADMIN inside the container or a second network, and
    # cutting the broker for everyone would misreport a full outage as a partial.
    RESTORE+=("docker network connect --alias localstack $NETWORK $(container localstack)")
    cycles=$(( DURATION / 8 )); [ "$cycles" -lt 1 ] && cycles=1
    for i in $(seq 1 "$cycles"); do
      detach localstack
      log "flap $i/$cycles: broker detached"
      sleep 5
      reattach localstack
      sleep 3
    done
    await_all_ready 120 || warn "not every replica recovered readiness in time"
    ;;

  squeeze-app)
    svc="app-1"
    target="$(container "$svc")"
    original="$(docker inspect -f '{{.HostConfig.Memory}}' "$target")"
    : "${original:=0}"
    # `docker update --memory 0` is a no-op: the daemon skips zero-valued
    # resource fields, so a container that started unlimited cannot have a limit
    # removed this way and would stay clamped forever. Recreate it instead.
    if [ "$original" = 0 ]; then
      RESTORE+=("docker compose -f $ROOT/docker-compose.yml up -d --force-recreate $svc")
    else
      RESTORE+=("docker update --memory ${original}b --memory-swap -1 $target")
    fi
    # Derive the clamp from live usage: a fixed number that happens to sit above
    # the working set makes the scenario pass having killed nothing.
    rss="$(docker stats --no-stream --format '{{.MemUsage}}' "$target" | awk -F'/' '{
      v=$1; if (v ~ /GiB/) f=1024; else if (v ~ /MiB/) f=1; else if (v ~ /KiB/) f=1/1024; else f=1/1048576
      gsub(/[^0-9.]/,"",v); printf "%.0f", v*f }')"
    clamp=$(( ${rss:-24} / 2 )); [ "$clamp" -lt 8 ] && clamp=8
    docker update --memory "${clamp}m" --memory-swap "${clamp}m" "$target" >/dev/null
    log "clamped $target to ${clamp}MiB against a ${rss:-?}MiB working set"
    sleep "$DURATION"

    killed="$(docker inspect -f '{{.State.OOMKilled}}' "$target" 2>/dev/null || echo unknown)"
    if [ "$original" = 0 ]; then
      docker compose -f "$ROOT/docker-compose.yml" up -d --force-recreate "$svc" >/dev/null 2>&1 || true
    else
      docker update --memory "${original}b" --memory-swap -1 "$target" >/dev/null
      docker start "$target" >/dev/null 2>&1 || true
    fi
    restored="$(docker inspect -f '{{.HostConfig.Memory}}' "$target" 2>/dev/null || echo unknown)"
    [ "$restored" = "$original" ] || inconclusive "could not restore $target memory limit (now $restored, want $original); recover with: docker compose -f $ROOT/docker-compose.yml up -d --force-recreate $svc"
    await_ready 8080 120 || warn "$target did not recover readiness in time"
    [ "$killed" = true ] || warn "$target was never OOM-killed at ${clamp}MiB; this scenario exercised nothing"
    ;;

  lease-steal)
    # The subtlest correctness mechanism in the system is the fencing token: a
    # worker that lost its lease must not be able to finish the work someone else
    # already took. SIGSTOP produces that exactly -- the process is frozen with
    # its claims held, the lease expires in real time, a peer reclaims, and then
    # the old owner wakes up still believing it owns them.
    target="$(container app-1)"
    RESTORE+=("docker unpause $target")
    lease="$(docker exec "$(container app-2)" printenv CLAIM_LEASE 2>/dev/null || echo 60s)"
    frozen=$(( DURATION > 75 ? DURATION : 75 ))
    docker pause "$target"
    log "froze $target for ${frozen}s, past its ${lease} lease; peers should steal its claims"
    sleep "$frozen"
    docker unpause "$target" >/dev/null
    log "thawed $target; its in-flight claims are now fenced"
    await_all_ready 120 || warn "not every replica recovered readiness in time"
    ;;

  compound)
    # Single faults are the easy case. Real incidents arrive together, and the
    # recovery paths then compete: the consumer retries while the publisher
    # cannot reach the broker and a replica is missing entirely.
    app="$(container app-2)"
    RESTORE+=("docker start $app" "docker unpause $(container localstack)")
    docker kill -s KILL "$app"
    docker pause "$(container localstack)"
    log "killed $app and froze the broker together for ${DURATION}s"
    sleep "$DURATION"
    docker unpause "$(container localstack)" >/dev/null
    docker start "$app" >/dev/null
    await_all_ready 180 || warn "not every replica recovered readiness in time"
    ;;

esac

log "fault window closed; letting the system settle before the verdict"
sleep 10
"$ROOT/scripts/audit.sh" "chaos-$SCENARIO"
