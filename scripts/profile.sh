#!/usr/bin/env bash
# Profiles the host and the Docker VM, then decides whether this machine can
# host a trustworthy run right now. It is a gate, not a banner: on this box the
# binding constraint is Docker memory, and a run started without headroom
# measures an OOM-killer, not the service.
#
# Usage: scripts/profile.sh [--json] [--quiet]

# shellcheck source=scripts/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

need docker

JSON=false
for arg in "$@"; do
  case "$arg" in
    --json) JSON=true ;;
    --quiet) exec 1>/dev/null ;;
    *) fail "unknown argument: $arg" ;;
  esac
done

# --- host ---------------------------------------------------------------
if [ "$(uname -s)" = Darwin ]; then
  HOST_CPU_BRAND="$(sysctl -n machdep.cpu.brand_string)"
  HOST_CORES="$(sysctl -n hw.logicalcpu)"
  HOST_PERF="$(sysctl -n hw.perflevel0.logicalcpu 2>/dev/null || echo 0)"
  HOST_EFF="$(sysctl -n hw.perflevel1.logicalcpu 2>/dev/null || echo 0)"
  HOST_MEM_GB=$(( $(sysctl -n hw.memsize) / 1073741824 ))
else
  HOST_CPU_BRAND="$(uname -p)"
  HOST_CORES="$(nproc)"
  HOST_PERF=0; HOST_EFF=0
  HOST_MEM_GB=$(( $(awk '/MemTotal/{print $2}' /proc/meminfo) / 1048576 ))
fi
HOST_LOAD="$(LC_ALL=C uptime | sed 's/.*averages*: //' | awk '{print $1}')"

# --- docker vm ----------------------------------------------------------
docker info >/dev/null 2>&1 || fail "Docker is not reachable; start Docker Desktop first"
VM_CPUS="$(docker info --format '{{.NCPU}}')"
VM_MEM_MIB=$(( $(docker info --format '{{.MemTotal}}') / 1048576 ))

# Sum every running container's resident memory to find the real headroom.
# docker stats mixes units in the same column -- a single container reporting
# "928KiB" read as MiB fabricates a gigabyte of usage and flips the verdict to
# a refusal to run, so every unit has to be handled explicitly.
USED_MIB="$(docker stats --no-stream --format '{{.MemUsage}}' 2>/dev/null |
  awk -F'/' '{
    v=$1
    if (v ~ /GiB/)      { f = 1024 }
    else if (v ~ /MiB/) { f = 1 }
    else if (v ~ /KiB/) { f = 1/1024 }
    else                { f = 1/1048576 }
    gsub(/[^0-9.]/, "", v)
    if (v == "") next
    s += v * f
  } END { printf "%.0f", s }')"
: "${USED_MIB:=0}"
FREE_MIB=$(( VM_MEM_MIB - USED_MIB ))

TOTAL_CONTAINERS="$(docker ps -q | wc -l | tr -d ' ')"
OURS="$(docker ps --filter "label=com.docker.compose.project=$COMPOSE_PROJECT" -q | wc -l | tr -d ' ')"
FOREIGN=$(( TOTAL_CONTAINERS - OURS ))

# --- budget -------------------------------------------------------------
# The generator and the auditor run natively on the host, so they are never the
# constraint. The offered rate is therefore budgeted against the VM's CPUs,
# which is what actually serves the requests. 200 rps per vCPU is the shape
# observed on this stack; it is a starting point to sweep from, not a promise.
SAFE_RATE=$(( VM_CPUS * 200 ))
[ "$FOREIGN" -gt 0 ] && SAFE_RATE=$(( SAFE_RATE / 2 ))

VERDICT=go
REASONS=()
if [ "$FREE_MIB" -lt 512 ]; then
  VERDICT=stop
  REASONS+=("only ${FREE_MIB}MiB of Docker memory free; the stack will be OOM-killed mid-run")
elif [ "$FREE_MIB" -lt 1024 ]; then
  VERDICT=caution
  REASONS+=("${FREE_MIB}MiB free is enough to start but not to absorb a chaos scenario")
fi
if [ "$VM_CPUS" -lt 4 ]; then
  VERDICT=stop
  REASONS+=("$VM_CPUS vCPU cannot host PostgreSQL, Keycloak, LocalStack and three replicas")
fi
if [ "$FOREIGN" -gt 0 ]; then
  [ "$VERDICT" = go ] && VERDICT=caution
  REASONS+=("$FOREIGN container(s) from other projects share this VM; results will not be reproducible")
fi
if [ "$VM_CPUS" -lt $(( HOST_CORES / 2 )) ]; then
  REASONS+=("Docker holds $VM_CPUS of $HOST_CORES host cores; raising it is the single highest-leverage change")
fi

if $JSON; then
  printf '{"host":{"cpu":"%s","cores":%s,"performance_cores":%s,"efficiency_cores":%s,"memory_gb":%s,"load1":"%s"},' \
    "$HOST_CPU_BRAND" "$HOST_CORES" "$HOST_PERF" "$HOST_EFF" "$HOST_MEM_GB" "$HOST_LOAD"
  printf '"docker":{"cpus":%s,"memory_mib":%s,"used_mib":%s,"free_mib":%s,"containers":%s,"foreign":%s},' \
    "$VM_CPUS" "$VM_MEM_MIB" "$USED_MIB" "$FREE_MIB" "$TOTAL_CONTAINERS" "$FOREIGN"
  printf '"budget":{"suggested_rate":%s},"verdict":"%s"}\n' "$SAFE_RATE" "$VERDICT"
else
  echo "host    : $HOST_CPU_BRAND, ${HOST_CORES} cores (${HOST_PERF}P+${HOST_EFF}E), ${HOST_MEM_GB}GB, load1=${HOST_LOAD}"
  echo "docker  : ${VM_CPUS} vCPU, ${VM_MEM_MIB}MiB total, ${USED_MIB}MiB used, ${FREE_MIB}MiB free"
  echo "tenancy : ${TOTAL_CONTAINERS} containers running, ${OURS} ours, ${FOREIGN} foreign"
  echo "budget  : start the sweep at ${SAFE_RATE} rps; the generator runs on the host and will not cap you"
  echo "verdict : $VERDICT"
  [ "${#REASONS[@]}" -gt 0 ] && printf '          - %s\n' "${REASONS[@]}"
fi

case "$VERDICT" in
  stop) exit 2 ;;
  caution) exit 1 ;;
  *) exit 0 ;;
esac
