#!/usr/bin/env bash
# bench/scripts/run-failure-scenarios.sh
#
# The 5 failure/resilience experiments from the project plan. Each targets
# manifold specifically (round_robin, config.example.yaml defaults) --
# nginx OSS has no circuit breaker, no passive health ejection, and no
# active health checking, so there is nothing comparable to exercise on it
# for scenarios 1, 2, 4, and 5. Scenario 3 (hot reload) is a manifold-only
# feature by definition. This script does not touch nginx at all.
#
# Each scenario emits one JSON file to the results directory:
#   01-kill-backend.json
#   02-restart-backend.json
#   03-hot-reload-x10.json
#   04-latency-spike-breaker.json
#   05-all-backends-down.json
# plus summary.json, which lists every assertion and its verdict.
#
# MEASUREMENT CONTRACT
#   - Every scenario emits a real measured number, or an explicit null with
#     a *_reason field saying why it could not be measured. A silent null is
#     a bug in this script, not a result.
#   - Every scenario asserts its target and records assertion_passed.
#   - Exit codes are distinguishable:
#       0  every assertion passed
#       2  the harness ran fine, but at least one assertion failed
#       1  the harness itself broke (die(), missing tool, service never came up)
#
# HOW THE MEASUREMENTS WORK (and why they are not client-side heuristics)
#
# The original version of this script inferred everything from the client
# side -- "traffic looks clean again, so the backend must have been
# ejected". That inference is unsound against manifold, because manifold
# retries (retry.max_attempts: 2, idempotent_only) across three upstreams.
# One dead or slow backend out of three is therefore *invisible* to a
# client: the retry lands on a healthy peer and the caller still sees 200.
# A client-side detector watching for errors to appear and then stop has
# nothing to detect, which is exactly how the old script produced nulls
# while every underlying feature worked.
#
# So the state transitions are read from manifold's own admin listener
# (:9090/metrics) by numeric value, not by grepping for words:
#   manifold_upstream_available{pool,upstream}   1 available / 0 not
#       -> ejection (1->0) and readmission (0->1). This is exactly
#          Backend.Available() == ActiveHealthy() && !Ejected(), i.e. the
#          same candidate-set membership the Go gate test asserts on.
#   manifold_breaker_state{pool,upstream}        0 closed / 1 open / 2 half-open
#       -> breaker trip. There is no metric whose NAME contains "open";
#          the old grep for /breaker.*open/ matched the HELP line of
#          manifold_breaker_state on every scrape and was a constant true.
#   manifold_breaker_transitions_total{pool,upstream,to}
#   manifold_config_reloads_total{result}        success | failure
#   manifold_requests_shed_total{pool}           max_in_flight backpressure only
#   manifold_upstream_requests_total{pool,upstream,status_class}
#
# The curl probe stream and the k6 background load are still recorded, but
# as the *client-visible blast radius* of each event (which is the number a
# reader actually cares about), never as the event detector.
#
# Usage:
#   ./run-failure-scenarios.sh
#   SCENARIOS="1 4" ./run-failure-scenarios.sh   # run a subset
#   STRICT_AC=1 ./run-failure-scenarios.sh       # restore the hard on-battery refusal

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_tools k6 taskset python3 curl date awk nproc
require_built_binaries
compute_core_groups

# ---------------------------------------------------------------------------
# AC power: a warning here, not a refusal.
#
# run-matrix.sh refuses to run on battery, and should: it produces
# throughput/latency numbers that are only meaningful when compared against
# each other, and battery-mode power capping makes two runs incomparable.
#
# This suite measures something else -- whether a state machine fires, and
# how long it takes to fire relative to configured thresholds that are
# whole seconds wide. A 20% frequency cap does not change whether a
# breaker opens. Refusing here just made the script exit 1 before it did
# any work, which is the least useful failure mode available. Warn, record
# the power state in summary.json so no one silently compares a battery run
# against an AC one, and continue. STRICT_AC=1 restores the refusal.
# ---------------------------------------------------------------------------
AC_POWER_STATE="ac_or_undetectable"
if ! ( check_ac_power ) >/dev/null 2>&1; then
  AC_POWER_STATE="battery"
  if [[ "${STRICT_AC:-0}" == "1" ]]; then
    check_ac_power   # re-run for its full diagnostic message, then die
  fi
  warn "running on BATTERY power. These scenarios measure state-machine timing against second-scale configured thresholds, so this is survivable -- but the numbers are not comparable against an AC run. Recorded as power_state=battery in summary.json. Set STRICT_AC=1 to refuse instead."
fi

TIMESTAMP="$(date -u '+%Y%m%dT%H%M%SZ')"
RESULTS_DIR="${RESULTS_ROOT}/${TIMESTAMP}-failure-scenarios"
mkdir -p "$RESULTS_DIR"
log "results directory: ${RESULTS_DIR}"

SCENARIOS="${SCENARIOS:-1 2 3 4 5}"
BACKGROUND_LOAD_VUS="${BACKGROUND_LOAD_VUS:-100}"
MANIFOLD_RELOAD_SIGNAL="${MANIFOLD_RELOAD_SIGNAL:-HUP}"
FAIL_FAST_THRESHOLD_S="${FAIL_FAST_THRESHOLD_S:-5}"

# Metric poll cadence for event detection. Each poll is one /metrics scrape
# (~13KB) plus a curl process, so the effective resolution is this plus a
# few ms of scrape cost -- reported alongside every metric-derived timing so
# nobody reads more precision into the number than it has.
METRIC_POLL_S="${METRIC_POLL_S:-0.05}"
METRIC_POLL_MS=50

# Values read out of config.example.yaml at startup, so the assertion bounds
# below track the config instead of being hardcoded a second time.
CFG_ACTIVE_INTERVAL_S=""
CFG_HEALTHY_THRESHOLD=""
CFG_UNHEALTHY_THRESHOLD=""
CFG_ACTIVE_TIMEOUT_S=""
CFG_FAILURE_THRESHOLD=""
CFG_OPEN_FOR_S=""
CFG_MAX_ATTEMPTS=""

# Scenario 4 renders its own config: see scenario_4 for why.
S4_RESPONSE_HEADER_TIMEOUT="${S4_RESPONSE_HEADER_TIMEOUT:-500ms}"
S4_INJECTED_LATENCY="${S4_INJECTED_LATENCY:-2000ms}"

BG_PID=""
PROBE_PID=""

scenario_cleanup() {
  # k6 and the probe loop outlive a scenario if it dies partway through.
  # Left alive they keep hammering :8080 across the *next* scenario, which
  # is how the old script produced phantom "probe failures during the
  # reload window": two 100-VU k6 runs plus a probe, on two pinned cores,
  # timing the probe's own curl out at -m 3.
  [[ -n "$BG_PID" ]] && kill "$BG_PID" 2>/dev/null || true
  [[ -n "$PROBE_PID" ]] && kill "$PROBE_PID" 2>/dev/null || true
  pkill -f 'k6 run' 2>/dev/null || true
  BG_PID=""
  PROBE_PID=""
}

cleanup_everything() {
  scenario_cleanup
  cleanup_all
}
trap cleanup_everything EXIT INT TERM

# ---------------------------------------------------------------------------
# Assertion bookkeeping
# ---------------------------------------------------------------------------
ASSERTION_FAILURES=0
declare -a ASSERTION_LOG=()

# Usage: assert_true <scenario_id> <name> <true|false> <detail>
# Records the verdict, logs it, and returns the verdict so callers can
# fold several checks into one assertion_passed field.
assert_true() {
  local sid="$1" name="$2" verdict="$3" detail="$4"
  if [[ "$verdict" == "true" ]]; then
    log "ASSERT PASS [${sid}] ${name}: ${detail}"
  else
    warn "ASSERT FAIL [${sid}] ${name}: ${detail}"
    ASSERTION_FAILURES=$(( ASSERTION_FAILURES + 1 ))
  fi
  ASSERTION_LOG+=("${sid}|${name}|${verdict}|${detail}")
}

# Usage: num_cmp <a> <op> <b>  -> prints true|false
# Numeric comparison that treats the literal "null" as always false, so an
# unmeasurable value can never accidentally satisfy a bound.
num_cmp() {
  local a="$1" op="$2" b="$3"
  [[ "$a" == "null" || -z "$a" ]] && { echo "false"; return; }
  if awk -v a="$a" -v b="$b" "BEGIN{exit !(a ${op} b)}"; then echo "true"; else echo "false"; fi
}

write_json() {
  local path="$1"
  shift
  python3 - "$path" "$@" <<'PYEOF'
import json, sys
path = sys.argv[1]
pairs = sys.argv[2:]
data = {}
for kv in pairs:
    k, _, v = kv.partition("=")
    if v == "null":
        data[k] = None
    elif v in ("true", "false"):
        data[k] = (v == "true")
    else:
        try:
            data[k] = int(v)
        except ValueError:
            try:
                data[k] = float(v)
            except ValueError:
                data[k] = v
with open(path, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2)
    f.write("\n")
PYEOF
  log "wrote ${path}"
}

# ---------------------------------------------------------------------------
# Config introspection
#
# Every assertion bound below is derived from the config manifold is actually
# running, so changing config.example.yaml moves the bounds with it instead
# of silently invalidating a hardcoded number in a comment.
# ---------------------------------------------------------------------------
read_config_values() {
  local vals
  vals="$(python3 - "${REPO_ROOT}/config.example.yaml" <<'PYEOF'
import re, sys

text = open(sys.argv[1], encoding="utf-8").read()

def dur_s(raw):
    m = re.fullmatch(r'(\d+(?:\.\d+)?)(ms|s|m)', raw.strip())
    if not m:
        return None
    v, unit = float(m.group(1)), m.group(2)
    return v / 1000.0 if unit == "ms" else (v * 60 if unit == "m" else v)

def scalar(block, key):
    m = re.search(rf'{block}:\s*\n(?:[ \t]+.*\n)*?[ \t]+{key}:\s*(\S+)', text)
    return m.group(1) if m else None

out = {
    "active_interval_s": dur_s(scalar("active", "interval") or "2s"),
    "active_timeout_s": dur_s(scalar("active", "timeout") or "500ms"),
    "healthy_threshold": scalar("active", "healthy_threshold") or "2",
    "unhealthy_threshold": scalar("active", "unhealthy_threshold") or "3",
    "failure_threshold": scalar("breaker", "failure_threshold") or "5",
    "open_for_s": dur_s(scalar("breaker", "open_for") or "5s"),
    "max_attempts": scalar("retry", "max_attempts") or "2",
}
print(" ".join(f"{k}={v}" for k, v in out.items()))
PYEOF
)"
  local kv
  for kv in $vals; do
    case "${kv%%=*}" in
      active_interval_s)    CFG_ACTIVE_INTERVAL_S="${kv#*=}" ;;
      active_timeout_s)     CFG_ACTIVE_TIMEOUT_S="${kv#*=}" ;;
      healthy_threshold)    CFG_HEALTHY_THRESHOLD="${kv#*=}" ;;
      unhealthy_threshold)  CFG_UNHEALTHY_THRESHOLD="${kv#*=}" ;;
      failure_threshold)    CFG_FAILURE_THRESHOLD="${kv#*=}" ;;
      open_for_s)           CFG_OPEN_FOR_S="${kv#*=}" ;;
      max_attempts)         CFG_MAX_ATTEMPTS="${kv#*=}" ;;
    esac
  done
  log "config: active interval=${CFG_ACTIVE_INTERVAL_S}s timeout=${CFG_ACTIVE_TIMEOUT_S}s healthy=${CFG_HEALTHY_THRESHOLD} unhealthy=${CFG_UNHEALTHY_THRESHOLD}; breaker failures=${CFG_FAILURE_THRESHOLD} open_for=${CFG_OPEN_FOR_S}s; retry max_attempts=${CFG_MAX_ATTEMPTS}"
}
read_config_values

# ---------------------------------------------------------------------------
# Background load
#
# The old version did bg_pid="$(start_background_load ...)". Command
# substitution runs the function in a SUBSHELL, so the k6 process it
# backgrounds is a grandchild: $! is meaningful only inside that subshell,
# and the parent's later `wait "$bg_pid"` fails instantly with "not a child
# of this shell" (swallowed by || true). Every scenario therefore stopped
# observing at the instant it injected the fault, ~0s of post-event data --
# which is why time_to_clean_traffic_s, requests_failed and requests_total
# were all null, and why k6's JSON did not exist yet when it was read.
# Setting a global from a normal function call keeps k6 a direct child, so
# wait_background_load actually waits.
# ---------------------------------------------------------------------------
start_background_load() {
  local out_json="$1" duration="$2"
  BG_JSON="$out_json"
  TARGET="manifold" STRATEGY="round_robin" CONCURRENCY="$BACKGROUND_LOAD_VUS" BACKEND_LATENCY="scenario" RUN_INDEX="1" \
    run_k6 "http://127.0.0.1:8080" "constant-vus" "$BACKGROUND_LOAD_VUS" 0 "$duration" "/" "$out_json" \
    >"${out_json%.json}.k6.log" 2>&1 &
  BG_PID=$!
  log "background load started (pid ${BG_PID}, ${BACKGROUND_LOAD_VUS} VUs, ${duration})"
}

wait_background_load() {
  [[ -z "$BG_PID" ]] && return 0
  wait "$BG_PID" 2>/dev/null || true
  BG_PID=""
}

# Usage: k6_field <json> <field>  -> value, or "null"
k6_field() {
  python3 - "$1" "$2" <<'PYEOF' 2>/dev/null || echo "null"
import json, sys
try:
    d = json.load(open(sys.argv[1], encoding="utf-8"))
except Exception:
    print("null"); raise SystemExit(0)
v = d.get(sys.argv[2])
print("null" if v is None else v)
PYEOF
}

# ---------------------------------------------------------------------------
# Metric reads
# ---------------------------------------------------------------------------

# Usage: metric_of <file> <awk_regex>  -> the sample value, or empty
metric_of() {
  awk -v re="$2" '$0 !~ /^#/ && $0 ~ re { print $NF; exit }' "$1" 2>/dev/null
}

# Usage: metric_now <awk_regex>  -> live sample value from :9090/metrics, or empty
metric_now() {
  scrape_manifold_metrics | awk -v re="$1" '$0 !~ /^#/ && $0 ~ re { print $NF; exit }'
}

# Usage: wait_for_metric <awk_regex> <op> <want> <timeout_s> <t0_epoch_ms>
# Polls :9090/metrics until the first sample matching <awk_regex> satisfies
# `value <op> want`, then prints the elapsed seconds since t0. Prints "null"
# and returns 1 on timeout. This is the event detector for ejection,
# readmission and breaker-open.
wait_for_metric() {
  local re="$1" op="$2" want="$3" timeout_s="$4" t0="$5"
  local deadline v
  deadline=$(awk -v a="$(now_ms)" -v t="$timeout_s" 'BEGIN{printf "%d", a + t*1000}')
  while (( $(now_ms) < deadline )); do
    v="$(metric_now "$re")"
    if [[ -n "$v" ]] && awk -v v="$v" -v w="$want" "BEGIN{exit !(v ${op} w)}"; then
      awk -v a="$t0" -v b="$(now_ms)" 'BEGIN{printf "%.3f\n", (b-a)/1000.0}'
      return 0
    fi
    sleep "$METRIC_POLL_S"
  done
  echo "null"
  return 1
}

# Usage: probe_stats <probe_file> <t0_ms> <t1_ms|end>  -> "<failures> <total>"
# A probe line is "epoch_ms http_status time_total_s"; http_status is 000 on
# connect failure or client timeout.
probe_stats() {
  local f="$1" t0="$2" t1="$3"
  [[ -s "$f" ]] || { echo "null null"; return; }
  awk -v t0="$t0" -v t1="$t1" '
    $1 >= t0 && (t1 == "end" || $1 <= t1) {
      total++
      if ($2 !~ /^2/) bad++
    }
    END { printf "%d %d\n", bad+0, total+0 }
  ' "$f"
}

setup_manifold_stack() {
  local logdir="$1" latency="${2:-1ms}"
  start_backends "$latency" "$logdir"
  start_manifold "round_robin" "$logdir"
}

teardown_manifold_stack() {
  stop_manifold
  stop_backends
}

reload_manifold() {
  [[ -z "$MANIFOLD_PID" ]] && die "reload_manifold: manifold is not running"
  kill -s "$MANIFOLD_RELOAD_SIGNAL" "$MANIFOLD_PID"
}

# ---------------------------------------------------------------------------
# 1. Kill one backend at steady state -> time to ejection, requests failed
# ---------------------------------------------------------------------------
scenario_1() {
  local logdir="${RESULTS_DIR}/01-kill-backend"
  mkdir -p "$logdir"
  log "=== scenario 1: kill one backend at steady state ==="
  setup_manifold_stack "$logdir"

  local port="${BACKEND_PORTS[0]}"
  local upstream_re="^manifold_upstream_available[{].*${port}"

  local probe_file="${logdir}/probe.tsv"
  PROBE_PID="$(start_probe_loop "http://127.0.0.1:8080/" "$probe_file" 0.1)"
  # 45s: the kill lands at t=8s and ejection takes unhealthy_threshold x
  # interval (~6s with config.example.yaml), so this leaves ~30s of
  # post-ejection observation. The old 30s duration was never observed at
  # all because the wait was a no-op; even if it had been, 30s minus an 8s
  # warm-up minus a 6s detection window is a thin post-event sample.
  start_background_load "${logdir}/background-load.json" "45s"

  sleep 8   # let steady state establish before injecting the failure

  local kill_epoch_ms
  kill_epoch_ms="$(now_ms)"
  log "killing backend on port ${port} (pid ${BACKEND_PIDS[0]})"
  kill -KILL "${BACKEND_PIDS[0]}" 2>/dev/null || true

  # Ejection = the backend leaving the candidate set, read straight off
  # manifold_upstream_available. This is the same predicate the Go gate test
  # asserts on (Backend.Available()), not an inference from client traffic.
  local eject_timeout_s
  eject_timeout_s="$(awk -v i="$CFG_ACTIVE_INTERVAL_S" -v u="$CFG_UNHEALTHY_THRESHOLD" 'BEGIN{printf "%.1f", i*u*3 + 10}')"
  local time_to_ejection_s ejection_epoch_ms
  time_to_ejection_s="$(wait_for_metric "$upstream_re" "==" 0 "$eject_timeout_s" "$kill_epoch_ms")" || true
  ejection_epoch_ms="$(now_ms)"

  local eject_reason="null"
  if [[ "$time_to_ejection_s" == "null" ]]; then
    eject_reason="manifold_upstream_available for ${port} never reached 0 within ${eject_timeout_s}s of SIGKILL"
  fi

  wait_background_load
  stop_probe_loop "$PROBE_PID"; PROBE_PID=""

  # Client-visible blast radius, split at the ejection instant. Note that
  # with retry.max_attempts=${CFG_MAX_ATTEMPTS} over three upstreams, a
  # single dead backend is expected to be invisible to callers even DURING
  # detection: the retry lands on a healthy peer. Zero here is the target,
  # not a measurement failure.
  local detect_fail detect_total after_fail after_total
  read -r detect_fail detect_total <<<"$(probe_stats "$probe_file" "$kill_epoch_ms" "$ejection_epoch_ms")"
  read -r after_fail after_total <<<"$(probe_stats "$probe_file" "$ejection_epoch_ms" "end")"

  local time_to_clean_s
  time_to_clean_s="$(time_to_sustained_ok "$probe_file" '^2' "$kill_epoch_ms" 20)"
  local clean_reason="null"
  if [[ "$time_to_clean_s" == "null" ]]; then
    clean_reason="fewer than 20 consecutive 2xx probes were recorded after the kill in ${probe_file}"
  fi

  local requests_failed requests_total error_rate bg_reason="null"
  requests_failed="$(k6_field "${logdir}/background-load.json" requests_failed)"
  requests_total="$(k6_field "${logdir}/background-load.json" requests_total)"
  error_rate="$(k6_field "${logdir}/background-load.json" error_rate)"
  [[ "$requests_total" == "null" ]] && bg_reason="k6 summary ${logdir}/background-load.json was not written or not parseable (see background-load.k6.log)"

  # --- assertions ---
  local bound_s
  bound_s="$(awk -v i="$CFG_ACTIVE_INTERVAL_S" -v u="$CFG_UNHEALTHY_THRESHOLD" -v t="$CFG_ACTIVE_TIMEOUT_S" 'BEGIN{printf "%.1f", i*u + t + 2}')"
  local a1 a2 passed
  a1="$(num_cmp "$time_to_ejection_s" "<=" "$bound_s")"
  assert_true 1 "ejected_within_bound" "$a1" \
    "time_to_ejection_s=${time_to_ejection_s} bound=${bound_s}s (unhealthy_threshold ${CFG_UNHEALTHY_THRESHOLD} x interval ${CFG_ACTIVE_INTERVAL_S}s + probe timeout ${CFG_ACTIVE_TIMEOUT_S}s + 2s slack)"
  a2="$(num_cmp "$after_fail" "==" 0)"
  assert_true 1 "no_client_errors_after_ejection" "$a2" \
    "probe failures after ejection = ${after_fail}/${after_total}"
  passed="false"; [[ "$a1" == "true" && "$a2" == "true" ]] && passed="true"

  write_json "${RESULTS_DIR}/01-kill-backend.json" \
    "scenario=kill_backend_at_steady_state" \
    "killed_backend_port=${port}" \
    "time_to_ejection_s=${time_to_ejection_s}" \
    "time_to_ejection_reason=${eject_reason}" \
    "ejection_bound_s=${bound_s}" \
    "metric_poll_resolution_ms=${METRIC_POLL_MS}" \
    "time_to_clean_traffic_s=${time_to_clean_s}" \
    "time_to_clean_traffic_reason=${clean_reason}" \
    "probe_failures_during_detection=${detect_fail}" \
    "probe_requests_during_detection=${detect_total}" \
    "probe_failures_after_ejection=${after_fail}" \
    "probe_requests_after_ejection=${after_total}" \
    "requests_failed=${requests_failed}" \
    "requests_total=${requests_total}" \
    "error_rate=${error_rate}" \
    "background_load_reason=${bg_reason}" \
    "config_active_interval_s=${CFG_ACTIVE_INTERVAL_S}" \
    "config_unhealthy_threshold=${CFG_UNHEALTHY_THRESHOLD}" \
    "config_retry_max_attempts=${CFG_MAX_ATTEMPTS}" \
    "method=metric-based: elapsed from SIGKILL until manifold_upstream_available{upstream=...:${port}} reads 0, polled every ${METRIC_POLL_MS}ms on :9090/metrics. Client counts are the blast radius, not the detector -- with max_attempts=${CFG_MAX_ATTEMPTS} over 3 upstreams a single dead backend is expected to be invisible to callers." \
    "assertion=ejected within ${bound_s}s AND zero client-visible failures after ejection" \
    "assertion_passed=${passed}" \
    "note=killed_backend_pid_is_now_dead_do_not_reuse_for_scenario_2"

  teardown_manifold_stack
}

# ---------------------------------------------------------------------------
# 2. Restart it -> time to readmission
# ---------------------------------------------------------------------------
scenario_2() {
  local logdir="${RESULTS_DIR}/02-restart-backend"
  mkdir -p "$logdir"
  log "=== scenario 2: restart a downed backend -> time to readmission ==="
  setup_manifold_stack "$logdir"

  local port="${BACKEND_PORTS[0]}"
  local admin_port; admin_port="$(backend_admin_port "$port")"
  local upstream_re="^manifold_upstream_available[{].*${port}"

  log "killing backend on port ${port}"
  local kill_epoch_ms; kill_epoch_ms="$(now_ms)"
  kill -KILL "${BACKEND_PIDS[0]}" 2>/dev/null || true

  # Wait for the ejection to actually be observed rather than sleeping a
  # guessed 5s. Readmission timing is only meaningful measured from a
  # confirmed-ejected starting state; a fixed sleep that is a hair too short
  # measures "time from restart to an ejection that had not happened yet".
  #
  # The kill deliberately precedes the load here. With no traffic flowing,
  # passive health records nothing, so nothing is ejected with a 30s
  # eject_for hold and readmission is governed purely by the active checker
  # (healthy_threshold x interval).
  local eject_timeout_s
  eject_timeout_s="$(awk -v i="$CFG_ACTIVE_INTERVAL_S" -v u="$CFG_UNHEALTHY_THRESHOLD" 'BEGIN{printf "%.1f", i*u*3 + 10}')"
  local time_to_ejection_s
  time_to_ejection_s="$(wait_for_metric "$upstream_re" "==" 0 "$eject_timeout_s" "$kill_epoch_ms")" || true
  if [[ "$time_to_ejection_s" == "null" ]]; then
    warn "backend ${port} was never observed leaving the candidate set; readmission cannot be measured from a known state"
  fi

  local probe_file="${logdir}/probe.tsv"
  PROBE_PID="$(start_probe_loop "http://127.0.0.1:8080/" "$probe_file" 0.1)"
  start_background_load "${logdir}/background-load.json" "30s"

  sleep 3
  local restart_epoch_ms; restart_epoch_ms="$(now_ms)"
  log "restarting backend on port ${port}"
  local new_pid
  new_pid="$(pinned_start "$CORES_BACKENDS" "${logdir}/backend-${port}-restarted.log" \
    "$BACKEND_BIN" -addr "127.0.0.1:${port}" -admin "127.0.0.1:${admin_port}" \
    -id "backend-${port}" -latency "1ms" -jitter "0ms" -error-rate "0" -health-path "/healthz")"
  BACKEND_PIDS[0]="$new_pid"

  local self_healthy="true"
  if ! wait_for_http_ok "http://127.0.0.1:${port}/healthz" 15; then
    self_healthy="false"
    warn "restarted backend on port ${port} never became healthy itself -- readmission cannot happen"
  fi

  # Readmission = manifold_upstream_available going back to 1. Directly
  # observed, not the old "run health.active.interval x healthy_threshold
  # worth of clean traffic and call that an upper bound" estimate -- which
  # could not distinguish readmission from the two surviving backends
  # serving everything perfectly well without it.
  local readmit_timeout_s
  readmit_timeout_s="$(awk -v i="$CFG_ACTIVE_INTERVAL_S" -v h="$CFG_HEALTHY_THRESHOLD" 'BEGIN{printf "%.1f", i*h*3 + 15}')"
  local time_to_readmission_s readmit_epoch_ms
  time_to_readmission_s="$(wait_for_metric "$upstream_re" "==" 1 "$readmit_timeout_s" "$restart_epoch_ms")" || true
  readmit_epoch_ms="$(now_ms)"

  local readmit_reason="null"
  if [[ "$time_to_readmission_s" == "null" ]]; then
    if [[ "$self_healthy" == "false" ]]; then
      readmit_reason="the restarted backend never answered its own /healthz, so manifold had nothing to readmit"
    else
      readmit_reason="manifold_upstream_available for ${port} never returned to 1 within ${readmit_timeout_s}s of the process restart"
    fi
  fi

  wait_background_load
  stop_probe_loop "$PROBE_PID"; PROBE_PID=""

  local after_fail after_total
  read -r after_fail after_total <<<"$(probe_stats "$probe_file" "$readmit_epoch_ms" "end")"

  local requests_failed requests_total bg_reason="null"
  requests_failed="$(k6_field "${logdir}/background-load.json" requests_failed)"
  requests_total="$(k6_field "${logdir}/background-load.json" requests_total)"
  [[ "$requests_total" == "null" ]] && bg_reason="k6 summary ${logdir}/background-load.json was not written or not parseable (see background-load.k6.log)"

  # Traffic must actually return to the readmitted backend. Rejoining the
  # candidate set is not the same as receiving requests: a strategy caching
  # a ring keyed on a generation that never moved would satisfy the timing
  # assertion and still never route there again.
  local served_after="null" served_reason="null"
  if served_after="$(curl -fsS -m 5 "http://127.0.0.1:${admin_port}/_control/stats" 2>/dev/null | python3 -c 'import json,sys;print(json.load(sys.stdin)["served"])' 2>/dev/null)"; then
    :
  else
    served_after="null"
    served_reason="the restarted backend's /_control/stats on :${admin_port} was unreachable"
  fi

  local bound_s
  bound_s="$(awk -v i="$CFG_ACTIVE_INTERVAL_S" -v h="$CFG_HEALTHY_THRESHOLD" -v t="$CFG_ACTIVE_TIMEOUT_S" 'BEGIN{printf "%.1f", i*h + t + 2}')"
  local a1 a2 passed
  a1="$(num_cmp "$time_to_readmission_s" "<=" "$bound_s")"
  assert_true 2 "readmitted_within_bound" "$a1" \
    "approx_time_to_readmission_s=${time_to_readmission_s} bound=${bound_s}s (healthy_threshold ${CFG_HEALTHY_THRESHOLD} x interval ${CFG_ACTIVE_INTERVAL_S}s + probe timeout ${CFG_ACTIVE_TIMEOUT_S}s + 2s slack)"
  a2="$(num_cmp "$served_after" ">" 0)"
  assert_true 2 "traffic_returned_to_backend" "$a2" \
    "restarted backend served ${served_after} data-plane requests after readmission"
  passed="false"; [[ "$a1" == "true" && "$a2" == "true" ]] && passed="true"

  write_json "${RESULTS_DIR}/02-restart-backend.json" \
    "scenario=restart_backend_readmission" \
    "restarted_backend_port=${port}" \
    "time_to_ejection_s=${time_to_ejection_s}" \
    "approx_time_to_readmission_s=${time_to_readmission_s}" \
    "approx_time_to_readmission_reason=${readmit_reason}" \
    "readmission_bound_s=${bound_s}" \
    "metric_poll_resolution_ms=${METRIC_POLL_MS}" \
    "requests_served_by_readmitted_backend=${served_after}" \
    "requests_served_reason=${served_reason}" \
    "probe_failures_after_readmission=${after_fail}" \
    "probe_requests_after_readmission=${after_total}" \
    "background_requests_failed=${requests_failed}" \
    "background_requests_total=${requests_total}" \
    "background_load_reason=${bg_reason}" \
    "config_healthy_threshold=${CFG_HEALTHY_THRESHOLD}" \
    "config_active_interval_s=${CFG_ACTIVE_INTERVAL_S}" \
    "method=metric-based: elapsed from the backend process restart until manifold_upstream_available{upstream=...:${port}} reads 1 again, polled every ${METRIC_POLL_MS}ms. Corroborated by the backend's own /_control/stats served counter, which proves the pool actually routed to it again rather than merely listing it." \
    "assertion=readmitted within ${bound_s}s AND received real traffic afterwards" \
    "assertion_passed=${passed}"

  teardown_manifold_stack
}

# ---------------------------------------------------------------------------
# 3. Hot config reload x10 under sustained load -> connections dropped (target 0)
# ---------------------------------------------------------------------------
scenario_3() {
  local logdir="${RESULTS_DIR}/03-hot-reload"
  mkdir -p "$logdir"
  log "=== scenario 3: hot config reload x10 under sustained load ==="
  setup_manifold_stack "$logdir"

  # Read the reload counter and the admin endpoint's health BEFORE any
  # reload. See the block below the reload loop for why "before" is the only
  # time this scrape is usable.
  local reloads_before metrics_status_before
  reloads_before="$(metric_now '^manifold_config_reloads_total[{]result="success"')"
  [[ -z "$reloads_before" ]] && reloads_before=0
  metrics_status_before="$(curl -s -o /dev/null -m 3 -w '%{http_code}' "http://127.0.0.1:9090/metrics" 2>/dev/null || echo "000")"

  local probe_file="${logdir}/probe.tsv"
  PROBE_PID="$(start_probe_loop "http://127.0.0.1:8080/" "$probe_file" 0.05)"
  start_background_load "${logdir}/background-load.json" "30s"

  sleep 3
  local reload_epoch_ms_start; reload_epoch_ms_start="$(now_ms)"
  local i delivered=0
  for i in $(seq 1 10); do
    log "reload ${i}/10"
    if ! reload_manifold; then
      warn "SIGHUP delivery failed on reload ${i} -- process may have exited; check ${logdir}/manifold.log"
      break
    fi
    delivered=$(( delivered + 1 ))
    sleep 1.5
  done
  local reload_epoch_ms_end; reload_epoch_ms_end="$(now_ms)"

  # ------------------------------------------------------------------
  # MANIFOLD DEFECT, not a measurement artefact: after the FIRST successful
  # reload, GET :9090/metrics returns HTTP 500 permanently, with a body of
  #
  #   9 error(s) occurred:
  #   * collected metric "manifold_upstream_inflight" {pool="api",
  #     upstream="http://127.0.0.1:9001"} was collected before with the same
  #     name and label values
  #   ... and the same for manifold_upstream_available and
  #       manifold_breaker_state, one error per upstream.
  #
  # The reload registers the new generation's scrape-time collector without
  # unregistering the retired one, so the gatherer sees every live gauge
  # twice and refuses to serve the entire exposition. The data plane and
  # :9090/healthz are unaffected -- this is a pure observability outage, and
  # it is permanent until the process restarts.
  #
  # Consequence here: manifold_config_reloads_total cannot be read after a
  # reload, because nothing can. Reload application is therefore confirmed
  # from manifold structured log output, which is authoritative and
  # unaffected: one {"msg":"config reloaded"} record with a monotonically
  # increasing "generation" per applied reload.
  # ------------------------------------------------------------------
  local metrics_status_after metrics_broken="false"
  metrics_status_after="$(curl -s -o /dev/null -m 3 -w '%{http_code}' "http://127.0.0.1:9090/metrics" 2>/dev/null || echo "000")"
  if [[ "$metrics_status_before" == "200" && "$metrics_status_after" != "200" ]]; then
    metrics_broken="true"
    warn "MANIFOLD DEFECT: :9090/metrics was ${metrics_status_before} before the reloads and is ${metrics_status_after} after them. Duplicate collector registration on reload takes the whole Prometheus exposition down permanently. Data plane unaffected. Recorded in 03-hot-reload-x10.json; not asserted here because scenario 3 targets dropped connections."
  fi

  local mlog="${logdir}/manifold.log"
  local reloads_ok reloads_failed generation_final
  reloads_ok="$(grep -c 'config reloaded' "$mlog" 2>/dev/null || true)"
  reloads_failed="$(grep -c 'config reload failed' "$mlog" 2>/dev/null || true)"
  [[ -z "$reloads_ok" ]] && reloads_ok=0
  [[ -z "$reloads_failed" ]] && reloads_failed=0
  generation_final="$(grep -o '"generation":[0-9]*' "$mlog" 2>/dev/null | tail -1 | cut -d: -f2)"
  [[ -z "$generation_final" ]] && generation_final="null"

  wait_background_load
  stop_probe_loop "$PROBE_PID"; PROBE_PID=""

  local dropped probed
  read -r dropped probed <<<"$(probe_stats "$probe_file" "$reload_epoch_ms_start" "$reload_epoch_ms_end")"

  local requests_failed requests_total rps bg_reason="null"
  requests_failed="$(k6_field "${logdir}/background-load.json" requests_failed)"
  requests_total="$(k6_field "${logdir}/background-load.json" requests_total)"
  rps="$(k6_field "${logdir}/background-load.json" rps)"
  [[ "$requests_total" == "null" ]] && bg_reason="k6 summary ${logdir}/background-load.json was not written or not parseable (see background-load.k6.log)"

  # The k6 numbers, not the 20-rps probe, are the real evidence here: 30s of
  # ~10k rps spanning the reload window is four orders of magnitude more
  # samples than the probe, and a drain-and-swap that drops connections
  # drops them from the connection pool k6 is holding open.
  local gen_advance="null"
  [[ "$generation_final" != "null" ]] && gen_advance=$(( generation_final - 1 ))

  local a1 a2 a3 a4 passed
  a1="$(num_cmp "$reloads_ok" "==" "$delivered")"
  assert_true 3 "all_reloads_applied" "$a1" \
    "manifold logged ${reloads_ok} config-reloaded records for ${delivered} signals delivered (reload failures=${reloads_failed})"
  a4="$(num_cmp "$gen_advance" "==" "$delivered")"
  assert_true 3 "generation_advanced_once_per_reload" "$a4" \
    "config generation reached ${generation_final}, i.e. advanced ${gen_advance} times from the initial generation 1"
  a2="$(num_cmp "$requests_failed" "==" 0)"
  assert_true 3 "zero_background_requests_dropped" "$a2" \
    "k6 http_req_failed=${requests_failed} of ${requests_total} at ${rps} rps across the reload window"
  a3="$(num_cmp "$dropped" "==" 0)"
  assert_true 3 "zero_probe_failures_in_window" "$a3" \
    "probe failures during the reload window = ${dropped}/${probed}"
  passed="false"; [[ "$a1" == "true" && "$a2" == "true" && "$a3" == "true" && "$a4" == "true" ]] && passed="true"

  write_json "${RESULTS_DIR}/03-hot-reload-x10.json" \
    "scenario=hot_reload_x10_under_load" \
    "reload_count=10" \
    "reload_signals_delivered=${delivered}" \
    "reloads_applied_successfully=${reloads_ok}" \
    "reloads_failed=${reloads_failed}" \
    "config_generation_final=${generation_final}" \
    "config_generation_advance=${gen_advance}" \
    "admin_metrics_http_status_before_reloads=${metrics_status_before}" \
    "admin_metrics_http_status_after_reloads=${metrics_status_after}" \
    "manifold_defect_metrics_endpoint_dies_on_reload=${metrics_broken}" \
    "manifold_defect_detail=After the first successful reload :9090/metrics returns 500 permanently. The new generation scrape-time collector (manifold_upstream_inflight, manifold_upstream_available, manifold_breaker_state) is registered without unregistering the retired one, so the gatherer rejects the whole exposition as duplicate. Data plane and :9090/healthz are unaffected. This is a manifold bug, not a harness artefact, and it is why reload application is confirmed from the log here rather than from manifold_config_reloads_total." \
    "config_reloads_metric_before_reloads=${reloads_before}" \
    "reload_signal=${MANIFOLD_RELOAD_SIGNAL}" \
    "probe_failures_during_reload_window=${dropped}" \
    "probe_requests_during_reload_window=${probed}" \
    "background_requests_failed=${requests_failed}" \
    "background_requests_total=${requests_total}" \
    "background_rps=${rps}" \
    "background_load_reason=${bg_reason}" \
    "target=0 dropped requests across 10 reloads (a clean drain-and-swap must not error in-flight or new client-facing requests)" \
    "method=SIGHUP x10 at 1.5s spacing under ${BACKGROUND_LOAD_VUS} VUs. Reload application is confirmed from manifold structured log output (one config-reloaded record with an incrementing generation per signal), so a signal that was delivered but silently ignored fails the run instead of scoring a free zero. Drops are counted from the k6 background stream (whole run, ~8-10k rps) and from the 20-rps probe restricted to the reload window." \
    "assertion=every signal applied AND the generation advanced once per reload AND zero dropped requests in both the k6 stream and the probe window" \
    "assertion_passed=${passed}"

  teardown_manifold_stack
}

# ---------------------------------------------------------------------------
# 4. Drive one backend's latency 1ms -> 2000ms via its control port ->
#    time for breaker to open, shed rate
# ---------------------------------------------------------------------------
#
# This scenario runs a MODIFIED config, and has to. Two reasons:
#
#  1. config.example.yaml sets transport.response_header_timeout: 10s. A
#     2000ms response is well inside that, so the spike produces slow
#     successes and not a single failure -- and recordBreaker only counts
#     transport errors and 5xx. With the stock config the breaker correctly
#     stays closed forever and the scenario measures nothing. Dropping the
#     timeout to ${S4_RESPONSE_HEADER_TIMEOUT} makes the injected latency a
#     genuine failure, which is the precondition the experiment needs.
#
#  2. passive health is disabled here. With it on, the same timeouts feed
#     both the breaker and the passive tracker, and whichever ejects first
#     starves the other of traffic -- so the measurement would be of a race,
#     not of the breaker. Isolating the breaker is the point of scenario 4;
#     passive ejection is exercised in scenarios 1 and 2.
#
# Both deltas are recorded in the output JSON.
start_manifold_scenario4() {
  local resultsdir="$1"
  local cfg="${resultsdir}/manifold-round_robin.yaml"
  render_manifold_config "round_robin" "$cfg"

  python3 - "$cfg" "$S4_RESPONSE_HEADER_TIMEOUT" <<'PYEOF'
import re, sys
path, rht = sys.argv[1], sys.argv[2]
text = open(path, encoding="utf-8").read()

text, n = re.subn(r'response_header_timeout:\s*\S+', f'response_header_timeout: {rht}', text, count=1)
if n != 1:
    raise SystemExit(f"could not patch response_header_timeout in {path}")

# Only the passive block's `enabled`, never the active block's.
text, n = re.subn(r'(passive:\s*\n\s+)enabled:\s*true', r'\1enabled: false', text, count=1)
if n != 1:
    raise SystemExit(f"could not disable passive health in {path}")

open(path, "w", encoding="utf-8").write(text)
PYEOF

  MANIFOLD_PID="$(pinned_start "$CORES_LB" "${resultsdir}/manifold.log" "$MANIFOLD_BIN" -config "$cfg")"
  if ! wait_for_http_ok "http://127.0.0.1:9090/healthz" 15; then
    lb_failure_report "manifold" "$MANIFOLD_PID" "${resultsdir}/manifold.log"
    die "manifold did not become healthy on :9090/healthz within 15s (see ${resultsdir}/manifold.log)"
  fi
  if ! wait_for_tcp 127.0.0.1 8080 10; then
    lb_failure_report "manifold" "$MANIFOLD_PID" "${resultsdir}/manifold.log"
    die "manifold data listener :8080 did not come up (see ${resultsdir}/manifold.log)"
  fi
  log "manifold up for scenario 4 (response_header_timeout=${S4_RESPONSE_HEADER_TIMEOUT}, passive health disabled, pid=${MANIFOLD_PID})"
}

scenario_4() {
  local logdir="${RESULTS_DIR}/04-latency-spike-breaker"
  mkdir -p "$logdir"
  log "=== scenario 4: latency spike on one backend -> breaker open, shed rate ==="
  start_backends "1ms" "$logdir"
  start_manifold_scenario4 "$logdir"

  local port="${BACKEND_PORTS[0]}"
  local breaker_re="^manifold_breaker_state[{].*${port}"

  local probe_file="${logdir}/probe.tsv"
  PROBE_PID="$(start_probe_loop "http://127.0.0.1:8080/" "$probe_file" 0.1)"
  start_background_load "${logdir}/background-load.json" "30s"

  sleep 5
  local metrics_before="${logdir}/metrics-before.txt" metrics_after="${logdir}/metrics-after.txt"
  local metrics_at_open="${logdir}/metrics-at-open.txt"
  scrape_manifold_metrics > "$metrics_before"

  local spike_epoch_ms; spike_epoch_ms="$(now_ms)"
  log "driving backend ${port} latency to ${S4_INJECTED_LATENCY} via its control port"
  local injected="true"
  if ! backend_control 0 "latency?d=${S4_INJECTED_LATENCY}" >/dev/null; then
    injected="false"
    warn "backend_control latency injection failed -- check bench/backend's /_control/latency contract"
  fi

  # Breaker-open read numerically off manifold_breaker_state (0 closed,
  # 1 open, 2 half-open). The old code grepped /metrics for a name matching
  # breaker.*open, which matched the HELP line of manifold_breaker_state on
  # every single scrape -- a constant, so it carried no information either
  # way. No metric name contains "open"; the state is a value.
  local time_to_open_s open_epoch_ms
  time_to_open_s="$(wait_for_metric "$breaker_re" ">=" 1 20 "$spike_epoch_ms")" || true
  open_epoch_ms="$(now_ms)"
  scrape_manifold_metrics > "$metrics_at_open"

  local open_reason="null"
  if [[ "$time_to_open_s" == "null" ]]; then
    if [[ "$injected" == "false" ]]; then
      open_reason="the latency injection itself failed, so no failures were ever produced for the breaker to count"
    else
      open_reason="manifold_breaker_state for ${port} never left 0 within 20s of the spike"
    fi
  fi

  sleep 10
  scrape_manifold_metrics > "$metrics_after"

  wait_background_load
  stop_probe_loop "$PROBE_PID"; PROBE_PID=""

  local breaker_state_at_open transitions_open
  breaker_state_at_open="$(metric_of "$metrics_at_open" "$breaker_re")"
  [[ -z "$breaker_state_at_open" ]] && breaker_state_at_open="null"
  transitions_open="$(metric_of "$metrics_after" "^manifold_breaker_transitions_total[{].*to=\"open\".*${port}")"
  [[ -z "$transitions_open" ]] && transitions_open="null"

  # Proof the open breaker actually removed the backend from the request
  # path: upstream_requests_total for the spiked backend must stop advancing
  # between the open instant and the end of the run.
  local up_at_open up_at_end up_delta="null"
  up_at_open="$(python3 - "$metrics_at_open" "$port" <<'PYEOF'
import sys
tot = 0
for line in open(sys.argv[1], encoding="utf-8"):
    if line.startswith("manifold_upstream_requests_total{") and sys.argv[2] in line:
        tot += float(line.rsplit(" ", 1)[1])
print(int(tot))
PYEOF
)"
  up_at_end="$(python3 - "$metrics_after" "$port" <<'PYEOF'
import sys
tot = 0
for line in open(sys.argv[1], encoding="utf-8"):
    if line.startswith("manifold_upstream_requests_total{") and sys.argv[2] in line:
        tot += float(line.rsplit(" ", 1)[1])
print(int(tot))
PYEOF
)"
  [[ -n "$up_at_open" && -n "$up_at_end" ]] && up_delta=$(( up_at_end - up_at_open ))

  local errors_on_spiked
  errors_on_spiked="$(metric_of "$metrics_after" "^manifold_upstream_requests_total[{].*status_class=\"error\".*${port}")"
  [[ -z "$errors_on_spiked" ]] && errors_on_spiked="null"

  local shed_before shed_after shed_delta="null"
  shed_before="$(metric_of "$metrics_before" '^manifold_requests_shed_total[{]')"
  shed_after="$(metric_of "$metrics_after" '^manifold_requests_shed_total[{]')"
  [[ -n "$shed_before" && -n "$shed_after" ]] && shed_delta="$(awk -v a="$shed_after" -v b="$shed_before" 'BEGIN{printf "%d", a-b}')"

  local post_fail post_total shed_rate="null"
  read -r post_fail post_total <<<"$(probe_stats "$probe_file" "$spike_epoch_ms" "end")"
  if [[ "$post_total" != "null" ]] && (( post_total > 0 )); then
    shed_rate="$(awk -v a="$post_fail" -v b="$post_total" 'BEGIN{printf "%.4f", a/b}')"
  fi

  local requests_failed requests_total bg_reason="null"
  requests_failed="$(k6_field "${logdir}/background-load.json" requests_failed)"
  requests_total="$(k6_field "${logdir}/background-load.json" requests_total)"
  [[ "$requests_total" == "null" ]] && bg_reason="k6 summary ${logdir}/background-load.json was not written or not parseable (see background-load.k6.log)"

  local a1 a2 a3 passed
  a1="$(num_cmp "$time_to_open_s" "<=" 10)"
  assert_true 4 "breaker_opened_after_spike" "$a1" \
    "time_to_breaker_open_s=${time_to_open_s} (manifold_breaker_state=${breaker_state_at_open}), bound 10s"
  a2="$(num_cmp "$transitions_open" ">=" 1)"
  assert_true 4 "transition_to_open_recorded" "$a2" \
    "manifold_breaker_transitions_total{to=open} for ${port} = ${transitions_open}"
  a3="$(num_cmp "$shed_rate" "<=" 0.01)"
  assert_true 4 "clients_shielded_from_the_slow_backend" "$a3" \
    "client-visible failure rate after the spike = ${shed_rate} (${post_fail}/${post_total})"
  passed="false"; [[ "$a1" == "true" && "$a2" == "true" && "$a3" == "true" ]] && passed="true"

  write_json "${RESULTS_DIR}/04-latency-spike-breaker.json" \
    "scenario=latency_spike_breaker_open" \
    "spiked_backend_port=${port}" \
    "injected_latency=${S4_INJECTED_LATENCY}" \
    "config_response_header_timeout=${S4_RESPONSE_HEADER_TIMEOUT}" \
    "config_response_header_timeout_note=overridden for this scenario only; config.example.yaml ships 10s, which the ${S4_INJECTED_LATENCY} spike does not exceed, so the stock config produces zero failures and the breaker correctly never opens" \
    "config_passive_health=disabled_for_this_scenario" \
    "config_failure_threshold=${CFG_FAILURE_THRESHOLD}" \
    "config_open_for_s=${CFG_OPEN_FOR_S}" \
    "time_to_breaker_open_s=${time_to_open_s}" \
    "time_to_breaker_open_reason=${open_reason}" \
    "metric_poll_resolution_ms=${METRIC_POLL_MS}" \
    "breaker_state_at_detection=${breaker_state_at_open}" \
    "breaker_transitions_to_open=${transitions_open}" \
    "upstream_error_requests_on_spiked_backend=${errors_on_spiked}" \
    "upstream_requests_to_spiked_backend_after_open=${up_delta}" \
    "requests_shed_total_delta=${shed_delta}" \
    "post_spike_shed_rate=${shed_rate}" \
    "post_spike_probe_failures=${post_fail}" \
    "post_spike_probe_requests=${post_total}" \
    "background_requests_failed=${requests_failed}" \
    "background_requests_total=${requests_total}" \
    "background_load_reason=${bg_reason}" \
    "method=metric-based: elapsed from the latency injection until manifold_breaker_state{upstream=...:${port}} reads >= 1 (0 closed / 1 open / 2 half-open), polled every ${METRIC_POLL_MS}ms. post_spike_shed_rate is the CLIENT-visible failure rate, and near-zero is the correct answer, not a miss: with two healthy peers and retry.max_attempts=${CFG_MAX_ATTEMPTS} nothing is refused at the edge. manifold_requests_shed_total counts max_in_flight backpressure only, which this scenario does not exercise, so its delta is expected to be 0." \
    "assertion=breaker opens within 10s of the spike AND a to=open transition is recorded AND client-visible failure rate stays <= 1%" \
    "assertion_passed=${passed}"

  teardown_manifold_stack
}

# ---------------------------------------------------------------------------
# 5. All backends down -> assert fail-fast, not hang; record time-to-first-error
# ---------------------------------------------------------------------------
scenario_5() {
  local logdir="${RESULTS_DIR}/05-all-backends-down"
  mkdir -p "$logdir"
  log "=== scenario 5: all backends down -> fail-fast assertion ==="
  setup_manifold_stack "$logdir"

  log "confirming steady state before killing all backends"
  wait_for_http_ok "http://127.0.0.1:8080/" 10 || warn "LB not answering cleanly before the test even started -- results may be meaningless"

  log "killing all 3 backends"
  local pid
  for pid in "${BACKEND_PIDS[@]}"; do
    kill -KILL "$pid" 2>/dev/null || true
  done
  BACKEND_PIDS=()

  # Single request, wall-clock timed, with a hard client-side cap so a
  # genuine hang doesn't wedge the harness itself.
  local start_ms end_ms elapsed_s http_code
  start_ms="$(now_ms)"
  http_code="$(curl -s -o /dev/null -m 30 -w '%{http_code}' "http://127.0.0.1:8080/" 2>/dev/null || echo "000")"
  end_ms="$(now_ms)"
  elapsed_s="$(awk -v a="$start_ms" -v b="$end_ms" 'BEGIN{printf "%.3f", (b-a)/1000.0}')"

  local fail_fast
  fail_fast="$(num_cmp "$elapsed_s" "<" "$FAIL_FAST_THRESHOLD_S")"

  # 502 and 503 are both correct and mean different things. 502 is
  # "manifold tried the backends and the connections were refused" -- the
  # window between the kill and ejection. 503 is "the pool has no eligible
  # backend", i.e. ejection already happened. A hang, a 200, or a 000
  # client-side timeout is the failure.
  local status_ok="false"
  [[ "$http_code" == "502" || "$http_code" == "503" ]] && status_ok="true"

  assert_true 5 "fail_fast_not_hang" "$fail_fast" \
    "responded in ${elapsed_s}s with all backends down (threshold ${FAIL_FAST_THRESHOLD_S}s)"
  assert_true 5 "status_is_502_or_503" "$status_ok" \
    "http_status=${http_code} (502 = connection refused before ejection, 503 = no eligible backend after it; both correct)"

  local passed="false"
  [[ "$fail_fast" == "true" && "$status_ok" == "true" ]] && passed="true"

  write_json "${RESULTS_DIR}/05-all-backends-down.json" \
    "scenario=all_backends_down_fail_fast" \
    "time_to_first_error_s=${elapsed_s}" \
    "http_status=${http_code}" \
    "fail_fast_threshold_s=${FAIL_FAST_THRESHOLD_S}" \
    "expected_status=502_or_503" \
    "assertion=responds in under ${FAIL_FAST_THRESHOLD_S}s with 502 or 503, rather than hanging" \
    "assertion_passed=${passed}"

  # Backends are already dead; only manifold needs stopping.
  stop_manifold
}

# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

for s in $SCENARIOS; do
  case "$s" in
    1) scenario_1 ;;
    2) scenario_2 ;;
    3) scenario_3 ;;
    4) scenario_4 ;;
    5) scenario_5 ;;
    *) warn "unknown scenario id '${s}', skipping" ;;
  esac
  scenario_cleanup
done

# ---------------------------------------------------------------------------
# Summary + exit code
# ---------------------------------------------------------------------------
python3 - "${RESULTS_DIR}/summary.json" "$SCENARIOS" "$AC_POWER_STATE" "$ASSERTION_FAILURES" "${ASSERTION_LOG[@]}" <<'PYEOF'
import json, sys
path, scenarios, power, failures = sys.argv[1:5]
rows = []
for raw in sys.argv[5:]:
    sid, name, verdict, detail = raw.split("|", 3)
    rows.append({"scenario": int(sid), "assertion": name,
                 "passed": verdict == "true", "detail": detail})
doc = {
    "scenarios_run": [int(x) for x in scenarios.split()],
    "power_state": power,
    "assertions_total": len(rows),
    "assertions_failed": int(failures),
    "all_passed": int(failures) == 0,
    "assertions": rows,
}
with open(path, "w", encoding="utf-8") as f:
    json.dump(doc, f, indent=2)
    f.write("\n")
PYEOF
log "wrote ${RESULTS_DIR}/summary.json"

log "failure scenarios complete. Results: ${RESULTS_DIR}"
if (( ASSERTION_FAILURES > 0 )); then
  warn "${ASSERTION_FAILURES} assertion(s) failed -- exiting 2 (the harness ran fine; manifold did not meet a target)"
  exit 2
fi
log "all assertions passed"
exit 0
