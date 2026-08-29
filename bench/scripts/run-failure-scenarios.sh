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
#
# IMPORTANT / assumptions made without access to the concurrently-developed
# manifold internals (internal/proxy, internal/observe were empty at the
# time this harness was written):
#   - Config hot-reload trigger: assumed to be SIGHUP to the manifold
#     process (the common Go convention). Override with
#     MANIFOLD_RELOAD_SIGNAL=<signal name> if manifold uses something else
#     (e.g. an admin HTTP endpoint) -- see reload_manifold() below, which
#     is the single place to change if so.
#   - Breaker-open detection: tries to grep it out of manifold's
#     Prometheus /metrics first (best-effort pattern match on
#     "breaker" + "open" in a metric name/value); if that pattern doesn't
#     match manifold's actual metric names, falls back to a purely
#     client-observable heuristic (see scenario 4 below).
# Both are called out again inline at the point they're used.
#
# Usage:
#   ./run-failure-scenarios.sh
#   SCENARIOS="1 4" ./run-failure-scenarios.sh   # run a subset
#   FORCE=1 ./run-failure-scenarios.sh           # override the on-battery refusal (see lib.sh:check_ac_power)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_tools k6 taskset python3 curl date awk nproc
require_built_binaries
compute_core_groups
check_ac_power

TIMESTAMP="$(date -u '+%Y%m%dT%H%M%SZ')"
RESULTS_DIR="${RESULTS_ROOT}/${TIMESTAMP}-failure-scenarios"
mkdir -p "$RESULTS_DIR"
log "results directory: ${RESULTS_DIR}"

SCENARIOS="${SCENARIOS:-1 2 3 4 5}"
BACKGROUND_LOAD_VUS="${BACKGROUND_LOAD_VUS:-100}"
MANIFOLD_RELOAD_SIGNAL="${MANIFOLD_RELOAD_SIGNAL:-HUP}"
FAIL_FAST_THRESHOLD_S="${FAIL_FAST_THRESHOLD_S:-5}"

trap cleanup_all EXIT INT TERM

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

start_background_load() {
  local out_json="$1" duration="$2"
  TARGET="manifold" STRATEGY="round_robin" CONCURRENCY="$BACKGROUND_LOAD_VUS" BACKEND_LATENCY="scenario" RUN_INDEX="1" \
    run_k6 "http://127.0.0.1:8080" "constant-vus" "$BACKGROUND_LOAD_VUS" 0 "$duration" "/" "$out_json" &
  echo $!
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

  local bg_json="${logdir}/background-load.json"
  local bg_pid probe_file="${logdir}/probe.tsv"
  bg_pid="$(start_background_load "$bg_json" "30s")"
  local probe_pid
  probe_pid="$(start_probe_loop "http://127.0.0.1:8080/" "$probe_file" 0.1)"

  sleep 8   # let steady state establish before injecting the failure

  local kill_epoch_ms
  kill_epoch_ms="$(date +%s%3N)"
  log "killing backend on port ${BACKEND_PORTS[0]} (pid ${BACKEND_PIDS[0]})"
  kill -KILL "${BACKEND_PIDS[0]}" 2>/dev/null || true

  # Wait for the background load + probe to run out their duration.
  wait "$bg_pid" 2>/dev/null || true
  stop_probe_loop "$probe_pid"

  # Ejection shows up as the probe stream returning to a clean run of 2xx
  # after the kill, once manifold stops routing to the dead backend and
  # requests stop occasionally landing on it. We look for the point after
  # which failures against the pool as a whole stop, which for a 3-backend
  # pool implies the dead one has been taken out of rotation.
  local time_to_clean_s
  time_to_clean_s="$(time_to_sustained_ok "$probe_file" '^2' "$kill_epoch_ms" 20)"

  local requests_failed="null" requests_total="null"
  if [[ -s "$bg_json" ]]; then
    requests_failed="$(python3 -c "import json;print(json.load(open('${bg_json}'))['requests_failed'])")"
    requests_total="$(python3 -c "import json;print(json.load(open('${bg_json}'))['requests_total'])")"
  fi

  write_json "${RESULTS_DIR}/01-kill-backend.json" \
    "scenario=kill_backend_at_steady_state" \
    "killed_backend_port=${BACKEND_PORTS[0]}" \
    "time_to_clean_traffic_s=${time_to_clean_s}" \
    "requests_failed=${requests_failed}" \
    "requests_total=${requests_total}" \
    "method=probe-based: elapsed time from SIGKILL to first sustained run of 20 consecutive 2xx probes at 100ms interval against the LB" \
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

  log "killing backend on port ${port}"
  kill -KILL "${BACKEND_PIDS[0]}" 2>/dev/null || true
  sleep 5   # let manifold's active/passive health actually eject it first

  local probe_file="${logdir}/probe.tsv"
  local probe_pid; probe_pid="$(start_probe_loop "http://127.0.0.1:8080/" "$probe_file" 0.1)"
  local bg_json="${logdir}/background-load.json"
  local bg_pid; bg_pid="$(start_background_load "$bg_json" "30s")"

  sleep 3
  local restart_epoch_ms; restart_epoch_ms="$(date +%s%3N)"
  log "restarting backend on port ${port}"
  local new_pid
  new_pid="$(pinned_start "$CORES_BACKENDS" "${logdir}/backend-${port}-restarted.log" \
    "$BACKEND_BIN" -addr "127.0.0.1:${port}" -admin "127.0.0.1:${admin_port}" \
    -id "backend-${port}" -latency "1ms" -jitter "0ms" -error-rate "0" -health-path "/healthz")"
  BACKEND_PIDS[0]="$new_pid"
  if ! wait_for_http_ok "http://127.0.0.1:${port}/healthz" 15; then
    warn "restarted backend on port ${port} never became healthy itself -- readmission cannot happen"
  fi

  wait "$bg_pid" 2>/dev/null || true
  stop_probe_loop "$probe_pid"

  # Readmission is inferred as manifold's active health check passing
  # health.active.healthy_threshold (2, per config.example.yaml) consecutive
  # probes and resuming routing to it -- observable client-side as request
  # latency/behavior settling back to the pre-failure steady state. We
  # approximate it as: time from process restart to the point client
  # traffic against the LB has run health.active.interval * healthy_threshold
  # (2s * 2 = 4s, from config.example.yaml) worth of clean 2xx responses,
  # which is the earliest the active checker could plausibly have readmitted
  # it. This is an upper-bound estimate, not an exact readmission timestamp,
  # since the client can't directly observe manifold's internal health
  # state machine.
  local time_to_clean_s
  time_to_clean_s="$(time_to_sustained_ok "$probe_file" '^2' "$restart_epoch_ms" 20)"

  write_json "${RESULTS_DIR}/02-restart-backend.json" \
    "scenario=restart_backend_readmission" \
    "restarted_backend_port=${port}" \
    "approx_time_to_readmission_s=${time_to_clean_s}" \
    "method=upper_bound_estimate: elapsed time from process restart to first sustained run of 20 consecutive 2xx probes at 100ms against the LB (client cannot see manifold's internal health state machine directly)" \
    "config_healthy_threshold=2" \
    "config_active_interval_s=2"

  teardown_manifold_stack
}

# ---------------------------------------------------------------------------
# 3. Hot config reload x10 under sustained load -> connections dropped (target 0)
# ---------------------------------------------------------------------------
scenario_3() {
  local logdir="${RESULTS_DIR}/03-hot-reload"
  mkdir -p "$logdir"
  log "=== scenario 3: hot config reload x10 under sustained load ==="
  log "ASSUMPTION: reload is triggered via SIGHUP (override with MANIFOLD_RELOAD_SIGNAL=<sig> if manifold uses a different mechanism, e.g. an admin HTTP endpoint)"
  setup_manifold_stack "$logdir"

  local bg_json="${logdir}/background-load.json"
  local probe_file="${logdir}/probe.tsv"
  local probe_pid; probe_pid="$(start_probe_loop "http://127.0.0.1:8080/" "$probe_file" 0.1)"
  local bg_pid; bg_pid="$(start_background_load "$bg_json" "25s")"

  sleep 3
  local reload_epoch_ms_start; reload_epoch_ms_start="$(date +%s%3N)"
  local i
  for i in $(seq 1 10); do
    log "reload ${i}/10"
    if ! reload_manifold; then
      warn "SIGHUP delivery failed on reload ${i} -- process may have exited; check ${logdir}/manifold.log"
      break
    fi
    sleep 1.5
  done
  local reload_epoch_ms_end; reload_epoch_ms_end="$(date +%s%3N)"

  wait "$bg_pid" 2>/dev/null || true
  stop_probe_loop "$probe_pid"

  # "Connections dropped" is read off the client-observed failure count
  # during the reload window specifically (not the whole run), since a
  # config swap that drains cleanly should show zero probe failures
  # between reload_epoch_ms_start and reload_epoch_ms_end.
  local dropped
  dropped="$(awk -v t0="$reload_epoch_ms_start" -v t1="$reload_epoch_ms_end" \
    '$1 >= t0 && $1 <= t1 && $2 !~ /^2/ { n++ } END { print n+0 }' "$probe_file")"

  local requests_failed="null" requests_total="null"
  if [[ -s "$bg_json" ]]; then
    requests_failed="$(python3 -c "import json;print(json.load(open('${bg_json}'))['requests_failed'])")"
    requests_total="$(python3 -c "import json;print(json.load(open('${bg_json}'))['requests_total'])")"
  fi

  write_json "${RESULTS_DIR}/03-hot-reload-x10.json" \
    "scenario=hot_reload_x10_under_load" \
    "reload_count=10" \
    "reload_signal=${MANIFOLD_RELOAD_SIGNAL}" \
    "probe_failures_during_reload_window=${dropped}" \
    "background_requests_failed=${requests_failed}" \
    "background_requests_total=${requests_total}" \
    "target=0 probe failures during the reload window (a clean drain-and-swap should not error in-flight or new client-facing requests)"

  teardown_manifold_stack
}

# ---------------------------------------------------------------------------
# 4. Drive one backend's latency 1ms -> 2000ms via its control port ->
#    time for breaker to open, shed rate
# ---------------------------------------------------------------------------
scenario_4() {
  local logdir="${RESULTS_DIR}/04-latency-spike-breaker"
  mkdir -p "$logdir"
  log "=== scenario 4: latency spike on one backend -> breaker open, shed rate ==="
  setup_manifold_stack "$logdir"

  local probe_file="${logdir}/probe.tsv"
  local probe_pid; probe_pid="$(start_probe_loop "http://127.0.0.1:8080/" "$probe_file" 0.1)"
  local bg_json="${logdir}/background-load.json"
  local bg_pid; bg_pid="$(start_background_load "$bg_json" "30s")"

  sleep 5
  local spike_epoch_ms; spike_epoch_ms="$(date +%s%3N)"
  log "driving backend ${BACKEND_PORTS[0]} latency to 2000ms via its control port"
  backend_control 0 "latency?d=2000ms" >/dev/null || warn "backend_control latency injection failed -- check bench/backend's actual /_control/latency contract"

  local metrics_before="${logdir}/metrics-before.txt" metrics_after="${logdir}/metrics-after.txt"
  scrape_manifold_metrics > "$metrics_before"

  sleep 15
  scrape_manifold_metrics > "$metrics_after"

  wait "$bg_pid" 2>/dev/null || true
  stop_probe_loop "$probe_pid"

  # Primary signal: a metric whose name mentions both "breaker" and "open"
  # appearing (or increasing) in /metrics after the spike. Best-effort --
  # manifold's actual metric names weren't available when this was written
  # (internal/observe was empty). If this doesn't match, breaker_open_detected
  # will be false and the fallback heuristic below is what to trust.
  local breaker_metric_hit="false"
  if grep -Eiq 'breaker.*open|open.*breaker' "$metrics_after" 2>/dev/null; then
    breaker_metric_hit="true"
  fi

  # Fallback / corroborating signal, purely client-observable: once the
  # breaker opens for the slow backend, requests that would have hit it
  # should fail fast (low time_total) instead of hanging ~2s. We look for
  # the first probe after the spike whose time_total is short (<0.5s) AND
  # non-2xx (a fast shed, i.e. 503), as opposed to the initial period where
  # some fraction of requests take ~2s waiting on the slow backend.
  local time_to_fast_shed_s
  time_to_fast_shed_s="$(awk -v t0="$spike_epoch_ms" '
    $1 >= t0 && $2 !~ /^2/ && $3 < 0.5 { printf "%.3f\n", ($1 - t0) / 1000.0; found=1; exit }
    END { if (!found) print "null" }
  ' "$probe_file")"

  local shed_rate
  shed_rate="$(awk -v t0="$spike_epoch_ms" '
    $1 >= t0 { total++; if ($2 !~ /^2/) sheds++ }
    END { if (total > 0) printf "%.4f\n", sheds/total; else print "null" }
  ' "$probe_file")"

  write_json "${RESULTS_DIR}/04-latency-spike-breaker.json" \
    "scenario=latency_spike_breaker_open" \
    "spiked_backend_port=${BACKEND_PORTS[0]}" \
    "injected_latency=2000ms" \
    "breaker_metric_hit=${breaker_metric_hit}" \
    "approx_time_to_fast_shed_s=${time_to_fast_shed_s}" \
    "post_spike_shed_rate=${shed_rate}" \
    "config_failure_threshold=5" \
    "config_open_for_s=5" \
    "method=primary:grep /metrics for a breaker/open metric name; fallback:first fast(<500ms) non-2xx probe after the spike, since breaker-open is the only mechanism that turns a 2s hang into an immediate 503"

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
  start_ms="$(date +%s%3N)"
  http_code="$(curl -s -o /dev/null -m 30 -w '%{http_code}' "http://127.0.0.1:8080/" 2>/dev/null || echo "000")"
  end_ms="$(date +%s%3N)"
  elapsed_s="$(awk -v a="$start_ms" -v b="$end_ms" 'BEGIN{printf "%.3f", (b-a)/1000.0}')"

  local fail_fast="false"
  if awk -v e="$elapsed_s" -v t="$FAIL_FAST_THRESHOLD_S" 'BEGIN{exit !(e < t)}'; then
    fail_fast="true"
  fi

  if [[ "$fail_fast" != "true" ]]; then
    warn "FAIL-FAST ASSERTION FAILED: response took ${elapsed_s}s (threshold ${FAIL_FAST_THRESHOLD_S}s) with all backends down -- this looks like a hang, not a fast failure"
  else
    log "fail-fast OK: ${elapsed_s}s to respond (http_code=${http_code}) with all backends down"
  fi

  write_json "${RESULTS_DIR}/05-all-backends-down.json" \
    "scenario=all_backends_down_fail_fast" \
    "time_to_first_error_s=${elapsed_s}" \
    "http_status=${http_code}" \
    "fail_fast_threshold_s=${FAIL_FAST_THRESHOLD_S}" \
    "assertion_passed=${fail_fast}"

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
done

log "failure scenarios complete. Results: ${RESULTS_DIR}"
