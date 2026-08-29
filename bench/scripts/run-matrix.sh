#!/usr/bin/env bash
# bench/scripts/run-matrix.sh
#
# Runs the full manifold-vs-nginx-vs-direct benchmark matrix:
#
#   concurrency (VUs): 50, 200, 1000 (5000 opt-in, see INCLUDE_C5000 below)
#   backend latency:   1ms, 25ms
#   strategy:          round_robin, least_conn, consistent_hash
#   target:            manifold, nginx, direct
#   3 runs per cell, medians (and min/max) reported by parse-results.py.
#
# ---------------------------------------------------------------------------
# WHY THE LOOP IS ORDERED THE WAY IT IS (read this before "simplifying" it)
# ---------------------------------------------------------------------------
# Target hardware is a laptop (Lenovo Yoga Slim 7 15ILL9, thin-and-light
# chassis, ~17-30W sustained envelope) running the whole stack -- k6, the
# LB, and 3 backends -- on one 8-core die. A matrix this size is a long
# sustained load, and the chassis WILL thermally throttle partway through.
#
# If the loop ran all manifold cells, then all nginx cells, then all
# direct cells (the natural, obvious way to write it), manifold and nginx
# would systematically be measured at DIFFERENT thermal states -- whichever
# ran first gets the cool chassis, whichever ran later eats the throttle.
# That confounds the entire manifold-vs-nginx comparison, which is the
# headline number of this whole project. A "manifold is 12% faster" result
# is worthless if it's actually "whichever target happened to run before
# the fans gave up is 12% faster".
#
# So: for a fixed (concurrency, latency, strategy, run), the applicable
# targets run BACK TO BACK, in randomized order (target is the innermost
# loop, shuffled with `shuf` each time), each preceded by a cooldown. This
# keeps the thermal state as close to identical as possible across targets
# at the moment each one is measured, and randomizing (rather than fixing
# an order) prevents any *systematic* first-mover advantage/disadvantage
# from building up across the whole matrix. See also the drift-check at
# the end of this script, which re-measures the very first cell at the
# very end specifically to catch whatever thermal drift this ordering
# doesn't fully cancel out.
#
# One consequence: nginx only has a strategy=round_robin config, and
# `direct` has no strategy dimension at all (see bench/nginx/nginx.conf's
# header and this file's active-targets logic below). Running either of
# them redundantly under every strategy value would triple otherwise-
# identical measurements for no benefit while adding more thermal load, so
# nginx is only included in the strategy=round_robin pass, and `direct`
# only in the very first strategy pass of each (concurrency, latency)
# pair. Both are still fully subject to the back-to-back interleaving with
# whichever other target(s) are active in that pass.
#
# Usage:
#   ./run-matrix.sh
#   CONCURRENCIES="50 200" LATENCIES="1ms" TARGETS="manifold direct" ./run-matrix.sh
#   RUNS_PER_CELL=1 WARMUP_DURATION=3s MEASURE_DURATION=10s ./run-matrix.sh   # smoke test
#   INCLUDE_C5000=1 ./run-matrix.sh   # add the harness-limited c=5000 cell, see below
#   FORCE=1 ./run-matrix.sh           # override the on-battery refusal (not recommended)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./lib.sh
source "${SCRIPT_DIR}/lib.sh"

# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------

require_tools k6 nginx taskset python3 curl envsubst getconf go awk date nproc shuf
require_built_binaries
compute_core_groups
check_ac_power

LATENCIES=(${LATENCIES:-1ms 25ms})
STRATEGIES=(${STRATEGIES:-round_robin least_conn consistent_hash})
TARGETS=(${TARGETS:-manifold nginx direct})
RUNS_PER_CELL="${RUNS_PER_CELL:-3}"
WARMUP_DURATION="${WARMUP_DURATION:-10s}"
MEASURE_DURATION="${MEASURE_DURATION:-30s}"
# Cooldown between EVERY individual measurement (not just between cells) --
# with target as the innermost loop, a "cell" boundary now happens after
# every single k6 run. 20s is a starting point for a thin-and-light chassis,
# not a measured recovery time; tune it up if the drift-check at the end
# keeps flagging thermally_compromised.
COOLDOWN_SECS="${COOLDOWN_SECS:-20}"
REQUEST_PATH="${REQUEST_PATH:-/}"
# rps drift beyond this percent between the first cell and its end-of-matrix
# re-run gets flagged thermally_compromised in meta.json / table.md.
DRIFT_THRESHOLD_PCT="${DRIFT_THRESHOLD_PCT:-10}"
INCLUDE_C5000="${INCLUDE_C5000:-0}"

# c=5000 on this 8-core laptop means k6 (2 cores), the LB, and 3 backends
# (sharing 4 cores) are all fighting for roughly 15000 sockets total. That
# cell measures the harness's own ceiling, not manifold's or nginx's -- see
# bench/README.md's "c=5000" section. Opt-in only, and reported separately.
if [[ -n "${CONCURRENCIES:-}" ]]; then
  CONCURRENCIES=(${CONCURRENCIES})
elif [[ "$INCLUDE_C5000" == "1" ]]; then
  CONCURRENCIES=(50 200 1000 5000)
  log "INCLUDE_C5000=1: including the harness-limited c=5000 cell (see bench/README.md)"
else
  CONCURRENCIES=(50 200 1000)
fi

TIMESTAMP="$(date -u '+%Y%m%dT%H%M%SZ')"
RESULTS_DIR="${RESULTS_ROOT}/${TIMESTAMP}"
mkdir -p "$RESULTS_DIR"
log "results directory: ${RESULTS_DIR}"

trap cleanup_all EXIT INT TERM

# ---------------------------------------------------------------------------
# Provenance: a benchmark without it is not reproducible.
# ---------------------------------------------------------------------------

write_meta() {
  local git_commit
  git_commit="$(cd "$REPO_ROOT" && git rev-parse HEAD 2>/dev/null || echo 'unknown (no commits in repo)')"
  local git_dirty="clean"
  if ! (cd "$REPO_ROOT" && git diff --quiet 2>/dev/null && git diff --cached --quiet 2>/dev/null); then
    git_dirty="dirty (uncommitted changes present at run time)"
  fi

  python3 - "$RESULTS_DIR/meta.json" <<PYEOF
import json, platform, subprocess, sys

out_path = sys.argv[1]

def sh(cmd):
    try:
        return subprocess.check_output(cmd, shell=True, text=True, stderr=subprocess.STDOUT).strip()
    except Exception as e:
        return f"unavailable ({e})"

meta = {
    "timestamp_utc": "${TIMESTAMP}",
    "hostname": platform.node(),
    "kernel": sh("uname -a"),
    "nproc": int(sh("nproc")),
    "core_assignment": {
        "k6": "${CORES_K6}",
        "lb": "${CORES_LB}",
        "backends": "${CORES_BACKENDS}",
    },
    "go_version": sh("go version"),
    "nginx_version": sh("nginx -V"),
    "k6_version": sh("k6 version"),
    "git_commit": "${git_commit}",
    "git_working_tree": "${git_dirty}",
    "matrix": {
        "concurrencies": "${CONCURRENCIES[*]}".split(),
        "latencies": "${LATENCIES[*]}".split(),
        "strategies": "${STRATEGIES[*]}".split(),
        "targets": "${TARGETS[*]}".split(),
        "runs_per_cell": ${RUNS_PER_CELL},
        "warmup_duration": "${WARMUP_DURATION}",
        "measure_duration": "${MEASURE_DURATION}",
        "cooldown_secs": ${COOLDOWN_SECS},
        "request_path": "${REQUEST_PATH}",
        "include_c5000": ${INCLUDE_C5000} == 1,
    },
    "hardware_note": "Lenovo Yoga Slim 7 15ILL9, Intel Core Ultra 7 256V (4P+4E, no SMT), 16GB RAM, running under WSL2 -- see bench/README.md for the P/E-core and thermal caveats this implies.",
    "thermally_compromised": None,
    "drift_check": None,
}

with open(out_path, "w", encoding="utf-8") as f:
    json.dump(meta, f, indent=2)
    f.write("\n")
PYEOF
  log "wrote ${RESULTS_DIR}/meta.json"
}

write_meta

# ---------------------------------------------------------------------------
# One measured cell: fresh backends + (optional) LB, warm up, sample,
# measure, merge, stop, cool down. Every single measurement in the matrix
# goes through this -- see the header comment for why targets are no longer
# batched under one long-lived LB process.
# ---------------------------------------------------------------------------

run_one_target_cell() {
  local target="$1" strategy_label="$2" concurrency="$3" latency="$4" run_idx="$5" out_json="$6"
  local cell_log_dir="${RESULTS_DIR}/logs/${target}-${strategy_label}-c${concurrency}-l${latency}-run${run_idx}"
  mkdir -p "$cell_log_dir"

  start_backends "$latency" "$cell_log_dir"

  local target_url="" lb_pid=""
  case "$target" in
    manifold)
      start_manifold "$strategy_label" "$cell_log_dir"
      target_url="http://127.0.0.1:8080"
      lb_pid="$MANIFOLD_PID"
      ;;
    nginx)
      start_nginx "$cell_log_dir"
      target_url="http://127.0.0.1:8080"
      lb_pid="$NGINX_PID"
      ;;
    direct)
      target_url="http://127.0.0.1:${BACKEND_PORTS[0]}"
      ;;
    *)
      die "unknown target '${target}'"
      ;;
  esac

  # Warm up (discarded) so connection-pool/JIT warmup isn't counted.
  local warm_json="${out_json%.json}.warmup.json"
  TARGET="$target" STRATEGY="$strategy_label" CONCURRENCY="$concurrency" BACKEND_LATENCY="$latency" RUN_INDEX="warmup" \
    run_k6 "$target_url" "constant-vus" "$concurrency" 0 "$WARMUP_DURATION" "$REQUEST_PATH" "$warm_json"
  rm -f "$warm_json"

  local sampler_pid="" samples_file="${cell_log_dir}/samples.tsv"
  if [[ -n "$lb_pid" ]]; then
    sampler_pid="$(start_cpu_sampler "$lb_pid" "$samples_file" 0.5)"
  fi

  TARGET="$target" STRATEGY="$strategy_label" CONCURRENCY="$concurrency" BACKEND_LATENCY="$latency" RUN_INDEX="$run_idx" \
    run_k6 "$target_url" "constant-vus" "$concurrency" 0 "$MEASURE_DURATION" "$REQUEST_PATH" "$out_json"

  local cpu_pct="null" rss_avg="null" rss_max="null"
  if [[ -n "$sampler_pid" ]]; then
    stop_cpu_sampler "$sampler_pid"
    read -r cpu_pct rss_avg rss_max < <(summarize_cpu_rss "$samples_file")
  fi

  python3 - "$out_json" "$cpu_pct" "$rss_avg" "$rss_max" "$K6_EXIT_CODE" <<'PYEOF'
import json, sys

path, cpu_pct, rss_avg, rss_max, k6_exit = sys.argv[1:6]

with open(path, "r", encoding="utf-8") as f:
    data = json.load(f)

def num_or_none(s):
    if s == "null":
        return None
    try:
        return float(s)
    except ValueError:
        return None

data["lb_cpu_pct"] = num_or_none(cpu_pct)
data["lb_rss_kb_avg"] = num_or_none(rss_avg)
data["lb_rss_kb_max"] = num_or_none(rss_max)
data["k6_exit_code"] = int(k6_exit)

with open(path, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2)
    f.write("\n")
PYEOF

  case "$target" in
    manifold) stop_manifold ;;
    nginx) stop_nginx ;;
  esac
  stop_backends

  log "cooling down ${COOLDOWN_SECS}s before the next measurement"
  sleep "$COOLDOWN_SECS"
}

# ---------------------------------------------------------------------------
# Matrix driver: concurrency > latency > strategy > run > target (shuffled).
# See the header comment for why target is innermost and shuffled.
# ---------------------------------------------------------------------------

FIRST_CELL_CAPTURED=0
FIRST_CELL_TARGET=""
FIRST_CELL_STRATEGY=""
FIRST_CELL_CONCURRENCY=""
FIRST_CELL_LATENCY=""

for concurrency in "${CONCURRENCIES[@]}"; do
  for latency in "${LATENCIES[@]}"; do
    for strategy in "${STRATEGIES[@]}"; do
      is_first_strategy_pass=0
      [[ "$strategy" == "${STRATEGIES[0]}" ]] && is_first_strategy_pass=1

      active_targets=()
      for t in "${TARGETS[@]}"; do
        case "$t" in
          manifold) active_targets+=("manifold") ;;
          nginx)
            if [[ "$strategy" == "round_robin" ]]; then
              active_targets+=("nginx")
            fi
            ;;
          direct)
            if (( is_first_strategy_pass )); then
              active_targets+=("direct")
            fi
            ;;
        esac
      done

      if [[ ${#active_targets[@]} -eq 0 ]]; then
        continue
      fi

      for (( run_idx=1; run_idx<=RUNS_PER_CELL; run_idx++ )); do
        readarray -t shuffled_targets < <(shuf -e "${active_targets[@]}")
        log "=== c=${concurrency} l=${latency} strategy=${strategy} run=${run_idx}/${RUNS_PER_CELL} targets(order)=${shuffled_targets[*]} ==="

        for target in "${shuffled_targets[@]}"; do
          case "$target" in
            manifold) strategy_label="$strategy" ;;
            nginx) strategy_label="round_robin" ;;
            direct) strategy_label="direct" ;;
          esac

          out_json="${RESULTS_DIR}/${target}-${strategy_label}-c${concurrency}-l${latency}-run${run_idx}.json"
          log "--- ${target}/${strategy_label}/c=${concurrency}/l=${latency} run ${run_idx}/${RUNS_PER_CELL} ---"
          run_one_target_cell "$target" "$strategy_label" "$concurrency" "$latency" "$run_idx" "$out_json"
          log "wrote ${out_json}"

          if [[ $FIRST_CELL_CAPTURED -eq 0 ]]; then
            FIRST_CELL_CAPTURED=1
            FIRST_CELL_TARGET="$target"
            FIRST_CELL_STRATEGY="$strategy_label"
            FIRST_CELL_CONCURRENCY="$concurrency"
            FIRST_CELL_LATENCY="$latency"
            log "first cell recorded for end-of-matrix drift check: ${FIRST_CELL_TARGET}/${FIRST_CELL_STRATEGY}/c=${FIRST_CELL_CONCURRENCY}/l=${FIRST_CELL_LATENCY}"
          fi
        done
      done
    done
  done
done

log "matrix complete."

# ---------------------------------------------------------------------------
# Thermal drift check: re-run the very first cell measured, at the very
# end, and compare. A benchmark on this chassis is not credible without
# this -- see the header comment and bench/README.md.
# ---------------------------------------------------------------------------

run_drift_check() {
  if [[ $FIRST_CELL_CAPTURED -eq 0 ]]; then
    warn "no cells were run (empty matrix?) -- skipping drift check"
    return 0
  fi

  log "=== drift check: re-running first cell (${FIRST_CELL_TARGET}/${FIRST_CELL_STRATEGY}/c=${FIRST_CELL_CONCURRENCY}/l=${FIRST_CELL_LATENCY}) ==="

  local original_pattern="${RESULTS_DIR}/${FIRST_CELL_TARGET}-${FIRST_CELL_STRATEGY}-c${FIRST_CELL_CONCURRENCY}-l${FIRST_CELL_LATENCY}-run"
  local recheck_files=()
  for (( i=1; i<=RUNS_PER_CELL; i++ )); do
    local out_json="${RESULTS_DIR}/drift-check-run${i}.json"
    run_one_target_cell "$FIRST_CELL_TARGET" "$FIRST_CELL_STRATEGY" "$FIRST_CELL_CONCURRENCY" "$FIRST_CELL_LATENCY" "drift${i}" "$out_json"
    recheck_files+=("$out_json")
  done

  python3 - "$RESULTS_DIR/drift-check.json" "$original_pattern" "$DRIFT_THRESHOLD_PCT" "${recheck_files[@]}" <<'PYEOF'
import glob, json, statistics, sys

out_path = sys.argv[1]
original_pattern = sys.argv[2]
threshold_pct = float(sys.argv[3])
recheck_files = sys.argv[4:]

def median_rps(paths):
    vals = []
    for p in paths:
        try:
            with open(p, "r", encoding="utf-8") as f:
                d = json.load(f)
            if d.get("rps") is not None:
                vals.append(d["rps"])
        except (OSError, json.JSONDecodeError):
            pass
    return statistics.median(vals) if vals else None

original_files = sorted(glob.glob(original_pattern + "*.json"))
original_median = median_rps(original_files)
recheck_median = median_rps(recheck_files)

result = {
    "original_files": original_files,
    "original_median_rps": original_median,
    "recheck_files": recheck_files,
    "recheck_median_rps": recheck_median,
    "drift_threshold_pct": threshold_pct,
    "drift_pct": None,
    "thermally_compromised": False,
}

if original_median and recheck_median is not None:
    drift_pct = abs(recheck_median - original_median) / original_median * 100.0
    result["drift_pct"] = drift_pct
    result["thermally_compromised"] = drift_pct > threshold_pct
else:
    result["thermally_compromised"] = True
    result["note"] = "could not compute one or both medians -- treating as compromised out of caution"

with open(out_path, "w", encoding="utf-8") as f:
    json.dump(result, f, indent=2)
    f.write("\n")

print(json.dumps(result))
PYEOF

  local compromised
  compromised="$(python3 -c "import json; print(json.load(open('${RESULTS_DIR}/drift-check.json'))['thermally_compromised'])")"

  if [[ "$compromised" == "True" ]]; then
    warn "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    warn "!!! THERMALLY COMPROMISED: first-cell rps drifted beyond ${DRIFT_THRESHOLD_PCT}% !!!"
    warn "!!! between the start and end of this matrix run. Treat all results  !!!"
    warn "!!! in ${RESULTS_DIR} with suspicion -- see drift-check.json.         !!!"
    warn "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
  else
    log "drift check OK: first-cell rps stable within ${DRIFT_THRESHOLD_PCT}%"
  fi

  # Fold the verdict into meta.json so parse-results.py can surface it.
  python3 - "$RESULTS_DIR/meta.json" "$RESULTS_DIR/drift-check.json" <<'PYEOF'
import json, sys

meta_path, drift_path = sys.argv[1], sys.argv[2]

with open(meta_path, "r", encoding="utf-8") as f:
    meta = json.load(f)
with open(drift_path, "r", encoding="utf-8") as f:
    drift = json.load(f)

meta["thermally_compromised"] = drift["thermally_compromised"]
meta["drift_check"] = drift

with open(meta_path, "w", encoding="utf-8") as f:
    json.dump(meta, f, indent=2)
    f.write("\n")
PYEOF
  log "updated ${RESULTS_DIR}/meta.json with drift-check verdict"
}

run_drift_check

log "all done. Results: ${RESULTS_DIR}"
log "Next: python3 ${SCRIPT_DIR}/parse-results.py ${RESULTS_DIR}"
