#!/usr/bin/env bash
# bench/scripts/lib.sh
#
# Shared helpers for the manifold benchmark harness. Sourced by
# run-matrix.sh and run-failure-scenarios.sh. Not meant to be executed
# directly.
#
# Targets WSL2 Ubuntu. Everything here assumes GNU coreutils, /proc, and
# bash >= 4. It is deliberately dependency-light: taskset (util-linux),
# curl, awk, python3, and getconf are the only things it leans on besides
# the tools each caller requires for itself (k6, nginx, go build output).

# Guard against being executed instead of sourced.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  echo "lib.sh is a library, source it from another script (e.g. 'source lib.sh')." >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

LIB_SH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${LIB_SH_DIR}/../.." && pwd)"
BENCH_DIR="${REPO_ROOT}/bench"
BIN_DIR="${REPO_ROOT}/bin"
RESULTS_ROOT="${BENCH_DIR}/results"
NGINX_CONF_TEMPLATE="${BENCH_DIR}/nginx/nginx.conf"

MANIFOLD_BIN="${BIN_DIR}/manifold"
BACKEND_BIN="${BIN_DIR}/backend"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

log()  { printf '[%s] %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*" >&2; }
warn() { printf '[%s] WARN: %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*" >&2; }
die()  { printf '[%s] FATAL: %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*" >&2; exit 1; }

# Milliseconds since the epoch. Computed from %N rather than %3N: %3N is not
# honoured everywhere (on the WSL2 box it silently returned full nanoseconds),
# which made every elapsed-time calculation downstream wrong by a factor of a
# million -- CPU% collapsed to 0.0 and would have done the same to the Week 3
# ejection timings. %N is always 9 digits, so this is unambiguous.
now_ms() { echo $(( $(date +%s%N) / 1000000 )); }

# ---------------------------------------------------------------------------
# Tool presence checks. Fail loudly and immediately, never silently degrade.
# ---------------------------------------------------------------------------

# Usage: require_tools tool1 tool2 ...
# Checks every tool up front and reports ALL missing ones in one message,
# instead of dying on the first and forcing a fix-run-fix-run loop.
require_tools() {
  local missing=()
  local t
  for t in "$@"; do
    if ! command -v "$t" >/dev/null 2>&1; then
      missing+=("$t")
    fi
  done
  if [[ ${#missing[@]} -gt 0 ]]; then
    echo "" >&2
    echo "Missing required tool(s): ${missing[*]}" >&2
    echo "" >&2
    echo "Install hints (WSL2 Ubuntu):" >&2
    for t in "${missing[@]}"; do
      case "$t" in
        k6)
          echo "  k6:        https://k6.io/docs/get-started/installation/#debian-ubuntu" >&2
          echo "             sudo gpg -k && sudo gpg --no-default-keyring --keyring /usr/share/keyrings/k6-archive-keyring.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D69" >&2
          echo "             echo 'deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main' | sudo tee /etc/apt/sources.list.d/k6.list" >&2
          echo "             sudo apt-get update && sudo apt-get install k6" >&2
          ;;
        nginx)
          echo "  nginx:     sudo apt-get update && sudo apt-get install nginx-core" >&2
          ;;
        taskset)
          echo "  taskset:   sudo apt-get install util-linux (usually preinstalled)" >&2
          ;;
        python3)
          echo "  python3:   sudo apt-get install python3" >&2
          ;;
        envsubst)
          echo "  envsubst:  sudo apt-get install gettext-base" >&2
          ;;
        go)
          echo "  go:        https://go.dev/doc/install" >&2
          ;;
        curl)
          echo "  curl:      sudo apt-get install curl" >&2
          ;;
        jq)
          echo "  jq:        sudo apt-get install jq" >&2
          ;;
        getconf)
          echo "  getconf:   sudo apt-get install libc-bin (usually preinstalled)" >&2
          ;;
        *)
          echo "  ${t}:      not found on PATH; install it and re-run." >&2
          ;;
      esac
    done
    echo "" >&2
    die "aborting: install the tool(s) above, then re-run."
  fi
}

# Usage: require_file /path/to/thing "what it is / how to produce it"
require_file() {
  local path="$1" hint="$2"
  [[ -e "$path" ]] || die "required file not found: ${path}\n  -> ${hint}"
}

require_built_binaries() {
  require_file "$MANIFOLD_BIN" "build it with: (cd '${REPO_ROOT}' && make build)"
  require_file "$BACKEND_BIN"  "build it with: (cd '${REPO_ROOT}' && make build)"
  [[ -x "$MANIFOLD_BIN" ]] || die "${MANIFOLD_BIN} exists but is not executable (chmod +x it)"
  [[ -x "$BACKEND_BIN" ]]  || die "${BACKEND_BIN} exists but is not executable (chmod +x it)"
}

# ---------------------------------------------------------------------------
# AC power pre-flight check
#
# This machine is a laptop (Lenovo Yoga Slim 7 15ILL9, thin-and-light
# thermal envelope). Sustained benchmark runs under battery power throttle
# CPU frequency and power limits differently -- and less predictably --
# than under AC, so a battery-powered run is not reproducible and should
# never be compared against an AC-powered one. Refuse by default; allow an
# explicit, informed override.
# ---------------------------------------------------------------------------

check_ac_power() {
  local ps_dir="/sys/class/power_supply"

  if [[ ! -d "$ps_dir" ]] || [[ -z "$(ls -A "$ps_dir" 2>/dev/null)" ]]; then
    warn "cannot determine AC/battery power state: ${ps_dir} has no entries visible from inside WSL2 (WSL2 frequently doesn't pass host power/battery info through to the guest at all). VERIFY MANUALLY that the laptop is plugged into AC power before trusting this run's numbers -- proceeding without an automated check."
    return 0
  fi

  local on_battery=0
  local supply type online status
  for supply in "$ps_dir"/*; do
    type="$(cat "$supply/type" 2>/dev/null || echo "")"
    if [[ "$type" == "Mains" || "$type" == "USB" ]]; then
      online="$(cat "$supply/online" 2>/dev/null || echo "1")"
      [[ "$online" == "0" ]] && on_battery=1
    elif [[ "$type" == "Battery" ]]; then
      status="$(cat "$supply/status" 2>/dev/null || echo "")"
      [[ "$status" == "Discharging" ]] && on_battery=1
    fi
  done

  if (( on_battery )); then
    if [[ "${FORCE:-0}" != "1" ]]; then
      die "$(cat <<EOF

Machine appears to be running on BATTERY power, not AC.

Benchmarking a laptop under battery-mode power/thermal throttling produces
numbers that are not reproducible and do not represent the sustained
performance this matrix is trying to characterize -- see bench/README.md's
"Required system tuning" / hardware section.

Plug in AC power and re-run, or, if you specifically intend to
characterize battery-mode behavior and understand the numbers won't be
comparable to an AC run, override explicitly:
  FORCE=1 ./run-matrix.sh
EOF
)"
    else
      warn "running on battery power with FORCE=1 set -- these results are NOT comparable to an AC-powered run. Label them as battery-mode when reporting."
    fi
  else
    log "AC power confirmed."
  fi
}

# ---------------------------------------------------------------------------
# CPU core pinning
#
# Three disjoint process groups are pinned with taskset so none of them can
# steal cycles from, or fight the scheduler with, another: the load
# generator (k6), the load balancer under test (manifold/nginx), and the
# backend pool (3 echo servers). Sharing cores between any two of these
# invalidates the numbers -- e.g. k6 and the LB contending for the same
# core makes the LB look slower than it is, and makes k6 itself an
# uncontrolled bottleneck.
#
# Default split is derived from `nproc` and exported as CORES_K6, CORES_LB,
# CORES_BACKENDS (taskset -c style lists, e.g. "0-1"). Set any of the three
# env vars yourself before calling compute_core_groups to override it.
#
# HONESTY NOTE (WSL2 + hybrid P/E-core CPUs, e.g. this harness's target
# machine, an Intel Core Ultra 7 256V with 4 performance cores + 4
# efficiency cores, no SMT): taskset here pins to WSL2's *virtual* CPUs.
# WSL2 is a Hyper-V VM; the Windows/Hyper-V host scheduler decides which
# physical P-core or E-core backs each virtual CPU at any given moment,
# and that mapping is not exposed to, or controllable from, the guest.
# So "CORES_LB=2-3" reliably keeps the load balancer off the cores k6 and
# the backends are using -- taskset still does that job, and both LB
# targets (manifold, nginx) get an *identical* virtual-core assignment so
# the comparison between them stays fair -- but it does NOT mean "the load
# balancer always runs on a P-core". Which physical core class backs a
# given virtual CPU can change between and even during runs, and that adds
# run-to-run variance this harness cannot pin away. The honest fix is a
# bare-metal Linux host (or two separate machines) if P/E placement needs
# to be controlled precisely; see bench/README.md.
# ---------------------------------------------------------------------------

MIN_CORES_FOR_SPLIT="${MIN_CORES_FOR_SPLIT:-6}"

compute_core_groups() {
  local nproc_n
  nproc_n="$(nproc)"

  if [[ -n "${CORES_K6:-}" && -n "${CORES_LB:-}" && -n "${CORES_BACKENDS:-}" ]]; then
    log "core pinning: using caller-supplied CORES_K6=${CORES_K6} CORES_LB=${CORES_LB} CORES_BACKENDS=${CORES_BACKENDS} (nproc=${nproc_n})"
    export CORES_K6 CORES_LB CORES_BACKENDS
    return 0
  fi

  if (( nproc_n < MIN_CORES_FOR_SPLIT )); then
    die "$(cat <<EOF

nproc reports ${nproc_n} cores; this harness requires at least ${MIN_CORES_FOR_SPLIT}
to give the load generator, the load balancer, and the backend pool
genuinely disjoint cores. Fewer than that and at least two groups would
share a core, which invalidates the comparison (the LB's CPU% and the
observed latency would both be contaminated by scheduler contention with
k6 or the backends).

Options:
  1. Run this on a machine/VM with >= ${MIN_CORES_FOR_SPLIT} cores.
  2. If you understand the tradeoff and want to proceed anyway on fewer
     cores, override the split explicitly (they must still be disjoint):
       export CORES_K6=0
       export CORES_LB=1
       export CORES_BACKENDS=2
       ./run-matrix.sh
  3. Lower MIN_CORES_FOR_SPLIT yourself if you have a specific reason to
     trust a tighter split:
       export MIN_CORES_FOR_SPLIT=4
EOF
)"
  fi

  # Default split: give the backend pool (3 separate processes) the
  # largest share since it's 3-way parallel work, split the remainder
  # evenly between k6 and the LB. Contiguous ranges, in taskset -c syntax.
  #
  # On the target 8-core laptop this yields CORES_K6=0-1, CORES_LB=2-3,
  # CORES_BACKENDS=4-7 -- 3 backend processes sharing 4 cores. At the
  # higher concurrency cells that pool can itself saturate before the LB
  # does, at which point the measurement is characterizing the backend
  # pool's ceiling, not the load balancer. That's an acceptable tradeoff
  # here because every target (manifold, nginx, direct) hits the exact
  # same backend ceiling, so the comparison between them stays fair -- but
  # it means high-concurrency rows should be read as "how much overhead
  # does the LB add on top of a saturated backend pool", not as the LB's
  # own standalone ceiling. See bench/README.md.
  local third=$(( nproc_n / 3 ))
  local k6_count=${third}
  local lb_count=${third}
  local backend_count=$(( nproc_n - k6_count - lb_count ))

  local k6_lo=0
  local k6_hi=$(( k6_lo + k6_count - 1 ))
  local lb_lo=$(( k6_hi + 1 ))
  local lb_hi=$(( lb_lo + lb_count - 1 ))
  local bk_lo=$(( lb_hi + 1 ))
  local bk_hi=$(( nproc_n - 1 ))

  CORES_K6="${k6_lo}-${k6_hi}"
  CORES_LB="${lb_lo}-${lb_hi}"
  CORES_BACKENDS="${bk_lo}-${bk_hi}"
  export CORES_K6 CORES_LB CORES_BACKENDS

  log "core pinning (derived from nproc=${nproc_n}): CORES_K6=${CORES_K6} CORES_LB=${CORES_LB} CORES_BACKENDS=${CORES_BACKENDS}"
}

# Number of cores in a taskset -c range/list like "0-3" or "0,2,4".
core_count_of() {
  local spec="$1"
  local total=0
  local part
  IFS=',' read -ra parts <<< "$spec"
  for part in "${parts[@]}"; do
    if [[ "$part" == *-* ]]; then
      local lo="${part%-*}" hi="${part#*-}"
      total=$(( total + (hi - lo + 1) ))
    else
      total=$(( total + 1 ))
    fi
  done
  echo "$total"
}

# ---------------------------------------------------------------------------
# Process lifecycle
# ---------------------------------------------------------------------------

# Usage: pinned_start <cores> <logfile> <cmd...>
# Launches cmd under taskset -c <cores>, backgrounded, stdout+stderr to
# logfile. Echoes the PID to stdout so the caller can capture it.
pinned_start() {
  local cores="$1" logfile="$2"
  shift 2
  taskset -c "$cores" "$@" >"$logfile" 2>&1 &
  local pid=$!
  echo "$pid"
}

# Usage: stop_pid <pid> <name> [timeout_seconds]
# Graceful SIGTERM, escalate to SIGKILL if it hasn't exited in time.
stop_pid() {
  local pid="$1" name="$2" timeout="${3:-10}"
  [[ -z "$pid" ]] && return 0
  if ! kill -0 "$pid" 2>/dev/null; then
    return 0
  fi
  log "stopping ${name} (pid ${pid})"
  kill -TERM "$pid" 2>/dev/null || true
  local waited=0
  while kill -0 "$pid" 2>/dev/null; do
    sleep 0.5
    waited=$(( waited + 1 ))
    if (( waited * 5 >= timeout * 10 )); then
      warn "${name} (pid ${pid}) did not exit within ${timeout}s, sending SIGKILL"
      kill -KILL "$pid" 2>/dev/null || true
      break
    fi
  done
  wait "$pid" 2>/dev/null || true
}

# Usage: wait_for_tcp <host> <port> <timeout_seconds>
# Probe from a separate bash process, not a subshell of this one. A subshell
# inherits `set -e`, and a refused /dev/tcp connect there could take the whole
# run down on the very first probe -- before the retry loop or the die message
# ran. That is why a service failing to start appeared as a silent exit 1 with
# no output whatsoever.
wait_for_tcp() {
  local host="$1" port="$2" timeout="${3:-15}"
  local deadline=$(( SECONDS + timeout ))
  while (( SECONDS < deadline )); do
    if bash -c "exec 3<>/dev/tcp/${host}/${port}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

# Usage: lb_failure_report <name> <pid> <logfile>...
# Say why a load balancer never came up, rather than dying with a bare path and
# leaving the operator to go hunting.
lb_failure_report() {
  local name="$1" pid="$2"; shift 2
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    warn "${name} (pid ${pid}) is alive but never accepted connections."
  else
    warn "${name} exited before it could accept connections."
  fi
  local f
  for f in "$@"; do
    if [[ -s "$f" ]]; then
      warn "--- tail of ${f} ---"
      tail -n 15 "$f" >&2 || true
    fi
  done
}

# Usage: wait_for_http_ok <url> <timeout_seconds>
wait_for_http_ok() {
  local url="$1" timeout="${2:-15}"
  local waited=0
  while true; do
    if curl -fsS -o /dev/null -m 2 "$url" 2>/dev/null; then
      return 0
    fi
    sleep 0.2
    waited=$(( waited + 1 ))
    if (( waited * 2 >= timeout * 10 )); then
      return 1
    fi
  done
}

# ---------------------------------------------------------------------------
# Backend pool (bench/backend) management
#
# Convention (this harness's choice, not dictated by bench/backend itself):
# backend N listens for traffic on 127.0.0.1:900N and exposes its admin/
# control API on 127.0.0.1:BACKEND_ADMIN_OFFSET+900N. Matches the 3
# upstreams hardcoded in config.example.yaml and bench/nginx/nginx.conf
# (127.0.0.1:9001-9003). Override BACKEND_ADMIN_OFFSET if bench/backend's
# actual default admin port differs from this assumption.
# ---------------------------------------------------------------------------

BACKEND_PORTS=(9001 9002 9003)
BACKEND_ADMIN_OFFSET="${BACKEND_ADMIN_OFFSET:-100}"

backend_admin_port() {
  local data_port="$1"
  echo $(( data_port + BACKEND_ADMIN_OFFSET ))
}

# Usage: start_backends <latency> <results_dir>
# Starts all 3 backends pinned to CORES_BACKENDS, waits for each to answer
# its health path. Populates the global arrays BACKEND_PIDS and
# BACKEND_ADMIN_PORTS. Dies if any backend fails to come up.
declare -a BACKEND_PIDS=()
declare -a BACKEND_ADMIN_PORTS=()

start_backends() {
  local latency="$1" logdir="$2"
  BACKEND_PIDS=()
  BACKEND_ADMIN_PORTS=()
  mkdir -p "$logdir"

  local port admin_port id pid
  for port in "${BACKEND_PORTS[@]}"; do
    admin_port="$(backend_admin_port "$port")"
    id="backend-${port}"
    pid="$(pinned_start "$CORES_BACKENDS" "${logdir}/${id}.log" \
      "$BACKEND_BIN" \
      -addr "127.0.0.1:${port}" \
      -admin "127.0.0.1:${admin_port}" \
      -id "$id" \
      -latency "$latency" \
      -jitter "0ms" \
      -error-rate "0" \
      -health-path "/healthz")"
    BACKEND_PIDS+=("$pid")
    BACKEND_ADMIN_PORTS+=("$admin_port")
  done

  for port in "${BACKEND_PORTS[@]}"; do
    if ! wait_for_http_ok "http://127.0.0.1:${port}/healthz" 15; then
      die "backend on port ${port} did not become healthy within 15s (see ${logdir}/backend-${port}.log)"
    fi
  done
  log "3 backends up on ports ${BACKEND_PORTS[*]} (latency=${latency}, cores=${CORES_BACKENDS})"
}

stop_backends() {
  local i
  for i in "${!BACKEND_PIDS[@]}"; do
    stop_pid "${BACKEND_PIDS[$i]}" "backend-${BACKEND_PORTS[$i]}" 5
  done
  BACKEND_PIDS=()
}

# Usage: backend_control <backend_index 0-2> <path+query, e.g. "health?state=down">
backend_control() {
  local idx="$1" pathq="$2"
  local admin_port="${BACKEND_ADMIN_PORTS[$idx]}"
  [[ -z "$admin_port" ]] && die "backend_control: no admin port recorded for index ${idx} (backends not started?)"
  curl -fsS -m 5 "http://127.0.0.1:${admin_port}/_control/${pathq}"
}

# ---------------------------------------------------------------------------
# manifold lifecycle
# ---------------------------------------------------------------------------

# Usage: render_manifold_config <strategy> <out_path>
# Copies config.example.yaml, swapping the pool's strategy (and, for
# consistent_hash, filling in hash_on since the schema requires it). Writes
# a derived file under the results dir -- never edits config.example.yaml.
render_manifold_config() {
  local strategy="$1" out_path="$2"
  require_file "${REPO_ROOT}/config.example.yaml" "should ship with the repo"

  python3 - "$strategy" "$out_path" <<'PYEOF'
import re
import sys

strategy, out_path = sys.argv[1], sys.argv[2]
with open("config.example.yaml", "r", encoding="utf-8") as f:
    text = f.read()

text = re.sub(r'strategy:\s*\S+', f'strategy: {strategy}', text, count=1)

if strategy == "consistent_hash" and "hash_on:" not in text:
    text = text.replace(
        f"strategy: {strategy}",
        f"strategy: {strategy}\n    hash_on: client_ip",
        1,
    )

with open(out_path, "w", encoding="utf-8") as f:
    f.write(text)
PYEOF
}

# Usage: start_manifold <strategy> <results_dir>
MANIFOLD_PID=""

start_manifold() {
  local strategy="$1" resultsdir="$2"
  local cfg="${resultsdir}/manifold-${strategy}.yaml"
  render_manifold_config "$strategy" "$cfg"

  MANIFOLD_PID="$(pinned_start "$CORES_LB" "${resultsdir}/manifold.log" \
    "$MANIFOLD_BIN" -config "$cfg")"

  if ! wait_for_http_ok "http://127.0.0.1:9090/healthz" 15; then
    lb_failure_report "manifold" "$MANIFOLD_PID" "${resultsdir}/manifold.log"
    die "manifold (strategy=${strategy}) did not become healthy on :9090/healthz within 15s (see ${resultsdir}/manifold.log)"
  fi
  if ! wait_for_tcp 127.0.0.1 8080 10; then
    lb_failure_report "manifold" "$MANIFOLD_PID" "${resultsdir}/manifold.log"
    die "manifold data listener :8080 did not come up (see ${resultsdir}/manifold.log)"
  fi
  log "manifold up (strategy=${strategy}, pid=${MANIFOLD_PID}, cores=${CORES_LB})"
}

stop_manifold() {
  stop_pid "$MANIFOLD_PID" "manifold" 20
  MANIFOLD_PID=""
}

# ---------------------------------------------------------------------------
# nginx lifecycle
# ---------------------------------------------------------------------------

NGINX_PID=""

# Usage: render_nginx_config <out_conf_path> <out_pid_path> <out_error_log_path> <out_access_log_path>
render_nginx_config() {
  local out_conf="$1" pidfile="$2" errlog="$3" acclog="$4"
  require_file "$NGINX_CONF_TEMPLATE" "should ship with the repo at bench/nginx/nginx.conf"
  require_tools envsubst

  local worker_processes worker_connections somaxconn
  worker_processes="$(core_count_of "$CORES_LB")"
  # worker_connections must cover the highest concurrency cell (5000) with
  # headroom per worker, since it's a per-worker cap on simultaneous
  # connections (both client-facing and upstream-facing share the pool).
  worker_connections=4096
  somaxconn="$(cat /proc/sys/net/core/somaxconn 2>/dev/null || echo 1024)"

  WORKER_PROCESSES="$worker_processes" \
  WORKER_CONNECTIONS="$worker_connections" \
  SOMAXCONN="$somaxconn" \
  NGINX_PID_FILE="$pidfile" \
  NGINX_ERROR_LOG="$errlog" \
  NGINX_ACCESS_LOG="$acclog" \
    envsubst '${WORKER_PROCESSES} ${WORKER_CONNECTIONS} ${SOMAXCONN} ${NGINX_PID_FILE} ${NGINX_ERROR_LOG} ${NGINX_ACCESS_LOG}' \
    < "$NGINX_CONF_TEMPLATE" > "$out_conf"
}

start_nginx() {
  local resultsdir="$1"
  local conf="${resultsdir}/nginx.conf"
  local pidfile="${resultsdir}/nginx.pid"
  local errlog="${resultsdir}/nginx-error.log"
  local acclog="/dev/null"

  render_nginx_config "$conf" "$pidfile" "$errlog" "$acclog"

  if ! nginx -t -c "$conf" >"${resultsdir}/nginx-configtest.log" 2>&1; then
    die "nginx -t failed for ${conf}, see ${resultsdir}/nginx-configtest.log"
  fi

  # nginx daemonizes and forks workers itself; pin the master (workers
  # inherit its affinity mask unless nginx's own `worker_cpu_affinity` is
  # set, which we deliberately leave unset so worker_processes governs
  # parallelism the same way manifold's GOMAXPROCS-driven scheduler does).
  # Only "daemon off" belongs in -g. The rendered config already carries a
  # `pid` directive, and nginx refuses a duplicate with
  #   [emerg] "pid" directive is duplicate
  # which `nginx -t` does NOT catch, because -t runs without these -g
  # globals. Config test clean, startup always broken.
  taskset -c "$CORES_LB" nginx -c "$conf" -g "daemon off;" \
    >"${resultsdir}/nginx.log" 2>&1 &
  NGINX_PID=$!

  if ! wait_for_tcp 127.0.0.1 8080 15; then
    lb_failure_report "nginx" "$NGINX_PID" "${resultsdir}/nginx.log" "${resultsdir}/nginx-error.log"
    die "nginx did not come up on :8080 within 15s"
  fi
  log "nginx up (pid=${NGINX_PID}, workers=$(core_count_of "$CORES_LB"), cores=${CORES_LB})"
}

stop_nginx() {
  stop_pid "$NGINX_PID" "nginx" 10
  NGINX_PID=""
}

# ---------------------------------------------------------------------------
# CPU% / RSS sampling of the LB process during a measured run
# ---------------------------------------------------------------------------

CLK_TCK="$(getconf CLK_TCK 2>/dev/null || echo 100)"

# Usage: start_cpu_sampler <pid> <outfile> [interval_seconds]
# Background loop appending "epoch_ms utime stime rss_kb" lines. Echoes the
# sampler's own PID so it can be stopped with stop_cpu_sampler.
#
# The background subshell MUST have its stdout redirected. Callers invoke this
# as sampler_pid="$(start_cpu_sampler ...)", and a command substitution does not
# return when the function returns -- it returns when every writer to the pipe
# closes it. A backgrounded subshell inherits stdout, so without the redirect it
# holds that pipe open for its entire life and "$(...)" blocks forever, hanging
# the whole run after the warmup pass. It only bites targets that have an LB
# process to sample, which is why `direct` cells completed and `manifold` cells
# did not.
# Usage: proc_tree_cpu_rss <root_pid>  ->  "utime stime rss_kb"
# Sums cumulative CPU jiffies and resident memory across a process and every
# descendant.
#
# This exists because nginx does all request handling in worker processes: its
# master accumulates no utime/stime at all, so sampling the master alone
# reported 0% CPU no matter the load, and would have flattered manifold in the
# headline comparison. manifold is a single process whose threads already roll
# up into its own stat, so it reads the same either way.
#
# RSS is summed across the tree, which double-counts memory shared between
# nginx workers. Stated in bench/README.md rather than silently corrected.
proc_tree_cpu_rss() {
  local root="$1"
  local -a queue=("$root")
  local -a seen=()
  local idx=0 cur kid
  while (( idx < ${#queue[@]} )); do
    cur="${queue[idx]}"
    idx=$(( idx + 1 ))
    [[ -d "/proc/${cur}" ]] || continue
    seen+=("$cur")
    for kid in $(cat /proc/"${cur}"/task/*/children 2>/dev/null); do
      queue+=("$kid")
    done
  done
  local u=0 s=0 r=0 cu cs cr
  for cur in "${seen[@]}"; do
    # Split after the "comm" field, which is parenthesised and may contain
    # spaces; utime/stime are the 12th and 13th fields after it.
    read -r cu cs < <(awk '{ n=index($0,") "); split(substr($0,n+2),a," "); print a[12], a[13] }' "/proc/${cur}/stat" 2>/dev/null)
    cr="$(awk '/^VmRSS:/{print $2}' "/proc/${cur}/status" 2>/dev/null)"
    [[ -n "$cu" ]] && u=$(( u + cu ))
    [[ -n "$cs" ]] && s=$(( s + cs ))
    [[ -n "$cr" ]] && r=$(( r + cr ))
  done
  printf '%s %s %s\n' "$u" "$s" "$r"
}

start_cpu_sampler() {
  local pid="$1" outfile="$2" interval="${3:-0.5}"
  : > "$outfile"
  (
    while kill -0 "$pid" 2>/dev/null; do
      local now agg
      now="$(now_ms)"
      agg="$(proc_tree_cpu_rss "$pid")"
      if [[ -n "$agg" ]]; then
        echo "${now} ${agg}" >> "$outfile"
      fi
      sleep "$interval"
    done
  ) >/dev/null 2>&1 &
  echo $!
}

stop_cpu_sampler() {
  local sampler_pid="$1"
  [[ -z "$sampler_pid" ]] && return 0
  kill "$sampler_pid" 2>/dev/null || true
  wait "$sampler_pid" 2>/dev/null || true
}

# Usage: summarize_cpu_rss <samples_file>
# Prints "cpu_pct_avg rss_kb_avg rss_kb_max" (space separated). cpu_pct_avg
# is computed from the first and last sample's cumulative jiffies over
# wall-clock elapsed time, normalized to a single core (100% = one core
# saturated), matching how top/ps report %CPU for a process.
summarize_cpu_rss() {
  local samples_file="$1"
  local clk_tck="$CLK_TCK"
  if [[ ! -s "$samples_file" ]]; then
    echo "null null null"
    return 0
  fi
  awk -v clk_tck="$clk_tck" '
    NR==1 { t0=$1; u0=$2; s0=$3 }
    { rss_sum += $4; if ($4 > rss_max) rss_max = $4; n++; t1=$1; u1=$2; s1=$3 }
    END {
      if (n == 0) { print "null null null"; exit }
      dt_s = (t1 - t0) / 1000.0
      djiffies = (u1 - u0) + (s1 - s0)
      cpu_pct = "null"
      if (dt_s > 0) {
        cpu_pct = (djiffies / clk_tck) / dt_s * 100.0
      }
      printf "%s %.0f %.0f\n", cpu_pct, rss_sum/n, rss_max
    }
  ' "$samples_file"
}

# ---------------------------------------------------------------------------
# k6 invocation
# ---------------------------------------------------------------------------

# Usage: run_k6 <target_url> <scenario> <vus> <rate> <duration> <path> <out_json>
# Never lets a k6 threshold breach (nonzero exit) kill the harness -- that's
# expected under chaos scenarios. Returns k6's exit code via K6_EXIT_CODE.
K6_EXIT_CODE=0

run_k6() {
  local target_url="$1" scenario="$2" vus="$3" rate="$4" duration="$5" path="$6" out_json="$7"
  local extra_env=("${@:8}")

  set +e
  taskset -c "$CORES_K6" k6 run \
    -e TARGET_URL="$target_url" \
    -e SCENARIO="$scenario" \
    -e VUS="$vus" \
    -e RATE="$rate" \
    -e DURATION="$duration" \
    -e PATH_URL="$path" \
    -e OUT_FILE="$out_json" \
    "${extra_env[@]}" \
    "${BENCH_DIR}/scripts/load.js"
  K6_EXIT_CODE=$?
  set -e

  if [[ $K6_EXIT_CODE -ne 0 ]]; then
    warn "k6 exited with code ${K6_EXIT_CODE} (likely a threshold breach under chaos -- check ${out_json} for the actual measurements before treating this as a hard failure)"
  fi
  [[ -s "$out_json" ]] || die "k6 did not write ${out_json} -- check its stderr output above"
}

# ---------------------------------------------------------------------------
# Fine-grained request probe (used by run-failure-scenarios.sh)
#
# k6's own output only gives aggregates over a whole run; the failure
# scenarios need a timestamped pass/fail/latency sequence to locate the
# moment ejection, readmission, or breaker-open actually happened. This is
# a lightweight, low-rate curl loop independent of whatever bulk load k6
# is generating in parallel.
# ---------------------------------------------------------------------------

# Usage: start_probe_loop <url> <outfile> [interval_seconds]
# Appends "epoch_ms http_status time_total_s" per line (http_status is 000
# on connect failure/timeout). Echoes the loop's PID.
start_probe_loop() {
  local url="$1" outfile="$2" interval="${3:-0.1}"
  : > "$outfile"
  (
    while true; do
      local now code ttime
      now="$(now_ms)"
      read -r code ttime < <(curl -s -o /dev/null -m 3 \
        -w '%{http_code} %{time_total}' "$url" 2>/dev/null || echo "000 0")
      echo "${now} ${code} ${ttime}" >> "$outfile"
      sleep "$interval"
    done
  ) >/dev/null 2>&1 &
  echo $!
}

stop_probe_loop() {
  local pid="$1"
  [[ -z "$pid" ]] && return 0
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

# Usage: time_to_first_status_change <probe_file> <baseline_ok_regex> <after_epoch_ms>
# Scans probe_file for the first line after after_epoch_ms whose http_status
# does NOT match baseline_ok_regex (e.g. '^2'), and prints the elapsed
# seconds from after_epoch_ms to that line's timestamp. Prints "null" if no
# such line exists.
time_to_first_status_change() {
  local probe_file="$1" ok_regex="$2" after_epoch_ms="$3"
  awk -v ok_re="$ok_regex" -v t0="$after_epoch_ms" '
    $1 >= t0 && $2 !~ ok_re { printf "%.3f\n", ($1 - t0) / 1000.0; found=1; exit }
    END { if (!found) print "null" }
  ' "$probe_file"
}

# Usage: time_to_sustained_ok <probe_file> <ok_regex> <after_epoch_ms> <sustain_count>
# First point in time (elapsed seconds from after_epoch_ms) at which
# `sustain_count` consecutive probes after after_epoch_ms all matched
# ok_regex. Prints "null" if that never happens in the file.
time_to_sustained_ok() {
  local probe_file="$1" ok_regex="$2" after_epoch_ms="$3" sustain="$4"
  awk -v ok_re="$ok_regex" -v t0="$after_epoch_ms" -v need="$sustain" '
    $1 >= t0 {
      if ($2 ~ ok_re) { run++; if (run == 1) first_ts=$1 } else { run=0 }
      if (run >= need) { printf "%.3f\n", (first_ts - t0) / 1000.0; found=1; exit }
    }
    END { if (!found) print "null" }
  ' "$probe_file"
}

# Usage: scrape_manifold_metrics
# Best-effort text dump of manifold's /metrics on its admin port. Failure
# scenarios grep this for breaker/ejection signal as a primary source when
# available, falling back to the probe-based heuristics above when a metric
# name assumption doesn't match manifold's actual exposition (internal/observe
# was still being developed alongside this harness, so exact metric names
# were not available to hardcode against).
scrape_manifold_metrics() {
  curl -fsS -m 3 "http://127.0.0.1:9090/metrics" 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# Cleanup safety net
# ---------------------------------------------------------------------------

cleanup_all() {
  stop_manifold
  stop_nginx
  stop_backends
}
