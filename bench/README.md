# manifold benchmark harness

Methodology, reproduction steps, and system-tuning requirements for the
manifold-vs-nginx-vs-direct benchmark matrix and the failure-scenario
suite. Read this before running anything under `bench/scripts/` or trusting
any number that comes out of it.

## Contents

- `bench/nginx/nginx.conf` -- the nginx baseline, templated (see comments
  at the top of the file for why and how it's rendered).
- `bench/backend` -- the Go echo backend used as the upstream in every run
  (owned/built by the main Go module, not this harness).
- `bench/scripts/lib.sh` -- shared bash library (tool checks, core
  pinning, process lifecycle, CPU/RSS sampling). Sourced, not run
  directly.
- `bench/scripts/run-matrix.sh` -- the full benchmark matrix.
- `bench/scripts/run-failure-scenarios.sh` -- the 5 resilience
  experiments.
- `bench/scripts/load.js` -- the k6 load generator both of the above
  drive.
- `bench/scripts/parse-results.py` -- turns a results directory into a
  markdown table with medians and a manifold-vs-nginx delta column.
- `bench/results/` -- output directory. Each run of `run-matrix.sh`
  creates `bench/results/<UTC timestamp>/`; each run of
  `run-failure-scenarios.sh` creates
  `bench/results/<UTC timestamp>-failure-scenarios/`.

## Target hardware

This harness is written and tuned against one specific known machine:

- **Lenovo Yoga Slim 7 15ILL9**, Intel Core Ultra 7 256V (Lunar Lake).
- **8 cores / 8 threads, no SMT** -- 4 performance cores (Lion Cove) + 4
  efficiency cores (Skymont), 2.2 GHz base.
- 16 GB RAM.
- Thin-and-light chassis, ~17-30 W sustained power envelope.
- Running the harness **under WSL2**, not bare-metal Linux.

Every default in this document and in `bench/scripts/lib.sh` /
`run-matrix.sh` (core split, `MIN_CORES_FOR_SPLIT`, the c=5000 opt-in, the
cooldown/drift-check defaults) is calibrated to this machine specifically.
Running on different hardware is fine, but re-derive the defaults rather
than assuming they still make sense -- a 32-core desktop doesn't need to
demote c=5000, and a machine with more thermal headroom may not need
`COOLDOWN_SECS=20`.

Two properties of this specific machine drive most of the caveats below:
it's a **thin-and-light laptop** (thermal throttling under sustained load
is a real risk, not a hypothetical), and it has a **hybrid P-core/E-core
CPU running under a hypervisor** (WSL2 cannot control which physical core
class backs which pinned virtual CPU). Both are addressed in detail in
"Core-pinning scheme" and "Thermal stability" below.

## What this benchmark does NOT measure

Be explicit about this whenever these numbers are quoted anywhere else:

- **No TLS.** Everything runs plain HTTP/1.1 over loopback. TLS handshake
  cost, cipher negotiation, and certificate handling are not exercised on
  either manifold or nginx.
- **HTTP/1.1 only.** No HTTP/2, no HTTP/3/QUIC, no gRPC. Connection
  multiplexing behavior specific to those protocols is out of scope.
- **Single host, loopback networking.** Load generator, load balancer, and
  backends all run on one machine talking over `127.0.0.1`. There is no
  real network: no NIC interrupts, no cross-host latency/jitter, no packet
  loss, no MTU effects. Results characterize CPU-bound proxying overhead
  and in-process scheduling, not a real deployment's network path.
- **Synthetic echo backends**, not a real application. Backend response
  bodies are trivial; there is no downstream database, no variable
  payload size sweep, no compression benefit/cost beyond what
  `transport.disable_compression` already turns off.
- **No multi-tenant / noisy-neighbor scenario.** The machine is assumed
  otherwise idle (see core pinning below) -- this is a controlled
  best-case comparison, not a measurement of behavior under host
  contention.
- **No sustained soak / memory-leak detection.** Runs are tens of seconds
  per cell, not hours. This harness cannot tell you about slow leaks or
  degradation over days of uptime.

## Hardware and kernel capture

Every `run-matrix.sh` invocation writes `meta.json` to its results
directory automatically (hostname, kernel, `nproc`, core assignment, Go
version, nginx version, k6 version, git commit, and whether the working
tree was dirty at run time). `parse-results.py` folds the relevant fields
into the header of `table.md`.

To capture the same information by hand (e.g. when reporting an anomaly,
or documenting the host outside of a matrix run):

```bash
uname -a
nproc
lscpu
free -h
cat /proc/cpuinfo | grep "model name" | head -1
go version
nginx -V
k6 version
git -C "$(git rev-parse --show-toplevel)" rev-parse HEAD
```

On WSL2, also capture the Windows host's CPU/RAM and whether other
Windows processes were active during the run -- WSL2's own CPU/memory
limits are governed by `.wslconfig` on the Windows side (see WSL2
caveats below), and the underlying hardware is shared with the host OS in
a way a native Linux box's `meta.json` fields don't fully capture.

## Core-pinning scheme

Three process groups run simultaneously during a measured cell: the load
generator (k6), the load balancer under test (manifold or nginx), and the
3-process backend pool. They are pinned to **disjoint** CPU core sets with
`taskset` so none of them can steal cycles from, or fight the kernel
scheduler with, another. Sharing a core between any two groups invalidates
the numbers -- for example, k6 and the LB contending for the same core
makes the LB look slower than it actually is, while simultaneously making
k6 itself an uncontrolled bottleneck instead of a clean load source.

`bench/scripts/lib.sh:compute_core_groups()` derives a default split from
`nproc`:

- The backend pool (3 separate OS processes, genuinely parallel work) gets
  the largest contiguous range.
- The load generator and the load balancer under test split the remainder
  evenly.
- Ranges are contiguous and non-overlapping. **On the target 8-core
  laptop this yields `CORES_K6=0-1`, `CORES_LB=2-3`, `CORES_BACKENDS=4-7`
  -- this exact split is kept as the default rather than recalculated,
  since it's the one this harness's other defaults (cooldown, drift
  threshold) were chosen against.** On a different core count the same
  derivation applies, e.g. a 12-core box gets `CORES_K6=0-3`,
  `CORES_LB=4-7`, `CORES_BACKENDS=8-11`.

The harness **refuses to run** on fewer than 6 cores (`MIN_CORES_FOR_SPLIT`,
overridable) and explains why in the error message, rather than silently
degrading to a co-located, contaminated split.

Override the split explicitly with environment variables (must remain
disjoint -- the harness does not check this for you when you override):

```bash
export CORES_K6=0-1
export CORES_LB=2-3
export CORES_BACKENDS=4-7
./bench/scripts/run-matrix.sh
```

nginx's `worker_processes` is set to the pinned LB core count for each
run (workers inherit the master's affinity mask; `worker_cpu_affinity` is
deliberately left unset in `bench/nginx/nginx.conf` so the whole pinned
range is available exactly the way manifold's GOMAXPROCS-driven scheduler
uses its pinned range).

### What this pinning does NOT deliver: P-core / E-core placement

**Be honest about this whenever the core-pinning scheme is described
elsewhere: `taskset` under WSL2 does not, and cannot, pin work to physical
performance cores vs. efficiency cores.**

WSL2 is a Hyper-V virtual machine. `taskset -c 2-3` pins a process to
WSL2's *virtual* CPUs 2 and 3 -- it has no visibility into, and no control
over, which physical P-core (Lion Cove) or E-core (Skymont) the Windows/
Hyper-V host scheduler backs those virtual CPUs with at any given moment.
That mapping is not exposed to the guest, is not guaranteed to be stable,
and can change between runs or even mid-run as the host scheduler
rebalances.

What the pinning **does** still deliver, and why it's kept rather than
dropped:

- It keeps k6, the load balancer, and the backend pool off each other's
  virtual cores, which is the property that matters most: none of the
  three process groups can starve or contend with another for scheduler
  time.
- Both LB targets (manifold and nginx) receive the **identical** virtual-
  core assignment (`CORES_LB`) for every run, so whatever P/E variance
  exists, it applies symmetrically to both sides of the manifold-vs-nginx
  comparison rather than favoring one.

What it does **not** deliver is precision: run-to-run variance on this
harness includes an irreducible component from not knowing whether a
given measurement's virtual CPUs happened to land on P-cores, E-cores, or
a mix, and that component is larger here than it would be on bare metal.
The min/median/max RPS reporting in `table.md` (see "Thermal stability"
below) is partly there to make this variance visible rather than papering
over it with a single median.

**The honest fix, if P/E placement needs to be controlled precisely, is a
bare-metal Linux host** (where `taskset` pins to real physical cores and
`lscpu -e` can identify which are P vs. E), **or two separate physical
machines** (one running manifold, one running nginx, eliminating shared-
hardware confounds entirely). Neither is what this harness does; don't
claim otherwise when presenting these numbers.

### Backend pool as a secondary bottleneck at high concurrency

3 backend processes share 4 pinned cores (`CORES_BACKENDS=4-7` on the
target laptop). At low-to-moderate concurrency this is not a constraint,
but at the higher end of the matrix the backend pool can itself saturate
before the load balancer does -- at which point the measurement is
characterizing the backend pool's ceiling as much as, or more than, the
load balancer's.

This is an acceptable tradeoff, not an oversight: **every target
(manifold, nginx, direct) hits the exact same backend ceiling**, so the
comparison between targets stays fair. But it does mean the
high-concurrency rows in `table.md` should be read as "how much overhead
does the LB add on top of an already-saturated backend pool", not as the
load balancer's own standalone ceiling in isolation. See "c=5000 is
opt-in and harness-limited" below for the most extreme case of this.

## Tool versions

Recorded per run in `meta.json`. As a starting baseline for this harness's
development:

| Tool  | Constraint |
|---|---|
| Go | matches `go.mod` (`go 1.27.0`) |
| k6 | any recent release supporting `constant-vus` / `constant-arrival-rate` executors and `summaryTrendStats` (v0.33+) |
| nginx | any release with OSS `keepalive`/`proxy_next_upstream_tries`/`hash ... consistent` support (1.15.1+); `nginx -V` output is captured in `meta.json` |

Pin exact versions in your own notes when publishing numbers -- a nginx
minor version bump has, historically, changed default buffer sizes and
keepalive behavior enough to move results.

## Thermal stability

The target machine is a thin-and-light laptop under a sustained,
multi-hour benchmark load. It **will** throttle at some point in a full
matrix run; the question is only when, by how much, and whether it
contaminates the manifold-vs-nginx comparison specifically. This harness
treats that as a first-class concern rather than an assumption to ignore.

**Targets are interleaved, not batched.** `run-matrix.sh` does NOT run all
manifold cells, then all nginx cells, then all direct cells. For a fixed
(concurrency, latency, strategy, run), the applicable targets run back to
back in randomized order (target is the innermost loop). Batching by
target would systematically measure whichever target ran later at a
hotter chassis state than whichever ran first -- confounding the exact
comparison the whole project is about. See the long comment at the top of
`run-matrix.sh` for the full reasoning, including why `nginx` and `direct`
are only included in a subset of strategy passes (they have no real
strategy dimension of their own) rather than tripling redundant
measurements.

**A cooldown separates every individual measurement**, not just every
cell: `COOLDOWN_SECS` (default 20s, env-overridable) is slept after every
single k6 run, once its backends and LB have been stopped. Raise it if the
drift check below keeps failing; 20s is a starting point for this
chassis, not a measured recovery time.

```bash
COOLDOWN_SECS=45 ./bench/scripts/run-matrix.sh
```

**The first cell is re-measured at the very end of the matrix and
compared.** Whatever (target, strategy, concurrency, latency) combination
is measured first gets a full fresh 3-run re-measurement after the entire
rest of the matrix has run, written to `drift-check.json` in the results
directory. If its median RPS differs from the original by more than
`DRIFT_THRESHOLD_PCT` (default 10, env-overridable), the run is flagged:

- A loud, hard-to-miss warning is printed to the terminal.
- `meta.json` gets `"thermally_compromised": true` plus a `"drift_check"`
  object with the original and recheck medians and the observed drift
  percentage.
- `parse-results.py` propagates this into a **banner at the very top of
  `table.md`** -- not a footnote. A results directory that drifted says so
  before you get anywhere near the numbers.

```bash
DRIFT_THRESHOLD_PCT=15 ./bench/scripts/run-matrix.sh   # more tolerant
```

**`table.md` reports min / median / max RPS per cell, not median alone.**
Spread across a cell's 3 runs is itself evidence for or against thermal
stability at that specific point in the matrix -- a cell whose 3 runs
range from 900 to 1100 rps tells a different story than one that's
1000/1002/998, even if both medians round to the same headline number.
Hiding that spread would hide the exact thing this section exists to
surface. (Other metrics -- latency percentiles, error rate, CPU/RSS --
remain median-only, to keep the table from becoming unreadable; RPS is
both the primary throughput indicator and the one the drift check itself
tracks.)

If a run comes back `thermally_compromised: true`, the fix is not to
delete the drift-check.json and move on -- it's to increase
`COOLDOWN_SECS`, reduce `MEASURE_DURATION`/scope the matrix down (fewer
concurrency/latency/strategy values per invocation), ensure nothing else
on the machine is contending for thermal headroom, and re-run.

## c=5000 is opt-in and harness-limited

With k6, the load balancer, and 3 backends all sharing one 8-core laptop,
concurrency=5000 means roughly 15,000 sockets open simultaneously on a box
where the load generator itself has exactly 2 pinned cores. At that point
the measurement characterizes **the harness's own ceiling** -- how much
load 2 cores of k6 can actually generate and how the backend pool copes
with the socket volume -- not manifold's or nginx's balancing overhead.

Because of that, **c=5000 is excluded from the default matrix** and only
runs when explicitly requested:

```bash
INCLUDE_C5000=1 ./bench/scripts/run-matrix.sh
```

When included, every c=5000 row in `table.md` is marked in its `Notes`
column as `harness-limited (c=5000, see README)` so it can't be
mistaken for a normal data point sitting alongside c=50/200/1000.

**The credible concurrency range on this hardware is c=50, 200, and
1000** -- that's the default matrix. Treat c=5000 as a separate,
clearly-labeled data point about the harness's own limits, not as part of
the headline manifold-vs-nginx comparison.

## Battery / AC power pre-flight check

`run-matrix.sh` (and `run-failure-scenarios.sh`) check
`/sys/class/power_supply/*/{online,status}` before doing anything else. If
the machine looks like it's running on battery, the script **refuses to
proceed** with an explanation, since a benchmark run under battery-mode
power/thermal throttling is not reproducible and would silently
contaminate every number in the run.

```bash
# refuses to run if on battery:
./bench/scripts/run-matrix.sh

# explicit, informed override (results will not be comparable to an
# AC-powered run -- label them as such if you use this):
FORCE=1 ./bench/scripts/run-matrix.sh
```

Note the honesty limit here too: WSL2 frequently does not expose host
battery/AC state to the guest at all, in which case
`/sys/class/power_supply/` has no entries and the check can only warn
that it couldn't determine the power state, not block. If you see that
warning, verify manually that the laptop is plugged in before trusting
the run.

## Reproducing end to end

All of this runs **inside WSL2 Ubuntu**, not on the Windows host directly
(see WSL2 caveats below).

1. Install prerequisites (each script also checks for these itself and
   fails with an install hint if one is missing):

   ```bash
   sudo apt-get update
   sudo apt-get install -y nginx-core util-linux gettext-base curl python3 golang-go
   # k6 (Ubuntu apt repo):
   sudo gpg -k
   sudo gpg --no-default-keyring --keyring /usr/share/keyrings/k6-archive-keyring.gpg \
     --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D69
   echo 'deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main' \
     | sudo tee /etc/apt/sources.list.d/k6.list
   sudo apt-get update && sudo apt-get install -y k6
   ```

2. Build manifold and the backend from the repo root:

   ```bash
   make build   # produces bin/manifold and bin/backend
   ```

3. Apply the system tuning below (only strictly required if you opt into
   `INCLUDE_C5000=1`; harmless, and a good idea anyway, at the default
   concurrency range).

4. Make the scripts executable (Windows filesystems do not reliably
   preserve the executable bit -- if you edited these files from the
   Windows side, re-run this inside WSL2 before executing them):

   ```bash
   chmod +x bench/scripts/*.sh bench/scripts/*.py bench/scripts/load.js
   ```

5. **Plug in AC power.** The matrix now runs 120+ individual measurements
   (target-interleaved, each with its own warmup + `COOLDOWN_SECS`), which
   is a long sustained run on a laptop -- see "Thermal stability" above.
   `run-matrix.sh` checks this itself and refuses to start on battery
   power (see "Battery / AC power pre-flight check" above); this step is
   just so you're not surprised by that refusal.

6. Run the matrix:

   ```bash
   ./bench/scripts/run-matrix.sh
   # smoke test first, before committing to the full multi-hour matrix
   # (interleaving + a 20s default cooldown between every measurement
   # adds up fast -- budget for it, or shrink it for a smoke test):
   RUNS_PER_CELL=1 CONCURRENCIES="50" LATENCIES="1ms" WARMUP_DURATION=3s \
     MEASURE_DURATION=5s COOLDOWN_SECS=2 \
     ./bench/scripts/run-matrix.sh
   # add the harness-limited c=5000 cell on top of the default range:
   INCLUDE_C5000=1 ./bench/scripts/run-matrix.sh
   ```

7. Generate the markdown table:

   ```bash
   python3 bench/scripts/parse-results.py bench/results/<timestamp>
   ```

   Check the top of `table.md` first for the thermal-drift banner before
   trusting anything below it.

8. Run the failure scenarios (separate from the matrix; manifold-only,
   see the header comment in the script for why nginx isn't included):

   ```bash
   ./bench/scripts/run-failure-scenarios.sh
   ```

Every script cleans up its own processes on exit, including on Ctrl-C
(`trap cleanup_all EXIT INT TERM` in both drivers) -- but if a run is
killed with `SIGKILL` (`kill -9`) or the shell itself dies, check for
orphaned `manifold`, `nginx`, or `backend` processes before starting
another run:

```bash
pgrep -fa 'bin/manifold|bin/backend|nginx: master'
```

## WSL2 caveats

- **CPU/memory limits are governed by Windows, not Linux.** WSL2 runs in
  a lightweight VM; its visible `nproc` and total memory are capped by
  `.wslconfig` (`%UserProfile%\.wslconfig` on the Windows side, `[wsl2]`
  section: `processors=`, `memory=`). If that file caps WSL2 below the
  physical core count, the "6+ cores" requirement above applies to the
  WSL2-visible count, not the host's.
- **Loopback networking inside WSL2 is still virtualized**, one layer
  removed from bare-metal Linux loopback. Absolute latency numbers from
  this harness on WSL2 are not directly comparable to the same matrix run
  on native Linux -- compare manifold vs. nginx vs. direct *within* one
  WSL2 run, don't compare absolute numbers *across* a WSL2 run and a
  native Linux run.
- **The Windows host's other activity is noise you can't fully pin
  away.** `taskset` pins WSL2's own view of CPUs, but Windows' scheduler
  ultimately arbitrates the physical cores across the whole machine.
  Close other CPU-heavy applications before running the matrix, especially
  before the `c=5000` cells.
- **The WSL2 VM's clock can drift** during long-suspended sessions
  (laptop sleep). If a run behaves suspiciously (huge latency spikes with
  no obvious cause), restart WSL2 (`wsl --shutdown` from PowerShell, then
  reopen) to force a clock resync before re-running.
- **File I/O across the `/mnt/c/...` boundary is slow.** Keep the repo
  checked out inside the WSL2 filesystem (e.g. `~/manifold`, an ext4 path)
  rather than working against `/mnt/c/Users/...` for anything
  performance-sensitive. Reading `nginx.conf`/`config.example.yaml` and
  writing small JSON result files is not perf-sensitive, but if you're
  also benchmarking disk-adjacent behavior, this matters.

## Required system tuning for c=5000

Only needed if you opt into `INCLUDE_C5000=1` (see "c=5000 is opt-in and
harness-limited" above) -- the default matrix (c=50/200/1000) does not
need this. At 5000 concurrent virtual users, default Ubuntu limits on
open files, ephemeral ports, and the accept backlog will produce
artifacts (connection refusals, `EADDRNOTAVAIL`, artificially inflated
tail latency) that are about the *test harness* running out of headroom,
not about manifold or nginx -- and given c=5000 is already
harness-limited on this hardware (see above), letting the harness ALSO be
the source of connection errors would make that row actively misleading
rather than just low-value. Apply all of the following before running the
`c=5000` cell. Most require root; the `ulimit` change only affects the
current shell (and whatever it forks, i.e. exactly the shell you run
`run-matrix.sh` from).

```bash
# 1. Raise the open-file-descriptor limit for this shell (and everything
#    run-matrix.sh forks from it: k6, manifold, nginx, the backends).
#    Each concurrent connection is at least one fd; at c=5000 with
#    keepalive pooling to 3 backends you can be well past the default
#    soft limit of 1024.
ulimit -n 65535

# 2. Widen the ephemeral port range. k6 (and nginx's own upstream
#    connections) open a new local port per outbound TCP connection;
#    the default range only has ~28k ports, which is tight once you
#    account for TIME_WAIT accumulation at high connection churn.
sudo sysctl -w net.ipv4.ip_local_port_range="10000 65535"

# 3. Raise the accept backlog. This is what nginx.conf's `backlog=` and
#    manifold's underlying net/http listener both draw from; the kernel
#    default of 128 will silently drop SYNs under a burst of 5000
#    concurrent connection attempts.
sudo sysctl -w net.core.somaxconn=65535

# 4. Allow fast reuse of TIME_WAIT sockets on loopback. At high connection
#    churn (short-lived connections, or a scenario that doesn't pool
#    keepalive) TIME_WAIT sockets can otherwise pile up faster than the
#    kernel recycles them, eventually exhausting the ephemeral port range
#    from step 2.
sudo sysctl -w net.ipv4.tcp_tw_reuse=1
```

Make the `sysctl` changes persistent across reboots (optional, but saves
re-running them every session) by adding to `/etc/sysctl.conf` or a file
under `/etc/sysctl.d/`:

```
net.ipv4.ip_local_port_range = 10000 65535
net.core.somaxconn = 65535
net.ipv4.tcp_tw_reuse = 1
```

then `sudo sysctl -p`. The `ulimit -n` change is per-shell and cannot be
made persistent the same way; either raise the hard limit in
`/etc/security/limits.conf` for the user running the benchmark, or simply
re-run `ulimit -n 65535` in every new shell before invoking
`run-matrix.sh`.

Verify before a `c=5000` run:

```bash
ulimit -n                                    # expect >= 65535
sysctl net.ipv4.ip_local_port_range          # expect "10000 65535" or wider
sysctl net.core.somaxconn                    # expect >= 65535
sysctl net.ipv4.tcp_tw_reuse                 # expect 1
```

## Reading the output

- Raw per-run JSON: `bench/results/<timestamp>/<target>-<strategy>-c<N>-l<latency>-run<i>.json`.
  Includes k6's measurements (rps, latency percentiles, error rate,
  request/failure counts) plus the LB process's sampled CPU% and RSS for
  that run (`null` for `target=direct`, which has no LB process). Because
  targets are now interleaved (see "Thermal stability" above), the 3 runs
  of one cell are not necessarily contiguous in the log timestamps, but
  they are grouped together within one strategy pass.
- `meta.json` in the same directory: run provenance (see above), plus
  `thermally_compromised` (bool) and `drift_check` (the full comparison
  object) once the matrix finishes.
- `drift-check.json`: the end-of-matrix re-measurement of the first cell
  and its comparison against the original -- see "Thermal stability".
- `table.md` in the same directory (after running `parse-results.py`):
  - A thermal-drift banner at the very top -- always read this before the
    table. It's loud if `thermally_compromised` is true, a quiet
    confirmation line if the run passed, and a note if no drift check was
    recorded at all (e.g. an older run predating this feature).
  - Min / median / max RPS per cell (not median alone), plus median
    latency percentiles, error rate, LB CPU%/RSS.
  - A manifold-vs-nginx RPS delta column, computed against the
    cell's median. Only computed for `strategy=round_robin`, since
    `bench/nginx/nginx.conf` only models manifold's default config -- see
    that file's header comment for why `least_conn`/`consistent_hash` have
    no nginx baseline to diff against.
  - A `Notes` column flagging `harness-limited (c=5000, see README)` on
    any row with `concurrency >= 5000` (only present when the matrix was
    run with `INCLUDE_C5000=1`) -- see "c=5000 is opt-in and
    harness-limited" above. Also keep the "backend pool as a secondary
    bottleneck" caveat from "Core-pinning scheme" in mind for the highest
    concurrency rows generally, whether or not they're the opt-in c=5000
    cell specifically.
- Failure-scenario output:
  `bench/results/<timestamp>-failure-scenarios/0N-<name>.json`, one file
  per scenario. Each documents its own detection method inline (a
  `method` field) because several of the signals (breaker-open timing,
  readmission timing) are inferred from client-observable behavior rather
  than a direct internal state read -- see the header comment in
  `run-failure-scenarios.sh` for the specific assumptions made about
  manifold's reload trigger and metric names.
