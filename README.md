# manifold

An L7 HTTP load balancer in Go — path/header routing across upstream pools, with health checking, per-upstream circuit breaking, bounded in-flight backpressure, and benchmarks measured against nginx on identical pinned hardware.

An intake manifold splits one flow across many outlets. So does this.

> **Status: Week 3 of 4.** The proxy serves real traffic across multiple upstreams with all three balancing strategies, active and passive health checking, per-upstream circuit breaking, bounded in-flight backpressure, hot config reload, and Prometheus metrics. Distributed tracing is not built yet, and the full benchmark matrix has not been run. The roadmap below marks exactly what exists and what does not, and this README will not claim otherwise before the code lands.

---

## Architecture

```mermaid
flowchart LR
    C[Clients] -->|HTTP/1.1| SUP

    subgraph MANIFOLD["manifold process"]
        direction TB
        SUP["reload.Supervisor<br/>atomic.Pointer to the live generation"]
        SUP --> LIM["limit<br/>bounded in-flight, sheds 503"]
        LIM --> RT["proxy router<br/>first match wins on host/path/method/header"]
        RT --> BAL["balance<br/>round-robin · least-conn · consistent hash"]
        BAL --> BRK["breaker<br/>per upstream: closed / open / half-open"]
        BRK --> RP["httputil.ReverseProxy<br/>pooled copy buffers, retry policy"]

        HP["health.Prober<br/>active, own transport"] -.->|eligibility| POOL
        HT["health.Tracker<br/>passive error window"] -.->|eligibility| POOL
        POOL[("upstream.Pool<br/>availability + generation")]
        POOL -.->|candidate set| BAL
        RP -.->|per-attempt outcome| HT
    end

    RP -->|"pooled keep-alive"| B1["backend 1"]
    RP --> B2["backend 2"]
    RP --> B3["backend 3"]
    HP -.->|"/healthz probes"| B1

    ADMIN["admin listener :9090<br/>/metrics · /healthz · pprof"] -.-> MANIFOLD
    OBS["observe<br/>Prometheus registry"] -.-> ADMIN
```

The data plane and the admin plane are separate listeners: `/metrics` and pprof are never reachable from whatever can reach the proxy, and admin traffic never lands in the benchmark numbers.

Health checking never touches the request path. The prober and the passive tracker both write eligibility into `upstream.Pool`, which bumps a generation counter; the balancer receives an already-filtered candidate set and caches derived structures against that generation.

## What works today

```
$ curl -sD - http://localhost:8080/hello | grep -i manifold
X-Manifold-Upstream: http://127.0.0.1:9002
```

- **L7 routing** — first-match-wins rules over host, path prefix, method, and headers, mapping requests to named upstream pools.
- **Three balancing strategies** — weighted round-robin and least-connections on a lock-free pick path, plus consistent hashing with virtual nodes for session affinity.
- **Active health checking** — out-of-band probes on their own transport, with configurable interval, timeout, and consecutive-success/failure thresholds.
- **Passive ejection** — a sliding error-rate window over real traffic, so a backend that answers `/healthz` but fails actual requests is still removed. Automatic readmission after a cooldown.
- **Retry policy** that is narrow on purpose: connection-level failures only, idempotent methods only, only before a byte has reached the client, only when the body is replayable, and never onto a backend already tried.
- **Per-upstream circuit breaking** — closed/open/half-open, with a strictly enforced half-open probe budget, held in a single atomic word.
- **Bounded in-flight backpressure** — a per-pool concurrency cap that sheds with 503 rather than queueing without limit.
- **Hot config reload** — SIGHUP or optional file watching, with an atomic swap and a bounded drain of the previous generation. A bad config is rejected and the running proxy keeps serving.
- **Prometheus metrics** on the admin listener — request and upstream counters, a latency histogram bucketed for this proxy's actual p50 and p99, live in-flight and availability gauges.
- **Graceful drain** on SIGINT/SIGTERM with a bounded deadline.
- **Config validation** that reports every problem at once with YAML paths, rejects unknown keys at any nesting depth, and distinguishes an omitted key from one explicitly set to zero or false.
- **Separate admin listener** for `/metrics`, `/healthz`, `/version`, and pprof — never reachable from the data-plane port.
- A **configurable backend** (`bench/backend`) that impersonates slow, flaky, and dead upstreams, with runtime chaos controls on its own port.
- A **benchmark harness** against nginx and direct-to-backend, with thermal-drift detection and honest methodology.

## Roadmap

| | Capability | Status |
|---|---|---|
| Week 1 | HTTP/1.1 reverse proxy, multiple pools | **done** |
| Week 1 | Round-robin, weighted, lock-free | **done** |
| Week 1 | YAML config, validation, presence-aware defaults | **done** |
| Week 1 | Retry on connection failure, idempotent-only | **done** |
| Week 1 | Graceful drain, bounded | **done** |
| Week 1 | Benchmark harness + nginx baseline captured | **done** |
| Week 2 | Active health probing | **done** |
| Week 2 | Passive ejection on error rate | **done** |
| Week 2 | Least-connections, consistent hash | **done** |
| Week 2 | Prometheus `/metrics` | **done** |
| Week 3 | Circuit breaker, half-open probing | **done** |
| Week 3 | Bounded in-flight, 503 shedding | **done** |
| Week 3 | Hot config reload, zero dropped connections | **done** |
| Week 3 | OpenTelemetry spans | not started |
| Week 4 | Benchmark results + methodology | not started |

### Week 2 acceptance gate

The plan's gate was *"a backend killed under load is ejected and re-admitted automatically, proven by test."* `TestGate_BackendKilledUnderLoadIsEjectedAndReadmitted` kills a backend under ~10k rps of concurrent load and measures it. Across five consecutive runs:

| | measured | target |
|---|---|---|
| Time to ejection | 38–58 ms | 60 ms (2x the 30 ms probe interval) |
| Time to readmission | 27–52 ms | 60 ms |
| Client-visible errors after ejection | **0** | 0 |
| Client-visible errors *during* detection | **0** | some tolerated |
| All backends down | 503 in <1 ms | must not hang |

Zero errors during the detection window was not required — retries absorbed every request that hit the dying backend before the prober noticed.

One honest correction to the plan's phrasing: **"ejection within 2x the health-check interval" is an expectation, not a worst-case bound.** The prober randomises each backend's probe phase to avoid a self-inflicted thundering herd, so the first probe after a failure falls uniformly in `[0, interval)`. `unhealthy_threshold x interval` is therefore the mean; the true worst case is that plus one probe round-trip.

### Week 3 acceptance gate

The plan's gate was *"10 consecutive config reloads at sustained load with **zero** dropped connections."* `TestReload_TenReloadsUnderLoadDropZeroConnections` drives concurrent traffic over a real socket through a real `http.Server` while reloading ten times. Across five consecutive runs:

| | measured |
|---|---|
| Requests per run | 8,086 – 11,596 |
| Non-2xx responses | **0** |
| Connection errors | **0** |
| Ten reloads completed in | 318 – 380 ms |

Every response is counted, not sampled, and a body that fails to read to completion counts as a dropped connection even behind a `200` status line. Both the old and new backend sets are verified to have served traffic, so the test cannot pass by the swap silently not happening.

## Quickstart

Requires Go 1.24 or newer.

```bash
make build
```

Start three backends, each on its own data and admin port:

```bash
./bin/backend -addr :9001 -admin :9101 -id b1 -latency 1ms & ./bin/backend -addr :9002 -admin :9102 -id b2 -latency 1ms & ./bin/backend -addr :9003 -admin :9103 -id b3 -latency 1ms &
```

Start the balancer:

```bash
./bin/manifold -config config.example.yaml
```

Watch it spread traffic:

```bash
for i in $(seq 1 60); do curl -sD - -o /dev/null localhost:8080/hello | awk '/[Xx]-[Mm]anifold-[Uu]pstream/{print $2}'; done | sort | uniq -c
```

Validate a config without starting a listener:

```bash
./bin/manifold -config config.example.yaml -check
```

## Configuration

[`config.example.yaml`](config.example.yaml) is the reference — every field is documented inline, and the values shown are the real defaults. A test asserts that file parses and validates on every CI run, so it cannot drift from the schema.

Defaulting is **presence-aware**: an omitted key gets its default, and a key you write wins even when you write the zero value. `max_in_flight: 0` means unlimited and is not silently replaced by the default; `enabled: false` turns health checking off even though the default is on. This is why decoding starts from a populated struct rather than a zero one — see [`internal/config/defaults.go`](internal/config/defaults.go).

Unknown keys are rejected at every depth, including inside `pools[]` and `pools[].health.active`:

```
parse: yaml: unmarshal errors:
  line 9: field intervall not found in type config.ActiveHealthConfig
```

## Design decisions

**Config snapshots are immutable.** A reload builds a whole new `*Config` and swaps the pointer. Nothing on the request path takes a lock to read configuration.

**Balancing strategies are pure functions over a candidate snapshot.** The proxy decides which backends are eligible — health, ejection, breaker state — and passes only those in. A strategy never learns what "healthy" means, so every algorithm is testable with a plain slice and no live backends.

**Round-robin is lock-free, and clumps.** A single atomic counter reduced modulo total weight, with a per-generation weight table cached beside it. The alternative — nginx's smooth weighted round-robin — interleaves heavier backends more evenly but mutates per-candidate state under a lock on the hottest path in the proxy. Over any window of total-weight requests both hand out identical shares; they differ only in ordering inside the window. We took the clumping.

**Retries are narrow, and 5xx is not a retry condition.** A 5xx is a *successful* round trip: the backend was reachable and did the work. Retrying it turns one bad request into an outage amplifier. Only connection-level failures retry, only on idempotent methods, only before the client has seen a status, only when the body is replayable, and only onto a backend not already tried.

**`X-Forwarded-For` is not trusted by default.** Any client can forge it. Preserving an inbound chain is opt-in via `server.trust_forwarded_for`, for deployments behind an ALB or CDN where the peer address is just the edge. Defaulting that to true would have been a security bug — anything downstream reading the left-most entry could be lied to.

**Breaker, health, limits, and retry policy are per-pool, not global.** One misbehaving backend pool must not trip anything for another.

**Ejection expiry is explicit state, never a clock comparison.** `Ejected()` does not check `time.Now()`. If it did, a backend would silently rejoin the available set with nobody bumping the generation counter, and a consistent-hash ring cached against the old generation would keep routing to it. A sweeper calls `Readmit()` instead, which goes through `Pool` and bumps the generation like every other transition.

**A 4xx is not a backend failure.** Passive ejection counts connection errors and 5xx only. Counting 4xx would mean a client spamming 404s could eject healthy backends — a self-inflicted outage with an external trigger.

**The health prober gets its own transport.** Probes on the data-plane pool would evict pooled connections under load, and a saturated data plane would starve the health checks meant to notice it was saturated.

**Probe phases are randomised per backend.** Every backend probing on the same tick is a thundering herd aimed at your own upstreams. The cost is that time-to-ejection becomes a distribution rather than a bound, which the gate results state plainly.

**Metric labels are a closed set.** HTTP method is client-controlled, so it is collapsed to nine known verbs plus `OTHER`; an unbounded label there is a memory-exhaustion vector, not just a cardinality annoyance. Label children are resolved once at startup, never per request — instrumentation costs 36ns and zero allocations per request, asserted by a test.

**The circuit breaker is a single atomic word.** State, both counters, and the reopen deadline are packed into one `uint64`. That is correctness, not micro-optimisation: opening must publish *"state is Open"* and *"until T"* as one act. Split across two words, publishing the deadline first lets a stale writer clobber a newer one, and publishing the state first lets a concurrent `Allow` read Open with the previous cycle's expired deadline and promote straight to half-open — re-hammering the backend that just failed. One word, one CAS, neither window exists.

**The open→half-open transition and the first probe admission are the same CAS.** Transitioning first and admitting after would let the entire backlog observe "half-open, budget free" simultaneously and stampede a backend that has just told you it is sick.

**Shedding answers 503, not 429.** 429 blames the caller, who may be sending one request a minute into a pool saturated by someone else. `Retry-After: 1` is an honest hint rather than a computed backoff, because manifold deliberately keeps no queue depth to extrapolate from.

**A failed reload keeps serving the old config.** Parse errors and configs that validate but cannot build both leave the running proxy untouched. A config typo must never be able to take down a healthy proxy.

**The new server is swapped in before the old one is drained.** In-flight requests still hold the old server; closing it at swap time is precisely how connections get dropped. The old generation is retired only once its in-flight count reaches zero, bounded by `drain_timeout`.

## What is standard library, and what is not

This matters, and overclaiming it is the kind of thing an infra interviewer catches in one question.

**Go's standard library does the following, not this project:**

- **Connection pooling and keep-alive** — `net/http.Transport`. manifold *sizes and bounds* it (`max_idle_conns_per_host`, `max_conns_per_host`, timeouts) and gives each pool its own transport. It does not implement pooling.
- **HTTP/1.1 parsing, chunked encoding, and the reverse-proxy mechanics** — `net/http` and `net/http/httputil.ReverseProxy`.
- **Hop-by-hop header stripping** — `ReverseProxy`, including headers a client names in its own `Connection` header.
- **Graceful shutdown primitive** — `http.Server.Shutdown`.

**This project implements:** the routing table and match evaluation, the balancing strategies, the retry policy and its safety conditions, the trust model for forwarding headers, per-pool transport construction and backend accounting, config schema/validation/presence-aware defaulting, the drain orchestration across two listeners, active and passive health checking with ejection and readmission, the Prometheus instrumentation, per-upstream circuit breaking, bounded in-flight backpressure, and hot configuration reload with atomic swap and bounded drain — and, in the week ahead, distributed tracing.

## Limitations

- **HTTP/1.1 only.** HTTP/2 is explicitly disabled (`ForceAttemptHTTP2: false`), not merely unconfigured. No HTTP/3, no gRPC.
- **No TLS termination.** Terminate at an edge in front of manifold.
- **Retries require a replayable body** — currently only empty-bodied requests. Buffering bodies for replay is out of scope.
- **No rate limiting, auth, or WAF.**
- **Route header matching is exact, single-valued** — a route cannot require a repeated header or match on absence.
- **Consistent hashing on `client_ip` uses the peer address**, not the left-most `X-Forwarded-For` entry, even when `trust_forwarded_for` is on. Reading the header would make session affinity forgeable by any client, so the gap is deliberate and unfixed rather than silently wrong.
- **Passive ejection needs traffic.** A backend receiving almost no requests is never ejected passively; active probing is what covers that.
- **`weight: 0` is treated as 1**, not rejected, because a slice element cannot distinguish an omitted weight from an explicit zero without a further mirror type. See the note in `internal/config/defaults.go`.

## Benchmarks

**Preliminary.** A single run at one point in the matrix, not the full Week 4
measurement. Directionally sound, but do not quote these as final.

c=50, 1ms backends, 3 upstreams, 5s measured after a 3s discarded warmup.
Load generator, balancer, and backends pinned to disjoint core sets.

| Target | RPS | p50 | p90 | p99 | p99.9 | LB CPU % | LB RSS |
|---|---|---|---|---|---|---|---|
| direct (1 backend) | 24,764 | 1.60 | 3.12 | 7.18 | 11.05 | — | — |
| nginx 1.28.3 | 19,174 | 1.97 | 4.27 | 11.00 | 17.93 | 82.6% | 18.9 MB |
| **manifold** | **13,719** | 3.19 | 5.69 | **9.89** | **16.30** | 132.6% | 17.6 MB |

Thermal drift across the run: 4.6% (threshold 10%, passed).

**manifold is 28.4% below nginx on throughput, against a target of 20%.** That
target is not met, and the reason is visible in the CPU column: manifold spends
132.6% CPU to serve 13.7k rps (103 rps per CPU-percent) where nginx spends 82.6%
for 19.2k (232 rps per CPU-percent) — roughly 2.2x the CPU per request. Neither
process saturates its two-core budget, so this is per-request work, not a
scheduling ceiling. Profiling that is Week 4's job; pprof is already exposed on
the admin listener.

**manifold's tail latency is better than nginx's** — p99 9.89ms vs 11.00ms, and
p99.9 16.30ms vs 17.93ms — while its median is worse. Lower throughput with a
tighter tail is a coherent trade, not noise, and it is worth understanding
before optimising the median away.

An earlier run of the same cell showed a 49.8% gap. It was thermally
compromised: the drift check re-runs the first cell at the end and measured 20%
throughput decay across a 90-second matrix on this chassis. Roughly half of that
apparent gap was the laptop throttling, not manifold. The number above comes
from a run that passed the drift check. Full methodology, including what the
core pinning does and does not deliver under WSL2, is in
[`bench/README.md`](bench/README.md). The raw output behind this table --
including the thermally compromised run, kept as evidence -- is committed in
[`bench/results/`](bench/results/README.md).

Targets this project set for itself, reported whether or not they are met:

| Target | Status |
|---|---|
| Within 20% of nginx throughput at c=1000, 1ms backends | **not met** — 28.4% at c=50; c=1000 not yet measured |
| p99 overhead over direct under 2ms at c=200 | not yet measured (2.71ms at c=50) |
| Zero dropped connections across 10 hot reloads | **met** — 0 of ~10,000 requests, five consecutive runs |
| Backend ejection within 2x the health-check interval | **met** — 38-58ms against a 60ms target, zero client errors |

## Development

```bash
make test
```

The race detector needs cgo and a C toolchain, so it does not run on a stock Windows box. CI runs it on Linux; locally use `make test-race` under WSL2.

## License

MIT
