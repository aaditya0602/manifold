# manifold

An L7 HTTP load balancer in Go — path/header routing across upstream pools, with health checking, per-upstream circuit breaking, bounded in-flight backpressure, and benchmarks measured against nginx on identical pinned hardware.

An intake manifold splits one flow across many outlets. So does this.

> **Status: Week 1 of 4 — walking skeleton.** The proxy serves real traffic across multiple upstreams with round-robin balancing, retries, and graceful drain. Health checking, circuit breaking, backpressure, observability, and the benchmark results are not built yet. The roadmap below marks exactly what exists and what does not, and this README will not claim otherwise before the code lands.

---

## What works today

```
$ curl -sD - http://localhost:8080/hello | grep -i manifold
X-Manifold-Upstream: http://127.0.0.1:9002
```

- **L7 routing** — first-match-wins rules over host, path prefix, method, and headers, mapping requests to named upstream pools.
- **Round-robin balancing**, weighted, on a lock-free pick path.
- **Retry policy** that is narrow on purpose: connection-level failures only, idempotent methods only, only before a byte has reached the client, only when the body is replayable, and never onto a backend already tried.
- **Graceful drain** on SIGINT/SIGTERM with a bounded deadline.
- **Config validation** that reports every problem at once with YAML paths, rejects unknown keys at any nesting depth, and distinguishes an omitted key from one explicitly set to zero or false.
- **Separate admin listener** for `/healthz`, `/version`, and pprof — never reachable from the data-plane port.
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
| Week 2 | Active health probing | not started |
| Week 2 | Passive ejection on error rate | not started |
| Week 2 | Least-connections, consistent hash | not started |
| Week 2 | Prometheus `/metrics` | not started |
| Week 3 | Circuit breaker, half-open probing | not started |
| Week 3 | Bounded in-flight, 503 shedding | not started |
| Week 3 | Hot config reload, zero dropped connections | not started |
| Week 3 | OpenTelemetry spans | not started |
| Week 4 | Benchmark results + methodology | not started |

`least_conn` and `consistent_hash` are accepted by config validation but return a startup error from the strategy factory. That is deliberate: the schema is stable from day one, and the gap is loud rather than silent.

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

## What is standard library, and what is not

This matters, and overclaiming it is the kind of thing an infra interviewer catches in one question.

**Go's standard library does the following, not this project:**

- **Connection pooling and keep-alive** — `net/http.Transport`. manifold *sizes and bounds* it (`max_idle_conns_per_host`, `max_conns_per_host`, timeouts) and gives each pool its own transport. It does not implement pooling.
- **HTTP/1.1 parsing, chunked encoding, and the reverse-proxy mechanics** — `net/http` and `net/http/httputil.ReverseProxy`.
- **Hop-by-hop header stripping** — `ReverseProxy`, including headers a client names in its own `Connection` header.
- **Graceful shutdown primitive** — `http.Server.Shutdown`.

**This project implements:** the routing table and match evaluation, the balancing strategies, the retry policy and its safety conditions, the trust model for forwarding headers, per-pool transport construction and backend accounting, config schema/validation/presence-aware defaulting, the drain orchestration across two listeners — and, in the weeks ahead, health checking, ejection, circuit breaking, backpressure, hot reload, and observability.

## Limitations

- **HTTP/1.1 only.** HTTP/2 is explicitly disabled (`ForceAttemptHTTP2: false`), not merely unconfigured. No HTTP/3, no gRPC.
- **No TLS termination.** Terminate at an edge in front of manifold.
- **Retries require a replayable body** — currently only empty-bodied requests. Buffering bodies for replay is out of scope.
- **No rate limiting, auth, or WAF.**
- **Route header matching is exact, single-valued** — a route cannot require a repeated header or match on absence.
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
[`bench/README.md`](bench/README.md).

Targets this project set for itself, reported whether or not they are met:

| Target | Status |
|---|---|
| Within 20% of nginx throughput at c=1000, 1ms backends | **not met** — 28.4% at c=50; c=1000 not yet measured |
| p99 overhead over direct under 2ms at c=200 | not yet measured (2.71ms at c=50) |
| Zero dropped connections across 10 hot reloads | not yet built (Week 3) |
| Backend ejection within 2x the health-check interval | not yet built (Week 2) |

## Development

```bash
make test
```

The race detector needs cgo and a C toolchain, so it does not run on a stock Windows box. CI runs it on Linux; locally use `make test-race` under WSL2.

## License

MIT
