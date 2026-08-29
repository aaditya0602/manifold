# backend

A configurable echo server used as the upstream-under-test for manifold's
benchmarks and chaos tests. It exposes two independent listeners:

- **data plane** (`-addr`) — the traffic manifold forwards, with dialable-in
  latency, jitter, error rate, and body size.
- **admin/control plane** (`-admin`) — a *separate* port used to flip health,
  latency, and error rate at runtime, and to read stats.

The two are kept on different ports deliberately: if control requests landed
on the same listener as the benchmarked traffic, they would show up in the
load balancer's own latency and throughput numbers and pollute every
benchmark run. Control traffic never touches the data listener.

## Build & run

```sh
go build -o backend.exe ./bench/backend
./backend.exe -addr :9001 -admin :9101
```

## Flags

| Flag           | Type       | Default      | Description                                            |
|----------------|------------|--------------|----------------------------------------------------------|
| `-addr`        | `string`   | `:9001`      | data-plane listen address                                |
| `-admin`       | `string`   | `:9101`      | control listen address (separate port)                   |
| `-id`          | `string`   | `""`         | backend identity; empty derives it from `-addr`           |
| `-latency`     | `duration` | `0`          | artificial per-request delay (e.g. `50ms`)                |
| `-jitter`      | `duration` | `0`          | uniform +/- added to latency                              |
| `-error-rate`  | `float64`  | `0`          | fraction of requests answered 500, in `[0,1]`             |
| `-body-size`   | `int`      | `0`          | response body padding in bytes                            |
| `-health-path` | `string`   | `/healthz`   | path on the data listener used for health checks          |

## Data-plane behaviour (`-addr`)

- `GET <health-path>` (default `/healthz`) — returns `200 ok` while healthy,
  `503 down` after `/_control/health?state=down` on the admin port.
- Any other path — sleeps `latency ± jitter`, then with probability
  `error-rate` returns `500`, otherwise `200` with a JSON body:

  ```json
  {
    "backend_id": "test-backend",
    "path": "/foo",
    "method": "GET",
    "latency_ms": 5,
    "in_flight": 1,
    "served": 42
  }
  ```

  If `-body-size` is larger than the natural body, a `"pad"` field of `x`
  filler characters is added to bring the marshalled response close to that
  size (see body-size padding below).

```sh
curl http://localhost:9001/some/path
```

## Admin/control-plane endpoints (`-admin`)

All control handlers validate their input and return `400` on a bad value.

### `GET /_control/health?state=down|up`

Flip health without restarting the process.

```sh
curl "http://localhost:9101/_control/health?state=down"
curl "http://localhost:9101/_control/health?state=up"
```

### `GET /_control/latency?d=<duration>`

Change latency at runtime, e.g. for the "backend latency 1ms -> 2000ms"
circuit-breaker experiment:

```sh
curl "http://localhost:9101/_control/latency?d=2s"
```

### `GET /_control/error-rate?r=<float in [0,1]>`

Change the error rate at runtime:

```sh
curl "http://localhost:9101/_control/error-rate?r=0.5"
```

### `GET /_control/stats`

Current settings plus served/in-flight counters, as JSON:

```sh
curl http://localhost:9101/_control/stats
```

```json
{
  "backend_id": "test-backend",
  "healthy": true,
  "latency_ms": 5,
  "error_rate": 0.5,
  "body_size": 0,
  "in_flight": 0,
  "served": 128,
  "health_path": "/healthz"
}
```

## Notes / behaviour decisions

- **Float error rate stored lock-free**: Go has no `atomic.Float64`, so
  `error-rate` is bit-cast into an `atomic.Uint64` via
  `math.Float64bits`/`Float64frombits` rather than guarded by a mutex. This
  keeps the hot read path (every data-plane request checks the error rate)
  lock-free at 5000 concurrent connections; latency is stored the same way
  as an `atomic.Int64` of nanoseconds.
- **Jitter** is applied as a uniform offset in `[-jitter, +jitter]` around the
  current latency, clamped to `>= 0`.
- **Graceful shutdown**: on `SIGINT`/`SIGTERM` both listeners stop accepting
  new connections and `http.Server.Shutdown` drains in-flight requests with a
  10s bound before the process exits.
- **Timeouts**: both servers set `ReadHeaderTimeout` (5s), `ReadTimeout`
  (15s), `WriteTimeout` (30s), and `IdleTimeout` (60s) explicitly so a stalled
  client can't pin a connection indefinitely under load.
- **`-id` default**: when empty, the backend identity used in responses and
  stats falls back to the `-addr` value (spec left the derivation rule
  unspecified).
- **`-health-path` must start with `/`**: `http.ServeMux` panics on a
  pattern without a leading slash, so this is validated at startup alongside
  `-error-rate` and `-body-size` and fails fast with `log.Fatalf` instead.
- **Body-size padding is precomputed once at startup**, not per request.
  `-body-size` never changes at runtime, so the filler string added to the
  `"pad"` field is computed a single time (`computePad` in `main.go`) against
  a representative response (root path, `GET`, zero-valued counters) and
  reused for every request — no second `json.Marshal` call and no fresh pad
  allocation on the hot path. Because a real request's path/method/counters
  can differ in length from that template, the actual response size can land
  a few bytes off `-body-size`; if `-body-size` is smaller than or equal to
  the natural (unpadded) body, no `"pad"` field is added at all. The filler
  uses printable `x` characters rather than zero bytes — `encoding/json`
  would otherwise escape each `\x00` as a 6-character unicode escape,
  inflating the body to roughly 6x the requested size.

## Tests

```sh
go test ./bench/backend/... -count=1
```

(`-race` is skipped on this host — no cgo toolchain — but is covered by CI on
ubuntu-latest.)
