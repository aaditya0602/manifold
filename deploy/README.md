# manifold — five-minute demo

One command brings up manifold in front of three deliberately different
backends, plus Prometheus. Everything below is copy-pasteable from a shell in
this directory (`deploy/`), and every claim it makes is something you can go
watch happen.

Topology:

| Service | What it is | Reachable at |
|---|---|---|
| `backend-fast` | echoes after 1ms | admin only: `localhost:9101` |
| `backend-slow` | echoes after 50ms | admin only: `localhost:9102` |
| `backend-flaky` | echoes after 5ms, fails 10% of requests | admin only: `localhost:9103` |
| `manifold` | the load balancer | data `localhost:8080`, admin `localhost:9090` |
| `prometheus` | scrapes manifold every 2s | `localhost:9091` |

The backends' own data ports never leave the compose network — the whole
point is that you reach them *through* manifold, at `:8080`. Each backend's
*admin* port is published so you can drive its chaos controls directly (see
step 3 and step 4).

## If `docker compose up` fails on a port

A collision on `8080` is the most likely reason this fails on someone else's
machine, and it surfaces as the last container failing to start:

```
Bind for 0.0.0.0:8080 failed: port is already allocated
```

Every host port is overridable, so nothing here needs editing:

```bash
MANIFOLD_PORT=18080 MANIFOLD_ADMIN_PORT=19090 PROMETHEUS_PORT=19091 FAST_ADMIN_PORT=19101 SLOW_ADMIN_PORT=19102 FLAKY_ADMIN_PORT=19103 docker compose -f deploy/docker-compose.yml up --build -d
```

Substitute those ports into the commands below. The walkthrough in this file
was verified end to end on exactly that override.


## 1. Bring it up

```sh
docker compose up --build -d
```

Wait for everything to report healthy:

```sh
docker compose ps
```

You're ready when all five services show `running (healthy)`. If any
backend is still `starting`, give it a few seconds — `manifold` won't report
healthy until it can bind its admin listener, which happens fast, but it
also depends on the three backends passing their own healthchecks first.

Sanity check:

```sh
curl -sD - -o /dev/null http://localhost:8080/hello
```

A `200` with an `X-Manifold-Upstream` header means traffic is flowing.

## 2. Watch traffic spread across backends

`X-Manifold-Upstream` names the backend that answered each request. Fire 60
requests and tally which backend served each one:

```sh
for i in $(seq 1 60); do
  curl -sD - -o /dev/null http://localhost:8080/hello
done | awk '/[Xx]-[Mm]anifold-[Uu]pstream/{print $2}' | sort | uniq -c
```

Expect a roughly even three-way split — the pool is `round_robin` (see
`manifold.yaml`), and none of the three backends is unhealthy or breaker-open
yet, so all three are eligible on every pick.

## 3. Kill a backend, watch it get ejected and readmitted

Watch manifold's logs in one terminal:

```sh
docker compose logs -f manifold
```

In another terminal, take `backend-slow` down via its admin port:

```sh
curl "http://localhost:9102/_control/health?state=down"
```

`manifold.yaml` probes every 500ms and ejects after 2 consecutive failures,
so within about a second you should see a JSON log line with
`"msg":"upstream unavailable","pool":"demo","upstream":"http://backend-slow:9001"`
(manifold logs JSON by default), and this flip to `0`:

```sh
curl -s http://localhost:9090/metrics | grep 'manifold_upstream_available{.*backend-slow'
```

Confirm the traffic tally actually stopped including it:

```sh
for i in $(seq 1 30); do
  curl -sD - -o /dev/null http://localhost:8080/hello
done | awk '/[Xx]-[Mm]anifold-[Uu]pstream/{print $2}' | sort | uniq -c
```

Bring it back:

```sh
curl "http://localhost:9102/_control/health?state=up"
```

One good probe (`healthy_threshold: 1`) readmits it — the log shows
`"msg":"upstream available","pool":"demo","upstream":"http://backend-slow:9001"`
within about 500ms, and the gauge flips back to `1`:

```sh
curl -s http://localhost:9090/metrics | grep 'manifold_upstream_available{.*backend-slow'
```

## 4. Trip the circuit breaker

The breaker opens on consecutive connection-level failures per upstream —
not on slow-but-successful responses — so this demo pushes `backend-slow`'s
latency past `manifold.yaml`'s `response_header_timeout: 700ms`, which turns
its responses into timeouts:

```sh
curl "http://localhost:9102/_control/latency?d=2s"
```

Send enough traffic that some of it lands on `backend-slow` (round-robin, so
~1 in 3 requests will):

```sh
for i in $(seq 1 20); do
  curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8080/hello
done | sort | uniq -c
```

Notice every response is still `200` — `retry.max_attempts: 2` means a timed
out request to `backend-slow` gets silently retried onto `fast` or `flaky`,
so the client never sees the failure. The breaker still counts each timeout
against `backend-slow` specifically, and after 3 consecutive ones
(`failure_threshold: 3`) it opens:

```sh
curl -s http://localhost:9090/metrics | grep 'manifold_breaker_state{.*backend-slow'
# 0 closed, 1 open, 2 half_open
```

A `1` means every request round-robin routes to `backend-slow` is caught by
the breaker and silently redirected to `fast` or `flaky` before a connection
is even attempted. Put the latency back and watch the breaker recover
(`open_for: 5s`, then one half-open probe):

```sh
curl "http://localhost:9102/_control/latency?d=50ms"
sleep 6
```

The open->half-open transition is lazy — it happens on the next admission
check against `backend-slow`, not on a timer — so send a few more requests
to guarantee round-robin's rotation reaches it, then check the state:

```sh
for i in $(seq 1 6); do curl -s -o /dev/null http://localhost:8080/hello; done
curl -s http://localhost:9090/metrics | grep 'manifold_breaker_state{.*backend-slow'
# back to 0 (half_open's one probe already succeeded and closed it)
```

## 5. Edit the config, reload with zero dropped requests

Start a load loop in one terminal and leave it running:

```sh
for i in $(seq 1 400); do
  curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8080/hello
  sleep 0.02
done | sort | uniq -c
```

While that's running, edit `manifold.yaml` in another terminal — for
example, tighten the in-flight cap from 32 to 16:

```sh
sed -i 's/max_in_flight: 32/max_in_flight: 16/' manifold.yaml
```

(Or open `manifold.yaml` in an editor and change it by hand — either way,
the file on disk is what gets reloaded.)

Reload the running container with SIGHUP — no restart, no dropped
connections:

```sh
docker compose kill -s HUP manifold
```

`manifold`'s log prints a `config reloaded` line with the result. When the
load loop above finishes, every line should read `200` — reload swaps
configuration generations atomically and drains the old one; it never closes
a socket a client is mid-request on.

Revert the edit before moving on, so the rest of this walkthrough (and the
next `docker compose up`) sees the original config:

```sh
sed -i 's/max_in_flight: 16/max_in_flight: 32/' manifold.yaml
```

## 6. Prometheus

Open [http://localhost:9091](http://localhost:9091) and query, for example:

- `manifold_requests_total` — request counts by pool/route/method/status
- `rate(manifold_upstream_requests_total[1m])` — per-backend request rate,
  to see step 2's spread as a graph instead of a tally
- `manifold_upstream_available` — the same gauge step 3 grepped from
  `/metrics` directly, now as a time series
- `manifold_breaker_state` — the same for step 4
- `histogram_quantile(0.99, rate(manifold_request_duration_seconds_bucket[1m]))` —
  p99 latency

Prometheus' own target health page is at
[http://localhost:9091/targets](http://localhost:9091/targets); `manifold`
should show as `UP`.

## Tearing down

```sh
docker compose down -v
```

Or, from the repo root: `make demo-down` (`make demo` brings it back up).
