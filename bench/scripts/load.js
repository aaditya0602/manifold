// bench/scripts/load.js
//
// k6 load generator for the manifold benchmark harness. Two scenarios,
// selected at run time by the SCENARIO env var:
//
//   constant-vus            fixed concurrency (a fixed number of virtual
//                            users, each firing requests back-to-back).
//                            Models "N clients hammering as fast as they
//                            can" -- throughput is whatever the system
//                            gives back.
//
//   constant-arrival-rate   fixed OFFERED load (k6 tries to start exactly
//                            RATE new iterations per second regardless of
//                            how long each one takes). This is the one
//                            that actually exposes queueing behavior: if
//                            the system can't keep up with the offered
//                            rate, latency and the VU pool both grow
//                            instead of throughput just plateauing.
//
// Only one scenario runs per invocation (k6 does not support toggling
// which configured scenarios execute at runtime, so this script builds a
// single-entry `scenarios` map from SCENARIO instead of defining both
// unconditionally).
//
// Env vars:
//   TARGET_URL          base URL, e.g. http://127.0.0.1:8080 (required)
//   SCENARIO            "constant-vus" | "constant-arrival-rate" (default constant-vus)
//   VUS                 for constant-vus: VU count. For constant-arrival-rate:
//                        used as a floor for preAllocatedVUs. (default 50)
//   RATE                constant-arrival-rate only: iterations/sec (default 1000)
//   DURATION             measured duration, e.g. "30s" (default 30s)
//   PATH_URL            request path (default "/") -- named PATH_URL, not
//                        PATH, because PATH collides with the shell $PATH
//                        env var k6 also inherits
//   PRE_ALLOCATED_VUS   constant-arrival-rate only: override the VU pool
//                        k6 pre-allocates
//   MAX_VUS             constant-arrival-rate only: override the VU pool
//                        ceiling k6 may grow into
//   OUT_FILE            path to write the JSON summary to (default ./result.json)
//
// Passthrough labels (purely descriptive, copied verbatim into the output
// JSON so bench/scripts/parse-results.py doesn't have to re-derive them
// from the file path): TARGET, STRATEGY, CONCURRENCY, BACKEND_LATENCY, RUN_INDEX.

import http from 'k6/http';
import { check } from 'k6';

const TARGET_URL = __ENV.TARGET_URL;
if (!TARGET_URL) {
  throw new Error('TARGET_URL env var is required, e.g. -e TARGET_URL=http://127.0.0.1:8080');
}

const SCENARIO_NAME = __ENV.SCENARIO || 'constant-vus';
const DURATION = __ENV.DURATION || '30s';
const PATH_URL = __ENV.PATH_URL || '/';
const VUS = parseInt(__ENV.VUS || '50', 10);
const RATE = parseInt(__ENV.RATE || '1000', 10);
const OUT_FILE = __ENV.OUT_FILE || './result.json';

let scenarios = {};
if (SCENARIO_NAME === 'constant-vus') {
  scenarios[SCENARIO_NAME] = {
    executor: 'constant-vus',
    vus: VUS,
    duration: DURATION,
  };
} else if (SCENARIO_NAME === 'constant-arrival-rate') {
  const floor = Math.max(VUS, RATE);
  scenarios[SCENARIO_NAME] = {
    executor: 'constant-arrival-rate',
    rate: RATE,
    timeUnit: '1s',
    duration: DURATION,
    preAllocatedVUs: parseInt(__ENV.PRE_ALLOCATED_VUS || String(floor), 10),
    maxVUs: parseInt(__ENV.MAX_VUS || String(floor * 4), 10),
  };
} else {
  throw new Error(`unknown SCENARIO "${SCENARIO_NAME}", expected "constant-vus" or "constant-arrival-rate"`);
}

export const options = {
  scenarios,
  discardResponseBodies: true,
  // Controls which percentiles get computed into the summary data object
  // (and thus are available in handleSummary below), not just the default
  // text report.
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(50)', 'p(90)', 'p(99)', 'p(99.9)'],
  // Informational only (abortOnFail defaults to false) -- these never cut
  // a run short. The failure-scenario suite deliberately drives error
  // rates and latency up; a hard abort here would silently truncate the
  // very data those scenarios exist to capture. Breach state still shows
  // up in the returned JSON's "thresholds_ok" field and k6's exit code.
  thresholds: {
    http_req_failed: [{ threshold: 'rate<0.5', abortOnFail: false }],
    http_req_duration: [{ threshold: 'p(99)<5000', abortOnFail: false }],
  },
};

export default function () {
  const res = http.get(`${TARGET_URL}${PATH_URL}`);
  check(res, { 'status is 2xx/3xx': (r) => r.status >= 200 && r.status < 400 });
}

export function handleSummary(data) {
  const m = data.metrics;

  const getVal = (metric, key, fallback = null) => {
    if (!m[metric] || !m[metric].values) return fallback;
    const v = m[metric].values[key];
    return v === undefined ? fallback : v;
  };

  const reqCount = getVal('http_reqs', 'count', 0);
  const rps = getVal('http_reqs', 'rate', 0);
  const errorRate = getVal('http_req_failed', 'rate', 0);
  // Rate metric .passes = count of iterations where http_req_failed was
  // true, i.e. the number of failed requests (not a percentage).
  const requestsFailed = getVal('http_req_failed', 'passes', 0);

  const latencyMs = {
    p50: getVal('http_req_duration', 'p(50)'),
    p90: getVal('http_req_duration', 'p(90)'),
    p99: getVal('http_req_duration', 'p(99)'),
    p999: getVal('http_req_duration', 'p(99.9)'),
    avg: getVal('http_req_duration', 'avg'),
    max: getVal('http_req_duration', 'max'),
  };

  let thresholdsOk = true;
  for (const name in data.metrics) {
    const th = data.metrics[name].thresholds;
    if (!th) continue;
    for (const key in th) {
      if (th[key] && th[key].ok === false) thresholdsOk = false;
    }
  }

  const result = {
    // descriptive passthrough, filled in by run-matrix.sh / run-failure-scenarios.sh
    target: __ENV.TARGET || null,
    strategy: __ENV.STRATEGY || null,
    concurrency: __ENV.CONCURRENCY ? parseInt(__ENV.CONCURRENCY, 10) : null,
    backend_latency: __ENV.BACKEND_LATENCY || null,
    // RUN_INDEX is a run number for measured runs but the literal string
    // "warmup" for discarded warmup passes. parseInt("warmup") is NaN, which
    // then printed as run=NaN and serialised as null -- so keep a non-numeric
    // value as-is. Warmup lines now read run=warmup, which is what you want
    // when scanning console output for the runs that actually counted.
    run: Number.isNaN(parseInt(__ENV.RUN_INDEX, 10))
      ? (__ENV.RUN_INDEX || null)
      : parseInt(__ENV.RUN_INDEX, 10),

    // what this k6 invocation actually did
    scenario: SCENARIO_NAME,
    target_url: TARGET_URL,
    path: PATH_URL,
    duration: DURATION,
    vus_param: VUS,
    rate_param: SCENARIO_NAME === 'constant-arrival-rate' ? RATE : null,

    // measurements
    requests_total: reqCount,
    requests_failed: requestsFailed,
    rps: rps,
    error_rate: errorRate,
    latency_ms: latencyMs,
    thresholds_ok: thresholdsOk,

    // filled in later by run-matrix.sh (k6 has no visibility into the LB
    // process's own resource usage)
    lb_cpu_pct: null,
    lb_rss_kb_avg: null,
    lb_rss_kb_max: null,

    generated_at: new Date().toISOString(),
  };

  const stdoutSummary =
    `[k6] scenario=${SCENARIO_NAME} target=${result.target} strategy=${result.strategy} ` +
    `concurrency=${result.concurrency} latency=${result.backend_latency} run=${result.run}\n` +
    `  requests=${reqCount} rps=${rps.toFixed(1)} error_rate=${(errorRate * 100).toFixed(3)}%\n` +
    `  p50=${latencyMs.p50}ms p90=${latencyMs.p90}ms p99=${latencyMs.p99}ms p99.9=${latencyMs.p999}ms\n` +
    `  thresholds_ok=${thresholdsOk}\n`;

  return {
    stdout: stdoutSummary,
    [OUT_FILE]: JSON.stringify(result, null, 2),
  };
}
