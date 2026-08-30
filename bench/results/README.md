# Committed benchmark results

Raw output is committed so the numbers in the top-level README can be checked
rather than taken on trust. Each directory is one `run-matrix.sh` invocation,
named by its UTC start time, and carries a `meta.json` recording the host,
kernel, core assignment, tool versions, and git commit that produced it.

## 20260830T172503Z - the run the README quotes

Complete: per-cell JSON, rendered nginx config, the manifold config actually
used, `/proc` CPU and RSS samples, process logs, and `table.md`.

Drift check passed at 4.6% against a 10% threshold, which is what makes this
run quotable.

## 20260830T170837Z - kept as evidence, not as a result

The same cell, measured 16 minutes earlier with a 2-second cooldown instead of
20. The end-of-matrix drift check re-ran the first cell and found throughput had
decayed 20.4% over a 90-second run, so `meta.json` carries
`"thermally_compromised": true`.

It is committed because the top-level README claims roughly half of an earlier
49.8% manifold-vs-nginx gap was the laptop throttling rather than the proxy.
That claim should be checkable. JSON only - the process logs add nothing.

Earlier runs from the same afternoon are not committed: they were produced by a
benchmark harness with known bugs (a readiness probe that killed runs silently,
CPU sampling that missed nginx's workers, timestamps read as milliseconds when
they were nanoseconds). Their numbers are not meaningful.
