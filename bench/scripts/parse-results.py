#!/usr/bin/env python3
"""bench/scripts/parse-results.py

Reads a results directory produced by run-matrix.sh, computes medians
across the (default 3) runs of each (target, strategy, concurrency,
latency) cell, and writes <results_dir>/table.md -- a markdown table ready
to paste into the top-level README, including a manifold-vs-nginx delta
column.

Stdlib only, no third-party dependencies (this harness assumes a bare
WSL2 Ubuntu + Python 3 install with nothing pip-installed).

Usage:
    python3 parse-results.py bench/results/20260101T000000Z
    python3 parse-results.py                      # uses the newest
                                                    # timestamped dir under
                                                    # bench/results/
"""

from __future__ import annotations

import json
import re
import statistics
import sys
from pathlib import Path

# Matches run-matrix.sh's file naming convention:
#   <target>-<strategy>-c<concurrency>-l<latency>-run<i>.json
# e.g. manifold-round_robin-c1000-l25ms-run2.json
CELL_FILE_RE = re.compile(
    r"^(?P<target>[a-zA-Z0-9]+)-(?P<strategy>[a-zA-Z0-9_]+)"
    r"-c(?P<concurrency>\d+)-l(?P<latency>\d+ms)-run(?P<run>\d+)\.json$"
)


def find_results_dir(arg: str | None) -> Path:
    results_root = Path(__file__).resolve().parents[1] / "results"
    if arg:
        p = Path(arg)
        if not p.is_dir():
            sys.exit(f"error: results directory not found: {p}")
        return p
    if not results_root.is_dir():
        sys.exit(f"error: no results directory to default to: {results_root}")
    candidates = sorted(
        (d for d in results_root.iterdir() if d.is_dir() and not d.name.endswith("-failure-scenarios")),
        key=lambda d: d.name,
    )
    if not candidates:
        sys.exit(f"error: no matrix run directories found under {results_root}")
    return candidates[-1]


def load_cell_files(results_dir: Path) -> list[dict]:
    rows = []
    skipped = []
    for f in sorted(results_dir.glob("*.json")):
        m = CELL_FILE_RE.match(f.name)
        if not m:
            # meta.json and drift-check.json are read separately; *.warmup.json
            # are the deliberately discarded warmup passes. None of them are
            # stray files, so do not report them as unexpected.
            if f.name not in ("meta.json", "drift-check.json") and not f.name.endswith(".warmup.json"):
                skipped.append(f.name)
            continue
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            print(f"warning: could not parse {f.name}: {e}", file=sys.stderr)
            continue
        data["_target"] = m.group("target")
        data["_strategy"] = m.group("strategy")
        data["_concurrency"] = int(m.group("concurrency"))
        data["_latency"] = m.group("latency")
        data["_run"] = int(m.group("run"))
        rows.append(data)
    if skipped:
        print(
            f"note: ignored {len(skipped)} file(s) not matching the run-cell naming "
            f"convention (e.g. failure-scenario output, warmup leftovers): "
            f"{', '.join(skipped[:5])}{' ...' if len(skipped) > 5 else ''}",
            file=sys.stderr,
        )
    return rows


def median_or_none(values: list) -> float | None:
    clean = [v for v in values if v is not None]
    if not clean:
        return None
    return statistics.median(clean)


def min_or_none(values: list) -> float | None:
    clean = [v for v in values if v is not None]
    return min(clean) if clean else None


def max_or_none(values: list) -> float | None:
    clean = [v for v in values if v is not None]
    return max(clean) if clean else None


def summarize_cell(runs: list[dict]) -> dict:
    def get_latency(field):
        return [r.get("latency_ms", {}).get(field) for r in runs]

    rps_values = [r.get("rps") for r in runs]

    return {
        "n_runs": len(runs),
        # rps gets min/median/max, not just median: on a thermally-variable
        # laptop chassis (see bench/README.md), the spread across a cell's 3
        # runs is itself evidence for or against thermal stability at that
        # cell, and hiding it would hide exactly the thing this benchmark
        # needs to be honest about. Other metrics stay median-only to keep
        # the table readable -- rps is the primary throughput indicator and
        # the one the end-of-matrix drift check itself tracks.
        "rps_min": min_or_none(rps_values),
        "rps_median": median_or_none(rps_values),
        "rps_max": max_or_none(rps_values),
        "p50": median_or_none(get_latency("p50")),
        "p90": median_or_none(get_latency("p90")),
        "p99": median_or_none(get_latency("p99")),
        "p999": median_or_none(get_latency("p999")),
        "error_rate": median_or_none([r.get("error_rate") for r in runs]),
        "lb_cpu_pct": median_or_none([r.get("lb_cpu_pct") for r in runs]),
        "lb_rss_kb_avg": median_or_none([r.get("lb_rss_kb_avg") for r in runs]),
    }


def fmt(v, digits=1, suffix=""):
    if v is None:
        return "n/a"
    return f"{v:.{digits}f}{suffix}"


def fmt_pct(v, digits=2):
    if v is None:
        return "n/a"
    return f"{v * 100:.{digits}f}%"


# Concurrency at/above this is the opt-in, harness-limited c=5000 cell (see
# INCLUDE_C5000 in run-matrix.sh and bench/README.md) -- on the 8-core
# laptop this harness targets, that row measures k6/backend-pool ceiling,
# not the load balancer, and the table calls that out per-row rather than
# letting it masquerade as a normal data point.
HARNESS_LIMITED_CONCURRENCY_THRESHOLD = 5000


def build_table(cells: dict) -> str:
    # cells: {(target, strategy, concurrency, latency): summary_dict}
    lines = []
    lines.append(
        "| Target | Strategy | Concurrency | Backend Latency | Runs | RPS min | "
        "RPS median | RPS max | p50 (ms) | p90 (ms) | p99 (ms) | p99.9 (ms) | "
        "Error Rate | LB CPU % | LB RSS (MB) | Δ RPS vs nginx | Notes |"
    )
    lines.append(
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"
    )

    def sort_key(k):
        target, strategy, concurrency, latency = k
        target_order = {"direct": 0, "manifold": 1, "nginx": 2}
        return (
            target_order.get(target, 99),
            strategy,
            latency,
            concurrency,
        )

    for key in sorted(cells.keys(), key=sort_key):
        target, strategy, concurrency, latency = key
        s = cells[key]

        delta_str = "n/a"
        if target == "manifold":
            nginx_key = ("nginx", "round_robin", concurrency, latency)
            nginx_cell = cells.get(nginx_key)
            if nginx_cell and nginx_cell["rps_median"] and s["rps_median"] is not None:
                delta_pct = (
                    (s["rps_median"] - nginx_cell["rps_median"]) / nginx_cell["rps_median"] * 100.0
                )
                sign = "+" if delta_pct >= 0 else ""
                delta_str = f"{sign}{delta_pct:.1f}%"
            elif strategy != "round_robin":
                delta_str = "n/a (nginx baseline only exists for round_robin)"

        rss_mb = s["lb_rss_kb_avg"] / 1024.0 if s["lb_rss_kb_avg"] is not None else None

        notes = []
        if concurrency >= HARNESS_LIMITED_CONCURRENCY_THRESHOLD:
            notes.append("harness-limited (c=5000, see README)")
        notes_str = "; ".join(notes) if notes else ""

        lines.append(
            "| {target} | {strategy} | {c} | {lat} | {n} | {rmin} | {rmed} | {rmax} | "
            "{p50} | {p90} | {p99} | {p999} | {err} | {cpu} | {rss} | {delta} | {notes} |".format(
                target=target,
                strategy=strategy,
                c=concurrency,
                lat=latency,
                n=s["n_runs"],
                rmin=fmt(s["rps_min"], 1),
                rmed=fmt(s["rps_median"], 1),
                rmax=fmt(s["rps_max"], 1),
                p50=fmt(s["p50"], 2),
                p90=fmt(s["p90"], 2),
                p99=fmt(s["p99"], 2),
                p999=fmt(s["p999"], 2),
                err=fmt_pct(s["error_rate"]),
                cpu=fmt(s["lb_cpu_pct"], 1, "%") if s["lb_cpu_pct"] is not None else "n/a",
                rss=fmt(rss_mb, 1),
                delta=delta_str,
                notes=notes_str,
            )
        )
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    arg = argv[1] if len(argv) > 1 else None
    results_dir = find_results_dir(arg)
    print(f"reading run-cell JSON files from: {results_dir}", file=sys.stderr)

    rows = load_cell_files(results_dir)
    if not rows:
        sys.exit(
            f"error: no run-cell JSON files found in {results_dir} "
            f"(expected names like manifold-round_robin-c200-l1ms-run1.json). "
            f"Has run-matrix.sh finished writing results here?"
        )

    grouped: dict[tuple, list[dict]] = {}
    for r in rows:
        key = (r["_target"], r["_strategy"], r["_concurrency"], r["_latency"])
        grouped.setdefault(key, []).append(r)

    cells = {key: summarize_cell(runs) for key, runs in grouped.items()}

    incomplete = [k for k, v in cells.items() if v["n_runs"] < 3]
    if incomplete:
        print(
            f"note: {len(incomplete)} cell(s) have fewer than 3 runs (median computed "
            f"from what's available): {incomplete[:5]}{' ...' if len(incomplete) > 5 else ''}",
            file=sys.stderr,
        )

    table_md = build_table(cells)

    meta_path = results_dir / "meta.json"
    header_lines = [f"# Benchmark results: {results_dir.name}", ""]
    meta = None
    if meta_path.is_file():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            header_lines += [
                f"- Host: `{meta.get('hostname', 'unknown')}`",
                f"- Kernel: `{meta.get('kernel', 'unknown')}`",
                f"- nproc: {meta.get('nproc', 'unknown')}",
                f"- Core assignment: `{meta.get('core_assignment', 'unknown')}`",
                f"- Go: `{meta.get('go_version', 'unknown')}`",
                f"- nginx: `{meta.get('nginx_version', 'unknown').splitlines()[0] if meta.get('nginx_version') else 'unknown'}`",
                f"- k6: `{meta.get('k6_version', 'unknown')}`",
                f"- Git commit: `{meta.get('git_commit', 'unknown')}` ({meta.get('git_working_tree', 'unknown')})",
                "",
            ]
        except (OSError, json.JSONDecodeError) as e:
            print(f"warning: could not read meta.json: {e}", file=sys.stderr)
    else:
        header_lines += ["(no meta.json found in this results directory -- provenance unknown)", ""]

    # Thermal drift banner. A benchmark that admits it drifted is worth more
    # than one that hides it -- this goes at the very top, above the table,
    # not buried in a footnote, and it's driven by run-matrix.sh's own
    # end-of-matrix re-run of the first cell (see drift-check.json).
    drift = (meta or {}).get("drift_check")
    thermally_compromised = (meta or {}).get("thermally_compromised")
    if thermally_compromised is True:
        drift_pct = drift.get("drift_pct") if drift else None
        threshold = drift.get("drift_threshold_pct") if drift else None
        header_lines += [
            "> ## :warning: THERMALLY COMPROMISED",
            "> "
            + (
                f"The first cell's median RPS drifted by **{drift_pct:.1f}%** between the "
                f"start and end of this matrix run (threshold: {threshold:.0f}%)."
                if drift_pct is not None and threshold is not None
                else "The end-of-matrix drift check could not be computed cleanly and was "
                "treated as compromised out of caution."
            ),
            "> This run's numbers should be treated with suspicion -- the chassis most "
            "likely throttled partway through. See `drift-check.json` in this results "
            "directory, and consider re-running with a longer `COOLDOWN_SECS`.",
            "",
        ]
    elif thermally_compromised is False:
        drift_pct = drift.get("drift_pct") if drift else None
        drift_desc = f"{drift_pct:.1f}% drift" if drift_pct is not None else "drift not computable"
        header_lines += [
            f"> Drift check passed: first-cell RPS stable ({drift_desc} observed between "
            f"start and end of this run).",
            "",
        ]
    else:
        header_lines += [
            "> No thermal drift check recorded for this run (older run-matrix.sh, or the "
            "matrix was empty) -- treat sustained-load numbers with the usual laptop-"
            "thermal skepticism. See bench/README.md.",
            "",
        ]

    out_path = results_dir / "table.md"
    out_path.write_text("\n".join(header_lines) + "\n" + table_md + "\n", encoding="utf-8")
    print(f"wrote {out_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
