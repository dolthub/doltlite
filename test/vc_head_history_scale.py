#!/usr/bin/env python3

import argparse
import collections
import os
import pathlib
import shutil
import statistics
import subprocess
import sys
import tempfile


QUERIES = (
    ("point_lookup", "SELECT v FROM t WHERE id=500;", "v_500"),
    (
        "table_scan",
        "SELECT count(*),sum(length(v)) FROM t;",
        "1000|4893",
    ),
    (
        "schema_lookup",
        "SELECT count(*) FROM sqlite_master "
        "WHERE name IN ('t','idx_t_v');",
        "2",
    ),
    ("status_clean", "SELECT count(*) FROM dolt_status;", "0"),
    (
        "working_diff_clean",
        "SELECT count(*) FROM dolt_diff_t WHERE to_commit='WORKING';",
        "0",
    ),
)


def run_sql(binary, database, sql):
    completed = subprocess.run(
        [str(binary), str(database)],
        input=sql,
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode:
        raise RuntimeError(
            f"{binary} failed with exit code {completed.returncode}: "
            f"{completed.stderr.strip()}"
        )
    return completed.stdout.strip()


def run_sql_file(binary, database, sql_path):
    with sql_path.open(encoding="utf-8") as sql_file:
        completed = subprocess.run(
            [str(binary), str(database)],
            stdin=sql_file,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
    if completed.returncode:
        raise RuntimeError(
            f"{binary} failed with exit code {completed.returncode}: "
            f"{completed.stderr.strip()}"
        )


def build_fixtures(binary, root, depths):
    root.mkdir(parents=True)
    work = root / "work.db"
    setup = """
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL);
CREATE INDEX idx_t_v ON t(v);
WITH RECURSIVE c(i) AS (
  VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<1000
)
INSERT INTO t SELECT i, 'v_' || i FROM c;
SELECT dolt_commit('-A','-m','base');
"""
    run_sql(binary, work, setup)
    current = 1
    shutil.copyfile(work, root / "depth_1.db")
    for depth in depths[1:]:
        sql_path = root / f"to_{depth}.sql"
        with sql_path.open("w", encoding="utf-8") as sql_file:
            for commit in range(current + 1, depth + 1):
                sql_file.write(
                    "SELECT dolt_commit('--allow-empty','-m',"
                    f"'history {commit}');\n"
                )
        run_sql_file(binary, work, sql_path)
        actual = run_sql(binary, work, "SELECT count(*) FROM dolt_log;")
        if actual != str(depth + 1):
            raise RuntimeError(
                f"fixture depth mismatch: expected {depth + 1}, got {actual}"
            )
        shutil.copyfile(work, root / f"depth_{depth}.db")
        current = depth
    fixtures = {depth: root / f"depth_{depth}.db" for depth in depths}
    expected = "\n".join(query[2] for query in QUERIES)
    validation = "\n".join(query[1] for query in QUERIES)
    for database in fixtures.values():
        actual = run_sql(binary, database, validation)
        if actual != expected:
            raise RuntimeError(
                f"fixture query mismatch: expected {expected!r}, got {actual!r}"
            )
    return fixtures


def write_workloads(root):
    workloads = {}
    for name, sql, _expected in QUERIES:
        path = root / f"{name}.sql"
        path.write_text(
            f".print BENCH_START\n{sql}\n.print BENCH_END\n",
            encoding="utf-8",
        )
        workloads[name] = path
    return workloads


def time_query(timer, database, workload):
    completed = subprocess.run(
        [str(timer), str(database), str(workload)],
        text=True,
        capture_output=True,
        check=False,
    )
    try:
        elapsed = int(completed.stdout.strip())
    except ValueError as exc:
        raise RuntimeError(
            f"invalid timer output: {completed.stdout.strip()!r}"
        ) from exc
    if completed.returncode or elapsed < 0:
        raise RuntimeError(
            f"timer failed with exit code {completed.returncode}: "
            f"{completed.stderr.strip()}"
        )
    return elapsed


def parse_depths(value):
    try:
        depths = [int(part) for part in value.replace(",", " ").split()]
    except ValueError as exc:
        raise argparse.ArgumentTypeError("depths must be integers") from exc
    if not depths or depths[0] != 1:
        raise argparse.ArgumentTypeError("depths must begin with 1")
    if depths != sorted(set(depths)):
        raise argparse.ArgumentTypeError("depths must be unique and increasing")
    return depths


def median(samples):
    return int(statistics.median(samples))


def write_results(path, medians, depths, has_baseline):
    with pathlib.Path(path).open("w", encoding="utf-8") as output:
        for name, _sql, _expected in QUERIES:
            for depth in depths:
                baseline = medians["baseline", name, depth] if has_baseline else 0
                candidate = medians["candidate", name, depth]
                output.write(
                    f"vc-scale\t{name}_depth_{depth}\t"
                    f"{baseline}\t{candidate}\n"
                )


def write_samples(path, samples, depths, runs, has_baseline):
    with pathlib.Path(path).open("w", encoding="utf-8") as output:
        output.write("section\ttest\trun\tbaseline_us\tcandidate_us\n")
        for name, _sql, _expected in QUERIES:
            for depth in depths:
                for run in range(1, runs + 1):
                    baseline = (
                        samples["baseline", name, depth, run]
                        if has_baseline
                        else 0
                    )
                    candidate = samples["candidate", name, depth, run]
                    output.write(
                        f"vc-scale\t{name}_depth_{depth}\t{run}\t"
                        f"{baseline}\t{candidate}\n"
                    )


def render_report(
    medians,
    depths,
    runs,
    has_baseline,
    baseline_label,
    candidate_label,
    max_ratio,
    min_delta_us,
):
    failures = []
    print("<!-- benchmark:vc-head-history-scale -->")
    print("## HEAD Query History Scaling")
    print()
    print(
        f"Runs: median of {runs} executions per query and depth. "
        "Every fixture has the same current schema and rows; deeper fixtures "
        "add only allow-empty ancestors."
    )
    print(
        "Connection open is excluded; first-statement preparation and "
        "execution are timed."
    )
    print(
        f"Gate: fail when {candidate_label} exceeds depth 1 by both "
        f"{max_ratio:.2f}x and {min_delta_us / 1000:.2f}ms."
    )
    print()
    if has_baseline:
        print(
            f"| Query | Ancestors | {baseline_label} ms | "
            f"{candidate_label} ms | vs depth 1 | Delta ms | Result |"
        )
        print("|---|---:|---:|---:|---:|---:|---|")
    else:
        print(
            f"| Query | Ancestors | {candidate_label} ms | "
            "vs depth 1 | Delta ms | Result |"
        )
        print("|---|---:|---:|---:|---:|---|")
    for name, _sql, _expected in QUERIES:
        shallow = medians["candidate", name, 1]
        for depth in depths:
            candidate = medians["candidate", name, depth]
            ratio = candidate / shallow
            delta = candidate - shallow
            failed = (
                depth != 1
                and ratio > max_ratio
                and delta > min_delta_us
            )
            status = "FAIL" if failed else ("REF" if depth == 1 else "PASS")
            if failed:
                failures.append((name, depth, ratio, delta))
            fields = [f"`{name}`", str(depth)]
            if has_baseline:
                fields.append(f"{medians['baseline', name, depth] / 1000:.2f}")
            fields.extend(
                (
                    f"{candidate / 1000:.2f}",
                    f"{ratio:.3f}x",
                    f"{delta / 1000:+.2f}",
                    status,
                )
            )
            print("| " + " | ".join(fields) + " |")
    return failures


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Measure current-state query cost across commit depths"
    )
    parser.add_argument("candidate")
    parser.add_argument(
        "--candidate-timer",
        default=os.environ.get("VC_HEAD_SCALE_CANDIDATE_TIMER", ""),
    )
    parser.add_argument(
        "--baseline", default=os.environ.get("VC_HEAD_SCALE_BASELINE", "")
    )
    parser.add_argument(
        "--baseline-timer",
        default=os.environ.get("VC_HEAD_SCALE_BASELINE_TIMER", ""),
    )
    parser.add_argument(
        "--depths",
        type=parse_depths,
        default=parse_depths(os.environ.get("VC_HEAD_SCALE_DEPTHS", "1 1000")),
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=int(os.environ.get("VC_HEAD_SCALE_RUNS", "7")),
    )
    parser.add_argument(
        "--max-ratio",
        type=float,
        default=float(os.environ.get("VC_HEAD_SCALE_MAX_RATIO", "1.25")),
    )
    parser.add_argument(
        "--min-delta-us",
        type=int,
        default=int(os.environ.get("VC_HEAD_SCALE_MIN_DELTA_US", "1000")),
    )
    args = parser.parse_args(argv)
    if args.runs < 1:
        parser.error("--runs must be at least 1")

    candidate = pathlib.Path(args.candidate).resolve()
    baseline = pathlib.Path(args.baseline).resolve() if args.baseline else None
    candidate_timer = pathlib.Path(
        args.candidate_timer
        or candidate.parent / "bench_timer_doltlite"
    ).resolve()
    baseline_timer = (
        pathlib.Path(
            args.baseline_timer
            or baseline.parent / "bench_timer_doltlite"
        ).resolve()
        if baseline
        else None
    )
    for binary in (candidate, baseline, candidate_timer, baseline_timer):
        if binary and not os.access(binary, os.X_OK):
            parser.error(f"binary is not executable: {binary}")

    samples = {}
    grouped = collections.defaultdict(list)
    with tempfile.TemporaryDirectory() as temporary:
        root = pathlib.Path(temporary)
        workloads = write_workloads(root)
        fixtures = {
            "candidate": build_fixtures(
                candidate, root / "candidate", args.depths
            )
        }
        timers = {"candidate": candidate_timer}
        if baseline:
            fixtures["baseline"] = build_fixtures(
                baseline, root / "baseline", args.depths
            )
            timers["baseline"] = baseline_timer

        for run in range(1, args.runs + 1):
            depths = args.depths if run % 2 else list(reversed(args.depths))
            queries = QUERIES if run % 2 else tuple(reversed(QUERIES))
            for depth in depths:
                for name, _sql, _expected in queries:
                    kinds = list(timers)
                    if run % 2 == 0:
                        kinds.reverse()
                    for kind in kinds:
                        elapsed = time_query(
                            timers[kind],
                            fixtures[kind][depth],
                            workloads[name],
                        )
                        samples[kind, name, depth, run] = elapsed
                        grouped[kind, name, depth].append(elapsed)

    medians = {key: median(values) for key, values in grouped.items()}
    results_path = os.environ.get("VC_PERF_RESULTS_OUTPUT")
    samples_path = os.environ.get("VC_PERF_SAMPLES_OUTPUT")
    if results_path:
        write_results(results_path, medians, args.depths, bool(baseline))
    if samples_path:
        write_samples(
            samples_path, samples, args.depths, args.runs, bool(baseline)
        )

    failures = render_report(
        medians,
        args.depths,
        args.runs,
        bool(baseline),
        os.environ.get("VC_PERF_BASELINE_LABEL", "PR base"),
        os.environ.get("VC_PERF_CANDIDATE_LABEL", "DoltLite"),
        args.max_ratio,
        args.min_delta_us,
    )
    if failures:
        print(
            f"{len(failures)} HEAD history-scaling gate(s) failed.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
