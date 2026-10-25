#!/usr/bin/env python3

import argparse
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import time

from performance_hotspots import sql


PAYLOAD_BYTES = 1000
CASES = ("copy_fsync", "push_initial", "clone_initial", "push_noop",
         "fetch_noop", "push_incremental", "fetch_incremental", "pull_fast_forward")
RELATIVE_RATIO = 1.25
AGGREGATE_RATIO = 1.15
MIN_DELTA_US = 5000
CEILINGS_US = {"push_initial": 2_000_000, "clone_initial": 3_000_000,
               "push_noop": 10_000, "fetch_noop": 10_000,
               "push_incremental": 750_000, "fetch_incremental": 300_000,
               "pull_fast_forward": 300_000}
METRICS = ("read_bytes", "write_bytes", "read_calls", "write_calls", "peak_rss_bytes")
TIMER = re.compile(r"Run Time: real ([0-9.]+) user ([0-9.]+) sys ([0-9.]+)")


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def file_url(path):
    return "file://" + str(path.resolve())


def payload_sql(seed, row="id", revision=0):
    blocks = [f"sha3(printf('%d:%d:%d:%d',{seed},{row},{revision},{block}),512)"
              for block in range(math.ceil(PAYLOAD_BYTES / 64))]
    return f"substr(CAST({'||'.join(blocks)} AS BLOB),1,{PAYLOAD_BYTES})"


def payload_bytes(seed, row, revision):
    return b"".join(hashlib.sha3_512(f"{seed}:{row}:{revision}:{block}".encode()).digest()
                    for block in range(math.ceil(PAYLOAD_BYTES / 64)))[:PAYLOAD_BYTES]


def changed_ids(rows, delta_rows):
    return [1 + i * rows // delta_rows for i in range(delta_rows)]


def setup_sql(rows, seed):
    return f"""BEGIN;
CREATE TABLE t(id INTEGER PRIMARY KEY,bucket INTEGER NOT NULL,
               revision INTEGER NOT NULL,payload BLOB NOT NULL);
CREATE INDEX t_bucket ON t(bucket);
WITH RECURSIVE c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<{rows})
INSERT INTO t SELECT i,i%1024,0,{payload_sql(seed, 'i')} FROM c;
COMMIT;"""


def delta_sql(rows, delta_rows, seed):
    return f"""BEGIN;
WITH RECURSIVE c(i) AS (VALUES(0) UNION ALL SELECT i+1 FROM c WHERE i+1<{delta_rows})
UPDATE t SET bucket=(id+1)%1024,revision=1,payload={payload_sql(seed, revision=1)}
WHERE id IN (SELECT 1+i*{rows}/{delta_rows} FROM c);
COMMIT;"""


def expected_summary(rows, delta_rows=0):
    buckets = sum(i % 1024 for i in range(1, rows + 1))
    if delta_rows:
        buckets += sum((i + 1) % 1024 - i % 1024 for i in changed_ids(rows, delta_rows))
    return f"{rows}|{rows * PAYLOAD_BYTES}|{rows * (rows + 1) // 2}|{delta_rows}|{buckets}"


def digest_query(table):
    return (f"CREATE TEMP VIEW remote_perf_rows AS SELECT id,bucket,revision,payload FROM {table};\n"
            "SELECT hex(sha3_query('SELECT id,bucket,revision,payload FROM remote_perf_rows ORDER BY id'));\n"
            "DROP VIEW remote_perf_rows;")


def fingerprint(binary, db):
    statements = f"""SELECT dolt_hashof('HEAD');
{digest_query('t')}
SELECT hex(sha3_query('SELECT type,name,tbl_name,sql FROM main.sqlite_schema ORDER BY type,name'));
SELECT count(*),sum(length(payload)),sum(id),sum(revision),sum(bucket) FROM t;
SELECT count(*) FROM dolt_status;
PRAGMA integrity_check;"""
    values = sql(binary, db, statements).splitlines()
    if len(values) != 6 or values[-2:] != ["0", "ok"]:
        raise ValueError(f"invalid database {db}: {values}")
    return dict(zip(("head", "data_sha3", "schema_sha3", "summary"), values[:4]))


def verify(binary, db, expected, tracking=None):
    actual = fingerprint(binary, db)
    if actual != expected:
        raise ValueError(f"database mismatch in {db}: expected {expected}, got {actual}")
    if tracking:
        ref = "remotes/origin/main"
        actual = sql(binary, db, f"SELECT dolt_hashof({literal(ref)});\n"
                     + digest_query(f"dolt_at_t({literal(ref)})")).splitlines()
        if actual != [tracking["head"], tracking["data_sha3"]]:
            raise ValueError(f"tracking snapshot mismatch in {db}: {actual}")


def copy_synced(source, destination):
    shutil.copyfile(source, destination)
    with destination.open("rb") as stream:
        os.fsync(stream.fileno())


def prepare_fixtures(binary, root, rows, delta_rows, seed):
    base, delta = root / "base.db", root / "delta.db"
    sql(binary, base, setup_sql(rows, seed) + "\n"
        "SELECT dolt_commit('-A','-m','remote benchmark base','--author',"
        "'Remote Performance <perf@example.com>','--date','2026-01-01T00:00:00Z');")
    base_state = fingerprint(binary, base)
    if base_state["summary"] != expected_summary(rows):
        raise ValueError(f"invalid base fixture: {base_state}")
    copy_synced(base, delta)
    sql(binary, delta, delta_sql(rows, delta_rows, seed) + "\n"
        "SELECT dolt_commit('-A','-m','remote benchmark delta','--author',"
        "'Remote Performance <perf@example.com>','--date','2026-01-02T00:00:00Z');")
    delta_state = fingerprint(binary, delta)
    if delta_state["summary"] != expected_summary(rows, delta_rows):
        raise ValueError(f"invalid delta fixture: {delta_state}")
    changed = set(changed_ids(rows, delta_rows))
    for db, modified in ((base, False), (delta, True)):
        ids = sorted({1, rows, rows // 2, *list(sorted(changed))[:2]})
        query = f"SELECT id,hex(payload) FROM t WHERE id IN ({','.join(map(str, ids))}) ORDER BY id;"
        expected = [f"{i}|{payload_bytes(seed, i, int(modified and i in changed)).hex().upper()}"
                    for i in ids]
        if sql(binary, db, query).splitlines() != expected:
            raise ValueError(f"fixture payload does not match seed: {db}")
    return {"base": base_state, "delta": delta_state,
            "base_file_bytes": base.stat().st_size,
            "delta_file_bytes": delta.stat().st_size}


def parse_timing(output, name):
    lines = output.splitlines()
    if len(lines) != 4 or lines[0] != f"BEGIN {name}" or lines[1] != "0" or lines[3] != f"END {name}":
        raise ValueError(f"invalid benchmark result for {name}: {output}")
    match = TIMER.fullmatch(lines[2])
    if not match:
        raise ValueError(f"missing timing for {name}: {output}")
    real, user, system = map(float, match.groups())
    if not all(math.isfinite(value) and value >= 0 for value in (real, user, system)) or real <= 0:
        raise ValueError(f"invalid timing for {name}: {output}")
    return {"wall_us": max(1, round(real * 1_000_000)),
            "cpu_us": round((user + system) * 1_000_000)}


def timed_sql(binary, db, name, statement, cache_kib, metrics_extension=None):
    prefix = ""
    if metrics_extension:
        prefix = (f".load {json.dumps(str(metrics_extension), ensure_ascii=False)} sqlite3_remoteperfmetrics_init\n"
                  f".open {json.dumps(str(db), ensure_ascii=False)}\n")
    script = prefix + f""".headers off
.mode list
PRAGMA cache_size=-{cache_kib};
{"SELECT remote_perf_reset();" if metrics_extension else ""}
.print BEGIN {name}
.timer on
{statement}
.timer off
.print END {name}"""
    if metrics_extension:
        script += "\nSELECT remote_perf_metrics();"
    output = sql(binary, ":memory:" if metrics_extension else db, script)
    if not metrics_extension:
        return parse_timing(output, name)
    lines = output.splitlines()
    if len(lines) != 6 or lines[0] != "METRICS RESET":
        raise ValueError(f"missing file-remote metrics: {output}")
    values = lines[-1].split("|")
    if len(values) != len(METRICS) or any(not value.isdigit() for value in values):
        raise ValueError(f"invalid file-remote metrics: {lines[-1]}")
    metrics = dict(zip(METRICS, map(int, values)))
    if metrics["peak_rss_bytes"] <= 0:
        raise ValueError("missing file-remote peak memory")
    return {**parse_timing("\n".join(lines[1:-1]), name), **metrics}


def file_size(path):
    return path.stat().st_size if path.exists() else 0


def measure(binary, fixtures, manifest, root, name, rows, delta_rows, cache_kib, sample,
            metrics_extension=None):
    local, remote = root / "local.db", root / "remote.db"
    base, delta = manifest["base"], manifest["delta"]
    local_state = delta if name == "push_incremental" else base
    if name in ("copy_fsync", "clone_initial"):
        copy_synced(fixtures / "base.db", remote)
        verify(binary, remote, base)
        destination = root / "destination.db"
    else:
        copy_synced(fixtures / ("delta.db" if name == "push_incremental" else "base.db"), local)
        verify(binary, local, local_state)
        destination = remote if name.startswith("push") else local
        sql(binary, local, f"SELECT dolt_remote('add','origin',{literal(file_url(remote))});")
        if name != "push_initial":
            copy_synced(fixtures / "base.db", remote)
            verify(binary, remote, base)
        if name.startswith("fetch") or name == "pull_fast_forward":
            sql(binary, local, "SELECT dolt_fetch('origin','main');")
            verify(binary, local, base, base)
            if name != "fetch_noop":
                copy_synced(fixtures / "delta.db", remote)
                verify(binary, remote, delta)
    before = file_size(destination)
    if name == "copy_fsync":
        start, cpu = time.monotonic_ns(), time.process_time_ns()
        copy_synced(remote, destination)
        sample.update(wall_us=max(1, (time.monotonic_ns() - start) // 1000),
                      cpu_us=(time.process_time_ns() - cpu) // 1000)
    else:
        operation = (f"SELECT dolt_clone({literal(file_url(remote))});" if name == "clone_initial"
                     else "SELECT dolt_push('origin','main');" if name.startswith("push")
                     else "SELECT dolt_pull('origin','main');" if name == "pull_fast_forward"
                     else "SELECT dolt_fetch('origin','main');")
        sample.update(timed_sql(binary, destination if name == "clone_initial" else local,
                                name, operation, cache_kib, metrics_extension))
    sample["destination_growth_bytes"] = file_size(destination) - before
    sample["logical_payload_bytes"] = (0 if name.endswith("noop") else
                                       delta_rows * PAYLOAD_BYTES if name in
                                       ("push_incremental", "fetch_incremental", "pull_fast_forward")
                                       else rows * PAYLOAD_BYTES)
    if name in ("copy_fsync", "clone_initial"):
        verify(binary, destination, base)
        verify(binary, remote, base)
    elif name.startswith("push"):
        verify(binary, local, local_state)
        verify(binary, remote, local_state)
    else:
        remote_state = base if name == "fetch_noop" else delta
        verify(binary, local, delta if name == "pull_fast_forward" else base, remote_state)
        verify(binary, remote, remote_state)
    sample["verified"] = True


def binary_info(binary):
    output = sql(binary, ":memory:", "SELECT doltlite_engine(); SELECT dolt_version();").splitlines()
    if len(output) != 2 or output[0] != "prolly":
        raise ValueError(f"not a DoltLite prolly engine: {binary}: {output}")
    return {"path": str(binary), "version": output[1],
            "sha256": hashlib.sha256(binary.read_bytes()).hexdigest()}


def gate_rows(result):
    rows = []
    for case, ceiling in CEILINGS_US.items():
        times = {arm: round(statistics.median(s["wall_us"] for s in result["samples"]
                                             if s["case"] == case and s["arm"] == arm))
                 for arm in result["binaries"]}
        rows.append((case, times.get("baseline", ceiling), times["candidate"]))
    return rows


def write_benchmark_results(result):
    rows = gate_rows(result)
    Path(os.environ["BENCH_RESULTS_OUTPUT"]).write_text("".join(
        f"file_remote\t{case}\t{reference}\t{candidate}\n"
        for case, reference, candidate in rows))
    by_case = {(s["case"], s["arm"], s["trial"]): s["wall_us"] for s in result["samples"]}
    with Path(os.environ["BENCH_SAMPLES_OUTPUT"]).open("w") as stream:
        stream.write("section\ttest\trun\tbaseline_us\tcandidate_us\n")
        for case in CEILINGS_US:
            for trial in range(1, result["configuration"]["runs"]+1):
                baseline = by_case.get((case, "baseline", trial), 0)
                candidate = by_case[case, "candidate", trial]
                stream.write(f"file_remote\t{case}\t{trial}\t{baseline}\t{candidate}\n")


def read_gated_results(path, mode=None):
    import benchmark_compare as compare
    result = read_results(path)
    recorded_mode = result["configuration"].get("gate_mode")
    if recorded_mode not in ("relative", "absolute") or (mode and recorded_mode != mode):
        raise ValueError("missing or mismatched file-remote gate mode")
    if result["configuration"]["rows"] * PAYLOAD_BYTES != 100_000_000:
        raise ValueError("file-remote gates require the 100 MB fixture")
    if result["status"] != "passed":
        raise ValueError("unverified file-remote measurements cannot be gated")
    expected_arms = {"baseline", "candidate"} if recorded_mode == "relative" else {"candidate"}
    if set(result["binaries"]) != expected_arms:
        raise ValueError("file-remote gate binaries do not match the gate mode")
    directory = Path(path).parent
    results, metadata = compare.parse_input_artifact(f"remote={directory / 'remote.tsv'}")
    actual = {(r.section, r.test, r.baseline_us, r.candidate_us) for r in results}
    if actual != {("file_remote", *row) for row in gate_rows(result)} or len(results) != len(CEILINGS_US):
        raise ValueError("file-remote gate results do not match raw measurements")
    _, history = compare.parse_attempt_history(f"remote={directory / 'remote-attempts.tsv'}")
    relative = recorded_mode == "relative"
    analysis = compare.analyze(results, RELATIVE_RATIO if relative else 1.0,
                               AGGREGATE_RATIO if relative else 1000,
                               MIN_DELTA_US if relative else 0)
    analysis = compare.apply_retry_confirmation(results, analysis, {"remote": history},
                                                {"remote": metadata})
    result["gates"] = {
        "failed": analysis["failed"], "attempts": len(history.failures),
        "confirmed": [list(gate) for gate in sorted(history.confirmed_failures)],
        "failures": sorted(key[2] for key in analysis["individual_failures"]),
        "transient": sorted(key[2] for key in analysis["observed_individual_failures"]
                            - analysis["individual_failures"]),
    }
    return result


def gates_pass(result):
    return result["status"] == "passed" and not result.get("gates", {}).get("failed", False)


def gate_status(result, case):
    if "gates" not in result:
        return "MEASURED"
    if case == "copy_fsync":
        return "CONTROL"
    if case in result["gates"]["failures"]:
        return "FAIL"
    if case in result["gates"]["transient"]:
        return "TRANSIENT"
    return "PASS"


def report(result):
    config = result["configuration"]
    lines = ["# File remote performance", "",
             f"Status: {'passed' if gates_pass(result) else 'failed'}. Transport: local file-backed remote.", "",
             f"{config['rows'] * PAYLOAD_BYTES:,} logical payload bytes; "
             f"{config['rows']:,} rows; {config['delta_rows']:,} changed rows; "
             f"{config['runs']} repetitions per operation and binary.", "",
             "SQL timings exclude fixture generation, copies, database open and verification. "
             "Each SQL sample uses a fresh process with the OS cache primed by setup verification. "
             f"SQL client cache: {config['cache_kib']:,} KiB. "
             "copy_fsync times shutil.copyfile plus fsync.", "",
             "Throughput uses logical payload bytes; destination growth is file-size change, "
             "not transferred bytes.", ""]
    mode = config.get("gate_mode")
    if mode == "relative":
        lines += ["Timing gates: >25% and >5 ms per operation, or >15% aggregate; "
                  "the same gate must fail three attempts. copy_fsync is a control.", ""]
    elif mode == "absolute":
        lines += ["Nightly median ceilings require three-attempt confirmation; copy_fsync is a control.", "",
                  "| Operation | Ceiling ms | Result |", "|---|---:|---|"]
        lines += [f"| {case} | {ceiling / 1000:g} | {gate_status(result, case)} |"
                  for case, ceiling in CEILINGS_US.items()]
        lines.append("")
    else:
        lines += ["No speed thresholds are enforced.", ""]
    if "gates" in result:
        lines += [f"Timing gate: {'PASS' if gates_pass(result) else 'FAIL'} "
                  f"after {result['gates']['attempts']} attempt(s).", ""]
    for arm, info in result["binaries"].items():
        lines += [f"{arm}: {info['version']}."]
    if len(result["binaries"]) > 1:
        lines += ["Baseline/candidate order alternates each repetition; both use the same fixtures."]
    lines.append("")
    if "fixture" in result:
        lines += [f"Fixture file bytes: base {result['fixture']['base_file_bytes']:,}; "
                  f"delta {result['fixture']['delta_file_bytes']:,}.", ""]
    lines += ["| Operation | Binary | Median ms | Min–max ms | CPU ms | Payload MB/s | Destination growth bytes |",
              "|---|---|---:|---:|---:|---:|---:|"]
    for name in CASES:
        for arm in result["binaries"]:
            samples = [s for s in result["samples"]
                       if s["case"] == name and s["arm"] == arm and s["verified"]]
            if not samples:
                continue
            times = [s["wall_us"] for s in samples]
            median = statistics.median(times)
            throughput = samples[0]["logical_payload_bytes"] / median
            rate = f"{throughput:.2f}" if throughput else "—"
            lines.append(f"| {name} | {arm} | {median / 1000:.3f} | "
                         f"{min(times) / 1000:.3f}–{max(times) / 1000:.3f} | "
                         f"{statistics.median(s['cpu_us'] for s in samples) / 1000:.3f} | {rate} | "
                         f"{statistics.median(s['destination_growth_bytes'] for s in samples):.0f} |")
    if config.get("metrics"):
        lines += ["", "<details>", "<summary>Memory and file I/O</summary>", "",
                  "Successful VFS bytes and VFS requests cover local and remote files during the operation; "
                  "they include OS-cache hits and are not physical disk traffic. "
                  "Peak RSS covers the fresh SQL process, including database open. "
                  "The Python copy control is not instrumented.", "",
                  "| Operation | Binary | Peak RSS MiB | Read MB | Written MB | Read requests | Write requests |",
                  "|---|---|---:|---:|---:|---:|---:|"]
        for name in CASES[1:]:
            for arm in result["binaries"]:
                samples = [s for s in result["samples"]
                           if s["case"] == name and s["arm"] == arm and s["verified"]]
                if not samples:
                    continue
                values = {key: statistics.median(s[key] for s in samples) for key in METRICS}
                lines.append(f"| {name} | {arm} | {values['peak_rss_bytes'] / 1048576:.2f} | "
                             f"{values['read_bytes'] / 1e6:.3f} | {values['write_bytes'] / 1e6:.3f} | "
                             f"{values['read_calls']:g} | {values['write_calls']:g} |")
        lines += ["", "</details>", ""]
    if result.get("error"):
        lines += ["", f"Error: {result['error']}"]
    return "\n".join(lines) + "\n"


def save_results(result, output):
    output.mkdir(parents=True, exist_ok=True)
    (output / "results.json").write_text(json.dumps(result, indent=2) + "\n")
    (output / "report.md").write_text(report(result))
    with (output / "samples.tsv").open("w", newline="") as stream:
        fields = ("case", "arm", "trial", "wall_us", "cpu_us", "logical_payload_bytes",
                  "destination_growth_bytes", "verified")
        if result["configuration"].get("metrics"):
            fields += METRICS
        writer = csv.DictWriter(stream, fields, delimiter="\t")
        writer.writeheader()
        writer.writerows(result["samples"])


def read_results(path):
    try:
        result = json.loads(Path(path).read_text())
        if result["format_version"] != 1 or result["status"] not in ("passed", "failed"):
            raise ValueError("invalid format or status")
        if result["status"] == "passed":
            runs = result["configuration"]["runs"]
            arms = set(result["binaries"])
            if type(runs) is not int or runs < 1 or arms not in (
                    {"candidate"}, {"baseline", "candidate"}):
                raise ValueError("invalid runs or binaries")
            expected = {(case, arm, trial) for case in CASES for arm in arms
                        for trial in range(1, runs + 1)}
            samples = result["samples"]
            actual = {(s["case"], s["arm"], s["trial"]) for s in samples}
            if actual != expected or len(samples) != len(expected):
                raise ValueError("incomplete or duplicate samples")
            if any(s["verified"] is not True for s in samples) or result.get("error"):
                raise ValueError("unverified samples or benchmark error")
        for sample in result["samples"]:
            if not sample["verified"]:
                continue
            for field in ("wall_us", "cpu_us", "logical_payload_bytes",
                          "destination_growth_bytes"):
                value = sample[field]
                if not isinstance(value, (int, float)) or not math.isfinite(value):
                    raise ValueError(f"invalid {field}")
            if sample["wall_us"] <= 0 or sample["cpu_us"] < 0:
                raise ValueError("invalid sample timing")
            if result["configuration"].get("metrics") and sample["case"] != "copy_fsync":
                if any(type(sample[key]) is not int or sample[key] < 0 for key in METRICS):
                    raise ValueError("invalid file-remote resource metrics")
                if sample["peak_rss_bytes"] == 0:
                    raise ValueError("missing file-remote peak memory")
        report(result)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise ValueError(f"invalid file-remote results: {path}: {exc}") from exc
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description="Measure verified file-backed DoltLite remote operations.")
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--size-mb", type=int, default=100, help="logical payload size in decimal MB")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--seed", type=int, default=20261024)
    parser.add_argument("--delta-rows", type=int, help="default: one percent of rows")
    parser.add_argument("--cache-kib", type=int, default=65536)
    parser.add_argument("--metrics-extension", type=Path)
    parser.add_argument("--work-dir", type=Path, help="parent for temporary fixtures and sample databases")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--benchmark-output", action="store_true")
    args = parser.parse_args(argv)
    if args.benchmark_output:
        if args.size_mb != 100 or not all(os.environ.get(key) for key in (
                "BENCH_RESULTS_OUTPUT", "BENCH_SAMPLES_OUTPUT")):
            parser.error("--benchmark-output requires the 100 MB fixture and benchmark_retry outputs")
        args.output_dir = Path(os.environ["BENCH_RESULTS_OUTPUT"]).parent
    elif args.output_dir is None:
        parser.error("--output-dir is required")
    if args.metrics_extension:
        args.metrics_extension = args.metrics_extension.resolve()
        if not args.metrics_extension.is_file():
            parser.error("--metrics-extension must name a built metrics extension")
    if args.work_dir:
        args.work_dir = args.work_dir.resolve()
    rows = args.size_mb * 1_000_000 // PAYLOAD_BYTES
    delta_rows = args.delta_rows if args.delta_rows is not None else max(1, rows // 100)
    if min(args.size_mb, args.runs, args.cache_kib, delta_rows) < 1 or delta_rows > rows:
        parser.error("size, runs, cache and delta rows must be positive; delta rows cannot exceed rows")
    binaries = {"candidate": args.candidate.resolve()}
    if args.baseline:
        binaries = {"baseline": args.baseline.resolve(), **binaries}
    result = {"format_version": 1, "status": "failed", "samples": [], "binaries": {},
              "configuration": {"size_mb": args.size_mb, "rows": rows, "payload_bytes": PAYLOAD_BYTES,
                                "delta_rows": delta_rows, "seed": args.seed, "runs": args.runs,
                                "cache_kib": args.cache_kib, "platform": platform.platform(),
                                "work_dir": str(args.work_dir.resolve()) if args.work_dir else tempfile.gettempdir()}}
    if args.benchmark_output:
        result["configuration"]["gate_mode"] = "relative" if args.baseline else "absolute"
    if args.metrics_extension:
        result["configuration"]["metrics"] = "vfs"
    try:
        result["binaries"] = {arm: binary_info(binary) for arm, binary in binaries.items()}
        with tempfile.TemporaryDirectory(prefix="doltlite-remote-perf-", dir=args.work_dir) as work:
            root = Path(work)
            fixtures = root / "fixtures"
            fixtures.mkdir()
            started = time.monotonic()
            result["fixture"] = prepare_fixtures(args.candidate.resolve(), fixtures, rows, delta_rows, args.seed)
            result["fixture_seconds"] = time.monotonic() - started
            for name in CASES:
                for trial in range(1, args.runs + 1):
                    order = list(binaries) if trial % 2 else list(reversed(binaries))
                    for arm in order:
                        sample = {"case": name, "arm": arm, "trial": trial, "verified": False}
                        result["samples"].append(sample)
                        with tempfile.TemporaryDirectory(prefix=f"{name}-{arm}-", dir=root) as sample_dir:
                            measure(binaries[arm], fixtures, result["fixture"], Path(sample_dir),
                                    name, rows, delta_rows, args.cache_kib, sample,
                                    metrics_extension=args.metrics_extension)
                        print(f"{name} {arm} {trial}/{args.runs}: {sample['wall_us'] / 1000:.3f} ms (verified)",
                              file=sys.stderr)
            result["status"] = "passed"
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as exc:
        result["error"] = str(exc)
    save_results(result, args.output_dir)
    if args.benchmark_output and result["status"] == "passed":
        write_benchmark_results(result)
    print(report(result), end="")
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
