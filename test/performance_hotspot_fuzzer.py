#!/usr/bin/env python3

import argparse
from dataclasses import asdict, dataclass, field, replace
import hashlib
import json
import math
from pathlib import Path
import platform
import random
import re
import statistics
import shutil
import subprocess
import tempfile
import time

from performance_hotspot_search import CHOICES, Search, VERSION, family_fingerprint, fingerprint, statement_fingerprint

TEST_DIR = Path(__file__).resolve().parent
TIMER = re.compile(r"Run Time: real ([0-9.]+) user [0-9.]+ sys [0-9.]+")


@dataclass(frozen=True)
class Profile:
    rows: int
    payload: int
    key: str
    groups: int
    skew: bool
    cache_kib: int
    stride: int
    lookups: int
    target: int
    start: int
    width: int
    memory: bool = False


@dataclass(frozen=True)
class Case:
    name: str
    sql: str
    verify: str = ""
    prepare: str = ""
    recipe: dict = field(default_factory=dict)
    warmup: str = ""


def profile_for(seed, index):
    rng = random.Random(f"hotspots-v1:{seed}:{index}")
    rows = rng.choice([16384, 65536, 262144])
    groups = rng.choice([16, 256, 4096])
    width = max(1, rows // rng.choice([4, 16, 128]))
    profile = Profile(rows, rng.choice([32, 256, 1024]),
                   rng.choice(["integer", "text"]), groups,
                   rng.choice([False, True]), rng.choice([4096, 16384, 65536]),
                   rng.choice([2, 8, 32]), rng.choice([1000, 10000]),
                   rng.randrange(groups), rng.randint(1, rows-width), width)
    return replace(profile, memory=rng.randrange(4)==0)


def key_sql(p, value):
    return str(value) if p.key == "integer" else f"printf('%016x', {value})"


def fixture_sql(p):
    group = f"i%{p.groups}"
    if p.skew:
        group = f"CASE WHEN i%10<9 THEN 0 ELSE {group} END"
    parts = [f"CREATE TABLE t(id {p.key.upper()} PRIMARY KEY, seq INTEGER NOT NULL, "
             "grp INTEGER NOT NULL, v INTEGER NOT NULL, tag TEXT NOT NULL, "
             "payload BLOB NOT NULL);", "BEGIN;"]
    for first in range(1, p.rows+1, 4096):
        last = min(p.rows, first+4095)
        parts.append(f"WITH RECURSIVE c(i) AS (VALUES({first}) UNION ALL "
                     f"SELECT i+1 FROM c WHERE i<{last}) INSERT INTO t SELECT "
                     f"{key_sql(p, 'i')},i,{group},(i*7919)%1000000,"
                     f"printf('tag-%08x',i%10000),"
                     f"CAST(printf('%0{p.payload}d',i) AS BLOB) FROM c;")
    parts += ["COMMIT;", "CREATE INDEX t_g ON t(grp);",
              "CREATE INDEX t_gv ON t(grp,v);", "ANALYZE;"]
    return "\n".join(parts) + "\n"


def cases_for(p):
    lo, hi = key_sql(p, p.start), key_sql(p, p.start+p.width-1)
    lookup = key_sql(p, f"1+(c.i*2654435761)%{p.rows}")
    points = (f"WITH RECURSIVE c(i) AS (VALUES(1) UNION ALL SELECT i+1 "
              f"FROM c WHERE i<{p.lookups}) SELECT ")
    verify = "SELECT count(*),sum(v),sum(seq),sum(length(payload)) FROM t;"
    return [
        Case("scan_payload", "SELECT sum(length(payload)),sum(v) FROM t NOT INDEXED;"),
        Case("scan_filter", f"SELECT sum(v) FROM t NOT INDEXED WHERE grp={p.target};"),
        Case("range_pk", f"SELECT sum(v),sum(length(payload)) FROM t WHERE id BETWEEN {lo} AND {hi};"),
        Case("point_pk", points + f"sum((SELECT v FROM t WHERE id={lookup})) FROM c;"),
        Case("point_payload", points + f"sum((SELECT length(payload) FROM t WHERE id={lookup})) FROM c;"),
        Case("index_cover", f"SELECT sum(v) FROM t INDEXED BY t_gv WHERE grp={p.target};"),
        Case("index_fetch", f"SELECT sum(length(payload)),sum(v) FROM t INDEXED BY t_g "
             f"WHERE grp BETWEEN {p.target} AND {min(p.groups-1, p.target+max(1,p.groups//16))};"),
        Case("reverse_scan", f"SELECT sum(v) FROM (SELECT v FROM t ORDER BY id DESC LIMIT {p.rows//2});"),
        Case("group_aggregate", "SELECT count(*),sum(s) FROM (SELECT grp,sum(v) s FROM t GROUP BY grp);"),
        Case("distinct", "SELECT count(DISTINCT tag) FROM t;"),
        Case("sort_limit", f"SELECT sum(seq) FROM (SELECT seq FROM t ORDER BY v,seq LIMIT {p.width});"),
        Case("window", "SELECT sum(r) FROM (SELECT row_number() OVER (PARTITION BY grp ORDER BY v) r FROM t);"),
        Case("join_pk", f"SELECT sum(b.v) FROM t a JOIN t b ON b.id=a.id WHERE a.seq%{p.stride}=0;"),
        Case("update_batch", f"UPDATE t SET v=v+1 WHERE seq%{p.stride}=0;", verify),
        Case("delete_batch", f"DELETE FROM t WHERE seq%{p.stride}=0;", verify),
        Case("create_index", "CREATE INDEX probe ON t(tag);", verify),
        Case("add_column_default", "ALTER TABLE t ADD COLUMN z INTEGER NOT NULL DEFAULT 7;",
             "SELECT count(*),sum(z) FROM t;"),
    ]


def prologue(cache_kib):
    return (".bail on\n.headers off\n.mode list\n.nullvalue NULL\n.output /dev/null\n"
            f"PRAGMA mmap_size=0;\nPRAGMA cache_size=-{cache_kib};\n"
            "PRAGMA temp_store=MEMORY;\n.output stdout\n"
            "SELECT name FROM sqlite_schema WHERE 0;\n")


def unit_sql(case, timed):
    parts = [".output /dev/null", case.warmup, ".output stdout"] if case.warmup else []
    if case.verify or case.prepare:
        parts.append("BEGIN;")
    if case.prepare:
        parts += [".output /dev/null", case.prepare, ".output stdout"]
    if timed:
        parts.append(".timer on")
    parts.append(case.sql)
    if timed:
        parts.append(".timer off")
    if case.verify:
        parts.append(case.verify)
    if case.verify or case.prepare:
        parts.append("ROLLBACK;")
    return "\n".join(parts) + "\n"


def session_sql(p, case, repeats, setup=None):
    prefix = ""
    if p.memory:
        if setup is None:
            raise ValueError("in-memory measurement requires fixture SQL")
        prefix = prologue(p.cache_kib)+".output /dev/null\n"+setup+"\n.output stdout\n"
    return (prefix + prologue(p.cache_kib) + ".print WARM\n" + unit_sql(case, False)
            + ".print MEASURE\n" + unit_sql(case, True)*repeats + ".print END\n")


def parse_measurement(output, repeats):
    lines = output.splitlines()
    if (len(lines) < 5 or lines[0] != "WARM" or lines[-1] != "END"
            or "MEASURE" not in lines):
        raise ValueError(f"invalid measurement framing: {output[:2000]}")
    start = lines.index("MEASURE")
    expected = lines[1:start]
    values, times = [], []
    for line in lines[start+1:-1]:
        match = TIMER.fullmatch(line)
        if match:
            value = float(match[1])
            if not math.isfinite(value) or value < 0:
                raise ValueError("invalid timer")
            times.append(value * 1000)
        else:
            values.append(line)
    if not expected or len(times) != repeats or values != expected*repeats:
        raise ValueError(f"missing timings or unstable results: {output[:2000]}")
    return {"ms": sum(times), "result": "\n".join(expected)}


class BudgetExpired(Exception):
    pass


class CaseTimeout(RuntimeError):
    pass


class Runner:
    def __init__(self, seconds, timeout):
        self.deadline = time.monotonic() + seconds
        self.timeout = timeout
        self.case_deadline = None

    def run(self, command, sql=None, setup=False):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise BudgetExpired()
        if self.case_deadline is not None:
            remaining = min(remaining, self.case_deadline - time.monotonic())
            if remaining <= 0:
                raise CaseTimeout(f"case exceeded its {self.timeout}s budget")
        try:
            result = subprocess.run(command, input=sql, text=True, capture_output=True,
                                    timeout=min(remaining, max(180, self.timeout) if setup else self.timeout))
        except subprocess.TimeoutExpired as exc:
            if time.monotonic() >= self.deadline:
                raise BudgetExpired() from exc
            raise CaseTimeout(f"command timed out: {command}") from exc
        if result.returncode or result.stderr.strip():
            raise RuntimeError(f"command failed: {command}\n{result.stdout[-2000:]}\n{result.stderr[-2000:]}")
        return result.stdout

    def measure(self, binary, db, p, case, repeats, setup=None):
        if p.memory:
            db = ":memory:"
        return parse_measurement(self.run([str(binary), str(db)], session_sql(p, case, repeats, setup)), repeats)


def classify(pairs, threshold, min_ms, runs):
    if len(pairs) < runs:
        return False
    return all(x["sqlite_ms"] > 0
               and x["doltlite_ms"] >= threshold*max(x["sqlite_ms"], min_ms)
               for x in pairs)


def pair_order(index):
    return ("sqlite", "doltlite") if index % 2 == 0 else ("doltlite", "sqlite")


def measure_case(runner, binaries, databases, p, case, runs, threshold, min_ms, setup=None):
    options = {'setup': setup} if p.memory else {}
    reference = runner.measure(binaries["sqlite"], databases["sqlite"], p, case, 1, **options)
    pilot = runner.measure(binaries["doltlite"], databases["doltlite"], p, case, 1, **options)
    if pilot["result"] != reference["result"]:
        raise ValueError(f"result mismatch: reference={reference}, pilot={pilot}")
    repeats = min(1024, max(1, math.ceil(min_ms*2.5 / max(reference["ms"], 0.01))),
                  max(1, int(1000/max(reference["ms"], pilot["ms"], 0.01))))
    pairs = []
    for i in range(runs+1):
        measurements = {}
        for arm in pair_order(i):
            measurements[arm] = runner.measure(binaries[arm], databases[arm], p, case, repeats, **options)
        if any(m["result"] != reference["result"] for m in measurements.values()):
            raise ValueError(f"result mismatch: reference={reference}, measurements={measurements}")
        pair = {arm+"_ms": measurements[arm]["ms"] for arm in binaries}
        if i == 0:
            screen = pair
            if pair["sqlite_ms"] <= 0 or pair["doltlite_ms"] < threshold*0.8*pair["sqlite_ms"]:
                break
        else:
            pairs.append(pair)
    samples = pairs or [screen]
    ratios = [x["doltlite_ms"]/x["sqlite_ms"] for x in samples if x["sqlite_ms"] > 0]
    return {"repeats": repeats, "screen": screen, "pairs": pairs,
            "result": reference["result"], "ratio": statistics.median(ratios) if ratios else None,
            "doltlite_ms": statistics.median(x["doltlite_ms"] for x in samples)/repeats,
            "sqlite_ms": statistics.median(x["sqlite_ms"] for x in samples)/repeats,
            "confirmed": classify(pairs, threshold, min_ms, runs)}


def binary_info(runner, path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024*1024), b""):
            digest.update(block)
    return {"path": str(path), "sha256": digest.hexdigest(),
            "version": runner.run([str(path), ":memory:", "SELECT sqlite_version(),sqlite_source_id();"]).strip()}


def save_report(output, report):
    (output/"results.json").write_text(json.dumps(report, indent=2) + "\n")
    good = sorted((x for x in report["cases"] if x.get("confirmed")), key=lambda x: -x["ratio"])
    errors = [x for x in report["cases"] if "error" in x]
    timeouts = [x for x in report["cases"] if "timeout" in x]
    lines = ["## Performance hotspot discovery", "",
             f"Seed: `{report['seed']}`. Profiles completed: {report['profiles_completed']}. "
             f"Cases measured: {sum('pairs' in x for x in report['cases'])}/{len(report['cases'])} attempted. "
             f"Search status: **{report['status']}**.", "",
             f"Confirmed means every one of {report['runs']} confirmation pairs was at least "
             f"{report['threshold']:g}× slower. For confirmation, SQLite timings below "
             f"{report['min_ms']:g} ms per batch are conservatively raised to that floor.",
             "Fresh connections, one untimed warm-up, identical byte cache budgets, mmap disabled. "
             "All writes use explicit transactions and roll back; setup and rollback are untimed.", "",
             "| Case | Configuration | DoltLite ms | SQLite ms | Median ratio | Minimum pair ratio | Reproducer |",
             "| --- | --- | ---: | ---: | ---: | ---: | --- |"]
    for case in good:
        low = min(x["doltlite_ms"]/x["sqlite_ms"] for x in case["pairs"])
        p = case["profile"]
        config = f"{p['rows']:,} rows; {p['key']} PK; {p['payload']} B payload; {p['cache_kib']//1024} MiB cache"
        config += "; in-memory" if p.get('memory', False) else "; file-backed"
        lines.append(f"| {case['id']} | {config} | {case['doltlite_ms']:.3f} | {case['sqlite_ms']:.3f} | "
                     f"{case['ratio']:.2f}× | {low:.2f}× | `{case['reproducer']}` |")
    if not good:
        lines += ["", "No confirmed hotspots in the completed cases."]
    lines += ["", "Per-query times above exclude repetition; raw batch samples, plans, fixture SQL, "
              "and measured SQL are in the artifact.", "",
              "Replay a case with the same two binaries:", "```sh",
              "python3 test/performance_hotspot_fuzzer.py --doltlite build/doltlite "
              "--sqlite build-stockref/sqlite3 --replay ARTIFACT/hotspot-discovery/p000/CASE.json --output replay-results",
              "```"]
    if timeouts:
        lines += ["", "### Timed out (unconfirmed)", ""]
        for case in timeouts:
            lines.append(f"- `{case['id']}`: `{case['reproducer']}`. {case['timeout']}")
    for case in errors:
        lines += ["", f"**Error: {case['id']}**", "```text", case["error"], "```"]
    (output/"summary.md").write_text("\n".join(lines) + "\n")


def positive(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def main(argv=None):
    parser = argparse.ArgumentParser(description="Seeded, paired DoltLite/SQLite hotspot discovery (no autocommit writes).")
    parser.add_argument("--doltlite", required=True, type=Path)
    parser.add_argument("--sqlite", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--seed", type=int, default=20260922)
    parser.add_argument("--search", action="store_true", help="generate SQL until the time budget expires")
    parser.add_argument("--operator", action="append", choices=CHOICES['operator'],
                        help="restrict generated search to these operators; may be repeated")
    parser.add_argument("--nightly-seeds", action="store_true", help="revisit retired PR workloads before random search")
    parser.add_argument("--history", type=Path)
    parser.add_argument("--issues", type=Path)
    parser.add_argument("--profiles", type=positive, default=12)
    parser.add_argument("--profile", type=int)
    parser.add_argument("--case", choices=[x.name for x in cases_for(profile_for(0, 0))])
    parser.add_argument("--replay", type=Path)
    parser.add_argument("--runs", type=positive, default=5)
    parser.add_argument("--seconds", type=positive, default=1800)
    parser.add_argument("--timeout", type=positive, default=60, help="seconds per case, including warm-up and confirmation")
    parser.add_argument("--min-ms", type=positive, default=20)
    args = parser.parse_args(argv)
    if args.runs < 5 or args.profiles > 128 or (args.profile is not None and args.profile < 0):
        parser.error("require at least 5 confirmation pairs, at most 128 profiles, and a nonnegative profile index")
    if args.search and (args.replay or args.profile is not None or args.case):
        parser.error("--search cannot be combined with replay/profile/case filters")
    if args.nightly_seeds and not args.search:
        parser.error("--nightly-seeds requires --search")
    if args.operator and not args.search:
        parser.error("--operator requires --search")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error("output directory must be empty")
    binaries = {"doltlite": args.doltlite.resolve(), "sqlite": args.sqlite.resolve()}
    issues = json.loads(args.issues.read_text()) if args.issues else []
    search = Search(args.seed, args.history, issues)
    runner = Runner(args.seconds, args.timeout)
    report = {"seed": args.seed, "runs": args.runs, "threshold": 3.0, "min_ms": args.min_ms,
              "generator_version": VERSION, "platform": platform.platform(),
              "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), "status": "running", "profiles_completed": 0, "cases": []}
    save_report(output, report)
    try:
        runner.run(["bash", str(TEST_DIR/"lib/assert_doltlite_engine.sh"), str(binaries["doltlite"])])
        runner.run(["bash", str(TEST_DIR/"assert_stock_reference.sh"), str(binaries["sqlite"]), str(binaries["doltlite"])])
        report["source_commit"] = runner.run(["git", "-C", str(TEST_DIR.parent), "rev-parse", "HEAD"]).strip()
        report["binaries"] = {arm: binary_info(runner, binary) for arm, binary in binaries.items()}
        indexes = [args.profile] if args.profile is not None else range(args.profiles)
        if args.replay:
            replay = json.loads(args.replay.read_text())
            report["seed"] = replay["seed"]
            if not re.fullmatch(r"[a-z][a-z0-9_]{0,80}", replay["case"]["name"]) or replay.get("setup", "setup.sql") != "setup.sql":
                raise ValueError("invalid reproducer")
            specs = [(0, Profile(**replay["profile"]), [Case(**replay["case"])],
                      replay["setup_sql"] if "setup_sql" in replay else (args.replay.parent/"setup.sql").read_text(), "replay")]
        elif args.search:
            specs = search.specs(args.seed, nightly_seeds=args.nightly_seeds, operators=args.operator)
        else:
            specs = []
            for index in indexes:
                profile = profile_for(args.seed, index)
                cases = [x for x in cases_for(profile) if not args.case or x.name == args.case]
                specs.append((index, profile, cases, fixture_sql(profile), "template"))
        for index, profile, cases, setup, origin in specs:
            if time.monotonic() >= runner.deadline:
                raise BudgetExpired()
            directory = output/f"p{index:03d}"
            directory.mkdir()
            (directory/"setup.sql").write_text(setup)
            with tempfile.TemporaryDirectory(prefix="doltlite-hotspots-") as tmp:
                databases = {arm: Path(tmp)/(arm+".db") for arm in binaries}
                try:
                    for arm, binary in binaries.items():
                        if not profile.memory:
                            runner.run([str(binary), str(databases[arm])], prologue(profile.cache_kib)+setup, setup=True)
                except CaseTimeout as exc:
                    # One slow fixture is a finding about that profile, not a
                    # reason to abandon the rest of the search.
                    record = {"id": f"p{index:03d}/setup", "profile": asdict(profile),
                              "reproducer": f"p{index:03d}/setup.sql", "timeout": str(exc)}
                    report["cases"].append(record)
                    save_report(output, report)
                    print(record["id"], "TIMEOUT (setup; profile skipped)", flush=True)
                    continue
                for case in cases:
                    with tempfile.TemporaryDirectory(prefix="case-", dir=tmp) as scratch:
                        case_databases = {arm: Path(scratch)/(arm+".db") for arm in binaries}
                        for arm in binaries:
                            if not profile.memory:
                                shutil.copyfile(databases[arm], case_databases[arm])
                        record = {"id": f"p{index:03d}/{case.name}", "profile": asdict(profile),
                                  "reproducer": f"p{index:03d}/{case.name}.json",
                                  "origin": "fresh" if args.search and case.name.startswith("generated_") and case.name != "generated_0" else origin}
                        repro = {"seed": report["seed"], "generator_version": VERSION, "profile": asdict(profile),
                                 "case": asdict(case), "setup": "setup.sql", "setup_sql": setup}
                        (directory/(case.name+".json")).write_text(json.dumps(repro, indent=2)+"\n")
                        (directory/(case.name+".sql")).write_text(session_sql(profile, case, 1, setup))
                        runner.case_deadline = time.monotonic() + args.timeout
                        try:
                            plan_setup = ".output /dev/null\n"+setup+"\n.output stdout\n" if profile.memory else ""
                            record["plans"] = {arm: runner.run([str(binary), ":memory:" if profile.memory else str(case_databases[arm])],
                                prologue(profile.cache_kib)+plan_setup+"EXPLAIN QUERY PLAN "+case.sql) for arm, binary in binaries.items()}
                            record["family"] = family_fingerprint(profile, case, record["plans"])
                            record["statement"] = statement_fingerprint(case)
                            record["fingerprint"] = fingerprint(profile, case, record["plans"])
                            options = {'setup': setup} if profile.memory else {}
                            record.update(measure_case(runner, binaries, case_databases, profile, case, args.runs, 3.0, args.min_ms, **options))
                            (directory/(case.name+".sql")).write_text(session_sql(profile, case, record["repeats"], setup))
                            repro.update(expected=record["result"], repeats=record["repeats"], fingerprint=record["fingerprint"], family=record["family"], statement=record["statement"])
                            (directory/(case.name+".json")).write_text(json.dumps(repro, indent=2)+"\n")
                        except BudgetExpired:
                            record["incomplete"] = "search budget exhausted during confirmation"
                            report["cases"].append(record)
                            raise
                        except CaseTimeout as exc:
                            record["timeout"] = str(exc)
                        except (RuntimeError, ValueError) as exc:
                            record["error"] = str(exc)
                        finally:
                            runner.case_deadline = None
                        search.observe(profile, case, record)
                        report["cases"].append(record)
                        save_report(output, report)
                        print(record["id"], "ERROR" if "error" in record else "TIMEOUT (unconfirmed)" if "timeout" in record else
                              f"{record['ratio'] or 0:.2f}x {'CONFIRMED' if record['confirmed'] else 'screened'}", flush=True)
            report["profiles_completed"] += 1
        report["status"] = "complete"
    except BudgetExpired:
        report["status"] = "budget exhausted (partial search)"
    except (OSError, RuntimeError, ValueError, KeyError, TypeError) as exc:
        report["status"] = "error"
        report["cases"].append({"id": "setup", "error": str(exc)})
    if any("error" in case for case in report["cases"]):
        report["status"] = "error"
    save_report(output, report)
    return int(report["status"] == "error")


if __name__ == "__main__":
    raise SystemExit(main())
