#!/usr/bin/env python3
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
from urllib.parse import quote

from sql_dump_roundtrip_sweep import literal, query, run


CORPUS = ("hot-journal", "wal-frames", "wal-empty-main", "wal-torn-header",
          "truncated-tail",
          "header-8", "header-100", "zero-header-16", "zero-header-168",
          "zero-sector")
MODES = ("plain", "rw", "ro", "immutable", "attach")


def image(directory):
    return {p.name: p.read_bytes() for p in directory.iterdir() if p.is_file()}


def crash_writer(stock, database, sql):
    ready = database.parent / "ready"
    process = subprocess.Popen([str(stock), str(database)], stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        script = (".bail on\n" + sql + "\n.output " + json.dumps(str(ready))
                  + "\n.print ready\n.output stdout\n")
        process.stdin.write(script.encode())
        process.stdin.flush()
        deadline = time.monotonic() + 10
        while not ready.exists() or ready.read_bytes() != b"ready\n":
            if process.poll() is not None or time.monotonic() >= deadline:
                process.kill()
                _, stderr = process.communicate()
                raise RuntimeError("stock crash writer failed: " + stderr.decode())
            time.sleep(0.01)
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate()
        ready.unlink(missing_ok=True)


def make_corpus(stock, directory):
    seed = directory / "seed.db"
    data = ("CREATE TABLE corpus(id INTEGER PRIMARY KEY,payload BLOB);"
            "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100)"
            " INSERT INTO corpus SELECT x,zeroblob(4000) FROM n;")
    run(stock, seed, data)
    for case in CORPUS:
        target = directory / case
        target.mkdir()
        database = target / "test.db"
        shutil.copyfile(seed, database)
        if case == "hot-journal":
            crash_writer(stock, database,
                         "PRAGMA cache_size=5; BEGIN IMMEDIATE;"
                         "UPDATE corpus SET payload=randomblob(4000);")
            journal = target / "test.db-journal"
            if not journal.exists() or journal.read_bytes()[:8] == bytes(8):
                raise AssertionError("stock writer did not produce a hot journal")
        elif case == "wal-torn-header":
            crash_writer(stock, database, "PRAGMA journal_mode=WAL;"
                         "PRAGMA wal_autocheckpoint=0;"
                         "UPDATE corpus SET payload=randomblob(4000) WHERE id<=10;")
            if not (target / "test.db-wal").exists():
                raise AssertionError("stock writer did not produce WAL frames")
            database.write_bytes(bytes(100) + database.read_bytes()[100:])
        elif case.startswith("wal-"):
            database.unlink()
            crash_writer(stock, database, "PRAGMA journal_mode=WAL;"
                         "PRAGMA wal_autocheckpoint=0;" + data)
            if not (target / "test.db-wal").exists():
                raise AssertionError("stock writer did not produce WAL frames")
            if case == "wal-empty-main":
                database.write_bytes(b"")
        elif case == "truncated-tail":
            database.write_bytes(database.read_bytes()[:-2000])
        elif case.startswith("header-"):
            database.write_bytes(database.read_bytes()[:int(case.split("-")[1])])
        else:
            count = 512 if case == "zero-sector" else int(case.split("-")[-1])
            original = database.read_bytes()
            database.write_bytes(bytes(count) + original[count:])


def compare_image(stock_before, stock_after, candidate_after):
    if stock_before["test.db"] == stock_after["test.db"]:
        if candidate_after.get("test.db") != stock_before["test.db"]:
            raise AssertionError("DoltLite changed a file stock left unchanged")
    if stock_after.get("test.db") != candidate_after.get("test.db"):
        raise AssertionError("main database bytes differ from stock")
    if set(stock_after) != set(candidate_after):
        raise AssertionError("sidecars differ: stock=%s, DoltLite=%s" % (
            sorted(stock_after), sorted(candidate_after)))


def corpus_case(engine, stock, source, mode, directory):
    stock_dir, candidate_dir = directory / "stock", directory / "doltlite"
    shutil.copytree(source, stock_dir)
    shutil.copytree(source, candidate_dir)
    before = image(stock_dir)
    outputs = []
    for binary, path in ((stock, stock_dir), (engine, candidate_dir)):
        database = path / "test.db"
        sql = ("SELECT count(*),sum(id),sum(length(payload)) FROM corpus;"
               "PRAGMA integrity_check;")
        if mode == "attach":
            sql = ("ATTACH %s AS legacy;" % literal(database)
                   + sql.replace("FROM corpus", "FROM legacy.corpus")
                   .replace("PRAGMA integrity_check", "PRAGMA legacy.integrity_check"))
            database = ":memory:"
        elif mode != "plain":
            database = "file:" + quote(str(database), safe="/")
            database += "?immutable=1" if mode == "immutable" else "?mode=" + mode
        outputs.append(run(binary, database, sql, require_ok=False))
    compare_image(before, image(stock_dir), image(candidate_dir))
    if outputs[0] != outputs[1]:
        raise AssertionError("open results differ: stock=%r, DoltLite=%r" % tuple(outputs))


def vc_cases(engine, stock, directory):
    stock_file = directory / "stock.db"
    run(stock, stock_file, "CREATE TABLE kept(id INTEGER PRIMARY KEY,v);"
        "INSERT INTO kept VALUES(1,'leave me alone');")
    functions = query(engine, ":memory:", "SELECT DISTINCT name,narg FROM "
                      "pragma_function_list WHERE name LIKE 'dolt%' ORDER BY name,narg;")
    modules = query(engine, ":memory:", "SELECT name FROM pragma_module_list "
                    "WHERE name GLOB 'dolt_*' ORDER BY name;")
    for function in functions:
        for argc in ([0, 1, 2, 3] if function["narg"] < 0 else [function["narg"]]):
            for values in ([[]] if argc == 0 else
                           [["NULL"] * argc, (["'HEAD'", "'kept'"] + ["'probe'"] * argc)[:argc]]):
                label = "null" if not values or values[0] == "NULL" else "text"
                yield function["name"] + "/%d-%s" % (argc, label), "SELECT %s(%s);" % (
                    function["name"], ",".join(values)), stock_file
    for module in modules:
        name = module["name"]
        yield name + "/read", "SELECT * FROM {schema}.%s;" % name, stock_file
        yield name + "/insert", "INSERT INTO {schema}.%s DEFAULT VALUES;" % name, stock_file
        yield name + "/delete", "DELETE FROM {schema}.%s;" % name, stock_file
        cols = query(engine, ":memory:", "PRAGMA table_xinfo(%s);" % literal(name))
        for column in cols:
            if column["hidden"] == 0:
                field = '"' + column["name"].replace('"', '""') + '"'
                yield name + "/update", "UPDATE {schema}.%s SET %s=%s;" % (
                    name, field, field), stock_file
                break
        if name == "dolt_ignore":
            yield name + "/rule", ("INSERT INTO {schema}.dolt_ignore(pattern,ignored) "
                                   "VALUES('leave_me_alone',1);"), stock_file
        elif name == "dolt_docs":
            yield name + "/document", ("INSERT INTO {schema}.dolt_docs(doc_name,doc_text) "
                                       "VALUES('README.md','leave me alone');"), stock_file
        elif name == "dolt_nonlocal_tables":
            yield name + "/rule", ("INSERT INTO {schema}.dolt_nonlocal_tables(table_name,target_ref) "
                                   "VALUES('kept','main');"), stock_file


class Session:
    def __init__(self, binary, database, directory, name):
        self.out = directory / (name + ".out")
        self.err_path = directory / (name + ".err")
        self.err = open(self.err_path, "wb")
        self.seen = 0
        self.process = subprocess.Popen([str(binary), str(database)],
                                        stdin=subprocess.PIPE,
                                        stdout=subprocess.DEVNULL,
                                        stderr=self.err, cwd=directory)

    def step(self, sql):
        self.out.unlink(missing_ok=True)
        script = ".output %s\n%s\n.print __step__\n.output stdout\n" % (
            json.dumps(str(self.out)), sql)
        self.process.stdin.write(script.encode())
        self.process.stdin.flush()
        deadline = time.monotonic() + 20
        while not self.out.exists() or not self.out.read_bytes().endswith(b"__step__\n"):
            if self.process.poll() is not None or time.monotonic() >= deadline:
                raise RuntimeError("session stopped during: " + sql)
            time.sleep(0.01)
        errors = self.err_path.read_bytes()[self.seen:]
        self.seen += len(errors)
        return self.out.read_bytes()[:-len(b"__step__\n")] + b"!" + errors

    def close(self):
        if self.process.poll() is None:
            self.process.stdin.close()
            self.process.wait(timeout=20)
        self.err.close()


# Two writers on one stock file, one SQL statement block at a time: each
# takes the write lock in turn, the other reads and is refused meanwhile.
INTERLEAVE = (
    ("b", "BEGIN; INSERT INTO t VALUES(2,'b');"),
    ("a", "SELECT count(*) FROM t;"),
    ("a", "INSERT INTO t VALUES(3,'a');"),
    ("b", "COMMIT;"),
    ("a", "INSERT INTO t VALUES(3,'a'); SELECT group_concat(id) FROM t;"),
    ("a", "BEGIN IMMEDIATE; UPDATE t SET who='a2' WHERE id=2;"),
    ("b", "SELECT who FROM t WHERE id=2;"),
    ("b", "INSERT INTO t VALUES(4,'b');"),
    ("a", "COMMIT;"),
    ("b", "SELECT who FROM t WHERE id=2; INSERT INTO t VALUES(4,'b');"),
    ("b", "BEGIN; SELECT count(*) FROM t;"),
    ("a", "INSERT INTO t VALUES(5,'a');"),
    ("b", "SELECT count(*) FROM t; COMMIT; SELECT count(*) FROM t;"),
    ("a", "INSERT OR IGNORE INTO t VALUES(5,'a'); PRAGMA integrity_check;"),
)


def interleave_run(first, second, stock, journal, directory):
    directory.mkdir()
    database = directory / "test.db"
    run(stock, database, "PRAGMA journal_mode=%s; CREATE TABLE t(id INTEGER PRIMARY KEY,"
        "who TEXT); INSERT INTO t VALUES(1,'seed');" % journal)
    sessions = {"a": Session(first, database, directory, "a"),
                "b": Session(second, database, directory, "b")}
    try:
        transcript = [name.encode() + b":" + sessions[name].step(sql)
                      for name, sql in INTERLEAVE]
    finally:
        for session in sessions.values():
            session.close()
    for name in ("a.out", "b.out", "a.err", "b.err"):
        (directory / name).unlink()
    final = run(stock, database, "PRAGMA journal_mode; PRAGMA integrity_check;"
                "SELECT group_concat(id||who) FROM (SELECT * FROM t ORDER BY id);")
    return transcript, final, sorted(p.name for p in directory.iterdir())


def interleave_case(engine, stock, journal, doltlite_first, directory):
    expected = interleave_run(stock, stock, stock, journal, directory / "stock")
    pair = (engine, stock) if doltlite_first else (stock, engine)
    actual = interleave_run(pair[0], pair[1], stock, journal, directory / "doltlite")
    for step, (want, got) in enumerate(zip(expected[0], actual[0])):
        if want != got:
            raise AssertionError("step %d (%s) differs: stock=%r, DoltLite=%r" % (
                step + 1, INTERLEAVE[step][1], want, got))
    if expected[1] != actual[1]:
        raise AssertionError("final state differs: stock=%r, DoltLite=%r" % (
            expected[1], actual[1]))
    if expected[2] != actual[2]:
        raise AssertionError("files differ: stock=%s, DoltLite=%s" % (
            expected[2], actual[2]))


def vc_case(engine, source, sql, attached, directory):
    database = directory / "test.db"
    shutil.copyfile(source, database)
    before = image(directory)
    sql = sql.replace("{schema}", "legacy" if attached else "main")
    if attached:
        sql = "ATTACH %s AS legacy;" % literal(database) + sql
        target = ":memory:"
    else:
        target = database
    env = dict(os.environ, DOLTLITE_CREDS_DIR=str(directory / "creds"))
    run(engine, target, sql, require_ok=False, cwd=directory, env=env)
    if image(directory) != before:
        raise AssertionError("VC operation changed the stock file or left a sidecar")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("doltlite", type=Path)
    parser.add_argument("stock", type=Path)
    parser.add_argument("--corpus", nargs="+", choices=CORPUS + ("all",))
    parser.add_argument("--vc", action="store_true")
    parser.add_argument("--interleave", action="store_true")
    parser.add_argument("--save-dir", type=Path)
    args = parser.parse_args()
    if not args.corpus and not args.vc and not args.interleave:
        parser.error("select --corpus cases, --vc or --interleave")
    engine, stock = args.doltlite.resolve(), args.stock.resolve()
    subprocess.run(["bash", str(Path(__file__).with_name("assert_stock_reference.sh")),
                    str(stock), str(engine)], check=True)
    subprocess.run(["bash", str(Path(__file__).parent / "lib/assert_doltlite_engine.sh"),
                    str(engine)], check=True)
    passed = failed = 0
    def attempt(name, function):
        nonlocal passed, failed
        with tempfile.TemporaryDirectory(prefix="sqlite-file-case-") as tmp:
            path = Path(tmp)
            try:
                function(path)
                passed += 1
            except (AssertionError, RuntimeError, subprocess.TimeoutExpired, OSError) as error:
                failed += 1
                print("FAIL %s: %s" % (name, error), flush=True)
                if args.save_dir:
                    dest = args.save_dir / name.replace("/", "-")
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copytree(path, dest, dirs_exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="sqlite-file-corpus-") as tmp:
        root = Path(tmp)
        if args.corpus:
            make_corpus(stock, root)
            for case in (CORPUS if "all" in args.corpus else args.corpus):
                for mode in MODES:
                    attempt(case + "/" + mode, lambda path, c=case, m=mode:
                            corpus_case(engine, stock, root / c, m, path))
        if args.vc:
            for name, sql, source in vc_cases(engine, stock, root):
                for attached in (False, True):
                    mode = "attach" if attached else "main"
                    attempt(name + "/" + mode, lambda path, s=sql, a=attached:
                            vc_case(engine, source, s, a, path))
        if args.interleave:
            for journal in ("delete", "wal"):
                for doltlite_first in (False, True):
                    name = "interleave/%s/%s" % (
                        journal, "doltlite-first" if doltlite_first else "stock-first")
                    attempt(name, lambda path, j=journal, d=doltlite_first:
                            interleave_case(engine, stock, j, d, path))
    print("Results: %d passed, %d failed" % (passed, failed), flush=True)
    return int(failed != 0)


if __name__ == "__main__":
    raise SystemExit(main())
