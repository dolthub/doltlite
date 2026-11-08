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
    parser.add_argument("--save-dir", type=Path)
    args = parser.parse_args()
    if not args.corpus and not args.vc:
        parser.error("select --corpus cases or --vc")
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
    print("Results: %d passed, %d failed" % (passed, failed), flush=True)
    return int(failed != 0)


if __name__ == "__main__":
    raise SystemExit(main())
