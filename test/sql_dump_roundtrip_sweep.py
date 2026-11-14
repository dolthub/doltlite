#!/usr/bin/env python3
import argparse
import difflib
import json
from pathlib import Path
import random
import re
import shutil
import subprocess
import tempfile
import time

from sql_differential_fuzzer import Gen, GROUPS


def ident(value):
    return '"' + value.replace('"', '""') + '"'


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def run(binary, database, sql, require_ok=True, cwd=None, env=None):
    result = subprocess.run([str(binary), str(database)],
                            input=sql.encode() if isinstance(sql, str) else sql,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            timeout=90, cwd=cwd, env=env)
    if result.returncode != 0 and (require_ok or result.returncode != 1):
        raise RuntimeError("%s exited %d:\n%s" % (
            binary.name, result.returncode, result.stdout.decode(errors="replace")))
    return result.returncode, result.stdout.replace(b"\r\n", b"\n")


def query(binary, database, sql):
    _, output = run(binary, database, ".mode json\n" + sql + "\n")
    return json.loads(output or "[]")


def schema(binary, database):
    return query(binary, database, "SELECT type,name,tbl_name,sql FROM sqlite_schema "
                 "ORDER BY type,name;")


def columns(binary, database, name):
    return query(binary, database, "PRAGMA table_xinfo(%s);" % literal(name))


def cell(column):
    return ("CASE WHEN typeof(%s)='text' THEN 'text:'||hex(CAST(%s AS BLOB)) "
            "ELSE typeof(%s)||':'||quote(%s) END" % ((column,) * 4))


def canonical(binary, database, normalized=False):
    objects = schema(binary, database)
    result = {"integrity": query(binary, database, "PRAGMA integrity_check;"),
              "metadata": query(binary, database,
                                "SELECT * FROM pragma_user_version,"
                                "pragma_application_id,pragma_encoding;"),
              "schema": objects, "columns": {}, "tables": {}}
    for obj in objects:
        if obj["type"] != "table":
            continue
        name = obj["name"]
        cols = columns(binary, database, name)
        result["columns"][name] = [{k: v for k, v in col.items() if k != "notnull"}
                                   for col in cols]
        names = [ident(c["name"]) for c in cols]
        if not any(c["pk"] for c in cols):
            aliases = {c["name"].lower() for c in cols}
            for alias in ("rowid", "_rowid_", "oid"):
                if alias not in aliases:
                    names.insert(0, ident(alias))
                    break
        values = [cell(c) + " AS " + ident(str(i)) for i, c in enumerate(names)]
        rows = query(binary, database, "SELECT %s FROM %s ORDER BY %s;" % (
            ",".join(values), ident(name),
            ",".join(str(i + 1) for i in range(len(values)))))
        result["tables"][name] = rows
    if normalized:
        result["schema"] = [{k: v for k, v in obj.items() if k != "sql"}
                            for obj in objects]
    return result


def check(name, actual, expected, directory):
    if actual == expected:
        return
    left = json.dumps(expected, ensure_ascii=True, indent=2, sort_keys=True)
    right = json.dumps(actual, ensure_ascii=True, indent=2, sort_keys=True)
    (directory / (name + "-expected.json")).write_text(left)
    (directory / (name + "-actual.json")).write_text(right)
    delta = list(difflib.unified_diff(left.splitlines(), right.splitlines(),
                                   fromfile="expected", tofile="actual"))
    raise AssertionError(name + " differs:\n" + "\n".join(delta[:50]))


def exotic(seed):
    rng = random.Random(seed)
    name = ident(rng.choice(['odd"table', 'select', 'line\nbreak', '🐬 table']))
    col = ident(rng.choice(['a"b', 'order', 'white space', '值']))
    values = ["NULL", "x'00ff'", "char(97,0,98)", "'a''b'", "'🐬 café'",
              "9223372036854775807", "-9223372036854775808", "1.2345678901234567"]
    rng.shuffle(values)
    script = ("CREATE TABLE %s(id INTEGER PRIMARY KEY,%s);\n" % (name, col)
            + "\n".join("INSERT INTO %s VALUES(%d,%s);" % (name, i + 1, v)
                        for i, v in enumerate(values)))
    script += '\nCREATE TABLE "rowid pool"(v);\n'
    for rowid in (-9223372036854775808, -17, -1, 0, 1, 9223372036854775807):
        script += 'INSERT INTO "rowid pool"(rowid,v) VALUES(%d,%s);\n' % (
            rowid, rng.choice(values))
    script += ('CREATE TABLE "shadowed rowid"(rowid,v);'
               'INSERT INTO "shadowed rowid"(_rowid_,rowid,v) '
               "VALUES(7,'visible','first'),(19,'visible',char(97,0,98));\n")
    return script + "PRAGMA user_version=%d; PRAGMA application_id=%d;\n" % (
        seed % 10000, seed % 2147483647)


def export(engine, source, directory, name, format="sqlite", ref=None):
    output = directory / (name + "." + format)
    args = ["--file-name", str(output), "-r", format]
    if ref is not None:
        args += ["--ref", ref]
    run(engine, source, "SELECT dolt_dump(%s);" % ",".join(map(literal, args)))
    return output


def import_sql(stock, database):
    objects = schema(stock, database)
    script = ["PRAGMA foreign_keys=OFF;", "ATTACH %s AS original;" % literal(database)]
    for obj in objects:
        if obj["type"] == "table" and not obj["name"].startswith("sqlite_"):
            script.append(obj["sql"] + ";")
    for obj in objects:
        if obj["type"] != "table":
            continue
        name = obj["name"]
        cols = columns(stock, database, name)
        names = [ident(c["name"]) for c in cols if c["hidden"] == 0]
        if not any(c["pk"] for c in cols):
            aliases = {c["name"].lower() for c in cols}
            for alias in ("rowid", "_rowid_", "oid"):
                if alias not in aliases:
                    names.insert(0, ident(alias))
                    break
        if name == "sqlite_stat1":
            script.append("ANALYZE main;")
        if name.startswith("sqlite_"):
            script.append("DELETE FROM %s;" % ident(name))
        projection = ",".join(names)
        script.append("INSERT INTO %s(%s) SELECT %s FROM original.%s;" % (
            ident(name), projection, projection, ident(name)))
    for obj in objects:
        if obj["type"] != "table" and obj["sql"]:
            script.append(obj["sql"] + ";")
    for pragma in ("user_version", "application_id"):
        value = query(stock, database, "PRAGMA %s;" % pragma)[0][pragma]
        script.append("PRAGMA %s=%d;" % (pragma, value))
    return "\n".join(script)


def dump_for_clustered_replay(stock, database):
    _, script = run(stock, database, ".dump --preserve-rowids\n")
    def token(name):
        return "(?:%s|%s)" % (re.escape(name), re.escape(ident(name)))
    for obj in schema(stock, database):
        if obj["type"] != "table":
            continue
        cols = columns(stock, database, obj["name"])
        if not any(c["pk"] for c in cols):
            continue
        names = {c["name"].lower() for c in cols}
        for alias in ("rowid", "_rowid_", "oid"):
            if alias not in names:
                fields = ",".join(token(c["name"]) for c in cols if c["hidden"] == 0)
                pattern = (r"^(INSERT INTO %s\()%s,(%s\) VALUES\()[-+]?\d+," % (
                    token(obj["name"]), token(alias), fields))
                script = re.sub(pattern.encode(), rb"\1\2", script, flags=re.MULTILINE)
                break
    return script


def handoff(engine, stock, reference, shared, seed, directory):
    rng = random.Random(seed)
    for i in range(6):
        sql = ("CREATE TABLE IF NOT EXISTS handoff(id INTEGER PRIMARY KEY,v BLOB);"
               "BEGIN; INSERT OR REPLACE INTO handoff VALUES(%d,%s);"
               "UPDATE handoff SET v=%s WHERE id=%d;"
               "DELETE FROM handoff WHERE id=%d; COMMIT;"
               "SELECT id,typeof(v),hex(v) FROM handoff ORDER BY id;" % (
                   rng.randrange(8), rng.choice(["x'00ff'", "'🐬'", "char(0,97)"]),
                   rng.randrange(-100, 100), rng.randrange(8), rng.randrange(8)))
        expected = run(stock, reference, sql)
        actual = run(engine if i % 2 else stock, shared, sql)
        check("handoff-out-%d" % i, actual[1].decode(), expected[1].decode(), directory)
        check("handoff-state-%d" % i, canonical(stock, shared),
              canonical(stock, reference), directory)


def seed_run(engine, stock, seed, directory, with_handoff):
    script = Gen(seed, GROUPS).run() + "\n" + exotic(seed)
    (directory / "workload.sql").write_text(script)
    s1, s2, source = [directory / name for name in ("stock.db", "shared.db", "source.db")]
    run(stock, s1, "PRAGMA user_version=0;")
    shutil.copyfile(s1, s2)
    expected = run(stock, s1, script, require_ok=False)
    actual = run(engine, s2, script, require_ok=False)
    check("out", [actual[0], actual[1].decode(errors="replace")],
          [expected[0], expected[1].decode(errors="replace")], directory)
    check("stock", canonical(stock, s2), canonical(stock, s1), directory)
    run(engine, source, script, require_ok=False)
    snapshot = canonical(engine, source)
    sqlite_export = export(engine, source, directory, "working")
    check("export", canonical(stock, sqlite_export), snapshot, directory)
    sql_export = export(engine, source, directory, "working", "sql")
    replay = directory / "sql-replay.db"
    run(stock, replay, sql_export.read_bytes())
    check("sink", canonical(stock, replay), snapshot, directory)
    run(engine, source, "SELECT dolt_commit('-A','-m','roundtrip');")
    head_export = export(engine, source, directory, "head", ref="HEAD")
    check("ref", canonical(stock, head_export), snapshot, directory)
    imported = directory / "imported.db"
    run(engine, imported, import_sql(stock, s1))
    imported_export = export(engine, imported, directory, "imported")
    check("import", canonical(stock, imported_export, normalized=True),
          canonical(stock, s1, normalized=True), directory)
    dump = dump_for_clustered_replay(stock, s1)
    stock_replay = directory / "stock-dump-replay.db"
    stock_dump_result = run(stock, stock_replay, dump, require_ok=False)
    dump_import = directory / "dump-import.db"
    import_result = run(engine, dump_import, dump, require_ok=False)
    check("dumpimp-out", [import_result[0], import_result[1].decode(errors="replace")],
          [stock_dump_result[0], stock_dump_result[1].decode(errors="replace")], directory)
    dump_export = export(engine, dump_import, directory, "dump-import")
    check("dumpimp", canonical(stock, dump_export, normalized=True),
          canonical(stock, stock_replay, normalized=True), directory)
    if with_handoff:
        handoff(engine, stock, s1, s2, seed, directory)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("doltlite", type=Path)
    parser.add_argument("stock", type=Path)
    parser.add_argument("first", type=int)
    parser.add_argument("last", type=int)
    parser.add_argument("--handoff", action="store_true")
    parser.add_argument("--seconds", type=float, default=0)
    parser.add_argument("--save-dir", type=Path)
    parser.add_argument("--max-failures", type=int, default=10)
    args = parser.parse_args()
    engine, stock = args.doltlite.resolve(), args.stock.resolve()
    if args.last < args.first:
        parser.error("last seed is before first seed")
    if args.seconds < 0 or args.max_failures < 1:
        parser.error("seconds must be nonnegative and max-failures must be positive")
    subprocess.run(["bash", str(Path(__file__).with_name("assert_stock_reference.sh")),
                    str(stock), str(engine)], check=True)
    subprocess.run(["bash", str(Path(__file__).parent / "lib/assert_doltlite_engine.sh"),
                    str(engine)], check=True)
    start = time.monotonic()
    passed = failed = 0
    for seed in range(args.first, args.last + 1):
        if args.seconds and time.monotonic() - start >= args.seconds:
            print("Time budget reached before seed %d" % seed, flush=True)
            break
        with tempfile.TemporaryDirectory(prefix="dump-roundtrip-") as tmp:
            directory = Path(tmp)
            try:
                seed_run(engine, stock, seed, directory, args.handoff)
                passed += 1
            except (AssertionError, RuntimeError, subprocess.TimeoutExpired,
                    ValueError, OSError) as error:
                failed += 1
                print("FAIL seed %d: %s" % (seed, error), flush=True)
                if args.save_dir:
                    dest = args.save_dir / ("seed_%d" % seed)
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copytree(directory, dest, dirs_exist_ok=True)
        if (passed + failed) % 20 == 0:
            print("Seeds through %d: %d passed, %d failed (%.1fs)" % (
                seed, passed, failed, time.monotonic() - start), flush=True)
        if failed >= args.max_failures:
            break
    print("Results: %d passed, %d failed (%.1fs)" % (
        passed, failed, time.monotonic() - start), flush=True)
    return int(failed != 0 or passed == 0)


if __name__ == "__main__":
    raise SystemExit(main())
