#!/usr/bin/env python3

import pathlib
import random
import struct
import subprocess
import sys
import tempfile


def sql(binary, database, statement):
    result = subprocess.run(
        [binary, "-batch", "-bail", str(database)], input=statement,
        text=True, capture_output=True, timeout=120,
    )
    if result.returncode:
        raise AssertionError(f"{binary}: {result.stderr}")
    return result.stdout.strip()


def check(actual, expected):
    if actual != expected:
        raise AssertionError(f"expected {expected!r}, got {actual!r}")


def roundtrip(writer, reader, directory, flat_only):
    rng = random.Random(42)
    vectors = [struct.pack("<24f", *(rng.uniform(-1, 1) for _ in range(24)))
               for _ in range(600)]
    models = {
        "flat": "'{index:\"flat\"}'",
        "ivf": "vec1_train(v, '{nbucket:4}')",
        "pq": "vec1_train(v, '{codesize:8,nbucket:4}')",
        "opq": "vec1_train(v, '{quantizer:\"opq\",codesize:8,nbucket:4}')",
        "bq": "vec1_train(v, '{quantizer:\"bq\",nbucket:4}')",
    }
    if flat_only:
        models = {"flat": models["flat"]}
    values = ",".join(f"({i},x'{v.hex()}')"
                      for i, v in enumerate(vectors, 1))
    for name, model in models.items():
        database = directory / f"{name}.db"
        sql(writer, database, f"""
            CREATE TABLE source(id INTEGER PRIMARY KEY, v BLOB);
            INSERT INTO source VALUES {values};
            CREATE VIRTUAL TABLE t USING vec1(v, label, n, data);
            INSERT INTO t(rowid,v,label,n,data)
              SELECT id,v,'row'||id,id,CAST(id AS BLOB) FROM source;
            INSERT INTO t(cmd,arg) SELECT 'rebuild',{model} FROM source LIMIT 1;
            SELECT dolt_commit('-Am','vectors');
        """)
        query = """
            SELECT rowid,label,n,hex(data)
              FROM t((SELECT v FROM source WHERE id=37), '{k:5,nprobe:4}')
             ORDER BY distance,rowid;
            PRAGMA integrity_check;
        """
        expected = sql(writer, database, query)
        if not expected.endswith("\nok"):
            raise AssertionError(expected)
        check(sql(reader, database, query), expected)
        sql(reader, database, """
            DELETE FROM t WHERE rowid=38;
            UPDATE t SET label='changed',n=-12345 WHERE rowid=37;
            INSERT INTO t(rowid,v,label,n,data)
              SELECT 1001,v,'new',7,x'0001' FROM source WHERE id=1;
            SELECT dolt_commit('-am','reader edits');
        """)
        expected = sql(reader, database, query)
        check(sql(writer, database, query), expected)
        for binary in (writer, reader):
            check(sql(binary, database, """
                SELECT count(*) FROM t;
                SELECT label,n FROM t WHERE rowid=37;
                SELECT count(*) FROM t WHERE rowid=38;
                SELECT label,n,hex(data) FROM t WHERE rowid=1001;
                PRAGMA integrity_check;
            """), "600\nchanged|-12345\n0\nnew|7|0001\nok")
        print(f"PASS: {name}: persisted models, KNN, metadata and writes")


if __name__ == "__main__":
    current, previous = map(lambda p: str(pathlib.Path(p).resolve()), sys.argv[1:3])
    flat_only = sys.argv[3:] == ["--flat-only"]
    with tempfile.TemporaryDirectory(prefix="doltlite-vec1-compat-") as temp:
        directory = pathlib.Path(temp)
        for name, writer, reader in (
            ("old-to-current", previous, current),
            ("current-to-old", current, previous),
        ):
            target = directory / name
            target.mkdir()
            roundtrip(writer, reader, target, flat_only)
            print(f"PASS: {name}: vec1 compatibility")
