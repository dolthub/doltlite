#!/usr/bin/env python3
from pathlib import Path
import sys
import tempfile
import unittest

from sql_dump_roundtrip_sweep import canonical, dump_for_clustered_replay, run
from sqlite_file_safety_sweep import compare_image


STOCK = Path(sys.argv.pop(1)).resolve()


class RoundtripChecks(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="roundtrip-check-")
        self.addCleanup(self.directory.cleanup)
        self.database = Path(self.directory.name) / "test.db"
        self.script = ("CREATE TABLE t(v);"
            "INSERT INTO t(rowid,v) VALUES(9,char(97,0,98));"
            "CREATE TABLE p(k TEXT PRIMARY KEY,v);"
            "INSERT INTO p(rowid,k,v) VALUES(17,'key','value');")
        run(STOCK, self.database, self.script)
        self.before = canonical(STOCK, self.database)
        self.normalized = canonical(STOCK, self.database, normalized=True)

    def changed(self):
        self.assertNotEqual(self.before, canonical(STOCK, self.database))
        self.assertNotEqual(self.normalized, canonical(STOCK, self.database, normalized=True))

    def test_nul_suffix(self):
        run(STOCK, self.database, "UPDATE t SET v=char(97,0,99);")
        self.changed()

    def test_value_type(self):
        run(STOCK, self.database, "UPDATE t SET v=CAST(v AS BLOB);")
        self.changed()

    def test_keyless_rowid(self):
        run(STOCK, self.database, "UPDATE t SET rowid=10;")
        self.changed()

    def test_missing_row(self):
        run(STOCK, self.database, "DELETE FROM t;")
        self.changed()

    def test_stored_dolt_table_is_included(self):
        run(STOCK, self.database, "CREATE TABLE dolt_fixture(v);"
            "INSERT INTO dolt_fixture VALUES('retained');")
        self.changed()
        self.assertIn("dolt_fixture", canonical(STOCK, self.database)["tables"])

    def test_metadata(self):
        run(STOCK, self.database, "PRAGMA application_id=5; PRAGMA user_version=7;")
        self.changed()

    def test_schema_object(self):
        run(STOCK, self.database, "CREATE INDEX changed ON t(v);")
        self.changed()

    def test_schema_column(self):
        run(STOCK, self.database, "ALTER TABLE t RENAME COLUMN v TO renamed;")
        self.changed()

    def test_schema_text_only_normalizes_for_import(self):
        other = Path(self.directory.name) / "other.db"
        run(STOCK, other, self.script.replace("t(v)", "t( v )"))
        self.assertNotEqual(self.before, canonical(STOCK, other))
        self.assertEqual(self.normalized, canonical(STOCK, other, normalized=True))

    def test_clustered_replay_retains_keyless_identity(self):
        run(STOCK, self.database, "UPDATE t SET v='without NUL';")
        script = dump_for_clustered_replay(STOCK, self.database)
        replay = Path(self.directory.name) / "replay.db"
        run(STOCK, replay, script)
        self.assertEqual(canonical(STOCK, self.database, normalized=True),
                         canonical(STOCK, replay, normalized=True))
        self.assertIn("INSERT INTO t(rowid,v) VALUES(9,", script)
        self.assertNotIn("INSERT INTO p(rowid,", script)

    def test_unchanged_stock_file_is_protected(self):
        with self.assertRaisesRegex(AssertionError, "changed a file"):
            compare_image({"test.db": b"stock"}, {"test.db": b"stock"},
                          {"test.db": b"changed"})

    def test_doltlite_sidecar_is_detected(self):
        with self.assertRaisesRegex(AssertionError, "sidecars differ"):
            compare_image({"test.db": b"stock"}, {"test.db": b"stock"},
                          {"test.db": b"stock", ".test.db-lock": b""})


if __name__ == "__main__":
    unittest.main()
