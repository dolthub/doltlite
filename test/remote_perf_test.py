import contextlib
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock

import remote_perf as perf


class RemotePerfTest(unittest.TestCase):
    def test_file_urls_preserve_filesystem_names(self):
        path = Path.cwd().resolve() / "remote perf's 100%.db"
        self.assertEqual(perf.file_url(path), "file://" + str(path))
        db = sqlite3.connect(":memory:")
        try:
            self.assertEqual(db.execute("SELECT " + perf.literal(perf.file_url(path))).fetchone()[0],
                             perf.file_url(path))
        finally:
            db.close()

    def test_seeded_fixture_and_scattered_delta(self):
        db = sqlite3.connect(":memory:")
        db.create_function("sha3", 2, lambda value, size:
                           perf.hashlib.sha3_512(value.encode()).digest())
        try:
            db.executescript(perf.setup_sql(48, 123))
            before = db.execute("SELECT id,bucket,revision,payload FROM t ORDER BY id").fetchall()
            self.assertEqual(len(before), 48)
            self.assertEqual(sum(len(row[3]) for row in before), 48_000)
            self.assertEqual(len({row[3] for row in before}), 48)
            for row_id, bucket, revision, payload in before:
                self.assertEqual((bucket, revision), (row_id % 1024, 0))
                self.assertEqual(payload, perf.payload_bytes(123, row_id, 0))
            db.executescript(perf.delta_sql(48, 7, 123))
            after = db.execute("SELECT id,bucket,revision,payload FROM t ORDER BY id").fetchall()
            changed = {1, 7, 14, 21, 28, 35, 42}
            self.assertEqual({row[0] for old, row in zip(before, after) if old != row}, changed)
            for row_id, bucket, revision, payload in after:
                modified = int(row_id in changed)
                self.assertEqual((bucket, revision), ((row_id + modified) % 1024, modified))
                self.assertEqual(payload, perf.payload_bytes(123, row_id, modified))
        finally:
            db.close()

    def test_summary_handles_wrapped_index_keys(self):
        rows, delta = 2048, 2048
        db = sqlite3.connect(":memory:")
        try:
            db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY,bucket,revision,payload)")
            db.executemany("INSERT INTO t VALUES(?,?,0,?)",
                           ((i, i % 1024, bytes(perf.PAYLOAD_BYTES)) for i in range(1, rows + 1)))
            db.commit()
            self.assertEqual(perf.expected_summary(rows), self.summary(db))
            for row in perf.changed_ids(rows, delta):
                db.execute("UPDATE t SET bucket=(id+1)%1024,revision=1 WHERE id=?", (row,))
            self.assertEqual(perf.expected_summary(rows, delta), self.summary(db))
        finally:
            db.close()

    def summary(self, db):
        return "|".join(map(str, db.execute(
            "SELECT count(*),sum(length(payload)),sum(id),sum(revision),sum(bucket) FROM t"
        ).fetchone()))

    def test_timing_rejects_missing_results_and_incomplete_sessions(self):
        valid = "BEGIN push_initial\n0\nRun Time: real 0.012345 user 0.002 sys 0.001\nEND push_initial\n"
        self.assertEqual(perf.parse_timing(valid, "push_initial"), {"wall_us": 12345, "cpu_us": 3000})
        invalid = (valid.replace("\n0\n", "\n1\n"),
                   valid.replace("\n0\n", "\n"),
                   valid.replace("real 0.012345", "real 0.0"),
                   valid.replace("real 0.012345", "real nan"),
                   valid.replace("END push_initial\n", ""),
                   valid + "unexpected\n", valid.replace("BEGIN push_initial", "BEGIN clone_initial"))
        for output in invalid:
            with self.subTest(output=output), self.assertRaises(ValueError):
                perf.parse_timing(output, "push_initial")

    def test_verification_rejects_wrong_rows_and_tracking_snapshots(self):
        expected = {"head": "base", "data_sha3": "rows", "schema_sha3": "schema", "summary": "counts"}
        for field in expected:
            actual = {**expected, field: "wrong"}
            with self.subTest(field=field), mock.patch.object(perf, "fingerprint", return_value=actual):
                with self.assertRaises(ValueError):
                    perf.verify("engine", "database", expected)
        with mock.patch.object(perf, "fingerprint", return_value=expected), \
             mock.patch.object(perf, "sql", return_value="base\nwrong rows\n"):
            with self.assertRaises(ValueError):
                perf.verify("engine", "database", expected, expected)

    def test_failed_verification_retains_sample_and_fails_run(self):
        def fail_measure(*args):
            args[-1].update(wall_us=100, cpu_us=50, logical_payload_bytes=1000,
                            destination_growth_bytes=1000)
            raise ValueError("database mismatch")

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "results"
            with mock.patch.object(perf, "binary_info", return_value={"version": "test"}), \
                 mock.patch.object(perf, "prepare_fixtures", return_value={
                     "base_file_bytes": 1000, "delta_file_bytes": 1100}), \
                 mock.patch.object(perf, "measure", side_effect=fail_measure), \
                 contextlib.redirect_stdout(io.StringIO()):
                rc = perf.main(["--candidate", "unused", "--size-mb", "1", "--runs", "1",
                                "--output-dir", str(output)])
            self.assertEqual(rc, 1)
            result = json.loads((output / "results.json").read_text())
            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["error"], "database mismatch")
            self.assertEqual(len(result["samples"]), 1)
            self.assertFalse(result["samples"][0]["verified"])
            self.assertNotIn("| copy_fsync |", (output / "report.md").read_text())
            self.assertIn("False", (output / "samples.tsv").read_text())

    def test_fixture_failure_writes_failed_report(self):
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(perf, "binary_info", side_effect=ValueError("wrong engine")), \
                 contextlib.redirect_stdout(io.StringIO()):
                rc = perf.main(["--candidate", "unused", "--output-dir", directory])
            result = json.loads((Path(directory) / "results.json").read_text())
            self.assertEqual((rc, result["status"], result["samples"]), (1, "failed", []))


if __name__ == "__main__":
    unittest.main()
