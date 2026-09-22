#!/usr/bin/env python3

import contextlib
import io
import os
import sqlite3
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest
from unittest.mock import patch

import benchmark_compare
import performance_hotspots as hotspots
import savepoint_rollback_perf as savepoints


class HotspotTests(unittest.TestCase):
    def setUp(self):
        self.names = ("scan_first", "scan_repeat", "point_10000",
                      "scan_after_points", "index_scan_row_fetch")
        self.cases = [("scan_first", "SELECT 42;", "42"),
                      ("scan_repeat", "SELECT 42;", "42")]
        self.output = ("BEGIN scan_first\n42\n"
                       "Run Time: real 0.120000 user 0.100000 sys 0.020000\n"
                       "END scan_first\nBEGIN scan_repeat\n42\n"
                       "Run Time: real 0.080000 user 0.070000 sys 0.010000\n"
                       "END scan_repeat\n")

    def test_query_workloads_validate_results_after_point_reads(self):
        cases = hotspots.workloads(17)
        self.assertEqual([name for name, _, _ in cases], list(self.names[:4]))
        with contextlib.closing(sqlite3.connect(":memory:")) as db:
            db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload BLOB)")
            db.executemany("INSERT INTO t VALUES(?,?)",
                           ((i, bytes(hotspots.PAYLOAD_BYTES)) for i in range(1, 18)))
            for name, query, expected in cases:
                with self.subTest(name=name):
                    self.assertEqual("|".join(map(str, db.execute(query).fetchone())),
                                     expected)

    def test_internal_timings(self):
        self.assertEqual(hotspots.parse_session(self.output, self.cases),
                         {"scan_first": 120000, "scan_repeat": 80000})

    def test_batch_timings_validate_every_result_and_timer(self):
        cases = [("batch", "SELECT 42; SELECT 43;", ["42", "43"])]
        output = ("BEGIN batch\n42\n"
                  "Run Time: real 0.120000 user 0.100000 sys 0.020000\n43\n"
                  "Run Time: real 0.080000 user 0.070000 sys 0.010000\nEND batch\n")
        self.assertEqual(hotspots.parse_session(output, cases), {"batch": 200000})
        for bad in (output.replace("43", "44"),
                    output.replace("Run Time: real 0.080000 user 0.070000 sys 0.010000\n", ""),
                    output.replace("0.080000", "0.000000")):
            with self.subTest(output=bad), self.assertRaises(ValueError):
                hotspots.parse_session(bad, cases)

    def test_index_fixture_results_and_plans(self):
        with tempfile.TemporaryDirectory() as directory, sqlite3.connect(":memory:") as db:
            fixture = Path(directory) / "fixture.sql"
            cases = hotspots.index_fixture(fixture, 2051)
            db.executescript(fixture.read_text().removeprefix(".bail on\n"))
            self.assertEqual([case[0] for case in cases], list(self.names[4:]))
            for name, queries, expected in cases:
                self.assertEqual(len(expected), 1000)
                results = ["|".join(map(str, db.execute(query).fetchone()))
                           for query in queries.splitlines()]
                self.assertEqual(results, expected)
                plan = db.execute("EXPLAIN QUERY PLAN " + queries.splitlines()[0]).fetchone()[3]
                self.assertIn("SEARCH orders USING INDEX orders_customer", plan)

    def test_index_plan_changes_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory) / "fixture.sql"
            fixture.touch()
            for name in self.names[4:]:
                with self.subTest(name=name), patch.object(hotspots, "run", return_value=""), \
                     patch.object(hotspots, "sql", side_effect=["1|1024\nok\n", "SCAN orders\n"]):
                    with self.assertRaisesRegex(ValueError, "unexpected .* plan"):
                        hotspots.prepare_index_queries(Path("unused"), Path("unused"), fixture, 1,
                                                       [(name, "SELECT 1;", ["1"])])

    def test_bad_or_missing_measurements_fail(self):
        for output in ("", self.output.replace("42", "41"),
                       self.output.replace("0.120000", "0.000000"),
                       self.output.replace("END scan_repeat", ""),
                       self.output + "Error: ignored failure\n",
                       self.output.replace("Run Time: real 0.080000 user 0.070000 sys 0.010000\n", ""),
                       self.output.replace("END scan_first", "Run Time: real 0.01 user 0.01 sys 0.00\nEND scan_first")):
            with self.subTest(output=output), self.assertRaises(ValueError):
                hotspots.parse_session(output, self.cases)

    def test_invalid_times_fail(self):
        for value in ("0", "-1", "nan", "inf", "oops"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                hotspots.positive_us(value)

    def test_fixture_must_exceed_cache(self):
        for options in (("--rows", "100"), ("--runs", "0"), ("--cache-kib", "0")):
            with self.subTest(options=options), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as caught:
                    hotspots.main(["--baseline", "unused", "--candidate", "unused",
                                   "--stock", "unused", *options])
                self.assertEqual(caught.exception.code, 2)

    def test_sql_failure_is_not_a_timing(self):
        with self.assertRaises(RuntimeError):
            hotspots.run(["sh", "-c", "echo 'partial output'; exit 1"])
        with self.assertRaises(RuntimeError):
            hotspots.run(["sh", "-c", "echo 'error' >&2"])

    def test_main_measures_queries_for_all_arms(self):
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory) / "results.tsv"
            raw = Path(directory) / "samples.tsv"
            values = {name: 100000 for name in self.names[:4]}
            index_values = {name: 100000 for name in self.names[4:]}
            with patch.object(hotspots, "run", return_value="validated") as run, \
                 patch.object(hotspots, "prepare", side_effect=lambda binary, db, rows: db.touch()), \
                 patch.object(hotspots, "measure_queries", side_effect=lambda *args: dict(values)) as measure, \
                 patch.object(hotspots, "index_fixture", return_value=[]), \
                 patch.object(hotspots, "prepare_index_queries", side_effect=lambda binary, db, *args: db.touch()), \
                 patch.object(hotspots, "measure_index_queries", return_value=index_values) as index_measure, \
                 patch.object(hotspots, "add_column_fixture", side_effect=lambda binary, db, rows: db.touch()), \
                 patch.object(hotspots, "measure_add_column",
                              return_value={"add_column_default": 100000}) as add_column_measure, \
                 patch.object(hotspots, "index_edit_fixture",
                              side_effect=lambda binary, db, rows: (db.touch(), {"x": "1"})[1]) as index_edit_fixture, \
                 patch.object(hotspots, "measure_index_edits",
                              return_value={"index_edit_update": 100000}) as index_edit_measure, \
                 patch.dict(os.environ, BENCH_RESULTS_OUTPUT=str(result), BENCH_SAMPLES_OUTPUT=str(raw)), \
                 contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                hotspots.main(["--baseline", "base", "--candidate", "candidate",
                               "--stock", "stock", "--runs", "2"])
            run.assert_called_once_with(["bash", str(hotspots.TEST_DIR / "assert_stock_reference.sh"),
                                         str(Path("stock").resolve()), str(Path("candidate").resolve())])
            self.assertEqual([call.args[0].name for call in measure.call_args_list],
                             ["base", "candidate", "stock", "stock", "candidate", "base"])
            self.assertEqual([call.args[0].name for call in index_measure.call_args_list],
                             ["base", "candidate", "stock", "stock", "candidate", "base"])
            self.assertEqual([call.args[0].name for call in add_column_measure.call_args_list],
                             ["base", "candidate", "stock", "stock", "candidate", "base"])
            # Each trial mutates a fresh copy: the work db is never the fixture.
            for call in add_column_measure.call_args_list:
                self.assertNotEqual(call.args[1], call.args[2])
                self.assertTrue(call.args[1].name.endswith("-add-column.db"), call.args[1])
                self.assertTrue(call.args[2].name.endswith("-add-column-run.db"), call.args[2])
            self.assertEqual([call.args[0].name for call in index_edit_measure.call_args_list],
                             ["base", "candidate", "stock", "stock", "candidate", "base"])
            for call in index_edit_measure.call_args_list:
                self.assertNotEqual(call.args[1], call.args[2])
                self.assertTrue(call.args[1].name.endswith("-index-edits.db"), call.args[1])
                self.assertTrue(call.args[2].name.endswith("-index-edits-run.db"), call.args[2])
                self.assertEqual(call.args[3], hotspots.INDEX_EDIT_CACHE_KIB)
                self.assertEqual(call.args[4], {"x": "1"})
            # This section sizes itself: the gap only opens up past --rows.
            self.assertEqual({call.args[2] for call in index_edit_fixture.call_args_list},
                             {hotspots.INDEX_EDIT_ROWS})
            self.assertGreater(hotspots.INDEX_EDIT_ROWS, 262144)
            self.assertEqual(result.read_text(), "".join(
                f"queries\t{name}\t100000\t100000\n" for name in self.names)
                + "add_column\tadd_column_default\t100000\t100000\n"
                + "index_edits\tindex_edit_update\t100000\t100000\n")
            self.assertEqual(len(raw.read_text().splitlines()), 15)

    def test_medians_raw_samples_and_stock_report(self):
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory) / "results.tsv"
            raw = Path(directory) / "samples.tsv"
            samples = {
                "baseline": [{name: n for name in self.names}
                             for n in (100000, 900000, 120000)],
                "candidate": [{name: n for name in self.names}
                              for n in (240000, 200000, 900000)],
                "stock": [{name: n for name in self.names}
                          for n in (50000, 45000, 55000)],
            }
            report = io.StringIO()
            with contextlib.redirect_stdout(report):
                hotspots.write_results(samples, result, raw)
            self.assertEqual(result.read_text(), "".join(
                f"queries\t{name}\t120000\t240000\n" for name in self.names))
            self.assertIn("240.000 | 2.00× | 50.000 | 4.80×", report.getvalue())
            self.assertIn("### Large Table Scans\n", report.getvalue())
            self.assertEqual(report.getvalue().count("| Workload |"), 1)
            self.assertEqual([line.split("|")[1].strip() for line in report.getvalue().splitlines()
                              if line.startswith("| ")][1:], list(self.names))
            for name in self.names:
                self.assertIn(f"| {name} |", report.getvalue())
                self.assertIn(f"queries\t{name}\t3\t120000\t900000\t55000\n", raw.read_text())
            self.assertNotIn("Large Table Appends", report.getvalue())
            self.assertNotIn("—", report.getvalue())
            parsed, _metadata = benchmark_compare.parse_input_artifact(f"hotspots={result}")
            analysis = benchmark_compare.analyze(parsed, 1.5, 1.25, 10000)
            self.assertTrue(analysis["individual_failures"])
            self.assertTrue(analysis["section_failures"])
            self.assertIn(("hotspots", "queries", "index_scan_row_fetch"),
                          analysis["individual_failures"])

    def test_stock_speed_is_reported_but_does_not_change_pr_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory) / "results.tsv"
            raw = Path(directory) / "samples.tsv"
            for candidate, fails in ((100000, False), (130000, True)):
                for stock in (1000, 1000000):
                    with self.subTest(candidate=candidate, stock=stock):
                        samples = {"baseline": [{"scan_first": 100000}],
                                   "candidate": [{"scan_first": candidate}],
                                   "stock": [{"scan_first": stock}]}
                        report = io.StringIO()
                        with contextlib.redirect_stdout(report):
                            hotspots.write_results(samples, result, raw)
                        self.assertIn(f"| {stock/1000:.3f} | {candidate/stock:.2f}× |", report.getvalue())
                        self.assertIn("1.25× per workload and 1.15× per section/suite", report.getvalue())
                        self.assertEqual(result.read_text(), f"queries\tscan_first\t100000\t{candidate}\n")
                        parsed, _ = benchmark_compare.parse_input_artifact(f"hotspots={result}")
                        analysis = benchmark_compare.analyze(parsed, 1.25, 1.15, 10000)
                        self.assertEqual(analysis["failed"], fails)

    def test_add_column_gets_its_own_section(self):
        names = ("scan_first", "add_column_default")
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory) / "results.tsv"
            raw = Path(directory) / "samples.tsv"
            samples = {"baseline": [{"scan_first": 120000, "add_column_default": 500}],
                       "candidate": [{"scan_first": 120000, "add_column_default": 130000}],
                       "stock": [{"scan_first": 20000, "add_column_default": 400}]}
            report = io.StringIO()
            with contextlib.redirect_stdout(report):
                hotspots.write_results(samples, result, raw)
            text = report.getvalue()
            self.assertEqual(result.read_text(),
                             "queries\tscan_first\t120000\t120000\n"
                             "add_column\tadd_column_default\t500\t130000\n")
            self.assertIn("add_column\tadd_column_default\t1\t500\t130000\t400\n", raw.read_text())
            self.assertEqual(text.count("| Workload |"), 2)
            self.assertLess(text.index("### Large Table Scans"), text.index("### Add Column With Default"))
            scans, add_column = text.split("### Add Column With Default")
            self.assertIn("| scan_first |", scans)
            self.assertNotIn("| add_column_default |", scans)
            self.assertIn("| add_column_default | 0.500 | 130.000 | 260.00× | 0.400 | 325.00× |", add_column)
            self.assertNotIn("| scan_first |", add_column)
            # The new section is gated like any other: a regression there is a failure.
            parsed, _metadata = benchmark_compare.parse_input_artifact(f"hotspots={result}")
            analysis = benchmark_compare.analyze(parsed, 1.5, 1.25, 10000)
            self.assertIn(("hotspots", "add_column", "add_column_default"), analysis["individual_failures"])
            self.assertEqual(len(hotspots.SECTIONS), 3)

    def test_index_edits_get_their_own_section_after_add_column(self):
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory) / "results.tsv"
            raw = Path(directory) / "samples.tsv"
            names = ("scan_first", "index_scan_row_fetch", "add_column_default",
                     "index_edit_update")
            samples = {"baseline": [{n: 1000 for n in names}],
                       "candidate": [{n: 1000 for n in names}],
                       "stock": [{n: 500 for n in names}]}
            report = io.StringIO()
            with contextlib.redirect_stdout(report):
                hotspots.write_results(samples, result, raw)
            text = report.getvalue()
            # index_scan_row_fetch is a query; only index_edit_* moves to the new section.
            self.assertIn("queries\tindex_scan_row_fetch\t1000\t1000\n", result.read_text())
            for name in names[3:]:
                self.assertIn(f"index_edits\t{name}\t1000\t1000\n", result.read_text())
            self.assertEqual(text.count("| Workload |"), 3)
            self.assertLess(text.index("### Add Column With Default"), text.index("### Large Index Edits"))
            edits = text.split("### Large Index Edits")[1]
            for name in names[3:]:
                self.assertIn(f"| {name} |", edits)
            self.assertNotIn("| index_scan_row_fetch |", edits)
            self.assertNotIn("| add_column_default |", edits)

    def test_savepoint_expected_rows_match_sql(self):
        for rows, operations in ((17, 50), (5000, 1000)):
            for rollback_every in (0, 1, 4, 7):
                with self.subTest(rows=rows, rollback_every=rollback_every), sqlite3.connect(":memory:") as db:
                    db.executescript("CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER NOT NULL);")
                    db.executemany("INSERT INTO t VALUES(?,?)", ((i, i % 100) for i in range(1, rows + 1)))
                    db.commit()
                    batch, expected = savepoints.workload(rows, operations, rollback_every)
                    db.executescript(batch)
                    actual = ",".join(f"{i}:{k}" for i, k in db.execute("SELECT id,k FROM t ORDER BY id"))
                    self.assertEqual(actual + "|ok", expected)
                    self.assertFalse(db.in_transaction)

    def test_savepoint_measurement_validates_rows_and_cache_size(self):
        session = ("BEGIN savepoint_rollback\nRun Time: real 0.009000 user 0.009 sys 0.0\n"
                   "1:2|ok\nEND savepoint_rollback\n")
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory) / "fixture.db"
            fixture.write_bytes(b"fixture")
            work = Path(directory) / "work.db"
            batch = "BEGIN; UPDATE t SET k=k+1; COMMIT;"
            with patch.object(savepoints, "sql", return_value=session) as sql:
                self.assertEqual(savepoints.measure("bin", fixture, work, batch, "1:2|ok", 64), 9000)
                script = sql.call_args.args[2]
                self.assertLess(script.index("SELECT sum(k) FROM t;"), script.index(".timer on"))
                self.assertLess(script.index(".timer on"), script.index(batch))
                self.assertLess(script.index(batch), script.index(".timer off"))
                self.assertLess(script.index(".timer off"), script.index("pragma_integrity_check"))
            for bad in (session.replace("1:2|ok", "1:1|ok"), session.replace("1:2|ok", "1:2|broken")):
                with patch.object(savepoints, "sql", return_value=bad), self.assertRaises(ValueError):
                    savepoints.measure("bin", fixture, work, batch, "1:2|ok", 64)
            fixture.write_bytes(bytes(16384))
            with patch.object(savepoints, "sql", return_value=session), self.assertRaisesRegex(ValueError, "quarter"):
                savepoints.measure("bin", fixture, work, batch, "1:2|ok", 64)
            with patch.object(savepoints, "sql"), self.assertRaisesRegex(ValueError, "quarter"):
                savepoints.prepare("bin", fixture, 5000, 64)

    def test_index_edit_fixture_expectations_match_real_sql(self):
        rows = 3000
        with patch.object(hotspots, "sql", return_value=f"{rows}\n"):
            expected = hotspots.index_edit_fixture("bin", Path("unused.db"), rows)
        db = sqlite3.connect(":memory:")
        db.executescript(f"""CREATE TABLE ie(id INTEGER PRIMARY KEY, k INTEGER NOT NULL, s TEXT NOT NULL);
            WITH RECURSIVE c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<{rows})
            INSERT INTO ie SELECT i,(i*7919)%1000,'s'||i FROM c;
            CREATE INDEX ie_k ON ie(k);
            UPDATE ie SET k=k+1 WHERE id%2=0;""")
        self.assertEqual(expected, {"index_edit_update": str(rows // 2)})
        self.assertEqual(db.execute("SELECT changes()").fetchone()[0], rows // 2)

    def test_measure_index_edits_times_each_statement(self):
        expected = {"index_edit_update": "500"}
        session = "BEGIN index_edit_update\nRun Time: real 0.200000 user 0.1 sys 0.0\n500\nEND index_edit_update\n"
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory) / "fixture.db"
            fixture.write_bytes(b"fixture")
            work = Path(directory) / "work.db"
            work.write_bytes(b"stale")
            work.with_name("work.db-lock").write_bytes(b"")
            with patch.object(hotspots, "sql", return_value=session) as sql:
                times = hotspots.measure_index_edits("bin", fixture, work, 4096, expected)
            self.assertEqual(times, {"index_edit_update": 200000})
            self.assertEqual(work.read_bytes(), b"fixture")
            self.assertFalse(work.with_name("work.db-lock").exists())
            script = sql.call_args.args[2]
            self.assertLess(script.index("BEGIN;"), script.index("UPDATE ie SET k=k+1 WHERE id%2=0;"))
            self.assertLess(script.index("UPDATE ie SET k=k+1"), script.index("SELECT changes();"))
            self.assertIn("ROLLBACK;", script.rsplit("index_edit_update", 1)[1])
            # The UPDATE is timed alone: its changes() check sits after the timer stops.
            update_block = script.split("BEGIN index_edit_update", 1)[1].split("END index_edit_update", 1)[0]
            self.assertLess(update_block.index(".timer off"), update_block.index("SELECT changes();"))
            bad = session.replace("\n500\n", "\n499\n")
            with patch.object(hotspots, "sql", return_value=bad), self.assertRaises(ValueError):
                hotspots.measure_index_edits("bin", fixture, work, 4096, expected)

    def test_measure_add_column_times_only_the_alter(self):
        rows = 1000
        session = ("BEGIN add_column_default\nRun Time: real 0.125000 user 0.1 sys 0.0\n"
                   f"{rows}|{rows * hotspots.ADD_COLUMN_DEFAULT}\nEND add_column_default\n")
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory) / "fixture.db"
            fixture.write_bytes(b"fixture")
            work = Path(directory) / "work.db"
            work.write_bytes(b"stale")
            work.with_name("work.db-lock").write_bytes(b"")
            with patch.object(hotspots, "sql", return_value=session) as sql:
                times = hotspots.measure_add_column("bin", fixture, work, rows, 4096)
            self.assertEqual(times, {"add_column_default": 125000})
            self.assertEqual(work.read_bytes(), b"fixture")
            self.assertFalse(work.with_name("work.db-lock").exists())
            script = sql.call_args.args[2]
            alter = f"ALTER TABLE ac ADD COLUMN z INTEGER NOT NULL DEFAULT {hotspots.ADD_COLUMN_DEFAULT};"
            self.assertLess(script.index(".timer on"), script.index(alter))
            self.assertLess(script.index(alter), script.index(".timer off"))
            self.assertLess(script.index(".timer off"), script.index("SELECT count(*),sum(z) FROM ac;"))
            # A wrong row count or default sum is not a timing.
            bad = session.replace(f"{rows}|{rows * hotspots.ADD_COLUMN_DEFAULT}", f"{rows}|0")
            with patch.object(hotspots, "sql", return_value=bad), self.assertRaises(ValueError):
                hotspots.measure_add_column("bin", fixture, work, rows, 4096)

    def test_hotspot_failure_reaches_existing_gate(self):
        workflow = (hotspots.TEST_DIR.parent / ".github/workflows/benchmark.yml").read_text()
        report = workflow.split("    - name: Generate relative performance report\n", 1)[1]
        report = textwrap.dedent(report.split("      run: |\n", 1)[1].split("\n    - name:", 1)[0])
        report = report.replace("${{ github.event.pull_request.base.sha }}", "base")
        report = report.replace("git rev-parse HEAD", "printf candidate")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "benchmark-results").mkdir()
            (root / "bin").mkdir()
            stub = root / "bin/python3"
            stub.write_text('#!/bin/sh\nwhile [ "$1" != --output ]; do shift; done\n'
                            'printf "Standard benchmarks passed\\n" > "$2"\n')
            stub.chmod(0o755)
            for suite in ("int", "textpk", "blobpk", "compositepk", "vc"):
                for suffix in (".tsv", "-attempts.tsv"):
                    (root / f"benchmark-results/{suite}{suffix}").write_text("fixture\n")
            for arm, value in (("baseline", "base"), ("candidate", "candidate")):
                (root / f"benchmark-{arm}-sha").write_text(value)
            for status in ("success", "failure", "skipped", "cancelled"):
                with self.subTest(status=status):
                    env = dict(os.environ, PATH=f"{root / 'bin'}:{os.environ['PATH']}",
                               RUNNER_TEMP=str(root), GITHUB_STEP_SUMMARY=str(root / "summary"),
                               GITHUB_OUTPUT=str(root / "output"))
                    subprocess.run(["bash", "-e", "-o", "pipefail", "-c",
                                    report.replace("${{ needs.hotspots.result }}", status)],
                                   cwd=root, env=env, check=True, capture_output=True)
                    self.assertEqual((root / "output").read_text().splitlines()[-1],
                                     f"report_rc={0 if status == 'success' else 1}")

    def test_hotspot_comment_summary(self):
        workflow = (hotspots.TEST_DIR.parent / ".github/workflows/benchmark.yml").read_text()
        step = workflow.split("    - name: Publish hotspot measurements\n", 1)[1]
        script = textwrap.dedent(step.split("      run: |\n", 1)[1].split("\n    - name:", 1)[0])
        for key, value in (("github.server_url", "https://github.com"),
                           ("github.repository", "dolthub/doltlite"),
                           ("github.run_id", "123")):
            script = script.replace("${{ " + key + " }}", value)
        for status, measured in (("success", True), ("failure", True), ("failure", False)):
            with self.subTest(status=status, measured=measured), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                results = root / "hotspot-results"
                if measured:
                    results.mkdir()
                    (results / "hotspots.md").write_text("## Performance hotspots\n\nMeasured table\n")
                env = dict(os.environ, RUNNER_TEMP=str(root), GITHUB_STEP_SUMMARY=str(root / "summary"))
                subprocess.run(["bash", "-e", "-o", "pipefail", "-c",
                                script.replace("${{ steps.hotspots.outcome }}", status)],
                               env=env, check=True, capture_output=True)
                body = (results / "summary.md").read_text()
                self.assertEqual(body, (root / "summary").read_text())
                self.assertIn("<!-- benchmark:hotspots -->", body)
                self.assertIn(f"**Gate:** {status}", body)
                self.assertIn("https://github.com/dolthub/doltlite/actions/runs/123", body)
                self.assertIn("Measured table" if measured else "No complete hotspot measurements", body)

    def test_hotspot_comment_creates_then_updates(self):
        workflow = (hotspots.TEST_DIR.parent / ".github/workflows/benchmark.yml").read_text()
        step = workflow.split("    - name: Comment PR with performance hotspots\n", 1)[1]
        script = textwrap.dedent(step.split("        script: |\n", 1)[1].split("\n    - name:", 1)[0])
        harness = r"""
const assert = require('node:assert/strict');
const AsyncFunction = Object.getPrototypeOf(async function() {}).constructor;
const publish = new AsyncFunction('github', 'context', 'require', 'process', process.argv[1]);
let body = '<!-- benchmark:hotspots -->\nfirst results';
let exists = true;
const calls = [];
const comments = [{id: 1, body: '<!-- benchmark:relative -->\nstandard results'}];
const issues = {
  listComments: 'list',
  createComment: async args => { calls.push(['create', args]); comments.push({id: 2, body: args.body}); },
  updateComment: async args => { calls.push(['update', args]); },
};
const github = {rest: {issues}, paginate: async (method, args) => {
  assert.equal(method, 'list');
  assert.equal(args.issue_number, 10);
  return comments;
}};
const context = {repo: {owner: 'dolthub', repo: 'doltlite'}, issue: {number: 10}};
const fakeRequire = name => {
  assert.equal(name, 'fs');
  return {existsSync: () => exists, readFileSync: () => body};
};
async function main() {
  const invoke = () => publish(github, context, fakeRequire, {env: {RUNNER_TEMP: '/tmp'}});
  await invoke();
  body = '<!-- benchmark:hotspots -->\nupdated results';
  await invoke();
  assert.deepEqual(calls.map(call => call[0]), ['create', 'update']);
  assert.equal(calls[0][1].issue_number, 10);
  assert.equal(calls[1][1].comment_id, 2);
  assert.equal(calls[1][1].body, body);
  assert.equal(comments[0].body, '<!-- benchmark:relative -->\nstandard results');
  context.issue.number = undefined;
  await invoke();
  context.issue.number = 10;
  exists = false;
  await invoke();
  assert.equal(calls.length, 2);
}
main().catch(error => { console.error(error); process.exitCode = 1; });
"""
        subprocess.run(["node", "-e", harness, script], check=True, capture_output=True)


if __name__ == "__main__":
    unittest.main()
