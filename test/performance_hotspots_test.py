#!/usr/bin/env python3

import contextlib
import io
import itertools
import json
import os
import re
import sqlite3
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest
from unittest.mock import DEFAULT, patch

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

    def test_fixture_sizes_must_be_positive(self):
        for options in (("--rows", "0"), ("--runs", "0"), ("--cache-kib", "0")):
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

    def test_main_measures_only_remaining_workloads_for_all_arms(self):
        paths = list((hotspots.TEST_DIR/'performance-hotspot-corpus').glob('*.json'))
        self.assertEqual(paths, [])

        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory) / "results.tsv"
            raw = Path(directory) / "samples.tsv"
            report = io.StringIO()
            with patch.object(hotspots, "run", return_value="validated") as run, \
                 patch.object(hotspots, "sql") as sql, \
                 patch.multiple(hotspots, prepare=DEFAULT, measure_queries=DEFAULT,
                                index_fixture=DEFAULT, measure_index_queries=DEFAULT,
                                index_edit_fixture=DEFAULT, measure_index_edits=DEFAULT) as retired, \
                 patch.object(hotspots, "add_column_fixture") as add_column_fixture, \
                 patch.object(hotspots, "measure_add_column",
                              return_value={"add_column_default": 100000}) as add_column_measure, \
                 patch.object(hotspots, "wide_fixture") as wide_fixture, \
                 patch.object(hotspots, "measure_wide",
                              side_effect=lambda binary, db: {hotspots.wide_name(*case): 100000
                                                              for case in hotspots.WIDE_CASES}) as measure_wide, \
                 patch.object(hotspots, "uncached_fixture") as uncached_fixture, \
                 patch.object(hotspots, "measure_uncached",
                              side_effect=lambda binary, db: {name: 100000 for name, _, _
                                                              in hotspots.uncached_workloads(64, 8)}) as measure_uncached, \
                 patch.multiple(hotspots, bucket_fixture=DEFAULT, measure_bucket=DEFAULT) as bucket, \
                 patch.object(hotspots, "prepare_retained", wraps=hotspots.prepare_retained) as prepare_retained, \
                 patch.object(hotspots, "measure_retained",
                              side_effect=lambda binary, arm, fixtures: {name: 100000 for name, _, _ in fixtures}) as measure_retained, \
                 patch.dict(os.environ, BENCH_RESULTS_OUTPUT=str(result), BENCH_SAMPLES_OUTPUT=str(raw)), \
                 contextlib.redirect_stdout(report), contextlib.redirect_stderr(io.StringIO()):
                bucket["measure_bucket"].side_effect = lambda binary, dbs: {
                    name: 100000 for name, *_ in hotspots.bucket_workloads()}
                hotspots.main(["--baseline", "base", "--candidate", "candidate",
                               "--stock", "stock", "--runs", "2"])
            prepare_retained.assert_called_once()
            self.assertEqual(sql.call_count, 0)
            bucket_fixture, measure_bucket = bucket["bucket_fixture"], bucket["measure_bucket"]
            fixtures = hotspots.bucket_fixture_names()
            self.assertEqual(bucket_fixture.call_count, 3 * len(fixtures))
            self.assertEqual([call.args[0].name for call in measure_bucket.call_args_list],
                             ["base", "candidate", "stock", "stock", "candidate", "base"])
            for call in measure_bucket.call_args_list:
                self.assertEqual(sorted(call.args[1]), fixtures)
            self.assertEqual([call.args[1] for call in measure_retained.call_args_list],
                             ["baseline", "candidate", "stock", "stock", "candidate", "baseline"])
            run.assert_called_once_with(["bash", str(hotspots.TEST_DIR / "assert_stock_reference.sh"),
                                         str(Path("stock").resolve()), str(Path("candidate").resolve())])
            self.assertEqual(add_column_fixture.call_count, 3)
            self.assertEqual([call.args[0].name for call in wide_fixture.call_args_list],
                             ["base", "candidate", "stock"])
            self.assertEqual([call.args[1].name for call in measure_wide.call_args_list],
                             ["baseline-wide.db", "candidate-wide.db", "stock-wide.db",
                              "stock-wide.db", "candidate-wide.db", "baseline-wide.db"])
            self.assertEqual([call.args[0].name for call in uncached_fixture.call_args_list],
                             ["base", "candidate", "stock"])
            self.assertEqual([call.args[1].name for call in measure_uncached.call_args_list],
                             ["baseline-uncached.db", "candidate-uncached.db", "stock-uncached.db",
                              "stock-uncached.db", "candidate-uncached.db", "baseline-uncached.db"])
            self.assertEqual([call.args[0].name for call in add_column_measure.call_args_list],
                             ["base", "candidate", "stock", "stock", "candidate", "base"])
            for call in add_column_measure.call_args_list:
                self.assertNotEqual(call.args[1], call.args[2])
                self.assertTrue(call.args[1].name.endswith("-add-column.db"), call.args[1])
                self.assertTrue(call.args[2].name.endswith("-add-column-run.db"), call.args[2])
            for mock in retired.values():
                mock.assert_not_called()
            for title in ("Large Table Scans", "Large Index Edits", "Wide Rows",
                          "Narrow Rows", "Zero Row Updates",
                          "Bulk Deletes", "Planner Choices"):
                self.assertNotIn(title, report.getvalue())
            self.assertIn("Add Column With Default", report.getvalue())
            self.assertIn("### Wide Row Trade-offs", report.getvalue())
            self.assertNotIn("### Wide Row Fetches", report.getvalue())
            self.assertEqual(report.getvalue().count("### "), 6)
            self.assertIn("### Uncached Reads\n", report.getvalue())
            self.assertIn("https://github.com/dolthub/doltlite/issues/3408", report.getvalue())
            self.assertNotIn("### In Transaction with Mutations", report.getvalue())
            self.assertNotIn("### Small Cache", report.getvalue())
            self.assertNotIn("### Index Row Fetches", report.getvalue())
            self.assertNotIn("### Bulk Writes", report.getvalue())
            self.assertNotIn("### Integer Keys", report.getvalue())
            self.assertNotIn("### After Deletes", report.getvalue())
            for title in ("Primary Key Index Rewrites", "Text Key Index Fetches",
                          "Pending Edit Map"):
                self.assertIn(f"### {title}\n", report.getvalue())
            for call in measure_retained.call_args_list:
                self.assertEqual(call.args[2], [])
            self.assertNotIn('https://github.com/dolthub/doltlite/issues/3427', report.getvalue())
            self.assertEqual(report.getvalue().count('### Pending Edit Map\n'), 1)
            self.assertNotIn('retained_3427_', result.read_text())
            self.assertEqual(len(result.read_text().splitlines()), 24)
            self.assertIn('add_column\tadd_column_default\t100000\t100000\n', result.read_text())
            self.assertEqual(len(raw.read_text().splitlines()), 49)

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
            self.assertEqual(len(hotspots.SECTIONS), 18)

    def test_wide_workloads_results_and_rollback(self):
        payloads = (256, 2048)
        with contextlib.closing(sqlite3.connect(":memory:")) as db:
            db.executescript(hotspots.wide_setup(rows=300, payloads=payloads))
            original = list(db.iterdump())
            for payload in payloads:
                for op, query, expected in hotspots.wide_workloads(payload, rows=300, lookups=50):
                    with self.subTest(payload=payload, op=op):
                        name = f"wide_p{payload}_{op}"
                        self.assertEqual(hotspots.section_of(name), "wide_tradeoffs")
                        self.assertEqual(hotspots.WIDE_NAME.fullmatch(name).groups(), (str(payload), op))
                        db.execute("BEGIN")
                        row = db.execute(query).fetchone()
                        if op.startswith("update"):
                            row = db.execute("SELECT changes()").fetchone()
                        self.assertEqual(str(row[0]), expected)
                        db.rollback()
            self.assertEqual(list(db.iterdump()), original)

    def test_wide_measurement_warms_and_rolls_back_writes(self):
        scripts = []

        def fake(binary, db, statements):
            scripts.append(statements)
            name = statements.split(".print BEGIN ", 1)[1].split("\n", 1)[0]
            payload, op = hotspots.WIDE_NAME.fullmatch(name).groups()
            expected = dict((o, e) for o, _, e in hotspots.wide_workloads(int(payload)))[op]
            timer = "Run Time: real 0.002 user 0.002 sys 0.0\n"
            one = timer + f"{expected}\n" if op.startswith("update") else f"{expected}\n" + timer
            return f"BEGIN {name}\n" + one * hotspots.BATCH.get(name, 1) + f"END {name}\n"

        with patch.object(hotspots, "sql", side_effect=fake):
            measured = hotspots.measure_wide("engine", "db")
        self.assertEqual(sorted(measured), sorted(hotspots.wide_name(*case) for case in hotspots.WIDE_CASES))
        self.assertEqual(len(measured), 8)
        self.assertIn("wide_thrash_p16384_index_fetch_small", measured)
        for name, value in measured.items():
            self.assertEqual(value, 2000 * hotspots.BATCH.get(name, 1), name)
        self.assertGreater(hotspots.BATCH["wide_p4096_scan_small"], 1)
        for script in scripts:
            query = script.split(".timer on\n", 1)[1].split("\n", 1)[0]
            self.assertLess(script.index(query), script.index(".timer on"))
            name = script.split(".print BEGIN ", 1)[1].split("\n", 1)[0]
            batch = hotspots.BATCH.get(name, 1)
            cache = hotspots.WIDE_THRASH_CACHE_KIB if "_thrash_" in name else hotspots.WIDE_CACHE_KIB
            self.assertIn(f"PRAGMA cache_size=-{cache};", script)
            self.assertEqual(script.count(".timer on"), batch)
            self.assertEqual(script.count(query), batch + 1)
            if query.startswith("UPDATE"):
                self.assertEqual(script.count("ROLLBACK;"), batch + 1)
                timed = script.split(f".print BEGIN {name}", 1)[1]
                for rep in timed.split("BEGIN;")[1:]:
                    self.assertLess(rep.index(".timer off"), rep.index("SELECT changes();"))
                    self.assertLess(rep.index("SELECT changes();"), rep.index("ROLLBACK;"))
            else:
                self.assertNotIn("BEGIN;", script)

    def test_batches_name_real_workloads_and_leave_large_ones_single(self):
        names = {hotspots.wide_name(*case) for case in hotspots.WIDE_CASES}
        names |= {name for name, *_ in hotspots.bucket_workloads()}
        self.assertLessEqual(set(hotspots.BATCH), names)
        self.assertTrue(all(type(k) is int and k > 1 for k in hotspots.BATCH.values()))
        uncached = {name for name, *_ in hotspots.uncached_workloads(64, 8)}
        self.assertFalse(uncached & set(hotspots.BATCH))

    def test_report_flags_batches_under_the_minimum(self):
        names = ["wide_p4096_scan_small", "uncached_scan_small", "pk_rewrite_text_no_indexes"]
        samples = {"baseline": [{names[0]: 20000, names[1]: 20000, names[2]: 90000}],
                   "candidate": [{name: 20000 for name in names}],
                   "stock": [{name: 20000 for name in names}]}
        with tempfile.TemporaryDirectory() as directory:
            report = io.StringIO()
            with contextlib.redirect_stdout(report):
                hotspots.write_results(samples, Path(directory)/"r.tsv", Path(directory)/"s.tsv")
        line = [l for l in report.getvalue().splitlines() if l.startswith("**Batch too small:**")]
        self.assertEqual(len(line), 1)
        self.assertIn("`wide_p4096_scan_small`", line[0])
        self.assertNotIn("uncached", line[0])
        self.assertNotIn("pk_rewrite", line[0])

    def test_wide_section_is_reported_and_gated(self):
        names = [hotspots.wide_name(*case) for case in hotspots.WIDE_CASES]
        samples = {"baseline": [{name: 100000 for name in names}],
                   "candidate": [{name: 200000 for name in names}],
                   "stock": [{name: 50000 for name in names}]}
        with tempfile.TemporaryDirectory() as directory:
            result, raw = Path(directory)/"results.tsv", Path(directory)/"samples.tsv"
            report = io.StringIO()
            with contextlib.redirect_stdout(report):
                hotspots.write_results(samples, result, raw)
            text = report.getvalue()
            section = text.split("### Wide Row Trade-offs\n", 1)[1]
            self.assertIn("https://github.com/dolthub/doltlite/issues/3325", section)
            self.assertEqual(section.count("| wide_"), 8)
            self.assertIn("| wide_thrash_p16384_index_fetch_small ×10 | 100.000 | 200.000 | 2.00× | 50.000 | 4.00× |", section)
            self.assertIn("A workload marked ×N times N repetitions", text)
            parsed, _ = benchmark_compare.parse_input_artifact(f"hotspots={result}")
            analysis = benchmark_compare.analyze(parsed, 1.25, 1.15, 10000)
            self.assertIn(("hotspots", "wide_tradeoffs"), analysis["section_failures"])
            for name in names:
                self.assertIn(("hotspots", "wide_tradeoffs", name), analysis["individual_failures"])

    def test_uncached_workloads_results_and_rollback(self):
        with contextlib.closing(sqlite3.connect(":memory:")) as db:
            db.executescript(hotspots.uncached_setup(rows=700, payload=64))
            original = list(db.iterdump())
            for name, query, expected in hotspots.uncached_workloads(rows=700, lookups=300):
                with self.subTest(name=name):
                    self.assertEqual(hotspots.section_of(name), "uncached_reads")
                    db.execute("BEGIN")
                    row = db.execute(query).fetchone()
                    if query.startswith("UPDATE"):
                        row = db.execute("SELECT changes()").fetchone()
                    self.assertEqual(str(row[0]), expected)
                    db.rollback()
            self.assertEqual(list(db.iterdump()), original)
        self.assertEqual(len(hotspots.uncached_workloads()), 5)

    def test_uncached_section_is_reported_and_gated(self):
        names = [name for name, _, _ in hotspots.uncached_workloads(64, 8)]
        samples = {"baseline": [{name: 100000 for name in names}],
                   "candidate": [{name: 200000 for name in names}],
                   "stock": [{name: 50000 for name in names}]}
        with tempfile.TemporaryDirectory() as directory:
            result, raw = Path(directory)/"results.tsv", Path(directory)/"samples.tsv"
            report = io.StringIO()
            with contextlib.redirect_stdout(report):
                hotspots.write_results(samples, result, raw)
            section = report.getvalue().split("### Uncached Reads\n", 1)[1]
            self.assertIn("https://github.com/dolthub/doltlite/issues/3408", section)
            self.assertEqual(section.count("| uncached_"), 5)
            parsed, _ = benchmark_compare.parse_input_artifact(f"hotspots={result}")
            analysis = benchmark_compare.analyze(parsed, 1.25, 1.15, 10000)
            self.assertIn(("hotspots", "uncached_reads"), analysis["section_failures"])

    def test_retired_corpus_categories_and_shared_fixtures(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(hotspots, 'sql') as sql:
            fixtures = hotspots.prepare_retained(
                {'baseline': 'base', 'candidate': 'new', 'stock': 'stock'},
                Path(directory), hotspots.TEST_DIR/'performance-hotspot-seeds')
        counts = {category: 0 for category in ('narrow_rows', 'zero_row_updates',
                                              'in_transaction_mutations', 'integer_keys',
                                              'bulk_writes', 'small_cache', 'after_deletes',
                                              'index_row_fetches', 'planner_choices', 'pending_edits')}
        for name, bundle, databases in fixtures:
            category = hotspots.section_of(name)
            counts[category] += 1
            self.assertEqual(category, bundle['category'])
            if category == 'integer_keys':
                self.assertIn(bundle['issue'], (3249, 3250))
                self.assertTrue(bundle['profile']['memory'])
            elif bundle['issue'] not in (3352, 3427):
                self.assertGreaterEqual(bundle['discovery']['minimum_ratio'], 3)
            self.assertNotIn('COMMIT', bundle['case']['sql'])
            if category == 'narrow_rows':
                self.assertIn(bundle['profile']['payload'], (256, 1024))
            elif category == 'zero_row_updates':
                self.assertTrue(bundle['expected'].startswith('0|'))
        self.assertEqual(counts, {'narrow_rows': 5, 'zero_row_updates': 2,
                                 'in_transaction_mutations': 6, 'integer_keys': 2,
                                 'bulk_writes': 10, 'small_cache': 7, 'after_deletes': 4,
                                 'index_row_fetches': 2, 'planner_choices': 1, 'pending_edits': 1})
        self.assertEqual(sql.call_count, 69)
        grouped = {}
        for name, bundle, databases in fixtures:
            previous = grouped.setdefault(bundle['setup_sql'], databases)
            self.assertEqual(databases, previous)
        self.assertEqual(len(grouped), 31)

    def test_retained_gate_measures_fixed_batches(self):
        bundle = json.loads((hotspots.TEST_DIR/'performance-hotspot-seeds/narrow_rows_point_pk.json').read_text())
        bundle['repeats'] = 2
        expected = bundle['expected']
        output = f'WARM\n{expected}\nMEASURE\n' + (f'{expected}\nRun Time: real 0.008 user 0.008 sys 0.0\n'*2) + 'END\n'
        with patch.object(hotspots, 'sql', return_value=output):
            measured = hotspots.measure_retained('engine', 'candidate', [('case', bundle, {'candidate': 'db'})])
        self.assertEqual(measured, {'case': 16000})

    def test_zero_row_updates_verify_zero_changes_and_rollback(self):
        from dataclasses import replace
        from performance_hotspot_fuzzer import Profile, fixture_sql
        paths = list((hotspots.TEST_DIR/'performance-hotspot-seeds').glob('zero_row_updates_*.json'))
        self.assertEqual(len(paths), 2)
        for path in paths:
            bundle = json.loads(path.read_text())
            profile = replace(Profile(**bundle['profile']), rows=64, start=1, width=16)
            case = bundle['case']
            with contextlib.closing(sqlite3.connect(':memory:')) as db:
                db.executescript(fixture_sql(profile))
                original = list(db.iterdump())
                for _ in range(2):
                    db.execute('BEGIN')
                    db.execute(case['prepare'])
                    self.assertEqual(db.execute('SELECT changes()').fetchone(), (2,))
                    db.execute(case['sql'])
                    result = db.execute(case['verify']).fetchone()
                    self.assertEqual(result[:2], (0, 62))
                    db.rollback()
                    self.assertEqual(list(db.iterdump()), original)

    def test_discovered_sections_are_reported_and_gated(self):
        names = [f'retained_3133_{category}_probe' for category in
                 ('narrow_rows', 'zero_row_updates', 'in_transaction_mutations', 'pending_edits')]
        samples = {'baseline': [{name: 100000 for name in names}],
                   'candidate': [{name: 200000 for name in names}],
                   'stock': [{name: 10000 for name in names}]}
        with tempfile.TemporaryDirectory() as directory:
            result, raw = Path(directory)/'results.tsv', Path(directory)/'samples.tsv'
            report = io.StringIO()
            with contextlib.redirect_stdout(report):
                hotspots.write_results(samples, result, raw)
            parsed, _ = benchmark_compare.parse_input_artifact(f'hotspots={result}')
            analysis = benchmark_compare.analyze(parsed, 1.25, 1.15, 10000)
            for name, title in zip(names, ('Narrow Rows', 'Zero Row Updates',
                                          'In Transaction with Mutations', 'Pending Edit Map')):
                category = hotspots.section_of(name)
                self.assertIn(f'### {title}\n', report.getvalue())
                self.assertIn(('hotspots', category, name), analysis['individual_failures'])
                self.assertIn(('hotspots', category), analysis['section_failures'])
            self.assertEqual(report.getvalue().count('https://github.com/dolthub/doltlite/issues/3133'), 4)

    def test_bucket_workloads_results_and_rollback(self):
        workloads = hotspots.bucket_workloads(rows=512, probes=64)
        self.assertEqual([hotspots.section_of(name) for name, *_ in workloads],
                         ["pk_rewrites"]*3 + ["text_key_fetches"]*2 + ["pending_edits"]*5)
        for name, storage, key, indexes, cache_kib, prepare, query, expected in workloads:
            with self.subTest(name=name), contextlib.closing(sqlite3.connect(":memory:")) as db:
                db.executescript(hotspots.bucket_setup(key, indexes, rows=512, payload=16))
                original = list(db.iterdump())
                db.execute("BEGIN")
                if prepare:
                    db.execute(prepare)
                row = db.execute(query).fetchone()
                if query.startswith("UPDATE"):
                    row = db.execute("SELECT changes()").fetchone()
                self.assertEqual("|".join(str(value) for value in row), expected)
                db.rollback()
                self.assertEqual(list(db.iterdump()), original)

    def test_bucket_measurement_warms_times_only_the_query_and_rolls_back(self):
        workloads = hotspots.bucket_workloads()
        timer = "Run Time: real 0.01 user 0.01 sys 0.0\n"
        outputs = [f"BEGIN {name}\n"
                   + ((timer + f"{expected}\n") if query.startswith("UPDATE")
                      else (f"{expected}\n" + timer)) * hotspots.BATCH.get(name, 1)
                   + f"END {name}\n"
                   for name, _s, _k, _i, _c, _p, query, expected in workloads]
        databases = {fixture: Path(f"{'-'.join(fixture)}.db") for fixture in hotspots.bucket_fixture_names()}
        with patch.object(hotspots, "sql", side_effect=outputs) as sql:
            measured = hotspots.measure_bucket("new", databases)
        self.assertEqual(measured, {name: 10000 * hotspots.BATCH.get(name, 1)
                                    for name, *_ in workloads})
        for call, (name, storage, key, indexes, cache_kib, prepare, query, _e) in zip(sql.call_args_list, workloads):
            db, script = call.args[1], call.args[2]
            setup = hotspots.bucket_setup(key, indexes)
            if storage == "memory":
                self.assertEqual(db, ":memory:")
                self.assertLess(script.index(setup), script.index("BEGIN;"))
            else:
                self.assertEqual(db, databases[(key, indexes)])
                self.assertNotIn(setup, script)
            self.assertIn(f"PRAGMA cache_size=-{cache_kib};", script)
            batch = hotspots.BATCH.get(name, 1)
            timed = script.split(f".print BEGIN {name}", 1)[1]
            self.assertEqual(script.count(query), batch + 1)
            self.assertEqual(timed.count(".timer on"), batch)
            body = script.split(setup, 1)[1] if storage == "memory" else script
            self.assertEqual(body.count("BEGIN;"), batch + 1)
            self.assertEqual(body.count("ROLLBACK;"), batch + 1)
            self.assertTrue(timed.rstrip().endswith(f"ROLLBACK;\n.print END {name}"))
            for rep in timed.split("BEGIN;")[1:]:
                if prepare:
                    self.assertLess(rep.index(prepare), rep.index(".timer on"))
                self.assertLess(rep.index(".timer on"), rep.index(query))
                self.assertLess(rep.index(query), rep.index(".timer off"))
                self.assertLess(rep.index(".timer off"), rep.index("ROLLBACK;"))

    def test_bucket_sections_are_reported_and_gated(self):
        names = [name for name, *_ in hotspots.bucket_workloads()]
        samples = {arm: [{name: value for name in names}] for arm, value in
                   (("baseline", 100000), ("candidate", 200000), ("stock", 50000))}
        with tempfile.TemporaryDirectory() as directory:
            result, raw = Path(directory)/"results.tsv", Path(directory)/"samples.tsv"
            report = io.StringIO()
            with contextlib.redirect_stdout(report):
                hotspots.write_results(samples, result, raw)
            text = report.getvalue()
            parsed, _ = benchmark_compare.parse_input_artifact(f"hotspots={result}")
            analysis = benchmark_compare.analyze(parsed, 1.25, 1.15, 10000)
        for section, issue, count in (("pk_rewrites", 3419, 3), ("text_key_fetches", 3492, 2),
                                      ("pending_edits", 3418, 5)):
            body = text.split(f"### {dict(hotspots.SECTIONS)[section]}\n", 1)[1].split("### ", 1)[0]
            self.assertIn(f"https://github.com/dolthub/doltlite/issues/{issue}", body)
            self.assertEqual(sum(1 for name in names
                                 if f"| {name} |" in body or f"| {name} ×" in body), count)
            self.assertIn(("hotspots", section), analysis["section_failures"])

    def test_transaction_mutations_preserve_storage_mode_and_timing(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(hotspots, 'sql') as sql:
            fixtures = hotspots.prepare_retained(
                {'baseline': 'base', 'candidate': 'new', 'stock': 'stock'}, Path(directory))
            active = [fixture for fixture in fixtures
                      if fixture[1]['category'] in ('in_transaction_mutations', 'after_deletes')]
            self.assertEqual(active, [])
            retired = hotspots.prepare_retained(
                {'baseline': 'base', 'candidate': 'new', 'stock': 'stock'},
                Path(directory), hotspots.TEST_DIR/'performance-hotspot-seeds')
            active = [fixture for fixture in retired if fixture[1]['issue'] == 3352]
            self.assertEqual(len(active), 1)
            bulk = [fixture for fixture in retired if fixture[1]['issue'] == 3301]
            self.assertEqual(len(bulk), 1)
            self.assertTrue(bulk[0][1]['profile']['memory'])
            fixtures = [fixture for fixture in retired
                         if fixture[1]['category'] == 'in_transaction_mutations']
            self.assertEqual(len(fixtures), 6)
            self.assertEqual(sum(b['profile']['memory'] for _, b, _ in fixtures), 2)
            for name, bundle, databases in fixtures + active + bulk:
                self.assertEqual(hotspots.section_of(name), bundle['category'])
                self.assertIn(bundle['category'], ('in_transaction_mutations', 'bulk_writes', 'after_deletes'))
                self.assertEqual(bundle['profile']['memory'], databases['candidate']==':memory:')
                expected, repeats = bundle['expected'], bundle['repeats']
                sql.return_value = (f'WARM\n{expected}\nMEASURE\n' +
                    f'Run Time: real 0.01 user 0.01 sys 0.0\n{expected}\n'*repeats + 'END\n')
                self.assertEqual(hotspots.measure_retained('new', 'candidate', [(name, bundle, databases)]),
                                 {name: repeats*10000})
                script = sql.call_args.args[2]
                if bundle['profile']['memory']:
                    self.assertLess(script.index(bundle['setup_sql']), script.index('.print WARM'))
                timed = script.split('.print MEASURE\n', 1)[1]
                self.assertLess(timed.index('BEGIN;'), timed.index(bundle['case']['prepare']))
                self.assertLess(timed.index(bundle['case']['prepare']), timed.index('.timer on'))
                self.assertLess(timed.index('.timer on'), timed.index(bundle['case']['sql']))
                self.assertLess(timed.index(bundle['case']['sql']), timed.index('.timer off'))
                self.assertLess(timed.index('.timer off'), timed.index('ROLLBACK;'))
                self.assertEqual(timed.count('.timer on'), repeats)
                self.assertEqual(timed.count('ROLLBACK;'), repeats)

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
            statuses = ("success", "failure", "skipped", "cancelled")
            for status, measurements in itertools.product(statuses, repeat=2):
                with self.subTest(status=status, measurements=measurements):
                    env = dict(os.environ, PATH=f"{root / 'bin'}:{os.environ['PATH']}",
                               RUNNER_TEMP=str(root), GITHUB_STEP_SUMMARY=str(root / "summary"),
                               GITHUB_OUTPUT=str(root / "output"))
                    script = report.replace("${{ needs.hotspots.result }}", status)
                    script = script.replace("${{ steps.measurements.outcome }}", measurements)
                    subprocess.run(["bash", "-e", "-o", "pipefail", "-c", script],
                                   cwd=root, env=env, check=True, capture_output=True)
                    passed = status == measurements == "success"
                    self.assertEqual((root / "output").read_text().splitlines()[-1],
                                     f"report_rc={0 if passed else 1}")
                    summary = (root / "benchmark-results/summary.md").read_text()
                    self.assertEqual("unable to download the latest benchmark measurements" in summary,
                                     measurements != "success")
                    self.assertEqual("FAILED:** performance hotspots" in summary,
                                     status != "success")

    def test_hotspot_comment_summary(self):
        workflow = (hotspots.TEST_DIR.parent / ".github/workflows/benchmark.yml").read_text()
        step = workflow.split("    - name: Publish hotspot measurements\n", 1)[1]
        script = textwrap.dedent(step.split("      run: |\n", 1)[1].split("\n    - name:", 1)[0])
        for key, value in (("github.server_url", "https://github.com"),
                           ("github.repository", "dolthub/doltlite"),
                           ("github.run_id", "123")):
            script = script.replace("${{ " + key + " }}", value)
        for status, measured, counters in (("success", True, "success"), ("failure", True, "success"),
                                           ("failure", False, "success"), ("success", True, "failure")):
            with self.subTest(status=status, measured=measured, counters=counters), \
                    tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                results = root / "hotspot-results"
                if measured:
                    results.mkdir()
                    (results / "hotspots.md").write_text("## Performance hotspots\n\nMeasured table\n")
                if counters == "failure":
                    results.mkdir(exist_ok=True)
                    (results / "counters.txt").write_text(
                        "w record_bytes: base=10 cand=0 ratio=0.000 diff\n"
                        "w sortkey_parse: base=100 cand=120 ratio=1.200 REGRESS\n"
                        "engine counters: 1 workloads, failures 1\n")
                env = dict(os.environ, RUNNER_TEMP=str(root), GITHUB_STEP_SUMMARY=str(root / "summary"))
                subprocess.run(["bash", "-e", "-o", "pipefail", "-c",
                                script.replace("${{ steps.hotspots.outcome }}", status)
                                      .replace("${{ steps.counters.outcome }}", counters)],
                               env=env, check=True, capture_output=True)
                body = (results / "summary.md").read_text()
                self.assertIn(f"**Engine counters:** {counters}", body)
                if counters == "failure":
                    self.assertIn("w sortkey_parse: base=100 cand=120 ratio=1.200 REGRESS", body)
                    self.assertNotIn("record_bytes", body)
                    self.assertIn("Measured table", body)
                self.assertEqual(body, (root / "summary").read_text())
                self.assertIn("<!-- benchmark:hotspots -->", body)
                self.assertIn(f"**Gate:** {status}", body)
                self.assertIn("https://github.com/dolthub/doltlite/actions/runs/123", body)
                self.assertIn("Measured table" if measured else "No complete hotspot measurements", body)

    def test_counter_regressions_report_then_fail_the_job(self):
        workflow = (hotspots.TEST_DIR.parent / ".github/workflows/benchmark.yml").read_text()
        job = workflow.split("  hotspots:\n", 1)[1].split("\n  relative-report:", 1)[0]
        steps = re.findall(r"^    - name: (.+)$", job, re.MULTILINE)
        counters = job.split("    - name: Gate deterministic engine counters\n", 1)[1].split("\n    - name:", 1)[0]
        self.assertIn("id: counters", counters)
        self.assertIn("continue-on-error: true", counters)
        self.assertIn("set -o pipefail", counters)
        self.assertIn('tee "$RUNNER_TEMP/hotspot-results/counters.txt"', counters)
        self.assertLess(steps.index("Gate deterministic engine counters"),
                        steps.index("Measure and gate performance hotspots"))
        self.assertEqual(steps[-1], "Fail on engine counter regressions")
        last = job.split("    - name: Fail on engine counter regressions\n", 1)[1]
        self.assertIn("if: ${{ steps.counters.outcome == 'failure' }}", last)
        self.assertIn("exit 1", last)

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
