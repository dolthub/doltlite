#!/usr/bin/env python3

import contextlib
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import performance_counters as counters


class ExceptionTests(unittest.TestCase):
    def test_exceptions_need_a_workload_counter_and_reason(self):
        text = ("Speed up fetches\n\n"
                "perf-counter-exception: retired_issue_3360 sortkey_parse one parse replaces a rebuild\n"
                "- perf-counter-exception: hotspot_scan compare  listed in a bullet  \n"
                "perf-counter-exception: hotspot_point seek\n"
                "see perf-counter-exception: hotspot_wide seek mid-line mentions do not count\n")
        self.assertEqual(counters.parse_exceptions(text), {
            ("retired_issue_3360", "sortkey_parse"): "one parse replaces a rebuild",
            ("hotspot_scan", "compare"): "listed in a bullet",
        })
        self.assertEqual(counters.parse_exceptions(None), {})

    def test_only_the_named_regression_is_allowed(self):
        exceptions = {("w", "sortkey_parse"): "reviewed"}
        used = set()
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            worse = counters.compare_counters(
                "w", {"sortkey_parse": 100, "seek": 100, "record_bytes": 50},
                {"sortkey_parse": 120, "seek": 120, "record_bytes": 0}, exceptions, used)
        self.assertEqual(worse, ["seek"])
        self.assertEqual(used, {("w", "sortkey_parse")})
        self.assertIn("w sortkey_parse: base=100 cand=120 ratio=1.200 ALLOWED (PR exception: reviewed)",
                      out.getvalue())
        self.assertIn("w seek: base=100 cand=120 ratio=1.200 REGRESS", out.getvalue())
        self.assertIn("w record_bytes: base=50 cand=0 ratio=0.000 diff", out.getvalue())

    def test_exception_for_another_workload_does_not_apply(self):
        with contextlib.redirect_stdout(io.StringIO()):
            worse = counters.compare_counters("w", {"seek": 100}, {"seek": 200},
                                              {("other", "seek"): "reviewed"}, set())
        self.assertEqual(worse, ["seek"])

    def run_main(self, text, cand):
        with tempfile.TemporaryDirectory() as directory:
            args = ["--baseline", "base", "--candidate", "cand"]
            if text is not None:
                path = Path(directory) / "pr.txt"
                path.write_text(text)
                args += ["--exceptions", str(path)]
            reads = {"base": {"sortkey_parse": 100}, "cand": cand}
            out = io.StringIO()
            with patch.object(counters, "stats_available", return_value=True), \
                 patch.object(counters, "workloads", return_value=[("w", "SELECT 1;")]), \
                 patch.object(counters, "read_counters",
                              side_effect=lambda binary, work, timeout: reads[str(binary)]), \
                 contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
                rc = counters.main(args)
            return rc, out.getvalue()

    def test_main_passes_with_an_exception_and_fails_without_one(self):
        regressed = {"sortkey_parse": 120}
        rc, out = self.run_main("perf-counter-exception: w sortkey_parse reviewed\n", regressed)
        self.assertEqual(rc, 0)
        self.assertIn("ALLOWED (PR exception: reviewed)", out)
        self.assertIn("engine counters: 1 workloads, failures 0", out)
        rc, out = self.run_main(None, regressed)
        self.assertEqual(rc, 1)
        self.assertIn("REGRESS", out)

    def test_main_reports_exceptions_that_allow_nothing(self):
        rc, out = self.run_main("perf-counter-exception: w seek stale\n", {"sortkey_parse": 100})
        self.assertEqual(rc, 0)
        self.assertIn("unused exception: w seek (no regression to allow)", out)


if __name__ == "__main__":
    unittest.main()
