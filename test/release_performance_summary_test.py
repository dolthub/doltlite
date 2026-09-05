#!/usr/bin/env python3

import importlib.util
import pathlib
import unittest


MODULE_PATH = (
    pathlib.Path(__file__).parents[1]
    / "tool"
    / "release-performance-summary.py"
)
SPEC = importlib.util.spec_from_file_location(
    "release_performance_summary", MODULE_PATH
)
summary = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(summary)


REPORT = """# Performance

## SQL workload summary

### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 10s | 11s | 1.1× | 1.4% | **PASS** |
| Writes | 2s | 3s | 1.5× | 1.2% | **PASS** |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio | Paired-ratio noise | Result |
|---|---:|---:|---:|---:|---|
| Reads | 12s | 11s | 0.9× | 1.5% | **PASS** |
| Autocommit writes | 800ms | 3s | 3.8× | 6.3% | **PASS** |

<details>
"""


class ReleasePerformanceSummaryTest(unittest.TestCase):
    def test_keeps_summary_timings_and_ratio(self):
        self.assertEqual(
            summary.render_summary(REPORT),
            """### In-memory

| Operation | SQLite median total | DoltLite median total | Ratio |
| --- | ---: | ---: | ---: |
| Reads | 10s | 11s | 1.1× |
| Writes | 2s | 3s | 1.5× |

### File-backed

| Operation | SQLite median total | DoltLite median total | Ratio |
| --- | ---: | ---: | ---: |
| Reads | 12s | 11s | 0.9× |
| Autocommit writes | 800ms | 3s | 3.8× |
""",
        )

    def test_requires_both_tables(self):
        with self.assertRaisesRegex(ValueError, "File-backed"):
            summary.render_summary(REPORT.replace("### File-backed", "### Disk"))

    def test_rejects_changed_columns(self):
        with self.assertRaisesRegex(ValueError, "unexpected columns"):
            summary.render_summary(REPORT.replace(" | Result |", " | Status |", 1))


if __name__ == "__main__":
    unittest.main()
