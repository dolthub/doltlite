#!/usr/bin/env python3

import importlib.util
import pathlib
import tempfile
import unittest


MODULE_PATH = pathlib.Path(__file__).with_name(
    "nightly_performance_report.py"
)
SPEC = importlib.util.spec_from_file_location(
    "nightly_performance_report", MODULE_PATH
)
nightly_report = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(nightly_report)


class NightlyPerformanceReportTest(unittest.TestCase):
    def write_suite(self, directory, name, status=0):
        if name == "vc":
            results = "vc\tstatus_clean\t200000\t100000\n"
            samples = [
                ("vc", "status_clean", 1, 0, 90000),
                ("vc", "status_clean", 2, 0, 100000),
                ("vc", "status_clean", 3, 0, 110000),
            ]
        else:
            results = (
                "mem_reads\tpoint\t100\t150\n"
                "ac_writes\tinsert_ac\t100\t500\n"
            )
            samples = [
                ("mem_reads", "point", 1, 100, 140),
                ("mem_reads", "point", 2, 100, 150),
                ("mem_reads", "point", 3, 100, 160),
                ("ac_writes", "insert_ac", 1, 100, 480),
                ("ac_writes", "insert_ac", 2, 100, 500),
                ("ac_writes", "insert_ac", 3, 100, 520),
            ]
        (directory / f"{name}.tsv").write_text(
            results, encoding="utf-8"
        )
        with (directory / f"{name}-samples.tsv").open(
            "w", encoding="utf-8"
        ) as handle:
            handle.write(
                "section\ttest\trun\tbaseline_us\tcandidate_us\n"
            )
            for row in samples:
                handle.write("\t".join(str(value) for value in row) + "\n")
        (directory / f"{name}.status").write_text(
            f"{status}\n", encoding="utf-8"
        )
        (directory / f"{name}.duration").write_text(
            "3723\n", encoding="utf-8"
        )

    def write_all_suites(self, directory):
        for name in nightly_report.ALL_SUITES:
            self.write_suite(directory, name)

    def test_generates_complete_pass_report(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            self.write_all_suites(directory)
            suites = [
                nightly_report.load_suite(directory, name)
                for name in nightly_report.ALL_SUITES
            ]
            report = nightly_report.render_report(
                suites,
                "abc123",
                "https://example.test/run",
                "2026-07-25 09:30 UTC",
                "ubuntu24 20260720.1",
            )
        self.assertIn("Nightly result: **PASS**", report)
        self.assertIn("| int | 2 | 3 | 1h 2m 3s |", report)
        self.assertIn("Median paired-ratio MAD", report)
        self.assertIn("Version-control latency", report)
        self.assertIn("50.0%", report)
        self.assertFalse(
            any(
                line.endswith((" ", "\t"))
                for line in report.splitlines()
            )
        )

    def test_failed_status_marks_overall_report_failed(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            self.write_all_suites(directory)
            self.write_suite(directory, "blobpk", status=1)
            suites = [
                nightly_report.load_suite(directory, name)
                for name in nightly_report.ALL_SUITES
            ]
            report = nightly_report.render_report(
                suites,
                "abc123",
                "https://example.test/run",
                "now",
                "ubuntu24",
            )
        self.assertIn("Nightly result: **FAIL**", report)
        self.assertRegex(report, r"\| blobpk .* \*\*FAIL\*\* \|")

    def test_rejects_result_without_matching_samples(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            self.write_suite(directory, "int")
            sample_path = directory / "int-samples.tsv"
            sample_path.write_text(
                sample_path.read_text(encoding="utf-8").replace(
                    "mem_reads\tpoint", "mem_reads\tother"
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "workload mismatch"):
                nightly_report.load_suite(directory, "int")

    def test_rejects_noncontiguous_sample_runs(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            self.write_suite(directory, "vc")
            sample_path = directory / "vc-samples.tsv"
            sample_path.write_text(
                sample_path.read_text(encoding="utf-8").replace(
                    "vc\tstatus_clean\t2", "vc\tstatus_clean\t4"
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "non-contiguous"):
                nightly_report.load_suite(directory, "vc")

    def test_main_writes_report(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            self.write_all_suites(directory)
            output = directory / "performance-report.md"
            rc = nightly_report.main(
                [
                    "--results-dir",
                    str(directory),
                    "--commit",
                    "abc123",
                    "--run-url",
                    "https://example.test/run",
                    "--generated-at",
                    "2026-07-25 09:30 UTC",
                    "--output",
                    str(output),
                ]
            )
            report = output.read_text(encoding="utf-8")
        self.assertEqual(rc, 0)
        self.assertIn("# DoltLite Performance Report", report)


if __name__ == "__main__":
    unittest.main()
