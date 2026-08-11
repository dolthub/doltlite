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
                "mem_writes\tinsert\t100\t175\n"
                "file_reads\tpoint\t200\t100\n"
                "file_writes\tinsert\t200\t250\n"
                "ac_reads\tpoint\t200\t210\n"
                "ac_writes\tinsert_ac\t100\t500\n"
            )
            samples = [
                ("mem_reads", "point", 1, 100, 140),
                ("mem_reads", "point", 2, 100, 150),
                ("mem_reads", "point", 3, 100, 160),
                ("mem_writes", "insert", 1, 100, 170),
                ("mem_writes", "insert", 2, 100, 175),
                ("mem_writes", "insert", 3, 100, 180),
                ("file_reads", "point", 1, 200, 90),
                ("file_reads", "point", 2, 200, 100),
                ("file_reads", "point", 3, 200, 110),
                ("file_writes", "insert", 1, 200, 240),
                ("file_writes", "insert", 2, 200, 250),
                ("file_writes", "insert", 3, 200, 260),
                ("ac_reads", "point", 1, 200, 200),
                ("ac_reads", "point", 2, 200, 210),
                ("ac_reads", "point", 3, 200, 220),
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
        self.assertIn("aggregates all key shapes", report)
        self.assertIn("### In-memory", report)
        self.assertIn("| Reads | 400µs | 600µs | 1.5× |", report)
        self.assertIn("### File-backed", report)
        self.assertIn(
            "| Autocommit writes | 400µs | 2.00ms | 5.0× |",
            report,
        )
        self.assertIn("Paired-ratio noise", report)
        self.assertNotIn("paired-ratio MAD", report)
        summary = report.split("<details>", 1)[0]
        self.assertNotIn("Workloads | Samples/workload", summary)
        self.assertNotIn("| Autocommit reads |", summary)
        self.assertNotIn("Key shape", summary)
        self.assertLess(
            summary.index("### In-memory"),
            summary.index("### File-backed"),
        )
        self.assertLess(
            summary.index("| Reads |"),
            summary.index("| Writes |"),
        )
        self.assertLess(
            summary.index("| Writes |", summary.index("### File-backed")),
            summary.index("| Autocommit writes |"),
        )
        self.assertIn(
            "<summary>Key-shape and individual-workload breakdown</summary>",
            report,
        )
        self.assertIn("Workloads | Samples/workload", report)
        self.assertIn("| In-memory | Reads | int | 1 | 3 |", report)
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

    def test_ceiling_failure_marks_report_and_machine_result_failed(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            self.write_all_suites(directory)
            results_path = directory / "int.tsv"
            results_path.write_text(
                results_path.read_text(encoding="utf-8").replace(
                    "ac_writes\tinsert_ac\t100\t500",
                    "ac_writes\tinsert_ac\t100\t700",
                ),
                encoding="utf-8",
            )
            output = directory / "performance-report.md"
            result_output = directory / "performance-report.result"
            rc = nightly_report.main(
                [
                    "--results-dir",
                    str(directory),
                    "--commit",
                    "abc123",
                    "--run-url",
                    "https://example.test/run",
                    "--generated-at",
                    "now",
                    "--output",
                    str(output),
                    "--result-output",
                    str(result_output),
                ]
            )
            report = output.read_text(encoding="utf-8")
            result = result_output.read_text(encoding="utf-8")
        self.assertEqual(rc, 0)
        self.assertIn("Nightly result: **FAIL**", report)
        self.assertIn("| Autocommit writes | 400µs | 2.20ms |", report)
        self.assertEqual(result, "FAIL\n")

    def test_audit_only_section_still_gates_report(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            self.write_all_suites(directory)
            results_path = directory / "int.tsv"
            results_path.write_text(
                results_path.read_text(encoding="utf-8").replace(
                    "ac_reads\tpoint\t200\t210",
                    "ac_reads\tpoint\t200\t600",
                ),
                encoding="utf-8",
            )
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
        summary = report.split("<details>", 1)[0]
        self.assertNotIn("| Autocommit reads |", summary)
        self.assertIn("Nightly result: **FAIL**", report)
        self.assertIn(
            "| File-backed | Autocommit reads | int | 1 | 3 | ", report
        )

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

    def test_main_removes_stale_outputs_on_generation_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            self.write_all_suites(directory)
            output = directory / "performance-report.md"
            result_output = directory / "performance-report.result"
            arguments = [
                "--results-dir",
                str(directory),
                "--commit",
                "abc123",
                "--run-url",
                "https://example.test/run",
                "--generated-at",
                "now",
                "--output",
                str(output),
                "--result-output",
                str(result_output),
            ]
            self.assertEqual(nightly_report.main(arguments), 0)
            results_path = directory / "int.tsv"
            results_path.write_text(
                "\n".join(
                    line
                    for line in results_path.read_text(
                        encoding="utf-8"
                    ).splitlines()
                    if not line.startswith("file_writes\t")
                )
                + "\n",
                encoding="utf-8",
            )
            samples_path = directory / "int-samples.tsv"
            samples_path.write_text(
                "\n".join(
                    line
                    for line in samples_path.read_text(
                        encoding="utf-8"
                    ).splitlines()
                    if not line.startswith("file_writes\t")
                )
                + "\n",
                encoding="utf-8",
            )
            self.assertEqual(nightly_report.main(arguments), 1)
            self.assertFalse(output.exists())
            self.assertFalse(result_output.exists())


if __name__ == "__main__":
    unittest.main()
