#!/usr/bin/env python3

import argparse
import csv
import pathlib
import statistics
import sys
from dataclasses import dataclass


SYSBENCH_SUITES = ("int", "textpk", "blobpk", "compositepk")
ALL_SUITES = SYSBENCH_SUITES + ("vc",)
SQL_SUMMARY_GROUPS = (
    ("In-memory", (("Reads", "mem_reads"), ("Writes", "mem_writes"))),
    (
        "File-backed",
        (
            ("Reads", "file_reads"),
            ("Writes", "file_writes"),
            ("Autocommit writes", "ac_writes"),
        ),
    ),
)
KEY_SHAPE_GROUPS = (
    ("In-memory", "Reads", "mem_reads"),
    ("In-memory", "Writes", "mem_writes"),
    ("File-backed", "Reads", "file_reads"),
    ("File-backed", "Writes", "file_writes"),
    ("File-backed", "Autocommit reads", "ac_reads"),
    ("File-backed", "Autocommit writes", "ac_writes"),
)
SYSBENCH_SECTIONS = tuple(
    section for _, _, section in KEY_SHAPE_GROUPS
)
SAMPLE_HEADER = (
    "section",
    "test",
    "run",
    "baseline_us",
    "candidate_us",
)


@dataclass(frozen=True)
class Result:
    section: str
    test: str
    baseline_us: int
    candidate_us: int


@dataclass(frozen=True)
class Sample:
    run: int
    baseline_us: int
    candidate_us: int


@dataclass(frozen=True)
class Suite:
    name: str
    results: tuple
    samples: dict
    status: int
    duration_seconds: int


def read_integer(path, description):
    try:
        value = int(path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError) as exc:
        raise ValueError(f"invalid {description}: {path}") from exc
    if value < 0:
        raise ValueError(f"negative {description}: {path}")
    return value


def read_results(path):
    results = []
    try:
        rows = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise ValueError(f"unable to read results: {path}") from exc
    for line_number, line in enumerate(rows, 1):
        columns = line.split("\t")
        if len(columns) != 4:
            raise ValueError(
                f"{path}:{line_number}: expected four tab-separated columns"
            )
        section, test, baseline_text, candidate_text = columns
        try:
            baseline = int(baseline_text)
            candidate = int(candidate_text)
        except ValueError as exc:
            raise ValueError(
                f"{path}:{line_number}: timing is not an integer"
            ) from exc
        if not section or not test or baseline <= 0 or candidate < 0:
            raise ValueError(f"{path}:{line_number}: invalid result")
        results.append(Result(section, test, baseline, candidate))
    if not results:
        raise ValueError(f"empty results: {path}")
    if len({(result.section, result.test) for result in results}) != len(
        results
    ):
        raise ValueError(f"duplicate result: {path}")
    return tuple(results)


def read_samples(path):
    samples = {}
    try:
        handle = path.open(newline="", encoding="utf-8")
    except OSError as exc:
        raise ValueError(f"unable to read samples: {path}") from exc
    with handle:
        reader = csv.reader(handle, delimiter="\t")
        try:
            header = tuple(next(reader))
        except StopIteration as exc:
            raise ValueError(f"empty samples: {path}") from exc
        if header != SAMPLE_HEADER:
            raise ValueError(f"unexpected sample header: {path}")
        for line_number, columns in enumerate(reader, 2):
            if len(columns) != 5:
                raise ValueError(
                    f"{path}:{line_number}: expected five columns"
                )
            section, test, run_text, baseline_text, candidate_text = columns
            try:
                sample = Sample(
                    int(run_text),
                    int(baseline_text),
                    int(candidate_text),
                )
            except ValueError as exc:
                raise ValueError(
                    f"{path}:{line_number}: sample is not an integer"
                ) from exc
            if (
                not section
                or not test
                or sample.run <= 0
                or sample.baseline_us < 0
                or sample.candidate_us < 0
            ):
                raise ValueError(f"{path}:{line_number}: invalid sample")
            samples.setdefault((section, test), []).append(sample)
    if not samples:
        raise ValueError(f"no samples: {path}")
    for key, values in samples.items():
        runs = [sample.run for sample in values]
        if runs != list(range(1, len(values) + 1)):
            raise ValueError(f"non-contiguous sample runs for {key}: {path}")
    return {key: tuple(values) for key, values in samples.items()}


def load_suite(results_dir, name):
    results = read_results(results_dir / f"{name}.tsv")
    samples = read_samples(results_dir / f"{name}-samples.tsv")
    result_keys = {(result.section, result.test) for result in results}
    if result_keys != set(samples):
        raise ValueError(f"result/sample workload mismatch for {name}")
    return Suite(
        name=name,
        results=results,
        samples=samples,
        status=read_integer(
            results_dir / f"{name}.status", f"{name} status"
        ),
        duration_seconds=read_integer(
            results_dir / f"{name}.duration", f"{name} duration"
        ),
    )


def median_relative_mad(values):
    median = statistics.median(values)
    if median <= 0:
        return 0.0
    mad = statistics.median(abs(value - median) for value in values)
    return 100.0 * mad / median


def workload_noise(suite, result):
    values = suite.samples[(result.section, result.test)]
    if suite.name == "vc":
        return median_relative_mad(
            [sample.candidate_us for sample in values]
        )
    ratios = []
    for sample in values:
        if sample.baseline_us <= 0:
            raise ValueError(
                f"invalid baseline sample for {suite.name}/"
                f"{result.section}/{result.test}"
            )
        ratios.append(sample.candidate_us / sample.baseline_us)
    return median_relative_mad(ratios)


def sample_count_text(suites, results=None):
    if isinstance(suites, Suite):
        suites = (suites,)
    if results is None:
        counts = sorted(
            {
                len(values)
                for suite in suites
                for values in suite.samples.values()
            }
        )
    else:
        counts = sorted(
            {
                len(suite.samples[(result.section, result.test)])
                for suite, result in results
            }
        )
    if len(counts) == 1:
        return str(counts[0])
    return f"{counts[0]}–{counts[-1]}"


def format_duration(seconds):
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m {seconds}s"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def format_time(microseconds):
    if microseconds >= 1_000_000:
        return f"{microseconds / 1_000_000:.2f}s"
    if microseconds >= 1_000:
        return f"{microseconds / 1_000:.2f}ms"
    return f"{microseconds}µs"


def escape_markdown(value):
    return value.replace("|", "\\|").replace("`", "\\`")


def individual_limit(result):
    return 6.0 if result.section == "ac_writes" else 2.4


def average_limit(section):
    return 5.0 if section == "ac_writes" else 1.95


def section_results(suites, section):
    return tuple(
        (suite, result)
        for suite in suites
        for result in suite.results
        if result.section == section
    )


def section_passes(suites, section):
    for suite in suites:
        results = [
            result for result in suite.results if result.section == section
        ]
        ratios = [
            result.candidate_us / result.baseline_us for result in results
        ]
        if not ratios:
            return False
        if any(
            ratio > individual_limit(result)
            for ratio, result in zip(ratios, results)
        ):
            return False
        average = float(f"{statistics.mean(ratios):.2f}")
        if average > average_limit(section):
            return False
    return True


def report_passes(suites):
    if any(suite.status != 0 for suite in suites):
        return False
    by_name = {suite.name: suite for suite in suites}
    sysbench_suites = tuple(by_name[name] for name in SYSBENCH_SUITES)
    if not all(
        section_passes(sysbench_suites, section)
        for section in SYSBENCH_SECTIONS
    ):
        return False
    return vc_passes(by_name["vc"])


def vc_passes(suite):
    return suite.status == 0 and all(
        result.candidate_us <= result.baseline_us
        for result in suite.results
    )


def render_section_summary(suites, section, prefix):
    results = section_results(suites, section)
    if not results:
        raise ValueError(f"missing results for section: {section}")
    baseline = sum(result.baseline_us for _, result in results)
    candidate = sum(result.candidate_us for _, result in results)
    ratio = candidate / baseline
    result = "PASS" if section_passes(suites, section) else "FAIL"
    return (
        f"| {prefix} | {len(results)} | "
        f"{sample_count_text(suites, results)} | "
        f"{format_time(baseline)} | {format_time(candidate)} | "
        f"{ratio:.3f}× | "
        f"{statistics.median(workload_noise(*item) for item in results):.2f}% "
        f"| **{result}** |"
    )


def render_sql_summary(suites):
    lines = []
    for storage, operations in SQL_SUMMARY_GROUPS:
        lines.extend(
            [
                f"### {storage}",
                "",
                "| Operation | Workloads | Samples/workload | "
                "SQLite median total | DoltLite median total | Ratio | "
                "Paired-ratio noise | Result |",
                "|---|---:|---:|---:|---:|---:|---:|---|",
            ]
        )
        for operation, section in operations:
            lines.append(
                render_section_summary(suites, section, operation)
            )
        lines.append("")
    return lines


def render_key_shape_breakdown(suites):
    lines = [
        "<details>",
        "<summary>Key-shape and individual-workload breakdown</summary>",
        "",
        "The integer, text, blob, and composite primary-key runs verify "
        "that performance holds across key shapes.",
        "",
        "| Storage | Operation | Key shape | Workloads | Samples/workload | "
        "SQLite median total | DoltLite median total | Ratio | "
        "Paired-ratio noise | Result |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for storage, operation, section in KEY_SHAPE_GROUPS:
        for suite in suites:
            prefix = f"{storage} | {operation} | {suite.name}"
            lines.append(
                render_section_summary((suite,), section, prefix)
            )
    lines.append("")
    for suite in suites:
        lines.extend(render_sysbench_details(suite))
    lines.extend(["</details>", ""])
    return lines


def render_sysbench_details(suite):
    lines = [
        "<details>",
        f"<summary>{suite.name} workload details</summary>",
        "",
        "| Section | Workload | SQLite median | DoltLite median | "
        "Ratio | Paired-ratio noise | Result |",
        "|---|---|---:|---:|---:|---:|---|",
    ]
    for result in suite.results:
        ratio = result.candidate_us / result.baseline_us
        status = "PASS" if ratio <= individual_limit(result) else "FAIL"
        lines.append(
            f"| {escape_markdown(result.section)} | "
            f"`{escape_markdown(result.test)}` | "
            f"{format_time(result.baseline_us)} | "
            f"{format_time(result.candidate_us)} | {ratio:.3f}× | "
            f"{workload_noise(suite, result):.2f}% | {status} |"
        )
    lines.extend(["", "</details>", ""])
    return lines


def render_vc(suite):
    lines = [
        "## Version-control latency",
        "",
        f"Wall time: {format_duration(suite.duration_seconds)}. "
        f"Samples per benchmark: {sample_count_text(suite)}.",
        "",
        "| Benchmark | Median | Ceiling | Ceiling used | MAD | Result |",
        "|---|---:|---:|---:|---:|---|",
    ]
    for result in suite.results:
        used = result.candidate_us / result.baseline_us
        status = "PASS" if used <= 1.0 else "FAIL"
        lines.append(
            f"| `{escape_markdown(result.test)}` | "
            f"{format_time(result.candidate_us)} | "
            f"{format_time(result.baseline_us)} | {used:.1%} | "
            f"{workload_noise(suite, result):.2f}% | {status} |"
        )
    result = "PASS" if vc_passes(suite) else "FAIL"
    lines.extend(["", f"Version-control ceiling result: **{result}**.", ""])
    return lines


def render_report(suites, commit, run_url, generated_at, runner):
    overall = "PASS" if report_passes(suites) else "FAIL"
    by_name = {suite.name: suite for suite in suites}
    lines = [
        "# DoltLite Performance Report",
        "",
        f"> Nightly result: **{overall}**",
        ">",
        f"> Generated: {generated_at}",
        ">",
        f"> Commit: [`{commit}`]"
        f"(https://github.com/dolthub/doltlite/commit/{commit})",
        ">",
        f"> Runner: {runner}",
        ">",
        f"> [GitHub Actions run]({run_url})",
        "",
        "This report compares optimized DoltLite against stock SQLite on the "
        "same GitHub-hosted runner. Baseline and candidate execution order "
        "alternates on each repetition. Reported timings are medians. "
        "Paired-ratio noise is the median absolute deviation of the paired "
        "DoltLite/SQLite ratios, expressed as a percentage.",
        "",
        "## SQL workload summary",
        "",
        "The primary view aggregates all key shapes and compares DoltLite "
        "with SQLite by storage mode and operation class.",
        "",
    ]
    sysbench_suites = tuple(by_name[name] for name in SYSBENCH_SUITES)
    lines.extend(render_sql_summary(sysbench_suites))
    lines.extend(
        [
            "The absolute ceiling is 2.4× per ordinary workload and 1.95× "
            "for a section average. Durable autocommit writes use 6.0× "
            "and 5.0× ceilings respectively.",
            "",
        ]
    )
    lines.extend(render_key_shape_breakdown(sysbench_suites))
    lines.extend(render_vc(by_name["vc"]))
    lines.extend(
        [
            "## Reproducing",
            "",
            "The workload definitions live in `test/sysbench_compare*.sh` "
            "and `test/vc_perf_ceiling.sh`. The nightly workflow retains "
            "the complete raw samples and generated reports as Actions "
            "artifacts for 30 days.",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-dir", type=pathlib.Path, required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--run-url", required=True)
    parser.add_argument("--generated-at", required=True)
    parser.add_argument(
        "--runner", default="GitHub Actions ubuntu-latest"
    )
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--result-output", type=pathlib.Path)
    return parser.parse_args(argv)


def remove_output(path):
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def main(argv=None):
    args = parse_args(argv)
    outputs = [args.output]
    if args.result_output is not None:
        outputs.append(args.result_output)
    try:
        if len(set(outputs)) != len(outputs):
            raise ValueError("output paths must be distinct")
        for path in outputs:
            remove_output(path)
        suites = [
            load_suite(args.results_dir, name) for name in ALL_SUITES
        ]
        report = render_report(
            suites,
            args.commit,
            args.run_url,
            args.generated_at,
            args.runner,
        )
        args.output.write_text(report, encoding="utf-8")
        if args.result_output is not None:
            result = "PASS" if report_passes(suites) else "FAIL"
            args.result_output.write_text(f"{result}\n", encoding="utf-8")
    except (OSError, ValueError) as exc:
        for path in outputs:
            try:
                remove_output(path)
            except OSError:
                pass
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
