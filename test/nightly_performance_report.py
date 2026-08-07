#!/usr/bin/env python3

import argparse
import csv
import pathlib
import statistics
import sys
from dataclasses import dataclass


SYSBENCH_SUITES = ("int", "textpk", "blobpk", "compositepk")
ALL_SUITES = SYSBENCH_SUITES + ("vc",)
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


def suite_noise(suite):
    return statistics.median(
        workload_noise(suite, result) for result in suite.results
    )


def sample_count_text(suite):
    counts = sorted({len(values) for values in suite.samples.values()})
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
    return 10.0 if result.section == "ac_writes" else 2.5


def render_sysbench_summary(suite):
    baseline = sum(result.baseline_us for result in suite.results)
    candidate = sum(result.candidate_us for result in suite.results)
    ratio = candidate / baseline
    result = "PASS" if suite.status == 0 else "FAIL"
    return (
        f"| {suite.name} | {len(suite.results)} | "
        f"{sample_count_text(suite)} | "
        f"{format_duration(suite.duration_seconds)} | "
        f"{format_time(baseline)} | {format_time(candidate)} | "
        f"{ratio:.3f}× | {suite_noise(suite):.2f}% | **{result}** |"
    )


def render_sysbench_details(suite):
    lines = [
        "<details>",
        f"<summary>{suite.name} workload details</summary>",
        "",
        "| Section | Workload | SQLite median | DoltLite median | "
        "Ratio | Paired-ratio MAD | Result |",
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
    result = "PASS" if suite.status == 0 else "FAIL"
    lines.extend(["", f"Version-control ceiling result: **{result}**.", ""])
    return lines


def render_report(suites, commit, run_url, generated_at, runner):
    overall_failed = any(suite.status != 0 for suite in suites)
    overall = "FAIL" if overall_failed else "PASS"
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
        "alternates on each repetition. Reported timings are medians; MAD is "
        "the median absolute deviation and describes run-to-run noise.",
        "",
        "## SQL workload summary",
        "",
        "| Key shape | Workloads | Samples/workload | Wall time | "
        "SQLite median total | DoltLite median total | Ratio | "
        "Median paired-ratio MAD | Result |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for name in SYSBENCH_SUITES:
        lines.append(render_sysbench_summary(by_name[name]))
    lines.extend(
        [
            "",
            "The absolute ceiling is 2.4× per ordinary workload and 1.95× "
            "for a section average. Durable autocommit writes use 6.0× "
            "and 5.0× ceilings respectively.",
            "",
        ]
    )
    for name in SYSBENCH_SUITES:
        lines.extend(render_sysbench_details(by_name[name]))
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
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    try:
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
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
