#!/usr/bin/env python3

import argparse
import dataclasses
import pathlib
import sys
from collections import defaultdict


@dataclasses.dataclass(frozen=True)
class Result:
    suite: str
    section: str
    test: str
    baseline_us: int
    candidate_us: int

    @property
    def valid(self):
        return self.baseline_us > 0 and self.candidate_us >= 0

    @property
    def ratio(self):
        if not self.valid:
            return None
        return self.candidate_us / self.baseline_us

    @property
    def delta_us(self):
        if not self.valid:
            return None
        return self.candidate_us - self.baseline_us


def parse_input(spec):
    if "=" not in spec:
        raise ValueError(f"input must be SUITE=PATH: {spec}")
    suite, path_text = spec.split("=", 1)
    if not suite or not path_text:
        raise ValueError(f"input must be SUITE=PATH: {spec}")
    path = pathlib.Path(path_text)
    results = []
    seen = set()
    with path.open(encoding="utf-8") as source:
        for line_number, line in enumerate(source, 1):
            columns = line.rstrip("\n").split("\t")
            if len(columns) != 4:
                raise ValueError(
                    f"{path}:{line_number}: expected 4 tab-separated columns"
                )
            section, test, baseline, candidate = columns
            key = (section, test)
            if key in seen:
                raise ValueError(
                    f"{path}:{line_number}: duplicate result {section}/{test}"
                )
            seen.add(key)
            try:
                baseline_us = int(baseline)
                candidate_us = int(candidate)
            except ValueError as exc:
                raise ValueError(
                    f"{path}:{line_number}: timings must be integers"
                ) from exc
            results.append(
                Result(
                    suite=suite,
                    section=section,
                    test=test,
                    baseline_us=baseline_us,
                    candidate_us=candidate_us,
                )
            )
    if not results:
        raise ValueError(f"{path}: no benchmark results")
    return results


def total_ratio(results):
    if not results or any(not result.valid for result in results):
        return None
    baseline = sum(result.baseline_us for result in results)
    candidate = sum(result.candidate_us for result in results)
    if baseline <= 0:
        return None
    return candidate / baseline


def total_delta(results):
    if not results or any(not result.valid for result in results):
        return None
    return sum(result.candidate_us - result.baseline_us for result in results)


def format_time(value):
    if value is None or value < 0:
        return "invalid"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}s"
    if value >= 1_000:
        return f"{value / 1_000:.2f}ms"
    return f"{value}us"


def format_delta(value):
    if value is None:
        return "invalid"
    sign = "+" if value >= 0 else "-"
    return f"{sign}{format_time(abs(value))}"


def format_ratio(value):
    if value is None:
        return "invalid"
    return f"{value:.3f}x"


def analyze(
    results,
    individual_ratio,
    aggregate_ratio,
    min_delta_us,
    individual_overrides=None,
):
    individual_overrides = individual_overrides or {}
    individual_failures = set()
    for result in results:
        key = (result.suite, result.section, result.test)
        result_ratio, result_delta = individual_overrides.get(
            result.suite,
            (individual_ratio, min_delta_us),
        )
        if not result.valid:
            individual_failures.add(key)
        elif (
            result.ratio > result_ratio
            and result.delta_us > result_delta
        ):
            individual_failures.add(key)

    sections = defaultdict(list)
    suites = defaultdict(list)
    for result in results:
        sections[(result.suite, result.section)].append(result)
        suites[result.suite].append(result)

    section_ratios = {
        key: total_ratio(group) for key, group in sections.items()
    }
    suite_ratios = {
        key: total_ratio(group) for key, group in suites.items()
    }
    section_failures = {
        key
        for key, ratio in section_ratios.items()
        if ratio is None
        or (
            ratio > aggregate_ratio
            and total_delta(sections[key]) > min_delta_us
        )
    }
    suite_failures = {
        key
        for key, ratio in suite_ratios.items()
        if ratio is None
        or (
            ratio > aggregate_ratio
            and total_delta(suites[key]) > min_delta_us
        )
    }
    overall_ratio = total_ratio(results)
    overall_failed = (
        overall_ratio is None
        or (
            overall_ratio > aggregate_ratio
            and total_delta(results) > min_delta_us
        )
    )
    failed = bool(
        individual_failures
        or section_failures
        or suite_failures
        or overall_failed
    )
    return {
        "failed": failed,
        "individual_failures": individual_failures,
        "section_failures": section_failures,
        "suite_failures": suite_failures,
        "section_ratios": section_ratios,
        "suite_ratios": suite_ratios,
        "overall_ratio": overall_ratio,
    }


def render(
    results,
    analysis,
    baseline_ref,
    candidate_ref,
    individual_ratio,
    aggregate_ratio,
    min_delta_us,
    individual_overrides=None,
):
    suites = defaultdict(list)
    for result in results:
        suites[result.suite].append(result)

    status = "PASS" if not analysis["failed"] else "FAIL"
    lines = [
        "<!-- benchmark:relative -->",
        "## DoltLite performance vs PR base",
        "",
        f"- **Baseline:** `{baseline_ref}`",
        f"- **Candidate:** `{candidate_ref}`",
        f"- **Overall:** {format_ratio(analysis['overall_ratio'])} — **{status}**",
        (
            f"- **Gates:** individual > {individual_ratio:.2f}x with more "
            f"than {format_time(min_delta_us)} regression; section, suite, "
            f"or overall > {aggregate_ratio:.2f}x with the same minimum delta"
        ),
    ]
    for suite, (ratio, delta) in sorted((individual_overrides or {}).items()):
        lines.append(
            f"- **{suite} individual gate:** > {ratio:.2f}x with more than "
            f"{format_time(delta)} regression"
        )
    lines.extend(
        [
            "",
            "| Suite | Workloads | Baseline total | Candidate total | Ratio | Result |",
            "|---|---:|---:|---:|---:|---|",
        ]
    )
    for suite in sorted(suites):
        group = suites[suite]
        ratio = analysis["suite_ratios"][suite]
        suite_status = "FAIL" if suite in analysis["suite_failures"] else "PASS"
        baseline_total = (
            sum(result.baseline_us for result in group)
            if all(result.valid for result in group)
            else None
        )
        candidate_total = (
            sum(result.candidate_us for result in group)
            if all(result.valid for result in group)
            else None
        )
        lines.append(
            f"| {suite} | {len(group)} | "
            f"{format_time(baseline_total)} | "
            f"{format_time(candidate_total)} | "
            f"{format_ratio(ratio)} | {suite_status} |"
        )

    for suite in sorted(suites):
        lines.extend(
            [
                "",
                "<details>",
                f"<summary>{suite} details</summary>",
                "",
                "| Section | Test | Baseline | Candidate | Delta | Ratio | Result |",
                "|---|---|---:|---:|---:|---:|---|",
            ]
        )
        for result in suites[suite]:
            key = (result.suite, result.section, result.test)
            row_status = (
                "FAIL"
                if key in analysis["individual_failures"]
                else "PASS"
            )
            delta = format_delta(result.delta_us)
            lines.append(
                f"| {result.section} | `{result.test}` | "
                f"{format_time(result.baseline_us)} | "
                f"{format_time(result.candidate_us)} | {delta} | "
                f"{format_ratio(result.ratio)} | {row_status} |"
            )
        lines.extend(["", "</details>"])

    if analysis["failed"]:
        lines.extend(
            [
                "",
                "**FAILED:** Candidate performance exceeded a regression gate.",
            ]
        )
    else:
        lines.extend(["", "All relative performance gates passed."])
    lines.append("")
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Compare paired DoltLite benchmark medians"
    )
    parser.add_argument(
        "--input",
        action="append",
        required=True,
        help="Benchmark result in SUITE=PATH form; may be repeated",
    )
    parser.add_argument("--baseline-ref", required=True)
    parser.add_argument("--candidate-ref", required=True)
    parser.add_argument("--individual-ratio", type=float, default=1.25)
    parser.add_argument("--aggregate-ratio", type=float, default=1.15)
    parser.add_argument("--min-delta-us", type=int, default=5000)
    parser.add_argument(
        "--suite-individual",
        action="append",
        default=[],
        metavar="SUITE=RATIO:MIN_DELTA_US",
        help="Override the individual gate for one suite; may be repeated",
    )
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)

    try:
        results = []
        for spec in args.input:
            results.extend(parse_input(spec))
    except (OSError, ValueError) as exc:
        parser.error(str(exc))

    individual_overrides = {}
    try:
        for spec in args.suite_individual:
            suite, value = spec.split("=", 1)
            ratio, delta = value.split(":", 1)
            individual_overrides[suite] = (float(ratio), int(delta))
    except ValueError:
        parser.error(
            "--suite-individual must be SUITE=RATIO:MIN_DELTA_US"
        )

    analysis = analyze(
        results,
        args.individual_ratio,
        args.aggregate_ratio,
        args.min_delta_us,
        individual_overrides,
    )
    report = render(
        results,
        analysis,
        args.baseline_ref,
        args.candidate_ref,
        args.individual_ratio,
        args.aggregate_ratio,
        args.min_delta_us,
        individual_overrides,
    )
    pathlib.Path(args.output).write_text(report, encoding="utf-8")
    print(report, end="")
    return 1 if analysis["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
