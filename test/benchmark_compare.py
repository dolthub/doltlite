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


def parse_attempt_history(spec):
    if "=" not in spec:
        raise ValueError(f"attempt history must be SUITE=PATH: {spec}")
    suite, path_text = spec.split("=", 1)
    if not suite or not path_text:
        raise ValueError(f"attempt history must be SUITE=PATH: {spec}")
    path = pathlib.Path(path_text)
    statuses = []
    with path.open(encoding="utf-8") as source:
        header = source.readline().rstrip("\n")
        if header != "attempt\tstatus":
            raise ValueError(f"{path}: invalid attempt history header")
        for line_number, line in enumerate(source, 2):
            columns = line.rstrip("\n").split("\t")
            if len(columns) != 2:
                raise ValueError(
                    f"{path}:{line_number}: expected attempt and status"
                )
            attempt, status = columns
            if attempt != str(len(statuses) + 1):
                raise ValueError(
                    f"{path}:{line_number}: attempts must be consecutive"
                )
            if status not in ("pass", "regression"):
                raise ValueError(
                    f"{path}:{line_number}: invalid status {status}"
                )
            statuses.append(status)
    if not statuses:
        raise ValueError(f"{path}: no benchmark attempts")
    if statuses[-1] == "pass":
        if any(status != "regression" for status in statuses[:-1]):
            raise ValueError(f"{path}: pass must end the attempt history")
    elif len(statuses) != 3 or any(
        status != "regression" for status in statuses
    ):
        raise ValueError(
            f"{path}: final regression requires three consecutive failures"
        )
    return suite, statuses


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
        "overall_failed": overall_failed,
    }


def validate_retry_confirmation(results, analysis, attempt_histories):
    result_suites = {result.suite for result in results}
    history_suites = set(attempt_histories)
    if result_suites != history_suites:
        missing = sorted(result_suites - history_suites)
        extra = sorted(history_suites - result_suites)
        details = []
        if missing:
            details.append(f"missing: {', '.join(missing)}")
        if extra:
            details.append(f"unexpected: {', '.join(extra)}")
        raise ValueError(
            "attempt histories do not match benchmark suites ("
            + "; ".join(details)
            + ")"
        )

    failed_suites = {
        suite for suite, _section, _test in analysis["individual_failures"]
    }
    failed_suites.update(
        suite for suite, _section in analysis["section_failures"]
    )
    failed_suites.update(analysis["suite_failures"])
    confirmed_suites = {
        suite
        for suite, statuses in attempt_histories.items()
        if statuses[-1] == "regression"
    }
    unconfirmed = sorted(failed_suites - confirmed_suites)
    if unconfirmed:
        raise ValueError(
            "benchmark regression lacks three-failure confirmation: "
            + ", ".join(unconfirmed)
        )
    if analysis["overall_failed"] and not confirmed_suites:
        raise ValueError(
            "overall benchmark regression lacks three-failure confirmation"
        )


def render(
    results,
    analysis,
    baseline_ref,
    candidate_ref,
    individual_ratio,
    aggregate_ratio,
    min_delta_us,
    individual_overrides=None,
    attempt_histories=None,
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
    retried = []
    for suite, statuses in sorted((attempt_histories or {}).items()):
        if len(statuses) == 1:
            continue
        if statuses[-1] == "pass":
            retried.append(
                f"{suite} passed attempt {len(statuses)} after "
                f"{len(statuses) - 1} transient gate "
                f"{'failure' if len(statuses) == 2 else 'failures'}"
            )
        else:
            retried.append(
                f"{suite} exceeded its gate in {len(statuses)} "
                "consecutive attempts"
            )
    lines.append(
        "- **Automatic retries:** "
        + ("; ".join(retried) if retried else "none")
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


def render_provenance_failure(
    baseline_ref,
    candidate_ref,
    expected_baseline_ref,
    expected_candidate_ref,
):
    return "\n".join(
        [
            "<!-- benchmark:relative -->",
            "## DoltLite performance vs PR base",
            "",
            "**FAILED:** benchmark revision provenance does not match "
            "the workflow inputs.",
            "",
            "| Revision | Packaged | Expected |",
            "|---|---|---|",
            f"| Baseline | `{baseline_ref}` | `{expected_baseline_ref}` |",
            f"| Candidate | `{candidate_ref}` | `{expected_candidate_ref}` |",
            "",
        ]
    )


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
    parser.add_argument("--expected-baseline-ref", required=True)
    parser.add_argument("--expected-candidate-ref", required=True)
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
    parser.add_argument(
        "--attempt-history",
        action="append",
        default=[],
        metavar="SUITE=PATH",
        help="Validated retry history for one suite; may be repeated",
    )
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)

    if (
        args.baseline_ref != args.expected_baseline_ref
        or args.candidate_ref != args.expected_candidate_ref
    ):
        report = render_provenance_failure(
            args.baseline_ref,
            args.candidate_ref,
            args.expected_baseline_ref,
            args.expected_candidate_ref,
        )
        pathlib.Path(args.output).write_text(report, encoding="utf-8")
        print(report, end="")
        return 1

    try:
        results = []
        for spec in args.input:
            results.extend(parse_input(spec))
        attempt_histories = {}
        for spec in args.attempt_history:
            suite, statuses = parse_attempt_history(spec)
            if suite in attempt_histories:
                raise ValueError(f"duplicate attempt history for {suite}")
            attempt_histories[suite] = statuses
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
    if attempt_histories:
        try:
            validate_retry_confirmation(
                results,
                analysis,
                attempt_histories,
            )
        except ValueError as exc:
            parser.error(str(exc))
    report = render(
        results,
        analysis,
        args.baseline_ref,
        args.candidate_ref,
        args.individual_ratio,
        args.aggregate_ratio,
        args.min_delta_us,
        individual_overrides,
        attempt_histories,
    )
    pathlib.Path(args.output).write_text(report, encoding="utf-8")
    print(report, end="")
    return 1 if analysis["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
