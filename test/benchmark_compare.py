#!/usr/bin/env python3

import argparse
import dataclasses
import json
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


@dataclasses.dataclass(frozen=True)
class AttemptHistory:
    suite: str
    failures: tuple

    @property
    def confirmed_failures(self):
        if len(self.failures) != 3:
            return frozenset()
        return frozenset.intersection(*self.failures)


def validate_gate_id(path, line_number, suite, value):
    if not isinstance(value, list) or not value:
        raise ValueError(f"{path}:{line_number}: invalid gate identifier")
    expected_lengths = {"individual": 4, "section": 3, "suite": 2}
    kind = value[0]
    if kind not in expected_lengths or len(value) != expected_lengths[kind]:
        raise ValueError(f"{path}:{line_number}: invalid gate identifier")
    if not all(isinstance(part, str) and part for part in value):
        raise ValueError(f"{path}:{line_number}: invalid gate identifier")
    if value[1] != suite:
        raise ValueError(
            f"{path}:{line_number}: gate suite {value[1]} "
            f"does not match history suite {suite}"
        )
    return tuple(value)


def parse_attempt_history(spec):
    if "=" not in spec:
        raise ValueError(f"attempt history must be SUITE=PATH: {spec}")
    suite, path_text = spec.split("=", 1)
    if not suite or not path_text:
        raise ValueError(f"attempt history must be SUITE=PATH: {spec}")
    path = pathlib.Path(path_text)
    failures = []
    with path.open(encoding="utf-8") as source:
        header = source.readline().rstrip("\n")
        if header != f"suite\t{suite}":
            recorded_suite = (
                header.split("\t", 1)[1] if "\t" in header else ""
            )
            raise ValueError(
                f"{path}: history suite {recorded_suite or '<missing>'} "
                f"does not match requested suite {suite}"
            )
        columns_header = source.readline().rstrip("\n")
        if columns_header != "attempt\tstatus\tfailed_gates":
            raise ValueError(f"{path}: invalid attempt history header")
        for line_number, line in enumerate(source, 3):
            columns = line.rstrip("\n").split("\t")
            if len(columns) != 3:
                raise ValueError(
                    f"{path}:{line_number}: expected attempt, status, "
                    "and failed gates"
                )
            attempt, status, gates_text = columns
            if attempt != str(len(failures) + 1):
                raise ValueError(
                    f"{path}:{line_number}: attempts must be consecutive"
                )
            if status not in ("pass", "regression"):
                raise ValueError(
                    f"{path}:{line_number}: invalid status {status}"
                )
            try:
                gates_json = json.loads(gates_text)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"{path}:{line_number}: invalid failed-gates JSON"
                ) from exc
            if not isinstance(gates_json, list):
                raise ValueError(
                    f"{path}:{line_number}: failed gates must be a list"
                )
            validated_gates = [
                validate_gate_id(path, line_number, suite, value)
                for value in gates_json
            ]
            gates = frozenset(validated_gates)
            if len(gates) != len(validated_gates):
                raise ValueError(
                    f"{path}:{line_number}: duplicate failed gate"
                )
            expected_status = "regression" if gates else "pass"
            if status != expected_status:
                raise ValueError(
                    f"{path}:{line_number}: status does not match failed gates"
                )
            failures.append(gates)
    if not failures:
        raise ValueError(f"{path}: no benchmark attempts")
    if len(failures) > 3:
        raise ValueError(f"{path}: at most three attempts are allowed")

    candidates = failures[0]
    for attempt, gates in enumerate(failures[1:], 2):
        if not candidates:
            raise ValueError(
                f"{path}: attempt {attempt} follows a resolved history"
            )
        candidates = candidates.intersection(gates)
    if candidates and len(failures) < 3:
        raise ValueError(
            f"{path}: possible regression requires three attempts"
        )
    return suite, AttemptHistory(suite, tuple(failures))


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


def failure_ids(analysis, include_overall=True):
    failures = {
        ("individual", suite, section, test)
        for suite, section, test in analysis["individual_failures"]
    }
    failures.update(
        ("section", suite, section)
        for suite, section in analysis["section_failures"]
    )
    failures.update(
        ("suite", suite) for suite in analysis["suite_failures"]
    )
    if include_overall and analysis["overall_failed"]:
        failures.add(("overall",))
    return failures


def apply_retry_confirmation(results, analysis, attempt_histories):
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
    for suite, history in attempt_histories.items():
        if history.suite != suite:
            raise ValueError(
                f"history suite {history.suite} does not match "
                f"requested suite {suite}"
            )

    confirmed = set()
    for history in attempt_histories.values():
        confirmed.update(history.confirmed_failures)

    observed = failure_ids(analysis, include_overall=False)
    inconsistent = sorted(confirmed - observed)
    if inconsistent:
        raise ValueError(
            "confirmed gates do not match final benchmark results: "
            + ", ".join(format_gate_id(gate) for gate in inconsistent)
        )

    individual_failures = {
        key
        for key in analysis["individual_failures"]
        if ("individual", *key) in confirmed
    }
    section_failures = {
        key
        for key in analysis["section_failures"]
        if ("section", *key) in confirmed
    }
    suite_failures = {
        key
        for key in analysis["suite_failures"]
        if ("suite", key) in confirmed
    }

    confirmed_analysis = dict(analysis)
    confirmed_analysis.update(
        {
            "failed": bool(
                individual_failures
                or section_failures
                or suite_failures
            ),
            "individual_failures": individual_failures,
            "section_failures": section_failures,
            "suite_failures": suite_failures,
            # Overall fan-in is measured once. In practice its 15% gate is
            # dominated by the per-suite aggregate gates, which are retried.
            "overall_failed": False,
            "observed_individual_failures": analysis[
                "individual_failures"
            ],
            "observed_section_failures": analysis["section_failures"],
            "observed_suite_failures": analysis["suite_failures"],
            "observed_overall_failed": analysis["overall_failed"],
            "confirmed_failure_ids": frozenset(confirmed),
        }
    )
    return confirmed_analysis


def format_gate_id(gate):
    kind = gate[0]
    if kind == "individual":
        return f"individual `{gate[1]} / {gate[2]} / {gate[3]}`"
    if kind == "section":
        return f"section `{gate[1]} / {gate[2]}`"
    if kind == "suite":
        return f"suite `{gate[1]}`"
    if kind == "overall":
        return "overall aggregate"
    raise ValueError(f"unknown gate kind: {kind}")


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

    gate_status = "PASS" if not analysis["failed"] else "FAIL"
    failed_gates = analysis.get(
        "confirmed_failure_ids",
        frozenset(failure_ids(analysis)),
    )
    lines = [
        "<!-- benchmark:relative -->",
        "## DoltLite performance vs PR base",
        "",
        f"- **Baseline:** `{baseline_ref}`",
        f"- **Candidate:** `{candidate_ref}`",
        f"- **Overall ratio:** {format_ratio(analysis['overall_ratio'])}",
        f"- **Gate result:** **{gate_status}**",
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
    if failed_gates:
        lines.extend(["- **Confirmed failed gates:**"])
        lines.extend(
            f"    - {format_gate_id(gate)}"
            for gate in sorted(failed_gates)
        )
    else:
        lines.append("- **Confirmed failed gates:** none")

    retried = []
    for suite, history in sorted((attempt_histories or {}).items()):
        attempt_count = len(history.failures)
        if attempt_count == 1:
            continue
        confirmed_count = len(history.confirmed_failures)
        if confirmed_count:
            retried.append(
                f"{suite} confirmed {confirmed_count} exact "
                f"{'gate' if confirmed_count == 1 else 'gates'} "
                f"in {attempt_count} attempts"
            )
        else:
            retried.append(
                f"{suite} cleared after {attempt_count} attempts; "
                "no gate failed every time"
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
        if suite in analysis["suite_failures"]:
            suite_status = "FAIL"
        elif suite in analysis.get(
            "observed_suite_failures",
            analysis["suite_failures"],
        ):
            suite_status = "TRANSIENT"
        else:
            suite_status = "PASS"
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
            if key in analysis["individual_failures"]:
                row_status = "FAIL"
            elif key in analysis.get(
                "observed_individual_failures",
                analysis["individual_failures"],
            ):
                row_status = "TRANSIENT"
            else:
                row_status = "PASS"
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
                "**FAILED:** Candidate performance exceeded the confirmed "
                "regression gates listed above.",
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
            analysis = apply_retry_confirmation(
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
