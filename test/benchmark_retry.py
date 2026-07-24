#!/usr/bin/env python3

import argparse
import os
import pathlib
import shutil
import subprocess
import sys

import benchmark_compare


def write_history(path, statuses):
    lines = ["attempt\tstatus"]
    lines.extend(
        f"{attempt}\t{status}"
        for attempt, status in enumerate(statuses, 1)
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def promote_attempt(attempt_dir, results_dir, suite):
    for suffix in (".tsv", "-samples.tsv", ".md"):
        source = attempt_dir / f"{suite}{suffix}"
        shutil.copy2(source, results_dir / source.name)


def run_with_retries(
    suite,
    results_dir,
    command,
    max_attempts,
    individual_ratio,
    aggregate_ratio,
    min_delta_us,
    individual_min_delta_us,
    command_runner=subprocess.run,
):
    results_dir = pathlib.Path(results_dir)
    attempts_dir = results_dir / "attempts"
    history_path = results_dir / f"{suite}-attempts.tsv"
    results_dir.mkdir(parents=True, exist_ok=True)
    statuses = []

    for attempt in range(1, max_attempts + 1):
        attempt_dir = attempts_dir / f"attempt-{attempt}"
        attempt_dir.mkdir(parents=True, exist_ok=True)
        result_path = attempt_dir / f"{suite}.tsv"
        samples_path = attempt_dir / f"{suite}-samples.tsv"
        markdown_path = attempt_dir / f"{suite}.md"
        gate_path = attempt_dir / f"{suite}-gate.md"

        environment = os.environ.copy()
        environment.update(
            {
                "BENCH_RESULTS_OUTPUT": str(result_path),
                "BENCH_SAMPLES_OUTPUT": str(samples_path),
                "VC_PERF_RESULTS_OUTPUT": str(result_path),
                "VC_PERF_SAMPLES_OUTPUT": str(samples_path),
            }
        )
        with markdown_path.open("w", encoding="utf-8") as output:
            completed = command_runner(
                command,
                env=environment,
                stdout=output,
                check=False,
            )

        if completed.returncode:
            statuses.append("command_error")
            write_history(history_path, statuses)
            print(
                f"{suite} attempt {attempt}/{max_attempts}: "
                f"command failed with exit code {completed.returncode}",
                file=sys.stderr,
            )
            return completed.returncode

        required = (result_path, samples_path, markdown_path)
        missing = [
            str(path)
            for path in required
            if not path.is_file() or path.stat().st_size == 0
        ]
        if missing:
            statuses.append("invalid_result")
            write_history(history_path, statuses)
            print(
                f"{suite} attempt {attempt}/{max_attempts}: "
                f"missing result files: {', '.join(missing)}",
                file=sys.stderr,
            )
            return 2

        try:
            results = benchmark_compare.parse_input(
                f"{suite}={result_path}"
            )
        except (OSError, ValueError) as exc:
            statuses.append("invalid_result")
            write_history(history_path, statuses)
            print(
                f"{suite} attempt {attempt}/{max_attempts}: {exc}",
                file=sys.stderr,
            )
            return 2

        analysis = benchmark_compare.analyze(
            results,
            individual_ratio,
            aggregate_ratio,
            min_delta_us,
            {suite: (individual_ratio, individual_min_delta_us)},
        )
        report = benchmark_compare.render(
            results,
            analysis,
            "PR base",
            "PR candidate",
            individual_ratio,
            aggregate_ratio,
            min_delta_us,
            {suite: (individual_ratio, individual_min_delta_us)},
        )
        gate_path.write_text(report, encoding="utf-8")

        status = "regression" if analysis["failed"] else "pass"
        statuses.append(status)
        write_history(history_path, statuses)
        if status == "pass":
            promote_attempt(attempt_dir, results_dir, suite)
            print(
                f"{suite} attempt {attempt}/{max_attempts}: passed"
            )
            return 0

        if attempt < max_attempts:
            print(
                f"{suite} attempt {attempt}/{max_attempts}: "
                "performance gate exceeded; retrying"
            )
            continue

        # A valid measurement is always published. The aggregate report job
        # enforces the gate after this third consecutive regression.
        promote_attempt(attempt_dir, results_dir, suite)
        print(
            f"{suite} attempt {attempt}/{max_attempts}: "
            "performance gate exceeded in all attempts"
        )
        return 0

    raise AssertionError("retry loop did not return")


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=(
            "Retry a paired benchmark only when its performance gate fails"
        )
    )
    parser.add_argument("--suite", required=True)
    parser.add_argument("--results-dir", required=True)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--individual-ratio", type=float, default=1.25)
    parser.add_argument("--aggregate-ratio", type=float, default=1.15)
    parser.add_argument("--min-delta-us", type=int, default=5000)
    parser.add_argument("--individual-min-delta-us", type=int, default=5000)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)

    command = args.command
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        parser.error("a benchmark command is required after --")
    if args.max_attempts < 1:
        parser.error("--max-attempts must be at least 1")

    return run_with_retries(
        args.suite,
        args.results_dir,
        command,
        args.max_attempts,
        args.individual_ratio,
        args.aggregate_ratio,
        args.min_delta_us,
        args.individual_min_delta_us,
    )


if __name__ == "__main__":
    sys.exit(main())
