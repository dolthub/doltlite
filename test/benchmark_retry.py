#!/usr/bin/env python3

import argparse
import json
import os
import pathlib
import shutil
import subprocess
import sys

import benchmark_compare


def write_history(path, suite, records):
    lines = [f"suite\t{suite}", "attempt\tstatus\tfailed_gates"]
    for attempt, (status, failures) in enumerate(records, 1):
        encoded = json.dumps(
            [list(gate) for gate in sorted(failures)],
            separators=(",", ":"),
        )
        lines.append(f"{attempt}\t{status}\t{encoded}")
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
    records = []
    candidates = None

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
            records.append(("command_error", frozenset()))
            write_history(history_path, suite, records)
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
            records.append(("invalid_result", frozenset()))
            write_history(history_path, suite, records)
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
            records.append(("invalid_result", frozenset()))
            write_history(history_path, suite, records)
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

        failures = frozenset(
            benchmark_compare.failure_ids(
                analysis,
                include_overall=False,
            )
        )
        status = "regression" if failures else "pass"
        records.append((status, failures))
        write_history(history_path, suite, records)

        if candidates is None:
            candidates = failures
        else:
            candidates = candidates.intersection(failures)

        if not candidates:
            promote_attempt(attempt_dir, results_dir, suite)
            if attempt == 1 and not failures:
                print(f"{suite} attempt {attempt}/{max_attempts}: passed")
            else:
                print(
                    f"{suite} attempt {attempt}/{max_attempts}: "
                    "no exact gate remains unbroken"
                )
            return 0

        if attempt < max_attempts:
            print(
                f"{suite} attempt {attempt}/{max_attempts}: "
                f"{len(candidates)} exact performance "
                f"{'gate remains' if len(candidates) == 1 else 'gates remain'}; "
                "retrying"
            )
            continue

        # A valid measurement is always published. The aggregate report job
        # enforces the gate after this third consecutive regression.
        promote_attempt(attempt_dir, results_dir, suite)
        print(
            f"{suite} attempt {attempt}/{max_attempts}: "
            f"{len(candidates)} exact performance "
            f"{'gate exceeded' if len(candidates) == 1 else 'gates exceeded'} "
            "in all attempts"
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
