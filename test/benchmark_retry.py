#!/usr/bin/env python3

import argparse
import json
import os
import pathlib
import shutil
import subprocess
import sys
import time
import uuid

import benchmark_compare


def write_history(path, suite, producer_id, records):
    lines = [
        f"suite\t{suite}",
        f"producer_id\t{producer_id}",
        "attempt\tstatus\tfailed_gates",
    ]
    for attempt, (status, failures) in enumerate(records, 1):
        encoded = json.dumps(
            [list(gate) for gate in sorted(failures)],
            separators=(",", ":"),
        )
        lines.append(f"{attempt}\t{status}\t{encoded}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def promote_attempt(
    attempt_dir,
    results_dir,
    suite,
    producer_id,
    copy_function=shutil.copy2,
    replace_function=os.replace,
):
    sources = [
        attempt_dir / f"{suite}{suffix}"
        for suffix in (".tsv", "-samples.tsv", ".md")
    ]
    missing = [
        str(path)
        for path in sources
        if not path.is_file() or path.stat().st_size == 0
    ]
    if missing:
        raise FileNotFoundError(
            "cannot promote incomplete benchmark attempt: "
            + ", ".join(missing)
        )

    staged = []
    backups = []
    promoted = []
    try:
        for source in sources:
            destination = results_dir / source.name
            temporary = results_dir / (
                f".{source.name}.{producer_id}.staged"
            )
            if source.name == f"{suite}.tsv":
                with temporary.open("w", encoding="utf-8") as output:
                    output.write(f"# suite\t{suite}\n")
                    output.write(f"# producer_id\t{producer_id}\n")
                    with source.open(encoding="utf-8") as input_file:
                        shutil.copyfileobj(input_file, output)
            else:
                copy_function(source, temporary)
            staged.append((temporary, destination))

        for _temporary, destination in staged:
            if destination.exists():
                backup = results_dir / (
                    f".{destination.name}.{producer_id}.backup"
                )
                replace_function(destination, backup)
                backups.append((backup, destination))

        for temporary, destination in staged:
            replace_function(temporary, destination)
            promoted.append(destination)
    except Exception as promotion_error:
        rollback_errors = []
        for destination in reversed(promoted):
            try:
                destination.unlink(missing_ok=True)
            except OSError as exc:
                rollback_errors.append(exc)
        for backup, destination in reversed(backups):
            try:
                replace_function(backup, destination)
            except OSError as exc:
                rollback_errors.append(exc)
        for temporary, _destination in staged:
            try:
                temporary.unlink(missing_ok=True)
            except OSError as exc:
                rollback_errors.append(exc)
        if rollback_errors:
            raise OSError(
                "benchmark artifact promotion and rollback failed: "
                + "; ".join(str(exc) for exc in rollback_errors)
            ) from promotion_error
        raise
    else:
        for backup, _destination in backups:
            backup.unlink(missing_ok=True)


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
    producer_id=None,
):
    results_dir = pathlib.Path(results_dir)
    attempts_dir = results_dir / "attempts"
    history_path = results_dir / f"{suite}-attempts.tsv"
    results_dir.mkdir(parents=True, exist_ok=True)
    producer_id = producer_id or uuid.uuid4().hex
    records = []
    candidates = None
    suite_started = time.monotonic()

    for attempt in range(1, max_attempts + 1):
        attempt_started = time.monotonic()
        print(
            f"{suite}: starting attempt {attempt}/{max_attempts}",
            flush=True,
        )
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
            write_history(history_path, suite, producer_id, records)
            print(
                f"{suite} attempt {attempt}/{max_attempts}: "
                f"command failed with exit code {completed.returncode} "
                f"after {time.monotonic() - attempt_started:.1f}s",
                file=sys.stderr,
                flush=True,
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
            write_history(history_path, suite, producer_id, records)
            print(
                f"{suite} attempt {attempt}/{max_attempts}: "
                f"missing result files: {', '.join(missing)}",
                file=sys.stderr,
                flush=True,
            )
            return 2

        try:
            results = benchmark_compare.parse_input(
                f"{suite}={result_path}"
            )
        except (OSError, ValueError) as exc:
            records.append(("invalid_result", frozenset()))
            write_history(history_path, suite, producer_id, records)
            print(
                f"{suite} attempt {attempt}/{max_attempts}: {exc}",
                file=sys.stderr,
                flush=True,
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
        write_history(history_path, suite, producer_id, records)

        attempt_elapsed = time.monotonic() - attempt_started
        cumulative_elapsed = time.monotonic() - suite_started
        failure_summary = (
            "; ".join(
                benchmark_compare.format_gate_id(gate)
                for gate in sorted(failures)
            )
            if failures
            else "none"
        )
        print(
            f"{suite}: completed attempt {attempt}/{max_attempts} "
            f"in {attempt_elapsed:.1f}s "
            f"({cumulative_elapsed:.1f}s cumulative); "
            f"failed gates: {failure_summary}",
            flush=True,
        )

        if candidates is None:
            candidates = failures
        else:
            candidates = candidates.intersection(failures)

        if not candidates:
            try:
                promote_attempt(
                    attempt_dir,
                    results_dir,
                    suite,
                    producer_id,
                )
            except OSError as exc:
                print(
                    f"{suite}: canonical artifact promotion failed: {exc}",
                    file=sys.stderr,
                    flush=True,
                )
                return 2
            if attempt == 1 and not failures:
                print(
                    f"{suite}: passed on attempt {attempt}/{max_attempts}",
                    flush=True,
                )
            else:
                print(
                    f"{suite}: stopping after attempt "
                    f"{attempt}/{max_attempts}; no exact gate "
                    "failed on every attempt",
                    flush=True,
                )
            return 0

        if attempt < max_attempts:
            print(
                f"{suite}: retrying after attempt {attempt}/{max_attempts}; "
                "gates still eligible for confirmation: "
                + "; ".join(
                    benchmark_compare.format_gate_id(gate)
                    for gate in sorted(candidates)
                ),
                flush=True,
            )
            continue

        # A valid measurement is always published. The aggregate report job
        # enforces the gate after this third consecutive regression.
        try:
            promote_attempt(
                attempt_dir,
                results_dir,
                suite,
                producer_id,
            )
        except OSError as exc:
            print(
                f"{suite}: canonical artifact promotion failed: {exc}",
                file=sys.stderr,
                flush=True,
            )
            return 2
        print(
            f"{suite}: confirmed after {attempt}/{max_attempts} attempts: "
            + "; ".join(
                benchmark_compare.format_gate_id(gate)
                for gate in sorted(candidates)
            ),
            flush=True,
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
