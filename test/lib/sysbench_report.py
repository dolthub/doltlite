import sys
from collections import defaultdict


def median(values):
    valid = sorted(int(value) for value in values if int(value) >= 0)
    return valid[len(valid) // 2] if valid else -1


# Baseline and candidate medians are drawn independently, so runner I/O noise
# moves them apart; the ratio of the paired samples is what survives it.
def paired_ratio_median(pairs):
    ratios = sorted(
        candidate / baseline
        for baseline, candidate in pairs
        if baseline > 0 and candidate >= 0
    )
    return ratios[len(ratios) // 2] if ratios else None


def paired_ratio_medians(path, section):
    pairs = defaultdict(list)
    with open(path) as stream:
        for line in stream:
            cols = line.rstrip("\n").split("\t")
            if len(cols) != 5 or cols[0] != section:
                continue
            try:
                baseline, candidate = int(cols[3]), int(cols[4])
            except ValueError:
                continue
            pairs[cols[1]].append((baseline, candidate))
    return {
        test: paired_ratio_median(values) for test, values in pairs.items()
    }


def section_report(path, section, tests, results):
    samples = defaultdict(lambda: ([], []))
    with open(path) as stream:
        for line in stream:
            _, test, _, baseline, candidate = line.rstrip("\n").split("\t")
            samples[test][0].append(baseline)
            samples[test][1].append(candidate)
    ratio_sum = 0
    ratio_count = 0
    with open(results, "a") as stream:
        for test in tests.split():
            baseline, candidate = (median(values) for values in samples[test])
            baseline_display = f"{baseline:,}" if baseline >= 0 else "crash"
            candidate_display = f"{candidate:,}" if candidate >= 0 else "crash"
            ratio = "--"
            if baseline > 0 and candidate >= 0:
                ratio = f"{candidate / baseline:.2f}"
                ratio_sum += candidate / baseline
                ratio_count += 1
            stream.write(f"{section}\t{test}\t{baseline}\t{candidate}\n")
            print(f"| {test} | {baseline_display} | {candidate_display} | {ratio} |")
    average = f"{ratio_sum / ratio_count:.2f}" if ratio_count else "--"
    print(f"| Average |  |  | {average} |")
    return 0


def ceiling(path, samples, section, tests, maximum):
    rows = {}
    with open(path) as stream:
        for line in stream:
            cols = line.rstrip("\n").split("\t")
            if len(cols) >= 4 and cols[0] == section:
                rows.setdefault(cols[1], cols[2:4])
    limit = float(maximum)
    paired = paired_ratio_medians(samples, section)
    failed = 0
    for test in tests.split():
        try:
            baseline, candidate = map(int, rows.get(test, ("", "")))
        except ValueError:
            baseline = candidate = -1
        if baseline <= 0 or candidate < 0:
            print(f"FAIL: {section}/{test} did not produce valid timings", file=sys.stderr)
            failed = 1
            continue
        ratio = candidate / baseline
        if ratio <= limit:
            continue
        confirmation = paired.get(test)
        if confirmation is None or confirmation > limit:
            confirmation_display = (
                f"{confirmation:.2f}x" if confirmation is not None else "unavailable"
            )
            print(
                f"FAIL: {section}/{test} = {ratio:.2f}x, paired "
                f"{confirmation_display} (ceiling: {maximum}x)",
                file=sys.stderr,
            )
            failed = 1
        else:
            print(
                f"NOISE: {section}/{test} = {ratio:.2f}x but paired "
                f"{confirmation:.2f}x (ceiling: {maximum}x)",
                file=sys.stderr,
            )
    return failed


def average_ceiling(path, samples, section, tests, maximum):
    wanted = set(tests.split())
    ratios = []
    with open(path) as stream:
        for line in stream:
            cols = line.rstrip("\n").split("\t")
            if len(cols) < 4 or cols[0] != section or cols[1] not in wanted:
                continue
            baseline, candidate = int(cols[2]), int(cols[3])
            if baseline > 0 and candidate >= 0:
                ratios.append(candidate / baseline)
    if len(ratios) != len(wanted):
        print(f"FAIL: {section} average is missing valid timings", file=sys.stderr)
        return 1
    limit = float(maximum)
    ratio = f"{sum(ratios) / len(ratios):.2f}"
    if float(ratio) <= limit:
        return 0
    paired = paired_ratio_medians(samples, section)
    confirmations = [paired.get(test) for test in sorted(wanted)]
    if any(value is None for value in confirmations):
        print(
            f"FAIL: {section} average = {ratio}x, paired unavailable "
            f"(ceiling: {maximum}x)",
            file=sys.stderr,
        )
        return 1
    confirmation = f"{sum(confirmations) / len(confirmations):.2f}"
    if float(confirmation) > limit:
        print(
            f"FAIL: {section} average = {ratio}x, paired {confirmation}x "
            f"(ceiling: {maximum}x)",
            file=sys.stderr,
        )
        return 1
    print(
        f"NOISE: {section} average = {ratio}x but paired {confirmation}x "
        f"(ceiling: {maximum}x)",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    commands = {"section": section_report, "ceiling": ceiling, "average": average_ceiling}
    sys.exit(commands[sys.argv[1]](*sys.argv[2:]))
