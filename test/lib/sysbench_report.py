import sys
from collections import defaultdict


def median(values):
    valid = sorted(int(value) for value in values if int(value) >= 0)
    return valid[len(valid) // 2] if valid else -1


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


def ceiling(path, section, tests, maximum):
    rows = {}
    with open(path) as stream:
        for line in stream:
            cols = line.rstrip("\n").split("\t")
            if len(cols) >= 4 and cols[0] == section:
                rows.setdefault(cols[1], cols[2:4])
    failed = 0
    for test in tests.split():
        try:
            baseline, candidate = map(int, rows.get(test, ("", "")))
        except ValueError:
            baseline = candidate = -1
        if baseline <= 0 or candidate < 0:
            print(f"FAIL: {section}/{test} did not produce valid timings", file=sys.stderr)
            failed = 1
        elif candidate / baseline > float(maximum):
            print(f"FAIL: {section}/{test} = {candidate / baseline:.2f}x (ceiling: {maximum}x)", file=sys.stderr)
            failed = 1
    return failed


def average_ceiling(path, section, tests, maximum):
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
    ratio = f"{sum(ratios) / len(ratios):.2f}"
    if float(ratio) > float(maximum):
        print(f"FAIL: {section} average = {ratio}x (ceiling: {maximum}x)", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    commands = {"section": section_report, "ceiling": ceiling, "average": average_ceiling}
    sys.exit(commands[sys.argv[1]](*sys.argv[2:]))
