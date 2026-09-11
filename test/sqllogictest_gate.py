import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path


def run(runner, path, timeout):
    try:
        result = subprocess.run(["timeout", timeout, runner, "--verify", path],
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except OSError as error:
        return str(error).encode(), 127 if isinstance(error, FileNotFoundError) else 126
    rc = result.returncode
    return result.stdout, rc if rc >= 0 else 128 - rc


def tokens(output):
    return set(re.findall(rb"!DIVERGE ([0-9]+)", output))


def main():
    doltlite, stock, directory, manifest, timeout, *files = sys.argv[1:]
    expected = defaultdict(list)
    if Path(manifest).is_file():
        for line in Path(manifest).read_bytes().splitlines():
            cols = line.split(b"#", 1)[0].split()
            if cols:
                expected[cols[0].decode()].append(cols[1] if len(cols) > 1 else b"")
    n_div_files = total_div = 0
    unexpected_lines, fixed_lines, crash_lines = [], [], []
    total_fixed = 0
    seen = set()
    for path in files:
        rel = path.removeprefix(directory + "/")
        seen.add(rel)
        output, rc = run(doltlite, path, timeout)
        if rc == 124 or b"errors out of" not in output:
            print(f"CRASH/TIMEOUT: {rel} (doltlite rc={rc})")
            crash_lines.append(f"  {rel} (rc={rc})")
            continue
        stock_output, _ = run(stock, path, timeout)
        divergences = tokens(output) - tokens(stock_output)
        exp = set(expected[rel]) - {b""}
        unexpected, fixed = divergences - exp, exp - divergences
        total_div += len(divergences)
        n_div_files += bool(divergences)
        if not unexpected and not fixed:
            if divergences:
                print(f"OK: {rel} ({len(divergences)} known divergences)")
            continue
        if unexpected:
            print(f"FAIL: {rel} — unexpected divergences (not in list):")
            for line in sorted(unexpected):
                number = line.decode()
                print(f"    line {number}")
                unexpected_lines.append(f"  {rel} {number}")
        if fixed:
            print(f"FIXED: {rel} — listed entries that no longer diverge (remove from list):")
            for line in sorted(fixed):
                number = line.decode()
                print(f"    line {number}")
                fixed_lines.append(f"  {rel} {number}")
            total_fixed += len(fixed)
    for rel in sorted(expected.keys() - seen):
        print(f"STALE: {rel} is in the divergence list but not present in the corpus")
        count = sum(bool(line) for line in expected[rel])
        total_fixed += count
        fixed_lines.append(f"  {rel} (file missing, {count} entries)")
    print("\n============================================")
    print(f"  files:                  {len(files)}")
    print(f"  files with divergences: {n_div_files}")
    print(f"  known divergences:      {total_div}")
    if unexpected_lines or crash_lines or total_fixed:
        if unexpected_lines:
            print(f"  UNEXPECTED divergences:  {len(unexpected_lines)}\n" + "\n".join(unexpected_lines))
        if crash_lines:
            print(f"  crashes/timeouts:        {len(crash_lines)}\n" + "\n".join(crash_lines))
        if total_fixed:
            print(f"  list entries to remove:  {total_fixed}\n" + "\n".join(fixed_lines))
        print("============================================")
        print(f"::error::sqllogictest divergence gate failed (unexpected={len(unexpected_lines)}, crashes={len(crash_lines)}, to-remove={total_fixed})")
        return 1
    print("============================================")
    print("OK: all doltlite divergences are accounted for in the allow-list.")
    return 0


if __name__ == "__main__":
    sys.stdout.reconfigure(line_buffering=True)
    sys.exit(main())
