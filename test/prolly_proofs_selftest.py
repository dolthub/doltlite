#!/usr/bin/env python3
import argparse
from pathlib import Path
import shutil
import subprocess
import tempfile


CASES = (
    (
        "lost overwrite",
        "Prolly/OrderedMap.lean",
        "else if k = e.1 then (k, v) :: es",
        "else if k = e.1 then e :: es",
        "error: Prolly/OrderedMap.lean:",
    ),
    (
        "missed deletion",
        "Prolly/OrderedMap.lean",
        "if k = e.1 then erase k es else e :: erase k es",
        "if k = e.1 then e :: erase k es else e :: erase k es",
        "error: Prolly/OrderedMap.lean:",
    ),
    (
        "exclusive fence routing",
        "Prolly/Tree.lean",
        "if k ≤ hi then treeLookup t k",
        "if k < hi then treeLookup t k",
        "error: Prolly/Tree.lean:",
    ),
    (
        "discarded completed chunk",
        "Prolly/Chunker.lean",
        "if cut pending then ⟨s.done ++ [pending], []⟩",
        "if cut pending then ⟨s.done, []⟩",
        "error: Prolly/Chunker.lean:",
    ),
    (
        "silently resolved conflict",
        "Prolly/DiffMerge.lean",
        "else none\n\ntheorem mergeValue_conflict_iff",
        "else some ours\n\ntheorem mergeValue_conflict_iff",
        "error: Prolly/DiffMerge.lean:",
    ),
    (
        "custom axiom",
        "Prolly/OrderedMap.lean",
        "end DoltLite.Prolly",
        "axiom injectedProof : False\n\nend DoltLite.Prolly",
        "forbidden axiom",
    ),
    (
        "unfinished proof",
        "Prolly/OrderedMap.lean",
        "end DoltLite.Prolly",
        "theorem unfinishedProof : False := by sorry\n\nend DoltLite.Prolly",
        "sorry",
    ),
)


def check(lake, directory):
    output = ""
    for args in (("--wfail", "build"), ("env", "lean", "Prolly/Audit.lean")):
        result = subprocess.run(
            [lake, *args], cwd=directory, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120,
        )
        output += result.stdout
        if result.returncode:
            return result.returncode, output
    return 0, output


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--lake", default="lake")
    args = parser.parse_args()
    source = Path(__file__).resolve().parents[1] / "formal" / "prolly"
    status, output = check(args.lake, source)
    if status or "Audited " not in output:
        raise SystemExit(f"Baseline proofs/audit failed:\n{output}")
    print("PASS: baseline proofs and trust audit", flush=True)
    for name, filename, old, new, expected in CASES:
        with tempfile.TemporaryDirectory(prefix="doltlite-prolly-proofs-") as tmp:
            shutil.copytree(source, tmp, dirs_exist_ok=True, ignore=shutil.ignore_patterns(".lake"))
            path = Path(tmp) / filename
            text = path.read_text()
            if text.count(old) != 1:
                raise SystemExit(f"{name}: mutation anchor must occur exactly once")
            path.write_text(text.replace(old, new, 1))
            status, output = check(args.lake, tmp)
            if not status or expected not in output:
                raise SystemExit(f"{name}: expected rejection was not observed:\n{output}")
            print(f"PASS: rejected {name}", flush=True)
    print(f"{len(CASES) + 1} verification checks passed")


if __name__ == "__main__":
    main()
