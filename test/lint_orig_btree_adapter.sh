#!/usr/bin/env bash
# Orig-adapter cursor ops must forward to stock B-tree, not stub or reimplement.
# Dummy Offset/CountRange returns were silent SQLite-file corruption.

set -euo pipefail

SRCDIR="${1:-src}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
cd "$REPO"

python3 - "$SRCDIR" <<'PY'
import re
import sys
from pathlib import Path

src = Path(sys.argv[1])
orig_c = (src / "prolly_btree_orig.c").read_text()
btree_c = (src / "prolly_btree.c").read_text()
api_c = (src / "btree_orig_api.c").read_text()

allow = {
    "origCursorRowCountEstVt": "unknown estimate is a valid stock answer",
    "origCursorCursorHintFlagsVt": "SEEK_EQ not forwarded; slower, not wrong",
    "origCursorCursorHasHintVt": "SEEK_EQ not forwarded; slower, not wrong",
    "origCursorCursorIsValidVt": "debug-only",
}

m = re.search(
    r"const struct BtCursorOps origCursorVtOps = \{([^}]+)\}",
    btree_c,
)
if not m:
    print("lint_orig_btree_adapter: origCursorVtOps not found", file=sys.stderr)
    sys.exit(1)
vtable = re.findall(r"\borigCursor\w+Vt\b", m.group(1))
if not vtable:
    print("lint_orig_btree_adapter: origCursorVtOps has no members", file=sys.stderr)
    sys.exit(1)


def func_body(text, name):
    pat = re.compile(
        r"(?:^|\n)(?:[\w\s\*]+?)" + re.escape(name) + r"\s*\((?:[^;]*?)\)\s*\{",
        re.S,
    )
    match = pat.search(text)
    if not match:
        return None
    start = match.end() - 1
    depth = 0
    for i, ch in enumerate(text[start:], start):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
    return None


fails = []
for name in vtable:
    if name in allow:
        continue
    body = func_body(orig_c, name)
    if body is None:
        fails.append("%s: missing definition in prolly_btree_orig.c" % name)
        continue
    expected = "origBtree" + name[len("origCursor") : -2]
    if expected not in body:
        fails.append(
            "%s: must call %s (do not stub or reimplement stock btree)"
            % (name, expected)
        )

for name, _reason in allow.items():
    if name in vtable and func_body(orig_c, name) is None:
        fails.append("%s: allowlisted but missing from prolly_btree_orig.c" % name)

if "origBtreeOffset" not in api_c or "orig_sqlite3BtreeOffset" not in api_c:
    fails.append("origBtreeOffset must forward orig_sqlite3BtreeOffset")
if "origBtreeCountRange" not in api_c or "orig_sqlite3BtreeCountRange" not in api_c:
    fails.append("origBtreeCountRange must forward orig_sqlite3BtreeCountRange")

if fails:
    print("lint_orig_btree_adapter: %d violation(s)" % len(fails), file=sys.stderr)
    for f in fails:
        print("  %s" % f, file=sys.stderr)
    sys.exit(1)
print("lint_orig_btree_adapter: all checks passed")
PY
