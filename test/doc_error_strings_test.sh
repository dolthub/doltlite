#!/usr/bin/env bash
# Every error message the docs quote must exist in the source. Messages are
# taken from the backticked first cell of tables whose header column is
# "Error" or "Message", and from any backticked cell in an "Error" column.
# Placeholders (<name>, N, ...) split a message into fragments; each fragment
# of 12+ characters must appear in src/*.c or src/shell.c.in, with adjacent C
# string literals joined and escapes decoded.
#
# Usage: doc_error_strings_test.sh

set -u
cd "$(dirname "${BASH_SOURCE[0]}")/.."

python3 - <<'EOF'
import glob, re, sys

def c_strings(path):
    s = open(path, encoding="utf-8", errors="replace").read()
    s = re.sub(r'"\s*\n\s*"', '', s)                  # join adjacent literals
    out = []
    for lit in re.findall(r'"((?:[^"\\]|\\.)*)"', s):
        lit = re.sub(r'\\x([0-9a-fA-F]{2})', lambda m: chr(int(m.group(1), 16)), lit)
        lit = lit.replace('\\"', '"').replace("\\n", "\n").replace("\\'", "'").replace("\\\\", "\\")
        out.append(lit)
    return out

source = "\n".join(l for p in glob.glob("src/*.c") + ["src/shell.c.in"] for l in c_strings(p))
source_bytes = source.encode("utf-8", "surrogatepass").decode("utf-8", "replace")

def quoted(cell):
    return re.findall(r"`([^`]+)`", cell)

messages = []   # (file, message)
for path in sorted(glob.glob("doc/doltlite/*.md")):
    lines = open(path, encoding="utf-8").read().split("\n")
    i = 0
    while i < len(lines):
        if lines[i].startswith("|") and i + 1 < len(lines) and re.match(r"^\|[-| ]+\|$", lines[i+1]):
            header = [h.strip() for h in lines[i].strip("|").split("|")]
            cols = [k for k, h in enumerate(header) if h in ("Error", "Message")]
            i += 2
            while i < len(lines) and lines[i].startswith("|"):
                cells = [c.strip() for c in lines[i].strip("|").split("|")]
                for k in cols:
                    if k < len(cells):
                        for q in quoted(cells[k]):
                            messages.append((path, q))
                i += 1
            continue
        i += 1

bad = 0
seen = 0
for path, msg in messages:
    if msg.startswith(("PRAGMA", "SELECT", "dolt_", "BEGIN", "--", "SQLITE_")) and " " not in msg.strip():
        continue                                       # a symbol, not a message
    if re.fullmatch(r"SQLITE_[A-Z_]+", msg):
        continue
    frags = [f.strip(" .:;,") for f in re.split(r"<[^>]+>|\.\.\.|\bN\b|`|%[a-z]", msg)]
    frags = [f for f in frags if len(f) >= 12]
    if not frags:
        continue
    seen += 1
    missing = [f for f in frags if f not in source_bytes]
    if missing:
        bad += 1
        print(f"FAIL: {path}: `{msg}`\n  not in source: {missing}")

print(f"\n{seen} quoted messages checked, {bad} missing")
sys.exit(1 if bad else 0)
EOF
