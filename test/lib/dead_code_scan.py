#!/usr/bin/env python3
"""Parts C/D of test/dead_code_check.sh.

C: static inline in a header whose identifier never appears elsewhere.
D: non-static function defined in DoltLite-owned .c with no identifier
   occurrence outside that file (should be static, or it is dead).
"""
from __future__ import annotations

import argparse
import glob
import os
import re
import sys
from collections import Counter

IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
INLINE = re.compile(
    r"static\s+(?:SQLITE_INLINE|inline)\s+[^{;]*?\b([A-Za-z_][A-Za-z0-9_]*)\s*\(",
    re.S,
)
DEF = re.compile(
    r"^(?!\s)(?!static\b)(?!typedef\b)(?!#)"
    r"(?:SQLITE_PRIVATE\s+|SQLITE_API\s+)?"
    r".+?\b([A-Za-z_][A-Za-z0-9_]*)\s*\(",
    re.M,
)
SKIP_DEF = {
    "if",
    "for",
    "while",
    "switch",
    "return",
    "sizeof",
    "case",
    "else",
    "do",
    "main",
}
SRC_GLOBS = (
    "doltlite.c",
    "doltlite_*.c",
    "chunk_*.c",
    "prolly_*.c",
    "remotesrv_main.c",
    "pager_shim.c",
    "sortkey.c",
)


def expand(root: str, patterns: list[str]) -> list[str]:
    out: set[str] = set()
    for pat in patterns:
        out.update(glob.glob(os.path.join(root, pat)))
    return sorted(out)


def strip_comments(text: str) -> str:
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    return re.sub(r"//.*", "", text)


def load_tokens(paths: list[str]) -> tuple[dict[str, str], dict[str, set[str]], dict[str, Counter]]:
    texts: dict[str, str] = {}
    idents: dict[str, set[str]] = {}
    counts: dict[str, Counter] = {}
    for path in paths:
        try:
            raw = open(path, errors="replace").read()
        except OSError:
            continue
        texts[path] = raw
        toks = IDENT.findall(strip_comments(raw))
        idents[path] = set(toks)
        counts[path] = Counter(toks)
    return texts, idents, counts


def scan_inlines(hdrs: list[str], corpus: list[str], texts: dict[str, str],
                 idents: dict[str, set[str]], counts: dict[str, Counter]) -> list[str]:
    dead: list[str] = []
    for path in hdrs:
        raw = texts.get(path)
        if raw is None:
            continue
        for match in INLINE.finditer(strip_comments(raw)):
            name = match.group(1)
            others = [q for q in corpus if q != path and name in idents.get(q, ())]
            if others:
                continue
            if counts.get(path, Counter())[name] <= 1:
                dead.append(f"  dead header inline: {name} ({path})")
    return dead


def looks_like_def(line: str, name: str) -> bool:
    before = line[: line.find(name)]
    if not (re.search(r"[A-Za-z_][A-Za-z0-9_]*\s+\**$", before) or "*" in before):
        return False
    # Declarations end with ';' and have no body. One-line definitions have '{'.
    if ";" in line and "{" not in line:
        return False
    return True


def scan_should_be_static(src_files: list[str], corpus: list[str], texts: dict[str, str],
                          idents: dict[str, set[str]]) -> list[str]:
    dead: list[str] = []
    for path in src_files:
        raw = texts.get(path)
        if raw is None:
            continue
        stripped = strip_comments(raw)
        for match in DEF.finditer(stripped):
            name = match.group(1)
            if name in SKIP_DEF:
                continue
            if name.startswith("sqlite3") or name.startswith("orig_sqlite3"):
                continue
            if not looks_like_def(match.group(0), name):
                continue
            others = [q for q in corpus if q != path and name in idents.get(q, ())]
            if others:
                continue
            line = stripped[: match.start()].count("\n") + 1
            dead.append(f"  should be static: {name} ({path}:{line})")
    return dead


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--src-root", default="")
    args = ap.parse_args()
    root = os.path.abspath(args.root)
    src_root = os.path.abspath(args.src_root or os.path.join(root, "src"))

    src_files = expand(src_root, list(SRC_GLOBS))
    hdrs = expand(src_root, ["*.h"])
    # Owned files are scanned for definitions; the whole src tree is searched
    # for uses (sqliteInt.h / main.c / vdbe.c hold Register and btree entry
    # points).
    corpus = expand(src_root, ["*.c", "*.h"]) + expand(root, [
        "test/*.c",
        "test/c/*.c",
        "ext/*/*.c",
        "ext/*/*.h",
    ])
    corpus = sorted(set(corpus))
    texts, idents, counts = load_tokens(corpus)

    dead = scan_inlines(hdrs, corpus, texts, idents, counts)
    dead += scan_should_be_static(src_files, corpus, texts, idents)
    for line in dead:
        print(line)
    return 1 if dead else 0


if __name__ == "__main__":
    sys.exit(main())
