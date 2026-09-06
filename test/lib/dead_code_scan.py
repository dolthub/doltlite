#!/usr/bin/env python3
"""Dead/duplicate-code scan for DoltLite-owned sources.

B: extern function defined in owned .c, never referenced from another .c and
   never called in its defining file (header declaration does not count).
C: static inline in a header whose identifier never appears elsewhere.
D: non-static function with no other .c mention. A header prototype
   does not count as a caller (should be static so Part A can see it).
   Btree vtable methods (prollyBtree* / origBtree*) stay non-static:
   other TUs call them through the ops table, not by name.
E: non-static prototype in an owned header that never appears in any .c.
F: #define in an owned header whose identifier never appears elsewhere
   (include guards skipped).
G: two owned .c functions have identical normalized bodies (whitespace
   collapsed, length >= MIN_CLONE_BODY), across files or same-file aliases.
H: non-static .c function whose body is a single return otherFn(...) and
   whose only extra-file .c mentions are under test/. doltliteTest* and
   *ForTest names are the C-test surface (tests link production libdoltlite)
   and are skipped.
I: file-local extern of an owned function whose prototype already appears
   in a header this .c includes.
"""
from __future__ import annotations

import argparse
import glob
import hashlib
import os
import re
import sys
from collections import Counter, defaultdict

IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
INLINE = re.compile(
    r"static\s+(?:SQLITE_INLINE|inline)\s+[^{;]*?\b([A-Za-z_][A-Za-z0-9_]*)\s*\(",
    re.S,
)
DEF = re.compile(
    r"^(?!\s)(?!typedef\b)(?!#)"
    r"(?:static\s+)?(?:SQLITE_PRIVATE\s+|SQLITE_API\s+)?"
    r".+?\b([A-Za-z_][A-Za-z0-9_]*)\s*\(",
    re.M,
)
PROTO = re.compile(
    r"^(?!\s)(?!static\b)(?!typedef\b)(?!#)"
    r"(?:SQLITE_PRIVATE\s+|SQLITE_API\s+|SQLITE_EXTERN\s+)?"
    r".+?\b([A-Za-z_][A-Za-z0-9_]*)\s*\([^;]*\)\s*;",
    re.M,
)
MACRO = re.compile(r"^\s*#\s*define\s+([A-Za-z_][A-Za-z0-9_]*)", re.M)
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
ALLOW_EXTERN = {
    "doltliteServe",
    "doltliteServeAsync",
    "doltliteServerStop",
}
VTBL_PREFIXES = (
    "prollyBtree",
    "prollyBtCursor",
    "origBtree",
    "origCursor",
)
SEAM_HDRS = frozenset({"prolly_btree_int.h", "doltlite_internal.h"})
ONE_CALL = re.compile(
    r"^return\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(.*\)\s*;$"
)
INCLUDE = re.compile(r'^\s*#\s*include\s+"([^"]+)"', re.M)
LOCAL_EXTERN = re.compile(
    r"^\s*extern\s+.+?\b([A-Za-z_][A-Za-z0-9_]*)\s*\([^;]*\)\s*;",
    re.M | re.S,
)
SRC_GLOBS = (
    "doltlite.c",
    "doltlite_*.c",
    "chunk_*.c",
    "prolly_*.c",
    "remotesrv_main.c",
    "pager_shim.c",
    "sortkey.c",
)
OWNED_HDR_GLOBS = (
    "doltlite*.h",
    "chunk_*.h",
    "prolly_*.h",
    "pager_shim.h",
    "sortkey.h",
    "record_codec.h",
)
MIN_CLONE_BODY = 100


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


def looks_like_def(line: str, name: str) -> bool:
    before = line[: line.find(name)]
    if not (re.search(r"[A-Za-z_][A-Za-z0-9_]*\s+\**$", before) or "*" in before):
        return False
    if ";" in line and "{" not in line:
        return False
    return True


def is_static_def(line: str) -> bool:
    return bool(re.match(r"^(?:SQLITE_PRIVATE\s+|SQLITE_API\s+)?static\b", line))


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


def iter_defs(path: str, texts: dict[str, str]):
    raw = texts.get(path)
    if raw is None:
        return
    stripped = strip_comments(raw)
    for match in DEF.finditer(stripped):
        name = match.group(1)
        if name in SKIP_DEF:
            continue
        if name.startswith("sqlite3") or name.startswith("orig_sqlite3"):
            continue
        line = match.group(0)
        if not looks_like_def(line, name):
            continue
        lineno = stripped[: match.start()].count("\n") + 1
        yield name, lineno, is_static_def(line), stripped, match


def is_test_api_name(name: str) -> bool:
    return name.startswith("doltliteTest") or name.endswith("ForTest")


def is_test_path(path: str, root: str) -> bool:
    rel = os.path.relpath(path, root)
    return rel.startswith("test" + os.sep) or rel.startswith("test/")


def extract_body(stripped: str, match: re.Match[str]) -> str | None:
    rest = stripped[match.end():]
    depth = 1
    i = 0
    while i < len(rest) and depth:
        ch = rest[i]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                break
        elif ch in ";{":
            break
        i += 1
    if i >= len(rest) or rest[i] != ")":
        return None
    j = rest.find("{", i)
    if j < 0 or ";" in rest[i:j]:
        return None
    body_start = match.end() + j + 1
    depth = 1
    k = body_start
    while k < len(stripped) and depth:
        if stripped[k] == "{":
            depth += 1
        elif stripped[k] == "}":
            depth -= 1
        k += 1
    return stripped[body_start:k - 1]


def scan_test_only_wrappers(
    src_files: list[str],
    corpus: list[str],
    root: str,
    texts: dict[str, str],
    idents: dict[str, set[str]],
) -> list[str]:
    dead: list[str] = []
    for path in src_files:
        raw = texts.get(path)
        if raw is None:
            continue
        stripped = strip_comments(raw)
        for match in DEF.finditer(stripped):
            name = match.group(1)
            if name in SKIP_DEF or is_test_api_name(name):
                continue
            if name.startswith("sqlite3") or name.startswith("orig_sqlite3"):
                continue
            line = match.group(0)
            if line.lstrip().startswith("static") or not looks_like_def(line, name):
                continue
            body = extract_body(stripped, match)
            if body is None:
                continue
            norm = re.sub(r"\s+", " ", body).strip()
            hit = ONE_CALL.match(norm)
            if not hit or hit.group(1) == name:
                continue
            extra_c = [
                q for q in corpus
                if q.endswith(".c") and q != path and name in idents.get(q, ())
            ]
            prod = [q for q in extra_c if not is_test_path(q, root)]
            if prod:
                continue
            if Counter(IDENT.findall(stripped))[name] > 1:
                continue
            lineno = stripped[: match.start()].count("\n") + 1
            dead.append(
                f"  test-only wrapper: {name} ({path}:{lineno}) -> {hit.group(1)}"
            )
    return dead


def is_vtbl_method(name: str) -> bool:
    return name.startswith(VTBL_PREFIXES)


def header_uses_beyond_proto(hpath: str, name: str, texts: dict[str, str]) -> bool:
    raw = texts.get(hpath)
    if raw is None:
        return False
    proto = re.compile(
        r"^(?!\s)(?:SQLITE_PRIVATE\s+|SQLITE_API\s+|SQLITE_EXTERN\s+)?"
        r".+?\b" + re.escape(name) + r"\s*\([^;]*\)\s*;",
        re.M,
    )
    leftover = proto.sub("", strip_comments(raw))
    return name in IDENT.findall(leftover)


def scan_should_be_static(src_files: list[str], corpus: list[str], texts: dict[str, str],
                          idents: dict[str, set[str]]) -> list[str]:
    dead: list[str] = []
    for path in src_files:
        for name, line, is_static, stripped, match in iter_defs(path, texts):
            if is_static or name in ALLOW_EXTERN or is_vtbl_method(name):
                continue
            others_c = [
                q for q in corpus
                if q.endswith(".c") and q != path and name in idents.get(q, ())
            ]
            if others_c:
                continue
            hdrs = [
                q for q in corpus
                if q.endswith(".h") and name in idents.get(q, ())
            ]
            if any(header_uses_beyond_proto(h, name, texts) for h in hdrs):
                continue
            dead.append(f"  should be static: {name} ({path}:{line})")
    return dead


def scan_duplicate_prototypes(
    owned_hdrs: list[str], texts: dict[str, str]
) -> list[str]:
    byname: dict[str, list[str]] = defaultdict(list)
    for path in owned_hdrs:
        raw = texts.get(path)
        if raw is None:
            continue
        for match in PROTO.finditer(strip_comments(raw)):
            name = match.group(1)
            if name in SKIP_DEF or name in ALLOW_EXTERN:
                continue
            if name.startswith("sqlite3") or name.startswith("orig_sqlite3"):
                continue
            byname[name].append(path)
    dead: list[str] = []
    for name, paths in sorted(byname.items()):
        uniq = sorted(set(paths))
        if len(uniq) < 2:
            continue
        bases = {os.path.basename(p) for p in uniq}
        if bases == SEAM_HDRS:
            continue
        shown = ", ".join(uniq)
        dead.append(f"  duplicate header prototype: {name} ({shown})")
    return dead


def scan_unused_externs(src_files: list[str], corpus: list[str], texts: dict[str, str],
                        idents: dict[str, set[str]], counts: dict[str, Counter]) -> list[str]:
    """Definition in one .c, no other .c mentions it, defining file count==1."""
    dead: list[str] = []
    seen: set[str] = set()
    for path in src_files:
        for name, line, is_static, stripped, match in iter_defs(path, texts):
            if is_static or name in ALLOW_EXTERN or name in seen:
                continue
            c_files = [q for q in corpus if q.endswith(".c") and name in idents.get(q, ())]
            if len(c_files) != 1 or c_files[0] != path:
                continue
            if counts.get(path, Counter())[name] > 1:
                continue
            hdrs_with = [q for q in corpus if q.endswith(".h") and name in idents.get(q, ())]
            proto = re.compile(
                r"^(?!\s)(?:SQLITE_PRIVATE\s+|SQLITE_API\s+|SQLITE_EXTERN\s+)?"
                r".+?\b" + re.escape(name) + r"\s*\([^;]*\)\s*;",
                re.M,
            )
            used_from_header = False
            for h in hdrs_with:
                leftover = proto.sub("", strip_comments(texts[h]))
                if name in IDENT.findall(leftover):
                    used_from_header = True
                    break
            if used_from_header:
                continue
            seen.add(name)
            dead.append(f"  dead (no caller): {name} ({path}:{line})")
    return dead


def scan_unused_prototypes(hdrs: list[str], corpus: list[str], texts: dict[str, str],
                           idents: dict[str, set[str]]) -> list[str]:
    dead: list[str] = []
    seen: set[str] = set()
    for path in hdrs:
        raw = texts.get(path)
        if raw is None:
            continue
        for match in PROTO.finditer(strip_comments(raw)):
            name = match.group(1)
            if name in SKIP_DEF or name in ALLOW_EXTERN:
                continue
            if name.startswith("sqlite3") or name.startswith("orig_sqlite3"):
                continue
            if name in seen:
                continue
            c_files = [q for q in corpus if q.endswith(".c") and name in idents.get(q, ())]
            if c_files:
                continue
            seen.add(name)
            dead.append(f"  dead header prototype: {name} ({path})")
    return dead


def scan_unused_macros(hdrs: list[str], corpus: list[str], texts: dict[str, str],
                       idents: dict[str, set[str]], counts: dict[str, Counter]) -> list[str]:
    dead: list[str] = []
    for path in hdrs:
        raw = texts.get(path)
        if raw is None:
            continue
        stripped = strip_comments(raw)
        for match in MACRO.finditer(stripped):
            name = match.group(1)
            if name.endswith("_H") or name.endswith("_H_"):
                continue
            others = [q for q in corpus if q != path and name in idents.get(q, ())]
            if others:
                continue
            if counts.get(path, Counter())[name] <= 1:
                dead.append(f"  dead header macro: {name} ({path})")
    return dead


def extract_func_bodies(path: str, texts: dict[str, str]):
    raw = texts.get(path)
    if raw is None:
        return
    stripped = strip_comments(raw)
    for match in DEF.finditer(stripped):
        name = match.group(1)
        if name in SKIP_DEF:
            continue
        line = match.group(0)
        if not looks_like_def(line, name):
            continue
        rest = stripped[match.end():]
        depth = 1
        i = 0
        while i < len(rest) and depth:
            ch = rest[i]
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    break
            elif ch in ";{":
                break
            i += 1
        if i >= len(rest) or rest[i] != ")":
            continue
        j = rest.find("{", i)
        if j < 0 or ";" in rest[i:j]:
            continue
        body_start = match.end() + j + 1
        depth = 1
        k = body_start
        while k < len(stripped) and depth:
            if stripped[k] == "{":
                depth += 1
            elif stripped[k] == "}":
                depth -= 1
            k += 1
        body = stripped[body_start:k - 1]
        norm = re.sub(r"\s+", " ", body).strip()
        if len(norm) < MIN_CLONE_BODY:
            continue
        lineno = stripped[: match.start()].count("\n") + 1
        digest = hashlib.sha1(norm.encode()).hexdigest()
        yield name, lineno, digest


def scan_clones(src_files: list[str], texts: dict[str, str]) -> list[str]:
    byhash: dict[str, list[tuple[str, int, str]]] = defaultdict(list)
    for path in src_files:
        for name, line, digest in extract_func_bodies(path, texts):
            byhash[digest].append((path, line, name))
    dead: list[str] = []
    for items in sorted(byhash.values(), key=lambda xs: (xs[0][0], xs[0][1])):
        if len(items) < 2:
            continue
        files = {p for p, _, _ in items}
        names = {n for _, _, n in items}
        if len(files) < 2 and len(names) < 2:
            continue
        first = items[0]
        for other in items[1:]:
            if other[0] == first[0] and other[2] == first[2]:
                continue
            dead.append(
                "  duplicate function body: "
                f"{first[2]} ({first[0]}:{first[1]}) identical to "
                f"{other[2]} ({other[0]}:{other[1]})"
            )
    return dead


def resolve_include(inc: str, from_path: str, src_root: str) -> str | None:
    cands = [
        os.path.normpath(os.path.join(os.path.dirname(from_path), inc)),
        os.path.normpath(os.path.join(src_root, inc)),
        os.path.normpath(os.path.join(src_root, os.path.basename(inc))),
    ]
    for cand in cands:
        if os.path.isfile(cand):
            return os.path.abspath(cand)
    return None


def header_has_proto(hpath: str, name: str, texts: dict[str, str]) -> bool:
    raw = texts.get(hpath)
    if raw is None:
        try:
            raw = open(hpath, errors="replace").read()
        except OSError:
            return False
    proto = re.compile(
        r"^(?!\s)(?:SQLITE_PRIVATE\s+|SQLITE_API\s+|SQLITE_EXTERN\s+)?"
        r".+?\b" + re.escape(name) + r"\s*\([^;]*\)\s*;",
        re.M,
    )
    return bool(proto.search(strip_comments(raw)))


def scan_redundant_externs(
    src_files: list[str],
    src_root: str,
    texts: dict[str, str],
) -> list[str]:
    dead: list[str] = []
    for path in src_files:
        raw = texts.get(path)
        if raw is None:
            continue
        stripped = strip_comments(raw)
        headers = []
        for match in INCLUDE.finditer(stripped):
            hpath = resolve_include(match.group(1), path, src_root)
            if hpath:
                headers.append(hpath)
        if not headers:
            continue
        for match in LOCAL_EXTERN.finditer(stripped):
            name = match.group(1)
            if name in SKIP_DEF or name in ALLOW_EXTERN:
                continue
            if name.startswith("sqlite3") or name.startswith("orig_sqlite3"):
                continue
            if not any(header_has_proto(h, name, texts) for h in headers):
                continue
            lineno = stripped[: match.start()].count("\n") + 1
            dead.append(f"  redundant local extern: {name} ({path}:{lineno})")
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
    owned_hdrs = expand(src_root, list(OWNED_HDR_GLOBS))
    corpus = expand(src_root, ["*.c", "*.h"]) + expand(root, [
        "test/*.c",
        "test/c/*.c",
        "ext/*/*.c",
        "ext/*/*.h",
    ])
    corpus = sorted(set(corpus))
    texts, idents, counts = load_tokens(corpus)

    dead: list[str] = []
    dead += scan_unused_externs(src_files, corpus, texts, idents, counts)
    dead += scan_inlines(hdrs, corpus, texts, idents, counts)
    dead += scan_should_be_static(src_files, corpus, texts, idents)
    dead += scan_unused_prototypes(owned_hdrs, corpus, texts, idents)
    dead += scan_duplicate_prototypes(owned_hdrs, texts)
    dead += scan_unused_macros(owned_hdrs, corpus, texts, idents, counts)
    dead += scan_clones(src_files, texts)
    dead += scan_test_only_wrappers(src_files, corpus, root, texts, idents)
    dead += scan_redundant_externs(src_files, src_root, texts)
    for line in dead:
        print(line)
    return 1 if dead else 0


if __name__ == "__main__":
    sys.exit(main())
