#!/usr/bin/env python3
"""Drop DOLTLITE_PROLLY branches the way unifdef -UDOLTLITE_PROLLY does.

Other #if directives are left in the text. Used by lint_prolly_guards.sh so
a DOLTLITE_PROLLY=0 compile of SQLite-core files stays stock.
"""

import re
import sys

DIR_RE = re.compile(
    r"^([ \t]*#[ \t]*)(ifdef|ifndef|if|elif|else|endif)\b(.*)$"
)


def _eval_if(expr):
    e = expr.strip()
    if "//" in e:
        e = e.split("//", 1)[0].strip()
    e = re.sub(r"/\*.*?\*/", " ", e)
    e = re.sub(r"\s+", " ", e).strip()
    if "DOLTLITE_PROLLY" not in e:
        return None
    e = re.sub(r"defined\s*\(\s*DOLTLITE_PROLLY\s*\)", "0", e)
    e = re.sub(r"\bDOLTLITE_PROLLY\b", "0", e)
    while True:
        n = e
        n = re.sub(r"!\s*0", "1", n)
        n = re.sub(r"!\s*1", "0", n)
        n = re.sub(r"\(\s*0\s*\)", "0", n)
        n = re.sub(r"\(\s*1\s*\)", "1", n)
        if n == e:
            break
        e = n
    e = e.strip()
    if e == "0":
        return False
    if e == "1":
        return True
    if re.match(r"0\s*&&", e):
        return False
    if re.match(r"1\s*\|\|", e):
        return True
    return None


def _eval_dir(kind, rest):
    rest = rest.strip()
    if kind == "ifdef":
        name = rest.split()[0] if rest else ""
        if name == "DOLTLITE_PROLLY":
            return False
        return None
    if kind == "ifndef":
        name = rest.split()[0] if rest else ""
        if name == "DOLTLITE_PROLLY":
            return True
        return None
    if kind in ("if", "elif"):
        return _eval_if(rest)
    return None


def strip(text):
    out = []
    # keep: emit lines. resolved: this frame ate its #if/#else/#endif.
    stack = []

    def parent_keep():
        return all(fr[0] for fr in stack)

    for line in text.splitlines(True):
        raw = line[:-1] if line.endswith("\n") else line
        m = DIR_RE.match(raw)
        if not m:
            if parent_keep():
                out.append(line)
            continue
        kind = m.group(2)
        rest = m.group(3)
        if kind in ("if", "ifdef", "ifndef"):
            val = _eval_dir(kind, rest)
            if not parent_keep():
                stack.append((False, True))
                continue
            if val is None:
                stack.append((True, False))
                out.append(line)
            else:
                stack.append((val, True))
            continue
        if kind == "elif":
            if not stack:
                if parent_keep():
                    out.append(line)
                continue
            keep, resolved = stack[-1]
            if not resolved:
                out.append(line)
                continue
            if keep:
                stack[-1] = (False, True)
            else:
                val = _eval_dir("elif", rest)
                if val is None:
                    stack[-1] = (True, False)
                    out.append(line)
                else:
                    stack[-1] = (val, True)
            continue
        if kind == "else":
            if not stack:
                if parent_keep():
                    out.append(line)
                continue
            keep, resolved = stack[-1]
            if not resolved:
                out.append(line)
                continue
            above = all(fr[0] for fr in stack[:-1])
            stack[-1] = (above and not keep, True)
            continue
        if kind == "endif":
            if not stack:
                if parent_keep():
                    out.append(line)
                continue
            _keep, resolved = stack.pop()
            if not resolved and parent_keep():
                out.append(line)
            continue
        if parent_keep():
            out.append(line)
    return "".join(out)


def main(argv):
    if len(argv) == 1 or argv[1] == "-":
        sys.stdout.write(strip(sys.stdin.read()))
        return 0
    for path in argv[1:]:
        with open(path) as fh:
            sys.stdout.write(strip(fh.read()))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
