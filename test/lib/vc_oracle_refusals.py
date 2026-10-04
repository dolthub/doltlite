#!/usr/bin/env python3
"""Which statements a VC oracle script refused.

Error text is not compared. Each statement gets a bit: 1 if that statement
failed, 0 if it ran. Session-setup statements that only one engine needs
(BEGIN, ROLLBACK, dot-commands, conflict-session SETs) are left out, even
when they fail, so the remaining bits can be compared directly.

DoltLite refuses dolt_merge, dolt_cherry_pick, dolt_revert, dolt_pull,
dolt_verify_constraints, and dolt_conflicts_resolve when the outcome is a
conflict or a constraint violation. Dolt returns that in the procedure row.
Those documented failures are recorded as D. D matches a success (0) or
another refusal (1) at the same position. Every other failure stays 1, so
a one-sided refusal of any other statement still mismatches.
"""
import re
import sys

_ERROR_LINE = re.compile(
    r"\berror:?\s+(?:near|on)\s+line\s+(\d+)",
    re.IGNORECASE,
)
_EXPECT = re.compile(r"--.*(?:error|fails|refused)", re.IGNORECASE)
_HARNESS = re.compile(
    r"^(?:set\s+@@autocommit\b.*|"
    r"set\s+@@dolt_allow_commit_conflicts\b.*|"
    r"set\s+@@dolt_force_transaction_commit\b.*|"
    r"begin(?:\s+(?:transaction|work))?|"
    r"commit|"
    r"rollback)$",
    re.IGNORECASE,
)


def statements(sql):
    """Return {text, start, end} with 1-based inclusive line numbers.

    A trigger body (BEGIN ... END after other tokens) stays one statement.
    A transaction BEGIN ends at its semicolon.
    """
    out = []
    buf = []
    start_line = 1
    line = 1
    i = 0
    n = len(sql)
    squote = False
    dquote = False
    bquote = False
    depth = 0
    while i < n:
        c = sql[i]
        if c == "\n":
            line += 1
            # A dot-command is one line. Leaving it open would swallow the
            # next statement up to its semicolon and hide that statement.
            if (
                not squote and not dquote and not bquote and depth == 0
                and "".join(buf).lstrip().startswith(".")
            ):
                text = "".join(buf).strip()
                out.append({"text": text, "start": start_line, "end": line - 1})
                buf = []
                start_line = line
                i += 1
                continue
        if not squote and not dquote and not bquote and c == "-" and i + 1 < n and sql[i + 1] == "-":
            while i < n and sql[i] != "\n":
                buf.append(sql[i])
                i += 1
            continue
        if not squote and not dquote and not bquote and c == "/" and i + 1 < n and sql[i + 1] == "*":
            buf.append(c)
            buf.append("*")
            i += 2
            while i < n:
                if sql[i] == "\n":
                    line += 1
                if sql[i] == "*" and i + 1 < n and sql[i + 1] == "/":
                    buf.append("*")
                    buf.append("/")
                    i += 2
                    break
                buf.append(sql[i])
                i += 1
            continue
        if c == "'" and not dquote and not bquote:
            squote = not squote
            buf.append(c)
            i += 1
            continue
        if c == '"' and not squote and not bquote:
            dquote = not dquote
            buf.append(c)
            i += 1
            continue
        if c == "`" and not squote and not dquote:
            bquote = not bquote
            buf.append(c)
            i += 1
            continue
        if not squote and not dquote and not bquote:
            word = None
            if i == 0 or not (sql[i - 1].isalnum() or sql[i - 1] == "_"):
                j = i
                while j < n and (sql[j].isalnum() or sql[j] == "_"):
                    j += 1
                word = sql[i:j] if j > i else None
            if word and word.lower() == "begin":
                prior = "".join(buf)
                prior = re.sub(r"--[^\n]*", " ", prior)
                prior = re.sub(r"/\*.*?\*/", " ", prior, flags=re.S)
                if prior.strip():
                    depth += 1
            elif word and word.lower() == "end" and depth > 0:
                depth -= 1
        if not squote and not dquote and not bquote and c == ";" and depth == 0:
            text = "".join(buf).strip()
            if text:
                out.append({"text": text, "start": start_line, "end": line})
            buf = []
            i += 1
            k = i
            while k < n and sql[k] in " \t":
                k += 1
            # A trailing comment on this line annotates the statement that
            # just ended. The next statement starts on the following line.
            if k < n and sql[k:k + 2] == "--":
                while k < n and sql[k] != "\n":
                    k += 1
                i = k
                start_line = line + 1 if i < n else line
                continue
            if i < n and sql[i] != "\n":
                start_line = line
            else:
                start_line = line + 1 if i < n else line
            continue
        buf.append(c)
        i += 1
    text = "".join(buf).strip()
    if text:
        out.append({"text": text, "start": start_line, "end": line})
    return out


def _is_harness(text):
    body = text.strip()
    if body.startswith("."):
        return True
    body = re.sub(r"--[^\n]*", " ", body)
    body = re.sub(r"/\*.*?\*/", " ", body, flags=re.S)
    body = re.sub(r"\s+", " ", body).strip().rstrip(";").strip()
    return _HARNESS.match(body) is not None


def error_lines(stderr):
    found = []
    for line in stderr.splitlines():
        match = _ERROR_LINE.search(line)
        if match:
            found.append(int(match.group(1)))
    return found


def _failed_indexes(stmts, lines):
    failed = set()
    uncovered = 0
    for n in lines:
        hit = False
        for idx, stmt in enumerate(stmts):
            if stmt["start"] <= n <= stmt["end"]:
                failed.add(idx)
                hit = True
                break
        if not hit:
            uncovered += 1
    return failed, uncovered


def _failed_map(stmts, stderr):
    """Map a statement index to the error line that named it."""
    failed = {}
    uncovered = 0
    for raw in stderr.splitlines():
        match = _ERROR_LINE.search(raw)
        if not match:
            continue
        n = int(match.group(1))
        hit = False
        for idx, stmt in enumerate(stmts):
            if stmt["start"] <= n <= stmt["end"]:
                prev = failed.get(idx, "")
                failed[idx] = (prev + "\n" + raw).strip()
                hit = True
                break
        if not hit:
            uncovered += 1
    return failed, uncovered


# These calls fail in DoltLite when the result is a conflict or a constraint
# violation. Dolt puts that result in the procedure row instead.
_STRICTER = re.compile(
    r"\b(?:select\s+|call\s+)?dolt_(?:merge|cherry_pick|revert|pull|verify_constraints|conflicts_resolve)\s*\(",
    re.IGNORECASE,
)
_DOCUMENTED_REFUSAL = re.compile(
    r"has \d+ conflict\(s\)|constraint violations|conflicts detected",
    re.IGNORECASE,
)


def _token(stmt_text, err_text):
    if not err_text:
        return "0"
    if _STRICTER.search(stmt_text) and _DOCUMENTED_REFUSAL.search(err_text):
        return "D"
    return "1"


def refusal_rows(sql, stderr):
    """Comparable statements as (token, one-line text)."""
    stmts = statements(sql)
    failed, uncovered = _failed_map(stmts, stderr)
    rows = []
    for idx, stmt in enumerate(stmts):
        if _is_harness(stmt["text"]):
            continue
        text = re.sub(r"\s+", " ", stmt["text"]).strip()
        rows.append((_token(stmt["text"], failed.get(idx, "")), text[:160]))
    rows.extend(("1", "<unmapped error>") for _ in range(uncovered))
    return rows


def refusal_bits(sql, stderr):
    return ",".join(token for token, _text in refusal_rows(sql, stderr))


def target_error(sql, stderr, conflicted_setup=False, pattern=""):
    stmts = statements(sql)
    failed, uncovered = _failed_map(stmts, stderr)
    if not stmts or uncovered or len(stmts) - 1 not in failed:
        return "the final statement did not report an attributable error"
    for idx, error in failed.items():
        if idx == len(stmts) - 1:
            continue
        if (conflicted_setup
                and re.search(r"\b(?:SELECT|CALL)\s+dolt_merge\s*\(",
                              stmts[idx]["text"], re.I)
                and re.search(r"Merge has [1-9]\d* conflict\(s\)", error)):
            continue
        return "setup statement %d failed: %s" % (idx + 1, error)
    if pattern and not re.search(pattern, failed[len(stmts) - 1]):
        return "the final statement did not match error pattern %s" % pattern
    return ""


def _split_bits(bits):
    if not bits:
        return []
    return bits.split(",")


def vectors_match(left, right):
    """True when both sides refused the same statements.

    D is a documented conflict or constraint-violation refusal. It agrees
    with 0, because Dolt reports that result in the procedure row, and with
    1, because the other engine refused the same call for a different reason.
    """
    a = _split_bits(left)
    b = _split_bits(right)
    if len(a) != len(b):
        return False
    for x, y in zip(a, b):
        if x == y or x == "D" or y == "D":
            continue
        return False
    return True


def annotated_bits(sql, output):
    """Expected bits from -- error/fails/refused comments, and actual bits."""
    stmts = statements(sql)
    raw = sql.splitlines()
    failed, uncovered = _failed_indexes(stmts, error_lines(output))
    expect = []
    actual = []
    for idx, stmt in enumerate(stmts):
        # A comment on its own line annotates the next statement. A trailing
        # comment belongs to the statement on that line, not the one after it.
        prev = raw[stmt["start"] - 2] if stmt["start"] >= 2 else ""
        if not re.match(r"\s*--", prev):
            prev = ""
        chunk = prev + "\n" + "\n".join(raw[stmt["start"] - 1:stmt["end"]])
        exp = 1 if _EXPECT.search(chunk) else 0
        act = 1 if idx in failed else 0
        if _is_harness(stmt["text"]) and exp == 0 and act == 0:
            continue
        expect.append(str(exp))
        actual.append(str(act))
    actual.extend(["1"] * uncovered)
    expect.extend(["0"] * uncovered)
    return ",".join(expect), ",".join(actual)


def _selftest():
    sql = "CREATE TABLE t(id INT);\nSELECT missing FROM t;\nSELECT 1;\n"
    err = "Parse error near line 2: no such column: missing\n  SELECT missing FROM t;\n"
    got = refusal_bits(sql, err)
    assert got == "0,1,0", got

    dl = "SET @@autocommit = 0;\nBEGIN;\nSELECT dolt_merge('feat');\nROLLBACK;\n"
    dt = (
        "SET @@dolt_allow_commit_conflicts = 1;\n"
        "SET @@dolt_force_transaction_commit = 1;\n"
        "CALL dolt_merge('feat');\n"
    )
    documented = (
        "Error near line 3: Merge has 1 conflict(s). "
        "Resolve and then commit with dolt_commit.\n"
    )
    # Documented conflict refusal. Dolt's call succeeds. A pull can also
    # refuse on Dolt with a different message; both of those agree with D.
    assert refusal_bits(dl, documented) == "D", refusal_bits(dl, documented)
    assert refusal_bits(dt, "") == "0"
    assert vectors_match("D", "0")
    assert vectors_match("0,D,0", "0,0,0")
    assert vectors_match("D", "1")
    assert not vectors_match("1", "0")
    assert vectors_match("", "")
    branch = "Error near line 3: no such branch: feat\n"
    assert refusal_bits(dl, branch) == "1", refusal_bits(dl, branch)
    assert not vectors_match(refusal_bits(dl, ""), refusal_bits(dt, branch))
    shared = "Error near line 3: cannot pull with uncommitted changes\n"
    assert refusal_bits(dl, shared) == refusal_bits(dt, shared) == "1"
    assert vectors_match(refusal_bits(dl, shared), refusal_bits(dt, shared))
    rollback = "SELECT 1;\nROLLBACK;\n"
    rollback_err = "Error near line 2: cannot rollback - no transaction is active\n"
    assert refusal_bits(rollback, rollback_err) == "0", refusal_bits(rollback, rollback_err)

    trigger = (
        "CREATE TRIGGER trg AFTER INSERT ON t BEGIN\n"
        "  INSERT INTO audit VALUES (NEW.id);\n"
        "END;\n"
        "SELECT missing FROM t;\n"
    )
    stmts = statements(trigger)
    assert len(stmts) == 2, stmts
    assert refusal_bits(trigger, "Parse error near line 4: no such column: missing\n") == "0,1"

    multi = "SELECT\n  missing\nFROM t;\nSELECT 1;\n"
    assert refusal_bits(multi, "Parse error near line 2: no such column: missing\n") == "1,0"

    dots = ".headers off\n.mode list\nSELECT missing FROM t;\n"
    assert refusal_bits(dots, "Parse error near line 3: no such column: missing\n") == "1"
    # The dot-command must not swallow CREATE, or that statement disappears
    # from one engine's vector.
    headed = ".headers off\nCREATE TABLE t(id INT);\nSELECT missing FROM t;\n"
    assert (
        refusal_bits(headed, "Parse error near line 3: no such column: missing\n")
        == "0,1"
    ), refusal_bits(headed, "Parse error near line 3: no such column: missing\n")

    noted = "SELECT 1; -- error: refused\nSELECT 2;\n"
    exp, act = annotated_bits(noted, "Error near line 1: refused\n")
    assert exp == act == "1,0", (exp, act)
    exp, act = annotated_bits(noted, "")
    assert exp == "1,0" and act == "0,0", (exp, act)
    leading = "-- error: next statement is refused\nSELECT 1;\nSELECT 2;\n"
    exp, act = annotated_bits(leading, "Error near line 2: refused\n")
    assert exp == act == "1,0", (exp, act)
    target = "SELECT 'quoted;value';\nSELECT\n  missing\nFROM t;\n"
    assert not target_error(target, "Parse error near line 2: no such column: missing")
    assert not target_error(trigger, "Parse error near line 4: no such column: missing")
    assert target_error(target, "Error near line 1: setup failed")
    assert target_error(target, "Error near line 1: setup failed\nError near line 2: target failed")
    assert target_error(target, "startup failure")
    assert target_error(target, "Error near line 99: unlocated")
    assert target_error(target, "")
    assert target_error(target, "Error near line 2: wrong class", pattern="expected class")
    conflicted = "BEGIN;\nSELECT dolt_merge('feature');\nSELECT dolt_commit('-m','c');\n"
    errors = "Error near line 2: Merge has 1 conflict(s)\nError near line 3: unresolved merge conflicts"
    assert target_error(conflicted, errors)
    assert not target_error(conflicted, errors, True, "unresolved merge conflicts")
    assert target_error(conflicted, errors.replace("Merge has 1 conflict(s)", "no such branch"), True)
    assert target_error(conflicted.replace("dolt_merge", "dolt_branch"), errors, True)
    print("vc_oracle_refusals: selftest passed")


def main(argv):
    if len(argv) < 2:
        sys.stderr.write(
            "usage: vc_oracle_refusals.py bits|rows|match|annotated|expected-errors|target-error|selftest\n"
        )
        return 2
    cmd = argv[1]
    if cmd == "selftest":
        _selftest()
        return 0
    if cmd == "expected-errors":
        with open(argv[2], "r", encoding="utf-8", errors="replace") as fh:
            error = fh.read()
        stmts = statements(sys.stdin.read())
        failed, uncovered = _failed_map(stmts, error)
        if not failed or uncovered or not argv[3]:
            return 1
        return 0 if all(re.search(argv[3], line) for message in failed.values()
                        for line in message.splitlines()) else 1
    if cmd == "target-error":
        with open(argv[2], "r", encoding="utf-8", errors="replace") as fh:
            error = fh.read()
        conflicted = len(argv) > 3 and argv[3] == "--conflicted-setup"
        pattern = argv[4] if len(argv) > 4 else ""
        reason = target_error(sys.stdin.read(), error, conflicted, pattern)
        if reason:
            sys.stdout.write(reason + "\n")
            return 1
        return 0
    if cmd == "match":
        left = argv[2] if len(argv) > 2 else ""
        right = argv[3] if len(argv) > 3 else ""
        return 0 if vectors_match(left, right) else 1
    if cmd == "bits":
        err_path = argv[2] if len(argv) > 2 and argv[2] else ""
        stderr = ""
        if err_path:
            try:
                with open(err_path, "r", encoding="utf-8", errors="replace") as fh:
                    stderr = fh.read()
            except OSError:
                stderr = ""
        sys.stdout.write(refusal_bits(sys.stdin.read(), stderr))
        return 0
    if cmd == "rows":
        err_path = argv[2] if len(argv) > 2 and argv[2] else ""
        stderr = ""
        if err_path:
            try:
                with open(err_path, "r", encoding="utf-8", errors="replace") as fh:
                    stderr = fh.read()
            except OSError:
                stderr = ""
        for bit, text in refusal_rows(sys.stdin.read(), stderr):
            sys.stdout.write("%s %s\n" % (bit, text))
        return 0
    if cmd == "annotated":
        sql_path, out_path = argv[2], argv[3]
        with open(sql_path, "r", encoding="utf-8", errors="replace") as fh:
            sql = fh.read()
        with open(out_path, "r", encoding="utf-8", errors="replace") as fh:
            output = fh.read()
        expect, actual = annotated_bits(sql, output)
        if expect != actual:
            sys.stdout.write(
                "expected refusals %s, got %s\n" % (expect or "<none>", actual or "<none>")
            )
            return 1
        return 0
    sys.stderr.write("unknown command %s\n" % cmd)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
