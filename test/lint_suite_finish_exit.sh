#!/usr/bin/env bash
# A suite that ends on *_finish reports that function's status. A later
# statement replaces it. The only statement allowed after that call is
# exit $?, which keeps the status. exit 0 turns a failed tally into success.
set -u
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if [ "${1:-}" = "--selftest" ]; then
  tmp=$(mktemp -d)
  trap 'rm -rf "$tmp"' EXIT
  mkdir -p "$tmp/test"
  fail=0
  # Each bad ending is the only suite in the tree. One rejection must
  # not hide another pattern the scanner still accepts.
  reject() {
    local name="$1"
    cat > "$tmp/test/$name"
    if ROOT="$tmp" bash "$SCRIPT_DIR/lint_suite_finish_exit.sh" >/dev/null 2>&1; then
      echo "selftest: $name was accepted"
      fail=1
    fi
    rm -f "$tmp/test/$name"
  }
  # The cross-op conflict suite used to end this way, so every failed
  # comparison still exited 0.
  reject vc_oracle_cross_op_conflict_cv_test.sh <<'EOF'
pull_dual_flow "pull_conflict_and_cv"

vc_oracle_finish
exit 0
EOF
  reject same_line_exit_0.sh <<'EOF'
vc_oracle_finish; exit 0
EOF
  reject exit_one.sh <<'EOF'
dltest_finish
exit 1
EOF
  cat > "$tmp/test/finish_only.sh" <<'EOF'
echo setup
vc_oracle_finish
EOF
  cat > "$tmp/test/finish_status.sh" <<'EOF'
stock_oracle_finish
exit $?
EOF
  cat > "$tmp/test/finish_same_line.sh" <<'EOF'
dltest_finish; exit $?
EOF
  # A finish call in the middle is not the suite's final statement.
  cat > "$tmp/test/finish_mid.sh" <<'EOF'
if vc_oracle_finish > log 2>&1; then
  echo 'FAIL: empty suite accepted'
  exit 1
fi
echo done
EOF
  if ! ROOT="$tmp" bash "$SCRIPT_DIR/lint_suite_finish_exit.sh"; then
    echo "selftest: a terminating finish was rejected"
    fail=1
  fi
  if [ "$fail" -ne 0 ]; then
    exit 1
  fi
  echo "lint_suite_finish_exit: selftest passed"
  exit 0
fi

root="${ROOT:-$REPO_ROOT}"
python3 - "$root" <<'PY'
import glob, os, sys
root = sys.argv[1]

def statements(text):
    out = []
    buf = []
    i = 0
    n = len(text)
    squote = False
    dquote = False
    while i < n:
        c = text[i]
        if c == "\\" and not squote and i + 1 < n:
            buf.append(c)
            buf.append(text[i + 1])
            i += 2
            continue
        if c == "'" and not dquote:
            squote = not squote
            buf.append(c)
            i += 1
            continue
        if c == '"' and not squote:
            dquote = not dquote
            buf.append(c)
            i += 1
            continue
        if not squote and not dquote and c == "#":
            while i < n and text[i] != "\n":
                i += 1
            continue
        if not squote and not dquote and c in ";\n":
            piece = "".join(buf).strip()
            if piece:
                out.append(piece)
            buf = []
            i += 1
            continue
        buf.append(c)
        i += 1
    piece = "".join(buf).strip()
    if piece:
        out.append(piece)
    return out

def is_finish(stmt):
    return stmt.endswith("_finish") and stmt.replace("_", "").isalnum()

bad = []
pattern = os.path.join(root, "test", "*.sh")
for path in sorted(glob.glob(pattern)):
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        stmts = statements(fh.read())
    if len(stmts) < 2 or not is_finish(stmts[-2]):
        continue
    if stmts[-1] != "exit $?":
        bad.append((path, stmts[-2], stmts[-1]))

if bad:
    for path, prev, last in bad:
        rel = os.path.relpath(path, root)
        print("%s: statement after %s must be 'exit $?'" % (rel, prev))
        print("  got: %s" % last)
    sys.exit(1)
print("lint_suite_finish_exit: ok")
PY
exit $?
