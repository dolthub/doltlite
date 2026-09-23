#!/usr/bin/env bash
# A new table is invisible to dolt_commit('-am'). Flag a shell suite that
# creates a table and commits with '-am' without dolt_add or an -A flag
# anywhere in the file, and a single heredoc or echoed SQL block that does
# the same on its own.
set -u
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if [ "${1:-}" = "--selftest" ]; then
  tmp=$(mktemp -d)
  trap 'rm -rf "$tmp"' EXIT
  mkdir -p "$tmp/test"
  cat > "$tmp/test/bad_block.sh" <<'EOF'
echo "CREATE TABLE t(id INT);
SELECT dolt_commit('-am','v1');" | doltlite db
EOF
  cat > "$tmp/test/bad_split.sh" <<'EOF'
echo "CREATE TABLE t(id INT);" | doltlite db
echo "INSERT INTO t VALUES(1); SELECT dolt_commit('-am','v1');" | doltlite db
EOF
  cat > "$tmp/test/ok_am.sh" <<'EOF'
echo "CREATE TABLE t(id INT);
SELECT dolt_commit('-Am','v1');
UPDATE t SET id=2;
SELECT dolt_commit('-am','v2');" | doltlite db
EOF
  fail=0
  if ROOT="$tmp" bash "$SCRIPT_DIR/lint_new_table_am.sh" >/dev/null 2>&1; then
    echo "selftest: bad fixtures were accepted"
    fail=1
  fi
  rm -f "$tmp/test/bad_block.sh" "$tmp/test/bad_split.sh"
  if ! ROOT="$tmp" bash "$SCRIPT_DIR/lint_new_table_am.sh"; then
    echo "selftest: -Am fixture was rejected"
    fail=1
  fi
  if [ "$fail" -ne 0 ]; then
    exit 1
  fi
  echo "lint_new_table_am: selftest passed"
  exit 0
fi

root="${ROOT:-$REPO_ROOT}"
python3 - "$root" <<'PY'
import os, re, sys
root = sys.argv[1]
create_re = re.compile(r"create\s+(?:temp\s+|temporary\s+)?table\b", re.I)
am_re = re.compile(r"""dolt_commit\(\s*['"]-am['"]""")

def offending(text):
    return (create_re.search(text)
            and am_re.search(text)
            and "-A" not in text
            and "dolt_add" not in text.lower())

def heredoc_blocks(text):
    lines = text.splitlines(keepends=True)
    i = 0
    out = []
    while i < len(lines):
        m = re.search(r"<<-?\s*['\"]?([A-Za-z_][A-Za-z0-9_]*)['\"]?", lines[i])
        if not m:
            i += 1
            continue
        strip = lines[i].find("<<-") != -1 and lines[i].find("<<-") < m.start() + 5
        # Prefer the operator nearest the end of the line.
        op = "<<-" if "<<-" in lines[i] else "<<"
        if op not in lines[i]:
            i += 1
            continue
        tag = m.group(1)
        start = i + 1
        body = []
        i += 1
        while i < len(lines):
            line = lines[i]
            check = line.lstrip("\t") if strip else line
            if check.strip() == tag:
                out.append(("".join(body), start))
                break
            body.append(line)
            i += 1
        i += 1
    return out

def echo_blocks(text):
    out = []
    i = 0
    while True:
        j = text.find('echo "', i)
        if j < 0:
            break
        k = j + len('echo "')
        buf = []
        while k < len(text):
            if text[k] == "\\" and k + 1 < len(text):
                buf.append(text[k:k+2])
                k += 2
                continue
            if text[k] == '"':
                break
            buf.append(text[k])
            k += 1
        body = "".join(buf)
        line = text[:j].count("\n") + 1
        out.append((body, line))
        i = k + 1
    return out

hits = []
test_root = os.path.join(root, "test")
for dirpath, _, files in os.walk(test_root):
    base = os.path.basename(dirpath)
    if base in ("lib",):
        continue
    for name in files:
        if not name.endswith(".sh") or name.startswith("lint_"):
            continue
        path = os.path.join(dirpath, name)
        rel = os.path.relpath(path, root)
        text = open(path, errors="replace").read()
        if offending(text):
            hits.append(f"{rel}: file creates a table and dolt_commit('-am') "
                        "without dolt_add or -A")
        for body, line in heredoc_blocks(text) + echo_blocks(text):
            if offending(body):
                hits.append(f"{rel}:{line}: SQL block creates a table and "
                            "dolt_commit('-am') without dolt_add or -A")

if hits:
    print("lint_new_table_am: new tables are skipped by dolt_commit('-am')")
    for hit in hits:
        print("  " + hit)
    sys.exit(1)
print("lint_new_table_am: ok")
PY
