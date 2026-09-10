#!/usr/bin/env bash
# Runs every ```sql block in doc/doltlite/*.md against doltlite, page by page,
# on a fixture database, and fails on any statement that errors unless its
# line carries a comment containing "error". Placeholders in the docs
# (example hashes, paths, credential ids) are substituted with fixture values.
#
# Usage: oracle_doc_examples_test.sh [doltlite] [ignored] [page.md]
# DOC_EXAMPLES_KEEP=1 keeps the temp dir (run.sql, out.txt) for debugging.

DOLTLITE="${1:-./doltlite}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCS="$SCRIPT_DIR/../doc/doltlite"
TMPDIR=$(mktemp -d)
[ -n "${DOC_EXAMPLES_KEEP:-}" ] && echo "keeping $TMPDIR" || trap 'rm -rf "$TMPDIR"' EXIT
ONLY="${3:-}"
export DOLTLITE_CREDS_DIR="$TMPDIR/creds"
pass=0; fail=0

case "$DOLTLITE" in /*) ;; *) DOLTLITE="$PWD/$DOLTLITE" ;; esac
if [ ! -x "$DOLTLITE" ] || ! "$DOLTLITE" :memory: "SELECT doltlite_engine();" >/dev/null 2>&1; then
  echo "FAIL: $DOLTLITE is not a runnable doltlite"; echo "Results: 0 passed, 1 failed"; exit 1
fi

# The shell exits 1 when any statement errored under .bail off; anything
# higher (a sanitizer abort, a signal) is an engine failure, never a pass.
engine_ok() { [ "$1" -le 1 ]; }

# Pages whose blocks need inputs a fixture cannot supply.
SKIP="demo.md vec1.md building.md using-existing-sqlite-bindings.md"

fixture() {  # fixture <db>: schema, three commits on main, a conflicting feature branch, tags, a remote
  local db="$1" remote="$TMPDIR/remote.db"
  rm -f "$db" "$remote"
  "$DOLTLITE" "$db" <<SQL >"$TMPDIR/fixture.out" 2>&1
SELECT dolt_config('user.name', 'Fixture'); SELECT dolt_config('user.email', 'fixture@example.com');
CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, email TEXT, active INT, confidence REAL);
CREATE INDEX users_by_email ON users(email);
CREATE TABLE orders(id INTEGER PRIMARY KEY, user_id INT, total REAL);
CREATE TABLE ratings(id INTEGER PRIMARY KEY, rating INT, confidence REAL);
CREATE TABLE parent(id INTEGER PRIMARY KEY);
CREATE TABLE child(id INTEGER PRIMARY KEY, pk INT, pid INT REFERENCES parent(id));
CREATE TABLE log(id INT);
INSERT INTO users VALUES(1,'ann','ann@example.com',1,0.5),(2,'bob','bob@example.com',1,0.9),(42,'cy','cy@example.com',1,0.7);
INSERT INTO orders VALUES(1,1,10.0),(2,2,20.0);
INSERT INTO ratings VALUES(1,3,0.2),(2,4,0.8);
INSERT INTO parent VALUES(1);
SELECT dolt_commit('-Am', 'initial data');
SELECT dolt_tag('v1.0');
SELECT dolt_branch('feature');
SELECT dolt_branch('topic');
SELECT dolt_checkout('topic');
INSERT INTO log VALUES(1);
SELECT dolt_commit('-am', 'topic work');
SELECT dolt_checkout('main');
SELECT dolt_branch('old'); SELECT dolt_branch('src');
SELECT dolt_checkout('feature');
UPDATE users SET name='ann-feature' WHERE id=1;
INSERT INTO users VALUES(3,'dee','dee@example.com',1,0.1);
SELECT dolt_commit('-am', 'feature work');
SELECT dolt_checkout('main');
INSERT INTO orders VALUES(3,1,30.0);
SELECT dolt_commit('-am', 'more orders');
UPDATE users SET name='ann-main' WHERE id=1;
SELECT dolt_commit('-am', 'rename ann');
SELECT dolt_tag('v2.0');
UPDATE ratings SET rating=5, confidence=0.9 WHERE id=1;
INSERT INTO ratings VALUES(3,2,0.95);
CREATE TABLE tmp_table(a);
SELECT dolt_remote('add', 'origin', 'file://$remote');
SELECT dolt_push('origin', 'main');
SELECT dolt_push('origin', '--tags');
SELECT dolt_remote('remove', 'origin');
SELECT dolt_creds_new();
SQL
  local rc=$?
  if [ "$rc" -ne 0 ] || grep -q 'Error' "$TMPDIR/fixture.out"; then
    echo "  fixture failed (rc=$rc): $(grep -m3 -iE 'error|Sanitizer|runtime error' "$TMPDIR/fixture.out" | tr '\n' ' ' | cut -c1-300)"
    return 1
  fi
}

prelude() {  # prelude <page>: extra statements before the page's blocks
  case "$1" in
    dolt_rebase.md) echo "SELECT dolt_checkout('topic'); INSERT INTO orders VALUES(98,1,1); SELECT dolt_commit('-am', 'fixup');" ;;
    dolt_merge.md|dolt_cherry_pick.md) echo "SELECT dolt_reset('--hard'); DROP TABLE tmp_table;" ;;
    dolt_constraint_violations.md) echo "PRAGMA foreign_keys=OFF; INSERT INTO child VALUES(2,2,99);" ;;
    dolt_hashof.md) echo "SELECT dolt_reset('--hard');" ;;
  esac
}

substitute() {  # substitute <db>: rewrite doc placeholders into fixture values
  local db="$1" hash cred
  hash=$("$DOLTLITE" "$db" "SELECT dolt_hashof('topic');" 2>/dev/null)
  cred=$(ls "$DOLTLITE_CREDS_DIR" 2>/dev/null | head -1 | sed 's/\.jwk$//')
  sed -e "s|0123abcd\.\.\.|$hash|g" \
      -e "s|<credential-id>|$cred|g" -e "s|<kid>|$cred|g" \
      -e "s|/srv/authorized-keys|$TMPDIR/authorized-keys|g" \
      -e "s|/path/to/authorized-keys|$TMPDIR/authorized-keys|g" \
      -e "s|/path/compact.db|$TMPDIR/compact.db|g" \
      -e "s|file:///data/remote.db|file://$TMPDIR/remote.db|g" \
      -e "s|file:///data/src.db|file://$TMPDIR/remote.db|g" \
      -e "s|file:///path/to/remote.doltlite|file://$TMPDIR/remote.db|g" \
      -e "s|file:///path/to/source.doltlite|file://$TMPDIR/remote.db|g" \
      -e "s|http://host:8080/mydb.db|file://$TMPDIR/remote.db|g" \
      -e "s|http://myserver:8080/mydb.db|file://$TMPDIR/remote.db|g" \
      -e "s|/path/to/events.sqlite|$TMPDIR/events.sqlite|g"
}

mkdir -p "$TMPDIR/authorized-keys"
if ! "$DOLTLITE" "file:$TMPDIR/events.sqlite?doltlite_engine=sqlite" \
  "CREATE TABLE events(id INTEGER PRIMARY KEY, thread_id INT, type TEXT); CREATE TABLE threads(id INTEGER PRIMARY KEY, title TEXT, archived INT); CREATE TABLE archive(id INTEGER PRIMARY KEY, title TEXT, archived INT); INSERT INTO events VALUES(1,1,'click');" >"$TMPDIR/events.out" 2>&1; then
  echo "FAIL: could not create the stock SQLite fixture: $(head -c 300 "$TMPDIR/events.out")"; echo "Results: 0 passed, 1 failed"; exit 1
fi

for page in "$DOCS"/*.md; do
  name=$(basename "$page")
  case " $SKIP " in *" $name "*) continue ;; esac
  [ -n "$ONLY" ] && [ "$ONLY" != "$name" ] && continue
  # A block that clones starts on a fresh empty database (via .open). Blocks
  # nested inside a four-backtick fence (quoted documents) are not run.
  db="$TMPDIR/$name.db"
  awk -v tmp="$TMPDIR" -v db="$db" -v name="$name" '/^````/{q=!q; next} q{next} /^```sql$/{f=1; n++; buf=""; next} /^```$/{if(f){ c=(buf ~ /dolt_clone\(/); if(c) printf ".open %s/%s.clone_%d.db\n", tmp, name, n; printf "%s", buf; if(c) printf ".open %s\n", db; print "-- @@BLOCK@@"} f=0; next} f{buf=buf $0 "\n"}' "$page" > "$TMPDIR/blocks.sql"
  [ -s "$TMPDIR/blocks.sql" ] || continue
  # The blocks are SQL for the engine. The shell's own escape hatches, dot
  # commands (.shell, .system, .open, ...) and its file functions, would run
  # with the CI runner's privileges; refuse the page. (-safe would block them
  # too, but it also blocks ATTACH and VACUUM INTO, which pages document.)
  if dots=$(grep -nE '^[[:space:]]*\.[A-Za-z]' "$TMPDIR/blocks.sql" | grep -vE '^[0-9]+:\.open '); then
    fail=$((fail+1)); echo "FAIL: $name (shell dot-command in a sql block)"; echo "$dots" | sed 's/^/    /'; continue
  fi
  if fns=$(grep -niE '\b(writefile|readfile|edit|fsdir|zipfile|load_extension)[[:space:]]*\(' "$TMPDIR/blocks.sql"); then
    fail=$((fail+1)); echo "FAIL: $name (shell file function in a sql block)"; echo "$fns" | sed 's/^/    /'; continue
  fi
  if ! fixture "$db"; then fail=$((fail+1)); echo "FAIL: $name (fixture)"; continue; fi
  { echo ".bail off"; prelude "$name"; substitute "$db" < "$TMPDIR/blocks.sql"; } > "$TMPDIR/run.sql"
  "$DOLTLITE" "$db" < "$TMPDIR/run.sql" > "$TMPDIR/out.txt" 2>&1
  rc=$?
  bad_lines=""
  if ! engine_ok "$rc"; then
    bad_lines="
    engine exited $rc: $(grep -m2 -iE 'Sanitizer|runtime error|Abort|Segmentation' "$TMPDIR/out.txt" | tr '\n' ' ' | cut -c1-300)"
  fi
  while IFS= read -r line; do
    case "$line" in
      *"error near line "*|*"Error near line "*|*"error in "*) ;;
      *) continue ;;
    esac
    n=$(echo "$line" | grep -oE 'line [0-9]+' | grep -oE '[0-9]+')
    stmt=$(sed -n "${n}p" "$TMPDIR/run.sql")
    # Allowed when the statement's own line, or the one before it, says so.
    prev=$(sed -n "$((n-1))p" "$TMPDIR/run.sql")
    if echo "$stmt$prev" | grep -qiE -- '--.*(error|fails|refused)'; then continue; fi
    bad_lines="$bad_lines
    $line
      > $stmt"
  done < "$TMPDIR/out.txt"
  if [ -z "$bad_lines" ]; then pass=$((pass+1)); echo "PASS: $name"
  else fail=$((fail+1)); echo "FAIL: $name$bad_lines"; fi
done

if [ "$pass" -eq 0 ]; then fail=$((fail+1)); echo "FAIL: no documentation page was run"; fi

echo ""
echo "================================"
echo "Results: $pass passed, $fail failed"
echo "================================"
[ "$fail" -gt 0 ] && exit 1
exit 0
