#!/bin/bash
#
# Version-control oracle test: dolt_ignore
#
# Pins doltlite's dolt_ignore behavior to Dolt 1.83.5. dolt_ignore is a
# real user table with schema:
#
#   CREATE TABLE dolt_ignore (
#     pattern TEXT NOT NULL,
#     ignored TINYINT(1) NOT NULL,
#     PRIMARY KEY(pattern)
#   );
#
# Rules observed against Dolt 1.83.5:
#
#   1. dolt_ignore exists from init; queryable but not in SHOW TABLES.
#   2. Pattern matching:
#        %  and  *   → zero or more characters
#        ?           → exactly one character
#        other       → literal
#   3. Pattern matching only gates *new* (untracked) tables. Once a
#      table is committed, modifications always show in dolt_status.
#   4. Among all matching patterns the most specific wins. "Most
#      specific" is roughly the longest literal-prefix match — an
#      exact literal beats any wildcard pattern.
#   5. If two matching patterns disagree on ignored and neither is
#      strictly more specific, dolt_status and dolt_add raise an
#      error identifying the conflict.
#   6. CALL dolt_add('-A') / dolt_add('.'):
#        - stages all non-ignored untracked tables
#        - silently skips ignored ones
#        - errors on an unresolved conflict
#   7. CALL dolt_add('<ignored_name>'):
#        - silent success (exit 0, no staging)
#   8. CALL dolt_commit('-A','-m',...) = dolt_add('-A') + commit;
#      respects dolt_ignore the same way.
#   9. Removing a pattern from dolt_ignore immediately re-exposes
#      previously-hidden tables to dolt_status / dolt_add -A.
#  10. dolt_ignore itself is a regular table: it needs staging and
#      committing to persist across sessions, and patterns like '*'
#      or 'dolt_ignore' hide it from dolt_status the same way.
#
# Comparisons use dolt_status's table_name / staged / status tuple as
# the ground truth, filtered to exclude dolt_ignore itself. Whether
# dolt_ignore shows up as "new table" vs "modified" is an internal
# representation detail — Dolt materializes it lazily on first write
# while doltlite pre-creates it in the seed commit — and the oracle
# tests the pattern-matching semantics, not dolt_ignore's own staging
# accounting. Error oracles compare only that both engines report an
# error; exact error text is allowed to differ.
#
# Usage: bash vc_oracle_ignore_test.sh [path/to/doltlite] [path/to/dolt]
#

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

normalize() { tr -d '\r' | grep -v '^S|dolt_ignore|' | sort; }

# oracle: run the same setup on both engines and compare their
# resulting dolt_status (table_name + staged + status).
oracle() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\nSELECT 'S|' || table_name || '|' || staged || '|' || status FROM dolt_status;\n" "$setup" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep '^S|' \
           | normalize)

  local dolt_setup
  dolt_setup=$(echo "$setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')

  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    echo "$dolt_setup" | "$DOLT" sql >/dev/null 2>"$dir/dt.err"
    "$DOLT" sql -r csv -q "SELECT concat('S|', table_name, '|', staged, '|', status) FROM dolt_status" 2>>"$dir/dt.err"
  ) > "$dir/dt.raw"

  local dt_out
  dt_out=$(grep '^S|' "$dir/dt.raw" | tr -d '"' | normalize)

  if [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite:"; echo "$dl_out" | sed 's/^/      /'
    echo "    dolt:"    ; echo "$dt_out" | sed 's/^/      /'
  fi
}

# oracle_error: both engines must error on the same setup. Exact
# message text is allowed to differ.
oracle_error() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/${name}_err"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_err=0
  echo "$setup" | "$DOLTLITE" "$dir/dl/db" >"$dir/dl.out" 2>"$dir/dl.err"
  if grep -qiE 'error|fail|conflict' "$dir/dl.out" "$dir/dl.err" 2>/dev/null; then dl_err=1; fi

  local dolt_setup
  dolt_setup=$(echo "$setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')
  local dt_err=0
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    echo "$dolt_setup" | "$DOLT" sql >"$dir/dt.out" 2>"$dir/dt.err"
  )
  if grep -qiE 'error|fail|conflict' "$dir/dt.out" "$dir/dt.err" 2>/dev/null; then dt_err=1; fi

  if [ "$dl_err" = 1 ] && [ "$dt_err" = 1 ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected both to error)"
    echo "    doltlite errored: $([ $dl_err = 1 ] && echo yes || echo NO)"
    echo "    dolt errored:     $([ $dt_err = 1 ] && echo yes || echo NO)"
  fi
}

echo "=== Version Control Oracle Tests: dolt_ignore ==="
echo ""

# ---------------------------------------------------------------
# Schema & bootstrap
# ---------------------------------------------------------------

echo "--- schema & bootstrap ---"

oracle "empty_ignore" "
CREATE TABLE t(x INT PRIMARY KEY);
CREATE TABLE u(x INT PRIMARY KEY);
"

# ---------------------------------------------------------------
# Basic pattern matching. Both * and % are wildcards.
# ---------------------------------------------------------------

echo "--- basic pattern matching ---"

oracle "literal" "
INSERT INTO dolt_ignore VALUES ('secret', 1);
CREATE TABLE secret(x INT PRIMARY KEY);
CREATE TABLE public(x INT PRIMARY KEY);
"

oracle "star_wild" "
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
CREATE TABLE tmp_a(x INT PRIMARY KEY);
CREATE TABLE tmp_b(x INT PRIMARY KEY);
CREATE TABLE keep(x INT PRIMARY KEY);
"

oracle "percent_wild" "
INSERT INTO dolt_ignore VALUES ('tmp_%', 1);
CREATE TABLE tmp_a(x INT PRIMARY KEY);
CREATE TABLE tmp_b(x INT PRIMARY KEY);
CREATE TABLE keep(x INT PRIMARY KEY);
"

oracle "trailing_wild" "
INSERT INTO dolt_ignore VALUES ('%_temp', 1);
CREATE TABLE foo_temp(x INT PRIMARY KEY);
CREATE TABLE foo(x INT PRIMARY KEY);
"

oracle "mid_wild" "
INSERT INTO dolt_ignore VALUES ('a_%_z', 1);
CREATE TABLE a_b_z(x INT PRIMARY KEY);
CREATE TABLE a_bc_z(x INT PRIMARY KEY);
CREATE TABLE a_b(x INT PRIMARY KEY);
CREATE TABLE b_z(x INT PRIMARY KEY);
"

oracle "question_mark" "
INSERT INTO dolt_ignore VALUES ('d_?', 1);
CREATE TABLE d_x(x INT PRIMARY KEY);
CREATE TABLE d_xy(x INT PRIMARY KEY);
CREATE TABLE d_(x INT PRIMARY KEY);
"

oracle "question_mark_multi" "
INSERT INTO dolt_ignore VALUES ('e??', 1);
CREATE TABLE eab(x INT PRIMARY KEY);
CREATE TABLE ea(x INT PRIMARY KEY);
CREATE TABLE eabc(x INT PRIMARY KEY);
"

# ---------------------------------------------------------------
# Specificity: exact beats wildcard, longer literal beats shorter.
# ---------------------------------------------------------------

echo "--- specificity ---"

oracle "exact_over_wild" "
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
INSERT INTO dolt_ignore VALUES ('tmp_keep', 0);
CREATE TABLE tmp_foo(x INT PRIMARY KEY);
CREATE TABLE tmp_keep(x INT PRIMARY KEY);
"

oracle "cascade" "
INSERT INTO dolt_ignore VALUES ('*', 1);
INSERT INTO dolt_ignore VALUES ('data_*', 0);
INSERT INTO dolt_ignore VALUES ('data_secret', 1);
CREATE TABLE data_public(x INT PRIMARY KEY);
CREATE TABLE data_secret(x INT PRIMARY KEY);
CREATE TABLE random_thing(x INT PRIMARY KEY);
"

oracle "unignore_then_ignore_again" "
INSERT INTO dolt_ignore VALUES ('foo_%', 1);
INSERT INTO dolt_ignore VALUES ('foo_keep_%', 0);
INSERT INTO dolt_ignore VALUES ('foo_keep_never', 1);
CREATE TABLE foo_drop(x INT PRIMARY KEY);
CREATE TABLE foo_keep_a(x INT PRIMARY KEY);
CREATE TABLE foo_keep_never(x INT PRIMARY KEY);
"

# ---------------------------------------------------------------
# Conflict: two equally-specific patterns disagree.
# ---------------------------------------------------------------

echo "--- conflicts ---"

oracle_error "conflict_two_wild" "
INSERT INTO dolt_ignore VALUES ('foo_%', 1);
INSERT INTO dolt_ignore VALUES ('%_bar', 0);
CREATE TABLE foo_bar(x INT PRIMARY KEY);
SELECT table_name FROM dolt_status;
"

oracle_error "conflict_dolt_add_A" "
INSERT INTO dolt_ignore VALUES ('foo_%', 1);
INSERT INTO dolt_ignore VALUES ('%_bar', 0);
CREATE TABLE foo_bar(x INT PRIMARY KEY);
SELECT dolt_add('-A');
"

oracle_error "conflict_dolt_add_name" "
INSERT INTO dolt_ignore VALUES ('foo_%', 1);
INSERT INTO dolt_ignore VALUES ('%_bar', 0);
CREATE TABLE foo_bar(x INT PRIMARY KEY);
SELECT dolt_add('foo_bar');
"

# ---------------------------------------------------------------
# dolt_add: -A, ., explicit name.
# ---------------------------------------------------------------

echo "--- dolt_add ---"

oracle "add_dash_A_skips_ignored" "
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
CREATE TABLE tmp_foo(x INT PRIMARY KEY);
CREATE TABLE keep(x INT PRIMARY KEY);
SELECT dolt_add('-A');
"

oracle "add_dot_skips_ignored" "
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
CREATE TABLE tmp_foo(x INT PRIMARY KEY);
CREATE TABLE keep(x INT PRIMARY KEY);
SELECT dolt_add('.');
"

oracle "add_explicit_ignored_is_noop" "
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
CREATE TABLE tmp_foo(x INT PRIMARY KEY);
CREATE TABLE keep(x INT PRIMARY KEY);
SELECT dolt_add('tmp_foo');
"

oracle "add_explicit_non_ignored" "
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
CREATE TABLE tmp_foo(x INT PRIMARY KEY);
CREATE TABLE keep(x INT PRIMARY KEY);
SELECT dolt_add('keep');
"

# ---------------------------------------------------------------
# dolt_commit -A respects dolt_ignore.
# ---------------------------------------------------------------

echo "--- dolt_commit -A ---"

oracle "commit_A_skips_ignored" "
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
CREATE TABLE tmp_foo(x INT PRIMARY KEY);
INSERT INTO tmp_foo VALUES (1);
CREATE TABLE keep(x INT PRIMARY KEY);
SELECT dolt_commit('-A', '-m', 'first');
CREATE TABLE tmp_bar(x INT PRIMARY KEY);
INSERT INTO keep VALUES (1);
"

# ---------------------------------------------------------------
# Scope: only gates NEW tables. Tracked tables keep showing edits.
# ---------------------------------------------------------------

echo "--- scope: only gates new tables ---"

oracle "already_tracked_still_shows" "
CREATE TABLE tmp_foo(x INT PRIMARY KEY);
INSERT INTO tmp_foo VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add pattern');
INSERT INTO tmp_foo VALUES (2);
"

# Tracked-modified table matching an ignore pattern: status shows it,
# but dolt_add -A does NOT stage it. This is Dolt's consistent rule:
# ignore gates staging (both new and tracked) but only hides NEW
# tables from status.
oracle "tracked_ignored_not_staged_by_A" "
CREATE TABLE tmp_foo(x INT PRIMARY KEY);
INSERT INTO tmp_foo VALUES (1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add pattern');
INSERT INTO tmp_foo VALUES (2);
CREATE TABLE keep(x INT PRIMARY KEY);
SELECT dolt_add('-A');
"

# ---------------------------------------------------------------
# Dynamic: pattern removal re-exposes a hidden table.
# ---------------------------------------------------------------

echo "--- dynamic pattern changes ---"

oracle "remove_pattern_exposes" "
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
CREATE TABLE tmp_foo(x INT PRIMARY KEY);
DELETE FROM dolt_ignore WHERE pattern='tmp_*';
"

# ---------------------------------------------------------------
# Persistence across commits.
# ---------------------------------------------------------------

echo "--- persistence ---"

oracle "persists_across_commit" "
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
CREATE TABLE tmp_foo(x INT PRIMARY KEY);
CREATE TABLE keep(x INT PRIMARY KEY);
"

# ---------------------------------------------------------------
# Star-everything hides dolt_ignore too (no special case).
# ---------------------------------------------------------------

echo "--- no special-case for dolt_ignore ---"

oracle "star_hides_dolt_ignore" "
INSERT INTO dolt_ignore VALUES ('*', 1);
CREATE TABLE foo(x INT PRIMARY KEY);
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
