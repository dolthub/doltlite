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

# doltlite needs the dolt_ignore user table explicitly created by
# the user; Dolt pre-materializes it as a reserved system name and
# rejects CREATE TABLE dolt_ignore with "reserved for internal use".
# So the oracle prepends the CREATE only on the doltlite side.
DL_IGNORE_PREFIX="CREATE TABLE IF NOT EXISTS dolt_ignore(pattern TEXT NOT NULL, ignored TINYINT NOT NULL, PRIMARY KEY(pattern));"

# oracle: run the same setup on both engines and compare their
# resulting dolt_status (table_name + staged + status).
oracle() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_setup="$DL_IGNORE_PREFIX
$setup"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\nSELECT 'S|' || table_name || '|' || staged || '|' || status FROM dolt_status;\n" "$dl_setup" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep '^S|' \
           | normalize)

  local dolt_setup
  dolt_setup=$(echo "$setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')

  # Run setup AND the status query in a single dolt sql invocation so
  # that branch checkouts and working-set state persist from setup
  # into the query (a second dolt sql call would reset to main and
  # lose the per-branch working set).
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    printf "%s\nSELECT concat('S|', table_name, '|', staged, '|', status) FROM dolt_status;\n" "$dolt_setup" \
      | "$DOLT" sql -r csv 2>"$dir/dt.err"
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

# doltlite_schema_reject: doltlite-only check that a CREATE TABLE
# dolt_ignore with a wrong schema is rejected at parse time. Not an
# oracle comparison — Dolt rejects any `dolt_` name wholesale with
# "reserved for internal use", so there's no row-level semantics to
# compare against. We just assert doltlite produces an error that
# mentions dolt_ignore.
doltlite_schema_reject() {
  local name="$1" sql="$2"
  local dir="$TMPROOT/${name}_rej"
  mkdir -p "$dir/dl"
  echo "$sql" | "$DOLTLITE" "$dir/dl/db" > "$dir/out" 2>&1
  if grep -qi 'dolt_ignore' "$dir/out" \
     && grep -qiE 'error|fail' "$dir/out"; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected doltlite to reject)"
    echo "    output:"; sed 's/^/      /' "$dir/out"
  fi
}

# oracle_error: both engines must error on the same setup. Exact
# message text is allowed to differ.
oracle_error() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/${name}_err"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_setup="$DL_IGNORE_PREFIX
$setup"

  local dl_err=0
  echo "$dl_setup" | "$DOLTLITE" "$dir/dl/db" >"$dir/dl.out" 2>"$dir/dl.err"
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

# ---------------------------------------------------------------
# Schema guard: doltlite rejects CREATE TABLE dolt_ignore with the
# wrong shape so the failure mode is a clear parse error instead of
# silent "no patterns apply". Doltlite-only — Dolt rejects any
# dolt_ prefix wholesale so there's nothing to compare against.
# ---------------------------------------------------------------

echo "--- schema guard ---"

doltlite_schema_reject "schema_one_column" "
CREATE TABLE dolt_ignore(pattern TEXT NOT NULL PRIMARY KEY);
"

doltlite_schema_reject "schema_three_columns" "
CREATE TABLE dolt_ignore(pattern TEXT NOT NULL, ignored TINYINT NOT NULL, extra INT, PRIMARY KEY(pattern));
"

doltlite_schema_reject "schema_wrong_first_name" "
CREATE TABLE dolt_ignore(foo TEXT NOT NULL, ignored TINYINT NOT NULL, PRIMARY KEY(foo));
"

doltlite_schema_reject "schema_wrong_second_name" "
CREATE TABLE dolt_ignore(pattern TEXT NOT NULL, flagged TINYINT NOT NULL, PRIMARY KEY(pattern));
"

doltlite_schema_reject "schema_wrong_pattern_type" "
CREATE TABLE dolt_ignore(pattern INTEGER NOT NULL, ignored TINYINT NOT NULL, PRIMARY KEY(pattern));
"

doltlite_schema_reject "schema_wrong_ignored_type" "
CREATE TABLE dolt_ignore(pattern TEXT NOT NULL, ignored TEXT NOT NULL, PRIMARY KEY(pattern));
"

doltlite_schema_reject "schema_pattern_nullable" "
CREATE TABLE dolt_ignore(pattern TEXT, ignored TINYINT NOT NULL, PRIMARY KEY(pattern));
"

doltlite_schema_reject "schema_ignored_nullable" "
CREATE TABLE dolt_ignore(pattern TEXT NOT NULL, ignored TINYINT, PRIMARY KEY(pattern));
"

doltlite_schema_reject "schema_compound_pk" "
CREATE TABLE dolt_ignore(pattern TEXT NOT NULL, ignored TINYINT NOT NULL, PRIMARY KEY(pattern, ignored));
"

doltlite_schema_reject "schema_no_pk" "
CREATE TABLE dolt_ignore(pattern TEXT NOT NULL, ignored TINYINT NOT NULL);
"

# Accepted variants: BOOLEAN, VARCHAR, INTEGER for ignored all map
# to the right affinities. Not an oracle comparison — just assert
# doltlite accepts them and INSERT works.
doltlite_schema_accept() {
  local name="$1" sql="$2"
  local dir="$TMPROOT/${name}_acc"
  mkdir -p "$dir/dl"
  echo "$sql" | "$DOLTLITE" "$dir/dl/db" > "$dir/out" 2>&1
  if ! grep -qiE 'error|fail' "$dir/out"; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected doltlite to accept)"
    echo "    output:"; sed 's/^/      /' "$dir/out"
  fi
}

doltlite_schema_accept "schema_varchar_boolean" "
CREATE TABLE dolt_ignore(pattern VARCHAR(255) NOT NULL, ignored BOOLEAN NOT NULL, PRIMARY KEY(pattern));
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
SELECT * FROM dolt_ignore;
"

doltlite_schema_accept "schema_integer_ignored" "
CREATE TABLE dolt_ignore(pattern TEXT NOT NULL, ignored INTEGER NOT NULL, PRIMARY KEY(pattern));
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
SELECT * FROM dolt_ignore;
"

# Runtime guard: temp-schema shadowing must not override repo ignore
# rules, and wrong-shape main objects must error rather than silently
# disabling filtering. Doltlite-only because Dolt does not expose temp
# objects through the same SQL surface.
doltlite_runtime_expect() {
  local name="$1" sql="$2" expect="$3"
  local dir="$TMPROOT/${name}_rt"
  mkdir -p "$dir/dl"
  printf "%s\n.headers off\n.mode list\n.separator '|'\nSELECT table_name || '|' || staged || '|' || status FROM dolt_status;\n" "$sql" \
    | "$DOLTLITE" "$dir/dl/db" > "$dir/out" 2>&1
  local got
  got=$(grep '^[^|].*|' "$dir/out" | grep -v '^dolt_ignore|' | sort)
  if [ "$got" = "$expect" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (unexpected doltlite status)"
    echo "    expected:"; echo "$expect" | sed 's/^/      /'
    echo "    got:"; echo "$got" | sed 's/^/      /'
    echo "    output:"; sed 's/^/      /' "$dir/out"
  fi
}

doltlite_runtime_reject() {
  local name="$1" sql="$2"
  local dir="$TMPROOT/${name}_rtrej"
  mkdir -p "$dir/dl"
  echo "$sql" | "$DOLTLITE" "$dir/dl/db" > "$dir/out" 2>&1
  if grep -qi 'unexpected schema' "$dir/out" \
     && grep -qiE 'error|fail' "$dir/out"; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected doltlite runtime reject)"
    echo "    output:"; sed 's/^/      /' "$dir/out"
  fi
}

doltlite_runtime_expect "temp_shadow_ignored_main_wins" "
CREATE TABLE dolt_ignore(pattern TEXT NOT NULL, ignored TINYINT NOT NULL, PRIMARY KEY(pattern));
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
CREATE TEMP TABLE dolt_ignore(pattern TEXT NOT NULL, ignored TINYINT NOT NULL, PRIMARY KEY(pattern));
INSERT INTO temp.dolt_ignore VALUES ('tmp_*', 0);
CREATE TABLE tmp_shadowed(x INT PRIMARY KEY);
" ""

doltlite_runtime_reject "runtime_wrong_shape_view" "
CREATE VIEW dolt_ignore AS SELECT 'tmp_*' AS pattern;
CREATE TABLE tmp_bad(x INT PRIMARY KEY);
SELECT * FROM dolt_status;
"

# ---------------------------------------------------------------
# cross-branch + merge + reset
# ---------------------------------------------------------------
#
# These scenarios pin dolt_ignore's behavior across branch, merge,
# and reset operations. All interactions are checked against Dolt
# 1.83.5; the oracle helpers already handle doltlite's manual
# CREATE TABLE dolt_ignore prefix.
#
# Note scenario 3 (committed_then_reset): after dolt_reset --hard
# HEAD~1, both engines leave the previously-untracked matching
# table in the working tree (reset --hard does not clean untracked
# files). Once the ignore pattern is gone, both that leftover table
# and any freshly-created matching table show up in dolt_status.
# Note scenario 5 (merge_conflicting_patterns): an UPDATE-UPDATE
# conflict on the same dolt_ignore row surfaces as a merge conflict
# in Dolt and as "Merge has N conflict(s)" in doltlite; oracle_error
# only requires both to report an error.

echo "--- cross-branch + merge + reset ---"

oracle "pattern_survives_branch_create" "
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
SELECT dolt_commit('-A', '-m', 'add pattern');
SELECT dolt_checkout('-b', 'feat');
CREATE TABLE tmp_foo(x INT PRIMARY KEY);
CREATE TABLE keep(x INT PRIMARY KEY);
"

oracle "pattern_different_on_branches" "
SELECT dolt_checkout('-b', 'bA');
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
SELECT dolt_commit('-A', '-m', 'bA ignores tmp_');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b', 'bB');
INSERT INTO dolt_ignore VALUES ('cache_*', 1);
SELECT dolt_commit('-A', '-m', 'bB ignores cache_');
CREATE TABLE tmp_foo(x INT PRIMARY KEY);
CREATE TABLE cache_bar(x INT PRIMARY KEY);
"

oracle "pattern_committed_then_reset" "
CREATE TABLE sentinel(x INT PRIMARY KEY);
SELECT dolt_commit('-A', '-m', 'base');
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
SELECT dolt_commit('-A', '-m', 'add pattern');
CREATE TABLE tmp_foo(x INT PRIMARY KEY);
SELECT dolt_reset('--hard', 'HEAD~1');
CREATE TABLE tmp_bar(x INT PRIMARY KEY);
"

oracle "merge_adds_pattern_ff" "
CREATE TABLE sentinel(x INT PRIMARY KEY);
SELECT dolt_commit('-A', '-m', 'base');
SELECT dolt_checkout('-b', 'feature');
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);
SELECT dolt_commit('-A', '-m', 'feature adds pattern');
SELECT dolt_checkout('main');
SELECT dolt_merge('feature');
CREATE TABLE tmp_new(x INT PRIMARY KEY);
CREATE TABLE keep(x INT PRIMARY KEY);
"

oracle_error "merge_conflicting_patterns" "
CREATE TABLE sentinel(x INT PRIMARY KEY);
SELECT dolt_commit('-A', '-m', 'base');
SELECT dolt_checkout('-b', 'b1');
INSERT INTO dolt_ignore VALUES ('shared_pat', 1);
SELECT dolt_commit('-A', '-m', 'b1 ignores shared_pat');
SELECT dolt_checkout('main');
SELECT dolt_checkout('-b', 'b2');
INSERT INTO dolt_ignore VALUES ('shared_pat', 0);
SELECT dolt_commit('-A', '-m', 'b2 unignores shared_pat');
SELECT dolt_checkout('main');
SELECT dolt_merge('b1');
SELECT dolt_merge('b2');
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
