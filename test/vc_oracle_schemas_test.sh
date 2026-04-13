#!/bin/bash
#
# Version-control oracle test: dolt_schemas vtable + dolt_diff view
# visibility.
#
# doltlite's dolt_schemas is a projection over sqlite_schema filtered
# to type IN ('view','trigger'). Since sqlite_schema is already in the
# branch-scoped catalog, views are version-controlled per branch for
# free: committed views appear in the commit they were created,
# uncommitted views show up as a WORKING dolt_schemas entry in
# dolt_diff, and branch switches surface the right view set.
#
# This oracle compares against Dolt 1.86.0+ on:
#
#   1. SELECT type, name, fragment FROM dolt_schemas ORDER BY type, name
#      — the live view set on the current branch. `extra` and
#      `sql_mode` are NOT compared (doltlite emits NULL for both;
#      Dolt uses them for MySQL-specific metadata).
#
#   2. SELECT table_name FROM dolt_diff — which commits touch
#      dolt_schemas. Compared on (message, table_name, data_change,
#      schema_change) via a LEFT JOIN with dolt_log so engine-
#      specific commit hashes don't matter. Both engines now emit
#      schema_change=1 on the FIRST view/trigger creation (the
#      implicit creation of dolt_schemas) and schema_change=0 on
#      subsequent view/trigger commits.
#
# Triggers: covered via dual-SQL helpers (oracle_schemas_dual /
# oracle_diff_touches_schemas_dual) that accept separate setup
# blocks per engine, since CREATE TRIGGER bodies diverge between
# Dolt's MySQL-ish dialect (single-statement FOR EACH ROW) and
# doltlite's SQLite dialect (BEGIN ... END;). The dual comparisons
# project only (type, name) — not `fragment` — because each engine
# stores its own dialect verbatim and the fragment text legitimately
# differs. Schema-row identity is what matters for VC semantics.
#
# Usage: bash vc_oracle_schemas_test.sh [path/to/doltlite] [path/to/dolt]
#

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

normalize() {
  tr -d '\r' | sort
}

# Oracle for dolt_schemas table contents after a setup script.
# Runs identical SQL on both engines, projects type|name|fragment
# rows, sorts, compares.
oracle_schemas() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/${name}_schemas"
  mkdir -p "$dir/dl" "$dir/dt"

  local q="SELECT 'S' || char(9) || type || char(9) || name || char(9) || fragment FROM dolt_schemas ORDER BY type, name"
  local q_dolt="SELECT concat('S', char(9), type, char(9), name, char(9), fragment) FROM dolt_schemas ORDER BY type, name"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$setup" "$q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep '^S' \
           | normalize)

  local dolt_setup
  dolt_setup=$(echo "$setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      printf '%s\n' "$dolt_setup"
      printf '%s;\n' "$q_dolt"
    } | "$DOLT" sql -r csv 2>"$dir/dt.err"
  )
  dt_out=$(echo "$dt_out" | tr -d '"' | grep '^S' | normalize)

  if [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite:"; echo "$dl_out" | sed 's/^/      /'
    echo "    dolt:";     echo "$dt_out" | sed 's/^/      /'
  fi
}

# Oracle for which commits touch dolt_schemas in dolt_diff, joined
# against dolt_log on commit_hash so we compare by commit MESSAGE
# (stable across engines). IMPORTANT: doltlite's dolt_diff is
# summary-only, so we query it directly and grep dolt_schemas rows out
# in shell instead of relying on row-history surfaces.
oracle_diff_touches_schemas() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/${name}_diff"
  mkdir -p "$dir/dl" "$dir/dt"

  local q="SELECT 'D' || char(9) || dd.table_name || char(9) || coalesce(dl.message, dd.commit_hash) || char(9) || dd.data_change || char(9) || dd.schema_change FROM dolt_diff dd LEFT JOIN dolt_log dl ON dl.commit_hash = dd.commit_hash"
  local q_dolt="SELECT concat('D', char(9), dd.table_name, char(9), coalesce(dl.message, dd.commit_hash), char(9), dd.data_change, char(9), dd.schema_change) FROM dolt_diff dd LEFT JOIN dolt_log dl ON dl.commit_hash = dd.commit_hash"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$setup" "$q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep '^D.*dolt_schemas' \
           | sed -e 's/	true$/	1/' -e 's/	false$/	0/' \
           | normalize)

  local dolt_setup
  dolt_setup=$(echo "$setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      printf '%s\n' "$dolt_setup"
      printf '%s;\n' "$q_dolt"
    } | "$DOLT" sql -r csv 2>"$dir/dt.err"
  )
  dt_out=$(echo "$dt_out" | tr -d '"' | grep '^D.*dolt_schemas' \
           | sed -e 's/	true$/	1/' -e 's/	false$/	0/' \
           | normalize)

  if [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite:"; echo "$dl_out" | sed 's/^/      /'
    echo "    dolt:";     echo "$dt_out" | sed 's/^/      /'
  fi
}

# Dual-setup variant of oracle_schemas: each engine runs its own
# setup SQL so trigger DDL can be written in the engine's native
# dialect. The projection is (type, name) only — fragment is
# intentionally omitted because each engine stores the CREATE
# TRIGGER body verbatim in its own dialect and those byte-strings
# legitimately differ. Schema-row identity is what matters for VC
# semantics: does the trigger appear in dolt_schemas, with the
# right name, on the right commit / branch.
oracle_schemas_dual() {
  local name="$1" dl_setup="$2" dt_setup="$3"
  local dir="$TMPROOT/${name}_schemas_dual"
  mkdir -p "$dir/dl" "$dir/dt"

  local q="SELECT 'S' || char(9) || type || char(9) || name FROM dolt_schemas ORDER BY type, name"
  local q_dolt="SELECT concat('S', char(9), type, char(9), name) FROM dolt_schemas ORDER BY type, name"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$dl_setup" "$q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep '^S' \
           | normalize)

  local dolt_setup
  dolt_setup=$(echo "$dt_setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      printf '%s\n' "$dolt_setup"
      printf '%s;\n' "$q_dolt"
    } | "$DOLT" sql -r csv 2>"$dir/dt.err"
  )
  dt_out=$(echo "$dt_out" | tr -d '"' | grep '^S' | normalize)

  if [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite:"; echo "$dl_out" | sed 's/^/      /'
    echo "    dolt:";     echo "$dt_out" | sed 's/^/      /'
  fi
}

# Dual-setup variant of oracle_diff_touches_schemas: same per-engine
# setup model. Compared surface is identical — (table_name, message,
# data_change, schema_change) — since that projection doesn't depend
# on trigger body text.
oracle_diff_touches_schemas_dual() {
  local name="$1" dl_setup="$2" dt_setup="$3"
  local dir="$TMPROOT/${name}_diff_dual"
  mkdir -p "$dir/dl" "$dir/dt"

  local q="SELECT 'D' || char(9) || dd.table_name || char(9) || coalesce(dl.message, dd.commit_hash) || char(9) || dd.data_change || char(9) || dd.schema_change FROM dolt_diff dd LEFT JOIN dolt_log dl ON dl.commit_hash = dd.commit_hash"
  local q_dolt="SELECT concat('D', char(9), dd.table_name, char(9), coalesce(dl.message, dd.commit_hash), char(9), dd.data_change, char(9), dd.schema_change) FROM dolt_diff dd LEFT JOIN dolt_log dl ON dl.commit_hash = dd.commit_hash"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$dl_setup" "$q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep '^D.*dolt_schemas' \
           | sed -e 's/	true$/	1/' -e 's/	false$/	0/' \
           | normalize)

  local dolt_setup
  dolt_setup=$(echo "$dt_setup" | sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g')

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      printf '%s\n' "$dolt_setup"
      printf '%s;\n' "$q_dolt"
    } | "$DOLT" sql -r csv 2>"$dir/dt.err"
  )
  dt_out=$(echo "$dt_out" | tr -d '"' | grep '^D.*dolt_schemas' \
           | sed -e 's/	true$/	1/' -e 's/	false$/	0/' \
           | normalize)

  if [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite:"; echo "$dl_out" | sed 's/^/      /'
    echo "    dolt:";     echo "$dt_out" | sed 's/^/      /'
  fi
}

SEED="
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1, 10);
INSERT INTO t VALUES(2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
"

echo "=== Version Control Oracle Tests: dolt_schemas ==="
echo ""

echo "--- dolt_schemas table contents ---"

oracle_schemas "no_views" "$SEED"

oracle_schemas "one_view" "
$SEED
CREATE VIEW high AS SELECT * FROM t WHERE v > 15;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_view');
"

oracle_schemas "two_views" "
$SEED
CREATE VIEW high AS SELECT * FROM t WHERE v > 15;
CREATE VIEW low AS SELECT * FROM t WHERE v < 100;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_views');
"

oracle_schemas "view_then_drop" "
$SEED
CREATE VIEW high AS SELECT * FROM t WHERE v > 15;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_view');
DROP VIEW high;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop_view');
"

oracle_schemas "uncommitted_view_visible" "
$SEED
CREATE VIEW pending AS SELECT * FROM t;
"

echo "--- dolt_diff visibility of view-touching commits ---"

# Adding a view in its own commit: dolt_diff should show dolt_schemas
# touched and NOT show the underlying table 't' touched.
oracle_diff_touches_schemas "view_only_commit" "
$SEED
CREATE VIEW high AS SELECT * FROM t WHERE v > 15;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'just_view');
"

# CREATE TABLE does NOT touch dolt_schemas in either engine.
oracle_diff_touches_schemas "table_only_no_schemas_touch" "
CREATE TABLE t(id INT PRIMARY KEY);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'just_table');
"

# Multiple commits, some touching views, some not.
oracle_diff_touches_schemas "mixed_commits" "
$SEED
CREATE VIEW v1 AS SELECT * FROM t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'view_commit_1');
INSERT INTO t VALUES(3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'data_commit');
DROP VIEW v1;
CREATE VIEW v2 AS SELECT * FROM t;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'view_commit_2');
"

# Combined commit: both a table change and a view change in one
# commit. dolt_diff should surface both as separate rows.
oracle_diff_touches_schemas "combined_table_and_view" "
$SEED
INSERT INTO t VALUES(3, 30);
CREATE VIEW high AS SELECT * FROM t WHERE v > 15;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'combined');
"

# ─── Triggers ────────────────────────────────────────────────────────
#
# Both engines expose triggers in dolt_schemas as (type='trigger',
# name=<trigger name>). The CREATE TRIGGER body text legitimately
# differs by dialect, so we compare on (type, name) only via
# oracle_schemas_dual. Dolt's `dolt sql` stdin parser has trouble
# with multi-statement trigger bodies wrapped in BEGIN/END, so
# every trigger body in these scenarios is a SINGLE statement —
# either "FOR EACH ROW <stmt>" on the Dolt side or
# "BEGIN <stmt>; END" on the doltlite side.
#
# SEED_TRIG extends $SEED with a log table so trigger bodies have
# somewhere to write. Same on both engines.
SEED_TRIG="
$SEED
CREATE TABLE log(id INT, v INT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_log');
"

echo "--- dolt_schemas table contents: triggers ---"

oracle_schemas_dual "one_trigger" "
$SEED_TRIG
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES(new.id, new.v); END;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_trigger');
" "
$SEED_TRIG
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(new.id, new.v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_trigger');
"

oracle_schemas_dual "two_triggers" "
$SEED_TRIG
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES(new.id, new.v); END;
CREATE TRIGGER t_ad AFTER DELETE ON t BEGIN INSERT INTO log VALUES(old.id, old.v); END;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_triggers');
" "
$SEED_TRIG
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(new.id, new.v);
CREATE TRIGGER t_ad AFTER DELETE ON t FOR EACH ROW INSERT INTO log VALUES(old.id, old.v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_triggers');
"

oracle_schemas_dual "trigger_then_drop" "
$SEED_TRIG
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES(new.id, new.v); END;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_trigger');
DROP TRIGGER t_ai;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop_trigger');
" "
$SEED_TRIG
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(new.id, new.v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_trigger');
DROP TRIGGER t_ai;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'drop_trigger');
"

oracle_schemas_dual "uncommitted_trigger_visible" "
$SEED_TRIG
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES(new.id, new.v); END;
" "
$SEED_TRIG
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(new.id, new.v);
"

# Mixed: a trigger and a view together in dolt_schemas.
oracle_schemas_dual "trigger_and_view" "
$SEED_TRIG
CREATE VIEW high AS SELECT * FROM t WHERE v > 15;
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES(new.id, new.v); END;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_view_and_trigger');
" "
$SEED_TRIG
CREATE VIEW high AS SELECT * FROM t WHERE v > 15;
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(new.id, new.v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_view_and_trigger');
"

# Branch isolation: trigger created on a feature branch must not
# appear in dolt_schemas on main after checkout. This is the core
# version-control property of sqlite_schema being branch-scoped.
oracle_schemas_dual "trigger_on_feature_branch_only" "
$SEED_TRIG
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES(new.id, new.v); END;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_trigger_on_feat');
SELECT dolt_checkout('main');
" "
$SEED_TRIG
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(new.id, new.v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_trigger_on_feat');
SELECT dolt_checkout('main');
"

# Modify trigger (DROP + CREATE same name) inside a single commit.
# The dolt_schemas projection is (type, name), so the end state is
# identical to one_trigger — but the path through it exercises an
# intra-commit delete-then-add on a schema row, which is a distinct
# code path in the catalog diff machinery.
oracle_schemas_dual "modify_trigger_single_commit" "
$SEED_TRIG
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES(1, new.v); END;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'initial_trigger');
DROP TRIGGER t_ai;
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES(2, new.v); END;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'replaced_trigger');
" "
$SEED_TRIG
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(1, new.v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'initial_trigger');
DROP TRIGGER t_ai;
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(2, new.v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'replaced_trigger');
"

echo "--- dolt_diff visibility of trigger-touching commits ---"

# Adding a trigger in its own commit: dolt_diff should show
# dolt_schemas touched and NOT show the underlying table 't'.
oracle_diff_touches_schemas_dual "trigger_only_commit" "
$SEED_TRIG
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES(new.id, new.v); END;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'just_trigger');
" "
$SEED_TRIG
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(new.id, new.v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'just_trigger');
"

# Multiple commits, some touching triggers, some not.
oracle_diff_touches_schemas_dual "mixed_trigger_commits" "
$SEED_TRIG
CREATE TRIGGER t1 AFTER INSERT ON t BEGIN INSERT INTO log VALUES(new.id, new.v); END;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'trigger_commit_1');
INSERT INTO t VALUES(3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'data_commit');
DROP TRIGGER t1;
CREATE TRIGGER t2 AFTER DELETE ON t BEGIN INSERT INTO log VALUES(old.id, old.v); END;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'trigger_commit_2');
" "
$SEED_TRIG
CREATE TRIGGER t1 AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(new.id, new.v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'trigger_commit_1');
INSERT INTO t VALUES(3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'data_commit');
DROP TRIGGER t1;
CREATE TRIGGER t2 AFTER DELETE ON t FOR EACH ROW INSERT INTO log VALUES(old.id, old.v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'trigger_commit_2');
"

# Combined commit: both a table data change and a trigger schema
# change in one commit. Mirrors the existing combined_table_and_view
# scenario from the view section. dolt_diff should surface the
# dolt_schemas row on the same commit as the table mutation.
oracle_diff_touches_schemas_dual "combined_table_and_trigger" "
$SEED_TRIG
INSERT INTO t VALUES(3, 30);
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES(new.id, new.v); END;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'combined');
" "
$SEED_TRIG
INSERT INTO t VALUES(3, 30);
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(new.id, new.v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'combined');
"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
