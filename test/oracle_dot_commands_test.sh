#!/bin/bash
#
# Oracle test: SQLite shell dot-commands
#
# Runs identical shell sessions against doltlite and stock sqlite3 built
# from the same tree, compares normalized output. Exercises dot-commands
# that should behave exactly like upstream SQLite — anything where
# doltlite is a superset should still emit the stock behavior for the
# shared surface.
#
# Covers: .tables, .schema, .indexes, .databases, .dump, .fullschema.
#
# Deliberately NOT covered (not comparable or not meaningful):
#   - .mode, .headers, .separator — just change display, not stateful
#     output. Exercised indirectly via queries in other oracles.
#   - .show, .help, .timer, .stats — display engine-specific state.
#   - .import — has its own oracle (oracle_import_test.sh).
#   - .output, .read, .quit, .open — I/O redirection / shell control.
#   - .backup, .restore, .recover — file-level ops not comparable.
#
# Known divergences from upstream SQLite (NOT oracle-tested here):
#
#   - .dbinfo: depends on the sqlite_dbpage shadow vtable, which
#     doltlite does not register (its storage backend is prolly
#     trees, not SQLite pages). doltlite errors with "no such
#     table: sqlite_dbpage".
#
#   - .schema sqlite_%: stock SQLite shows comment-form entries for
#     sqlite_dbpage and sqlite_stmt alongside the real
#     sqlite_schema/sqlite_sequence rows. doltlite lacks both
#     system vtables and so omits those comment lines.
#
#   Both are downstream of the same gap — doltlite's prolly storage
#   doesn't expose page- or statement-level introspection vtables.
#
# Usage: bash oracle_dot_commands_test.sh [path/to/doltlite] [path/to/sqlite3]
#

set -u

DOLTLITE="${1:-./doltlite}"
SQLITE3="${2:-./sqlite3}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

# Normalize filesystem paths to a placeholder so .databases output
# (which prints the absolute DB path) compares equal even when the two
# runs use different temp dirs. On macOS `/tmp` resolves to `/private/tmp`
# via a symlink, which is what sqlite3's realpath-based .databases output
# prints — strip the /private prefix first so the two sides line up.
normalize() {
  tr -d '\r' \
    | sed -e "s|/private$TMPROOT|TMP|g" -e "s|$TMPROOT|TMP|g" \
    | sed -e 's|/dl/db|/db|g' -e 's|/sq/db|/db|g' \
    | sed -e 's/[[:space:]]\{1,\}/ /g' -e 's/^ //' -e 's/ $//'
}

# $1=name, $2=setup SQL, $3=dot command(s) to run
# Both sides run setup + the command in one invocation so state matches.
# String literals in setup MUST use single quotes to avoid the DQS=0
# build option both engines share ("alice" would be parsed as a column).
oracle() {
  local name="$1" setup="$2" cmd="$3"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir"

  # Use the same filename per side so .databases output (which prints
  # the full DB path) compares equal after TMPROOT normalization.
  mkdir -p "$dir/dl" "$dir/sq"
  local dl_out
  dl_out=$(printf '%s\n%s\n' "$setup" "$cmd" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | normalize)

  local sq_out
  sq_out=$(printf '%s\n%s\n' "$setup" "$cmd" \
           | "$SQLITE3" "$dir/sq/db" 2>"$dir/sq.err" \
           | normalize)

  if [ "$dl_out" = "$sq_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    cmd: $cmd"
    echo "    doltlite:"; echo "$dl_out" | sed 's/^/      /'
    echo "    sqlite3:";  echo "$sq_out" | sed 's/^/      /'
  fi
}

# Common schemas for the test scenarios.
SEED_EMPTY=""

SEED_ONE_TABLE="CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10);
INSERT INTO t VALUES(2,20);"

SEED_MANY_TABLES="CREATE TABLE users(id INT PRIMARY KEY, name TEXT);
CREATE TABLE user_prefs(user_id INT, key TEXT, val TEXT);
CREATE TABLE posts(id INT PRIMARY KEY, user_id INT, title TEXT);
INSERT INTO users VALUES(1,'alice');
INSERT INTO users VALUES(2,'bob');
INSERT INTO posts VALUES(10,1,'hello');"

SEED_WITH_INDEX="CREATE TABLE t(id INT PRIMARY KEY, v INT, tag TEXT);
CREATE INDEX idx_t_v ON t(v);
CREATE INDEX idx_t_tag ON t(tag);
INSERT INTO t VALUES(1,10,'a');
INSERT INTO t VALUES(2,20,'b');"

SEED_WITH_VIEW="CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10);
INSERT INTO t VALUES(2,20);
CREATE VIEW high AS SELECT * FROM t WHERE v > 15;"

SEED_FK="CREATE TABLE a(id INT PRIMARY KEY);
CREATE TABLE b(id INT PRIMARY KEY, a_id INT, FOREIGN KEY(a_id) REFERENCES a(id));
INSERT INTO a VALUES(1);
INSERT INTO b VALUES(10,1);"

SEED_TRIGGER="CREATE TABLE t(id INT PRIMARY KEY, v INT);
CREATE TABLE log_t(old_v INT, new_v INT);
CREATE TRIGGER log_t_trig AFTER UPDATE ON t BEGIN INSERT INTO log_t VALUES(old.v, new.v); END;
INSERT INTO t VALUES(1,10);"

SEED_AUTOINC="CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);
INSERT INTO t(v) VALUES('a');
INSERT INTO t(v) VALUES('b');"

SEED_CHECK="CREATE TABLE t(id INT PRIMARY KEY, age INT CHECK (age > 0));
INSERT INTO t VALUES(1, 5);"

SEED_COMPOSITE_PK="CREATE TABLE t(a INT, b INT, v TEXT, PRIMARY KEY(a,b));
INSERT INTO t VALUES(1,2,'x');
INSERT INTO t VALUES(1,3,'y');"

SEED_DEFAULTS="CREATE TABLE t(id INT PRIMARY KEY, name TEXT DEFAULT 'unknown', flag INT DEFAULT 0);
INSERT INTO t(id) VALUES(1);"

SEED_GENERATED="CREATE TABLE t(a INT, b INT, c INT AS (a+b) STORED);
INSERT INTO t(a,b) VALUES(1,2);
INSERT INTO t(a,b) VALUES(3,4);"

SEED_BLOB_NULL="CREATE TABLE t(id INT PRIMARY KEY, n TEXT, b BLOB);
INSERT INTO t VALUES(1,NULL,NULL);
INSERT INTO t VALUES(2,'with ''apostrophe''',NULL);
INSERT INTO t VALUES(3,'ok',x'deadbeef');"

SEED_QUOTED_NAME='CREATE TABLE "my-table"(id INT, "col name" TEXT);
INSERT INTO "my-table" VALUES(1,''hi'');'

SEED_MIXED_CASE="CREATE TABLE Users(id INT);
CREATE TABLE USERS_LOG(id INT);
CREATE TABLE posts(id INT);"

echo "=== Oracle Tests: SQLite shell dot-commands ==="
echo ""

echo "--- .tables ---"

oracle "tables_empty" "$SEED_EMPTY" ".tables"
oracle "tables_one"   "$SEED_ONE_TABLE" ".tables"
oracle "tables_many"  "$SEED_MANY_TABLES" ".tables"
oracle "tables_pattern_prefix" "$SEED_MANY_TABLES" ".tables user%"
oracle "tables_pattern_suffix" "$SEED_MANY_TABLES" ".tables %s"
oracle "tables_pattern_contains" "$SEED_MANY_TABLES" ".tables %post%"
oracle "tables_pattern_nomatch"  "$SEED_MANY_TABLES" ".tables nomatch%"
oracle "tables_with_view"        "$SEED_WITH_VIEW" ".tables"

echo "--- .schema ---"

oracle "schema_empty"           "$SEED_EMPTY" ".schema"
oracle "schema_one_table"       "$SEED_ONE_TABLE" ".schema"
oracle "schema_many_tables"     "$SEED_MANY_TABLES" ".schema"
oracle "schema_single_table"    "$SEED_MANY_TABLES" ".schema users"
oracle "schema_pattern"         "$SEED_MANY_TABLES" ".schema us%"
oracle "schema_nomatch"         "$SEED_MANY_TABLES" ".schema nomatch"
oracle "schema_with_index"      "$SEED_WITH_INDEX" ".schema"
oracle "schema_with_view"       "$SEED_WITH_VIEW" ".schema"
oracle "schema_view_only"       "$SEED_WITH_VIEW" ".schema high"
oracle "schema_with_fk"         "$SEED_FK" ".schema"
oracle "schema_indent_flag"     "$SEED_ONE_TABLE" ".schema --indent"

echo "--- .indexes ---"

oracle "indexes_empty"            "$SEED_EMPTY" ".indexes"
oracle "indexes_none"             "$SEED_ONE_TABLE" ".indexes"
oracle "indexes_with_index"       "$SEED_WITH_INDEX" ".indexes"
oracle "indexes_specific_table"   "$SEED_WITH_INDEX" ".indexes t"
oracle "indexes_nomatch_table"    "$SEED_WITH_INDEX" ".indexes nomatch"

echo "--- .databases ---"

oracle "databases_single" "$SEED_ONE_TABLE" ".databases"

echo "--- .dump ---"

oracle "dump_empty"         "$SEED_EMPTY" ".dump"
oracle "dump_one_table"     "$SEED_ONE_TABLE" ".dump"
oracle "dump_many_tables"   "$SEED_MANY_TABLES" ".dump"
oracle "dump_with_index"    "$SEED_WITH_INDEX" ".dump"
oracle "dump_with_view"     "$SEED_WITH_VIEW" ".dump"
oracle "dump_with_fk"       "$SEED_FK" ".dump"
oracle "dump_single_table"  "$SEED_MANY_TABLES" ".dump users"
oracle "dump_pattern"       "$SEED_MANY_TABLES" ".dump user%"

echo "--- .fullschema ---"

oracle "fullschema_empty"       "$SEED_EMPTY" ".fullschema"
oracle "fullschema_one_table"   "$SEED_ONE_TABLE" ".fullschema"
oracle "fullschema_with_index"  "$SEED_WITH_INDEX" ".fullschema"
oracle "fullschema_with_view"   "$SEED_WITH_VIEW" ".fullschema"

echo "--- DDL feature coverage ---"

oracle "schema_trigger"        "$SEED_TRIGGER" ".schema"
oracle "dump_trigger"          "$SEED_TRIGGER" ".dump"
oracle "schema_autoinc"        "$SEED_AUTOINC" ".schema"
oracle "dump_autoinc"          "$SEED_AUTOINC" ".dump"
oracle "tables_autoinc"        "$SEED_AUTOINC" ".tables"
oracle "schema_check"          "$SEED_CHECK" ".schema"
oracle "dump_check"            "$SEED_CHECK" ".dump"
oracle "schema_composite_pk"   "$SEED_COMPOSITE_PK" ".schema"
oracle "dump_composite_pk"     "$SEED_COMPOSITE_PK" ".dump"
oracle "schema_defaults"       "$SEED_DEFAULTS" ".schema"
oracle "dump_defaults"         "$SEED_DEFAULTS" ".dump"
oracle "schema_generated_col"  "$SEED_GENERATED" ".schema"
oracle "dump_generated_col"    "$SEED_GENERATED" ".dump"
oracle "dump_null_blob"        "$SEED_BLOB_NULL" ".dump"
oracle "schema_quoted_name"    "$SEED_QUOTED_NAME" ".schema"
oracle "dump_quoted_name"      "$SEED_QUOTED_NAME" ".dump"
oracle "tables_mixed_case"     "$SEED_MIXED_CASE" ".tables"
oracle "schema_mixed_case"     "$SEED_MIXED_CASE" ".schema"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
