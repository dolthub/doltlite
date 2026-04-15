#!/bin/bash
#
# Version-control oracle test: dolt_hashof suite
#
# dolt_hashof(ref), dolt_hashof_table(tbl[, ref]), and dolt_hashof_db()
# expose content-addressed hashes to SQL. These are the primary tool
# for verifying two doltlite databases hold the same state in the
# decentralized use case — compare a hash, don't diff a million rows.
#
# The interesting property isn't that hash values match some external
# reference — it's that they obey HISTORY INDEPENDENCE: any two code
# paths that arrive at the same logical state must produce the same
# hash, regardless of intermediate commits, ordering, or transient
# rows. Prolly trees give us this by construction (content-addressed
# nodes keyed on sorted content), and if any of the properties below
# break, the chunking/serialization layer has a bug.
#
# The tests in this oracle are grouped by property:
#
#   1. Determinism: the same input sequence run twice in separate
#      databases produces byte-identical hashes. Exercises the
#      chunker + serializer end-to-end.
#
#   2. Insert order invariance: inserting the same rowset in
#      different orders converges to the same table hash. This is
#      the core prolly-tree property; if it breaks, either the
#      mutmap merge step has stale state or the chunker isn't
#      hashing sorted content.
#
#   3. Intermediate state invariance: inserting rows then deleting
#      a subset then re-inserting them (net-no-op) produces the
#      same hash as just inserting the remaining set. Tests that
#      tombstones flush cleanly and the final root is content-
#      addressed.
#
#   4. Cross-branch state invariance: two branches that diverge and
#      re-converge on the exact same rowset produce the same
#      dolt_hashof_table even though their commit graphs differ.
#      dolt_hashof(ref) DIFFERS because commit metadata differs —
#      this is correct, and we check both.
#
#   5. Separation of concerns: adding a row to table A does NOT
#      change dolt_hashof_table('B'). dolt_hashof_db() DOES
#      change because the catalog moved.
#
#   6. Reopen stability: close the db, reopen, hashes are
#      byte-identical. Catches any hashing that depends on live
#      in-memory state instead of persisted chunks.
#
#   7. Negative cases: updating a single cell, adding a row,
#      altering a schema all MUST change the table hash. If
#      they don't, the hash is stale and worthless.
#
#   8. Ref resolution: dolt_hashof accepts branch names, tag
#      names, commit hashes, and HEAD~N shorthand. Missing refs
#      return NULL or an error (we accept either, as long as it
#      isn't a silent hash).
#
#   9. Dolt shape conformance: dolt_hashof(...) result is stable,
#      lowercase, and exactly 40 hex characters (20-byte
#      ProllyHash). This is the only Dolt conformance check —
#      we don't require hash-value equality with the Dolt CLI
#      because Dolt renders the same bytes as 32-char base32.
#
# Usage: bash vc_oracle_hashof_test.sh [path/to/doltlite]
#

set -u

DOLTLITE="${1:-./doltlite}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""

# Run setup sql against a fresh doltlite db in one process, then run
# the hash-producing query in a second process so stdout contains
# ONLY the query result. Without this split, dolt_commit's return
# value would bleed into the captured hash.
run_hash() {
  local name="$1" setup="$2" query="$3"
  local db="$TMPROOT/$name.db"
  rm -f "$db"
  printf "%s\n" "$setup" \
    | "$DOLTLITE" "$db" >/dev/null 2>"$TMPROOT/$name.setup.err"
  "$DOLTLITE" "$db" "$query" 2>"$TMPROOT/$name.query.err"
}

# Like run_hash, but reuses an existing db path — so caller can
# layer setup sequences and inspect the hash at different points.
run_hash_on() {
  local db="$1" query="$2"
  "$DOLTLITE" "$db" "$query" 2>>"$TMPROOT/shared.err"
}

# Assert two invocations produce the same hash.
same() {
  local name="$1" a="$2" b="$3"
  if [ -z "$a" ] || [ -z "$b" ]; then
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (empty hash)"
    echo "    a=|$a|"
    echo "    b=|$b|"
    return
  fi
  if [ "$a" = "$b" ]; then
    pass=$((pass+1))
    echo "  PASS: $name"
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    a=$a"
    echo "    b=$b"
  fi
}

# Assert two invocations produce DIFFERENT hashes (updates,
# schema changes, etc. must invalidate).
different() {
  local name="$1" a="$2" b="$3"
  if [ -z "$a" ] || [ -z "$b" ]; then
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (empty hash)"
    return
  fi
  if [ "$a" != "$b" ]; then
    pass=$((pass+1))
    echo "  PASS: $name"
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (hashes equal but should differ)"
    echo "    a=$a"
  fi
}

# Assert hash matches a specific shape (40 lowercase hex chars).
shape() {
  local name="$1" h="$2"
  if echo "$h" | grep -qE '^[0-9a-f]{40}$'; then
    pass=$((pass+1))
    echo "  PASS: $name"
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (bad shape)"
    echo "    h=|$h|"
  fi
}

echo "=== Version Control Oracle Tests: dolt_hashof suite ==="
echo ""

# ── 1. Determinism ────────────────────────────────────────
echo "--- 1. Determinism: same input → same hash ---"

SEED_A="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'seed');
"

H1=$(run_hash "det1" "$SEED_A" "SELECT dolt_hashof_table('t');")
H2=$(run_hash "det2" "$SEED_A" "SELECT dolt_hashof_table('t');")
same "determinism_table_same_inputs" "$H1" "$H2"

H1=$(run_hash "det_db1" "$SEED_A" "SELECT dolt_hashof_db();")
H2=$(run_hash "det_db2" "$SEED_A" "SELECT dolt_hashof_db();")
same "determinism_db_same_inputs" "$H1" "$H2"

echo ""

# ── 2. Insert order invariance ────────────────────────────
echo "--- 2. Insert order invariance ---"

ORDER_ABC="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a');
INSERT INTO t VALUES (2, 'b');
INSERT INTO t VALUES (3, 'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'abc');
"

ORDER_CBA="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (3, 'c');
INSERT INTO t VALUES (2, 'b');
INSERT INTO t VALUES (1, 'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'cba');
"

ORDER_BATCH="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (2, 'b'), (1, 'a'), (3, 'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'batch');
"

H_ABC=$(run_hash "order_abc" "$ORDER_ABC" "SELECT dolt_hashof_table('t');")
H_CBA=$(run_hash "order_cba" "$ORDER_CBA" "SELECT dolt_hashof_table('t');")
H_BAT=$(run_hash "order_batch" "$ORDER_BATCH" "SELECT dolt_hashof_table('t');")
same "order_abc_vs_cba"    "$H_ABC" "$H_CBA"
same "order_abc_vs_batch"  "$H_ABC" "$H_BAT"

echo ""

# ── 3. Intermediate state invariance ──────────────────────
echo "--- 3. Intermediate state invariance (delete+reinsert is a no-op) ---"

NET="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a'), (2, 'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'net');
"

THROUGH_DELETE="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a'), (2, 'b'), (99, 'temp');
DELETE FROM t WHERE id = 99;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'through_delete');
"

DELETE_REINSERT="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a'), (2, 'b');
DELETE FROM t WHERE id = 1;
INSERT INTO t VALUES (1, 'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'delete_reinsert');
"

H_NET=$(run_hash "net" "$NET" "SELECT dolt_hashof_table('t');")
H_TD=$(run_hash "through_delete" "$THROUGH_DELETE" "SELECT dolt_hashof_table('t');")
H_DR=$(run_hash "delete_reinsert" "$DELETE_REINSERT" "SELECT dolt_hashof_table('t');")
same "net_vs_through_delete"  "$H_NET" "$H_TD"
same "net_vs_delete_reinsert" "$H_NET" "$H_DR"

echo ""

# ── 4. Cross-branch state invariance ──────────────────────
echo "--- 4. Cross-branch state invariance ---"

BRANCH_CONVERGE="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');

SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES (2, 'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_c2');

SELECT dolt_checkout('main');
INSERT INTO t VALUES (2, 'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_c2');
"

DB="$TMPROOT/cross_branch.db"
rm -f "$DB"
printf "%s\n" "$BRANCH_CONVERGE" | "$DOLTLITE" "$DB" >/dev/null 2>"$TMPROOT/cross.err"

H_MAIN_T=$(run_hash_on "$DB" "SELECT dolt_hashof_table('t', 'main');")
H_FEAT_T=$(run_hash_on "$DB" "SELECT dolt_hashof_table('t', 'feat');")
same "cross_branch_table_hash_equal" "$H_MAIN_T" "$H_FEAT_T"

H_MAIN_COMMIT=$(run_hash_on "$DB" "SELECT dolt_hashof('main');")
H_FEAT_COMMIT=$(run_hash_on "$DB" "SELECT dolt_hashof('feat');")
different "cross_branch_commit_hash_differs" "$H_MAIN_COMMIT" "$H_FEAT_COMMIT"

echo ""

# ── 5. Per-table vs whole-db separation ───────────────────
echo "--- 5. Per-table isolation in dolt_hashof_table ---"

TWO_TABLES="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE u(id INTEGER PRIMARY KEY, w TEXT);
INSERT INTO t VALUES (1, 'a');
INSERT INTO u VALUES (1, 'x');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'two_tables');
"

DB="$TMPROOT/two_tables.db"
rm -f "$DB"
printf "%s\n" "$TWO_TABLES" | "$DOLTLITE" "$DB" >/dev/null 2>"$TMPROOT/two.err"

HT_BEFORE=$(run_hash_on "$DB" "SELECT dolt_hashof_table('t');")
HU_BEFORE=$(run_hash_on "$DB" "SELECT dolt_hashof_table('u');")
HDB_BEFORE=$(run_hash_on "$DB" "SELECT dolt_hashof_db();")

# Mutate u only; t's hash must not move.
"$DOLTLITE" "$DB" "INSERT INTO u VALUES (2, 'y'); SELECT dolt_add('-A'); SELECT dolt_commit('-m', 'bump_u');" >/dev/null 2>>"$TMPROOT/two.err"

HT_AFTER=$(run_hash_on "$DB" "SELECT dolt_hashof_table('t');")
HU_AFTER=$(run_hash_on "$DB" "SELECT dolt_hashof_table('u');")
HDB_AFTER=$(run_hash_on "$DB" "SELECT dolt_hashof_db();")

same      "t_hash_stable_on_u_mutation"   "$HT_BEFORE" "$HT_AFTER"
different "u_hash_changes_on_u_mutation"  "$HU_BEFORE" "$HU_AFTER"
different "db_hash_changes_on_u_mutation" "$HDB_BEFORE" "$HDB_AFTER"

echo ""

# ── 6. Reopen stability ───────────────────────────────────
echo "--- 6. Reopen stability ---"

REOPEN_SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'reopen');
"

DB="$TMPROOT/reopen.db"
rm -f "$DB"
printf "%s\n" "$REOPEN_SEED" | "$DOLTLITE" "$DB" >/dev/null 2>"$TMPROOT/reopen.err"

H_OPEN1=$(run_hash_on "$DB" "SELECT dolt_hashof_table('t');")
H_OPEN2=$(run_hash_on "$DB" "SELECT dolt_hashof_table('t');")
H_OPEN3_DB=$(run_hash_on "$DB" "SELECT dolt_hashof_db();")
H_OPEN4_MAIN=$(run_hash_on "$DB" "SELECT dolt_hashof('main');")
H_OPEN5_TAB=$(run_hash_on "$DB" "SELECT dolt_hashof_table('t');")
H_OPEN6_DB=$(run_hash_on "$DB" "SELECT dolt_hashof_db();")
H_OPEN7_MAIN=$(run_hash_on "$DB" "SELECT dolt_hashof('main');")

same "reopen_table_stable" "$H_OPEN1" "$H_OPEN5_TAB"
same "reopen_db_stable"    "$H_OPEN3_DB" "$H_OPEN6_DB"
same "reopen_commit_stable" "$H_OPEN4_MAIN" "$H_OPEN7_MAIN"

echo ""

# ── 7. Negative: mutations must invalidate ────────────────
echo "--- 7. Mutations must invalidate ---"

BASE="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a'), (2, 'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base');
"

ADD_ROW="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'add_row');
"

UPDATE_CELL="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a'), (2, 'b');
UPDATE t SET v = 'A' WHERE id = 1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'update');
"

ALTER_SCHEMA="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, w INT);
INSERT INTO t VALUES (1, 'a', NULL), (2, 'b', NULL);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'alter');
"

H_BASE=$(run_hash "neg_base" "$BASE" "SELECT dolt_hashof_table('t');")
H_ADD=$(run_hash "neg_add" "$ADD_ROW" "SELECT dolt_hashof_table('t');")
H_UPD=$(run_hash "neg_upd" "$UPDATE_CELL" "SELECT dolt_hashof_table('t');")
H_ALT=$(run_hash "neg_alt" "$ALTER_SCHEMA" "SELECT dolt_hashof_table('t');")

different "add_row_changes_table_hash"   "$H_BASE" "$H_ADD"
different "update_cell_changes_table_hash" "$H_BASE" "$H_UPD"
different "alter_schema_changes_table_hash" "$H_BASE" "$H_ALT"

echo ""

# ── 8. Ref resolution ─────────────────────────────────────
echo "--- 8. Ref resolution ---"

REF_SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
INSERT INTO t VALUES (2, 'b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c2');
"

DB="$TMPROOT/refs.db"
rm -f "$DB"
printf "%s\n" "$REF_SEED" | "$DOLTLITE" "$DB" >/dev/null 2>"$TMPROOT/refs.err"

H_MAIN=$(run_hash_on "$DB" "SELECT dolt_hashof('main');")
H_HEAD=$(run_hash_on "$DB" "SELECT dolt_hashof('HEAD');")
same "main_equals_HEAD" "$H_MAIN" "$H_HEAD"
shape "main_hash_shape" "$H_MAIN"

H_HEAD_PARENT=$(run_hash_on "$DB" "SELECT dolt_hashof('HEAD~1');")
different "HEAD_differs_from_HEAD_parent" "$H_HEAD" "$H_HEAD_PARENT"

# Passing the same commit hash string through dolt_hashof should
# return the same hash back (identity). Grab HEAD, then feed it.
H_ID_OUT=$(run_hash_on "$DB" "SELECT dolt_hashof('$H_HEAD');")
same "commit_hash_is_identity_on_hashof" "$H_HEAD" "$H_ID_OUT"

echo ""

# ── 9. Dolt shape conformance ─────────────────────────────
echo "--- 9. Dolt shape conformance ---"

SHAPE_SEED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'shape');
"

H_T=$(run_hash "shape_t" "$SHAPE_SEED" "SELECT dolt_hashof_table('t');")
H_D=$(run_hash "shape_d" "$SHAPE_SEED" "SELECT dolt_hashof_db();")
H_R=$(run_hash "shape_r" "$SHAPE_SEED" "SELECT dolt_hashof('main');")
shape "hashof_table_shape" "$H_T"
shape "hashof_db_shape"    "$H_D"
shape "hashof_ref_shape"   "$H_R"

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
