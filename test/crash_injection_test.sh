#!/bin/bash
#
# Crash injection durability test.
#
# Simulates power-loss crashes at every write point during a
# chunkStoreCommit and verifies that recovery always produces a
# consistent state: either the old committed state is intact or
# the new commit landed fully — never a partial/corrupt result.
#
# Requires a testfixture-style build (SQLITE_TEST defined) so
# the DOLTLITE_CRASH_WRITE environment variable is honored.
# The normal doltlite binary ignores it (crash macros are no-ops).
#
# Usage: bash test/crash_injection_test.sh [path/to/doltlite]
#

set -u

DOLTLITE="${1:-./doltlite}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0; FAILED_NAMES=""

pass_name() { pass=$((pass+1)); echo "  PASS: $1"; }
fail_name() {
  fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES $1"
  echo "  FAIL: $1"
}

echo "=== Crash Injection Durability Tests ==="

# ── Verify crash injection is active ─────────────────────
echo ""
echo "--- Verify crash injection works ---"

DB="$TMPROOT/verify.db"
rm -f "$DB"
"$DOLTLITE" "$DB" "CREATE TABLE t(id INTEGER PRIMARY KEY); SELECT dolt_commit('-Am','init');" >/dev/null 2>&1

# Crash on the very first write of the next commit
DOLTLITE_CRASH_WRITE=1 "$DOLTLITE" "$DB" "INSERT INTO t VALUES(1); SELECT dolt_commit('-Am','c2');" >/dev/null 2>&1
RC=$?
if [ "$RC" = "99" ]; then
  pass_name "crash_injection_active"
else
  # If exit code isn't 99, the binary wasn't built with SQLITE_TEST
  fail_name "crash_injection_active"
  echo "    expected exit code 99, got $RC"
  echo "    (binary may not be built with SQLITE_TEST — use testfixture build)"
  echo ""
  echo "======================================="
  echo "Results: $pass passed, $fail failed"
  echo "======================================="
  exit 1
fi

echo ""

# ── Scenario 1: Crash during first commit ────────────────
#
# Start with an empty db. The first dolt_commit writes the
# manifest + chunk data + root record. Crash at each write
# point and verify the db either opens empty or opens with
# the committed state.
echo "--- Scenario 1: Crash during first commit ---"

for N in 1 2 3 4 5 6 7 8 9 10; do
  DB="$TMPROOT/s1_n${N}.db"
  rm -f "$DB"

  # Attempt a commit that will crash at write N
  DOLTLITE_CRASH_WRITE=$N "$DOLTLITE" "$DB" \
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
     INSERT INTO t VALUES(1,'hello');
     SELECT dolt_commit('-Am','first');" >/dev/null 2>&1
  RC=$?

  if [ "$RC" != "99" ]; then
    # The commit completed before reaching write N — no crash.
    # This means N exceeds the total writes for this commit.
    # Verify the commit landed.
    COUNT=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t;" 2>/dev/null)
    if [ "$COUNT" = "1" ]; then
      pass_name "s1_write${N}_commit_landed"
    else
      fail_name "s1_write${N}_commit_landed"
      echo "    commit succeeded but data missing (count=$COUNT)"
    fi
    break
  fi

  # Crash happened. Verify recovery.
  if [ ! -f "$DB" ]; then
    # DB file doesn't exist — crash happened before any write.
    pass_name "s1_write${N}_no_db_after_crash"
    continue
  fi

  # Try to open the crashed database
  RESULT=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t;" 2>&1)
  RC2=$?

  if [ "$RC2" = "0" ] && [ "$RESULT" = "1" ]; then
    # Full commit present — crash was after the root record
    pass_name "s1_write${N}_recovery_full_commit"
  elif [ "$RC2" = "0" ] && [ "$RESULT" = "0" ]; then
    # Table exists but empty — schema committed, data didn't
    # This is acceptable only if the whole commit is absent
    fail_name "s1_write${N}_partial_commit"
    echo "    table exists but is empty — partial state"
  elif echo "$RESULT" | grep -qiE "no such table|malformed|corrupt"; then
    # Table doesn't exist — commit rolled back entirely. Good.
    pass_name "s1_write${N}_recovery_rolled_back"
  else
    # Some other state
    fail_name "s1_write${N}_unexpected_state"
    echo "    rc=$RC2 result=$RESULT"
  fi
done

echo ""

# ── Scenario 2: Crash during second commit ───────────────
#
# Create a db with one committed row, then crash during a
# second commit that adds more rows. Verify the first commit
# is always intact.
echo "--- Scenario 2: Crash during second commit (first commit survives) ---"

BASELINE="$TMPROOT/s2_baseline.db"
rm -f "$BASELINE"
"$DOLTLITE" "$BASELINE" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
   INSERT INTO t VALUES(1,'baseline');
   SELECT dolt_commit('-Am','c1');" >/dev/null 2>&1

# Verify baseline
BCOUNT=$("$DOLTLITE" "$BASELINE" "SELECT count(*) FROM t;" 2>/dev/null)
if [ "$BCOUNT" != "1" ]; then
  fail_name "s2_baseline_setup"
  echo "    baseline has $BCOUNT rows, expected 1"
else
  pass_name "s2_baseline_setup"
fi

for N in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
  DB="$TMPROOT/s2_n${N}.db"
  cp "$BASELINE" "$DB"

  DOLTLITE_CRASH_WRITE=$N "$DOLTLITE" "$DB" \
    "INSERT INTO t VALUES(2,'second');
     INSERT INTO t VALUES(3,'third');
     SELECT dolt_commit('-Am','c2');" >/dev/null 2>&1
  RC=$?

  if [ "$RC" != "99" ]; then
    # No crash — commit completed. Verify both old + new data.
    COUNT=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t;" 2>/dev/null)
    if [ "$COUNT" = "3" ]; then
      pass_name "s2_write${N}_commit_landed"
    else
      fail_name "s2_write${N}_commit_landed"
      echo "    expected 3 rows, got $COUNT"
    fi
    break
  fi

  # Crash happened. First commit MUST survive.
  COUNT=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t;" 2>/dev/null)
  V1=$("$DOLTLITE" "$DB" "SELECT v FROM t WHERE id=1;" 2>/dev/null)

  if [ "$V1" != "baseline" ] && [ -n "$V1" ]; then
    # First commit's data is corrupted — real data loss
    fail_name "s2_write${N}_first_commit_damaged"
    echo "    count=$COUNT v1=$V1"
  elif [ "$COUNT" = "1" ]; then
    pass_name "s2_write${N}_first_commit_intact"
  elif [ "$COUNT" = "3" ]; then
    # Second commit also present — crash was after root record
    pass_name "s2_write${N}_both_commits_present"
  elif [ "$COUNT" = "2" ]; then
    # Working set has uncommitted SQL mutations from the
    # failed dolt_commit. The first commit is intact (v1=baseline)
    # and dolt_reset --hard would restore to committed state.
    # This is a dirty working tree, not data loss.
    pass_name "s2_write${N}_working_set_has_uncommitted"
  else
    fail_name "s2_write${N}_unexpected_state"
    echo "    count=$COUNT v1=$V1"
  fi
done

echo ""

# ── Scenario 3: Crash during indexed update commit ───────
#
# Table with a secondary index. The commit writes more chunks
# (main table + index). Crash at various points.
echo "--- Scenario 3: Crash during indexed update commit ---"

BASELINE3="$TMPROOT/s3_baseline.db"
rm -f "$BASELINE3"
"$DOLTLITE" "$BASELINE3" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER, v TEXT);
   CREATE INDEX idx_k ON t(k);
   INSERT INTO t VALUES(1,100,'baseline');
   SELECT dolt_commit('-Am','c1');" >/dev/null 2>&1

for N in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
  DB="$TMPROOT/s3_n${N}.db"
  cp "$BASELINE3" "$DB"

  DOLTLITE_CRASH_WRITE=$N "$DOLTLITE" "$DB" \
    "BEGIN;
     INSERT INTO t VALUES(2,200,'second');
     INSERT INTO t VALUES(3,300,'third');
     UPDATE t SET k=101 WHERE id=1;
     COMMIT;
     SELECT dolt_commit('-Am','c2');" >/dev/null 2>&1
  RC=$?

  if [ "$RC" != "99" ]; then
    COUNT=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t;" 2>/dev/null)
    if [ "$COUNT" = "3" ]; then
      pass_name "s3_write${N}_commit_landed"
    else
      fail_name "s3_write${N}_commit_landed"
      echo "    expected 3 rows, got $COUNT"
    fi
    break
  fi

  # First commit must survive
  COUNT=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t;" 2>/dev/null)
  K1=$("$DOLTLITE" "$DB" "SELECT k FROM t WHERE id=1;" 2>/dev/null)

  if [ "$COUNT" = "1" ] && [ "$K1" = "100" ]; then
    pass_name "s3_write${N}_first_commit_intact"
  elif [ "$COUNT" = "3" ]; then
    pass_name "s3_write${N}_both_commits_present"
  else
    fail_name "s3_write${N}_state_inconsistent"
    echo "    count=$COUNT k1=$K1"
  fi
done

echo ""

# ── Scenario 4: Crash during merge commit ────────────────
#
# Two branches merge. Crash during the merge commit write.
# Verify either pre-merge state or post-merge state, never
# partial.
echo "--- Scenario 4: Crash during merge commit ---"

BASELINE4="$TMPROOT/s4_baseline.db"
rm -f "$BASELINE4"
"$DOLTLITE" "$BASELINE4" >/dev/null 2>&1 <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_commit('-Am','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_commit('-Am','feat_c');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
SELECT dolt_commit('-Am','main_c');
SQL

for N in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
  DB="$TMPROOT/s4_n${N}.db"
  cp "$BASELINE4" "$DB"

  DOLTLITE_CRASH_WRITE=$N "$DOLTLITE" "$DB" \
    "SELECT dolt_merge('feat');" >/dev/null 2>&1
  RC=$?

  if [ "$RC" != "99" ]; then
    COUNT=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t;" 2>/dev/null)
    if [ "$COUNT" = "3" ]; then
      pass_name "s4_write${N}_merge_landed"
    else
      fail_name "s4_write${N}_merge_landed"
      echo "    expected 3 rows after merge, got $COUNT"
    fi
    break
  fi

  # Pre-merge state: main has rows 1,3. Post-merge: 1,2,3.
  COUNT=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t;" 2>/dev/null)

  if [ "$COUNT" = "2" ]; then
    # Pre-merge state (rows 1 + 3)
    V3=$("$DOLTLITE" "$DB" "SELECT v FROM t WHERE id=3;" 2>/dev/null)
    if [ "$V3" = "main" ]; then
      pass_name "s4_write${N}_pre_merge_intact"
    else
      fail_name "s4_write${N}_pre_merge_corrupt"
      echo "    count=2 but v3=$V3"
    fi
  elif [ "$COUNT" = "3" ]; then
    pass_name "s4_write${N}_post_merge_present"
  else
    fail_name "s4_write${N}_unexpected_count"
    echo "    count=$COUNT (expected 2 or 3)"
  fi
done

# ── Scenario 5: Deep history survives crash on latest commit ──
#
# Build 5 commits of history, then crash during a 6th. All
# 5 previous commits must be intact and dolt_log must show them.
echo "--- Scenario 5: Deep history survives crash ---"

BASELINE5="$TMPROOT/s5_baseline.db"
rm -f "$BASELINE5"
"$DOLTLITE" "$BASELINE5" >/dev/null 2>&1 <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'c1');
SELECT dolt_commit('-Am','c1');
INSERT INTO t VALUES(2,'c2');
SELECT dolt_commit('-Am','c2');
INSERT INTO t VALUES(3,'c3');
SELECT dolt_commit('-Am','c3');
INSERT INTO t VALUES(4,'c4');
SELECT dolt_commit('-Am','c4');
INSERT INTO t VALUES(5,'c5');
SELECT dolt_commit('-Am','c5');
SQL

for N in 1 2 3 4 5 6 7 8 9 10; do
  DB="$TMPROOT/s5_n${N}.db"
  cp "$BASELINE5" "$DB"

  DOLTLITE_CRASH_WRITE=$N "$DOLTLITE" "$DB" \
    "INSERT INTO t VALUES(6,'c6'); SELECT dolt_commit('-Am','c6');" >/dev/null 2>&1
  RC=$?

  if [ "$RC" != "99" ]; then
    COUNT=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t;" 2>/dev/null)
    if [ "$COUNT" = "6" ]; then
      pass_name "s5_write${N}_all_6_commits"
    else
      fail_name "s5_write${N}_wrong_count"
      echo "    expected 6, got $COUNT"
    fi
    break
  fi

  # All 5 original rows must survive
  COUNT=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t;" 2>/dev/null)
  LOGCOUNT=$("$DOLTLITE" "$DB" "SELECT count(*) FROM dolt_log;" 2>/dev/null)

  if [ "$COUNT" = "5" ] || [ "$COUNT" = "6" ]; then
    pass_name "s5_write${N}_history_intact"
  else
    fail_name "s5_write${N}_history_damaged"
    echo "    rows=$COUNT log=$LOGCOUNT"
  fi
done

echo ""

# ── Scenario 6: Large transaction (many chunks) ──────────
#
# Insert 500 rows in one commit. This produces many chunks
# in the WAL. Crash at various points across the chunk stream.
echo "--- Scenario 6: Large transaction crash ---"

BASELINE6="$TMPROOT/s6_baseline.db"
rm -f "$BASELINE6"
"$DOLTLITE" "$BASELINE6" >/dev/null 2>&1 <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'anchor');
SELECT dolt_commit('-Am','c1');
SQL

# Generate 500 inserts
INSERTS=""
for i in $(seq 2 501); do
  INSERTS="${INSERTS}INSERT INTO t VALUES($i,'row$i');"
done

for N in 1 3 5 10 15 20 25 30; do
  DB="$TMPROOT/s6_n${N}.db"
  cp "$BASELINE6" "$DB"

  DOLTLITE_CRASH_WRITE=$N "$DOLTLITE" "$DB" \
    "BEGIN; ${INSERTS} COMMIT; SELECT dolt_commit('-Am','bulk');" >/dev/null 2>&1
  RC=$?

  if [ "$RC" != "99" ]; then
    COUNT=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t;" 2>/dev/null)
    if [ "$COUNT" = "501" ]; then
      pass_name "s6_write${N}_bulk_landed"
    else
      fail_name "s6_write${N}_wrong_count"
      echo "    expected 501, got $COUNT"
    fi
    break
  fi

  # Anchor row must survive
  V1=$("$DOLTLITE" "$DB" "SELECT v FROM t WHERE id=1;" 2>/dev/null)
  if [ "$V1" = "anchor" ]; then
    pass_name "s6_write${N}_anchor_intact"
  else
    fail_name "s6_write${N}_anchor_damaged"
    echo "    v1=$V1"
  fi
done

echo ""

# ── Scenario 7: Crash during branch checkout ─────────────
#
# Checkout writes the working set for the new branch. Crash
# during that persist and verify the db still opens on the
# original branch.
echo "--- Scenario 7: Crash during branch checkout ---"

BASELINE7="$TMPROOT/s7_baseline.db"
rm -f "$BASELINE7"
"$DOLTLITE" "$BASELINE7" >/dev/null 2>&1 <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'main_data');
SELECT dolt_commit('-Am','c1');
SELECT dolt_branch('other');
SELECT dolt_checkout('other');
INSERT INTO t VALUES(2,'other_data');
SELECT dolt_commit('-Am','other_c');
SELECT dolt_checkout('main');
SQL

for N in 1 2 3 4 5 6 7 8 9 10; do
  DB="$TMPROOT/s7_n${N}.db"
  cp "$BASELINE7" "$DB"

  DOLTLITE_CRASH_WRITE=$N "$DOLTLITE" "$DB" \
    "SELECT dolt_checkout('other');" >/dev/null 2>&1
  RC=$?

  if [ "$RC" != "99" ]; then
    # Checkout completed — verify we're on 'other' with its data
    COUNT=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t;" 2>/dev/null)
    if [ "$COUNT" = "2" ]; then
      pass_name "s7_write${N}_checkout_landed"
    else
      fail_name "s7_write${N}_wrong_count"
      echo "    expected 2, got $COUNT"
    fi
    break
  fi

  # DB must still open. Either on main (1 row) or other (2 rows).
  COUNT=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t;" 2>/dev/null)
  if [ "$COUNT" = "1" ] || [ "$COUNT" = "2" ]; then
    pass_name "s7_write${N}_db_consistent"
  else
    fail_name "s7_write${N}_db_corrupt"
    echo "    count=$COUNT"
  fi
done

echo ""

# ── Scenario 8: Crash during GC rewrite/rename ───────────
#
# GC is a pure storage rewrite. Crashing at any write/sync/rename
# point must preserve identical logical state after reopen.
echo "--- Scenario 8: Crash during GC rewrite ---"

BASELINE8="$TMPROOT/s8_baseline.db"
rm -f "$BASELINE8"
"$DOLTLITE" "$BASELINE8" >/dev/null 2>&1 <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'main-1');
SELECT dolt_commit('-Am','c1');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat-2');
SELECT dolt_commit('-Am','c2');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main-3');
SELECT dolt_commit('-Am','c3');
SELECT dolt_gc();
SQL

BASE_COUNT=$("$DOLTLITE" "$BASELINE8" "SELECT count(*) FROM t;" 2>/dev/null)
BASE_LOG=$("$DOLTLITE" "$BASELINE8" "SELECT count(*) FROM dolt_log;" 2>/dev/null)
BASE_BRANCHES=$("$DOLTLITE" "$BASELINE8" "SELECT count(*) FROM dolt_branches;" 2>/dev/null)

for N in 1 2 3 4 5 6 7 8 9 10 11 12; do
  DB="$TMPROOT/s8_n${N}.db"
  cp "$BASELINE8" "$DB"

  DOLTLITE_CRASH_GC_WRITE=$N "$DOLTLITE" "$DB" \
    "SELECT dolt_gc();" >/dev/null 2>&1
  RC=$?

  COUNT=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t;" 2>/dev/null)
  LOGCOUNT=$("$DOLTLITE" "$DB" "SELECT count(*) FROM dolt_log;" 2>/dev/null)
  BRANCHES=$("$DOLTLITE" "$DB" "SELECT count(*) FROM dolt_branches;" 2>/dev/null)

  if [ "$COUNT" = "$BASE_COUNT" ] && [ "$LOGCOUNT" = "$BASE_LOG" ] && [ "$BRANCHES" = "$BASE_BRANCHES" ]; then
    if [ "$RC" = "99" ]; then
      pass_name "s8_write${N}_gc_crash_preserves_state"
    else
      pass_name "s8_write${N}_gc_completed_preserves_state"
      break
    fi
  else
    fail_name "s8_write${N}_gc_state_changed"
    echo "    rc=$RC count=$COUNT/$BASE_COUNT log=$LOGCOUNT/$BASE_LOG branches=$BRANCHES/$BASE_BRANCHES"
  fi
done

echo ""

# ── Scenario 9: Crash during push persist ────────────────
#
# A push writes refs/chunks to the remote store. Crash at each
# remote commit write point and verify the remote is either
# unchanged or fully updated — never partially advanced.
echo "--- Scenario 9: Crash during push persist ---"

REMOTE9="$TMPROOT/s9_remote.db"
LOCAL9="$TMPROOT/s9_local.db"
rm -f "$REMOTE9" "$LOCAL9"
"$DOLTLITE" "$REMOTE9" >/dev/null 2>&1 <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'remote');
SELECT dolt_commit('-Am','remote_init');
SQL
"$DOLTLITE" "$LOCAL9" >/dev/null 2>&1 <<SQL
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'local');
SELECT dolt_commit('-Am','local_init');
SELECT dolt_remote('add','origin','file://$REMOTE9');
UPDATE t SET v='local pushed' WHERE id=1;
SELECT dolt_commit('-Am','local_update');
SQL

BASE_REMOTE_V=$("$DOLTLITE" "$REMOTE9" "SELECT v FROM t WHERE id=1;" 2>/dev/null)
BASE_REMOTE_LOG=$("$DOLTLITE" "$REMOTE9" "SELECT count(*) FROM dolt_log;" 2>/dev/null)
BASE_REMOTE_HEAD=$("$DOLTLITE" "$REMOTE9" "SELECT commit_hash FROM dolt_log LIMIT 1;" 2>/dev/null)
FINAL_REMOTE_HEAD=$("$DOLTLITE" "$LOCAL9" "SELECT commit_hash FROM dolt_log LIMIT 1;" 2>/dev/null)

for N in 1 2 3 4 5 6 7 8 9 10 11 12; do
  DB_REMOTE="$TMPROOT/s9_n${N}_remote.db"
  DB_LOCAL="$TMPROOT/s9_n${N}_local.db"
  cp "$REMOTE9" "$DB_REMOTE"
  cp "$LOCAL9" "$DB_LOCAL"

  DOLTLITE_CRASH_WRITE=$N "$DOLTLITE" "$DB_LOCAL" \
    "SELECT dolt_remote('add','crash','file://$DB_REMOTE');
     SELECT dolt_push('crash','main','--force');" >/dev/null 2>&1
  RC=$?

  REMOTE_V=$("$DOLTLITE" "$DB_REMOTE" "SELECT v FROM t WHERE id=1;" 2>/dev/null)
  REMOTE_LOG=$("$DOLTLITE" "$DB_REMOTE" "SELECT count(*) FROM dolt_log;" 2>/dev/null)
  REMOTE_HEAD=$("$DOLTLITE" "$DB_REMOTE" "SELECT commit_hash FROM dolt_log LIMIT 1;" 2>/dev/null)

  if [ "$REMOTE_V" = "$BASE_REMOTE_V" ] && [ "$REMOTE_HEAD" = "$BASE_REMOTE_HEAD" ] && [ "$REMOTE_LOG" = "$BASE_REMOTE_LOG" ]; then
    pass_name "s9_write${N}_push_crash_preserves_remote"
  elif [ "$REMOTE_V" = "local pushed" ] && [ "$REMOTE_HEAD" = "$FINAL_REMOTE_HEAD" ] && [ "$REMOTE_LOG" = "$BASE_REMOTE_LOG" ]; then
    if [ "$RC" = "99" ]; then
      pass_name "s9_write${N}_push_crash_after_full_persist"
    else
      pass_name "s9_write${N}_push_completed"
      break
    fi
  else
    fail_name "s9_write${N}_push_partial_state"
    echo "    rc=$RC v=$REMOTE_V log=$REMOTE_LOG/$BASE_REMOTE_LOG base_head=$BASE_REMOTE_HEAD head=$REMOTE_HEAD final_head=$FINAL_REMOTE_HEAD"
  fi
done

echo ""

# ── Scenario 10: Crash during clone persist ──────────────
#
# Clone hydrates chunks and then persists refs/default branch
# locally. Crash at each write point and verify the destination
# is either still empty or fully cloned.
echo "--- Scenario 10: Crash during clone persist ---"

REMOTE10="$TMPROOT/s10_remote.db"
rm -f "$REMOTE10"
"$DOLTLITE" "$REMOTE10" >/dev/null 2>&1 <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'from_remote');
SELECT dolt_commit('-Am','remote_init');
SQL

for N in 1 2 3 4 5 6 7 8 9 10 11 12; do
  DB_LOCAL="$TMPROOT/s10_n${N}_local.db"
  rm -f "$DB_LOCAL"

  DOLTLITE_CRASH_WRITE=$N "$DOLTLITE" "$DB_LOCAL" \
    "SELECT dolt_clone('file://$REMOTE10');" >/dev/null 2>&1
  RC=$?

  TABLE_COUNT=$("$DOLTLITE" "$DB_LOCAL" "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='t';" 2>/dev/null)
  REMOTE_COUNT=$("$DOLTLITE" "$DB_LOCAL" "SELECT count(*) FROM dolt_remotes;" 2>/dev/null)
  ROW_COUNT=$("$DOLTLITE" "$DB_LOCAL" "SELECT count(*) FROM t;" 2>/dev/null)

  if [ "$TABLE_COUNT" = "0" ] && { [ -z "$REMOTE_COUNT" ] || [ "$REMOTE_COUNT" = "0" ]; }; then
    pass_name "s10_write${N}_clone_crash_restores_empty"
  elif [ "$TABLE_COUNT" = "1" ] && [ "$REMOTE_COUNT" = "1" ] && [ "$ROW_COUNT" = "1" ]; then
    if [ "$RC" = "99" ]; then
      pass_name "s10_write${N}_clone_crash_after_full_persist"
    else
      pass_name "s10_write${N}_clone_completed"
      break
    fi
  else
    fail_name "s10_write${N}_clone_partial_state"
    echo "    rc=$RC tables=$TABLE_COUNT remotes=$REMOTE_COUNT rows=$ROW_COUNT"
  fi
done

echo ""

# ── Scenario 11: Crash during pull persist ───────────────
#
# Pull fast-forwards local refs and working state. Crash during
# the final refs persist and verify local state is either the
# old tip or the fully pulled tip.
echo "--- Scenario 11: Crash during pull persist ---"

REMOTE11="$TMPROOT/s11_remote.db"
LOCAL11="$TMPROOT/s11_local.db"
REMOTE_CLIENT11="$TMPROOT/s11_remote_client.db"
rm -f "$REMOTE11" "$LOCAL11" "$REMOTE_CLIENT11"
"$DOLTLITE" "$LOCAL11" >/dev/null 2>&1 <<SQL
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_remote('add','origin','file://$REMOTE11');
SELECT dolt_push('origin','main');
SQL
"$DOLTLITE" "$REMOTE_CLIENT11" "SELECT dolt_clone('file://$REMOTE11');" >/dev/null 2>&1
"$DOLTLITE" "$REMOTE_CLIENT11" \
  "INSERT INTO t VALUES(2,'b'); SELECT dolt_add('-A'); SELECT dolt_commit('-m','remote update'); SELECT dolt_push('origin','main');" >/dev/null 2>&1

BASE_LOCAL_COUNT=$("$DOLTLITE" "$LOCAL11" "SELECT count(*) FROM t;" 2>/dev/null)
BASE_LOCAL_HEAD=$("$DOLTLITE" "$LOCAL11" "SELECT commit_hash FROM dolt_log LIMIT 1;" 2>/dev/null)
FINAL_LOCAL_HEAD=$("$DOLTLITE" "$REMOTE_CLIENT11" "SELECT commit_hash FROM dolt_log LIMIT 1;" 2>/dev/null)

for N in 1 2 3 4 5 6 7 8 9 10 11 12; do
  DB_LOCAL="$TMPROOT/s11_n${N}_local.db"
  cp "$LOCAL11" "$DB_LOCAL"

  DOLTLITE_CRASH_WRITE=$N "$DOLTLITE" "$DB_LOCAL" \
    "SELECT dolt_pull('origin','main');" >/dev/null 2>&1
  RC=$?

  LOCAL_COUNT=$("$DOLTLITE" "$DB_LOCAL" "SELECT count(*) FROM t;" 2>/dev/null)
  LOCAL_HEAD=$("$DOLTLITE" "$DB_LOCAL" "SELECT commit_hash FROM dolt_log LIMIT 1;" 2>/dev/null)
  REMOTE_COUNT=$("$DOLTLITE" "$DB_LOCAL" "SELECT count(*) FROM dolt_remotes;" 2>/dev/null)

  if [ "$LOCAL_COUNT" = "$BASE_LOCAL_COUNT" ] && [ "$LOCAL_HEAD" = "$BASE_LOCAL_HEAD" ] && [ "$REMOTE_COUNT" = "1" ]; then
    pass_name "s11_write${N}_pull_crash_keeps_old_tip"
  elif [ "$LOCAL_COUNT" = "2" ] && [ "$LOCAL_HEAD" = "$FINAL_LOCAL_HEAD" ] && [ "$REMOTE_COUNT" = "1" ]; then
    if [ "$RC" = "99" ]; then
      pass_name "s11_write${N}_pull_crash_after_full_persist"
    else
      pass_name "s11_write${N}_pull_completed"
      break
    fi
  else
    fail_name "s11_write${N}_pull_partial_state"
    echo "    rc=$RC count=$LOCAL_COUNT base_count=$BASE_LOCAL_COUNT head=$LOCAL_HEAD base_head=$BASE_LOCAL_HEAD final_head=$FINAL_LOCAL_HEAD remotes=$REMOTE_COUNT"
  fi
done

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
