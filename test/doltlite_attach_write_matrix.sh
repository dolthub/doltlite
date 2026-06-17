#!/bin/bash
# ATTACH write matrix: every write that targets a doltlite database
# must succeed, whether the doltlite DB is main or attached, and
# whether the other side is doltlite, stock SQLite, or :memory:.
#
# Excluded: stock sqlite3 binary opening a doltlite file (file headers
# differ; the inverse — doltlite opening sqlite files — already works
# via the origBtree path).
#
# Tests are organized by configuration. Each config runs the same
# case set:
#   R1: read attached table
#   R2: cross-DB join
#   W1: autocommit INSERT into attached
#   W2: autocommit UPDATE on attached
#   W3: autocommit DELETE on attached
#   W4: autocommit INSERT into main
#   D1: CREATE TABLE on attached
#   D2: CREATE INDEX on attached
#   D3: DROP TABLE on attached
#   X1: INSERT INTO main SELECT FROM attached
#   X2: INSERT INTO attached SELECT FROM main
#   X3: CREATE TABLE main.copy AS SELECT FROM attached
#   X4: clone-like — copy whole schema + data from attached to main
#   T1: BEGIN; write attached; COMMIT
#   T2: BEGIN; write main and attached; COMMIT
#   T3: BEGIN; write main and attached; ROLLBACK
#   P1: writes durable across reopen (main + attached separately)
#
# Atomicity (E1, E2) is exercised in a separate suite.

DOLTLITE="${1:-./doltlite}"
SQLITE3="${2:-$(command -v sqlite3 2>/dev/null)}"
TMP=$(mktemp -d)
trap "rm -rf $TMP" EXIT
PASS=0; FAIL=0; SKIP=0
ERRORS=""

want_eq() {
  local name="$1" got="$2" want="$3"
  if [ "$got" = "$want" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\n  FAIL: $name\n    want: $(printf %q "$want")\n    got:  $(printf %q "$got")"
  fi
}

# Run a SQL string against doltlite, return the LAST line of stdout.
dl_last() { printf '%s\n' "$1" | $DOLTLITE "$2" 2>&1 | tail -1; }
# Run a SQL string against doltlite, return all stdout.
dl_all()  { printf '%s\n' "$1" | $DOLTLITE "$2" 2>&1; }
# Same for sqlite3.
sq_last() { printf '%s\n' "$1" | $SQLITE3 "$2" 2>&1 | tail -1; }

# Spin up a fresh doltlite DB with optional initial schema (no commit).
mk_dl() {
  local p="$1" sch="$2"
  rm -f "$p"
  if [ -n "$sch" ]; then printf '%s\n' "$sch" | $DOLTLITE "$p" >/dev/null 2>&1; fi
}
mk_sq() {
  local p="$1" sch="$2"
  rm -f "$p"
  if [ -n "$sch" ]; then printf '%s\n' "$sch" | $SQLITE3 "$p"; fi
}

# Runs every case for a (main_kind, attached_kind) pair.
# main_kind: "dl" | "dlmem" — main is doltlite (file or :memory:)
# attached_kind: "dl" | "sq" | "mem" — attached file is doltlite, sqlite, or :memory:
# Uses a unique prefix to avoid collisions.
run_config() {
  local cfg="$1" main_kind="$2" attached_kind="$3"
  local M A IS_SQL=0 IS_MEM=0 ATT_PATH
  echo ""
  echo "=== config: $cfg ==="

  # Main DB path
  if [ "$main_kind" = "dlmem" ]; then
    M=":memory:"
  else
    M="$TMP/${cfg}_main.db"
    rm -f "$M"
  fi

  # Attached DB path
  case "$attached_kind" in
    sq)  ATT_PATH="$TMP/${cfg}_att.db"; IS_SQL=1 ;;
    mem) ATT_PATH=":memory:"; IS_MEM=1 ;;
    dl)  ATT_PATH="$TMP/${cfg}_att.db" ;;
  esac
  rm -f "$ATT_PATH" 2>/dev/null || true

  # Seed attached so it has a table 'u'.
  local ATT_SEED="CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO u VALUES(1,'seed_a');"
  if [ "$IS_SQL" = "1" ]; then
    mk_sq "$ATT_PATH" "$ATT_SEED"
  elif [ "$IS_MEM" = "1" ]; then
    : # can't pre-seed :memory: — seed in the same connection
  else
    mk_dl "$ATT_PATH" "$ATT_SEED"
  fi

  # Seed main with table 't'.
  local MAIN_SEED="CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(10,'seed_m');"
  if [ "$main_kind" = "dlmem" ]; then
    : # seeded in-line below
  else
    printf '%s\n' "$MAIN_SEED" | $DOLTLITE "$M" >/dev/null 2>&1
  fi

  # Build the prelude that ATTACHes and (for :memory: attached) seeds it.
  local PRELUDE="ATTACH '$ATT_PATH' AS x;"
  if [ "$IS_MEM" = "1" ]; then
    PRELUDE="$PRELUDE $ATT_SEED"
    # rewrite to target attached schema
    PRELUDE="ATTACH '$ATT_PATH' AS x; CREATE TABLE x.u(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO x.u VALUES(1,'seed_a');"
  fi
  if [ "$main_kind" = "dlmem" ]; then
    PRELUDE="$MAIN_SEED $PRELUDE"
  fi

  # --- R1: read attached
  R=$(dl_last "$PRELUDE SELECT v FROM x.u WHERE id=1;" "$M")
  want_eq "$cfg/R1_read_attached" "$R" "seed_a"

  # --- R2: cross-DB join
  R=$(dl_last "$PRELUDE SELECT m.v||'+'||a.v FROM t m JOIN x.u a ON m.id=10 AND a.id=1;" "$M")
  want_eq "$cfg/R2_cross_db_join" "$R" "seed_m+seed_a"

  # Helper to reset both DBs to seeded state between mutating cases.
  reset_dbs() {
    if [ "$main_kind" != "dlmem" ]; then
      rm -f "$M"; printf '%s\n' "$MAIN_SEED" | $DOLTLITE "$M" >/dev/null 2>&1
    fi
    if [ "$IS_SQL" = "1" ]; then
      mk_sq "$ATT_PATH" "$ATT_SEED"
    elif [ "$IS_MEM" != "1" ]; then
      mk_dl "$ATT_PATH" "$ATT_SEED"
    fi
  }

  # --- W1: autocommit INSERT into attached
  reset_dbs
  R=$(dl_last "$PRELUDE INSERT INTO x.u VALUES(2,'w1'); SELECT v FROM x.u WHERE id=2;" "$M")
  want_eq "$cfg/W1_autocommit_insert_attached" "$R" "w1"

  # --- W2: autocommit UPDATE on attached
  reset_dbs
  R=$(dl_last "$PRELUDE UPDATE x.u SET v='w2' WHERE id=1; SELECT v FROM x.u WHERE id=1;" "$M")
  want_eq "$cfg/W2_autocommit_update_attached" "$R" "w2"

  # --- W3: autocommit DELETE on attached
  reset_dbs
  R=$(dl_last "$PRELUDE DELETE FROM x.u WHERE id=1; SELECT count(*) FROM x.u WHERE id=1;" "$M")
  want_eq "$cfg/W3_autocommit_delete_attached" "$R" "0"

  # --- W4: autocommit INSERT into main
  reset_dbs
  R=$(dl_last "$PRELUDE INSERT INTO t VALUES(11,'w4'); SELECT v FROM t WHERE id=11;" "$M")
  want_eq "$cfg/W4_autocommit_insert_main" "$R" "w4"

  # --- D1: CREATE TABLE on attached
  reset_dbs
  R=$(dl_last "$PRELUDE CREATE TABLE x.newt(id INTEGER PRIMARY KEY); INSERT INTO x.newt VALUES(7); SELECT id FROM x.newt;" "$M")
  want_eq "$cfg/D1_create_table_attached" "$R" "7"

  # --- D2: CREATE INDEX on attached
  reset_dbs
  R=$(dl_last "$PRELUDE CREATE INDEX x.idx_u_v ON u(v); SELECT name FROM x.sqlite_master WHERE type='index';" "$M")
  want_eq "$cfg/D2_create_index_attached" "$R" "idx_u_v"

  # --- D3: DROP TABLE on attached
  reset_dbs
  R=$(dl_last "$PRELUDE CREATE TABLE x.tmp(id INTEGER PRIMARY KEY); DROP TABLE x.tmp; SELECT count(*) FROM x.sqlite_master WHERE name='tmp';" "$M")
  want_eq "$cfg/D3_drop_table_attached" "$R" "0"

  # --- X1: INSERT INTO main SELECT FROM attached
  reset_dbs
  R=$(dl_last "$PRELUDE INSERT INTO t(id,v) SELECT id+100, v FROM x.u; SELECT count(*) FROM t WHERE id>100;" "$M")
  want_eq "$cfg/X1_insert_main_from_attached" "$R" "1"

  # --- X2: INSERT INTO attached SELECT FROM main
  reset_dbs
  R=$(dl_last "$PRELUDE INSERT INTO x.u(id,v) SELECT id+50, v FROM t WHERE id=10; SELECT count(*) FROM x.u WHERE id>50;" "$M")
  want_eq "$cfg/X2_insert_attached_from_main" "$R" "1"

  # --- X3: CREATE TABLE main.copy AS SELECT FROM attached
  reset_dbs
  R=$(dl_last "$PRELUDE CREATE TABLE main_copy AS SELECT * FROM x.u; SELECT count(*) FROM main_copy;" "$M")
  want_eq "$cfg/X3_ctas_from_attached" "$R" "1"

  # --- X4: clone-like — open empty main, copy everything from attached.
  # Skip when the attached side is :memory: — X4 opens a fresh connection
  # without the prelude that seeds a :memory: attached, so there is nothing
  # to clone from (the other X-cases seed it in-connection via $PRELUDE).
  reset_dbs
  if [ "$main_kind" != "dlmem" ] && [ "$attached_kind" != "mem" ]; then
    local M2="$TMP/${cfg}_clone.db"
    rm -f "$M2"
    R=$(dl_last "ATTACH '$ATT_PATH' AS src; CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO u SELECT * FROM src.u; SELECT count(*) FROM u;" "$M2")
    want_eq "$cfg/X4_clone_like_into_empty_main" "$R" "1"
  fi

  # --- T1: explicit BEGIN; write attached; COMMIT
  if [ "$main_kind" != "dlmem" ]; then
    rm -f "$M"; printf '%s\n' "$MAIN_SEED" | $DOLTLITE "$M" >/dev/null 2>&1
  fi
  if [ "$IS_SQL" = "1" ]; then mk_sq "$ATT_PATH" "$ATT_SEED"
  elif [ "$IS_MEM" != "1" ]; then mk_dl "$ATT_PATH" "$ATT_SEED"; fi
  R=$(dl_last "$PRELUDE BEGIN; INSERT INTO x.u VALUES(20,'t1'); COMMIT; SELECT v FROM x.u WHERE id=20;" "$M")
  want_eq "$cfg/T1_explicit_txn_attached" "$R" "t1"

  # --- T2: explicit BEGIN; write both; COMMIT
  if [ "$main_kind" != "dlmem" ]; then
    rm -f "$M"; printf '%s\n' "$MAIN_SEED" | $DOLTLITE "$M" >/dev/null 2>&1
  fi
  if [ "$IS_SQL" = "1" ]; then mk_sq "$ATT_PATH" "$ATT_SEED"
  elif [ "$IS_MEM" != "1" ]; then mk_dl "$ATT_PATH" "$ATT_SEED"; fi
  R=$(dl_last "$PRELUDE BEGIN; INSERT INTO t VALUES(30,'t2_m'); INSERT INTO x.u VALUES(30,'t2_a'); COMMIT; SELECT (SELECT v FROM t WHERE id=30) || '/' || (SELECT v FROM x.u WHERE id=30);" "$M")
  want_eq "$cfg/T2_explicit_txn_both_commit" "$R" "t2_m/t2_a"

  # --- T3: explicit BEGIN; write both; ROLLBACK
  if [ "$main_kind" != "dlmem" ]; then
    rm -f "$M"; printf '%s\n' "$MAIN_SEED" | $DOLTLITE "$M" >/dev/null 2>&1
  fi
  if [ "$IS_SQL" = "1" ]; then mk_sq "$ATT_PATH" "$ATT_SEED"
  elif [ "$IS_MEM" != "1" ]; then mk_dl "$ATT_PATH" "$ATT_SEED"; fi
  R=$(dl_last "$PRELUDE BEGIN; INSERT INTO t VALUES(40,'t3_m'); INSERT INTO x.u VALUES(40,'t3_a'); ROLLBACK; SELECT (SELECT count(*) FROM t WHERE id=40) || '/' || (SELECT count(*) FROM x.u WHERE id=40);" "$M")
  want_eq "$cfg/T3_explicit_txn_both_rollback" "$R" "0/0"

  # --- P1: durability across reopen
  # Only meaningful for file-based attached DBs.
  if [ "$attached_kind" != "mem" ] && [ "$main_kind" != "dlmem" ]; then
    rm -f "$M"; printf '%s\n' "$MAIN_SEED" | $DOLTLITE "$M" >/dev/null 2>&1
    if [ "$IS_SQL" = "1" ]; then mk_sq "$ATT_PATH" "$ATT_SEED"
    else mk_dl "$ATT_PATH" "$ATT_SEED"; fi
    # Write
    dl_all "$PRELUDE INSERT INTO t VALUES(50,'p1_m'); INSERT INTO x.u VALUES(50,'p1_a');" "$M" >/dev/null
    # Re-open each independently
    RM=$(dl_last "SELECT v FROM t WHERE id=50;" "$M")
    if [ "$IS_SQL" = "1" ]; then
      RA=$(sq_last "SELECT v FROM u WHERE id=50;" "$ATT_PATH")
    else
      RA=$(dl_last "SELECT v FROM u WHERE id=50;" "$ATT_PATH")
    fi
    want_eq "$cfg/P1_durable_main" "$RM" "p1_m"
    want_eq "$cfg/P1_durable_attached" "$RA" "p1_a"
  fi
}

echo "=== ATTACH write matrix ==="

# Always-runnable configs (no sqlite3 required).
run_config "dl_file__dl_file" "dl" "dl"
run_config "dl_file__mem"     "dl" "mem"
run_config "dlmem__dl_file"   "dlmem" "dl"
run_config "dlmem__mem"       "dlmem" "mem"

# Configs that need stock sqlite3.
if [ -z "$SQLITE3" ] || [ ! -x "$SQLITE3" ]; then
  echo ""
  echo "SKIP: stock sqlite3 not available; skipping mixed configs"
  SKIP=$((SKIP+1))
else
  run_config "dl_file__sq_file" "dl"    "sq"
  run_config "dlmem__sq_file"   "dlmem" "sq"
fi

echo ""
echo "============================="
echo "Results: $PASS passed, $FAIL failed, $SKIP skipped"
echo "============================="
if [ $FAIL -gt 0 ]; then
  echo -e "$ERRORS" | head -80
  exit 1
fi
