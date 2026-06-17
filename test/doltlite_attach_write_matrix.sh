#!/bin/bash

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

dl_last() { printf '%s\n' "$1" | $DOLTLITE "$2" 2>&1 | tail -1; }
dl_all()  { printf '%s\n' "$1" | $DOLTLITE "$2" 2>&1; }
sq_last() { printf '%s\n' "$1" | $SQLITE3 "$2" 2>&1 | tail -1; }

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

run_config() {
  local cfg="$1" main_kind="$2" attached_kind="$3"
  local M A IS_SQL=0 IS_MEM=0 ATT_PATH
  echo ""
  echo "=== config: $cfg ==="

  if [ "$main_kind" = "dlmem" ]; then
    M=":memory:"
  else
    M="$TMP/${cfg}_main.db"
    rm -f "$M"
  fi

  case "$attached_kind" in
    sq)  ATT_PATH="$TMP/${cfg}_att.db"; IS_SQL=1 ;;
    mem) ATT_PATH=":memory:"; IS_MEM=1 ;;
    dl)  ATT_PATH="$TMP/${cfg}_att.db" ;;
  esac
  rm -f "$ATT_PATH" 2>/dev/null || true

  local ATT_SEED="CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO u VALUES(1,'seed_a');"
  if [ "$IS_SQL" = "1" ]; then
    mk_sq "$ATT_PATH" "$ATT_SEED"
  elif [ "$IS_MEM" = "1" ]; then
    : # can't pre-seed :memory: — seed in the same connection
  else
    mk_dl "$ATT_PATH" "$ATT_SEED"
  fi

  local MAIN_SEED="CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(10,'seed_m');"
  if [ "$main_kind" = "dlmem" ]; then
    : # seeded in-line below
  else
    printf '%s\n' "$MAIN_SEED" | $DOLTLITE "$M" >/dev/null 2>&1
  fi

  local PRELUDE="ATTACH '$ATT_PATH' AS x;"
  if [ "$IS_MEM" = "1" ]; then
    PRELUDE="$PRELUDE $ATT_SEED"
    PRELUDE="ATTACH '$ATT_PATH' AS x; CREATE TABLE x.u(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO x.u VALUES(1,'seed_a');"
  fi
  if [ "$main_kind" = "dlmem" ]; then
    PRELUDE="$MAIN_SEED $PRELUDE"
  fi

  R=$(dl_last "$PRELUDE SELECT v FROM x.u WHERE id=1;" "$M")
  want_eq "$cfg/R1_read_attached" "$R" "seed_a"

  R=$(dl_last "$PRELUDE SELECT m.v||'+'||a.v FROM t m JOIN x.u a ON m.id=10 AND a.id=1;" "$M")
  want_eq "$cfg/R2_cross_db_join" "$R" "seed_m+seed_a"

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

  reset_dbs
  R=$(dl_last "$PRELUDE INSERT INTO x.u VALUES(2,'w1'); SELECT v FROM x.u WHERE id=2;" "$M")
  want_eq "$cfg/W1_autocommit_insert_attached" "$R" "w1"

  reset_dbs
  R=$(dl_last "$PRELUDE UPDATE x.u SET v='w2' WHERE id=1; SELECT v FROM x.u WHERE id=1;" "$M")
  want_eq "$cfg/W2_autocommit_update_attached" "$R" "w2"

  reset_dbs
  R=$(dl_last "$PRELUDE DELETE FROM x.u WHERE id=1; SELECT count(*) FROM x.u WHERE id=1;" "$M")
  want_eq "$cfg/W3_autocommit_delete_attached" "$R" "0"

  reset_dbs
  R=$(dl_last "$PRELUDE INSERT INTO t VALUES(11,'w4'); SELECT v FROM t WHERE id=11;" "$M")
  want_eq "$cfg/W4_autocommit_insert_main" "$R" "w4"

  reset_dbs
  R=$(dl_last "$PRELUDE CREATE TABLE x.newt(id INTEGER PRIMARY KEY); INSERT INTO x.newt VALUES(7); SELECT id FROM x.newt;" "$M")
  want_eq "$cfg/D1_create_table_attached" "$R" "7"

  reset_dbs
  R=$(dl_last "$PRELUDE CREATE INDEX x.idx_u_v ON u(v); SELECT name FROM x.sqlite_master WHERE type='index';" "$M")
  want_eq "$cfg/D2_create_index_attached" "$R" "idx_u_v"

  reset_dbs
  R=$(dl_last "$PRELUDE CREATE TABLE x.tmp(id INTEGER PRIMARY KEY); DROP TABLE x.tmp; SELECT count(*) FROM x.sqlite_master WHERE name='tmp';" "$M")
  want_eq "$cfg/D3_drop_table_attached" "$R" "0"

  reset_dbs
  R=$(dl_last "$PRELUDE INSERT INTO t(id,v) SELECT id+100, v FROM x.u; SELECT count(*) FROM t WHERE id>100;" "$M")
  want_eq "$cfg/X1_insert_main_from_attached" "$R" "1"

  reset_dbs
  R=$(dl_last "$PRELUDE INSERT INTO x.u(id,v) SELECT id+50, v FROM t WHERE id=10; SELECT count(*) FROM x.u WHERE id>50;" "$M")
  want_eq "$cfg/X2_insert_attached_from_main" "$R" "1"

  reset_dbs
  R=$(dl_last "$PRELUDE CREATE TABLE main_copy AS SELECT * FROM x.u; SELECT count(*) FROM main_copy;" "$M")
  want_eq "$cfg/X3_ctas_from_attached" "$R" "1"

  # X4 clone-like: skip a :memory: attached — its fresh connection isn't
  # seeded by $PRELUDE, so there's nothing to clone from.
  reset_dbs
  if [ "$main_kind" != "dlmem" ] && [ "$attached_kind" != "mem" ]; then
    local M2="$TMP/${cfg}_clone.db"
    rm -f "$M2"
    R=$(dl_last "ATTACH '$ATT_PATH' AS src; CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO u SELECT * FROM src.u; SELECT count(*) FROM u;" "$M2")
    want_eq "$cfg/X4_clone_like_into_empty_main" "$R" "1"
  fi

  if [ "$main_kind" != "dlmem" ]; then
    rm -f "$M"; printf '%s\n' "$MAIN_SEED" | $DOLTLITE "$M" >/dev/null 2>&1
  fi
  if [ "$IS_SQL" = "1" ]; then mk_sq "$ATT_PATH" "$ATT_SEED"
  elif [ "$IS_MEM" != "1" ]; then mk_dl "$ATT_PATH" "$ATT_SEED"; fi
  R=$(dl_last "$PRELUDE BEGIN; INSERT INTO x.u VALUES(20,'t1'); COMMIT; SELECT v FROM x.u WHERE id=20;" "$M")
  want_eq "$cfg/T1_explicit_txn_attached" "$R" "t1"

  if [ "$main_kind" != "dlmem" ]; then
    rm -f "$M"; printf '%s\n' "$MAIN_SEED" | $DOLTLITE "$M" >/dev/null 2>&1
  fi
  if [ "$IS_SQL" = "1" ]; then mk_sq "$ATT_PATH" "$ATT_SEED"
  elif [ "$IS_MEM" != "1" ]; then mk_dl "$ATT_PATH" "$ATT_SEED"; fi
  R=$(dl_last "$PRELUDE BEGIN; INSERT INTO t VALUES(30,'t2_m'); INSERT INTO x.u VALUES(30,'t2_a'); COMMIT; SELECT (SELECT v FROM t WHERE id=30) || '/' || (SELECT v FROM x.u WHERE id=30);" "$M")
  want_eq "$cfg/T2_explicit_txn_both_commit" "$R" "t2_m/t2_a"

  if [ "$main_kind" != "dlmem" ]; then
    rm -f "$M"; printf '%s\n' "$MAIN_SEED" | $DOLTLITE "$M" >/dev/null 2>&1
  fi
  if [ "$IS_SQL" = "1" ]; then mk_sq "$ATT_PATH" "$ATT_SEED"
  elif [ "$IS_MEM" != "1" ]; then mk_dl "$ATT_PATH" "$ATT_SEED"; fi
  R=$(dl_last "$PRELUDE BEGIN; INSERT INTO t VALUES(40,'t3_m'); INSERT INTO x.u VALUES(40,'t3_a'); ROLLBACK; SELECT (SELECT count(*) FROM t WHERE id=40) || '/' || (SELECT count(*) FROM x.u WHERE id=40);" "$M")
  want_eq "$cfg/T3_explicit_txn_both_rollback" "$R" "0/0"

  if [ "$attached_kind" != "mem" ] && [ "$main_kind" != "dlmem" ]; then
    rm -f "$M"; printf '%s\n' "$MAIN_SEED" | $DOLTLITE "$M" >/dev/null 2>&1
    if [ "$IS_SQL" = "1" ]; then mk_sq "$ATT_PATH" "$ATT_SEED"
    else mk_dl "$ATT_PATH" "$ATT_SEED"; fi
    dl_all "$PRELUDE INSERT INTO t VALUES(50,'p1_m'); INSERT INTO x.u VALUES(50,'p1_a');" "$M" >/dev/null
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

run_config "dl_file__dl_file" "dl" "dl"
run_config "dl_file__mem"     "dl" "mem"
run_config "dlmem__dl_file"   "dlmem" "dl"
run_config "dlmem__mem"       "dlmem" "mem"

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
