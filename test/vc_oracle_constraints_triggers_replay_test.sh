#!/bin/bash

set -u
set -o pipefail

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

translate_for_dolt() {
  sed -E 's/SELECT[[:space:]]+(dolt_[a-z_]+\()/CALL \1/g'
}

oracle() {
  local name="$1" setup="$2" query="$3" allow_empty="${4:-}"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(printf ".bail off\n%s\n.headers off\n.mode list\n%s\n" "$setup" "$query" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | tr -d '\r' \
           | grep '^R|' | sort)

  local dolt_setup dolt_query
  dolt_setup=$(echo "$setup" | translate_for_dolt)
  dolt_query=$(echo "$query" | translate_for_dolt)

  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      echo "$dolt_setup"
      echo "$dolt_query"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  local dt_out
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^R|' | sort)

  if [ "$allow_empty" = "EXPECT_EMPTY" ]; then
    vc_oracle_assert_match_allow_empty "$name" "$dl_out" "$dt_out"
  else
    vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
  fi
}

# replay_fail: both engines continue past errors (bail off / sql -c).
oracle_replay_fail() {
  local name="$1" setup="$2" replay="$3" query="$4"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(printf ".bail off\n%s\n%s\n.headers off\n.mode list\n%s\n" \
                  "$setup" "$replay" "$query" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | tr -d '\r' \
           | grep '^R|' | sort)

  local dolt_setup dolt_replay dolt_query
  dolt_setup=$(echo "$setup" | translate_for_dolt)
  dolt_replay=$(echo "$replay" | translate_for_dolt)
  dolt_query=$(echo "$query" | translate_for_dolt)

  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      echo "$dolt_setup"
      echo "$dolt_replay"
      echo "$dolt_query"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"
  local dt_out
  dt_out=$(tr -d '"\r' < "$dir/dt.raw" | grep '^R|' | sort)

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

# Trigger syntax: SQLite BEGIN...END vs MySQL FOR EACH ROW. Setup+query in one session.
oracle_triggers_dual() {
  local name="$1" dl_setup="$2" dt_setup="$3" query="$4" allow_empty="${5:-}"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(printf ".bail off\n%s\n.headers off\n.mode list\n%s\n" "$dl_setup" "$query" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep -v '^[0-9]*$' \
           | grep -v '^[0-9a-f]\{40\}$' \
           | grep -v '^$' \
           | grep -vi 'already up to date' \
           | grep -vi 'Fast-forward' \
           | tr -d '"' \
           | grep '^R|' \
           | tr -d '\r' | sort)

  local dolt_setup_translated dolt_query_translated
  dolt_setup_translated=$(vc_oracle_translate_for_dolt "$dt_setup")
  dolt_query_translated=$(vc_oracle_translate_for_dolt "$query")

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    {
      printf '%s\n' "$dolt_setup_translated"
      printf '%s\n' "$dolt_query_translated"
    } | "$DOLT" sql -c -r csv 2>"$dir/dt.err"
  )
  dt_out=$(echo "$dt_out" | tr -d '"' | grep '^R|' | tr -d '\r' | sort)

  if [ "$allow_empty" = "EXPECT_EMPTY" ]; then
    vc_oracle_assert_match_allow_empty "$name" "$dl_out" "$dt_out"
  else
    vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
  fi
}

echo "=== Version Control Oracle Tests: constraints and triggers through replay ==="
echo ""

echo "--- Group A: CHECK constraint + cherry-pick ---"

oracle "check_cherrypick_valid" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT CHECK(v>0));
INSERT INTO t VALUES(1,10);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,20);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_add');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT CONCAT('R|',id,'|',v) FROM t ORDER BY id;"

oracle_replay_fail "check_cherrypick_violation" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,-5);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_bad');
SELECT dolt_checkout('main');
CREATE TABLE t2(id INTEGER PRIMARY KEY, v INT CHECK(v>0));
INSERT INTO t2 SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t2 RENAME TO t;
SELECT dolt_add('-A'); SELECT dolt_commit('-m','main_check');
" "SELECT dolt_cherry_pick('feat');" \
  "SELECT CONCAT('R|',id,'|',v) FROM t ORDER BY id;"

echo ""
echo "--- Group B: UNIQUE constraint + cherry-pick ---"

oracle "unique_cherrypick_valid" "
CREATE TABLE t(id INTEGER PRIMARY KEY, u INT UNIQUE);
INSERT INTO t VALUES(1,10);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,20);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_add');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT CONCAT('R|',id,'|',u) FROM t ORDER BY id;"

oracle_replay_fail "unique_cherrypick_violation" "
CREATE TABLE t(id INTEGER PRIMARY KEY, u INT);
INSERT INTO t VALUES(1,10);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,10);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_dup');
SELECT dolt_checkout('main');
CREATE TABLE t2(id INTEGER PRIMARY KEY, u INT UNIQUE);
INSERT INTO t2 SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t2 RENAME TO t;
SELECT dolt_add('-A'); SELECT dolt_commit('-m','main_unique');
" "SELECT dolt_cherry_pick('feat');" \
  "SELECT CONCAT('R|',id,'|',u) FROM t ORDER BY id;"

echo ""
echo "--- Group C: FK constraint + cherry-pick ---"

oracle "fk_cherrypick_valid" "
CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, pid INT, FOREIGN KEY(pid) REFERENCES p(id));
INSERT INTO p VALUES(1,100),(2,200);
INSERT INTO c VALUES(1,1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO c VALUES(2,2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_child');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT CONCAT('R|',id,'|',pid) FROM c ORDER BY id;"

oracle_replay_fail "fk_cherrypick_orphan" "
CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT);
INSERT INTO p VALUES(1,100);
INSERT INTO c VALUES(1,100);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO c VALUES(2,200);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_orphan');
SELECT dolt_checkout('main');
CREATE TABLE c2(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY(u) REFERENCES p(u));
INSERT INTO c2 SELECT * FROM c;
DROP TABLE c;
ALTER TABLE c2 RENAME TO c;
SELECT dolt_add('-A'); SELECT dolt_commit('-m','main_fk');
" "SELECT dolt_cherry_pick('feat');" \
  "SELECT CONCAT('R|',id,'|',u) FROM c ORDER BY id;"

echo ""
echo "--- Group D: CHECK constraint + rebase ---"

oracle "check_rebase_valid" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT CHECK(v>0));
INSERT INTO t VALUES(1,10);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,20);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_add');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,30);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','main_advance');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('main');
" "SELECT CONCAT('R|',id,'|',v) FROM t ORDER BY id;"

oracle_replay_fail "check_rebase_violation" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES(1,10);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,-1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_bad');
SELECT dolt_checkout('main');
CREATE TABLE t2(id INTEGER PRIMARY KEY, v INT CHECK(v>0));
INSERT INTO t2 SELECT * FROM t;
DROP TABLE t;
ALTER TABLE t2 RENAME TO t;
SELECT dolt_add('-A'); SELECT dolt_commit('-m','main_check');
SELECT dolt_checkout('feat');
" "SELECT dolt_rebase('main');" \
  "SELECT dolt_checkout('feat');
SELECT CONCAT('R|',id,'|',v) FROM t ORDER BY id;"

echo ""
echo "--- Group E: FK constraint + rebase ---"

oracle "fk_rebase_valid" "
CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, pid INT, FOREIGN KEY(pid) REFERENCES p(id));
INSERT INTO p VALUES(1,100);
INSERT INTO c VALUES(1,1);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO p VALUES(2,200);
INSERT INTO c VALUES(2,2);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_add');
SELECT dolt_checkout('main');
INSERT INTO p VALUES(3,300);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','main_advance');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('main');
" "SELECT CONCAT('R|',id,'|',pid) FROM c ORDER BY id;"

oracle_replay_fail "fk_rebase_orphan" "
CREATE TABLE p(id INTEGER PRIMARY KEY, u INT UNIQUE);
CREATE TABLE c(id INTEGER PRIMARY KEY, u INT);
INSERT INTO p VALUES(1,100);
INSERT INTO c VALUES(1,100);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','init');
SELECT dolt_checkout('-b','feat');
INSERT INTO c VALUES(2,200);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_orphan');
SELECT dolt_checkout('main');
CREATE TABLE c2(id INTEGER PRIMARY KEY, u INT, FOREIGN KEY(u) REFERENCES p(u));
INSERT INTO c2 SELECT * FROM c;
DROP TABLE c;
ALTER TABLE c2 RENAME TO c;
SELECT dolt_add('-A'); SELECT dolt_commit('-m','main_fk');
SELECT dolt_checkout('feat');
" "SELECT dolt_rebase('main');" \
  "SELECT dolt_checkout('feat');
SELECT CONCAT('R|',id,'|',u) FROM c ORDER BY id;"

echo ""
echo "--- Group F: AFTER triggers fire based on commit diff, not replay DML ---"

oracle_triggers_dual "trigger_insert_log_travels_with_cherrypick" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE log(id INTEGER PRIMARY KEY);
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT OR IGNORE INTO log VALUES(new.id); END;
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'b');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_inserts');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v VARCHAR(16));
CREATE TABLE log(id INTEGER PRIMARY KEY);
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT IGNORE INTO log VALUES(new.id);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'b');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_inserts');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT CONCAT('R|',id) FROM log ORDER BY id;" "EXPECT_EMPTY"

oracle_triggers_dual "trigger_update_log_travels_with_cherrypick" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
CREATE TABLE log(id INTEGER PRIMARY KEY, delta INT);
CREATE TRIGGER t_au AFTER UPDATE ON t BEGIN INSERT INTO log VALUES(new.id, new.v - old.v); END;
INSERT INTO t VALUES(1,10),(2,20);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v=30 WHERE id=1;
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_update');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
CREATE TABLE log(id INTEGER PRIMARY KEY, delta INTEGER);
CREATE TRIGGER t_au AFTER UPDATE ON t FOR EACH ROW INSERT INTO log VALUES(new.id, new.v - old.v);
INSERT INTO t VALUES(1,10),(2,20);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
UPDATE t SET v=30 WHERE id=1;
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_update');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT CONCAT('R|',id,'|',IFNULL(delta,'')) FROM log ORDER BY id;" "EXPECT_EMPTY"

oracle_triggers_dual "trigger_delete_log_travels_with_cherrypick" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE log(id INTEGER PRIMARY KEY);
CREATE TRIGGER t_ad AFTER DELETE ON t BEGIN INSERT OR IGNORE INTO log VALUES(old.id); END;
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id>=2;
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_delete');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v VARCHAR(8));
CREATE TABLE log(id INTEGER PRIMARY KEY);
CREATE TRIGGER t_ad AFTER DELETE ON t FOR EACH ROW INSERT IGNORE INTO log VALUES(old.id);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
DELETE FROM t WHERE id>=2;
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_delete');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT CONCAT('R|',id) FROM log ORDER BY id;" "EXPECT_EMPTY"

echo ""
echo "--- Group G: trigger schema preserved through rebase, fires on subsequent DML ---"

oracle_triggers_dual "trigger_preserved_after_rebase_fires_on_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE log(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT OR IGNORE INTO log VALUES(new.id); END;
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_trigger');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_data');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(10,'main');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','main_advance');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('main');
INSERT INTO t VALUES(20,'after_rebase');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','post_rebase');
" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v VARCHAR(16));
CREATE TABLE log(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT IGNORE INTO log VALUES(new.id);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_trigger');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_data');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(10,'main');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','main_advance');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('main');
INSERT INTO t VALUES(20,'after_rebase');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','post_rebase');
" "SELECT CONCAT('R|',id) FROM log ORDER BY id;"

oracle_triggers_dual "trigger_rebase_multi_commit_post_insert" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE log(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT OR IGNORE INTO log VALUES(new.id); END;
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_trigger');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_c2');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_c3');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(10,'m');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','main_advance');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('main');
INSERT INTO t VALUES(30,'post');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','post_rebase');
" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v VARCHAR(8));
CREATE TABLE log(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT IGNORE INTO log VALUES(new.id);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_trigger');
INSERT INTO t VALUES(2,'b');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_c2');
INSERT INTO t VALUES(3,'c');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_c3');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(10,'m');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','main_advance');
SELECT dolt_checkout('feat');
SELECT dolt_rebase('main');
INSERT INTO t VALUES(30,'post');
SELECT dolt_add('-A'); SELECT dolt_commit('-m','post_rebase');
" "SELECT CONCAT('R|',id) FROM log ORDER BY id;"

echo ""
echo "--- Group H: triggers on composite PK tables through cherry-pick ---"

oracle_triggers_dual "trigger_composite_pk_cherrypick_fires_on_target_insert" "
CREATE TABLE t(a INTEGER, b TEXT, v INT, PRIMARY KEY(a, b));
CREATE TABLE log(a INTEGER, b TEXT, PRIMARY KEY(a, b));
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT OR IGNORE INTO log VALUES(new.a, new.b); END;
INSERT INTO t VALUES(1,'x',10);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'y',20);
INSERT INTO t VALUES(2,'z',30);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_inserts');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "
CREATE TABLE t(a INTEGER, b VARCHAR(4), v INTEGER, PRIMARY KEY(a, b));
CREATE TABLE log(a INTEGER, b VARCHAR(4), PRIMARY KEY(a, b));
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT IGNORE INTO log VALUES(new.a, new.b);
INSERT INTO t VALUES(1,'x',10);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
INSERT INTO t VALUES(2,'y',20);
INSERT INTO t VALUES(2,'z',30);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_inserts');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
" "SELECT CONCAT('R|',a,'|',b) FROM log ORDER BY a,b;" "EXPECT_EMPTY"

oracle_triggers_dual "trigger_composite_pk_post_merge_fires" "
CREATE TABLE t(a INTEGER, b TEXT, v INT, PRIMARY KEY(a, b));
CREATE TABLE log(a INTEGER, b TEXT, PRIMARY KEY(a, b));
INSERT INTO t VALUES(1,'x',10);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT OR IGNORE INTO log VALUES(new.a, new.b); END;
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_trigger');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
INSERT INTO t VALUES(2,'y',20);
INSERT INTO t VALUES(2,'z',30);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','post_merge');
" "
CREATE TABLE t(a INTEGER, b VARCHAR(4), v INTEGER, PRIMARY KEY(a, b));
CREATE TABLE log(a INTEGER, b VARCHAR(4), PRIMARY KEY(a, b));
INSERT INTO t VALUES(1,'x',10);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT IGNORE INTO log VALUES(new.a, new.b);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','feat_trigger');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
INSERT INTO t VALUES(2,'y',20);
INSERT INTO t VALUES(2,'z',30);
SELECT dolt_add('-A'); SELECT dolt_commit('-m','post_merge');
" "SELECT CONCAT('R|',a,'|',b) FROM log ORDER BY a,b;"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failures:$FAILED_NAMES"
  exit 1
fi
