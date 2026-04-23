#!/bin/bash
#
# Triggers + version control oracle tests (doltlite vs Dolt).
#
# SQLite triggers use BEGIN…END; bodies. MySQL (Dolt) uses single-
# statement "FOR EACH ROW <stmt>" bodies. The syntaxes are mutually
# exclusive, so a single SQL string can't run on both engines.
#
# The oracle_triggers_dual helper below takes separate doltlite and
# Dolt setup scripts and compares the final state of a shared query.
# Each test pair is written so the *semantics* are identical; only
# the trigger body syntax differs.
#
# Usage: bash vc_oracle_triggers_test.sh [path/to/doltlite] [path/to/dolt]
#

set -u
set -o pipefail

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

normalize() {
  tr -d '\r' | grep -v '^$' | sort
}

# oracle_triggers_dual NAME DOLTLITE_SETUP DOLT_SETUP QUERY
#   DOLTLITE_SETUP: SQLite-style setup (triggers use BEGIN…END;)
#   DOLT_SETUP:     MySQL-style setup (triggers use FOR EACH ROW <stmt>)
#                   vc_oracle_translate_for_dolt still rewrites the
#                   SELECT dolt_XXX(…) calls to CALL dolt_XXX(…) on
#                   the Dolt side.
#   QUERY:          identical on both engines (no trigger DDL in here).
oracle_triggers_dual() {
  local name="$1" dl_setup="$2" dt_setup="$3" query="$4"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode csv\n%s\n" "$dl_setup" "$query" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep -v '^[0-9]*$' \
           | grep -v '^[0-9a-f]\{40\}$' \
           | grep -v '^$' \
           | grep -vi 'already up to date' \
           | grep -vi 'Fast-forward' \
           | tr -d '"' \
           | normalize)

  local dolt_setup dolt_query
  dolt_setup=$(vc_oracle_translate_for_dolt "$dt_setup")
  dolt_query=$(vc_oracle_translate_for_dolt "$query")

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    printf "%s\n" "$dolt_setup" | "$DOLT" sql -c >/dev/null 2>"$dir/dt.err"
    printf "%s\n" "$dolt_query" | "$DOLT" sql -c -r csv 2>>"$dir/dt.err" \
      | tail -n +2 | tr -d '"'
  ) 2>/dev/null
  dt_out=$(echo "$dt_out" | normalize)

  if [ "$dl_out" = "$dt_out" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite:"; echo "$dl_out" | head -20 | sed 's/^/      /'
    echo "    dolt:"    ; echo "$dt_out" | head -20 | sed 's/^/      /'
  fi
}

echo "=== Triggers + VC Oracle Tests ==="
echo ""

# ═══════════════════════════════════════════════════════════════════
# Section 1: AFTER INSERT trigger, one-sided, merged
# ═══════════════════════════════════════════════════════════════════
echo "--- AFTER INSERT one side ---"

oracle_triggers_dual "after_insert_trigger_fires_on_feat_inserts" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE log(id INTEGER PRIMARY KEY, when_txt TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES(new.id, 'insert'); END;
INSERT INTO t VALUES(2,'feat_b');
INSERT INTO t VALUES(3,'feat_c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat with trigger and inserts');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE log(id INTEGER PRIMARY KEY, when_txt VARCHAR(16));
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(new.id, 'insert');
INSERT INTO t VALUES(2,'feat_b');
INSERT INTO t VALUES(3,'feat_c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat with trigger and inserts');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM log ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 2: Trigger created on feat, fires when main inserts after merge
# ═══════════════════════════════════════════════════════════════════
echo "--- trigger survives merge and fires post-merge ---"

oracle_triggers_dual "trigger_on_feat_fires_on_main_after_merge" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE log(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES(new.id, new.v); END;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat adds trigger');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
INSERT INTO t VALUES(2,'main_post');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main post-merge insert');
" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v VARCHAR(16));
CREATE TABLE log(id INTEGER PRIMARY KEY, v VARCHAR(16));
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(new.id, new.v);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat adds trigger');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
INSERT INTO t VALUES(2,'main_post');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main post-merge insert');
" "SELECT id, v FROM log ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 3: AFTER UPDATE trigger
# ═══════════════════════════════════════════════════════════════════
echo "--- AFTER UPDATE trigger ---"

oracle_triggers_dual "after_update_trigger_logs_old_and_new" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
CREATE TABLE log(id INTEGER PRIMARY KEY, diff INTEGER);
INSERT INTO t VALUES(1,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TRIGGER t_au AFTER UPDATE ON t BEGIN INSERT INTO log VALUES(new.id, new.v - old.v); END;
UPDATE t SET v=30 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat updates');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
CREATE TABLE log(id INTEGER PRIMARY KEY, diff INTEGER);
INSERT INTO t VALUES(1,10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TRIGGER t_au AFTER UPDATE ON t FOR EACH ROW INSERT INTO log VALUES(new.id, new.v - old.v);
UPDATE t SET v=30 WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat updates');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, diff FROM log ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 4: AFTER DELETE trigger
# ═══════════════════════════════════════════════════════════════════
echo "--- AFTER DELETE trigger ---"

oracle_triggers_dual "after_delete_trigger_logs_removed_rows" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE log(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TRIGGER t_ad AFTER DELETE ON t BEGIN INSERT INTO log VALUES(old.id); END;
DELETE FROM t WHERE id>=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat deletes + trigger');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v VARCHAR(8));
CREATE TABLE log(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TRIGGER t_ad AFTER DELETE ON t FOR EACH ROW INSERT INTO log VALUES(old.id);
DELETE FROM t WHERE id>=2;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat deletes + trigger');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id FROM log ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 5: Trigger + independent main-side DML merged
# ═══════════════════════════════════════════════════════════════════
echo "--- trigger + independent main DML ---"

oracle_triggers_dual "trigger_on_feat_main_inserts_independently" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE log(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES(new.id); END;
INSERT INTO t VALUES(2,'feat2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(10,'main_pre');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main pre-merge');
SELECT dolt_merge('feat');
INSERT INTO t VALUES(20,'main_post');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main post');
" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v VARCHAR(16));
CREATE TABLE log(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(new.id);
INSERT INTO t VALUES(2,'feat2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(10,'main_pre');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main pre-merge');
SELECT dolt_merge('feat');
INSERT INTO t VALUES(20,'main_post');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main post');
" "SELECT id FROM log ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 6: Trigger dropped on feat, merged
# ═══════════════════════════════════════════════════════════════════
echo "--- trigger dropped on feat ---"

oracle_triggers_dual "trigger_drop_on_feat_main_inserts_no_log_entries" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE log(id INTEGER PRIMARY KEY);
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES(new.id); END;
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base with trigger');
SELECT dolt_checkout('-b','feat');
DROP TRIGGER t_ai;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat drops trigger');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
INSERT INTO t VALUES(2,'post_merge_no_log');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main post');
" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v VARCHAR(16));
CREATE TABLE log(id INTEGER PRIMARY KEY);
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(new.id);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base with trigger');
SELECT dolt_checkout('-b','feat');
DROP TRIGGER t_ai;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat drops trigger');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
INSERT INTO t VALUES(2,'post_merge_no_log');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main post');
" "SELECT count(*) FROM log;"

# ═══════════════════════════════════════════════════════════════════
# Section 7: Trigger + cherry-pick
# ═══════════════════════════════════════════════════════════════════
echo "--- trigger + cherry-pick ---"

oracle_triggers_dual "cherry_pick_of_trigger_creation_commit" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE log(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES(new.id); END;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat adds trigger');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
INSERT INTO t VALUES(2,'post');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main post');
" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v VARCHAR(16));
CREATE TABLE log(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(new.id);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat adds trigger');
SELECT dolt_checkout('main');
SELECT dolt_cherry_pick('feat');
INSERT INTO t VALUES(2,'post');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main post');
" "SELECT id FROM log;"

# ═══════════════════════════════════════════════════════════════════
# Section 8: Trigger fires during a DELETE cascade-like chain
# ═══════════════════════════════════════════════════════════════════
echo "--- DELETE trigger on multi-row delete + merge ---"

oracle_triggers_dual "delete_trigger_counts_rows_removed" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp INTEGER);
CREATE TABLE log(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1,1),(2,1),(3,2),(4,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TRIGGER t_ad AFTER DELETE ON t BEGIN INSERT INTO log VALUES(old.id); END;
DELETE FROM t WHERE grp=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat deletes grp=1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp INTEGER);
CREATE TABLE log(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1,1),(2,1),(3,2),(4,1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TRIGGER t_ad AFTER DELETE ON t FOR EACH ROW INSERT INTO log VALUES(old.id);
DELETE FROM t WHERE grp=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat deletes grp=1');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT count(*) FROM log;"

# ═══════════════════════════════════════════════════════════════════
# Section 9: Two triggers on same table
# ═══════════════════════════════════════════════════════════════════
echo "--- two triggers on same table ---"

oracle_triggers_dual "insert_and_delete_triggers_both_log" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE log(id INTEGER PRIMARY KEY, kind TEXT);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES(new.id,'i'); END;
CREATE TRIGGER t_ad AFTER DELETE ON t BEGIN INSERT INTO log(id,kind) VALUES(old.id+100,'d'); END;
INSERT INTO t VALUES(2,'b');
DELETE FROM t WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v VARCHAR(8));
CREATE TABLE log(id INTEGER PRIMARY KEY, kind VARCHAR(4));
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(new.id,'i');
CREATE TRIGGER t_ad AFTER DELETE ON t FOR EACH ROW INSERT INTO log(id,kind) VALUES(old.id+100,'d');
INSERT INTO t VALUES(2,'b');
DELETE FROM t WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
" "SELECT id, kind FROM log ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Section 10: Trigger fires across reopen (persists)
# ═══════════════════════════════════════════════════════════════════
echo "--- trigger persists across commit/reopen-like sequence ---"

oracle_triggers_dual "trigger_still_active_after_merge_reset_flow" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE log(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES(new.id); END;
INSERT INTO t VALUES(2,'feat2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
INSERT INTO t VALUES(3,'after_merge1');
INSERT INTO t VALUES(4,'after_merge2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','more inserts');
" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v VARCHAR(16));
CREATE TABLE log(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1,'a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_checkout('-b','feat');
CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(new.id);
INSERT INTO t VALUES(2,'feat2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_merge('feat');
INSERT INTO t VALUES(3,'after_merge1');
INSERT INTO t VALUES(4,'after_merge2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','more inserts');
" "SELECT id FROM log ORDER BY id;"

# ═══════════════════════════════════════════════════════════════════
# Results
# ═══════════════════════════════════════════════════════════════════
echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failures:$FAILED_NAMES"
  exit 1
fi
