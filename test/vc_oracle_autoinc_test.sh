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

normalize() { tr -d '\r'; }

translate_autoinc_for_dolt() {
  printf '%s\n' "$1" | sed -E 's/AUTOINCREMENT/AUTO_INCREMENT/g'
}

oracle() {
  local name="$1" setup="$2" query="$3"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  printf '%s\n' "$setup" | "$DOLTLITE" "$dir/dl/db" >/dev/null 2>"$dir/dl.err"
  local dl_out
  dl_out=$(printf ".headers off\n.mode list\n.separator '\t'\n%s;\n" "$query" \
           | "$DOLTLITE" "$dir/dl/db" 2>>"$dir/dl.err" \
           | normalize)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  dolt_setup=$(translate_autoinc_for_dolt "$dolt_setup")

  local dt_out
  dt_out=$(
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    printf '%s\n' "$dolt_setup" | "$DOLT" sql >/dev/null 2>"$dir/dt.err"
    "$DOLT" sql -r csv -q "$query;" 2>>"$dir/dt.err" \
      | tail -n +2 \
      | tr ',' '\t' \
      | tr -d '"' \
      | normalize
  )

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

oracle single_branch \
"CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);
INSERT INTO t(v) VALUES('a'),('b'),('c');
SELECT dolt_commit('-A','-m','init');" \
"SELECT id, v FROM t ORDER BY id"

oracle cross_branch_then_main \
"CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);
INSERT INTO t(v) VALUES('a'),('b');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t(v) VALUES('feat3'),('feat4');
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t(v) VALUES('main3'),('main4');
SELECT dolt_commit('-A','-m','main');" \
"SELECT id, v FROM t ORDER BY id"

oracle merge_after_cross_branch \
"CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);
INSERT INTO t(v) VALUES('a'),('b');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t(v) VALUES('feat3'),('feat4');
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t(v) VALUES('main3'),('main4');
SELECT dolt_commit('-A','-m','main');
SELECT dolt_merge('feat');" \
"SELECT id, v FROM t ORDER BY id"

oracle insert_after_merge \
"CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);
INSERT INTO t(v) VALUES('a'),('b');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t(v) VALUES('feat3'),('feat4');
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t(v) VALUES('main3'),('main4');
SELECT dolt_commit('-A','-m','main');
SELECT dolt_merge('feat');
INSERT INTO t(v) VALUES('post');" \
"SELECT id, v FROM t ORDER BY id"

oracle drop_create_resets \
"CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);
INSERT INTO t(v) VALUES('a'),('b'),('c');
SELECT dolt_commit('-A','-m','init');
DROP TABLE t;
CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);
INSERT INTO t(v) VALUES('x'),('y');" \
"SELECT id, v FROM t ORDER BY id"

oracle rename_carries_counter \
"CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);
INSERT INTO t(v) VALUES('a'),('b'),('c');
SELECT dolt_commit('-A','-m','init');
ALTER TABLE t RENAME TO t2;
INSERT INTO t2(v) VALUES('d');" \
"SELECT id, v FROM t2 ORDER BY id"

oracle two_tables_independent \
"CREATE TABLE a(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);
INSERT INTO a(v) VALUES('a1'),('a2');
INSERT INTO b(v) VALUES('b1');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO a(v) VALUES('a3');
INSERT INTO b(v) VALUES('b2'),('b3');
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
INSERT INTO a(v) VALUES('a-post');
INSERT INTO b(v) VALUES('b-post');" \
"SELECT 'a' AS tbl, id, v FROM a UNION ALL SELECT 'b' AS tbl, id, v FROM b ORDER BY tbl, id"

oracle explicit_id_jumps_counter \
"CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);
INSERT INTO t(v) VALUES('a');
INSERT INTO t VALUES(100, 'big');
INSERT INTO t(v) VALUES('next');" \
"SELECT id, v FROM t ORDER BY id"

oracle explicit_id_across_branches \
"CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);
INSERT INTO t(v) VALUES('a');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(50, 'feat-big');
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
INSERT INTO t(v) VALUES('main-next');" \
"SELECT id, v FROM t ORDER BY id"

oracle delete_doesnt_reset_counter \
"CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);
INSERT INTO t(v) VALUES('a'),('b'),('c');
SELECT dolt_commit('-A','-m','init');
DELETE FROM t;
INSERT INTO t(v) VALUES('d');" \
"SELECT id, v FROM t ORDER BY id"

oracle branch_off_branch \
"CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);
INSERT INTO t(v) VALUES('a'),('b');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('f1');
SELECT dolt_checkout('f1');
INSERT INTO t(v) VALUES('f1a'),('f1b');
SELECT dolt_commit('-A','-m','f1');
SELECT dolt_branch('f2');
SELECT dolt_checkout('f2');
INSERT INTO t(v) VALUES('f2a');
SELECT dolt_commit('-A','-m','f2');
SELECT dolt_checkout('main');
INSERT INTO t(v) VALUES('m');" \
"SELECT id, v FROM t ORDER BY id"

oracle sequential_merges \
"CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);
INSERT INTO t(v) VALUES('a'),('b');
SELECT dolt_commit('-A','-m','init');
SELECT dolt_branch('f1');
SELECT dolt_checkout('f1');
INSERT INTO t(v) VALUES('f1a');
SELECT dolt_commit('-A','-m','f1');
SELECT dolt_checkout('main');
SELECT dolt_branch('f2');
SELECT dolt_checkout('f2');
INSERT INTO t(v) VALUES('f2a');
SELECT dolt_commit('-A','-m','f2');
SELECT dolt_checkout('main');
SELECT dolt_merge('f1');
SELECT dolt_merge('f2');
INSERT INTO t(v) VALUES('post');" \
"SELECT id, v FROM t ORDER BY id"

oracle reset_hard_keeps_counter \
"CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);
INSERT INTO t(v) VALUES('a'),('b');
SELECT dolt_commit('-A','-m','first');
INSERT INTO t(v) VALUES('c'),('d');
SELECT dolt_commit('-A','-m','second');
SELECT dolt_reset('--hard','HEAD~1');
INSERT INTO t(v) VALUES('new');" \
"SELECT id, v FROM t ORDER BY id"

echo
echo "vc_oracle_autoinc: $pass passed, $fail failed"
if [ "$fail" -gt 0 ]; then
  echo "FAILED:$FAILED_NAMES"
  exit 1
fi
