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

# Single-branch baseline: ordinary AUTOINCREMENT still produces 1,2,3.
oracle single_branch \
"CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);
INSERT INTO t(v) VALUES('a'),('b'),('c');
SELECT dolt_commit('-A','-m','init');" \
"SELECT id, v FROM t ORDER BY id"

# Cross-branch counter: feat allocates 3,4; back on main, next ids are 5,6 (not 3,4).
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

# Merge after cross-branch inserts: 6 distinct ids, no duplicate-pk conflict.
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

# After merge, the next insert continues past the merged max.
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

# Two tables share neither counter nor name; each advances independently.
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

echo
echo "vc_oracle_autoinc: $pass passed, $fail failed"
if [ "$fail" -gt 0 ]; then
  echo "FAILED:$FAILED_NAMES"
  exit 1
fi
