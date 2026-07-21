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

oracle() {
  local name="$1" setup="$2" allow_empty="${3:-}"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local q='SELECT name || char(9) || url || char(9) || fetch_specs || char(9) || params FROM dolt_remotes ORDER BY name'

  local dl_out
  dl_out=$(printf "%s\n.headers off\n.mode list\n.separator '\t'\n%s;\n" "$setup" "$q" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep -v '^[0-9]*$' \
           | grep -v '^[0-9a-f]\{40\}$' \
           | normalize)

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")

  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    echo "$dolt_setup" | "$DOLT" sql -c >/dev/null 2>"$dir/dt.err"
    "$DOLT" sql -r csv -q "SELECT concat(name, char(9), url, char(9), fetch_specs, char(9), params) FROM dolt_remotes ORDER BY name;" 2>>"$dir/dt.err"
  ) > "$dir/dt.raw"

  local dt_out
  dt_out=$(tail -n +2 "$dir/dt.raw" \
           | sed -E 's/^"(.*)"$/\1/' \
           | sed 's/""/"/g' \
           | normalize)

  if [ "$allow_empty" = "EXPECT_EMPTY" ]; then
    vc_oracle_assert_match_allow_empty "$name" "$dl_out" "$dt_out"
  else
    vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
  fi
}

oracle_error() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/${name}_err"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_rc
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup"
  dl_rc=$?

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  local dt_rc
  vc_oracle_run_dolt_script_for_error "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup"
  dt_rc=$?

  if vc_oracle_is_clean_error "$dl_rc" && vc_oracle_is_clean_error "$dt_rc"; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected both to error)"
    echo "    doltlite rc: $dl_rc"
    echo "    dolt rc:     $dt_rc"
  fi
}

oracle_savepoint_remote_poststate() {
  local name="$1" setup="$2"
  local dir="$TMPROOT/${name}_sp"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_rc dt_rc dl_v dl_remotes dt_v dt_remotes

  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup"
  dl_rc=$?
  dl_v=$(printf ".headers off\n.mode list\nSELECT v FROM t WHERE id=1;\n" \
         | "$DOLTLITE" "$dir/dl/db" 2>>"$dir/dl.err")
  dl_remotes=$(printf ".headers off\n.mode list\nSELECT coalesce(group_concat(name, ','), '') FROM dolt_remotes;\n" \
               | "$DOLTLITE" "$dir/dl/db" 2>>"$dir/dl.err")

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  vc_oracle_run_dolt_script_for_error "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup"
  dt_rc=$?
  dt_v=$(cd "$dir/dt" && "$DOLT" sql -r csv -q "SELECT v FROM t WHERE id=1;" 2>>"$dir/dt.err" | tail -n +2 | tr -d '"')
  dt_remotes=$(cd "$dir/dt" && "$DOLT" sql -r csv -q "SELECT coalesce(group_concat(name, ','), '') FROM dolt_remotes;" 2>>"$dir/dt.err" | tail -n +2 | tr -d '"')

  if vc_oracle_is_clean_error "$dl_rc" && vc_oracle_is_clean_error "$dt_rc" && [ "$dl_v" = "$dt_v" ] && [ "$dl_remotes" = "$dt_remotes" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite rc/v/remotes:"; { echo "$dl_rc"; echo "$dl_v"; echo "$dl_remotes"; } | sed 's/^/      /'
    echo "    dolt rc/v/remotes:"; { echo "$dt_rc"; echo "$dt_v"; echo "$dt_remotes"; } | sed 's/^/      /'
  fi
}

oracle_savepoint_clone_poststate() {
  local name="$1" setup="$2" query="$3"
  local dir="$TMPROOT/${name}_clone_sp"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_rc dt_rc dl_post dt_post

  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$setup"
  dl_rc=$?
  dl_post=$(printf ".headers off\n.mode list\n%s\n" "$query" \
            | "$DOLTLITE" "$dir/dl/db" 2>>"$dir/dl.err" \
            | tr -d '\r')

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  vc_oracle_run_dolt_script_for_error "$dir/dt" "$dir/dt.out" "$dir/dt.err" "$dolt_setup"
  dt_rc=$?
  dt_post=$(cd "$dir/dt" && "$DOLT" sql -r csv -q "$query" 2>>"$dir/dt.err" | tail -n +2 | tr -d '"\r')

  if vc_oracle_is_clean_error "$dl_rc" && vc_oracle_is_clean_error "$dt_rc" && [ "$dl_post" = "$dt_post" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite rc/post:"; { echo "$dl_rc"; echo "$dl_post"; } | sed 's/^/      /'
    echo "    dolt rc/post:"; { echo "$dt_rc"; echo "$dt_post"; } | sed 's/^/      /'
  fi
}

oracle_nested_pull_rollback_poststate() {
  local name="$1"
  local dir="$TMPROOT/${name}_pull_nested"
  local dl_remote_url="file://$dir/dl_remote.db"
  local dt_remote_dir="$dir/dt_remote"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_rows dl_log dt_rows dt_log

  cat >"$dir/dl_setup.sql" <<SQL
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_remote('add', 'origin', '$dl_remote_url');
SELECT dolt_push('origin', 'main');
SQL
  "$DOLTLITE" "$dir/dl/db" <"$dir/dl_setup.sql" >/dev/null 2>"$dir/dl_setup.err"

  cat >"$dir/dl_other.sql" <<SQL
SELECT dolt_clone('$dl_remote_url');
INSERT INTO t VALUES (2, 'other');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'other');
SELECT dolt_push('origin', 'main');
SQL
  "$DOLTLITE" "$dir/dl_other.db" <"$dir/dl_other.sql" >/dev/null 2>"$dir/dl_other.err"

  cat >"$dir/dl_pull.sql" <<SQL
BEGIN;
SAVEPOINT sp1;
SELECT dolt_pull('origin', 'main');
ROLLBACK TO sp1;
SQL
  vc_oracle_run_doltlite_script "$dir/dl/db" "$dir/dl.out" "$dir/dl.err" "$(cat "$dir/dl_pull.sql")"
  dl_rows=$(printf ".headers off\n.mode list\nSELECT count(*) FROM t;\n" | "$DOLTLITE" "$dir/dl/db" 2>>"$dir/dl.err")
  dl_log=$(printf ".headers off\n.mode list\nSELECT count(*)-1 FROM dolt_log;\n" | "$DOLTLITE" "$dir/dl/db" 2>>"$dir/dl.err")

  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    cat >setup.sql <<SQL
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
CALL dolt_add('-A');
CALL dolt_commit('-m', 'init');
CALL dolt_remote('add', 'origin', 'file://$dt_remote_dir');
CALL dolt_push('origin', 'main');
SQL
    mkdir -p "$dt_remote_dir"
    (
      cd "$dt_remote_dir" || exit 1
      "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    )
    "$DOLT" sql -c < setup.sql >/dev/null 2>"$dir/dt_setup.err"
  )
  (
    mkdir -p "$dir/dt_other"
    cd "$dir/dt_other" || exit 1
    "$DOLT" clone "file://$dt_remote_dir" clone_repo >/dev/null 2>&1 || exit 1
    cd clone_repo || exit 1
    cat >other.sql <<SQL
INSERT INTO t VALUES (2, 'other');
CALL dolt_add('-A');
CALL dolt_commit('-m', 'other');
CALL dolt_push('origin', 'main');
SQL
    "$DOLT" sql -c < other.sql >/dev/null 2>"$dir/dt_other.err"
  )
  (
    cd "$dir/dt" || exit 1
    cat >pull.sql <<SQL
BEGIN;
SAVEPOINT sp1;
CALL dolt_pull('origin', 'main');
ROLLBACK TO sp1;
SQL
    "$DOLT" sql -c < pull.sql >/dev/null 2>"$dir/dt.err" || true
    dt_rows=$("$DOLT" sql -r csv -q "SELECT count(*) FROM t;" 2>>"$dir/dt.err" | tail -n +2 | tr -d '"')
    dt_log=$("$DOLT" sql -r csv -q "SELECT count(*)-1 FROM dolt_log;" 2>>"$dir/dt.err" | tail -n +2 | tr -d '"')
    printf "%s\n%s\n" "$dt_rows" "$dt_log" >"$dir/dt.post"
  )
  dt_rows=$(sed -n '1p' "$dir/dt.post")
  dt_log=$(sed -n '2p' "$dir/dt.post")

  if [ "$dl_rows" = "$dt_rows" ] && [ "$dl_log" = "$dt_log" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name"
    echo "    doltlite rows/log:"; { echo "$dl_rows"; echo "$dl_log"; } | sed 's/^/      /'
    echo "    dolt rows/log:"; { echo "$dt_rows"; echo "$dt_log"; } | sed 's/^/      /'
  fi
}

oracle_fetch_checkout_tracking_poststate() {
  local name="$1"
  local dir="$TMPROOT/${name}_fetch_checkout"
  local dl_remote_url="file://$dir/dl_remote.db"
  local dt_remote_dir="$dir/dt_remote"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_post dt_post

  cat >"$dir/dl_setup.sql" <<SQL
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_remote('add', 'origin', '$dl_remote_url');
SELECT dolt_push('origin', 'main');
SELECT dolt_branch('branchA');
SELECT dolt_checkout('branchA');
INSERT INTO t VALUES (2, 'branchA');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'branchA');
SELECT dolt_push('origin', 'branchA');
SELECT dolt_checkout('main');
SQL
  "$DOLTLITE" "$dir/dl/db" <"$dir/dl_setup.sql" >/dev/null 2>"$dir/dl_setup.err"

  cat >"$dir/dl_clone.sql" <<SQL
SELECT dolt_clone('$dl_remote_url');
SQL
  "$DOLTLITE" "$dir/dl_clone.db" <"$dir/dl_clone.sql" >/dev/null 2>"$dir/dl_clone.err"

  cat >"$dir/dl_test.sql" <<SQL
SELECT dolt_fetch('origin', 'branchA');
SELECT dolt_checkout('-b', 'topic', 'origin/branchA');
SQL
  vc_oracle_run_doltlite_script "$dir/dl_clone.db" "$dir/dl.out" "$dir/dl.err" "$(cat "$dir/dl_test.sql")"
  dl_post=$(printf ".headers off\n.mode list\n.separator '\t'\nSELECT active_branch() || char(9) || count(*) FROM t;\n" \
            | "$DOLTLITE" "$dir/dl_clone.db" 2>>"$dir/dl.err" \
            | tr -d '\r')

  mkdir -p "$dt_remote_dir"
  (
    cd "$dt_remote_dir" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
  )
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    cat >setup.sql <<SQL
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
CALL dolt_add('-A');
CALL dolt_commit('-m', 'init');
CALL dolt_remote('add', 'origin', 'file://$dt_remote_dir');
CALL dolt_push('origin', 'main');
CALL dolt_branch('branchA');
CALL dolt_checkout('branchA');
INSERT INTO t VALUES (2, 'branchA');
CALL dolt_add('-A');
CALL dolt_commit('-m', 'branchA');
CALL dolt_push('origin', 'branchA');
CALL dolt_checkout('main');
SQL
    "$DOLT" sql -c < setup.sql >/dev/null 2>"$dir/dt_setup.err"
  )
  (
    cd "$dir" || exit 1
    "$DOLT" clone "file://$dt_remote_dir" dt_clone >/dev/null 2>&1
    cd dt_clone || exit 1
    cat >test.sql <<SQL
CALL dolt_fetch('origin', 'branchA');
CALL dolt_checkout('-b', 'topic', 'origin/branchA');
SQL
    "$DOLT" sql -c < test.sql >/dev/null 2>"$dir/dt.err"
    dt_post=$("$DOLT" sql -r csv -q "SELECT concat(active_branch(), char(9), count(*)) FROM t;" 2>>"$dir/dt.err" | tail -n +2 | tr -d '\"\r')
    printf "%s\n" "$dt_post" >"$dir/dt.post"
  )
  dt_post=$(cat "$dir/dt.post")

  vc_oracle_assert_match "$name" "$dl_post" "$dt_post"
}

oracle_fetch_ref_consumer_poststate() {
  local name="$1" dl_test_sql="$2" dt_test_sql="$3" query="$4"
  local dir="$TMPROOT/${name}_fetch_consumer"
  local dl_remote_url="file://$dir/dl_remote.db"
  local dt_remote_dir="$dir/dt_remote"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_post dt_post

  cat >"$dir/dl_setup.sql" <<SQL
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_remote('add', 'origin', '$dl_remote_url');
SELECT dolt_push('origin', 'main');
SELECT dolt_branch('branchA');
SELECT dolt_checkout('branchA');
INSERT INTO t VALUES (2, 'branchA');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'branchA');
SELECT dolt_push('origin', 'branchA');
SELECT dolt_checkout('main');
SQL
  "$DOLTLITE" "$dir/dl/db" <"$dir/dl_setup.sql" >/dev/null 2>"$dir/dl_setup.err"

  cat >"$dir/dl_clone.sql" <<SQL
SELECT dolt_clone('$dl_remote_url');
SQL
  "$DOLTLITE" "$dir/dl_clone.db" <"$dir/dl_clone.sql" >/dev/null 2>"$dir/dl_clone.err"

  vc_oracle_run_doltlite_script "$dir/dl_clone.db" "$dir/dl.out" "$dir/dl.err" "$dl_test_sql"
  dl_post=$(printf ".headers off\n.mode list\n%s\n" "$query" \
            | "$DOLTLITE" "$dir/dl_clone.db" 2>>"$dir/dl.err" \
            | tr -d '\r')

  mkdir -p "$dt_remote_dir"
  (
    cd "$dt_remote_dir" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
  )
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    cat >setup.sql <<SQL
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
CALL dolt_add('-A');
CALL dolt_commit('-m', 'init');
CALL dolt_remote('add', 'origin', 'file://$dt_remote_dir');
CALL dolt_push('origin', 'main');
CALL dolt_branch('branchA');
CALL dolt_checkout('branchA');
INSERT INTO t VALUES (2, 'branchA');
CALL dolt_add('-A');
CALL dolt_commit('-m', 'branchA');
CALL dolt_push('origin', 'branchA');
CALL dolt_checkout('main');
SQL
    "$DOLT" sql -c < setup.sql >/dev/null 2>"$dir/dt_setup.err"
  )
  (
    cd "$dir" || exit 1
    "$DOLT" clone "file://$dt_remote_dir" dt_clone >/dev/null 2>&1
    cd dt_clone || exit 1
    cat >test.sql <<SQL
$dt_test_sql
SQL
    "$DOLT" sql -c < test.sql >/dev/null 2>"$dir/dt.err" || true
    dt_post=$("$DOLT" sql -r csv -q "$query" 2>>"$dir/dt.err" | tail -n +2 | tr -d '\"\r')
    printf "%s\n" "$dt_post" >"$dir/dt.post"
  )
  dt_post=$(cat "$dir/dt.post")

  vc_oracle_assert_match "$name" "$dl_post" "$dt_post"
}

oracle_fetch_ref_consumer_reopen_poststate() {
  local name="$1" dl_test_sql="$2" dt_test_sql="$3" query="$4"
  local dir="$TMPROOT/${name}_fetch_consumer_reopen"
  local dl_remote_url="file://$dir/dl_remote.db"
  local dt_remote_dir="$dir/dt_remote"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_post dt_post

  cat >"$dir/dl_setup.sql" <<SQL
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_remote('add', 'origin', '$dl_remote_url');
SELECT dolt_push('origin', 'main');
SELECT dolt_branch('branchA');
SELECT dolt_checkout('branchA');
INSERT INTO t VALUES (2, 'branchA');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'branchA');
SELECT dolt_push('origin', 'branchA');
SELECT dolt_checkout('main');
SQL
  "$DOLTLITE" "$dir/dl/db" <"$dir/dl_setup.sql" >/dev/null 2>"$dir/dl_setup.err"
  "$DOLTLITE" "$dir/dl_clone.db" "SELECT dolt_clone('$dl_remote_url');" >/dev/null 2>"$dir/dl_clone.err"

  vc_oracle_run_doltlite_script "$dir/dl_clone.db" "$dir/dl.out" "$dir/dl.err" "$dl_test_sql"
  dl_post=$(printf ".headers off\n.mode list\n%s\n" "$query" \
            | "$DOLTLITE" "$dir/dl_clone.db" 2>>"$dir/dl.err" \
            | tr -d '\r')

  mkdir -p "$dt_remote_dir"
  (
    cd "$dt_remote_dir" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
  )
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    cat >setup.sql <<SQL
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
CALL dolt_add('-A');
CALL dolt_commit('-m', 'init');
CALL dolt_remote('add', 'origin', 'file://$dt_remote_dir');
CALL dolt_push('origin', 'main');
CALL dolt_branch('branchA');
CALL dolt_checkout('branchA');
INSERT INTO t VALUES (2, 'branchA');
CALL dolt_add('-A');
CALL dolt_commit('-m', 'branchA');
CALL dolt_push('origin', 'branchA');
CALL dolt_checkout('main');
SQL
    "$DOLT" sql -c < setup.sql >/dev/null 2>"$dir/dt_setup.err"
  )
  (
    cd "$dir" || exit 1
    "$DOLT" clone "file://$dt_remote_dir" dt_clone >/dev/null 2>&1
    cd dt_clone || exit 1
    cat >test.sql <<SQL
$dt_test_sql
SQL
    "$DOLT" sql -c < test.sql >/dev/null 2>"$dir/dt.err" || true
    dt_post=$("$DOLT" sql -r csv -q "$query" 2>>"$dir/dt.err" | tail -n +2 | tr -d '\"\r')
    printf "%s\n" "$dt_post" >"$dir/dt.post"
  )
  dt_post=$(cat "$dir/dt.post")

  vc_oracle_assert_match "$name" "$dl_post" "$dt_post"
}

oracle_fetch_ref_multitable_reopen_poststate() {
  local name="$1" dl_test_sql="$2" dt_test_sql="$3" query="$4"
  local dir="$TMPROOT/${name}_fetch_multitable_reopen"
  local dl_remote_url="file://$dir/dl_remote.db"
  local dt_remote_dir="$dir/dt_remote"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_post dt_post

  cat >"$dir/dl_setup.sql" <<SQL
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a VALUES (1, 'base_a');
INSERT INTO b VALUES (1, 'base_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_remote('add', 'origin', '$dl_remote_url');
SELECT dolt_push('origin', 'main');
SELECT dolt_branch('branchA');
SELECT dolt_checkout('branchA');
INSERT INTO a VALUES (2, 'branchA_a');
INSERT INTO b VALUES (2, 'branchA_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'branchA');
SELECT dolt_push('origin', 'branchA');
SELECT dolt_checkout('main');
SQL
  "$DOLTLITE" "$dir/dl/db" <"$dir/dl_setup.sql" >/dev/null 2>"$dir/dl_setup.err"
  "$DOLTLITE" "$dir/dl_clone.db" "SELECT dolt_clone('$dl_remote_url');" >/dev/null 2>"$dir/dl_clone.err"

  vc_oracle_run_doltlite_script "$dir/dl_clone.db" "$dir/dl.out" "$dir/dl.err" "$dl_test_sql"
  dl_post=$(printf ".headers off\n.mode list\n%s\n" "$query" \
            | "$DOLTLITE" "$dir/dl_clone.db" 2>>"$dir/dl.err" \
            | tr -d '\r')

  mkdir -p "$dt_remote_dir"
  (
    cd "$dt_remote_dir" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
  )
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    cat >setup.sql <<SQL
CREATE TABLE a(id INT PRIMARY KEY, v TEXT);
CREATE TABLE b(id INT PRIMARY KEY, v TEXT);
INSERT INTO a VALUES (1, 'base_a');
INSERT INTO b VALUES (1, 'base_b');
CALL dolt_add('-A');
CALL dolt_commit('-m', 'init');
CALL dolt_remote('add', 'origin', 'file://$dt_remote_dir');
CALL dolt_push('origin', 'main');
CALL dolt_branch('branchA');
CALL dolt_checkout('branchA');
INSERT INTO a VALUES (2, 'branchA_a');
INSERT INTO b VALUES (2, 'branchA_b');
CALL dolt_add('-A');
CALL dolt_commit('-m', 'branchA');
CALL dolt_push('origin', 'branchA');
CALL dolt_checkout('main');
SQL
    "$DOLT" sql -c < setup.sql >/dev/null 2>"$dir/dt_setup.err"
  )
  (
    cd "$dir" || exit 1
    "$DOLT" clone "file://$dt_remote_dir" dt_clone >/dev/null 2>&1
    cd dt_clone || exit 1
    cat >test.sql <<SQL
$dt_test_sql
SQL
    "$DOLT" sql -c < test.sql >/dev/null 2>"$dir/dt.err" || true
    dt_post=$("$DOLT" sql -r csv -q "$query" 2>>"$dir/dt.err" | tail -n +2 | tr -d '\"\r')
    printf "%s\n" "$dt_post" >"$dir/dt.post"
  )
  dt_post=$(cat "$dir/dt.post")

  vc_oracle_assert_match "$name" "$dl_post" "$dt_post"
}

oracle_fetch_refresh_ref_consumer_poststate() {
  local name="$1" dl_test_sql="$2" dt_test_sql="$3" query="$4"
  local dir="$TMPROOT/${name}_fetch_refresh"
  local dl_remote_url="file://$dir/dl_remote.db"
  local dt_remote_dir="$dir/dt_remote"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_post dt_post

  cat >"$dir/dl_setup.sql" <<SQL
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_remote('add', 'origin', '$dl_remote_url');
SELECT dolt_push('origin', 'main');
SELECT dolt_branch('branchA');
SELECT dolt_checkout('branchA');
INSERT INTO t VALUES (2, 'branchA');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'branchA1');
SELECT dolt_push('origin', 'branchA');
SELECT dolt_checkout('main');
SQL
  "$DOLTLITE" "$dir/dl/db" <"$dir/dl_setup.sql" >/dev/null 2>"$dir/dl_setup.err"
  "$DOLTLITE" "$dir/dl_clone.db" "SELECT dolt_clone('$dl_remote_url'); SELECT dolt_fetch('origin', 'branchA');" >/dev/null 2>"$dir/dl_clone.err"
  "$DOLTLITE" "$dir/dl/db" "SELECT dolt_checkout('branchA'); INSERT INTO t VALUES(3, 'branchA2'); SELECT dolt_add('-A'); SELECT dolt_commit('-m','branchA2'); SELECT dolt_push('origin','branchA');" >/dev/null 2>"$dir/dl_advance.err"

  vc_oracle_run_doltlite_script "$dir/dl_clone.db" "$dir/dl.out" "$dir/dl.err" "$dl_test_sql"
  dl_post=$(printf ".headers off\n.mode list\n%s\n" "$query" \
            | "$DOLTLITE" "$dir/dl_clone.db" 2>>"$dir/dl.err" \
            | tr -d '\r')

  mkdir -p "$dt_remote_dir"
  (
    cd "$dt_remote_dir" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
  )
  (
    cd "$dir/dt" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    cat >setup.sql <<SQL
CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base');
CALL dolt_add('-A');
CALL dolt_commit('-m', 'init');
CALL dolt_remote('add', 'origin', 'file://$dt_remote_dir');
CALL dolt_push('origin', 'main');
CALL dolt_branch('branchA');
CALL dolt_checkout('branchA');
INSERT INTO t VALUES (2, 'branchA');
CALL dolt_add('-A');
CALL dolt_commit('-m', 'branchA1');
CALL dolt_push('origin', 'branchA');
CALL dolt_checkout('main');
SQL
    "$DOLT" sql -c < setup.sql >/dev/null 2>"$dir/dt_setup.err"
  )
  (
    cd "$dir" || exit 1
    "$DOLT" clone "file://$dt_remote_dir" dt_clone >/dev/null 2>&1
    cd dt_clone || exit 1
    "$DOLT" sql -c -q "CALL dolt_fetch('origin', 'branchA');" >/dev/null 2>"$dir/dt_clone.err"
  )
  (
    mkdir -p "$dir/dt_adv"
    cd "$dir/dt_adv" || exit 1
    "$DOLT" clone "file://$dt_remote_dir" adv >/dev/null 2>&1
    cd adv || exit 1
    cat >advance.sql <<SQL
CALL dolt_checkout('branchA');
INSERT INTO t VALUES (3, 'branchA2');
CALL dolt_add('-A');
CALL dolt_commit('-m', 'branchA2');
CALL dolt_push('origin', 'branchA');
SQL
    "$DOLT" sql -c < advance.sql >/dev/null 2>"$dir/dt_advance.err"
  )
  (
    cd "$dir/dt_clone" || exit 1
    cat >test.sql <<SQL
$dt_test_sql
SQL
    "$DOLT" sql -c < test.sql >/dev/null 2>"$dir/dt.err" || true
    dt_post=$("$DOLT" sql -r csv -q "$query" 2>>"$dir/dt.err" | tail -n +2 | tr -d '\"\r')
    printf "%s\n" "$dt_post" >"$dir/dt.post"
  )
  dt_post=$(cat "$dir/dt.post")

  vc_oracle_assert_match "$name" "$dl_post" "$dt_post"
}

echo "=== Version Control Oracle Tests: dolt_remotes ==="
echo ""

echo "--- baseline ---"

oracle "no_remotes_on_fresh_repo" "
SELECT 1;
" "EXPECT_EMPTY"

echo "--- add ---"

oracle "add_single_remote" "
SELECT dolt_remote('add', 'origin', 'file:///tmp/oracle_origin');
"

oracle "add_two_remotes" "
SELECT dolt_remote('add', 'origin', 'file:///tmp/oracle_origin');
SELECT dolt_remote('add', 'upstream', 'file:///tmp/oracle_upstream');
"

oracle "add_remote_with_non_standard_name" "
SELECT dolt_remote('add', 'backup-1', 'file:///tmp/oracle_backup');
"

echo "--- remove ---"

oracle "remove_only_remote" "
SELECT dolt_remote('add', 'origin', 'file:///tmp/oracle_origin');
SELECT dolt_remote('remove', 'origin');
" "EXPECT_EMPTY"

oracle "remove_one_keep_others" "
SELECT dolt_remote('add', 'origin', 'file:///tmp/oracle_origin');
SELECT dolt_remote('add', 'upstream', 'file:///tmp/oracle_upstream');
SELECT dolt_remote('remove', 'origin');
"

oracle "add_remove_add_same_name" "
SELECT dolt_remote('add', 'origin', 'file:///tmp/oracle_origin');
SELECT dolt_remote('remove', 'origin');
SELECT dolt_remote('add', 'origin', 'file:///tmp/oracle_new');
"

echo "--- error paths ---"

oracle_error "add_duplicate_remote" "
SELECT dolt_remote('add', 'origin', 'file:///tmp/oracle_origin');
SELECT dolt_remote('add', 'origin', 'file:///tmp/oracle_other');
"

oracle_error "remove_nonexistent_remote" "
SELECT dolt_remote('remove', 'nonexistent');
"

oracle_error "unknown_action" "
SELECT dolt_remote('whatever', 'origin', 'file:///tmp/oracle_origin');
"

echo "--- savepoint parity ---"

oracle_savepoint_remote_poststate "remote_add_inside_savepoint_releases_savepoint" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SAVEPOINT sp1;
UPDATE t SET v='dirty' WHERE id=1;
SELECT dolt_remote('add', 'origin', 'file:///tmp/oracle_origin');
ROLLBACK TO sp1;
"

oracle_savepoint_remote_poststate "remote_remove_missing_inside_savepoint_invalidates" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SAVEPOINT sp1;
SELECT dolt_remote('remove', 'missing');
ROLLBACK TO sp1;
"

oracle_savepoint_remote_poststate "push_missing_remote_inside_savepoint_invalidates" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SAVEPOINT sp1;
SELECT dolt_push('missing', 'main');
ROLLBACK TO sp1;
"

oracle_savepoint_remote_poststate "fetch_missing_remote_inside_savepoint_invalidates" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SAVEPOINT sp1;
SELECT dolt_fetch('missing');
ROLLBACK TO sp1;
"

oracle_savepoint_remote_poststate "pull_missing_remote_inside_savepoint_invalidates" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'first');
SAVEPOINT sp1;
SELECT dolt_pull('missing', 'main');
ROLLBACK TO sp1;
"

oracle_savepoint_clone_poststate "clone_bad_url_inside_savepoint_invalidates" "
SAVEPOINT sp1;
SELECT dolt_clone('bogus://remote');
ROLLBACK TO sp1;
" "SELECT active_branch();"

oracle_nested_pull_rollback_poststate "pull_nested_savepoint_rollback_restores_state"
oracle_fetch_checkout_tracking_poststate "fetch_then_checkout_remote_tracking_branch"
oracle_fetch_ref_consumer_poststate \
  "fetch_then_bare_checkout_remote_tracking_branch" \
  "SELECT dolt_fetch('origin', 'branchA');
SELECT dolt_checkout('branchA');" \
  "CALL dolt_fetch('origin', 'branchA');
CALL dolt_checkout('branchA');" \
  "SELECT concat(active_branch(), char(9), (SELECT count(*) FROM dolt_branches WHERE name='branchA'), char(9), count(*), char(9), (SELECT count(*)-1 FROM dolt_log)) FROM t;"
oracle_fetch_ref_consumer_poststate \
  "fetch_then_branch_remote_tracking_ref" \
  "SELECT dolt_fetch('origin', 'branchA');
SELECT dolt_branch('topic', 'origin/branchA');" \
  "CALL dolt_fetch('origin', 'branchA');
CALL dolt_branch('topic', 'origin/branchA');" \
  "SELECT concat(active_branch(), char(9), (SELECT count(*) FROM dolt_branches WHERE name='topic'), char(9), count(*), char(9), (SELECT count(*)-1 FROM dolt_log)) FROM t;"
oracle_fetch_ref_consumer_poststate \
  "fetch_then_merge_remote_tracking_ref" \
  "SELECT dolt_fetch('origin', 'branchA');
SELECT dolt_merge('origin/branchA');" \
  "CALL dolt_fetch('origin', 'branchA');
CALL dolt_merge('origin/branchA');" \
  "SELECT concat(active_branch(), char(9), (SELECT count(*) FROM dolt_branches WHERE name='topic'), char(9), count(*), char(9), (SELECT count(*)-1 FROM dolt_log)) FROM t;"
oracle_fetch_ref_consumer_poststate \
  "fetch_then_branch_remote_tracking_ref_begin_rollback" \
  "SELECT dolt_fetch('origin', 'branchA');
BEGIN;
SELECT dolt_branch('topic', 'origin/branchA');
ROLLBACK;" \
  "CALL dolt_fetch('origin', 'branchA');
BEGIN;
CALL dolt_branch('topic', 'origin/branchA');
ROLLBACK;" \
  "SELECT concat(active_branch(), char(9), (SELECT count(*) FROM dolt_branches WHERE name='topic'), char(9), count(*), char(9), (SELECT count(*)-1 FROM dolt_log)) FROM t;"
oracle_fetch_ref_consumer_poststate \
  "fetch_then_merge_remote_tracking_ref_begin_rollback" \
  "SELECT dolt_fetch('origin', 'branchA');
BEGIN;
SELECT dolt_merge('origin/branchA');
ROLLBACK;" \
  "CALL dolt_fetch('origin', 'branchA');
BEGIN;
CALL dolt_merge('origin/branchA');
ROLLBACK;" \
  "SELECT concat(active_branch(), char(9), (SELECT count(*) FROM dolt_branches WHERE name='topic'), char(9), count(*), char(9), (SELECT count(*)-1 FROM dolt_log)) FROM t;"
oracle_fetch_ref_consumer_poststate \
  "fetch_then_branch_remote_tracking_ref_savepoint_invalidates" \
  "SELECT dolt_fetch('origin', 'branchA');
SAVEPOINT sp1;
SELECT dolt_branch('topic', 'origin/branchA');
ROLLBACK TO sp1;" \
  "CALL dolt_fetch('origin', 'branchA');
SAVEPOINT sp1;
CALL dolt_branch('topic', 'origin/branchA');
ROLLBACK TO sp1;" \
  "SELECT concat(active_branch(), char(9), (SELECT count(*) FROM dolt_branches WHERE name='topic'), char(9), count(*), char(9), (SELECT count(*)-1 FROM dolt_log)) FROM t;"
oracle_fetch_ref_consumer_poststate \
  "fetch_then_merge_remote_tracking_ref_savepoint_invalidates" \
  "SELECT dolt_fetch('origin', 'branchA');
SAVEPOINT sp1;
SELECT dolt_merge('origin/branchA');
ROLLBACK TO sp1;" \
  "CALL dolt_fetch('origin', 'branchA');
SAVEPOINT sp1;
CALL dolt_merge('origin/branchA');
ROLLBACK TO sp1;" \
  "SELECT concat(active_branch(), char(9), (SELECT count(*) FROM dolt_branches WHERE name='topic'), char(9), count(*), char(9), (SELECT count(*)-1 FROM dolt_log)) FROM t;"
oracle_fetch_ref_consumer_poststate \
  "fetch_then_rebase_remote_tracking_ref_errors_cleanly" \
  "SELECT dolt_fetch('origin', 'branchA');
SELECT dolt_rebase('origin/branchA');" \
  "CALL dolt_fetch('origin', 'branchA');
CALL dolt_rebase('origin/branchA');" \
  "SELECT concat(active_branch(), char(9), (SELECT count(*) FROM dolt_branches WHERE name='topic'), char(9), count(*), char(9), (SELECT count(*)-1 FROM dolt_log)) FROM t;"
oracle_fetch_ref_consumer_poststate \
  "fetch_then_branch_full_remote_ref" \
  "SELECT dolt_fetch('origin', 'branchA');
SELECT dolt_branch('topic', 'refs/remotes/origin/branchA');" \
  "CALL dolt_fetch('origin', 'branchA');
CALL dolt_branch('topic', 'refs/remotes/origin/branchA');" \
  "SELECT concat(active_branch(), char(9), (SELECT count(*) FROM dolt_branches WHERE name='topic'), char(9), count(*), char(9), (SELECT count(*)-1 FROM dolt_log)) FROM t;"
oracle_fetch_ref_consumer_poststate \
  "fetch_then_merge_full_remote_ref" \
  "SELECT dolt_fetch('origin', 'branchA');
SELECT dolt_merge('refs/remotes/origin/branchA');" \
  "CALL dolt_fetch('origin', 'branchA');
CALL dolt_merge('refs/remotes/origin/branchA');" \
  "SELECT concat(active_branch(), char(9), (SELECT count(*) FROM dolt_branches WHERE name='topic'), char(9), count(*), char(9), (SELECT count(*)-1 FROM dolt_log)) FROM t;"
oracle_fetch_ref_consumer_poststate \
  "fetch_then_checkout_branch_full_remote_ref" \
  "SELECT dolt_fetch('origin', 'branchA');
SELECT dolt_checkout('-b', 'topic', 'refs/remotes/origin/branchA');" \
  "CALL dolt_fetch('origin', 'branchA');
CALL dolt_checkout('-b', 'topic', 'refs/remotes/origin/branchA');" \
  "SELECT concat(active_branch(), char(9), (SELECT count(*) FROM dolt_branches WHERE name='topic'), char(9), count(*), char(9), (SELECT count(*)-1 FROM dolt_log)) FROM t;"
oracle_fetch_ref_consumer_reopen_poststate \
  "fetch_then_checkout_table_remote_tracking_ref_reopens" \
  "SELECT dolt_fetch('origin', 'branchA');
SELECT dolt_checkout('origin/branchA', 't');" \
  "CALL dolt_fetch('origin', 'branchA');
CALL dolt_checkout('origin/branchA', 't');" \
  "SELECT concat(active_branch(), char(9), count(*), char(9), (SELECT count(*)-1 FROM dolt_log)) FROM t;"
oracle_fetch_ref_multitable_reopen_poststate \
  "fetch_then_checkout_multiple_tables_remote_tracking_ref_reopens" \
  "SELECT dolt_fetch('origin', 'branchA');
SELECT dolt_checkout('origin/branchA', 'a', 'b');" \
  "CALL dolt_fetch('origin', 'branchA');
CALL dolt_checkout('origin/branchA', 'a', 'b');" \
  "SELECT concat(active_branch(), char(9), (SELECT count(*) FROM a), char(9), (SELECT count(*) FROM b), char(9), (SELECT count(*)-1 FROM dolt_log));"
oracle_fetch_refresh_ref_consumer_poststate \
  "fetch_refresh_then_branch_tracking_ref_then_checkout" \
  "SELECT dolt_fetch('origin', 'branchA');
SELECT dolt_branch('topic', 'origin/branchA');
SELECT dolt_checkout('topic');" \
  "CALL dolt_fetch('origin', 'branchA');
CALL dolt_branch('topic', 'origin/branchA');
CALL dolt_checkout('topic');" \
  "SELECT concat(active_branch(), char(9), count(*), char(9), (SELECT count(*)-1 FROM dolt_log)) FROM t;"
oracle_fetch_refresh_ref_consumer_poststate \
  "fetch_refresh_then_merge_tracking_ref" \
  "SELECT dolt_fetch('origin', 'branchA');
SELECT dolt_merge('origin/branchA');" \
  "CALL dolt_fetch('origin', 'branchA');
CALL dolt_merge('origin/branchA');" \
  "SELECT concat(active_branch(), char(9), count(*), char(9), (SELECT count(*)-1 FROM dolt_log)) FROM t;"
oracle_fetch_ref_consumer_poststate \
  "fetch_branch_checkout_sequence" \
  "SELECT dolt_fetch('origin', 'branchA');
SELECT dolt_branch('topic', 'origin/branchA');
SELECT dolt_checkout('topic');" \
  "CALL dolt_fetch('origin', 'branchA');
CALL dolt_branch('topic', 'origin/branchA');
CALL dolt_checkout('topic');" \
  "SELECT concat(active_branch(), char(9), count(*), char(9), (SELECT count(*)-1 FROM dolt_log)) FROM t;"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
