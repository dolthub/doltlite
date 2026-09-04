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

run_dl() {
  local dbspec="$1" query="$2"
  printf ".headers off\n.mode list\n%s\n" "$query" \
    | "$DOLTLITE" "$dbspec"
}

run_dt() {
  local repo="$1" branch="$2" query="$3"
  local parent repo_name
  parent=$(dirname "$repo")
  repo_name=$(basename "$repo")
  ( cd "$parent" && printf "%s\n" "$query" | "$DOLT" --use-db "$repo_name/$branch" sql -r csv -c ) \
    | tail -n +2 | tr -d '"' | tr -d '\r'
}

setup_pair() {
  local dir="$1"
  mkdir -p "$dir/dl" "$dir/dt"

  cat >"$dir/setup_dl.sql" <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'init');
SELECT dolt_tag('v1');
SELECT dolt_branch('side');
SELECT dolt_checkout('side');
UPDATE t SET v='side' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'side');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'main2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main2');
SQL
  "$DOLTLITE" "$dir/dl/db.sqlite" <"$dir/setup_dl.sql" >/dev/null 2>"$dir/dl.err"

  cat >"$dir/setup_dt.sql" <<'SQL'
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'main');
CALL dolt_commit('-Am', 'init');
CALL dolt_tag('v1');
CALL dolt_branch('side');
CALL dolt_checkout('side');
UPDATE t SET v='side' WHERE id=1;
CALL dolt_commit('-Am', 'side');
CALL dolt_checkout('main');
INSERT INTO t VALUES(2,'main2');
CALL dolt_commit('-Am', 'main2');
SQL
  (
    cd "$dir/dt" &&
    "$DOLT" config --global --add user.name "CI" >/dev/null &&
    "$DOLT" config --global --add user.email "ci@example.com" >/dev/null &&
    "$DOLT" init >/dev/null &&
    "$DOLT" sql <"$dir/setup_dt.sql" >/dev/null 2>"$dir/dt.err"
  )
}

oracle() {
  local name="$1" dbspec="$2" branch="$3"
  local dir="$TMPROOT/$name"
  local q="SELECT active_branch() AS value UNION ALL SELECT v FROM t WHERE id=1;"
  local dl_out dt_out

  setup_pair "$dir"

  dl_out=$(run_dl "$dbspec" "$q" 2>>"$dir/dl.err" | tr -d '\r')
  dt_out=$(run_dt "$dir/dt" "$branch" "$q" 2>>"$dir/dt.err")

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

oracle_with_query() {
  local name="$1" dbspec="$2" branch="$3" query="$4"
  local dir="$TMPROOT/$name"
  local dl_out dt_out

  setup_pair "$dir"

  dl_out=$(run_dl "$dbspec" "$query" 2>>"$dir/dl.err" | tr -d '\r')
  dt_out=$(run_dt "$dir/dt" "$branch" "$query" 2>>"$dir/dt.err")

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

oracle_with_mutation() {
  local name="$1" setup_extra="$2" dbspec="$3" branch="$4" query="$5"
  local dir="$TMPROOT/$name"
  local dl_out dt_out

  setup_pair "$dir"
  if [ -n "$setup_extra" ]; then
    run_dl "$dir/dl/db.sqlite" "$setup_extra" >/dev/null 2>>"$dir/dl.err"
    (
      cd "$dir/dt" || exit 1
      printf "%s\n" "$(printf "%s" "$setup_extra" | sed "s/SELECT dolt_/CALL dolt_/g")" | "$DOLT" sql -c >/dev/null 2>>"$dir/dt.err"
    )
  fi

  dl_out=$(run_dl "$dbspec" "$query" 2>>"$dir/dl.err" | tr -d '\r')
  dt_out=$(run_dt "$dir/dt" "$branch" "$query" 2>>"$dir/dt.err")

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

oracle_error() {
  local name="$1" setup_extra="$2" dbspec="$3" branch="$4" query="$5"
  local dir="$TMPROOT/$name"
  local dl_rc dt_rc parent repo_name

  setup_pair "$dir"
  if [ -n "$setup_extra" ]; then
    run_dl "$dir/dl/db.sqlite" "$setup_extra" >/dev/null 2>>"$dir/dl.err"
    (
      cd "$dir/dt" || exit 1
      printf "%s\n" "$(printf "%s" "$setup_extra" | sed "s/SELECT dolt_/CALL dolt_/g")" | "$DOLT" sql -c >/dev/null 2>>"$dir/dt.err"
    )
  fi

  run_dl "$dbspec" "$query" >/dev/null 2>>"$dir/dl.err"
  dl_rc=$?
  parent=$(dirname "$dir/dt")
  repo_name=$(basename "$dir/dt")
  (
    cd "$parent" || exit 1
    printf "%s\n" "$query" \
      | "$DOLT" --use-db "$repo_name/$branch" sql \
          >"$dir/dt.out" 2>>"$dir/dt.err"
  )
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

oracle_hash_revision() {
  local name="$1"
  local dir="$TMPROOT/$name"
  local dl_hash dt_hash dl_out dt_out
  local query="SELECT IFNULL(active_branch(),'NULL') AS value UNION ALL SELECT CAST(COUNT(*) AS CHAR) FROM t;"

  setup_pair "$dir"
  dl_hash=$(run_dl "$dir/dl/db.sqlite" "SELECT commit_hash FROM dolt_log WHERE message='init';")
  dt_hash=$(
    cd "$dir/dt" || exit 1
    "$DOLT" sql -r csv -q "SELECT commit_hash FROM dolt_log WHERE message='init';" \
      | tail -n +2 | tr -d '"\r'
  )
  dl_out=$(run_dl "$dir/dl/db.sqlite/$dl_hash" "$query" 2>>"$dir/dl.err" | tr -d '\r')
  dt_out=$(run_dt "$dir/dt" "$dt_hash" "$query" 2>>"$dir/dt.err")
  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

oracle_reattach() {
  local name="$1"
  local dir="$TMPROOT/$name"
  local dl_out dt_out

  setup_pair "$dir"
  dl_out=$(run_dl "$dir/dl/db.sqlite/v1" "
SELECT 'Q0:' || IFNULL(active_branch(),'NULL') || ':' || COUNT(*) FROM t;
SELECT dolt_checkout('main');
SELECT 'Q1:' || active_branch() || ':' || COUNT(*) FROM t;
INSERT INTO t VALUES(3,'reattached');
SELECT 'Q2:' || COUNT(*) FROM t;
" 2>>"$dir/dl.err" | tr -d '\r' | awk '/^Q[0-9]:/')
  dt_out=$(run_dt "$dir/dt" "v1" "
SELECT CONCAT('Q0:',IFNULL(active_branch(),'NULL'),':',COUNT(*)) FROM t;
CALL dolt_checkout('main');
SELECT CONCAT('Q1:',active_branch(),':',COUNT(*)) FROM t;
INSERT INTO t VALUES(3,'reattached');
SELECT CONCAT('Q2:',COUNT(*)) FROM t;
" 2>>"$dir/dt.err" | awk '/^Q[0-9]:/')
  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

oracle_detached_command_error() {
  local name="$1" dl_query="$2" dt_query="$3"
  local dir="$TMPROOT/$name"
  local parent repo_name dl_rc dt_rc

  setup_pair "$dir"
  run_dl "$dir/dl/db.sqlite/v1" "$dl_query" \
    >"$dir/dl.out" 2>"$dir/dl.err"
  dl_rc=$?
  parent=$(dirname "$dir/dt")
  repo_name=$(basename "$dir/dt")
  (
    cd "$parent" || exit 1
    printf "%s\n" "$dt_query" \
      | "$DOLT" --use-db "$repo_name/v1" sql \
          >"$dir/dt.out" 2>"$dir/dt.err"
  )
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

oracle_detached_command_success() {
  local name="$1" dl_query="$2" dt_query="$3"
  local dir="$TMPROOT/$name"
  local parent repo_name dl_rc dt_rc

  setup_pair "$dir"
  run_dl "$dir/dl/db.sqlite/v1" "$dl_query" \
    >"$dir/dl.out" 2>"$dir/dl.err"
  dl_rc=$?
  parent=$(dirname "$dir/dt")
  repo_name=$(basename "$dir/dt")
  (
    cd "$parent" || exit 1
    printf "%s\n" "$dt_query" \
      | "$DOLT" --use-db "$repo_name/v1" sql -c \
          >"$dir/dt.out" 2>"$dir/dt.err"
  )
  dt_rc=$?

  if [ "$dl_rc" -eq 0 ] && [ "$dt_rc" -eq 0 ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected both to succeed)"
    echo "    doltlite rc: $dl_rc"
    echo "    dolt rc:     $dt_rc"
  fi
}

oracle_detached_pull_error() {
  local name="$1"
  local dir="$TMPROOT/$name"
  local parent repo_name dl_rc dt_rc dl_out dt_out

  setup_pair "$dir"
  run_dl "$dir/dl/remote.sqlite" \
    "SELECT dolt_clone('file://$dir/dl/db.sqlite');" >/dev/null
  run_dl "$dir/dl/db.sqlite" \
    "SELECT dolt_remote('add','origin','file://$dir/dl/remote.sqlite');" \
    >/dev/null
  (
    cd "$dir" || exit 1
    "$DOLT" clone "file://$dir/dt" dt_remote >/dev/null 2>&1
    cd "$dir/dt" || exit 1
    printf "%s\n" \
      "CALL dolt_remote('add','origin','file://$dir/dt_remote');" \
      | "$DOLT" sql -c >/dev/null
  )

  run_dl "$dir/dl/db.sqlite/v1" "SELECT dolt_pull('origin','main');" \
    >"$dir/dl.out" 2>"$dir/dl.err"
  dl_rc=$?
  parent=$(dirname "$dir/dt")
  repo_name=$(basename "$dir/dt")
  (
    cd "$parent" || exit 1
    printf "%s\n" "CALL dolt_pull('origin','main');" \
      | "$DOLT" --use-db "$repo_name/v1" sql \
          >"$dir/dt.out" 2>"$dir/dt.err"
  )
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

  dl_out=$(run_dl "$dir/dl/db.sqlite/v1" \
    "SELECT IFNULL(active_branch(),'NULL') || ':' || COUNT(*) FROM t;" \
    2>>"$dir/dl.err" | tr -d '\r')
  dt_out=$(run_dt "$dir/dt" "v1" \
    "SELECT CONCAT(IFNULL(active_branch(),'NULL'),':',COUNT(*)) FROM t;" \
    2>>"$dir/dt.err")
  vc_oracle_assert_match "${name}_preserves_snapshot" "$dl_out" "$dt_out"
}

echo "=== Oracle Tests: Connection Branch Selection ==="
echo ""

oracle "default_open_uses_main" "$TMPROOT/default_open_uses_main/dl/db.sqlite" "main"
oracle "at_branch_selects_branch" "$TMPROOT/at_branch_selects_branch/dl/db.sqlite@side" "side"
oracle "slash_branch_selects_branch" "$TMPROOT/slash_branch_selects_branch/dl/db.sqlite/side" "side"

oracle_with_mutation "renamed_branch_open_selects_branch" \
  "SELECT dolt_branch('-m','side','renamed');" \
  "$TMPROOT/renamed_branch_open_selects_branch/dl/db.sqlite@renamed" \
  "renamed" \
  "SELECT active_branch() AS value UNION ALL SELECT v FROM t WHERE id=1;"

oracle_with_mutation "copied_branch_open_selects_branch" \
  "SELECT dolt_branch('-c','side','copy');" \
  "$TMPROOT/copied_branch_open_selects_branch/dl/db.sqlite/copy" \
  "copy" \
  "SELECT active_branch() AS value UNION ALL SELECT v FROM t WHERE id=1;"

echo "--- detached revisions ---"

oracle_with_query "tag_open_is_detached" \
  "$TMPROOT/tag_open_is_detached/dl/db.sqlite/v1" \
  "v1" \
  "SELECT IFNULL(active_branch(),'NULL') AS value UNION ALL SELECT CAST(COUNT(*) AS CHAR) FROM t;"

oracle_with_query "at_tag_open_is_detached" \
  "$TMPROOT/at_tag_open_is_detached/dl/db.sqlite@v1" \
  "v1" \
  "SELECT IFNULL(active_branch(),'NULL') AS value UNION ALL SELECT CAST(COUNT(*) AS CHAR) FROM t;"

oracle_with_query "branch_ancestor_is_detached" \
  "$TMPROOT/branch_ancestor_is_detached/dl/db.sqlite/main~1" \
  "main~1" \
  "SELECT IFNULL(active_branch(),'NULL') AS value UNION ALL SELECT CAST(COUNT(*) AS CHAR) FROM t;"

oracle_with_query "tag_ancestor_is_detached" \
  "$TMPROOT/tag_ancestor_is_detached/dl/db.sqlite/v1~0" \
  "v1~0" \
  "SELECT IFNULL(active_branch(),'NULL') AS value UNION ALL SELECT CAST(COUNT(*) AS CHAR) FROM t;"

oracle_hash_revision "raw_hash_open_is_detached"

oracle_with_query "detached_read_surfaces" \
  "$TMPROOT/detached_read_surfaces/dl/db.sqlite/v1" \
  "v1" \
  "SELECT IFNULL(active_branch(),'NULL') AS value UNION ALL SELECT CAST(COUNT(*) AS CHAR) FROM dolt_status UNION ALL SELECT CAST(COUNT(*) AS CHAR) FROM dolt_log UNION ALL SELECT CAST(COUNT(*) AS CHAR) FROM dolt_tags;"

oracle_with_mutation "branch_wins_over_tag" \
  "SELECT dolt_branch('same'); SELECT dolt_tag('same','v1');" \
  "$TMPROOT/branch_wins_over_tag/dl/db.sqlite/same" \
  "same" \
  "SELECT active_branch() AS value UNION ALL SELECT CAST(COUNT(*) AS CHAR) FROM t;"

oracle_error "detached_insert_is_readonly" "" \
  "$TMPROOT/detached_insert_is_readonly/dl/db.sqlite/v1" \
  "v1" \
  "INSERT INTO t VALUES(3,'blocked');"

oracle_error "detached_ddl_is_readonly" "" \
  "$TMPROOT/detached_ddl_is_readonly/dl/db.sqlite/v1" \
  "v1" \
  "CREATE TABLE blocked(id INTEGER PRIMARY KEY);"

oracle_error "detached_temp_table_is_readonly" "" \
  "$TMPROOT/detached_temp_table_is_readonly/dl/db.sqlite/v1" \
  "v1" \
  "CREATE TEMPORARY TABLE blocked(id INTEGER PRIMARY KEY);"

oracle_error "missing_revision_errors" "" \
  "$TMPROOT/missing_revision_errors/dl/db.sqlite/missing" \
  "missing" \
  "SELECT COUNT(*) FROM t;"

oracle_detached_command_error "detached_add_is_rejected" \
  "SELECT dolt_add('-A');" "CALL dolt_add('-A');"
oracle_detached_command_error "detached_commit_is_rejected" \
  "SELECT dolt_commit('-A','-m','blocked');" \
  "CALL dolt_commit('-Am','blocked');"
oracle_detached_command_error "detached_branch_is_rejected" \
  "SELECT dolt_branch('blocked');" "CALL dolt_branch('blocked');"
oracle_detached_command_error "detached_tag_is_rejected" \
  "SELECT dolt_tag('blocked');" "CALL dolt_tag('blocked');"
oracle_detached_command_error "detached_reset_is_rejected" \
  "SELECT dolt_reset('--hard');" "CALL dolt_reset('--hard');"
oracle_detached_command_error "detached_merge_is_rejected" \
  "SELECT dolt_merge('main');" "CALL dolt_merge('main');"
oracle_detached_pull_error "detached_pull_is_rejected"
oracle_detached_command_error "checkout_tag_does_not_detach" \
  "SELECT dolt_checkout('v1');" "CALL dolt_checkout('v1');"
oracle_error "attached_checkout_tag_does_not_detach" "" \
  "$TMPROOT/attached_checkout_tag_does_not_detach/dl/db.sqlite" \
  "main" \
  "SELECT dolt_checkout('v1');"
oracle_detached_command_error "checkout_new_branch_is_rejected" \
  "SELECT dolt_checkout('-b','blocked');" \
  "CALL dolt_checkout('-b','blocked');"
oracle_detached_command_error "detached_working_ref_is_invalid" \
  "SELECT dolt_hashof('WORKING');" "SELECT hashof('WORKING');"
oracle_detached_command_error "detached_staged_ref_is_invalid" \
  "SELECT dolt_hashof('STAGED');" "SELECT hashof('STAGED');"
oracle_detached_command_success "detached_gc_is_allowed" \
  "SELECT dolt_gc();" "CALL dolt_gc();"

oracle_reattach "checkout_branch_reattaches"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
