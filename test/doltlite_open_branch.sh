#!/bin/bash

DOLTLITE="${1:-./doltlite}"
DOLTLITE_SYSTEM="${DOLTLITE_SYSTEM:-$DOLTLITE}"
DOLTLITE_SYSTEM_NULL="${DOLTLITE_SYSTEM_NULL:-/dev/null}"
PASS=0; FAIL=0; ERRORS=""

normalize_output() {
  if [ "${DLTEST_STRIP_CR:-0}" = "1" ]; then
    tr -d '\r'
  else
    cat
  fi
}

check_eq() {
  local name="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $name\n  expected: $expected\n  got:      $actual"
  fi
}

check_match() {
  local name="$1" pattern="$2" actual="$3"
  if echo "$actual" | grep -qE "$pattern"; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $name\n  pattern: $pattern\n  got:     $actual"
  fi
}

echo "=== Doltlite Open Branch Tests ==="
echo ""

DB=/tmp/test_open_branch_$$.db
rm -f "$DB"
cat <<'SQL' | "$DOLTLITE" "$DB" >/dev/null 2>&1
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','init');
SELECT dolt_tag('v1');
SELECT dolt_branch('side');
SELECT dolt_checkout('side');
UPDATE t SET v='side' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','side');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(2,'main2');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main2');
SQL

res=$("$DOLTLITE" "$DB" "SELECT active_branch(); SELECT v FROM t WHERE id=1;" | normalize_output)
check_eq "default_uses_main" $'main\nmain' "$res"

res=$("$DOLTLITE" "$DB@side" "SELECT active_branch(); SELECT v FROM t WHERE id=1;" | normalize_output)
check_eq "at_selects_side" $'side\nside' "$res"

res=$("$DOLTLITE" "$DB/side" "SELECT active_branch(); SELECT v FROM t WHERE id=1;" | normalize_output)
check_eq "slash_selects_side" $'side\nside' "$res"

res=$("$DOLTLITE" "$DB" "SELECT active_branch(); SELECT v FROM t WHERE id=1;" | normalize_output)
check_eq "branch_open_does_not_change_default" $'main\nmain' "$res"

res=$("$DOLTLITE" "$DB@missing" "SELECT active_branch();" 2>&1 | normalize_output || true)
check_match "missing_branch_errors" "unable to open database|unable to select branch|branch.*not found|SQLITE_NOTFOUND" "$res"

cat <<'SQL' | "$DOLTLITE" "$DB" >/dev/null 2>&1
SELECT dolt_branch('-m','side','renamed');
SELECT dolt_branch('-c','renamed','copy');
SQL

res=$("$DOLTLITE" "$DB@renamed" "SELECT active_branch(); SELECT v FROM t WHERE id=1;" | normalize_output)
check_eq "renamed_branch_open" $'renamed\nside' "$res"

res=$("$DOLTLITE" "$DB/copy" "SELECT active_branch(); SELECT v FROM t WHERE id=1;" | normalize_output)
check_eq "copied_branch_open" $'copy\nside' "$res"

res=$("$DOLTLITE" "$DB@renamed" "INSERT INTO t VALUES(2,'more'); SELECT dolt_commit('-A','-m','more'); SELECT active_branch(); SELECT count(*) FROM t;" | normalize_output)
check_match "commit_on_open_branch_path_hash" "^[0-9a-f]{40}$" "$(echo "$res" | sed -n '1p')"
check_eq "commit_on_open_branch_path_state" $'renamed\n2' "$(echo "$res" | tail -n +2)"

cat <<'SQL' | "$DOLTLITE" "$DB" >/dev/null 2>&1
SELECT dolt_branch('-D','copy');
SQL

res=$("$DOLTLITE" "$DB@copy" "SELECT active_branch();" 2>&1 | normalize_output || true)
check_match "deleted_branch_open_errors" "unable to open database|unable to select branch|branch.*not found|SQLITE_NOTFOUND" "$res"

res=$("$DOLTLITE" "$DB/v1" "SELECT IFNULL(active_branch(),'NULL'); SELECT group_concat(id || ':' || v, ',') FROM t;" | normalize_output)
check_eq "tag_open_is_detached" $'NULL\n1:main' "$res"

res=$("$DOLTLITE" "$DB@v1" "SELECT IFNULL(active_branch(),'NULL'); SELECT dolt_hashof('HEAD') = (SELECT commit_hash FROM dolt_log WHERE message='init');" | normalize_output)
check_eq "at_tag_open_is_detached" $'NULL\n1' "$res"

INIT_HASH=$("$DOLTLITE" "$DB" "SELECT commit_hash FROM dolt_log WHERE message='init';" | normalize_output)
res=$("$DOLTLITE" "$DB/$INIT_HASH" "SELECT IFNULL(active_branch(),'NULL'); SELECT group_concat(id || ':' || v, ',') FROM t;" | normalize_output)
check_eq "hash_open_is_detached" $'NULL\n1:main' "$res"

res=$("$DOLTLITE" "$DB/main~1" "SELECT IFNULL(active_branch(),'NULL'); SELECT group_concat(id || ':' || v, ',') FROM t;" | normalize_output)
check_eq "branch_ancestor_open_is_detached" $'NULL\n1:main' "$res"

res=$("$DOLTLITE" "$DB/v1~0" "SELECT IFNULL(active_branch(),'NULL'); SELECT group_concat(id || ':' || v, ',') FROM t;" | normalize_output)
check_eq "tag_ancestor_open_is_detached" $'NULL\n1:main' "$res"

res=$("$DOLTLITE" "$DB/v1" "INSERT INTO t VALUES(3,'blocked');" 2>&1 | normalize_output || true)
check_match "detached_insert_is_readonly" "read-only|readonly" "$res"

res=$("$DOLTLITE" "$DB/v1" "CREATE TABLE blocked(id INTEGER PRIMARY KEY);" 2>&1 | normalize_output || true)
check_match "detached_ddl_is_readonly" "read-only|readonly" "$res"

res=$("$DOLTLITE" "$DB/v1" "CREATE TEMPORARY TABLE blocked(id INTEGER PRIMARY KEY);" 2>&1 | normalize_output || true)
check_match "detached_temp_ddl_is_readonly" "read-only|readonly" "$res"

res=$("$DOLTLITE" "$DB/v1" "SELECT dolt_add('-A');" 2>&1 | normalize_output || true)
check_match "detached_add_is_rejected" "detached head|read-only|readonly" "$res"

res=$("$DOLTLITE" "$DB/v1" "SELECT dolt_branch('blocked');" 2>&1 | normalize_output || true)
check_match "detached_branch_is_rejected" "detached head|read-only|readonly" "$res"

for command_case in \
  "tag|SELECT dolt_tag('blocked')" \
  "commit|SELECT dolt_commit('-A','-m','blocked')" \
  "reset|SELECT dolt_reset('--hard')" \
  "merge|SELECT dolt_merge('main')" \
  "cherry_pick|SELECT dolt_cherry_pick('$INIT_HASH')" \
  "revert|SELECT dolt_revert('$INIT_HASH')" \
  "rebase|SELECT dolt_rebase('main')" \
  "verify_constraints|SELECT dolt_verify_constraints()" \
  "connect_branch|SELECT dolt_connect_branch('main')"; do
  command_name=${command_case%%|*}
  command_sql=${command_case#*|}
  res=$("$DOLTLITE" "$DB/v1" "$command_sql" 2>&1 | normalize_output || true)
  check_match "detached_${command_name}_is_rejected" "detached head|read-only|readonly" "$res"
done

res=$("$DOLTLITE" "$DB/v1" "SELECT dolt_checkout('v1');" 2>&1 | normalize_output || true)
check_match "checkout_tag_does_not_enter_detached" "does not support a detached head state" "$res"

res=$("$DOLTLITE" "$DB" "SELECT dolt_checkout('v1');" 2>&1 | normalize_output || true)
check_match "attached_checkout_tag_does_not_detach" "no such branch or table|does not support a detached head state" "$res"

res=$("$DOLTLITE" "$DB/v1" "SELECT dolt_checkout('-b','blocked');" 2>&1 | normalize_output || true)
check_match "checkout_new_branch_from_detached_is_rejected" "read-only database" "$res"

res=$("$DOLTLITE" "$DB/v1" "SELECT dolt_hashof('WORKING');" 2>&1 | normalize_output || true)
check_match "detached_working_ref_is_invalid" "not found|invalid|error" "$res"

res=$("$DOLTLITE" "$DB/v1" "SELECT dolt_hashof('STAGED');" 2>&1 | normalize_output || true)
check_match "detached_staged_ref_is_invalid" "not found|invalid|error" "$res"

res=$("$DOLTLITE" "$DB/v1" "SELECT dolt_gc(); SELECT IFNULL(active_branch(),'NULL'); SELECT count(*) FROM t;" | normalize_output)
check_match "detached_gc_is_allowed" "chunks removed, [0-9]+ chunks kept" "$(echo "$res" | sed -n '1p')"
check_eq "detached_gc_preserves_snapshot" $'NULL\n1' "$(echo "$res" | tail -n 2)"

res=$("$DOLTLITE" "$DB/v1" "SELECT dolt_checkout('main'); SELECT active_branch(); SELECT count(*) FROM t; INSERT INTO t VALUES(3,'reattached'); SELECT count(*) FROM t;" | normalize_output)
check_eq "checkout_branch_reattaches_session" $'0\nmain\n2\n3' "$res"

res=$("$DOLTLITE" "$DB" "SELECT active_branch(); SELECT count(*) FROM t;" | normalize_output)
check_eq "detached_state_does_not_survive_reopen" $'main\n3' "$res"

"$DOLTLITE" "$DB" "SELECT dolt_branch('same'); SELECT dolt_tag('same','v1');" >/dev/null
res=$("$DOLTLITE" "$DB/same" "SELECT IFNULL(active_branch(),'NULL'); SELECT count(*) FROM t;" | normalize_output)
check_eq "branch_wins_over_tag" $'same\n2' "$res"

res=$("$DOLTLITE" "$DB/0000000000000000000000000000000000000000" "SELECT 1;" 2>&1 | normalize_output || true)
check_match "missing_hash_errors" "unable to open database|unable to select branch|not found|SQLITE_NOTFOUND" "$res"

res=$(printf ".headers off\nSELECT count(*) FROM t;\n.system %s %s \"INSERT INTO t VALUES(4,'peer'); SELECT dolt_commit('-A','-m','peer'); SELECT dolt_tag('-d','v1');\" >%s\nSELECT IFNULL(active_branch(),'NULL');\nSELECT count(*) FROM t;\n" "$DOLTLITE_SYSTEM" "$DB" "$DOLTLITE_SYSTEM_NULL" \
  | "$DOLTLITE" "$DB/v1" | normalize_output)
check_eq "detached_reader_stays_on_pinned_commit" $'1\nNULL\n1' "$res"

res=$("$DOLTLITE" "$DB" "SELECT count(*) FROM t; SELECT count(*) FROM dolt_tags WHERE tag_name='v1';" | normalize_output)
check_eq "peer_advance_and_tag_delete_are_durable" $'4\n0' "$res"

echo ""
echo "Results: $PASS passed, $FAIL failed"
if [ $FAIL -gt 0 ]; then
  printf "%b\n" "$ERRORS"
  exit 1
fi
