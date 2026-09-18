#!/bin/bash
# A refused reset on a read-only connection must not unlock later VC writers.
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== readonly VC writers stay refused after a failed reset ==="
echo ""

DB=$(mktemp /tmp/dl_ro_vc_XXXXXX.db)
rm -f "$DB"
trap 'rm -f "$DB"' EXIT

"$DOLTLITE" "$DB" "
CREATE TABLE t(a INT PRIMARY KEY, b TEXT);
INSERT INTO t VALUES(1,'x');
SELECT dolt_commit('-A','-m','c1');
SELECT dolt_tag('v1');
SELECT dolt_branch('feat');
INSERT INTO t VALUES(2,'y');
SELECT dolt_add('t');
" >/dev/null

# One connection: a refused reset must not let later VC writers persist.
RO_OUT=$(
  "$DOLTLITE" -readonly "$DB" 2>&1 <<'SQL' || true
SELECT dolt_branch('before_reset');
SELECT dolt_reset('t');
SELECT dolt_checkout('feat');
SELECT dolt_branch('after_reset');
SELECT dolt_tag('ro_tag');
SELECT dolt_tag('-d','v1');
SELECT dolt_branch('-D','feat');
SELECT dolt_commit('-m','ro commit');
INSERT INTO t VALUES(3,'z');
SQL
)

expect_all_readonly() {
  local name="$1"
  local n
  n=$(printf '%s\n' "$RO_OUT" | grep -ci 'readonly\|read-only')
  if [ "$n" -ge 9 ]; then
    dltest_pass
  else
    dltest_fail "$name" "  expected >=9 readonly errors, got $n\n$RO_OUT"
  fi
}
expect_all_readonly "ro_session_every_writer_refused"

run_test "rw_branches_unchanged" \
  "SELECT group_concat(name) FROM (SELECT name FROM dolt_branches ORDER BY name);" \
  "feat,main" "$DB"
run_test "rw_tags_unchanged" \
  "SELECT group_concat(tag_name) FROM (SELECT tag_name FROM dolt_tags ORDER BY tag_name);" \
  "v1" "$DB"
run_test "rw_log_unchanged" \
  "SELECT message FROM dolt_log LIMIT 1;" \
  "c1" "$DB"
run_test "rw_still_staged" \
  "SELECT count(*) FROM dolt_status WHERE staged=1;" \
  "1" "$DB"

dltest_finish
