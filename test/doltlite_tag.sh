#!/bin/bash
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""
run_test() { local n="$1" s="$2" e="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if [ "$r" = "$e" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi; }
run_test_match() { local n="$1" s="$2" p="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if echo "$r"|grep -qE "$p"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"; fi; }

echo "=== Doltlite Tag Tests ==="
echo ""
DB=/tmp/test_tag_$$.db; rm -f "$DB"
echo "CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','first');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "tag_head" "SELECT dolt_tag('v1.0','-m','release one');" "0" "$DB"
run_test "list_tags" "SELECT count(*) FROM dolt_tags;" "1" "$DB"
run_test "tag_name" "SELECT tag_name FROM dolt_tags;" "v1.0" "$DB"
run_test_match "tag_hash" "SELECT tag_hash FROM dolt_tags;" "^[0-9a-f]{40}$" "$DB"
run_test "tag_message" "SELECT message FROM dolt_tags;" "release one" "$DB"

# Second commit, tag specific commit
echo "INSERT INTO t VALUES(2); SELECT dolt_commit('-A','-m','second');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "tag_specific" \
  "SELECT dolt_tag('v0.9', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1));" "0" "$DB"
run_test "two_tags" "SELECT count(*) FROM dolt_tags;" "2" "$DB"

# Duplicate tag error
run_test_match "dup_tag" "SELECT dolt_tag('v1.0');" "already exists" "$DB"

# Explicit tag target must resolve to a real commit
run_test_match "tag_missing_commit" \
  "SELECT dolt_tag('badtag','0123456789abcdef0123456789abcdef01234567');" \
  "commit not found" "$DB"

# Delete tag
run_test "delete_tag" "SELECT dolt_tag('-d','v0.9');" "0" "$DB"
run_test "one_tag_left" "SELECT count(*) FROM dolt_tags;" "1" "$DB"
run_test_match "delete_tag_extra_arg" \
  "SELECT dolt_tag('-d','v1.0','extra');" \
  "too many positional arguments to dolt_tag" "$DB"
run_test "delete_extra_arg_keeps_tag" \
  "SELECT count(*) FROM dolt_tags WHERE tag_name='v1.0';" "1" "$DB"

# Delete nonexistent
run_test_match "delete_missing" "SELECT dolt_tag('-d','nope');" "not found" "$DB"

# Tag persists across reopen
run_test "tag_persists" "SELECT tag_name FROM dolt_tags;" "v1.0" "$DB"

rm -f "$DB"
echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
