#!/bin/bash
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== Doltlite Tag Tests ==="
echo ""
DB=/tmp/test_tag_$$.db; rm -f "$DB"
echo "CREATE TABLE t(x); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','first');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "tag_head" "SELECT dolt_tag('v1.0','-m','release one');" "0" "$DB"
run_test "list_tags" "SELECT count(*) FROM dolt_tags;" "1" "$DB"
run_test "tag_name" "SELECT tag_name FROM dolt_tags;" "v1.0" "$DB"
run_test_match "tag_hash" "SELECT tag_hash FROM dolt_tags;" "^[0-9a-f]{40}$" "$DB"
run_test "tag_message" "SELECT message FROM dolt_tags;" "release one" "$DB"

echo "INSERT INTO t VALUES(2); SELECT dolt_commit('-A','-m','second');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "tag_specific" \
  "SELECT dolt_tag('v0.9', (SELECT commit_hash FROM dolt_log LIMIT 1 OFFSET 1));" "0" "$DB"
run_test "tag_head_parent" \
  "SELECT dolt_tag('parent','HEAD^1');" "0" "$DB"
run_test "tag_head_tilde" \
  "SELECT dolt_tag('parenttilde','HEAD~1');" "0" "$DB"
run_test "four_tags" "SELECT count(*) FROM dolt_tags;" "4" "$DB"

DB2=/tmp/test_tag_branch_$$.db; rm -f "$DB2"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_commit('-A','-m','base');
SELECT dolt_branch('feat');
SELECT dolt_checkout('feat');
INSERT INTO t VALUES(2,'feat');
SELECT dolt_commit('-A','-m','feat');
SELECT dolt_checkout('main');
SELECT dolt_tag('feat-tag','feat');" | $DOLTLITE "$DB2" > /dev/null 2>&1
run_test "tag_branch_ref" "SELECT count(*) FROM dolt_tags WHERE tag_name='feat-tag';" "1" "$DB2"

run_test_match "dup_tag" "SELECT dolt_tag('v1.0');" "already exists" "$DB"

run_test_match "tag_missing_commit" \
  "SELECT dolt_tag('badtag','0123456789abcdef0123456789abcdef01234567');" \
  "commit not found" "$DB"

run_test_match "tag_empty_name" "SELECT dolt_tag('');" "invalid tag name" "$DB"
run_test_match "tag_reserved_working" "SELECT dolt_tag('WORKING');" "invalid tag name" "$DB"
run_test_match "tag_hash_name" \
  "SELECT dolt_tag('0123456789012345678901234567890123456789');" \
  "invalid tag name" "$DB"

run_test "delete_tag" "SELECT dolt_tag('-d','v0.9');" "0" "$DB"
run_test "delete_and_recreate_same_name" "SELECT dolt_tag('-d','parenttilde'); SELECT dolt_tag('parenttilde');" "0
0" "$DB"
run_test "three_tags_left" "SELECT count(*) FROM dolt_tags;" "3" "$DB"
run_test_match "delete_multiple_tags_with_missing" \
  "SELECT dolt_tag('-d','v1.0','extra');" \
  "not found" "$DB"
run_test "delete_multiple_tags_with_missing_keeps_tag" \
  "SELECT count(*) FROM dolt_tags WHERE tag_name='v1.0';" "1" "$DB"

run_test_match "delete_missing" "SELECT dolt_tag('-d','nope');" "not found" "$DB"

run_test "tag_persists" "SELECT tag_name FROM dolt_tags ORDER BY tag_name;" "parent
parenttilde
v1.0" "$DB"

DB3=/tmp/test_tag_author_$$.db; rm -f "$DB3"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT dolt_commit('-Am','base');" | $DOLTLITE "$DB3" > /dev/null 2>&1

run_test_match "tag_malformed_author" \
  "SELECT dolt_tag('bad','--author','not-an-author');" \
  "Author not formatted correctly" "$DB3"
run_test "tag_malformed_author_not_created" \
  "SELECT count(*) FROM dolt_tags WHERE tag_name='bad';" "0" "$DB3"
run_test "tag_author_without_closing_bracket" \
  "SELECT dolt_tag('missing-close','--author','Tag Name <tag@example.com');
SELECT tagger || '|' || email FROM dolt_tags WHERE tag_name='missing-close';" \
  "0
Tag Name|tag@example.com" "$DB3"
run_test "tag_author_resumes_after_parenthesis" \
  "SELECT dolt_tag('after-paren','--author','ignored)Real Name <real@example.com>');
SELECT tagger || '|' || email FROM dolt_tags WHERE tag_name='after-paren';" \
  "0
Real Name|real@example.com" "$DB3"

DB4=/tmp/test_tag_batch_$$.db; rm -f "$DB4"
echo "CREATE TABLE t(id INTEGER PRIMARY KEY); INSERT INTO t VALUES(1); SELECT dolt_commit('-A','-m','init'); SELECT dolt_tag('one'); SELECT dolt_tag('two');" | $DOLTLITE "$DB4" > /dev/null 2>&1
run_test "delete_multiple_tags" "SELECT dolt_tag('-d','one','two');" "0" "$DB4"
run_test "delete_multiple_tags_removed" "SELECT count(*) FROM dolt_tags;" "0" "$DB4"
echo "SELECT dolt_tag('one'); SELECT dolt_tag('two');" | $DOLTLITE "$DB4" > /dev/null 2>&1
run_test_match "delete_multiple_tags_missing_is_atomic" "SELECT dolt_tag('-d','one','missing','two');" "not found" "$DB4"
run_test "delete_multiple_tags_missing_keeps_tags" "SELECT count(*) FROM dolt_tags;" "2" "$DB4"

rm -f "$DB" "$DB2" "$DB3" "$DB4"
dltest_finish
