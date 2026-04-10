#!/bin/bash
#
# End-to-end integration test: exercises the full Doltlite workflow
# like a real user would. Creates a project, makes commits, branches,
# merges, handles conflicts, tags releases, diffs history.
#
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""
run_test() { local n="$1" s="$2" e="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if [ "$r" = "$e" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi; }
run_test_match() { local n="$1" s="$2" p="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if echo "$r"|grep -qE "$p"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"; fi; }

echo "=== Doltlite End-to-End Integration Tests ==="
echo ""

DB=/tmp/test_e2e_$$.db; rm -f "$DB"

# ============================================================
# Phase 1: Initial schema + first commit
# ============================================================

echo "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, email TEXT);
CREATE TABLE posts(id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT, body TEXT);
INSERT INTO users VALUES(1,'Alice','alice@example.com');
INSERT INTO users VALUES(2,'Bob','bob@example.com');
INSERT INTO posts VALUES(1,1,'Hello World','First post!');
INSERT INTO posts VALUES(2,2,'Bobs Post','Hi there');
SELECT dolt_commit('-A','-m','Initial schema: users and posts');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "e2e_initial_users" "SELECT count(*) FROM users;" "2" "$DB"
run_test "e2e_initial_posts" "SELECT count(*) FROM posts;" "2" "$DB"
run_test "e2e_initial_branch" "SELECT active_branch();" "main" "$DB"
run_test "e2e_initial_log" "SELECT count(*) FROM dolt_log;" "2" "$DB"
run_test "e2e_clean_status" "SELECT count(*) FROM dolt_status;" "0" "$DB"

# Tag the initial release
echo "SELECT dolt_tag('v0.1');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "e2e_tag_exists" "SELECT tag_name FROM dolt_tags;" "v0.1" "$DB"

# ============================================================
# Phase 2: Feature branch — add comments table
# ============================================================

echo "SELECT dolt_branch('add-comments');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('add-comments');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "e2e_on_feature" "SELECT active_branch();" "add-comments" "$DB"

echo "CREATE TABLE comments(id INTEGER PRIMARY KEY, post_id INTEGER, body TEXT);
INSERT INTO comments VALUES(1,1,'Great post Alice!');
INSERT INTO comments VALUES(2,1,'Thanks!');
SELECT dolt_commit('-A','-m','Add comments table');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "e2e_feature_comments" "SELECT count(*) FROM comments;" "2" "$DB"

# Main branch shouldn't have comments
echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test_match "e2e_main_no_comments" "SELECT * FROM comments;" "no such table" "$DB"

# ============================================================
# Phase 3: Concurrent work on main — update users
# ============================================================

echo "INSERT INTO users VALUES(3,'Charlie','charlie@example.com');
UPDATE users SET email='alice@newmail.com' WHERE id=1;
SELECT dolt_commit('-A','-m','Add Charlie, update Alice email');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "e2e_main_3_users" "SELECT count(*) FROM users;" "3" "$DB"
run_test "e2e_alice_new_email" "SELECT email FROM users WHERE id=1;" "alice@newmail.com" "$DB"

# ============================================================
# Phase 4: Merge feature into main (non-conflicting)
# ============================================================

run_test_match "e2e_merge_comments" "SELECT dolt_merge('add-comments');" "^[0-9a-f]{40}$" "$DB"

# After merge: main has users (3 rows) + posts (2 rows) + comments (2 rows)
run_test "e2e_merged_users" "SELECT count(*) FROM users;" "3" "$DB"
run_test "e2e_merged_posts" "SELECT count(*) FROM posts;" "2" "$DB"
run_test "e2e_merged_comments" "SELECT count(*) FROM comments;" "2" "$DB"

# Log should show merge commit
run_test_match "e2e_merge_in_log" "SELECT message FROM dolt_log LIMIT 1;" "Merge" "$DB"

# Tag v0.2
echo "SELECT dolt_tag('v0.2');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "e2e_two_tags" "SELECT count(*) FROM dolt_tags;" "2" "$DB"

# ============================================================
# Phase 5: Diff between tags
# ============================================================

run_test_match "e2e_diff_users_v01_v02" \
  "SELECT count(*) FROM dolt_diff('users', (SELECT tag_hash FROM dolt_tags WHERE tag_name='v0.1'), (SELECT tag_hash FROM dolt_tags WHERE tag_name='v0.2'));" \
  "^[0-9]" "$DB"

# ============================================================
# Phase 6: Conflicting merge
# ============================================================

echo "SELECT dolt_branch('hotfix');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_checkout('hotfix');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE users SET name='ALICE' WHERE id=1;
SELECT dolt_commit('-A','-m','Hotfix: capitalize Alice');" | $DOLTLITE "$DB" > /dev/null 2>&1

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "UPDATE users SET name='alice_updated' WHERE id=1;
SELECT dolt_commit('-A','-m','Main: update Alice name');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Merge hotfix — should conflict on users row 1
run_test_match "e2e_conflict_merge" "SELECT dolt_merge('hotfix');" "conflict" "$DB"

# Verify conflict state
run_test "e2e_has_conflicts" "SELECT num_conflicts FROM dolt_conflicts WHERE table_name='users';" "1" "$DB"

# Commit should be blocked
run_test_match "e2e_commit_blocked" "SELECT dolt_commit('-A','-m','fail');" "unresolved" "$DB"

# Resolve with --ours
echo "SELECT dolt_conflicts_resolve('--ours','users');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "e2e_conflicts_cleared" "SELECT count(*) FROM dolt_conflicts;" "0" "$DB"
run_test "e2e_ours_kept" "SELECT name FROM users WHERE id=1;" "alice_updated" "$DB"

# ============================================================
# Phase 7: Reset
# ============================================================

echo "INSERT INTO users VALUES(99,'Temp','temp@test.com');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "e2e_before_reset" "SELECT count(*) FROM users;" "4" "$DB"

echo "SELECT dolt_reset('--hard');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "e2e_after_reset" "SELECT count(*) FROM users;" "3" "$DB"
run_test "e2e_reset_clean" "SELECT count(*) FROM dolt_status;" "0" "$DB"

# ============================================================
# Phase 8: Staging workflow
# ============================================================

echo "INSERT INTO users VALUES(4,'Dave','dave@example.com');
INSERT INTO posts VALUES(3,4,'Daves Post','New here');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "e2e_two_unstaged" "SELECT count(*) FROM dolt_status WHERE staged=0;" "2" "$DB"

echo "SELECT dolt_add('users');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "e2e_one_staged" "SELECT count(*) FROM dolt_status WHERE staged=1;" "1" "$DB"
run_test "e2e_one_unstaged" "SELECT count(*) FROM dolt_status WHERE staged=0;" "1" "$DB"

# Soft reset unstages
echo "SELECT dolt_reset('--soft');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "e2e_after_soft_reset" "SELECT count(*) FROM dolt_status WHERE staged=1;" "0" "$DB"
run_test "e2e_data_preserved" "SELECT count(*) FROM users;" "4" "$DB"

# Stage all and commit
run_test_match "e2e_commit_all" "SELECT dolt_commit('-A','-m','Add Dave');" "^[0-9a-f]{40}$" "$DB"

# ============================================================
# Phase 9: Branch listing and management
# ============================================================

run_test "e2e_branch_count" "SELECT count(*) FROM dolt_branches;" "3" "$DB"

echo "SELECT dolt_branch('-d','hotfix');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "e2e_deleted_branch" "SELECT count(*) FROM dolt_branches;" "2" "$DB"
run_test_match "e2e_delete_gone" "SELECT dolt_checkout('hotfix');" "not found" "$DB"

# ============================================================
# Phase 10: Full history
# ============================================================

run_test_match "e2e_log_count" "SELECT count(*) FROM dolt_log;" "^[5-9]" "$DB"

# Verify data integrity at the end
run_test "e2e_final_users" "SELECT count(*) FROM users;" "4" "$DB"
run_test "e2e_final_posts" "SELECT count(*) FROM posts;" "3" "$DB"
run_test "e2e_final_comments" "SELECT count(*) FROM comments;" "2" "$DB"
run_test "e2e_final_branch" "SELECT active_branch();" "main" "$DB"

# ============================================================
# Phase 11: Persistence — close and reopen
# ============================================================

run_test "e2e_persist_users" "SELECT name FROM users WHERE id=4;" "Dave" "$DB"
run_test "e2e_persist_log" "SELECT message FROM dolt_log LIMIT 1;" "Add Dave" "$DB"
run_test "e2e_persist_tags" "SELECT count(*) FROM dolt_tags;" "2" "$DB"
run_test "e2e_persist_branches" "SELECT count(*) FROM dolt_branches;" "2" "$DB"

rm -f "$DB"
echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
