#!/bin/bash
#
# Doltlite demo test — adapted from the Dolt getting-started tutorial.
# Exercises the full workflow: schema, data, branches, merges, diffs,
# history, schema diff, point-in-time queries, and audit log.
#
DOLTLITE=./doltlite
PASS=0; FAIL=0; ERRORS=""
run_test() { local n="$1" s="$2" e="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if [ "$r" = "$e" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  expected: $e\n  got:      $r"; fi; }
run_test_match() { local n="$1" s="$2" p="$3" d="$4"; local r=$(echo "$s"|perl -e 'alarm(10);exec @ARGV' $DOLTLITE "$d" 2>&1); if echo "$r"|grep -qE "$p"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $n\n  pattern: $p\n  got:     $r"; fi; }

echo "=== Doltlite Demo Test (Dolt Getting Started) ==="
echo ""

DB=/tmp/test_demo_$$.db; rm -f "$DB"

# ============================================================
# Phase 1: Create schema and first commit
# ============================================================

echo "CREATE TABLE employees(id INTEGER PRIMARY KEY, last_name TEXT, first_name TEXT);
CREATE TABLE teams(id INTEGER PRIMARY KEY, team_name TEXT);
CREATE TABLE employees_teams(team_id INTEGER, employee_id INTEGER, PRIMARY KEY(team_id, employee_id));
SELECT dolt_commit('-A','-m','Created initial schema');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "schema_tables" "SELECT count(*) FROM sqlite_master WHERE type='table';" "3" "$DB"
run_test "schema_log" "SELECT count(*) FROM dolt_log;" "1" "$DB"
run_test_match "schema_msg" "SELECT message FROM dolt_log;" "Created initial schema" "$DB"
run_test "schema_clean" "SELECT count(*) FROM dolt_status;" "0" "$DB"

# ============================================================
# Phase 2: Populate data and commit
# ============================================================

echo "INSERT INTO employees VALUES(0,'Sehn','Tim');
INSERT INTO employees VALUES(1,'Hendriks','Brian');
INSERT INTO employees VALUES(2,'Son','Aaron');
INSERT INTO employees VALUES(3,'Fitzgerald','Brian');
INSERT INTO teams VALUES(0,'Engineering');
INSERT INTO teams VALUES(1,'Sales');
INSERT INTO employees_teams VALUES(0,0);
INSERT INTO employees_teams VALUES(0,1);
INSERT INTO employees_teams VALUES(0,2);
INSERT INTO employees_teams VALUES(1,0);
INSERT INTO employees_teams VALUES(1,3);" | $DOLTLITE "$DB" > /dev/null 2>&1

# Status shows 3 dirty tables
run_test "data_status" "SELECT count(*) FROM dolt_status;" "3" "$DB"

# Query data
run_test "data_emp_count" "SELECT count(*) FROM employees;" "4" "$DB"
run_test "data_brian_count" "SELECT count(*) FROM employees WHERE first_name='Brian';" "2" "$DB"

# Join query: Engineering team
run_test "data_eng_team" \
  "SELECT count(*) FROM employees e JOIN employees_teams et ON e.id=et.employee_id JOIN teams t ON t.id=et.team_id WHERE t.team_name='Engineering';" \
  "3" "$DB"

# Commit
echo "SELECT dolt_commit('-A','-m','Populated tables with data');" | $DOLTLITE "$DB" > /dev/null 2>&1
echo "SELECT dolt_tag('v1');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "data_log" "SELECT count(*) FROM dolt_log;" "2" "$DB"
run_test "data_clean" "SELECT count(*) FROM dolt_status;" "0" "$DB"

# ============================================================
# Phase 3: Branch — modify data on 'modifications'
# ============================================================

echo "SELECT dolt_branch('modifications');
SELECT dolt_checkout('modifications');
UPDATE employees SET first_name='Timothy' WHERE first_name='Tim';
INSERT INTO employees VALUES(4,'Wilkins','Daylon');
INSERT INTO employees_teams VALUES(0,4);
DELETE FROM employees_teams WHERE employee_id=0 AND team_id=1;
SELECT dolt_commit('-A','-m','Modifications on a branch');" | $DOLTLITE "$DB" > /dev/null 2>&1

# Verify modifications branch
run_test "mod_branch" "SELECT active_branch();" "modifications" "$DB"
run_test "mod_emp_count" "SELECT count(*) FROM employees;" "5" "$DB"
run_test "mod_timothy" "SELECT first_name FROM employees WHERE id=0;" "Timothy" "$DB"

# ============================================================
# Phase 4: Back to main — data unchanged
# ============================================================

echo "SELECT dolt_checkout('main');" | $DOLTLITE "$DB" > /dev/null 2>&1

run_test "main_branch" "SELECT active_branch();" "main" "$DB"
run_test "main_emp_count" "SELECT count(*) FROM employees;" "4" "$DB"
run_test "main_tim" "SELECT first_name FROM employees WHERE id=0;" "Tim" "$DB"
run_test "main_no_daylon" "SELECT count(*) FROM employees WHERE id=4;" "0" "$DB"

# ============================================================
# Phase 5: Point-in-time query — see modifications branch data
# ============================================================

run_test "asof_mod_count" "SELECT count(*) FROM dolt_at('employees','modifications');" "5" "$DB"
run_test "asof_v1_count" "SELECT count(*) FROM dolt_at('employees','v1');" "4" "$DB"

# ============================================================
# Phase 6: Merge modifications into main
# ============================================================

run_test_match "merge_mod" "SELECT dolt_merge('modifications');" "^[0-9a-f]{40}$" "$DB"

run_test "merged_emp_count" "SELECT count(*) FROM employees;" "5" "$DB"
run_test "merged_timothy" "SELECT first_name FROM employees WHERE id=0;" "Timothy" "$DB"
run_test "merged_daylon" "SELECT first_name FROM employees WHERE id=4;" "Daylon" "$DB"

# Sales team: Tim was removed from Sales by modifications branch
run_test "merged_sales" \
  "SELECT count(*) FROM employees e JOIN employees_teams et ON e.id=et.employee_id JOIN teams t ON t.id=et.team_id WHERE t.team_name='Sales';" \
  "1" "$DB"

# Engineering team now includes Daylon
run_test "merged_eng" \
  "SELECT count(*) FROM employees e JOIN employees_teams et ON e.id=et.employee_id JOIN teams t ON t.id=et.team_id WHERE t.team_name='Engineering';" \
  "4" "$DB"

# ============================================================
# Phase 7: Reset (undo uncommitted changes)
# ============================================================

echo "INSERT INTO employees VALUES(99,'Temp','Temp');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "reset_before" "SELECT count(*) FROM employees;" "6" "$DB"

echo "SELECT dolt_reset('--hard');" | $DOLTLITE "$DB" > /dev/null 2>&1
run_test "reset_after" "SELECT count(*) FROM employees;" "5" "$DB"
run_test "reset_clean" "SELECT count(*) FROM dolt_status;" "0" "$DB"

# ============================================================
# Phase 8: Branches listing
# ============================================================

run_test "branches_count" "SELECT count(*) FROM dolt_branches;" "2" "$DB"
run_test_match "branches_names" "SELECT group_concat(name) FROM (SELECT name FROM dolt_branches ORDER BY name);" "main.*modifications" "$DB"

# ============================================================
# Phase 9: Full log
# ============================================================

run_test_match "log_latest" "SELECT message FROM dolt_log LIMIT 1;" "Modifications|Merge" "$DB"
run_test_match "log_count" "SELECT count(*) FROM dolt_log;" "^[3-9]" "$DB"

# ============================================================
# Phase 10: Audit log (dolt_diff_<table>)
# ============================================================

# dolt_diff_employees shows all changes across commits
run_test_match "audit_count" "SELECT count(*) FROM dolt_diff_employees;" "^[4-9]" "$DB"
run_test_match "audit_has_added" "SELECT count(*) FROM dolt_diff_employees WHERE diff_type='added';" "^[4-9]" "$DB"

# ============================================================
# Phase 11: History (dolt_history_<table>)
# ============================================================

# Employee 0 exists in multiple commits
run_test_match "history_emp0" "SELECT count(*) FROM dolt_history_employees WHERE rowid_val=0;" "^[2-9]" "$DB"

# Multiple distinct commits for employee 0
run_test_match "history_commits" "SELECT count(DISTINCT commit_hash) FROM dolt_history_employees WHERE rowid_val=0;" "^[2-9]" "$DB"

# ============================================================
# Phase 12: Schema diff
# ============================================================

# Between first commit and current: employees_teams and teams were added
# (they share a commit with employees)
run_test "schema_diff_same" \
  "SELECT count(*) FROM dolt_schema_diff((SELECT commit_hash FROM dolt_log LIMIT 1),(SELECT commit_hash FROM dolt_log LIMIT 1));" \
  "0" "$DB"

# ============================================================
# Phase 13: Tags
# ============================================================

run_test "tags_count" "SELECT count(*) FROM dolt_tags;" "1" "$DB"
run_test "tags_v1" "SELECT name FROM dolt_tags;" "v1" "$DB"

# Data at v1: 4 employees
run_test "tags_v1_data" "SELECT count(*) FROM dolt_at('employees','v1');" "4" "$DB"

# ============================================================
# Phase 14: Persistence — reopen and verify
# ============================================================

run_test "persist_emp" "SELECT count(*) FROM employees;" "5" "$DB"
run_test "persist_log" "SELECT count(*) FROM dolt_log;" "3" "$DB"
run_test "persist_branch" "SELECT active_branch();" "main" "$DB"
run_test "persist_tags" "SELECT count(*) FROM dolt_tags;" "1" "$DB"

rm -f "$DB"

# ============================================================
# Done
# ============================================================

echo ""
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ $FAIL -gt 0 ]; then echo -e "$ERRORS"; exit 1; fi
