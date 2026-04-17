#!/bin/bash
#
# Remote operations integration test for doltlite.
# Tests: dolt_remote, dolt_push, dolt_clone, dolt_fetch, dolt_pull
#

DOLTLITE="${1:-$(dirname "$0")/../build/doltlite}"
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

pass=0
fail=0

check() {
  local desc="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    echo "  PASS: $desc"; pass=$((pass+1))
  else
    echo "  FAIL: $desc"
    echo "    expected: |$(echo "$expected" | head -5)|"
    echo "    actual:   |$(echo "$actual" | head -5)|"
    fail=$((fail+1))
  fi
}

DB="$DOLTLITE"
R="file://$TMPDIR"

# ============================================================
# Setup: create a source repo with some data
# ============================================================
"$DB" "$TMPDIR/src.db" <<ENDSQL
CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
INSERT INTO users VALUES(1,'alice',30),(2,'bob',25),(3,'charlie',35);
CREATE TABLE scores(uid INTEGER, score REAL, FOREIGN KEY(uid) REFERENCES users(id));
INSERT INTO scores VALUES(1,95.5),(2,87.3),(3,91.0);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','initial: two tables with data');
SELECT dolt_remote('add','origin','$R/remote.db');
.quit
ENDSQL

# ============================================================
echo "=== 1. Remote management ==="
# ============================================================
result=$("$DB" "$TMPDIR/src.db" "SELECT name, url FROM dolt_remotes;")
check "remote registered" "origin|$R/remote.db" "$result"

"$DB" "$TMPDIR/src.db" "SELECT dolt_remote('add','backup','$R/backup.db');" > /dev/null
result=$("$DB" "$TMPDIR/src.db" "SELECT count(*) FROM dolt_remotes;")
check "two remotes" "2" "$result"

"$DB" "$TMPDIR/src.db" "SELECT dolt_remote('remove','backup');" > /dev/null
result=$("$DB" "$TMPDIR/src.db" "SELECT count(*) FROM dolt_remotes;")
check "remote removed" "1" "$result"

# ============================================================
echo "=== 2. Push ==="
# ============================================================
result=$("$DB" "$TMPDIR/src.db" "SELECT dolt_push('origin','main');")
check "push returns 0" "0" "$result"

src_head=$("$DB" "$TMPDIR/src.db" "SELECT commit_hash FROM dolt_log LIMIT 1;")
result=$("$DB" "$TMPDIR/remote.db" "SELECT commit_hash FROM dolt_log LIMIT 1;")
check "remote head matches pushed main" "$src_head" "$result"

# ============================================================
echo "=== 3. Clone ==="
# ============================================================
result=$("$DB" "$TMPDIR/clone.db" "SELECT dolt_clone('$R/remote.db');")
check "clone returns 0" "0" "$result"

result=$("$DB" "$TMPDIR/clone.db" "SELECT active_branch();")
check "clone branch is main" "main" "$result"

result=$("$DB" "$TMPDIR/clone.db" "SELECT count(*) FROM users;")
check "clone has 3 users" "3" "$result"

result=$("$DB" "$TMPDIR/clone.db" "SELECT count(*) FROM scores;")
check "clone has 3 scores" "3" "$result"

result=$("$DB" "$TMPDIR/clone.db" "SELECT name FROM dolt_remotes;")
check "clone has origin" "origin" "$result"

result=$("$DB" "$TMPDIR/clone.db" "SELECT message FROM dolt_log LIMIT 1;")
check "clone has commit history" "initial: two tables with data" "$result"

# ============================================================
echo "=== 4. Push from clone ==="
# ============================================================
"$DB" "$TMPDIR/clone.db" <<'ENDSQL'
INSERT INTO users VALUES(4,'diana',28);
INSERT INTO scores VALUES(4,99.1);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add diana');
.quit
ENDSQL
result=$("$DB" "$TMPDIR/clone.db" "SELECT dolt_push('origin','main');")
check "push from clone returns 0" "0" "$result"

clone_head=$("$DB" "$TMPDIR/clone.db" "SELECT commit_hash FROM dolt_log LIMIT 1;")
result=$("$DB" "$TMPDIR/remote.db" "SELECT commit_hash FROM dolt_log LIMIT 1;")
check "remote head matches clone push" "$clone_head" "$result"

# ============================================================
echo "=== 5. Fetch ==="
# ============================================================
result=$("$DB" "$TMPDIR/src.db" "SELECT dolt_fetch('origin','main');")
check "fetch returns 0" "0" "$result"

result=$("$DB" "$TMPDIR/src.db" "SELECT count(*) FROM users;")
check "data unchanged before pull" "3" "$result"

# ============================================================
echo "=== 6. Pull (fast-forward) ==="
# ============================================================
result=$("$DB" "$TMPDIR/src.db" "SELECT dolt_pull('origin','main');")
check "pull returns 0" "0" "$result"

result=$("$DB" "$TMPDIR/src.db" "SELECT count(*) FROM users;")
check "src has 4 users after pull" "4" "$result"

result=$("$DB" "$TMPDIR/src.db" "SELECT name FROM users WHERE id=4;")
check "src has diana after pull" "diana" "$result"

# ============================================================
echo "=== 7. Push new branch ==="
# ============================================================
"$DB" "$TMPDIR/src.db" <<'ENDSQL'
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO users VALUES(5,'eve',22);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add eve on feature');
SELECT dolt_push('origin','feature');
.quit
ENDSQL

result=$("$DB" "$TMPDIR/remote.db" "SELECT dolt_checkout('feature'); SELECT count(*) FROM users;")
check "remote feature has 5 users" "0
5" "$result"

# ============================================================
echo "=== 8. Fetch new branch ==="
# ============================================================
result=$("$DB" "$TMPDIR/clone.db" "SELECT dolt_fetch('origin','feature');")
check "fetch feature returns 0" "0" "$result"

# ============================================================
echo "=== 9. Multiple commits then push ==="
# ============================================================
"$DB" "$TMPDIR/src.db" <<'ENDSQL'
SELECT dolt_checkout('main');
INSERT INTO users VALUES(6,'frank',40);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add frank');
INSERT INTO users VALUES(7,'grace',33);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add grace');
SELECT dolt_push('origin','main');
.quit
ENDSQL

# remote.db session is on feature (from test 7), must checkout main first
result=$("$DB" "$TMPDIR/remote.db" "SELECT dolt_checkout('main'); SELECT count(*) FROM users;")
check "remote has 6 users after multi-commit push" "0
6" "$result"

# ============================================================
echo "=== 10. Pull multiple commits ==="
# ============================================================
# main has 6 users: alice,bob,charlie,diana,frank,grace (eve is on feature only)
result=$("$DB" "$TMPDIR/clone.db" "SELECT dolt_pull('origin','main'); SELECT count(*) FROM users;")
check "clone has 6 users after multi-commit pull" "0
6" "$result"

# ============================================================
echo "=== 11. Already up-to-date ==="
# ============================================================
result=$("$DB" "$TMPDIR/src.db" "SELECT dolt_push('origin','main');")
check "push when up-to-date returns 0" "0" "$result"

result=$("$DB" "$TMPDIR/src.db" "SELECT dolt_pull('origin','main');")
check "pull when up-to-date returns 0" "0" "$result"

# ============================================================
echo "=== 12. Force push ==="
# ============================================================
# Create diverged history: src resets to an earlier commit
"$DB" "$TMPDIR/src.db" <<'ENDSQL'
DELETE FROM users WHERE id>5;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','revert to 5 users');
.quit
ENDSQL
# Normal push should work (it's still a descendant)
result=$("$DB" "$TMPDIR/src.db" "SELECT dolt_push('origin','main');")
check "push descendant commit succeeds" "0" "$result"

src_head=$("$DB" "$TMPDIR/src.db" "SELECT commit_hash FROM dolt_log LIMIT 1;")
result=$("$DB" "$TMPDIR/remote.db" "SELECT commit_hash FROM dolt_log LIMIT 1;")
check "remote head matches revert push" "$src_head" "$result"

# ============================================================
echo "=== 13. Schema changes push/pull ==="
# ============================================================
"$DB" "$TMPDIR/src.db" <<'ENDSQL'
SELECT dolt_checkout('main');
ALTER TABLE users ADD COLUMN email TEXT;
UPDATE users SET email='alice@test.com' WHERE id=1;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','add email column');
SELECT dolt_push('origin','main');
.quit
ENDSQL

# remote.db session needs checkout to main to see the pushed schema change
src_head=$("$DB" "$TMPDIR/src.db" "SELECT commit_hash FROM dolt_log LIMIT 1;")
result=$("$DB" "$TMPDIR/remote.db" "SELECT commit_hash FROM dolt_log LIMIT 1;")
check "remote head matches schema push" "$src_head" "$result"

result=$("$DB" "$TMPDIR/clone.db" "SELECT dolt_reset('--hard');")
check "clone reset before schema pull" "0" "$result"

result=$("$DB" "$TMPDIR/clone.db" "SELECT dolt_pull('origin','main');")
check "clone pull schema change succeeds" "0" "$result"

result=$("$DB" "$TMPDIR/clone.db" "SELECT email FROM users WHERE id=1;")
check "clone pulled schema change" "alice@test.com" "$result"

# ============================================================
echo "=== 14. Pull full-ancestry fast-forward ==="
# ============================================================
"$DB" "$TMPDIR/anc_src.db" <<ENDSQL
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_branch('side');
SELECT dolt_checkout('side');
INSERT INTO t VALUES(2,'side');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','side commit');
SELECT dolt_checkout('main');
INSERT INTO t VALUES(3,'main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main commit');
SELECT dolt_remote('add','origin','$R/anc_remote.db');
SELECT dolt_push('origin','main');
SELECT dolt_push('origin','side');
.quit
ENDSQL

"$DB" "$TMPDIR/anc_remote_client.db" <<ENDSQL
SELECT dolt_clone('$R/anc_remote.db');
SELECT dolt_checkout('main');
SELECT dolt_merge('side');
SELECT dolt_branch('--force','side','main');
SELECT dolt_push('origin','side');
.quit
ENDSQL

result=$("$DB" "$TMPDIR/anc_src.db" "SELECT dolt_checkout('side'); SELECT dolt_pull('origin','side'); SELECT count(*) FROM t;")
check "pull with non-first-parent ancestry succeeds" "0
0
3" "$result"

result=$("$DB" "$TMPDIR/anc_src.db" "SELECT v FROM t ORDER BY id;")
check "pull with non-first-parent ancestry brings merged rows" "base
side
main" "$result"

# ============================================================
echo "=== 15. Clone into seeded dirty db fails safely ==="
# ============================================================
"$DB" "$TMPDIR/seeded_dirty.db" <<'ENDSQL'
CREATE TABLE local_only(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO local_only VALUES(1,'local');
.quit
ENDSQL

result=$("$DB" "$TMPDIR/seeded_dirty.db" "SELECT dolt_clone('$R/remote.db');" 2>&1)
check "clone into seeded dirty db errors" "1" "$(echo "$result" | grep -c 'uncommitted changes')"

result=$("$DB" "$TMPDIR/seeded_dirty.db" "SELECT count(*) FROM local_only;")
check "clone into seeded dirty db keeps local rows" "1" "$result"

result=$("$DB" "$TMPDIR/seeded_dirty.db" "SELECT count(*) FROM dolt_remotes;")
check "clone into seeded dirty db keeps remotes empty" "0" "$result"

# ============================================================
echo "=== 16. Error cases ==="
# ============================================================
result=$("$DB" "$TMPDIR/src.db" "SELECT dolt_push('nonexistent','main');" 2>&1)
check "push to unknown remote errors" "1" "$(echo "$result" | grep -c 'remote not found')"

result=$("$DB" "$TMPDIR/src.db" "SELECT dolt_push('origin','nonexistent');" 2>&1)
check "push unknown branch errors" "1" "$(echo "$result" | grep -c 'push failed')"

result=$("$DB" "$TMPDIR/src.db" "SELECT dolt_remote('add','origin','$R/duplicate.db');" 2>&1)
check "duplicate remote add errors" "1" "$(echo "$result" | grep -c 'remote already exists')"

result=$("$DB" "$TMPDIR/src.db" "SELECT dolt_remote('remove','missing_remote');" 2>&1)
check "missing remote remove errors" "1" "$(echo "$result" | grep -c 'remote not found')"

# clone into a db that already has data errors
result=$("$DB" "$TMPDIR/src.db" "SELECT dolt_clone('$R/remote.db');" 2>&1)
check "clone into non-empty errors" "1" "$(echo "$result" | grep -c 'not empty')"

result=$("$DB" "$TMPDIR/err.db" "SELECT dolt_clone('/no/scheme');" 2>&1)
check "clone without scheme errors" "1" "$(echo "$result" | grep -c 'file://')"

result=$("$DB" "$TMPDIR/err2.db" "SELECT dolt_clone('file:///nonexistent/path.db');" 2>&1)
check "clone nonexistent file errors" "1" "$(echo "$result" | grep -c 'clone failed')"

# ============================================================
echo "=== 17. Empty table push/clone ==="
# ============================================================
"$DB" "$TMPDIR/empty_src.db" <<ENDSQL
CREATE TABLE empty_t(id INTEGER PRIMARY KEY);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','empty table');
SELECT dolt_remote('add','origin','$R/empty_remote.db');
SELECT dolt_push('origin','main');
.quit
ENDSQL
result=$("$DB" "$TMPDIR/empty_clone.db" "SELECT dolt_clone('$R/empty_remote.db');")
check "clone with empty table returns 0" "0" "$result"

result=$("$DB" "$TMPDIR/empty_clone.db" "SELECT count(*) FROM empty_t;")
check "empty table exists in clone" "0" "$result"

# ============================================================
echo "=== 18. Large data push/clone ==="
# ============================================================
"$DB" "$TMPDIR/large_src.db" <<ENDSQL
CREATE TABLE big(id INTEGER PRIMARY KEY, data TEXT);
WITH RECURSIVE cnt(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM cnt WHERE x<500)
INSERT INTO big SELECT x, printf('row_%d_padding_data_%s', x, hex(randomblob(50))) FROM cnt;
SELECT dolt_add('-A');
SELECT dolt_commit('-m','500 rows');
SELECT dolt_remote('add','origin','$R/large_remote.db');
SELECT dolt_push('origin','main');
.quit
ENDSQL
result=$("$DB" "$TMPDIR/large_clone.db" "SELECT dolt_clone('$R/large_remote.db');")
check "clone 500 rows returns 0" "0" "$result"

result=$("$DB" "$TMPDIR/large_clone.db" "SELECT count(*) FROM big;")
check "clone has 500 rows" "500" "$result"

# ============================================================
echo "=== 19. Push to second remote ==="
# ============================================================
"$DB" "$TMPDIR/src.db" "SELECT dolt_remote('add','mirror','$R/mirror.db'); SELECT dolt_push('mirror','main');" > /dev/null
src_head=$("$DB" "$TMPDIR/src.db" "SELECT commit_hash FROM dolt_log LIMIT 1;")
result=$("$DB" "$TMPDIR/mirror.db" "SELECT commit_hash FROM dolt_log LIMIT 1;")
check "mirror head matches pushed main" "$src_head" "$result"

# ============================================================
echo "=== 20. Clone preserves multiple branches ==="
# ============================================================
result=$("$DB" "$TMPDIR/multi_clone.db" "SELECT dolt_clone('$R/remote.db');")
check "multi-branch clone returns 0" "0" "$result"

result=$("$DB" "$TMPDIR/multi_clone.db" "SELECT count(*) FROM dolt_branches;")
check "clone has 2 branches" "2" "$result"

# ============================================================
echo "=== 21. Deep history push/clone (20 commits) ==="
# ============================================================
"$DB" "$TMPDIR/deep_src.db" <<ENDSQL
CREATE TABLE log(id INTEGER PRIMARY KEY, step INTEGER, msg TEXT);
SELECT dolt_add('-A');
SELECT dolt_commit('-m','create log table');
.quit
ENDSQL
for i in $(seq 1 20); do
  "$DB" "$TMPDIR/deep_src.db" "INSERT INTO log VALUES($i,$i,'step $i'); SELECT dolt_add('-A'); SELECT dolt_commit('-m','step $i');" > /dev/null
done
"$DB" "$TMPDIR/deep_src.db" "SELECT dolt_remote('add','origin','$R/deep_remote.db'); SELECT dolt_push('origin','main');" > /dev/null

result=$("$DB" "$TMPDIR/deep_clone.db" "SELECT dolt_clone('$R/deep_remote.db');")
check "deep clone returns 0" "0" "$result"

result=$("$DB" "$TMPDIR/deep_clone.db" "SELECT count(*) FROM log;")
check "deep clone has 20 rows" "20" "$result"

# Count commits in clone (22 = seed + create + 20 inserts)
result=$("$DB" "$TMPDIR/deep_clone.db" "SELECT count(*) FROM dolt_log;")
check "deep clone has 22 commits" "22" "$result"

# ============================================================
echo "=== 22. Diverged branches: push both, clone gets all ==="
# ============================================================
"$DB" "$TMPDIR/div_src.db" <<ENDSQL
CREATE TABLE items(id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO items VALUES(1,'base');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base commit');
SELECT dolt_branch('branchA');
SELECT dolt_branch('branchB');
.quit
ENDSQL

# Build 10 commits on branchA
"$DB" "$TMPDIR/div_src.db" "SELECT dolt_checkout('branchA');" > /dev/null
for i in $(seq 2 11); do
  "$DB" "$TMPDIR/div_src.db" "INSERT INTO items VALUES($i,'A_$i'); SELECT dolt_add('-A'); SELECT dolt_commit('-m','A step $i');" > /dev/null
done

# Build 10 different commits on branchB
"$DB" "$TMPDIR/div_src.db" "SELECT dolt_checkout('branchB');" > /dev/null
for i in $(seq 100 109); do
  "$DB" "$TMPDIR/div_src.db" "INSERT INTO items VALUES($i,'B_$i'); SELECT dolt_add('-A'); SELECT dolt_commit('-m','B step $i');" > /dev/null
done

# Push all three branches
"$DB" "$TMPDIR/div_src.db" <<ENDSQL
SELECT dolt_remote('add','origin','$R/div_remote.db');
SELECT dolt_push('origin','main');
SELECT dolt_push('origin','branchA');
SELECT dolt_push('origin','branchB');
.quit
ENDSQL

# Clone and verify all branches
result=$("$DB" "$TMPDIR/div_clone.db" "SELECT dolt_clone('$R/div_remote.db');")
check "diverged clone returns 0" "0" "$result"

result=$("$DB" "$TMPDIR/div_clone.db" "SELECT count(*) FROM dolt_branches;")
check "div clone has 3 branches" "3" "$result"

result=$("$DB" "$TMPDIR/div_clone.db" "SELECT dolt_checkout('branchA'); SELECT count(*) FROM items;")
check "div clone branchA has 11 items" "0
11" "$result"

result=$("$DB" "$TMPDIR/div_clone.db" "SELECT dolt_checkout('branchB'); SELECT count(*) FROM items;")
check "div clone branchB has 11 items" "0
11" "$result"

result=$("$DB" "$TMPDIR/div_clone.db" "SELECT dolt_checkout('main'); SELECT count(*) FROM items;")
check "div clone main has 1 item" "0
1" "$result"

# ============================================================
echo "=== 23. Incremental fetch: push more, fetch only new ==="
# ============================================================
# Add 5 more commits on branchA in source
"$DB" "$TMPDIR/div_src.db" "SELECT dolt_checkout('branchA');" > /dev/null
for i in $(seq 12 16); do
  "$DB" "$TMPDIR/div_src.db" "INSERT INTO items VALUES($i,'A_$i'); SELECT dolt_add('-A'); SELECT dolt_commit('-m','A step $i');" > /dev/null
done
"$DB" "$TMPDIR/div_src.db" "SELECT dolt_push('origin','branchA');" > /dev/null

# Fetch in clone (should only transfer 5 new commits, not all)
result=$("$DB" "$TMPDIR/div_clone.db" "SELECT dolt_fetch('origin','branchA');")
check "incremental fetch returns 0" "0" "$result"

# Pull to see new data
"$DB" "$TMPDIR/div_clone.db" "SELECT dolt_checkout('branchA');" > /dev/null
result=$("$DB" "$TMPDIR/div_clone.db" "SELECT dolt_pull('origin','branchA'); SELECT count(*) FROM items;")
check "incremental pull has 16 items" "0
16" "$result"

# ============================================================
echo "=== 24. Push after merge ==="
# ============================================================
"$DB" "$TMPDIR/merge_src.db" <<ENDSQL
CREATE TABLE doc(id INTEGER PRIMARY KEY, text TEXT);
INSERT INTO doc VALUES(1,'original');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','base');
SELECT dolt_branch('edit');
SELECT dolt_checkout('edit');
UPDATE doc SET text='edited' WHERE id=1;
INSERT INTO doc VALUES(2,'new from edit');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','edit changes');
SELECT dolt_checkout('main');
INSERT INTO doc VALUES(3,'new from main');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','main changes');
SELECT dolt_merge('edit');
SELECT dolt_remote('add','origin','$R/merge_remote.db');
SELECT dolt_push('origin','main');
.quit
ENDSQL

# Clone the merged repo and verify history
result=$("$DB" "$TMPDIR/merge_clone.db" "SELECT dolt_clone('$R/merge_remote.db');")
check "merge clone returns 0" "0" "$result"

result=$("$DB" "$TMPDIR/merge_clone.db" "SELECT count(*) FROM doc;")
check "merge clone has 3 docs" "3" "$result"

result=$("$DB" "$TMPDIR/merge_clone.db" "SELECT count(*) FROM dolt_log;")
check "merge clone has commit history" "5" "$result"

# ============================================================
echo "=== 25. Round-trip: A→remote→B→remote→A (3 hops) ==="
# ============================================================
"$DB" "$TMPDIR/hop_a.db" <<ENDSQL
CREATE TABLE chain(id INTEGER PRIMARY KEY, who TEXT);
INSERT INTO chain VALUES(1,'hop_a');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','from A');
SELECT dolt_remote('add','origin','$R/hop_remote.db');
SELECT dolt_push('origin','main');
.quit
ENDSQL

# B clones, adds, pushes
result=$("$DB" "$TMPDIR/hop_b.db" "SELECT dolt_clone('$R/hop_remote.db');")
check "hop B clone ok" "0" "$result"

"$DB" "$TMPDIR/hop_b.db" <<'ENDSQL'
INSERT INTO chain VALUES(2,'hop_b');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','from B');
SELECT dolt_push('origin','main');
.quit
ENDSQL

# A pulls B's changes
result=$("$DB" "$TMPDIR/hop_a.db" "SELECT dolt_pull('origin','main'); SELECT count(*) FROM chain;")
check "A pulled B's data" "0
2" "$result"

# A adds more, pushes
"$DB" "$TMPDIR/hop_a.db" <<'ENDSQL'
INSERT INTO chain VALUES(3,'hop_a_again');
SELECT dolt_add('-A');
SELECT dolt_commit('-m','from A again');
SELECT dolt_push('origin','main');
.quit
ENDSQL

# B pulls A's latest
result=$("$DB" "$TMPDIR/hop_b.db" "SELECT dolt_pull('origin','main'); SELECT count(*) FROM chain;")
check "B pulled A's latest" "0
3" "$result"

result=$("$DB" "$TMPDIR/hop_b.db" "SELECT who FROM chain ORDER BY id;")
check "B has full chain" "hop_a
hop_b
hop_a_again" "$result"

# ============================================================
echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
[ "$fail" -eq 0 ] && exit 0 || exit 1
