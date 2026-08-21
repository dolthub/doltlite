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

# Fold representation: is_merging 1/0 vs true/false; hashes to <HASH>;
# unmerged_tables sorted (Dolt's Go map is unordered).
normalize() {
  tr -d '\r"' \
    | awk -F'|' '
        function hashish(s) {
          return s ~ /^[0-9a-f]{40}$/ || s ~ /^[0-9a-v]{32}$/
        }
        NF < 5 { print; next }
        {
          merging = ($1 == "true" || $1 == "1") ? 1 : 0
          src = hashish($2) ? "<HASH>" : $2
          cmt = hashish($3) ? "<HASH>" : $3
          n = split($5, parts, /, /)
          for (i = 1; i <= n; i++) {
            for (j = i + 1; j <= n; j++) {
              if (parts[j] < parts[i]) { t = parts[i]; parts[i] = parts[j]; parts[j] = t }
            }
          }
          tables = ""
          for (i = 1; i <= n; i++) tables = tables (i > 1 ? ", " : "") parts[i]
          print merging "|" src "|" cmt "|" $4 "|" tables
        }
      '
}

DL_PROJECT="SELECT is_merging || '|' || coalesce(source,'~') || '|' || \
coalesce(source_commit,'~') || '|' || coalesce(target,'~') || '|' || \
coalesce(unmerged_tables,'~') FROM dolt_merge_status"

DT_PROJECT="SELECT concat(is_merging, '|', coalesce(source,'~'), '|', \
coalesce(source_commit,'~'), '|', coalesce(target,'~'), '|', \
coalesce(unmerged_tables,'~')) FROM dolt_merge_status"

# Read status in the same session as the merge (Dolt checkout is session-only).
# Conflicted merges are uncommittable in DoltLite, so status is in-txn.
oracle() {
  local name="$1" setup="$2" merge="${3:-}" after="${4:-}"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_script="$setup"
  local dt_script
  dt_script="SET @@dolt_allow_commit_conflicts=1;
SET @@dolt_force_transaction_commit=1;
$(vc_oracle_translate_for_dolt "$setup")"
  if [ -n "$merge" ]; then
    # Autocommit conflicted merge rolls back whole; observe/resolve in the same txn.
    dl_script="$dl_script
BEGIN;
SELECT dolt_merge('$merge');
$after
$DL_PROJECT;
ROLLBACK;"
    dt_script="$dt_script
CALL dolt_merge('$merge');
$(vc_oracle_translate_for_dolt "$after")
$DT_PROJECT;"
  else
    dl_script="$dl_script
$DL_PROJECT;"
    dt_script="$dt_script
$DT_PROJECT;"
  fi

  local dl_out
  dl_out=$(printf '.headers off\n.mode list\n%s\n' "$dl_script" \
           | "$DOLTLITE" "$dir/dl/db" 2>"$dir/dl.err" \
           | grep -F '|' \
           | tail -1 \
           | normalize)

  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    printf '%s\n' "$dt_script" | "$DOLT" sql -r csv 2>"$dir/dt.err"
  ) > "$dir/dt.raw"

  local dt_out
  dt_out=$(grep -F '|' "$dir/dt.raw" | tail -1 | normalize)

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

BASE="CREATE TABLE t(id INT PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, 'base'), (2, 'base');
SELECT dolt_commit('-Am', 'base');"

oracle "clean_no_merge" "$BASE"

oracle "conflict_active" "$BASE
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v = 'theirs' WHERE id = 1;
SELECT dolt_commit('-Am', 'theirs');
SELECT dolt_checkout('main');
UPDATE t SET v = 'ours' WHERE id = 1;
SELECT dolt_commit('-Am', 'ours');" "feature"

oracle "fast_forward_no_merge" "$BASE
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (3, 'ahead');
SELECT dolt_commit('-Am', 'ahead');
SELECT dolt_checkout('main');" "feature"

oracle "clean_three_way_no_merge" "$BASE
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (3, 'theirs');
SELECT dolt_commit('-Am', 'theirs');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (4, 'ours');
SELECT dolt_commit('-Am', 'ours');" "feature"

oracle "two_conflicted_tables" "CREATE TABLE zeta(id INT PRIMARY KEY, v TEXT);
CREATE TABLE alpha(id INT PRIMARY KEY, v TEXT);
INSERT INTO zeta VALUES (1, 'base');
INSERT INTO alpha VALUES (1, 'base');
SELECT dolt_commit('-Am', 'base');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE zeta SET v = 'theirs';
UPDATE alpha SET v = 'theirs';
SELECT dolt_commit('-Am', 'theirs');
SELECT dolt_checkout('main');
UPDATE zeta SET v = 'ours';
UPDATE alpha SET v = 'ours';
SELECT dolt_commit('-Am', 'ours');" "feature"

oracle "target_non_default_branch" "$BASE
SELECT dolt_branch('dev');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v = 'theirs' WHERE id = 1;
SELECT dolt_commit('-Am', 'theirs');
SELECT dolt_checkout('dev');
UPDATE t SET v = 'ours' WHERE id = 1;
SELECT dolt_commit('-Am', 'ours');" "feature"

oracle "cleared_after_resolve" "$BASE
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v = 'theirs' WHERE id = 1;
SELECT dolt_commit('-Am', 'theirs');
SELECT dolt_checkout('main');
UPDATE t SET v = 'ours' WHERE id = 1;
SELECT dolt_commit('-Am', 'ours');" "feature" \
"SELECT dolt_conflicts_resolve('--ours', 't');
SELECT dolt_commit('-m', 'resolved');"

# Resolved but uncommitted: still merging; unmerged_tables is "" not NULL.
oracle "resolved_but_uncommitted" "$BASE
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v = 'theirs' WHERE id = 1;
SELECT dolt_commit('-Am', 'theirs');
SELECT dolt_checkout('main');
UPDATE t SET v = 'ours' WHERE id = 1;
SELECT dolt_commit('-Am', 'ours');" "feature" \
"SELECT dolt_conflicts_resolve('--ours', 't');"

# CV-only merge (no row conflict) is still unfinished.
oracle "cv_only_merge_is_merging" "CREATE TABLE t(id INT PRIMARY KEY, v INT UNIQUE);
INSERT INTO t VALUES (1, 10);
SELECT dolt_commit('-Am', 'base');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (2, 99);
SELECT dolt_commit('-Am', 'theirs');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (3, 99);
SELECT dolt_commit('-Am', 'ours');" "feature"

oracle "conflict_survives_later_edit" "$BASE
CREATE TABLE other(id INT PRIMARY KEY, v TEXT);
SELECT dolt_commit('-Am', 'other');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v = 'theirs' WHERE id = 1;
SELECT dolt_commit('-Am', 'theirs');
SELECT dolt_checkout('main');
UPDATE t SET v = 'ours' WHERE id = 1;
SELECT dolt_commit('-Am', 'ours');" "feature"

# Partial resolve must not blank the merge source (commit still owes a 2nd parent).
oracle "source_survives_partial_conflict_resolution" "$BASE
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v = 'theirs' WHERE id = 1;
UPDATE t SET v = 'theirs' WHERE id = 2;
SELECT dolt_commit('-Am', 'theirs');
SELECT dolt_checkout('main');
UPDATE t SET v = 'ours' WHERE id = 1;
UPDATE t SET v = 'ours' WHERE id = 2;
SELECT dolt_commit('-Am', 'ours');" "feature" \
"DELETE FROM dolt_conflicts_t WHERE our_id = 1;"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
