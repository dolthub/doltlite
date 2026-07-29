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

# Compare semantics, not representation. Three things legitimately differ and
# are folded away here:
#   * is_merging is a MySQL boolean in Dolt (true/false) and an INTEGER in
#     DoltLite (1/0).
#   * commit hashes are base32 in Dolt and hex in DoltLite, so any hash-shaped
#     field collapses to <HASH>. Because a raw-hash merge spec makes source
#     equal source_commit, collapsing both still proves that relationship.
#   * unmerged_tables is built from a Go map in Dolt, so its order is
#     unspecified for more than one table; both sides get sorted.
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

# Both sides read dolt_merge_status in the same session that ran the merge:
# Dolt's CALL dolt_checkout only moves the session, so a follow-up `dolt sql`
# invocation would report on the repo's checked-out branch instead of the one
# merged into. Cross-connection persistence is covered by
# test/doltlite_merge_status.sh.
#
# A conflicted merge is never committable in DoltLite, so the status has to be
# read inside the transaction that produced it. Dolt would let the commit through
# behind dolt_allow_commit_conflicts, but reading in-session gives the same answer
# there, so both sides are compared at the same point and the projection is part
# of the script rather than a follow-up connection.
oracle() {
  local name="$1" setup="$2" merge="${3:-}" after="${4:-}"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  local dl_script="$setup"
  local dt_script
  # Dolt gates the two unfinished-merge outcomes behind separate session vars:
  # conflicts on dolt_allow_commit_conflicts, constraint violations on
  # dolt_force_transaction_commit. DoltLite's equivalent for both is running the
  # merge in an explicit transaction.
  dt_script="SET @@dolt_allow_commit_conflicts=1;
SET @@dolt_force_transaction_commit=1;
$(vc_oracle_translate_for_dolt "$setup")"
  if [ -n "$merge" ]; then
    # `after` runs inside DoltLite's transaction: an autocommit merge that
    # conflicts is rolled back whole, so anything meant to observe or resolve
    # the conflict has to be in the same transaction as the merge.
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

# No merge anywhere in the picture: one row, is_merging false, rest NULL.
oracle "clean_no_merge" "$BASE"

# A conflicted merge: source names the spec, target names the merged-into ref,
# unmerged_tables lists the conflicted table.
oracle "conflict_active" "$BASE
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v = 'theirs' WHERE id = 1;
SELECT dolt_commit('-Am', 'theirs');
SELECT dolt_checkout('main');
UPDATE t SET v = 'ours' WHERE id = 1;
SELECT dolt_commit('-Am', 'ours');" "feature"

# A fast-forward leaves no merge active.
oracle "fast_forward_no_merge" "$BASE
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (3, 'ahead');
SELECT dolt_commit('-Am', 'ahead');
SELECT dolt_checkout('main');" "feature"

# A clean three-way merge also leaves no merge active.
oracle "clean_three_way_no_merge" "$BASE
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
INSERT INTO t VALUES (3, 'theirs');
SELECT dolt_commit('-Am', 'theirs');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (4, 'ours');
SELECT dolt_commit('-Am', 'ours');" "feature"

# Two conflicted tables: both appear, order-insensitively.
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

# Merging into a branch other than the default: target must follow it.
oracle "target_non_default_branch" "$BASE
SELECT dolt_branch('dev');
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v = 'theirs' WHERE id = 1;
SELECT dolt_commit('-Am', 'theirs');
SELECT dolt_checkout('dev');
UPDATE t SET v = 'ours' WHERE id = 1;
SELECT dolt_commit('-Am', 'ours');" "feature"

# Resolving the conflict and committing ends the merge on both engines.
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

# Conflicts resolved but not yet committed: the merge is still active and
# unmerged_tables is the empty string, not NULL.
oracle "resolved_but_uncommitted" "$BASE
SELECT dolt_branch('feature');
SELECT dolt_checkout('feature');
UPDATE t SET v = 'theirs' WHERE id = 1;
SELECT dolt_commit('-Am', 'theirs');
SELECT dolt_checkout('main');
UPDATE t SET v = 'ours' WHERE id = 1;
SELECT dolt_commit('-Am', 'ours');" "feature" \
"SELECT dolt_conflicts_resolve('--ours', 't');"

# A merge stopped only by constraint violations, with no row conflict anywhere,
# is still an unfinished merge on both engines.
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

# A merge already recorded stays reported after the conflict is left in place
# and a second, unrelated table is changed on top of it.
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

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
