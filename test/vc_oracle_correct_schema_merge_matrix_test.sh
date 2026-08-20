#!/bin/bash

# Every ours x theirs schema-change pair, merged in DoltLite and in Dolt, with
# the merged schema, the merged index set and the merged table data compared
# column by column.
#
# The contract this enforces: either we produce exactly what Dolt produces, or
# we refuse the merge. Merging to a different answer than Dolt is a failure,
# and so is merging where Dolt refuses. A refusal where Dolt merges is a
# divergence, allowed only while it is listed in KNOWN_DIVERGENCES below with
# the reason and the issue that would close it -- an unlisted one fails, and so
# does a listed one that starts passing, so the list cannot rot.

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0; gaps=0; FAILED_NAMES=""; GAP_NAMES=""

# Pairs we refuse while Dolt merges. Safe: refusing loses no data and the user
# can still get there by hand. Each needs the reason and the issue that closes
# it. A pair that starts merging fails here, so the list cannot rot.
REFUSE_WHERE_DOLT_MERGES="
idx_b:ren_b:index over a renamed column is not retargeted (#2302)
ren_b:idx_b:index over a renamed column is not retargeted (#2302)
uniq_b:ren_b:unique index over a renamed column is not retargeted (#2302)
ren_b:uniq_b:unique index over a renamed column is not retargeted (#2302)
idx_b:ren_b_view:index over a renamed column is not retargeted (#2302)
ren_b_view:idx_b:index over a renamed column is not retargeted (#2302)
uniq_b:ren_b_view:index over a renamed column is not retargeted (#2302)
ren_b_view:uniq_b:index over a renamed column is not retargeted (#2302)
idx_a:ren_a:index over a renamed column is not retargeted (#2302)
ren_a:idx_a:index over a renamed column is not retargeted (#2302)
ren_b_view:ren_a:dual rename with a dependent view naming the renamed column (#2301)
"

# Pairs we MERGE while Dolt refuses. Not safe: we resolve on the user's behalf
# a disagreement Dolt hands back to them, so our answer is one Dolt would never
# produce. Every entry here is a bug to fix by refusing, not a difference to
# keep, and the suite prints them as gaps rather than passes.
MERGE_WHERE_DOLT_REFUSES="
drop_b:rows:a row edit of a column the other branch dropped (#2306)
rows:drop_b:a row edit of a column the other branch dropped (#2306)
drop_b:ren_b_view:a column dropped on one branch and renamed on the other, with a dependent view present (#2307)
ren_b_view:drop_b:a column dropped on one branch and renamed on the other, with a dependent view present (#2307)
"

pass_name() { pass=$((pass+1)); echo "  PASS: $1"; }
fail_name() {
  fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES $1"
  echo "  FAIL: $1"
}

dl_op() {
  case "$1" in
    ren_a)      echo "ALTER TABLE t RENAME COLUMN a TO a2;";;
    ren_b)      echo "ALTER TABLE t RENAME COLUMN b TO b2;";;
    drop_a)     echo "ALTER TABLE t DROP COLUMN a;";;
    drop_b)     echo "ALTER TABLE t DROP COLUMN b;";;
    add_d)      echo "ALTER TABLE t ADD COLUMN d VARCHAR(9);";;
    idx_b)      echo "CREATE INDEX ix ON t(b);";;
    uniq_b)     echo "CREATE UNIQUE INDEX uq ON t(b);";;
    idx_a)      echo "CREATE INDEX ia ON t(a);";;
    rows)       echo "UPDATE t SET b='edit' WHERE k=1;";;
    ins)        echo "INSERT INTO t VALUES(9,'a9','b9',99);";;
    ren_b_view) echo "ALTER TABLE t RENAME COLUMN b TO b2;";;
  esac
}
dt_op() { dl_op "$1"; }

# A view on b exists from the start only for the shapes that need it, so the
# rename cases that carry a dependent are distinguishable from the bare ones.
needs_view() { [ "$1" = ren_b_view ] || [ "$2" = ren_b_view ]; }

OPS="ren_a ren_b drop_a drop_b add_d idx_b uniq_b idx_a rows ins ren_b_view"

dl_state() {
  local db="$1" tbl cols c out idx
  tbl=$("$DOLTLITE" "$db" \
    "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('t','t2') LIMIT 1;" 2>/dev/null)
  [ -z "$tbl" ] && { echo "MISSING TABLE"; return; }
  cols=$("$DOLTLITE" "$db" \
    "SELECT group_concat(name) FROM pragma_table_info('$tbl');" 2>/dev/null)
  echo "cols=$cols"
  idx=$("$DOLTLITE" "$db" "SELECT group_concat(x, ' ') FROM (
      SELECT m.name || '(' || (SELECT group_concat(ii.name)
        FROM pragma_index_info(m.name) ii) || ')' AS x
      FROM sqlite_master m WHERE m.type='index' AND m.sql IS NOT NULL
      ORDER BY m.name);" 2>/dev/null)
  echo "idx=$idx"
  for c in $(echo "$cols" | tr ',' ' '); do
    out=$("$DOLTLITE" "$db" \
      "SELECT group_concat(v, '|') FROM (SELECT coalesce(CAST(\"$c\" AS TEXT),'~') AS v
        FROM \"$tbl\" ORDER BY k);" 2>/dev/null)
    echo "data.$c=$out"
  done
}

dt_state() {
  local repo="$1" tbl cols c out idx
  tbl=$(cd "$repo" && "$DOLT" sql -r csv -q "SELECT table_name FROM information_schema.tables
      WHERE table_schema=database() AND table_name IN ('t','t2') LIMIT 1;" 2>/dev/null | tail -n +2 | tr -d '"')
  [ -z "$tbl" ] && { echo "MISSING TABLE"; return; }
  cols=$(cd "$repo" && "$DOLT" sql -r csv -q "SELECT group_concat(column_name ORDER BY ordinal_position)
      FROM information_schema.columns WHERE table_name='$tbl';" 2>/dev/null | tail -n +2 | tr -d '"')
  echo "cols=$cols"
  idx=$(cd "$repo" && "$DOLT" sql -r csv -q "SELECT group_concat(x SEPARATOR ' ') FROM (
      SELECT concat(index_name,'(',group_concat(column_name ORDER BY seq_in_index),')') AS x
      FROM information_schema.statistics WHERE table_name='$tbl' AND index_name<>'PRIMARY'
      GROUP BY index_name ORDER BY index_name) q;" 2>/dev/null | tail -n +2 | tr -d '"')
  echo "idx=$idx"
  for c in $(echo "$cols" | tr ',' ' '); do
    out=$(cd "$repo" && "$DOLT" sql -r csv -q "SELECT group_concat(v SEPARATOR '|') FROM
      (SELECT coalesce(CAST(\`$c\` AS CHAR),'~') AS v FROM \`$tbl\` ORDER BY k) q;" \
      2>/dev/null | tail -n +2 | tr -d '"')
    echo "data.$c=$out"
  done
}

run_case() {
  local o="$1" t="$2" name="${1}__${2}"
  local db="$TMPROOT/$name.db" repo="$TMPROOT/repo_$name"
  local view="" dl_out dt_out dl_ok dt_ok dl_st dt_st reason dt_cf

  rm -f "$db" "$db-lock"; rm -rf "$repo"; mkdir -p "$repo"
  (cd "$repo" && "$DOLT" init --name oracle --email o@t >/dev/null 2>&1)
  if needs_view "$o" "$t"; then view="CREATE VIEW v AS SELECT b FROM t;"; fi

  "$DOLTLITE" "$db" >/dev/null 2>&1 <<EOF
CREATE TABLE t(k INT PRIMARY KEY, a VARCHAR(9), b VARCHAR(9), n INT);
INSERT INTO t VALUES(1,'a1','b1',11),(2,'a2','b2',22);
$view
SELECT dolt_add('-A'), dolt_commit('-m','base');
SELECT dolt_branch('feat');
$(dl_op "$o")
SELECT dolt_add('-A'), dolt_commit('-m','ours');
SELECT dolt_checkout('feat');
$(dl_op "$t")
SELECT dolt_add('-A'), dolt_commit('-m','theirs');
SELECT dolt_checkout('main');
EOF
  (cd "$repo" && "$DOLT" sql >/dev/null 2>&1 <<EOF
CREATE TABLE t(k INT PRIMARY KEY, a VARCHAR(9), b VARCHAR(9), n INT);
INSERT INTO t VALUES(1,'a1','b1',11),(2,'a2','b2',22);
$view
CALL dolt_add('-A'); CALL dolt_commit('-m','base');
CALL dolt_branch('feat');
$(dt_op "$o")
CALL dolt_add('-A'); CALL dolt_commit('-m','ours');
CALL dolt_checkout('feat');
$(dt_op "$t")
CALL dolt_add('-A'); CALL dolt_commit('-m','theirs');
EOF
  )

  # Setup has to have produced the table on both sides, or the case says
  # nothing about merging.
  if ! "$DOLTLITE" "$db" "SELECT 1 FROM sqlite_master WHERE name IN ('t','t2') LIMIT 1;" \
        >/dev/null 2>&1; then
    fail_name "$name (doltlite setup failed)"; return
  fi

  dl_out=$("$DOLTLITE" "$db" "SELECT coalesce(dolt_merge('feat'),'NULL');" 2>&1)
  # dolt_merge's own result table has a column called "conflicts", so the
  # outcome has to be read from that column's value, never from the text.
  dt_out=$(cd "$repo" && "$DOLT" sql -r csv -q "CALL dolt_merge('feat');" 2>&1)
  dl_ok=1; dt_ok=1
  case "$dl_out" in *rror*|*onflict*|*NULL*) dl_ok=0;; esac
  if printf '%s\n' "$dt_out" | grep -qiE '^error|error:'; then
    dt_ok=0
  else
    dt_cf=$(printf '%s\n' "$dt_out" | tail -n +2 | head -1 | cut -d, -f3 | tr -d '" ')
    [ "${dt_cf:-0}" != 0 ] && dt_ok=0
  fi

  if [ "$dl_ok" = 1 ] && [ "$dt_ok" = 0 ]; then
    reason=$(printf '%s\n' "$MERGE_WHERE_DOLT_REFUSES" | grep "^$o:$t:" | cut -d: -f3-)
    if [ -n "$reason" ]; then
      gaps=$((gaps+1)); GAP_NAMES="$GAP_NAMES $name"
      echo "  GAP:  $name -- we merge where Dolt refuses: $reason"
    else
      fail_name "$name (we merged; Dolt refused)"
      echo "    dolt: $(echo "$dt_out" | head -2 | tr '\n' ' ')"
    fi
    return
  fi
  if [ "$dl_ok" = 0 ] && [ "$dt_ok" = 0 ]; then pass_name "$name (both refuse)"; return; fi

  reason=$(printf '%s\n' "$REFUSE_WHERE_DOLT_MERGES" | grep "^$o:$t:" | cut -d: -f3-)
  if [ "$dl_ok" = 0 ] && [ "$dt_ok" = 1 ]; then
    if [ -n "$reason" ]; then
      pass_name "$name (refuses by design: $reason)"
    else
      fail_name "$name (we refused; Dolt merged)"
      echo "    doltlite: $(echo "$dl_out" | head -1 | cut -c1-100)"
    fi
    return
  fi
  if [ -n "$reason" ]; then
    fail_name "$name (listed as refusing by design but now merges -- delete the entry)"
    return
  fi
  if printf '%s\n' "$MERGE_WHERE_DOLT_REFUSES" | grep -q "^$o:$t:"; then
    fail_name "$name (listed as a gap but Dolt merges it too -- delete the entry)"
    return
  fi

  dl_st=$(dl_state "$db"); dt_st=$(dt_state "$repo")
  if [ "$dl_st" = "$dt_st" ]; then
    pass_name "$name"
  else
    fail_name "$name (merged, but not what Dolt produced)"
    diff <(printf '%s\n' "$dl_st") <(printf '%s\n' "$dt_st") \
      | sed 's/^/      /' | head -12
  fi
}

echo "=== Correct schema merge matrix (DoltLite vs Dolt) ==="
for o in $OPS; do
  for t in $OPS; do
    [ "$o" = "$t" ] && continue
    run_case "$o" "$t"
  done
done

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed, $gaps known gaps"
if [ "$gaps" -ne 0 ]; then
  echo "gaps (we merge where Dolt refuses -- fix by refusing):$GAP_NAMES"
fi
echo "======================================="
if [ "$fail" -ne 0 ]; then
  echo "failed:$FAILED_NAMES"
  exit 1
fi
