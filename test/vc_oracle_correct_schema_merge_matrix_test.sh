#!/bin/bash

# Ours×theirs schema pairs vs Dolt. Match or refuse; a different answer or
# merging where Dolt refuses fails. A refusal where Dolt merges is listed
# below; unlisted, or a listed pair that starts passing, fails.

set -u

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0; gaps=0; corrupt=0; differs=0; skipped=0; FAILED_NAMES=""; GAP_NAMES=""; CORRUPT_NAMES=""; DIFFER_NAMES=""; SKIP_NAMES=""

# Refuse while Dolt merges. A pair that starts merging fails here.
# Dolt keeps a trigger on a renamed-away table name; no loadable SQLite
# catalog can hold that, so the refusal stands as a documented divergence.
REFUSE_WHERE_DOLT_MERGES="
trig:ren_tbl:Dolt keeps a trigger on the old table name, which no loadable catalog can hold (#2333)
ren_tbl:trig:Dolt keeps a trigger on the old table name, which no loadable catalog can hold (#2333)
"

# Merge while Dolt refuses: a bug to fix by refusing, printed as a gap.
MERGE_WHERE_DOLT_REFUSES="
"

# "disk image is malformed" is never an acceptable refusal; listed so rot is visible.
CORRUPT_TODAY="
"

# Both merge, different answers: match Dolt or refuse.
MERGE_DIFFERS_TODAY="
"

pass_name() { pass=$((pass+1)); echo "  PASS: $1"; }
fail_name() {
  fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES $1"
  echo "  FAIL: $1"
}

# Some pairs only fail with an unrelated index already on the table.
base_extra() {
  case "$1" in
    drop_b_with_index|idx_b_with_index) echo "CREATE INDEX ix0 ON t(a);";;
    *) echo "";;
  esac
}

dl_op() {
  case "$1" in
    drop_b_with_index) echo "ALTER TABLE t DROP COLUMN b;";;
    trig)       echo "CREATE TRIGGER tg AFTER INSERT ON t FOR EACH ROW BEGIN UPDATE t SET n=n WHERE k=NEW.k; END;";;
    ren_tbl)    echo "ALTER TABLE t RENAME TO t2;";;
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
dt_op() {
  case "$1" in
    trig) echo "CREATE TRIGGER tg BEFORE INSERT ON t FOR EACH ROW SET NEW.n = NEW.n;";;
    *)    dl_op "$1";;
  esac
}

# View on b only for rename cases that carry a dependent.
needs_view() { [ "$1" = ren_b_view ] || [ "$2" = ren_b_view ]; }

OPS="ren_a ren_b drop_a drop_b add_d idx_b uniq_b idx_a rows ins ren_b_view drop_b_with_index trig ren_tbl"

# Dual rename/index/view/trigger bugs only show with a dependent already present.
BASE_VARIANT=""
BASE_VARIANTS="bidx bview btrig"
# Ops that can move a column/table out from under a dependent (not the full matrix).
VARIANT_OPS="ren_a ren_b drop_a drop_b idx_a ren_tbl rows"

base_variant_dl() {
  case "$1" in
    bidx)  echo "CREATE INDEX ix0 ON t(a);";;
    bview) echo "CREATE VIEW v0 AS SELECT a FROM t;";;
    btrig) echo "CREATE TRIGGER tg0 AFTER INSERT ON t FOR EACH ROW BEGIN UPDATE t SET a=a WHERE k=NEW.k; END;";;
    *)     echo "";;
  esac
}
base_variant_dt() {
  case "$1" in
    btrig) echo "CREATE TRIGGER tg0 BEFORE INSERT ON t FOR EACH ROW SET NEW.a = NEW.a;";;
    *)     base_variant_dl "$1";;
  esac
}

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
  # Views and triggers, by name and the table they run on. The definitions
  # themselves are not comparable across engines -- a trigger body is written in
  # each engine's own dialect -- but a dependent that the merge dropped, kept
  # when it should have gone, or left pointing at another table shows up here.
  # Without this a merge could match on columns, indexes and rows while its
  # view or trigger was missing.
  dep=$("$DOLTLITE" "$db" "SELECT group_concat(x, ' ') FROM (
      SELECT type || ':' || name || '->' || coalesce(tbl_name,'') AS x
      FROM sqlite_master WHERE type IN ('view','trigger') ORDER BY x);" 2>&1)
  case "$(printf '%s' "$dep" | tr 'A-Z' 'a-z')" in
    *error*|*"table not found"*|*malformed*) dep=UNREADABLE;; esac
  echo "dep=$dep"
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
  # Triggers come from SHOW TRIGGERS, not information_schema.triggers: that
  # view resolves the trigger body and fails outright once a rename has left it
  # naming a column that is gone (dolthub/dolt#11587), which took five
  # comparable cases out of the suite. SHOW TRIGGERS returns the stored
  # definition without resolving it. The unreadable guard stays as a backstop.
  dep_trg=$(cd "$repo" && "$DOLT" sql -r csv -q "SHOW TRIGGERS;" 2>&1)
  case "$(printf '%s' "$dep_trg" | tr 'A-Z' 'a-z')" in
    *error*|*"table not found"*|*malformed*) dep=UNREADABLE;;
    *)
      dep_trg=$(printf '%s\n' "$dep_trg" | tail -n +2 \
        | awk -F, 'NF>2 {gsub(/"/,"",$1); gsub(/"/,"",$3); print "trigger:" $1 "->" $3}')
      dep_vw=$(cd "$repo" && "$DOLT" sql -r csv -q "SELECT table_name
          FROM information_schema.tables
          WHERE table_schema=database() AND table_type='VIEW';" 2>/dev/null \
        | tail -n +2 | tr -d '"' | awk 'NF {print "view:" $1 "->" $1}')
      dep=$(printf '%s\n%s\n' "$dep_trg" "$dep_vw" | grep -v '^$' | sort \
        | tr '\n' ' ' | sed 's/ *$//')
      ;;
  esac
  echo "dep=$dep"
  for c in $(echo "$cols" | tr ',' ' '); do
    out=$(cd "$repo" && "$DOLT" sql -r csv -q "SELECT group_concat(v SEPARATOR '|') FROM
      (SELECT coalesce(CAST(\`$c\` AS CHAR),'~') AS v FROM \`$tbl\` ORDER BY k) q;" \
      2>/dev/null | tail -n +2 | tr -d '"')
    echo "data.$c=$out"
  done
}

run_case() {
  local o="$1" t="$2"
  local name="${BASE_VARIANT:+${BASE_VARIANT}__}${1}__${2}"
  local key="${BASE_VARIANT:+${BASE_VARIANT}:}${1}:${2}"
  local db="$TMPROOT/$name.db" repo="$TMPROOT/repo_$name"
  local view="" dl_out dt_out dl_ok dt_ok dl_corrupt dl_st dt_st reason dt_cf dl_pre dt_pre dl_setup_out dt_setup_out dl_setup_err dt_setup_err

  rm -f "$db" "$db-lock"; rm -rf "$repo"; mkdir -p "$repo"
  (cd "$repo" && "$DOLT" init --name oracle --email o@t >/dev/null 2>&1)
  if needs_view "$o" "$t"; then view="CREATE VIEW v AS SELECT b FROM t;"; fi
  view="$view
$(base_extra "$o")
$(base_extra "$t")"

  dl_setup_out=$("$DOLTLITE" "$db" 2>&1 <<EOF
CREATE TABLE t(k INT PRIMARY KEY, a VARCHAR(9), b VARCHAR(9), n INT);
INSERT INTO t VALUES(1,'a1','b1',11),(2,'a2','b2',22);
$(base_variant_dl "$BASE_VARIANT")
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
)
  dt_setup_out=$(cd "$repo" && "$DOLT" sql 2>&1 <<EOF
CREATE TABLE t(k INT PRIMARY KEY, a VARCHAR(9), b VARCHAR(9), n INT);
INSERT INTO t VALUES(1,'a1','b1',11),(2,'a2','b2',22);
$(base_variant_dt "$BASE_VARIANT")
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
  dl_setup_err=$(printf '%s\n' "$dl_setup_out" | grep -iE '^error|error:' | head -1 | cut -c1-40)
  dt_setup_err=$(printf '%s\n' "$dt_setup_out" | grep -iE '^error|error:' | head -1 | cut -c1-40)

  # Setup must produce the table on both sides.
  if ! "$DOLTLITE" "$db" "SELECT 1 FROM sqlite_master WHERE name IN ('t','t2') LIMIT 1;" \
        >/dev/null 2>&1; then
    fail_name "$name (doltlite setup failed)"; return
  fi

  # SQLite may reject DDL MySQL accepts (e.g. drop a covered column); skip mismatched setups.
  dl_pre=$("$DOLTLITE" "$db" \
    "SELECT group_concat(name) FROM pragma_table_info((SELECT name FROM sqlite_master
       WHERE type='table' AND name IN ('t','t2') LIMIT 1));" 2>/dev/null)
  dt_pre=$(cd "$repo" && "$DOLT" sql -r csv -q "SELECT group_concat(column_name ORDER BY ordinal_position)
       FROM information_schema.columns WHERE table_name IN ('t','t2');" 2>/dev/null \
       | tail -n +2 | tr -d '"')
  if [ "$dl_pre" != "$dt_pre" ] || [ -n "$dl_setup_err" ] || [ -n "$dt_setup_err" ]; then
    skipped=$((skipped+1)); SKIP_NAMES="$SKIP_NAMES $name"
    echo "  SKIP: $name -- one engine rejected a setup statement (ours=${dl_setup_err:-none} dolt=${dt_setup_err:-none} main-cols ours=$dl_pre dolt=$dt_pre)"
    return
  fi

  dl_out=$("$DOLTLITE" "$db" "SELECT coalesce(dolt_merge('feat'),'NULL');" 2>&1)
  dl_rc=$?
  # dolt_merge's result has a "conflicts" column; read that value, not the text.
  dt_out=$(cd "$repo" && "$DOLT" sql -r csv -q "CALL dolt_merge('feat');" 2>&1)
  dt_rc=$?
  dl_ok=1; dt_ok=1; dl_corrupt=0
  [ "$dl_rc" -ne 0 ] && dl_ok=0
  [ "$dt_rc" -ne 0 ] && dt_ok=0
  case "$dl_out" in *rror*|*onflict*|*NULL*) dl_ok=0;; esac
  case "$dl_out" in *"disk image is malformed"*) dl_corrupt=1;; esac
  if printf '%s\n' "$dt_out" | grep -qiE '^error|error:'; then
    dt_ok=0
  else
    dt_cf=$(printf '%s\n' "$dt_out" | tail -n +2 | head -1 | cut -d, -f3 | tr -d '" ')
    [ "${dt_cf:-0}" != 0 ] && dt_ok=0
  fi

  if [ "$dl_corrupt" = 1 ]; then
    reason=$(printf '%s\n' "$CORRUPT_TODAY" | grep "^$key:" | cut -d: -f3-)
    if [ -n "$reason" ]; then
      corrupt=$((corrupt+1)); CORRUPT_NAMES="$CORRUPT_NAMES $name"
      echo "  CORRUPT: $name -- reports the database as malformed: $reason"
    else
      fail_name "$name (merge reported the database as malformed)"
      echo "    doltlite: $(echo "$dl_out" | head -1 | cut -c1-100)"
    fi
    return
  fi
  if printf '%s\n' "$CORRUPT_TODAY" | grep -q "^$key:"; then
    fail_name "$name (listed as corrupting but no longer does -- delete the entry)"
    return
  fi

  if [ "$dl_ok" = 1 ] && [ "$dt_ok" = 0 ]; then
    reason=$(printf '%s\n' "$MERGE_WHERE_DOLT_REFUSES" | grep "^$key:" | cut -d: -f3-)
    if [ -n "$reason" ]; then
      gaps=$((gaps+1)); GAP_NAMES="$GAP_NAMES $name"
      echo "  GAP:  $name -- we merge where Dolt refuses: $reason"
    else
      fail_name "$name (we merged; Dolt refused)"
      echo "    dolt: $(echo "$dt_out" | head -2 | tr '\n' ' ')"
    fi
    return
  fi
  if [ "$dl_ok" = 0 ] && [ "$dt_ok" = 0 ]; then
    if printf '%s\n' "$MERGE_WHERE_DOLT_REFUSES" | grep -q "^$key:"; then
      fail_name "$name (listed as a gap but we no longer merge -- delete the entry)"
      return
    fi
    pass_name "$name (both refuse)"
    return
  fi

  reason=$(printf '%s\n' "$REFUSE_WHERE_DOLT_MERGES" | grep "^$key:" | cut -d: -f3-)
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
  if printf '%s\n' "$MERGE_WHERE_DOLT_REFUSES" | grep -q "^$key:"; then
    fail_name "$name (listed as a gap but Dolt merges it too -- delete the entry)"
    return
  fi

  dl_st=$(dl_state "$db"); dt_st=$(dt_state "$repo")
  if printf '%s%s' "$dl_st" "$dt_st" | grep -q 'dep=UNREADABLE'; then
    skipped=$((skipped+1)); SKIP_NAMES="$SKIP_NAMES $name"
    echo "  SKIP: $name -- an engine could not read its own dependent catalog back"
    return
  fi
  if [ "$dl_st" = "$dt_st" ]; then
    if printf '%s\n' "$MERGE_DIFFERS_TODAY" | grep -q "^$key:"; then
      fail_name "$name (listed as differing but now matches -- delete the entry)"
      return
    fi
    pass_name "$name"
  else
    reason=$(printf '%s\n' "$MERGE_DIFFERS_TODAY" | grep "^$key:" | cut -d: -f3-)
    if [ -n "$reason" ]; then
      differs=$((differs+1)); DIFFER_NAMES="$DIFFER_NAMES $name"
      echo "  DIFFERS: $name -- merged to a different answer than Dolt: $reason"
    else
      fail_name "$name (merged, but not what Dolt produced)"
      diff <(printf '%s\n' "$dl_st") <(printf '%s\n' "$dt_st") \
        | sed 's/^/      /' | head -12
    fi
  fi
}

echo "=== Correct schema merge matrix (DoltLite vs Dolt) ==="
for o in $OPS; do
  for t in $OPS; do
    [ "$o" = "$t" ] && continue
    run_case "$o" "$t"
  done
done

for BASE_VARIANT in $BASE_VARIANTS; do
  echo "--- base already carries a dependent: $BASE_VARIANT ---"
  for o in $VARIANT_OPS; do
    for t in $VARIANT_OPS; do
      [ "$o" = "$t" ] && continue
      run_case "$o" "$t"
    done
  done
done
BASE_VARIANT=""

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed, $gaps gaps, $differs differing, $corrupt corrupting, $skipped not comparable"
if [ "$skipped" -ne 0 ]; then
  echo "not comparable (a setup statement one engine rejects):$SKIP_NAMES"
fi
if [ "$differs" -ne 0 ]; then
  echo "differing (merged to a different answer than Dolt -- worst class):$DIFFER_NAMES"
fi
if [ "$corrupt" -ne 0 ]; then
  echo "corrupting (report the database as malformed -- fix first):$CORRUPT_NAMES"
fi
if [ "$gaps" -ne 0 ]; then
  echo "gaps (we merge where Dolt refuses -- fix by refusing):$GAP_NAMES"
fi
echo "======================================="
if [ "$fail" -ne 0 ]; then
  echo "failed:$FAILED_NAMES"
  exit 1
fi
