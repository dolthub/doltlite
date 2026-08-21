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

run_dl_query() {
  local db="$1" query="$2" out="$3" err="$4"
  printf "%s\n" "$query" | "$DOLTLITE" "$db" >"$out" 2>"$err"
}

run_dt_query() {
  local repo="$1" query="$2" out="$3" err="$4"
  (
    cd "$repo" || exit 1
    "$DOLT" sql -q "$query" >"$out" 2>"$err"
  )
}

dolt_repo_setup() {
  local repo="$1" sql="$2"
  mkdir -p "$repo"
  (
    cd "$repo" || exit 1
    "$DOLT" init --name oracle --email oracle@test >/dev/null 2>&1
    printf "%s\n" "$sql" | "$DOLT" sql >/dev/null 2>"$repo/.setup.err"
  )
}

oracle_refs_equal() {
  local name="$1" setup="$2" ref="$3" expected="$4"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl"

  local dl_query="SELECT CASE
    WHEN dolt_hashof('$ref') = dolt_hashof('$expected') THEN 'REF_OK'
    ELSE 'REF_WRONG'
  END AS result;"
  run_dl_query "$dir/dl/db" "$(printf '%s\n%s\n' "$setup" "$dl_query")" "$dir/dl.out" "$dir/dl.err"
  local dl_rc=$?

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  dolt_repo_setup "$dir/dt" "$dolt_setup"
  run_dt_query "$dir/dt" "$dl_query" "$dir/dt.out" "$dir/dt.err"
  local dt_rc=$?

  if [ "$dl_rc" -ne 0 ] || [ "$dt_rc" -ne 0 ]; then
    fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected both to succeed; dl_rc=$dl_rc dt_rc=$dt_rc)"
    echo "    doltlite stderr: $(cat "$dir/dl.err" 2>/dev/null)"
    echo "    dolt stderr:     $(cat "$dir/dt.err" 2>/dev/null)"
    return
  fi

  if grep -q '^REF_OK$' "$dir/dl.out" && grep -q 'REF_OK' "$dir/dt.out"; then
    pass=$((pass+1))
  else
    fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected $ref to resolve to $expected)"
    echo "    doltlite output: $(cat "$dir/dl.out" 2>/dev/null)"
    echo "    dolt output:     $(cat "$dir/dt.out" 2>/dev/null)"
  fi
}

oracle_both_error() {
  local name="$1" setup="$2" ref="$3"
  local dir="$TMPROOT/${name}_err"
  mkdir -p "$dir/dl"

  local dl_query="SELECT dolt_hashof('$ref');"
  run_dl_query "$dir/dl/db" "$(printf '%s\n%s\n' "$setup" "$dl_query")" "$dir/dl.out" "$dir/dl.err"
  local dl_rc=$?

  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  dolt_repo_setup "$dir/dt" "$dolt_setup"
  run_dt_query "$dir/dt" "$dl_query" "$dir/dt.out" "$dir/dt.err"
  local dt_rc=$?

  if vc_oracle_is_clean_error "$dl_rc" && vc_oracle_is_clean_error "$dt_rc"; then
    pass=$((pass+1))
  else
    fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (expected both to error; dl_rc=$dl_rc dt_rc=$dt_rc)"
    echo "    doltlite stderr: $(cat "$dir/dl.err" 2>/dev/null)"
    echo "    dolt stderr:     $(cat "$dir/dt.err" 2>/dev/null)"
  fi
}

echo "=== Version Control Oracle Tests: ancestor spec (F4 LCA + F9 composed refs) ==="
echo ""

echo "--- F9: composed ref expressions (HEAD, ~, ^, ~N, ^N, composed) ---"

MERGED="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'c1');
SELECT dolt_branch('base');
SELECT dolt_checkout('-b', 'feat');
INSERT INTO t VALUES (10, 100);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'feat_c1');
SELECT dolt_tag('feat_tip', 'HEAD');
SELECT dolt_checkout('main');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'main_c2');
SELECT dolt_branch('main_premerge');
SELECT dolt_merge('feat');
"

oracle_refs_equal "head"           "$MERGED" "HEAD" "main"
oracle_refs_equal "main"           "$MERGED" "main" "HEAD"
oracle_refs_equal "head_tilde"     "$MERGED" "HEAD~" "main_premerge"
oracle_refs_equal "head_caret"     "$MERGED" "HEAD^" "main_premerge"
oracle_refs_equal "head_tilde_1"   "$MERGED" "HEAD~1" "main_premerge"
oracle_refs_equal "head_caret_1"   "$MERGED" "HEAD^1" "main_premerge"
oracle_refs_equal "head_caret_2"   "$MERGED" "HEAD^2" "feat"
oracle_refs_equal "head_double_tilde" "$MERGED" "HEAD~~" "base"
oracle_refs_equal "head_double_caret" "$MERGED" "HEAD^^" "base"
oracle_refs_equal "head_caret2_tilde1" "$MERGED" "HEAD^2~1" "base"
oracle_refs_equal "head_tilde1_caret1" "$MERGED" "HEAD~1^1" "base"
oracle_refs_equal "head_tilde_0"       "$MERGED" "HEAD~0" "HEAD"
oracle_refs_equal "branch_tilde_1"     "$MERGED" "feat~1" "base"
oracle_refs_equal "tag_tilde_1"        "$MERGED" "feat_tip~1" "base"

oracle_both_error "head_caret_0"   "$MERGED" "HEAD^0"
oracle_both_error "head_tilde3_caret2" "$MERGED" "HEAD~3^2"
oracle_both_error "head_tilde1_caret2" "$MERGED" "HEAD~1^2"
oracle_both_error "nonmerge_branch_caret2" "$MERGED" "feat^2"
oracle_both_error "bare_tilde" "$MERGED" "~"
oracle_both_error "bare_tilde_1" "$MERGED" "~1"
oracle_both_error "bare_caret" "$MERGED" "^"
oracle_both_error "bare_caret_1" "$MERGED" "^1"

# Walks past the root; must error. Iterative so deep chains stay O(1) stack.
oracle_both_error "head_many_carets" "$MERGED" "HEAD$(printf '^%.0s' $(seq 1 64))"

echo ""
echo "--- F4: LCA must be deterministic on criss-cross merge ---"

CRISS_CROSS_FWD="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base', '--date', '2020-01-01T00:00:00');
SELECT dolt_branch('A');
SELECT dolt_branch('B');
SELECT dolt_checkout('A');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'a1', '--date', '2030-01-01T00:00:00');
SELECT dolt_checkout('B');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'b1', '--date', '2020-06-01T00:00:00');
SELECT dolt_branch('C', 'A');
SELECT dolt_branch('D', 'B');
SELECT dolt_checkout('C');
SELECT dolt_merge('B');
SELECT dolt_checkout('D');
SELECT dolt_merge('A');
SELECT dolt_checkout('C');
"

CRISS_CROSS_REV="
CREATE TABLE t(id INTEGER PRIMARY KEY, v INT);
INSERT INTO t VALUES (1, 10);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'base', '--date', '2020-01-01T00:00:00');
SELECT dolt_branch('A');
SELECT dolt_branch('B');
SELECT dolt_checkout('A');
INSERT INTO t VALUES (2, 20);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'a1', '--date', '2020-06-01T00:00:00');
SELECT dolt_checkout('B');
INSERT INTO t VALUES (3, 30);
SELECT dolt_add('-A');
SELECT dolt_commit('-m', 'b1', '--date', '2030-01-01T00:00:00');
SELECT dolt_branch('C', 'A');
SELECT dolt_branch('D', 'B');
SELECT dolt_checkout('C');
SELECT dolt_merge('B');
SELECT dolt_checkout('D');
SELECT dolt_merge('A');
SELECT dolt_checkout('C');
"

lca_query_dl="SELECT CASE
  WHEN dolt_merge_base('C', 'D') = dolt_hashof('A') THEN 'LCA|a1'
  WHEN dolt_merge_base('C', 'D') = dolt_hashof('B') THEN 'LCA|b1'
  ELSE 'LCA|OTHER'
END;"

lca_query_dt="SELECT CASE
  WHEN dolt_merge_base('C', 'D') = dolt_hashof('A') THEN CONCAT('LCA', '|', 'a1')
  WHEN dolt_merge_base('C', 'D') = dolt_hashof('B') THEN CONCAT('LCA', '|', 'b1')
  ELSE CONCAT('LCA', '|', 'OTHER')
END;"

run_lca_doltlite() {
  local setup="$1" db="$2" out="$3" err="$4"
  printf "%s\n.headers off\n.mode list\n%s\n" "$setup" "$lca_query_dl" \
    | "$DOLTLITE" "$db" >"$out" 2>"$err"
}

assert_deterministic_lca() {
  local name="$1" setup="$2"
  local dl_runs=3
  local prev=""
  local i
  for ((i=1; i<=dl_runs; i++)); do
    local db="$TMPROOT/${name}_dl_${i}.db"
    local out="$TMPROOT/${name}_dl_${i}.out"
    local err="$TMPROOT/${name}_dl_${i}.err"
    rm -f "$db"
    run_lca_doltlite "$setup" "$db" "$out" "$err"
    local cur
    cur=$(grep '^LCA|' "$out" | head -n 1 | sed 's/^LCA|//')
    if [ -z "$cur" ]; then
      fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES ${name}_run${i}"
      echo "  FAIL: ${name}_run${i} (no LCA line)"
      echo "    output: $(cat "$out")"
      return
    fi
    if [ "$cur" != "a1" ] && [ "$cur" != "b1" ]; then
      fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES ${name}_run${i}"
      echo "  FAIL: ${name}_run${i} (LCA not in {a1,b1}): $cur"
      return
    fi
    if [ -n "$prev" ] && [ "$cur" != "$prev" ]; then
      fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES ${name}_determinism"
      echo "  FAIL: ${name} (LCA differs across runs: run1=$prev runN=$cur)"
      return
    fi
    prev="$cur"
  done
  pass=$((pass+1))
}

assert_dolt_in_set() {
  local name="$1" setup="$2"
  local dolt_setup
  dolt_setup=$(vc_oracle_translate_for_dolt "$setup")
  local repo="$TMPROOT/${name}_dt"
  dolt_repo_setup "$repo" "$dolt_setup"
  local out="$TMPROOT/${name}_dt.out"
  local err="$TMPROOT/${name}_dt.err"
  (cd "$repo" && "$DOLT" sql -q "$lca_query_dt") >"$out" 2>"$err"
  local rc=$?
  local got
  got=$(grep -oE 'LCA\|[a-zA-Z0-9_]+' "$out" | head -n 1 | sed 's/^LCA|//')
  if [ "$rc" -ne 0 ]; then
    fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (dolt query failed rc=$rc)"
    echo "    stderr: $(cat "$err")"
    return
  fi
  if [ "$got" = "a1" ] || [ "$got" = "b1" ]; then
    pass=$((pass+1))
  else
    fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (dolt LCA not in {a1,b1}): |$got|"
    echo "    output: $(cat "$out")"
  fi
}

assert_deterministic_lca "criss_cross_fwd_dl_determinism" "$CRISS_CROSS_FWD"
assert_deterministic_lca "criss_cross_rev_dl_determinism" "$CRISS_CROSS_REV"
assert_dolt_in_set       "criss_cross_fwd_dt_in_set"     "$CRISS_CROSS_FWD"
assert_dolt_in_set       "criss_cross_rev_dt_in_set"     "$CRISS_CROSS_REV"

echo ""
echo "=== Results: $pass passed, $fail failed ==="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
