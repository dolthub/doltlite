#!/usr/bin/env bash
# Replays hand-written model logs through `vc_model_stress_test --check`.
#
#   bash test/vc_model_stress_check_test.sh path/to/vc_model_stress_test path/to/doltlite

set -u

BIN="${1:-./vc_model_stress_test}"
DOLTLITE="${2:-./doltlite}"
for f in "$BIN" "$DOLTLITE"; do
  if [ ! -x "$f" ]; then echo "ERROR: $f not built" >&2; exit 2; fi
done

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
fails=0

check(){
  name=$1 want=$2 dir=$3
  status=0
  out=$("$BIN" --check "$dir" "${4:-1}" 2>&1) || status=$?
  want_status=0
  if [ "$want" -ne 0 ]; then want_status=1; fi
  got=$(echo "$out" | grep -c '^VIOLATION')
  pattern_ok=1
  if [ -n "${5:-}" ] && ! grep -Fq -- "$5" <<<"$out"; then pattern_ok=0; fi
  if [ "$got" = "$want" ] && [ "$status" = "$want_status" ] && [ "$pattern_ok" -eq 1 ]; then
    echo "ok $name"
  else
    echo "FAIL $name: expected $want violations/status $want_status, got $got/status $status"
    echo "$out" | sed 's/^/  /'
    fails=$((fails+1))
  fi
}

newdir(){
  d="$TMP/$1"
  mkdir -p "$d"
  "$DOLTLITE" "$d/db.db" "CREATE TABLE w(id ${2:-INTEGER} PRIMARY KEY, v TEXT);
SELECT dolt_commit('-Am','base'); SELECT dolt_branch('b1'); SELECT dolt_branch('b2');" >/dev/null
  echo "$d"
}

# A row committed in m_9, amended into m_13 while a reset --hard HEAD~1
# waited for the lock; the reset then dropped m_13 (its logged victim).
d=$(newdir amend_then_reset)
printf '%s\n' \
  $'I\t100000012\tmain\t1000\t1100' \
  $'C\tm_9\tmain\t2000\t2100\t1' \
  $'X\t7\tmain\t3000\t9000\treset_hard_1\tm_13' \
  $'X\t4\tmain\t4000\t4500\tamend\tm_9\tm_13' \
  $'C\tm_13\tmain\t4000\t4500\t1' > "$d/log.0"
check amend_then_reset_excused 0 "$d"

# Same history, but the reset dropped an unrelated commit: the row is lost.
d=$(newdir reset_other_victim)
printf '%s\n' \
  $'I\t100000012\tmain\t1000\t1100' \
  $'C\tm_9\tmain\t2000\t2100\t1' \
  $'X\t7\tmain\t3000\t9000\treset_hard_1\tm_other' \
  $'X\t4\tmain\t4000\t4500\tamend\tm_9\tm_13' \
  $'C\tm_13\tmain\t4000\t4500\t1' > "$d/log.0"
check reset_other_victim_lost 1 "$d"


for swapped in 0 1; do
  for victim in m_d m_unrelated; do
    d=$(newdir "amend_chain_${swapped}_${victim}")
    printf '%s\n' \
      $'I\t100000012\tmain\t1000\t1100' \
      $'X\t4\tmain\t2000\t2100\tamend\tm_before\tm_a' \
      $'C\tm_a\tmain\t2000\t2100\t1' \
      $'X\t4\tmain\t4000\t4100\tamend\tm_c\tm_d' \
      $'C\tm_d\tmain\t4000\t4100\t1' > "$d/log.$swapped"
    printf 'X\t7\tmain\t5000\t5100\treset_hard_1\t%s\n' "$victim" >> "$d/log.$swapped"
    printf '%s\n' \
      $'X\t4\tmain\t3000\t3100\tamend\tm_a\tm_c' \
      $'C\tm_c\tmain\t3000\t3100\t1' > "$d/log.$((1-swapped))"
    want=0
    if [ "$victim" = m_unrelated ]; then want=1; fi
    check "amend_chain_${swapped}_${victim}" "$want" "$d" 2
  done
done

for soft_branch in main b1; do
  d=$(newdir "soft_reset_while_waiting_${soft_branch}")
  if [ "$soft_branch" = main ]; then
    "$DOLTLITE" "$d/db.db" "INSERT INTO w VALUES(100000012,'acked'); SELECT dolt_commit('-Am','m_victim');" >/dev/null
  fi
  "$DOLTLITE" "$d/db.db" "SELECT dolt_commit('--allow-empty','-m','m_top');" >/dev/null
  if [ "$soft_branch" = main ]; then
    "$DOLTLITE" "$d/db.db" "SELECT dolt_reset('--soft','HEAD~1'); SELECT dolt_reset('--hard','HEAD~1');" >/dev/null
  fi
  printf '%s\n' \
    $'I\t100000012\tmain\t1000\t1100' \
    $'C\tm_victim\tmain\t2000\t2100\t1' \
    $'C\tm_top\tmain\t2500\t2600\t1' \
    $'S\t3000\t6000\tc0\tBEGIN IMMEDIATE' \
    $'X\t7\tmain\t3000\t9000\treset_hard_1\tm_victim' > "$d/log.0"
  printf 'X\t6\t%s\t4000\t4500\treset_soft_1\tm_top\n' "$soft_branch" > "$d/log.1"
  want=0
  name=soft_reset_while_waiting_excused
  if [ "$soft_branch" = b1 ]; then
    want=1
    name=other_branch_reset_cannot_excuse_lost_write
  fi
  check "$name" "$want" "$d" 2
  if [ "$soft_branch" = main ]; then
    printf 'I\t100000013\tmain\t10000\t10100\n' >> "$d/log.0"
    check write_after_resets_lost 1 "$d" 2
  fi
done

# A plain reset --hard waits for the lock while a peer's reset --soft HEAD~1
# drops the commit holding the row; the hard reset then reads the older HEAD.
for soft_branch in main b1; do
  d=$(newdir "plain_reset_after_soft_${soft_branch}")
  "$DOLTLITE" "$d/db.db" "INSERT INTO w VALUES(100000012,'acked'); SELECT dolt_commit('-Am','m_victim');" >/dev/null
  if [ "$soft_branch" = main ]; then
    "$DOLTLITE" "$d/db.db" "SELECT dolt_reset('--soft','HEAD~1'); SELECT dolt_reset('--hard');" >/dev/null
  else
    "$DOLTLITE" "$d/db.db" "DELETE FROM w WHERE id=100000012;" >/dev/null
  fi
  printf '%s\n' \
    $'I\t100000012\tmain\t1000\t1100' \
    $'C\tm_victim\tmain\t2000\t2100\t1' \
    $'S\t3000\t6000\tc0\tBEGIN IMMEDIATE' \
    $'X\t1\tmain\t3000\t9000\treset_hard\t*' > "$d/log.0"
  printf 'X\t6\t%s\t4000\t4500\treset_soft_1\tm_victim\n' "$soft_branch" > "$d/log.1"
  if [ "$soft_branch" = main ]; then
    check plain_reset_after_soft_reset_excused 0 "$d" 2
  else
    check plain_reset_other_branch_soft_reset_lost 1 "$d" 2
  fi
done

# A soft reset on main whose connection could not read its branch while a
# peer's gc held the lock logs the excuse on "?". Without FG it covers no
# branch and the rewound commit reads as lost.
for flags in 6 14; do
  d=$(newdir "soft_reset_unknown_branch_${flags}")
  "$DOLTLITE" "$d/db.db" "INSERT INTO w VALUES(100000012,'acked'); SELECT dolt_commit('-Am','m_victim');
SELECT dolt_reset('--soft','HEAD~1');" >/dev/null
  printf '%s\n' \
    $'I\t100000012\tmain\t1000\t1100' \
    $'C\tm_victim\tmain\t2000\t2100\t1' > "$d/log.0"
  printf 'X\t%s\t?\t3000\t3500\treset_soft_1\tm_victim\n' "$flags" > "$d/log.1"
  if [ "$flags" = 14 ]; then
    check soft_reset_unknown_branch_global_excused 0 "$d" 2
  else
    check soft_reset_unknown_branch_lost 2 "$d" 2 "LOST acked commit 'm_victim'"
  fi
done

d=$(newdir commit_during_lock_wait)
"$DOLTLITE" "$d/db.db" "SELECT dolt_commit('--allow-empty','-m','m_protected');" >/dev/null
printf '%s\n' \
  $'I\t100000012\tmain\t1000\t1100' \
  $'C\tm_protected\tmain\t4000\t4500\t1' \
  $'S\t3000\t6000\tc0\tBEGIN IMMEDIATE' \
  $'X\t7\tmain\t3000\t9000\treset_hard_1\tm_other' > "$d/log.0"
check commit_during_lock_wait_protects_row 1 "$d"

for key in INTEGER INT; do
  d=$(newdir "hard_reset_before_create_$key" "$key")
  "$DOLTLITE" "$d/db.db" "INSERT INTO w VALUES(100000012,'acked');
SELECT dolt_commit('-Am','m_victim');
SELECT dolt_reset('--hard','HEAD~1'); SELECT dolt_reset('--hard','HEAD~1');" >/dev/null
  printf '%s\n' \
    $'I\t100000012\tmain\t1000\t1100' \
    $'C\tm_victim\tmain\t2000\t2100\t1' \
    $'X\t7\tmain\t3000\t3100\treset_hard_1\tm_victim' \
    $'X\t7\tmain\t4000\t4100\treset_hard_1\tbase' > "$d/log.0"
  check "hard_reset_before_create_$key" 0 "$d"
  printf 'I\t100000013\tmain\t5000\t5100\n' >> "$d/log.0"
  check "missing_table_still_checks_later_writes_$key" 1 "$d" 1 \
    'LOST acked id 100000013'

  d=$(newdir "soft_reset_before_create_$key" "$key")
  "$DOLTLITE" "$d/db.db" "INSERT INTO w VALUES(100000012,'acked');
SELECT dolt_commit('-Am','m_victim');
SELECT dolt_reset('--soft','HEAD~1'); SELECT dolt_reset('--soft','HEAD~1');" >/dev/null
  printf '%s\n' \
    $'I\t100000012\tmain\t1000\t1100' \
    $'C\tm_victim\tmain\t2000\t2100\t1' \
    $'X\t6\tmain\t3000\t3100\treset_soft_1\tm_victim' \
    $'X\t6\tmain\t4000\t4100\treset_soft_1\tbase' > "$d/log.0"
  check "soft_reset_before_create_$key" 0 "$d"
  printf 'C\tm_missing\tmain\t5000\t5100\t1\n' >> "$d/log.0"
  check "missing_head_still_checks_commits_$key" 2 "$d" 1 \
    "LOST acked commit 'm_missing'"

  for missing in working both; do
    d=$(newdir "unexplained_missing_${missing}_$key" "$key")
    "$DOLTLITE" "$d/db.db" "INSERT INTO w VALUES(100000012,'acked');
SELECT dolt_commit('-Am','m_victim');" >/dev/null
    want=1
    if [ "$missing" = working ]; then
      "$DOLTLITE" "$d/db.db" 'DROP TABLE w;' >/dev/null
    else
      "$DOLTLITE" "$d/db.db" "SELECT dolt_reset('--hard','HEAD~2');" >/dev/null
      want=3
    fi
    printf '%s\n' \
      $'I\t100000012\tmain\t1000\t1100' \
      $'C\tm_victim\tmain\t2000\t2100\t1' > "$d/log.0"
    check "missing_${missing}_table_still_checks_acked_rows_$key" "$want" "$d" 1 \
      'LOST acked id 100000012'
  done

  d=$(newdir "invalid_schema_$key" "$key")
  "$DOLTLITE" "$d/db.db" 'ALTER TABLE w RENAME COLUMN id TO missing;' >/dev/null
  : > "$d/log.0"
  check "missing_column_still_fails_$key" 1 "$d" 1 'no such column: id'
done

if [ "$fails" -ne 0 ]; then
  echo "vc_model_stress_check: $fails failed"
  exit 1
fi
echo "vc_model_stress_check: all passed"
