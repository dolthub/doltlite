#!/usr/bin/env bash

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DOLTLITE="${1:-./doltlite}"
source "$SCRIPT_DIR/lib/doltlite_test_common.sh"

CONTRACT="$SCRIPT_DIR/concurrency_contract.tsv"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "=== Concurrency contract ==="

header="$(head -n 1 "$CONTRACT")"
if [[ "$header" != $'id\tstatus\tevidence\tcontract' ]]; then
  dltest_fail "contract_header" "  unexpected header: $header"
else
  dltest_pass
fi

ids="|"
while IFS=$'\t' read -r id status evidence contract; do
  if [[ -z "$id" || -z "$status" || -z "$evidence" || -z "$contract" ]]; then
    dltest_fail "contract_row" "  incomplete row: $id"
    continue
  fi
  case "$ids" in
    *"|$id|"*) dltest_fail "contract_unique_id" "  duplicate id: $id" ;;
    *) ids="$ids$id|" ;;
  esac
  case "$status" in
    compatible|adapted|unsupported) ;;
    *) dltest_fail "contract_status" "  $id has invalid status: $status" ;;
  esac

  IFS=',' read -ra checks <<<"$evidence"
  for check in "${checks[@]}"; do
    path="${check%%#*}"
    needle="${check#*#}"
    if [[ "$path" = "$check" || ! -f "$REPO_ROOT/$path" ]]; then
      dltest_fail "contract_evidence" "  $id has invalid evidence: $check"
    elif ! grep -Fq -- "$needle" "$REPO_ROOT/$path"; then
      dltest_fail "contract_evidence" "  $id evidence selector is missing: $check"
    fi
  done
done < <(tail -n +2 "$CONTRACT")

# Two processes, sequential commits.
DB="$TMP/multiproc.db"
rm -f "$DB"
seed_out="$("$DOLTLITE" "$DB" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1, 'a');
SELECT length(dolt_commit('-Am', 'seed'));
" 2>&1 | tail -1)"
if [[ "$seed_out" = "40" ]]; then
  dltest_pass
else
  dltest_fail "multiproc_seed" "  expected commit hash length 40, got: $seed_out"
fi

out2="$("$DOLTLITE" "$DB" "INSERT INTO t VALUES(2, 'b'); SELECT length(dolt_commit('-Am', 'second'));" 2>&1 | tail -1)"
if [[ "$out2" = "40" ]]; then
  dltest_pass
else
  dltest_fail "multiproc_sequential_commits" "  expected commit hash length 40, got: $out2"
fi

run_test "multiproc_sequential_rows" \
  "SELECT count(*) FROM t;" "2" "$DB"
# init + seed + second
run_test "multiproc_sequential_log" \
  "SELECT count(*) FROM dolt_log;" "3" "$DB"

# Graph lock is not a SQLite -wal/-shm.
if [[ -e "$DB-journal" || -e "$DB-wal" || -e "$DB-shm" ]]; then
  dltest_fail "no_sqlite_sidecars_under_multiproc" "  SQLite sidecar exists for $DB"
else
  dltest_pass
fi

dltest_finish
