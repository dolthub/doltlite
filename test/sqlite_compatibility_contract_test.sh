#!/usr/bin/env bash

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DOLTLITE="${1:-./doltlite}"
source "$SCRIPT_DIR/lib/doltlite_test_common.sh"

CONTRACT="$SCRIPT_DIR/sqlite_compatibility_contract.tsv"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "=== SQLite compatibility contract ==="

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

DB="$TMP/contract.db"
run_test "contract_seed" \
  "CREATE TABLE keyed(k TEXT PRIMARY KEY, v TEXT); INSERT INTO keyed VALUES('a','one');" \
  "" "$DB"

sqlite_magic="$(printf 'SQLite format 3\000' | od -An -tx1 | tr -d ' \n')"
db_magic="$(od -An -tx1 -N16 "$DB" | tr -d ' \n')"
if [[ "$db_magic" = "$sqlite_magic" ]]; then
  dltest_fail "distinct_file_format" "  DoltLite database has a SQLite file header"
else
  dltest_pass
fi

if [[ -e "$DB-journal" || -e "$DB-wal" || -e "$DB-shm" ]]; then
  dltest_fail "sidecars_absent" "  SQLite sidecar exists for $DB"
else
  dltest_pass
fi

run_test_match "non_integer_pk_has_no_rowid" \
  "SELECT rowid FROM keyed;" \
  "no such column: rowid" "$DB"

MAIN_DB="$TMP/multifile-main.db"
AUX_DB="$TMP/multifile-aux.db"
run_test_match "multifile_temp_trigger_rejected" \
  "CREATE TABLE insert_log(db TEXT, a, b, c);
ATTACH '$AUX_DB' AS aux;
CREATE TABLE aux.t4(a INTEGER PRIMARY KEY, b, c);
CREATE TEMP TRIGGER trig AFTER INSERT ON aux.t4 BEGIN
  INSERT INTO insert_log VALUES('aux', new.a, new.b, new.c);
END;
INSERT INTO aux.t4 VALUES(7,8,9);" \
  "atomic commit across multiple file-backed databases is not supported" \
  "$MAIN_DB"

run_test "multifile_trigger_rolls_back_main" \
  "SELECT count(*) FROM insert_log;" "0" "$MAIN_DB"

run_test "multifile_trigger_rolls_back_attached" \
  "ATTACH '$AUX_DB' AS aux; SELECT count(*) FROM aux.t4;" "0" "$MAIN_DB"

dltest_finish
