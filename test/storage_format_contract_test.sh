#!/usr/bin/env bash

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DOLTLITE="${1:-./doltlite}"
source "$SCRIPT_DIR/lib/doltlite_test_common.sh"

CONTRACT="$SCRIPT_DIR/storage_format_contract.tsv"
CORPUS_DIR="$SCRIPT_DIR/format-corpus/epoch1"
CORPUS_DB="$CORPUS_DIR/seed.db"
CORPUS_MANIFEST="$CORPUS_DIR/MANIFEST"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "=== Storage format contract (epoch 1) ==="

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
    frozen|compatible|adapted|unsupported) ;;
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

if [[ ! -f "$CORPUS_DB" || ! -f "$CORPUS_MANIFEST" ]]; then
  dltest_fail "corpus_present" "  missing $CORPUS_DB or MANIFEST"
  dltest_finish
  exit 1
fi
dltest_pass

# Header: magic 0x444C5443 (LE bytes CTLD) + version 12
if python3 -c "
import sys
b=open('$CORPUS_DB','rb').read(8)
sys.exit(0 if int.from_bytes(b[:4],'little')==0x444C5443
              and int.from_bytes(b[4:8],'little')==12 else 1)
"; then
  dltest_pass
else
  dltest_fail "corpus_has_dltc_magic" "  header is not DLTC v12"
fi

# MANIFEST must match compiled constants
hdr_ver="$(python3 -c "
import re
h=open('$REPO_ROOT/src/chunk_store.h').read()
print(re.search(r'#define CHUNK_STORE_VERSION\s+(\d+)', h).group(1))
")"
man_ver="$(grep -E '^chunk_store_version=' "$CORPUS_MANIFEST" | cut -d= -f2)"
if [[ "$hdr_ver" = "$man_ver" ]]; then
  dltest_pass
else
  dltest_fail "manifest_matches_header" "  header=$hdr_ver manifest=$man_ver"
fi

# Open golden corpus (copy so checkout does not dirty the committed corpus).
OPEN_DB="$TMP/open_seed.db"
cp "$CORPUS_DB" "$OPEN_DB"
run_test "corpus_epoch1_row_count" \
  "SELECT count(*) FROM t;" "2" "$OPEN_DB"
run_test "corpus_epoch1_log" \
  "SELECT count(*) FROM dolt_log;" "2" "$OPEN_DB"
run_test "corpus_epoch1_feature_branch" \
  "SELECT count(*) FROM dolt_branches WHERE name='feature';" "1" "$OPEN_DB"
run_test "corpus_epoch1_feature_rows" \
  "SELECT dolt_checkout('feature'); SELECT count(*) FROM t;" "0
3" "$OPEN_DB"

# Version skew: patch header version, expect NOTADB / cannot open productively
patch_version() {
  local src="$1" dst="$2" ver="$3"
  python3 -c "
import shutil
shutil.copyfile('$src', '$dst')
with open('$dst', 'r+b') as f:
    f.seek(4)
    f.write(int('$ver').to_bytes(4, 'little'))
"
}

expect_notadb() {
  local name="$1" db="$2"
  local err
  err="$("$DOLTLITE" "$db" "SELECT count(*) FROM t;" 2>&1)" || true
  if echo "$err" | grep -qiE 'not a database|NOTADB|file is not a database|unsupported|incompatible|unable to open'; then
    dltest_pass
  elif echo "$err" | grep -qE '^[0-9]+$' && [[ "$(echo "$err" | tail -1)" = "2" ]]; then
    dltest_fail "$name" "  opened skewed DB successfully: $err"
  else
    # Any hard failure to read user data counts as refuse
    if echo "$err" | grep -qiE 'error|Error|unable|fail'; then
      dltest_pass
    else
      dltest_fail "$name" "  expected NOTADB-class failure, got: $err"
    fi
  fi
}

patch_version "$CORPUS_DB" "$TMP/v13.db" 13
expect_notadb "skew_version_13_notadb" "$TMP/v13.db"

patch_version "$CORPUS_DB" "$TMP/v11.db" 11
expect_notadb "skew_version_11_notadb" "$TMP/v11.db"

# Fresh write still stamps epoch-1 header
FRESH="$TMP/fresh.db"
rm -f "$FRESH"
"$DOLTLITE" "$FRESH" "CREATE TABLE x(i INT PRIMARY KEY); SELECT dolt_commit('-Am', 'fresh');" >/dev/null 2>&1
if python3 -c "
import sys
b=open('$FRESH','rb').read(8)
sys.exit(0 if int.from_bytes(b[:4],'little')==0x444C5443
              and int.from_bytes(b[4:8],'little')==12 else 1)
"; then
  dltest_pass
else
  dltest_fail "fresh_write_epoch1_header" "  fresh DB header is not epoch 1"
fi

dltest_finish
