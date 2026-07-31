#!/usr/bin/env bash

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DOLTLITE="${1:-./doltlite}"
source "$SCRIPT_DIR/lib/doltlite_test_common.sh"

CONTRACT="$SCRIPT_DIR/storage_format_contract.tsv"
CORPUS_DIR="$SCRIPT_DIR/format-corpus/v12"
CORPUS_DB="$CORPUS_DIR/seed.db"
CORPUS_MANIFEST="$CORPUS_DIR/MANIFEST"
CORPUS_RECIPE="$CORPUS_DIR/seed.sql"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "=== Storage format contract (version 12) ==="

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

if [[ ! -f "$CORPUS_DB" || ! -f "$CORPUS_MANIFEST" || ! -f "$CORPUS_RECIPE" ]]; then
  dltest_fail "corpus_present" "  missing version 12 corpus artifact"
  dltest_finish
  exit 1
fi
dltest_pass

expected_sha="$(sed -n 's/^sha256=//p' "$CORPUS_MANIFEST")"
actual_sha="$(python3 -c "import hashlib; print(hashlib.sha256(open('$CORPUS_DB','rb').read()).hexdigest())")"
if [[ -n "$expected_sha" && "$actual_sha" = "$expected_sha" ]]; then
  dltest_pass
else
  dltest_fail "corpus_v12_checksum" "  expected $expected_sha, got $actual_sha"
fi

hdr_ver="$(python3 -c "
import re
h=open('$REPO_ROOT/src/chunk_store.h').read()
print(re.search(r'#define CHUNK_STORE_VERSION\s+(\d+)', h).group(1))
")"
man_ver="$(sed -n 's/^chunk_store_version=//p' "$CORPUS_MANIFEST")"
if [[ "$hdr_ver" = "12" && "$man_ver" = "12" ]]; then
  dltest_pass
else
  dltest_fail "manifest_matches_header" "  source=$hdr_ver manifest=$man_ver"
fi

if python3 -c "
import sys
b=open('$CORPUS_DB','rb').read(8)
sys.exit(0 if len(b)==8
              and int.from_bytes(b[:4],'little')==0x444C5443
              and int.from_bytes(b[4:8],'little')==12 else 1)
"; then
  dltest_pass
else
  dltest_fail "corpus_v12_header" "  corpus header is not chunk-store version 12"
fi

OPEN_DB="$TMP/open_seed.db"
cp "$CORPUS_DB" "$OPEN_DB"
run_test "corpus_v12_main_rows" \
  "SELECT group_concat(id || ':' || name || ':' || v, ',') FROM t ORDER BY id;" \
  "1:alpha:10,2:beta:20" "$OPEN_DB"
run_test "corpus_v12_index_rows" \
  "SELECT group_concat(id, ',') FROM (SELECT id FROM t INDEXED BY idx_name ORDER BY name);" \
  "1,2" "$OPEN_DB"
run_test "corpus_v12_composite_blob_rows" \
  "SELECT group_concat(a || ':' || hex(b) || ':' || v, ',') FROM (SELECT * FROM keyed ORDER BY a, b);" \
  "a:00FF:1.5,b:1020:-2.25" "$OPEN_DB"
run_test "corpus_v12_schema" \
  "SELECT sql FROM sqlite_schema WHERE name='idx_name';" \
  "CREATE INDEX idx_name ON t(name)" "$OPEN_DB"
run_test "corpus_v12_sequence" \
  "SELECT seq FROM sqlite_sequence WHERE name='seq';" "1" "$OPEN_DB"
run_test "corpus_v12_integrity" \
  "PRAGMA integrity_check;" "ok" "$OPEN_DB"
run_test "corpus_v12_log" \
  "SELECT count(*) FROM dolt_log;" "2" "$OPEN_DB"
run_test "corpus_v12_feature_branch" \
  "SELECT count(*) FROM dolt_branches WHERE name='feature';" "1" "$OPEN_DB"
run_test "corpus_v12_tag" \
  "SELECT count(*) FROM dolt_tags WHERE tag_name='v12-seed';" "1" "$OPEN_DB"
run_test "corpus_v12_feature_rows" \
  "SELECT dolt_checkout('feature'); SELECT count(*) FROM t; SELECT count(*) FROM dolt_log;" \
  "0
3
3" "$OPEN_DB"

WRITE_DB="$TMP/write_seed.db"
cp "$CORPUS_DB" "$WRITE_DB"
run_test "corpus_v12_write_commit" \
  "INSERT INTO t VALUES(4, 'delta', 40); SELECT length(dolt_commit('-A', '-m', 'extend v12'));" \
  "40" "$WRITE_DB"
run_test "corpus_v12_reopen_after_write" \
  "SELECT count(*) FROM t; SELECT count(*) FROM dolt_log;" \
  "3
3" "$WRITE_DB"
run_test_match "corpus_v12_gc" \
  "SELECT dolt_gc();" "chunks removed" "$WRITE_DB"
run_test "corpus_v12_post_gc_rows" \
  "SELECT group_concat(id, ',') FROM (SELECT id FROM t INDEXED BY idx_name ORDER BY name); SELECT count(*) FROM dolt_log;" \
  "1,2,4
3" "$WRITE_DB"

patch_u32() {
  local src="$1" dst="$2" off="$3" value="$4"
  python3 -c "
import shutil
shutil.copyfile('$src', '$dst')
with open('$dst', 'r+b') as f:
    f.seek(int('$off'))
    f.write(int('$value', 0).to_bytes(4, 'little'))
"
}

expect_exact_notadb() {
  local name="$1" db="$2"
  local err status
  err="$("$DOLTLITE" "$db" "SELECT count(*) FROM t;" 2>&1)"
  status=$?
  if [[ "$status" -eq 1 \
     && "$err" = *"file is not a database (26)"* \
     && "$err" != *"AddressSanitizer"* \
     && "$err" != *"UndefinedBehaviorSanitizer"* \
     && "$err" != *"Segmentation fault"* \
     && "$err" != *"timed out"* ]]; then
    dltest_pass
  else
    dltest_fail "$name" "  expected normal SQLITE_NOTADB exit, status=$status output=$err"
  fi
}

patch_u32 "$CORPUS_DB" "$TMP/v13.db" 4 13
expect_exact_notadb "skew_version_13_notadb" "$TMP/v13.db"

patch_u32 "$CORPUS_DB" "$TMP/v11.db" 4 11
expect_exact_notadb "skew_version_11_notadb" "$TMP/v11.db"

patch_u32 "$CORPUS_DB" "$TMP/bad_magic.db" 0 0x01234567
expect_exact_notadb "skew_magic_notadb" "$TMP/bad_magic.db"

FRESH="$TMP/fresh.db"
fresh_out="$("$DOLTLITE" "$FRESH" "CREATE TABLE x(i INT PRIMARY KEY); SELECT length(dolt_commit('-Am', 'fresh'));" 2>&1)"
fresh_status=$?
if [[ "$fresh_status" -eq 0 && "$fresh_out" = "40" ]] && python3 -c "
import sys
b=open('$FRESH','rb').read(8)
sys.exit(0 if len(b)==8
              and int.from_bytes(b[:4],'little')==0x444C5443
              and int.from_bytes(b[4:8],'little')==12 else 1)
"; then
  dltest_pass
else
  dltest_fail "fresh_write_v12_header" "  fresh write failed or did not stamp version 12"
fi

dltest_finish
