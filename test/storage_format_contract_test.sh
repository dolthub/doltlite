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
CORPUS_RECIPE="$CORPUS_DIR/generate.sh"
CORPUS_SQL="$CORPUS_DIR/seed.sql"
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

if [[ ! -f "$CORPUS_DB" || ! -f "$CORPUS_MANIFEST" \
   || ! -x "$CORPUS_RECIPE" || ! -f "$CORPUS_SQL" ]]; then
  dltest_fail "corpus_present" "  missing version 12 corpus artifact"
  dltest_finish
  exit 1
fi
dltest_pass

expected_sha="$(sed -n 's/^sha256=//p' "$CORPUS_MANIFEST")"
actual_sha="$(python3 -c \
  "import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest())" \
  "$CORPUS_DB")"
if [[ -n "$expected_sha" && "$actual_sha" = "$expected_sha" ]]; then
  dltest_pass
else
  dltest_fail "corpus_v12_checksum" "  expected $expected_sha, got $actual_sha"
fi

hdr_ver="$(python3 -c "
import re,sys
h=open(sys.argv[1]).read()
print(re.search(r'#define CHUNK_STORE_VERSION\s+(\d+)', h).group(1))
" "$REPO_ROOT/src/chunk_store.h")"
man_ver="$(sed -n 's/^chunk_store_version=//p' "$CORPUS_MANIFEST")"
if [[ "$hdr_ver" = "12" && "$man_ver" = "12" ]]; then
  dltest_pass
else
  dltest_fail "manifest_matches_header" "  source=$hdr_ver manifest=$man_ver"
fi

if python3 -c "
import sys
b=open(sys.argv[1],'rb').read(8)
sys.exit(0 if len(b)==8
              and int.from_bytes(b[:4],'little')==0x444C5443
              and int.from_bytes(b[4:8],'little')==12 else 1)
" "$CORPUS_DB"; then
  dltest_pass
else
  dltest_fail "corpus_v12_header" "  corpus header is not chunk-store version 12"
fi

OPEN_DB="$TMP/open_seed.db"
cp "$CORPUS_DB" "$OPEN_DB"
run_test "corpus_v12_deep_tree" \
  "SELECT count(*) FROM deep NOT INDEXED;" "20001" "$OPEN_DB/main"
run_test "corpus_v12_secondary_indexes" \
  "SELECT count(*) FROM deep INDEXED BY deep_grp_score;
   SELECT group_concat(id, ',') FROM (
     SELECT id FROM deep INDEXED BY deep_score_partial
      WHERE id % 2 = 0 AND id IN (30, 40) ORDER BY id
   );" \
  "20001
30,40" "$OPEN_DB/main"
run_test "corpus_v12_composite_blob_rows" \
  "SELECT group_concat(a || ':' || hex(b) || ':' || v, ',') FROM (SELECT * FROM keyed ORDER BY a, b);" \
  "Alpha:00FF:1.5,beta:1020:-2.25,gamma:FF00:3.75" "$OPEN_DB/main"
run_test "corpus_v12_catalog_objects" \
  "SELECT count(*) FROM sqlite_schema
    WHERE name IN ('deep','deep_grp_score','deep_score_partial','keyed',
                   'generated_values','deep_even','deep_audit','docs',
                   'feature_only','main_only','branch_data');
   SELECT group_concat(id || ':' || stored || ':' || virtual, ',')
     FROM generated_values ORDER BY id;
   SELECT count(*) FROM deep_even;
   SELECT count(*) FROM docs WHERE docs MATCH 'prolly';
   SELECT count(*) FROM audit;" \
  "11
1:10:6,2:18:10
10001
1
2" "$OPEN_DB/main"
run_test "corpus_v12_sequences" \
  "SELECT group_concat(name || ':' || seq, ',')
     FROM (SELECT name, seq FROM sqlite_sequence ORDER BY name);" \
  "audit:2,seq:3" "$OPEN_DB/main"
run_test "corpus_v12_integrity" \
  "PRAGMA integrity_check;" "ok" "$OPEN_DB/main"
run_test "corpus_v12_merge_commit" \
  "SELECT count(*) FROM dolt_commit_ancestors
    WHERE commit_hash=(
      SELECT commit_hash FROM dolt_log WHERE message LIKE 'Merge branch%'
    );" "2" "$OPEN_DB/main"
run_test "corpus_v12_branches" \
  "SELECT group_concat(name, ',') FROM (SELECT name FROM dolt_branches ORDER BY name);" \
  "dolt_rebase_rebase_source,feature,main,rebase_source,violations,workspace" \
  "$OPEN_DB/main"
run_test "corpus_v12_annotated_tags" \
  "SELECT group_concat(tag_name || ':' || tagger || ':' || email || ':' || message, '|')
     FROM (SELECT * FROM dolt_tags ORDER BY tag_name);" \
  "v12-base:Format Tagger:tagger@example.com:annotated format baseline|v12-merge:Merge Tagger:merge@example.com:annotated merge result" \
  "$OPEN_DB/main"
run_test "corpus_v12_remote_tracking" \
  "SELECT group_concat(name || ':' || (url GLOB 'file://*/origin.db'), ',')
     FROM dolt_remotes;
   SELECT group_concat(name, ',') FROM dolt_remote_branches;" \
  "origin:1
remotes/origin/main" "$OPEN_DB/main"
run_test "corpus_v12_dirty_staged_working_set" \
  "SELECT group_concat(id || ':' || grp, ',') FROM (
     SELECT id, grp FROM deep WHERE id>20000 ORDER BY id
   );
   SELECT group_concat(table_name || ':' || status || ':' || staged, ',') FROM (
     SELECT table_name, status, staged FROM dolt_status ORDER BY table_name, staged
   );" \
  "20001:working,20002:unstaged
deep:modified:0,deep:modified:1" "$OPEN_DB/workspace"
run_test "corpus_v12_constraint_violations" \
  "SELECT count(*) FROM child;
   SELECT group_concat(\"table\" || ':' || num_violations, ',')
     FROM dolt_constraint_violations;
   SELECT violation_type FROM dolt_constraint_violations_child;" \
  "2
child:1
foreign key" "$OPEN_DB/violations"
run_test "corpus_v12_rebase_working_set" \
  "SELECT count(*) FROM dolt_rebase;
   SELECT group_concat(rebase_order || ':' || action || ':' || commit_message, ',')
     FROM dolt_rebase;" \
  "1
1.0:pick:v12 rebase source" "$OPEN_DB/dolt_rebase_rebase_source"
run_test "corpus_v12_rebase_return_branch" \
  "SELECT count(*) FROM sqlite_schema WHERE name='dolt_rebase';" \
  "1" "$OPEN_DB/main"

REBUILT_DB="$TMP/rebuilt_seed.db"
if "$CORPUS_RECIPE" "$DOLTLITE" "$REBUILT_DB" >/dev/null \
 && [[ "$("$DOLTLITE" "$REBUILT_DB/main" "SELECT count(*) FROM deep NOT INDEXED;")" = "20001" ]]; then
  dltest_pass
else
  dltest_fail "corpus_v12_recipe_rebuild" "  generation recipe did not reproduce the semantic fixture"
fi

WRITE_DB="$TMP/write_seed.db"
cp "$CORPUS_DB" "$WRITE_DB"
run_test "corpus_v12_write_commit" \
  "INSERT INTO deep(id, grp, score, payload)
   VALUES(21000, 'forward', 2100.0, x'11223344');
   SELECT length(dolt_commit('-A', '-m', 'extend v12'));" \
  "40" "$WRITE_DB/feature"
run_test "corpus_v12_reopen_after_write" \
  "SELECT count(*) FROM deep NOT INDEXED;
   SELECT message FROM dolt_log LIMIT 1;" \
  "20001
extend v12" "$WRITE_DB/feature"
run_test_match "corpus_v12_gc" \
  "SELECT dolt_gc();" "chunks removed" "$WRITE_DB/feature"
run_test "corpus_v12_post_gc_rows" \
  "SELECT count(*) FROM deep NOT INDEXED;
   SELECT count(*) FROM deep WHERE id=21000;
   PRAGMA integrity_check;" \
  "20001
1
ok" "$WRITE_DB/feature"
run_test "corpus_v12_post_gc_working_sets" \
  "SELECT count(*) FROM deep WHERE id IN (20001, 20002);
   SELECT count(*) FROM dolt_status WHERE table_name='deep';" \
  "2
2" "$WRITE_DB/workspace"
run_test "corpus_v12_post_gc_constraint_violations" \
  "SELECT coalesce(sum(num_violations), 0) FROM dolt_constraint_violations;" \
  "1" "$WRITE_DB/violations"
run_test "corpus_v12_post_gc_rebase" \
  "SELECT count(*) FROM dolt_rebase;" "1" \
  "$WRITE_DB/dolt_rebase_rebase_source"
run_test "corpus_v12_post_gc_tracking" \
  "SELECT count(*) FROM dolt_remote_branches WHERE name='remotes/origin/main';" \
  "1" "$WRITE_DB/main"

patch_u32() {
  local src="$1" dst="$2" off="$3" value="$4"
  python3 -c "
import shutil,sys
src,dst,off,value=sys.argv[1:]
shutil.copyfile(src, dst)
with open(dst, 'r+b') as f:
    f.seek(int(off))
    f.write(int(value, 0).to_bytes(4, 'little'))
" "$src" "$dst" "$off" "$value"
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
b=open(sys.argv[1],'rb').read(8)
sys.exit(0 if len(b)==8
              and int.from_bytes(b[:4],'little')==0x444C5443
              and int.from_bytes(b[4:8],'little')==12 else 1)
" "$FRESH"; then
  dltest_pass
else
  dltest_fail "fresh_write_v12_header" "  fresh write failed or did not stamp version 12"
fi

dltest_finish
