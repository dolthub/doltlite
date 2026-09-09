#!/usr/bin/env bash
# Pins doc/doltlite/pragmas.md to the engine. Every release-build pragma in
# tool/mkpragmatab.tcl must be classified in the doc; each "Same as SQLite"
# pragma must produce byte-identical output on doltlite and stock; each
# "Accepted and inert" pragma must read back the documented value.
#
# Usage: oracle_pragma_matrix_test.sh [doltlite] [stock]

DOLTLITE="${1:-./doltlite}"
SQLITE3="${2:-./sqlite3-stock}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOC="$SCRIPT_DIR/../doc/doltlite/pragmas.md"
PRAGTAB="$SCRIPT_DIR/../tool/mkpragmatab.tcl"
TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT
pass=0; fail=0

if ! bash "$SCRIPT_DIR/assert_stock_reference.sh" "$SQLITE3" "$DOLTLITE"; then
  exit 1
fi

ok()   { pass=$((pass+1)); echo "PASS: $1"; }
bad()  { fail=$((fail+1)); echo "FAIL: $1"; [ -n "$2" ] && echo "  $2"; }

section() {  # section <heading> -> lines of that section
  awk -v h="## $1" '$0==h{f=1;next} /^## /{f=0} f' "$DOC"
}
names_in() { grep -oE '`[a-z_]+`' | tr -d '`' | sort -u; }

same_list=$(section "Same as SQLite" | sed -n '/^`/,/^$/p' | names_in)
adapted_list=$(section "Same intent, DoltLite mechanics" | grep '^| `' | cut -d'|' -f2 | names_in)
inert_list=$(section "Accepted and inert" | grep '^| `' | cut -d'|' -f2 | names_in)
documented=$(printf '%s\n%s\n%s\n' "$same_list" "$adapted_list" "$inert_list" | sort -u)

# Release-build pragmas: skip debug/test-only and platform-gated entries.
in_scope=$(awk '
  /^  NAME:/ {n=$2; gate=""}
  /^  IF:/   {gate=$0}
  /^$/ && n {
    if (gate !~ /SQLITE_DEBUG|SQLITE_TEST|SQLITE_ENABLE_CEROD|SQLITE_OS_WIN|SQLITE_ENABLE_LOCKING_STYLE/) print n
    n=""
  }' "$PRAGTAB" | sort -u)

missing=$(comm -23 <(echo "$in_scope") <(echo "$documented"))
if [ -z "$missing" ]; then ok "every release-build pragma is classified in pragmas.md"
else bad "pragmas missing from pragmas.md" "$(echo "$missing" | tr '\n' ' ')"; fi
extra=$(comm -13 <(echo "$in_scope") <(echo "$documented"))
if [ -z "$extra" ]; then ok "pragmas.md names only real pragmas"
else bad "pragmas.md names unknown pragmas" "$(echo "$extra" | tr '\n' ' ')"; fi

SCHEMA="CREATE TABLE p(id INTEGER PRIMARY KEY, name TEXT NOT NULL, n INT CHECK(n>0));
CREATE TABLE c(id INTEGER PRIMARY KEY AUTOINCREMENT, pid INT REFERENCES p(id), v TEXT);
CREATE INDEX c_pid ON c(pid);
CREATE VIEW v AS SELECT * FROM p;
INSERT INTO p VALUES(1,'a',1),(2,'b',2);
INSERT INTO c(pid,v) VALUES(1,'x'),(2,'y');"

# Read, write, read again. Pragmas with no write form just read.
stmts_for() {
  case "$1" in
    analysis_limit)     echo "PRAGMA analysis_limit; PRAGMA analysis_limit=100; PRAGMA analysis_limit;" ;;
    application_id)     echo "PRAGMA application_id; PRAGMA application_id=42; PRAGMA application_id;" ;;
    automatic_index)    echo "PRAGMA automatic_index; PRAGMA automatic_index=0; PRAGMA automatic_index;" ;;
    busy_timeout)       echo "PRAGMA busy_timeout; PRAGMA busy_timeout=250; PRAGMA busy_timeout;" ;;
    case_sensitive_like) echo "PRAGMA case_sensitive_like=1; SELECT 'A' LIKE 'a';" ;;
    collation_list)     echo "SELECT name FROM pragma_collation_list ORDER BY 1;" ;;
    compile_options)    echo "SELECT count(*)>10 FROM pragma_compile_options;" ;;
    count_changes)      echo "PRAGMA count_changes; PRAGMA count_changes=1; UPDATE p SET n=n WHERE id=1;" ;;
    data_version)       echo "PRAGMA data_version;" ;;
    database_list)      echo "SELECT seq, name FROM pragma_database_list;" ;;
    default_cache_size) echo "PRAGMA default_cache_size; PRAGMA default_cache_size=1000; PRAGMA default_cache_size;" ;;
    defer_foreign_keys) echo "PRAGMA defer_foreign_keys; PRAGMA defer_foreign_keys=1; PRAGMA defer_foreign_keys;" ;;
    empty_result_callbacks) echo "PRAGMA empty_result_callbacks; PRAGMA empty_result_callbacks=1; PRAGMA empty_result_callbacks;" ;;
    foreign_key_check)  echo "PRAGMA foreign_keys=0; INSERT INTO c(pid,v) VALUES(99,'z'); PRAGMA foreign_key_check; PRAGMA foreign_key_check(c);" ;;
    foreign_key_list)   echo "PRAGMA foreign_key_list(c);" ;;
    foreign_keys)       echo "PRAGMA foreign_keys; PRAGMA foreign_keys=1; PRAGMA foreign_keys; INSERT INTO c(pid,v) VALUES(99,'z');" ;;
    full_column_names)  echo "PRAGMA full_column_names; PRAGMA full_column_names=1; PRAGMA full_column_names;" ;;
    fullfsync)          echo "PRAGMA fullfsync; PRAGMA fullfsync=1; PRAGMA fullfsync;" ;;
    function_list)      echo "SELECT count(*)>100 FROM pragma_function_list;" ;;
    hard_heap_limit)    echo "PRAGMA hard_heap_limit; PRAGMA hard_heap_limit=200000000; PRAGMA hard_heap_limit;" ;;
    ignore_check_constraints) echo "PRAGMA ignore_check_constraints=1; INSERT INTO p VALUES(9,'z',-1); SELECT n FROM p WHERE id=9;" ;;
    index_info)         echo "PRAGMA index_info(c_pid);" ;;
    index_list)         echo "PRAGMA index_list(c);" ;;
    legacy_alter_table) echo "PRAGMA legacy_alter_table; PRAGMA legacy_alter_table=1; PRAGMA legacy_alter_table;" ;;
    module_list)        echo "SELECT count(*)>3 FROM pragma_module_list;" ;;
    optimize)           echo "PRAGMA optimize; PRAGMA optimize(0x10002);" ;;
    pragma_list)        echo "SELECT count(*)>50 FROM pragma_pragma_list;" ;;
    query_only)         echo "PRAGMA query_only; PRAGMA query_only=1; PRAGMA query_only; INSERT INTO p VALUES(8,'q',1); PRAGMA query_only=0;" ;;
    read_uncommitted)   echo "PRAGMA read_uncommitted; PRAGMA read_uncommitted=1; PRAGMA read_uncommitted;" ;;
    recursive_triggers) echo "PRAGMA recursive_triggers; PRAGMA recursive_triggers=1; PRAGMA recursive_triggers;" ;;
    reverse_unordered_selects) echo "PRAGMA reverse_unordered_selects=1; SELECT id FROM p;" ;;
    short_column_names) echo "PRAGMA short_column_names; PRAGMA short_column_names=0; PRAGMA short_column_names;" ;;
    shrink_memory)      echo "PRAGMA shrink_memory;" ;;
    soft_heap_limit)    echo "PRAGMA soft_heap_limit; PRAGMA soft_heap_limit=100000000; PRAGMA soft_heap_limit;" ;;
    temp_store)         echo "PRAGMA temp_store; PRAGMA temp_store=MEMORY; PRAGMA temp_store;" ;;
    temp_store_directory) echo "PRAGMA temp_store_directory;" ;;
    threads)            echo "PRAGMA threads; PRAGMA threads=4; PRAGMA threads;" ;;
    trusted_schema)     echo "PRAGMA trusted_schema; PRAGMA trusted_schema=0; PRAGMA trusted_schema;" ;;
    user_version)       echo "PRAGMA user_version; PRAGMA user_version=7; PRAGMA user_version;" ;;
    journal_mode)       echo "PRAGMA journal_mode; PRAGMA journal_mode=DELETE; PRAGMA journal_mode;" ;;
    journal_size_limit) echo "PRAGMA journal_size_limit; PRAGMA journal_size_limit=1000; PRAGMA journal_size_limit;" ;;
    wal_checkpoint)     echo "PRAGMA wal_checkpoint; PRAGMA wal_checkpoint(TRUNCATE);" ;;
    wal_autocheckpoint) echo "PRAGMA wal_autocheckpoint; PRAGMA wal_autocheckpoint=100; PRAGMA wal_autocheckpoint;" ;;
    auto_vacuum)        echo "PRAGMA auto_vacuum; PRAGMA auto_vacuum=FULL; PRAGMA auto_vacuum;" ;;
    incremental_vacuum) echo "PRAGMA incremental_vacuum; PRAGMA incremental_vacuum(1);" ;;
    encoding)           echo "PRAGMA encoding; PRAGMA encoding='UTF-16le'; PRAGMA encoding;" ;;
    page_size)          echo "PRAGMA page_size; PRAGMA page_size=8192; PRAGMA page_size;" ;;
    page_count)         echo "PRAGMA page_count;" ;;
    max_page_count)     echo "PRAGMA max_page_count; PRAGMA max_page_count=1000; PRAGMA max_page_count;" ;;
    freelist_count)     echo "PRAGMA freelist_count;" ;;
    cache_spill)        echo "PRAGMA cache_spill; PRAGMA cache_spill=0; PRAGMA cache_spill;" ;;
    cache_size)         echo "PRAGMA cache_size; PRAGMA cache_size=-500; PRAGMA cache_size;" ;;
    mmap_size)          echo "PRAGMA mmap_size; PRAGMA mmap_size=1000000; PRAGMA mmap_size;" ;;
    secure_delete)      echo "PRAGMA secure_delete; PRAGMA secure_delete=1; PRAGMA secure_delete;" ;;
    locking_mode)       echo "PRAGMA locking_mode; PRAGMA locking_mode=EXCLUSIVE; PRAGMA locking_mode;" ;;
    cell_size_check)    echo "PRAGMA cell_size_check; PRAGMA cell_size_check=1; PRAGMA cell_size_check;" ;;
    checkpoint_fullfsync) echo "PRAGMA checkpoint_fullfsync; PRAGMA checkpoint_fullfsync=1; PRAGMA checkpoint_fullfsync;" ;;
    synchronous)        echo "PRAGMA synchronous; PRAGMA synchronous=OFF; PRAGMA synchronous;" ;;
    integrity_check)    echo "PRAGMA integrity_check; PRAGMA integrity_check(p);" ;;
    quick_check)        echo "PRAGMA quick_check;" ;;
    table_info)         echo "PRAGMA table_info(p); PRAGMA table_info(c);" ;;
    table_xinfo)        echo "PRAGMA table_xinfo(p);" ;;
    table_list)         echo "SELECT schema, name, type, ncol, wr, strict FROM pragma_table_list WHERE name IN ('p','c') ORDER BY name;" ;;
    index_xinfo)        echo "PRAGMA index_xinfo(c_pid);" ;;
    writable_schema)    echo "PRAGMA writable_schema; PRAGMA writable_schema=1; PRAGMA writable_schema;" ;;
    schema_version)     echo "PRAGMA schema_version=500; PRAGMA schema_version;" ;;
    data_store_directory|lock_proxy_file) echo "SELECT 'platform';" ;;
    *) return 1 ;;
  esac
}

run_on() {  # run_on <binary> <dbfile> <sql> <outfile>; returns the engine status
  local st
  rm -f "$2"
  if ! "$1" "$2" "$SCHEMA" >"$4" 2>&1; then
    echo "SETUP FAILED: $(tr '\n' ' ' <"$4")" >"$4"
    return 99
  fi
  "$1" "$2" "$3" >"$4.raw" 2>&1
  st=$?
  sed -E 's/^(Parse|Runtime) error[^:]*: /ERROR: /; s/^Error[^:]*: /ERROR: /' "$4.raw" >"$4"
  return $st
}

# Same-as-SQLite: identical normalized output and identical exit status. A
# probe may error on purpose (a violated constraint) as long as both do.
for prag in $same_list; do
  sql=$(stmts_for "$prag") || { bad "same_$prag" "no probe statements for $prag; add a case to stmts_for"; continue; }
  run_on "$DOLTLITE" "$TMPDIR/dl.db" "$sql" "$TMPDIR/dl.out"; st_dl=$?
  run_on "$SQLITE3" "$TMPDIR/sq.db" "$sql" "$TMPDIR/sq.out"; st_sq=$?
  dl=$(tr '\n' '|' <"$TMPDIR/dl.out"); sq=$(tr '\n' '|' <"$TMPDIR/sq.out")
  if [ "$st_dl" = 99 ] || [ "$st_sq" = 99 ]; then bad "same_$prag" "doltlite: $dl  stock: $sq"
  elif [ "$dl" = "$sq" ] && [ "$st_dl" = "$st_sq" ]; then ok "same_$prag"
  else bad "same_$prag" "doltlite(rc=$st_dl): $dl  stock(rc=$st_sq): $sq"; fi
done

# Accepted and inert: DoltLite reads back the documented value and exits 0.
asserted=""
expect_dl() {  # expect_dl <name> <sql> <expected-output-joined-by-|>
  local got st
  asserted="$asserted $1"
  run_on "$DOLTLITE" "$TMPDIR/dl.db" "$2" "$TMPDIR/dl.out"; st=$?
  got=$(tr '\n' '|' <"$TMPDIR/dl.out")
  if [ "$st" = 0 ] && [ "$got" = "$3" ]; then ok "inert_$1"
  else bad "inert_$1" "got $got (rc=$st) expected $3 (rc=0)"; fi
}
expect_dl journal_mode   "PRAGMA journal_mode=DELETE; PRAGMA journal_mode=MEMORY; PRAGMA journal_mode;" "wal|wal|wal|"
expect_dl wal_checkpoint "PRAGMA wal_checkpoint; PRAGMA wal_checkpoint(TRUNCATE);" "0|0|0|0|0|0|"
expect_dl wal_autocheckpoint "PRAGMA wal_autocheckpoint; PRAGMA wal_autocheckpoint=100; PRAGMA wal_autocheckpoint;" "1000|100|100|"
expect_dl auto_vacuum    "PRAGMA auto_vacuum=FULL; PRAGMA auto_vacuum;" "0|"
expect_dl incremental_vacuum "PRAGMA incremental_vacuum; PRAGMA incremental_vacuum(1); SELECT 'done';" "done|"
expect_dl encoding       "PRAGMA encoding='UTF-16le'; PRAGMA encoding;" "UTF-8|"
expect_dl page_size      "PRAGMA page_size=8192; PRAGMA page_size;" "8192|"
expect_dl page_count     "SELECT typeof(page_count), page_count>0 FROM pragma_page_count;" "integer|1|"
expect_dl max_page_count "PRAGMA max_page_count=1000; SELECT typeof(max_page_count), max_page_count >= (SELECT page_count FROM pragma_page_count) FROM pragma_max_page_count;" "1000|integer|1|"
expect_dl freelist_count "PRAGMA freelist_count;" "0|"
expect_dl cache_spill    "PRAGMA cache_spill=0; PRAGMA cache_spill;" "0|"
expect_dl mmap_size      "PRAGMA mmap_size=1000000; PRAGMA mmap_size;" "0|0|"
expect_dl journal_size_limit "PRAGMA journal_size_limit=1000; PRAGMA journal_size_limit;" "-1|-1|"
expect_dl secure_delete  "PRAGMA secure_delete=1; PRAGMA secure_delete;" "0|0|"
expect_dl locking_mode   "PRAGMA locking_mode; PRAGMA locking_mode=EXCLUSIVE; PRAGMA locking_mode;" "normal|exclusive|exclusive|"
expect_dl cell_size_check "PRAGMA cell_size_check; PRAGMA cell_size_check=1; PRAGMA cell_size_check;" "0|1|"
expect_dl checkpoint_fullfsync "PRAGMA checkpoint_fullfsync; PRAGMA checkpoint_fullfsync=1; PRAGMA checkpoint_fullfsync;" "0|1|"
expect_dl schema_version "PRAGMA schema_version=500; SELECT CASE WHEN (SELECT 1 FROM pragma_schema_version WHERE schema_version=500) IS NULL THEN 'ignored' END;" "ignored|"

unasserted=$(comm -23 <(echo "$inert_list") <(echo "$asserted" | tr ' ' '\n' | sort -u))
if [ -z "$unasserted" ]; then ok "every inert pragma in pragmas.md has an assertion"
else bad "inert pragmas documented without an assertion" "$(echo "$unasserted" | tr '\n' ' ')"; fi

echo ""
echo "================================"
echo "Results: $pass passed, $fail failed"
echo "================================"
[ "$fail" -gt 0 ] && exit 1
exit 0
