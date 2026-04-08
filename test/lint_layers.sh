#!/bin/bash
#
# Layering lint for DoltLite source.
#
# Enforces the dependency DAG:
#   doltlite_*.c  ->  prolly_*.c  ->  chunk_store.*
#
# Exit 1 if any violation is found.

SRCDIR="${1:-src}"
TMPFILE=$(mktemp)
trap "rm -f $TMPFILE" EXIT

lint() {
  echo "LINT: $1" >> "$TMPFILE"
  echo "LINT: $1"
}

# --- Rule 1: prolly_*.c must not include doltlite_*.c headers ---
# (doltlite_commit.h is exempt — it's a shared data format header)
for f in "$SRCDIR"/prolly_*.c; do
  while IFS= read -r line; do
    lint "$f:$line — prolly layer must not include doltlite headers"
  done < <(grep -n '#include.*"doltlite_' "$f" | grep -v 'doltlite_commit\.h')
done

# --- Rule 2: prolly_*.c must not call sqlite3_prepare_v2/sqlite3_step ---
# (only match actual code, not comments)
for f in "$SRCDIR"/prolly_*.c; do
  while IFS= read -r line; do
    lint "$f:$line — prolly layer must not use high-level SQL APIs"
  done < <(grep -n 'sqlite3_prepare_v2\|sqlite3_step\|sqlite3_exec' "$f" | grep -v ':[[:space:]]*/\*\|:[[:space:]]*\*\*\|:[[:space:]]*\*[[:space:]]')
done

# --- Rule 3: chunk_store.* must not include prolly_btree or doltlite headers ---
for f in "$SRCDIR"/chunk_store.c "$SRCDIR"/chunk_store.h; do
  [ -f "$f" ] || continue
  while IFS= read -r line; do
    lint "$f:$line — chunk_store must not depend on prolly_btree or doltlite"
  done < <(grep -n '#include.*"prolly_btree\|#include.*"doltlite_' "$f")
done

# --- Rule 4: No pointer arithmetic into BtShared via sizeof(ChunkStore) ---
for f in "$SRCDIR"/doltlite_*.c; do
  while IFS= read -r line; do
    lint "$f:$line — use doltliteGetCache() instead of pointer arithmetic"
  done < <(grep -n 'sizeof(ChunkStore)' "$f")
done

# --- Rule 5: No inline extern declarations in doltlite_*.c ---
# (everything should come from doltlite_internal.h)
for f in "$SRCDIR"/doltlite_*.c; do
  while IFS= read -r line; do
    lint "$f:$line — use #include \"doltlite_internal.h\" instead of inline externs"
  done < <(grep -n '^extern.*doltliteGet\|^extern.*doltliteLoad\|^extern.*doltliteFlush\|^extern.*doltliteResolve\|^extern.*doltliteHardReset' "$f")
done

# --- Rule 6: No duplicate struct TableEntry definitions ---
for f in "$SRCDIR"/doltlite_*.c; do
  if grep -q 'struct TableEntry {' "$f"; then
    lint "$f — defines struct TableEntry locally (should come from doltlite_internal.h)"
  fi
done

NFAIL=$(wc -l < "$TMPFILE" | tr -d ' ')
if [ "$NFAIL" -eq 0 ]; then
  echo "lint_layers: all checks passed"
  exit 0
else
  echo ""
  echo "lint_layers: $NFAIL violation(s) found"
  exit 1
fi
