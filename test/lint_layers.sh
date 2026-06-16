#!/bin/bash

SRCDIR="${1:-src}"
TMPFILE=$(mktemp)
trap "rm -f $TMPFILE" EXIT

lint() {
  echo "LINT: $1" >> "$TMPFILE"
  echo "LINT: $1"
}

for f in "$SRCDIR"/prolly_*.c; do
  while IFS= read -r line; do
    lint "$f:$line — prolly layer must not include doltlite headers"
  done < <(grep -n '#include.*"doltlite_' "$f" | grep -v 'doltlite_commit\.h')
done

for f in "$SRCDIR"/prolly_*.c; do
  while IFS= read -r line; do
    lint "$f:$line — prolly layer must not use high-level SQL APIs"
  done < <(grep -n 'sqlite3_prepare_v2\|sqlite3_step\|sqlite3_exec' "$f" | grep -v ':[[:space:]]*/\*\|:[[:space:]]*\*\*\|:[[:space:]]*\*[[:space:]]')
done

for f in "$SRCDIR"/chunk_*.c "$SRCDIR"/chunk_*.h; do
  [ -f "$f" ] || continue
  while IFS= read -r line; do
    lint "$f:$line — chunk_* layer must not depend on prolly_btree or doltlite"
  done < <(grep -n '#include.*"prolly_btree\|#include.*"doltlite_' "$f")
done

for f in "$SRCDIR"/doltlite_*.c; do
  while IFS= read -r line; do
    lint "$f:$line — use doltliteGetCache() instead of pointer arithmetic"
  done < <(grep -n 'sizeof(ChunkStore)' "$f")
done

for f in "$SRCDIR"/doltlite_*.c; do
  while IFS= read -r line; do
    lint "$f:$line — use #include \"doltlite_internal.h\" instead of inline externs"
  done < <(grep -n '^extern.*doltliteGet\|^extern.*doltliteLoad\|^extern.*doltliteFlush\|^extern.*doltliteResolve\|^extern.*doltliteHardReset' "$f")
done

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
