#!/bin/bash

SRCDIR="${1:-src}"
TMPFILE=$(mktemp)
trap "rm -f $TMPFILE" EXIT

lint() {
  echo "LINT: $1" >> "$TMPFILE"
  echo "LINT: $1"
}

# Keep the Prolly B-tree implementation split into reviewable units. The
# private header is intentionally excluded because it contains shared types.
for f in "$SRCDIR"/prolly_btree*.c; do
  [ -f "$f" ] || continue
  nline=$(wc -l < "$f" | tr -d ' ')
  if [ "$nline" -gt 3000 ]; then
    lint "$f:$nline lines — Prolly B-tree modules must stay at or below 3000 lines"
  fi
done

# Keep doltlite merge split into reviewable units. The core catalog/rows/
# schema/pass1 modules must exist (so a re-monolith can't drop a file and
# still pass by only checking what remains) and stay under 1500 lines.
# Command and constraints modules get a higher but still finite cap.
for f in \
  "$SRCDIR"/doltlite_merge.c \
  "$SRCDIR"/doltlite_merge_pass1.c \
  "$SRCDIR"/doltlite_merge_rows.c \
  "$SRCDIR"/doltlite_merge_schema.c
do
  if [ ! -f "$f" ]; then
    lint "$f: missing — doltlite merge must stay split (catalog/pass1/rows/schema)"
    continue
  fi
  nline=$(wc -l < "$f" | tr -d ' ')
  if [ "$nline" -gt 1500 ]; then
    lint "$f:$nline lines — doltlite merge core modules must stay at or below 1500 lines"
  fi
done

for f in "$SRCDIR"/doltlite_merge_cmd.c "$SRCDIR"/doltlite_merge_constraints.c; do
  if [ ! -f "$f" ]; then
    lint "$f: missing — expected doltlite merge command/constraints module"
    continue
  fi
  nline=$(wc -l < "$f" | tr -d ' ')
  if [ "$nline" -gt 2000 ]; then
    lint "$f:$nline lines — doltlite merge cmd/constraints must stay at or below 2000 lines"
  fi
done

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
