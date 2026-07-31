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

# Cursor count extraction: require the split file. Cap only the extracted
# module at 1500 for now; the parent cursor file is tightened after
# payload/seek extractions land.
if [ ! -f "$SRCDIR/prolly_btree_cursor_count.c" ]; then
  lint "$SRCDIR/prolly_btree_cursor_count.c: missing — prolly cursor count must stay split"
else
  nline=$(wc -l < "$SRCDIR/prolly_btree_cursor_count.c" | tr -d ' ')
  if [ "$nline" -gt 1500 ]; then
    lint "$SRCDIR/prolly_btree_cursor_count.c:$nline lines — prolly_btree_cursor_count.c must stay at or below 1500 lines"
  fi
fi

if [ ! -f "$SRCDIR/prolly_btree_cursor_payload.c" ]; then
  lint "$SRCDIR/prolly_btree_cursor_payload.c: missing — prolly cursor payload must stay split"
else
  nline=$(wc -l < "$SRCDIR/prolly_btree_cursor_payload.c" | tr -d ' ')
  if [ "$nline" -gt 1500 ]; then
    lint "$SRCDIR/prolly_btree_cursor_payload.c:$nline lines — prolly_btree_cursor_payload.c must stay at or below 1500 lines"
  fi
fi

if [ ! -f "$SRCDIR/prolly_btree_cursor_seek.c" ]; then
  lint "$SRCDIR/prolly_btree_cursor_seek.c: missing — prolly cursor seek must stay split"
else
  nline=$(wc -l < "$SRCDIR/prolly_btree_cursor_seek.c" | tr -d ' ')
  if [ "$nline" -gt 1500 ]; then
    lint "$SRCDIR/prolly_btree_cursor_seek.c:$nline lines — prolly_btree_cursor_seek.c must stay at or below 1500 lines"
  fi
fi

# Branch/checkout split: require doltlite_branches.c; cap at 1500.
if [ ! -f "$SRCDIR/doltlite_branches.c" ]; then
  lint "$SRCDIR/doltlite_branches.c: missing — dolt_branches vtab must stay split"
else
  nline=$(wc -l < "$SRCDIR/doltlite_branches.c" | tr -d ' ')
  if [ "$nline" -gt 1500 ]; then
    lint "$SRCDIR/doltlite_branches.c:$nline lines — doltlite_branches.c must stay at or below 1500 lines"
  fi
fi

if [ ! -f "$SRCDIR/doltlite_checkout.c" ]; then
  lint "$SRCDIR/doltlite_checkout.c: missing — doltlite checkout must stay split"
else
  nline=$(wc -l < "$SRCDIR/doltlite_checkout.c" | tr -d ' ')
  if [ "$nline" -gt 1500 ]; then
    lint "$SRCDIR/doltlite_checkout.c:$nline lines — doltlite_checkout.c must stay at or below 1500 lines"
  fi
fi

# chunk_store split: require lock module; cap at 1200.
if [ ! -f "$SRCDIR/chunk_store_lock.c" ]; then
  lint "$SRCDIR/chunk_store_lock.c: missing — chunk_store lock must stay split"
else
  nline=$(wc -l < "$SRCDIR/chunk_store_lock.c" | tr -d ' ')
  if [ "$nline" -gt 1200 ]; then
    lint "$SRCDIR/chunk_store_lock.c:$nline lines — chunk_store_lock.c must stay at or below 1200 lines"
  fi
fi

if [ ! -f "$SRCDIR/chunk_store_refs_api.c" ]; then
  lint "$SRCDIR/chunk_store_refs_api.c: missing — chunk_store refs api must stay split"
else
  nline=$(wc -l < "$SRCDIR/chunk_store_refs_api.c" | tr -d ' ')
  if [ "$nline" -gt 1200 ]; then
    lint "$SRCDIR/chunk_store_refs_api.c:$nline lines — chunk_store_refs_api.c must stay at or below 1200 lines"
  fi
fi

if [ ! -f "$SRCDIR/chunk_store_commit.c" ]; then
  lint "$SRCDIR/chunk_store_commit.c: missing — chunk_store commit must stay split"
else
  nline=$(wc -l < "$SRCDIR/chunk_store_commit.c" | tr -d ' ')
  if [ "$nline" -gt 1200 ]; then
    lint "$SRCDIR/chunk_store_commit.c:$nline lines — chunk_store_commit.c must stay at or below 1200 lines"
  fi
fi

# Keep doltlite merge split into reviewable units. The core catalog/rows/
# schema/pass1/pass2 modules must exist (so a re-monolith can't drop a file
# and still pass by only checking what remains) and stay under 1500 lines.
# Command and constraints modules get a higher but still finite cap.
for f in \
  "$SRCDIR"/doltlite_merge.c \
  "$SRCDIR"/doltlite_merge_pass1.c \
  "$SRCDIR"/doltlite_merge_pass2.c \
  "$SRCDIR"/doltlite_merge_rows.c \
  "$SRCDIR"/doltlite_merge_schema.c
do
  if [ ! -f "$f" ]; then
    lint "$f: missing — doltlite merge must stay split (catalog/pass1/pass2/rows/schema)"
    continue
  fi
  nline=$(wc -l < "$f" | tr -d ' ')
  if [ "$nline" -gt 1500 ]; then
    lint "$f:$nline lines — doltlite merge core modules must stay at or below 1500 lines"
  fi
done

if [ ! -f "$SRCDIR"/doltlite_merge_cmd.c ]; then
  lint "$SRCDIR/doltlite_merge_cmd.c: missing — expected doltlite merge command module"
else
  nline=$(wc -l < "$SRCDIR"/doltlite_merge_cmd.c | tr -d ' ')
  if [ "$nline" -gt 2000 ]; then
    lint "$SRCDIR/doltlite_merge_cmd.c:$nline lines — doltlite merge cmd must stay at or below 2000 lines"
  fi
fi

# Constraint detectors: require the FK/unique/CHECK split so the monolith
# cannot return. Cap shared helpers + each detector at 1500.
for f in \
  "$SRCDIR"/doltlite_merge_constraints.c \
  "$SRCDIR"/doltlite_merge_constraints_unique.c \
  "$SRCDIR"/doltlite_merge_constraints_check.c \
  "$SRCDIR"/doltlite_merge_constraints_fk.c
do
  if [ ! -f "$f" ]; then
    lint "$f: missing — doltlite merge constraints must stay split (shared/unique/check/fk)"
    continue
  fi
  nline=$(wc -l < "$f" | tr -d ' ')
  if [ "$nline" -gt 1500 ]; then
    lint "$f:$nline lines — doltlite merge constraint modules must stay at or below 1500 lines"
  fi
done

if [ ! -f "$SRCDIR"/doltlite_merge_constraints_int.h ]; then
  lint "$SRCDIR/doltlite_merge_constraints_int.h: missing — merge constraints private header"
fi

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

for f in "$SRCDIR"/doltlite_*.c; do
  case "$f" in
    "$SRCDIR/doltlite_core.c"|"$SRCDIR/doltlite_config.c") continue ;;
  esac
  while IFS= read -r line; do
    lint "$f:$line — branch advances must use doltliteCompareAndAdvanceBranch()"
  done < <(grep -n 'doltliteAdvanceBranch[[:space:]]*(' "$f")
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
