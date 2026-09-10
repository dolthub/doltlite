#!/usr/bin/env bash
# SQLite-core files above btree.h must match .sqlite-upstream-base after
# DOLTLITE_PROLLY branches are stripped. Otherwise DOLTLITE_PROLLY=0 is
# not stock and the vs-stock oracles compare the engine to itself.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
SRC="${1:-$REPO/src}"
STRIP="$HERE/lib/strip_doltlite_prolly.py"
BASE_FILE="$REPO/.sqlite-upstream-base"

# Still differ from upstream after stripping. Shrink this list; do not grow it.
skip_file() {
  case "$1" in
    test*.c|tclsqlite*)
      return 0 ;;
    backup.c|btmutex.c|btree.c|btree.h|dbstat.c|func.c|memdb.c|os.c|os_kv.c|os_unix.c|os_win.c|pcache.c|sqlite.h.in|sqliteInt.h|vacuum.c|vdbeaux.c|vdbeblob.c|vdbesort.c|vtab.c|wherecode.c)
      return 0 ;;
    *) return 1 ;;
  esac
}

if [ ! -f "$STRIP" ]; then
  echo "lint_prolly_guards: missing $STRIP" >&2
  exit 2
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "lint_prolly_guards: python3 is required" >&2
  exit 2
fi
if [ ! -f "$BASE_FILE" ]; then
  echo "lint_prolly_guards: missing $BASE_FILE" >&2
  exit 2
fi

SHA=$(tr -d '[:space:]' < "$BASE_FILE")
if [ -z "$SHA" ]; then
  echo "lint_prolly_guards: $BASE_FILE is empty" >&2
  exit 2
fi

cd "$REPO"
if ! git cat-file -e "$SHA:src/vdbe.c" 2>/dev/null; then
  echo "lint_prolly_guards: fetching $SHA for the upstream originals"
  git fetch --depth=1 origin "$SHA" || git fetch origin "$SHA"
  if ! git cat-file -e "$SHA:src/vdbe.c" 2>/dev/null; then
    echo "lint_prolly_guards: cannot read $SHA:src/vdbe.c" >&2
    echo "  that SHA is .sqlite-upstream-base; fetch it or fix the file" >&2
    exit 2
  fi
fi

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/up"
git archive "$SHA" src | tar -x -C "$WORK/up"

FAIL=0
CHECKED=0
SKIPPED=0

shopt -s nullglob
for path in "$SRC"/*.c "$SRC"/*.h; do
  [ -f "$path" ] || continue
  base=$(basename "$path")
  if skip_file "$base"; then
    SKIPPED=$((SKIPPED + 1))
    continue
  fi
  up="$WORK/up/src/$base"
  [ -f "$up" ] || continue
  CHECKED=$((CHECKED + 1))
  python3 "$STRIP" "$path" > "$WORK/head"
  if ! diff -q "$up" "$WORK/head" >/dev/null; then
    echo "LINT: src/$base differs from $SHA after stripping DOLTLITE_PROLLY"
    echo "      wrap the edit in #ifdef DOLTLITE_PROLLY, or if this is an"
    echo "      upstream merge, update .sqlite-upstream-base"
    diff -u "$up" "$WORK/head" | head -40 | sed 's/^/      /'
    FAIL=$((FAIL + 1))
  fi
done

if [ "$CHECKED" -eq 0 ]; then
  echo "lint_prolly_guards: checked no files" >&2
  exit 2
fi

if [ "$FAIL" -ne 0 ]; then
  echo "lint_prolly_guards: $FAIL file(s) drifted from stock SQLite ($CHECKED checked, $SKIPPED still-open)"
  exit 1
fi
echo "lint_prolly_guards: $CHECKED file(s) match $SHA after stripping DOLTLITE_PROLLY"
exit 0
