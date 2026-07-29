#!/bin/bash
#
# The libdoltlite export filter is spelled twice because ld and ld64 take
# different formats: src/libdoltlite.map (ELF version script) and
# src/libdoltlite.sym (Mach-O exported symbols list, names underscore-prefixed).
# Drift between them means one platform ships a different ABI than the other,
# and CI only notices on whichever platform lost a symbol. Compare the pattern
# sets directly.

SRCDIR="${1:-src}"
MAP="$SRCDIR/libdoltlite.map"
SYM="$SRCDIR/libdoltlite.sym"
rc=0

for f in "$MAP" "$SYM"; do
  if [ ! -f "$f" ]; then
    echo "LINT: $f: missing — libdoltlite must ship an export filter"
    exit 1
  fi
done

map_pats=$(sed 's;/\*.*\*/;;' "$MAP" \
           | sed -n '/global:/,/local:/p' \
           | grep -oE '[A-Za-z_][A-Za-z0-9_]*\*?;' \
           | tr -d ';' | sort -u)

sym_pats=$(grep -vE '^\s*(#|$)' "$SYM" | sed 's/^_//' | sort -u)

if [ "$map_pats" != "$sym_pats" ]; then
  echo "LINT: $MAP and $SYM declare different export patterns"
  diff <(printf '%s\n' "$map_pats") <(printf '%s\n' "$sym_pats") \
    | sed 's/^/LINT:   /'
  rc=1
fi

if ! grep -qE '^\s*local:' "$MAP"; then
  echo "LINT: $MAP: no 'local: *;' clause — nothing would be hidden"
  rc=1
fi

if [ "$rc" = 0 ]; then
  echo "lint_export_filters: all checks passed"
fi

exit $rc
