#!/usr/bin/env bash
set -euo pipefail

library="${1:?usage: release_column_metadata_exports.sh <shared library>}"
case "$library" in
  *.so|*.so.*)
    exports=$(nm -D --defined-only "$library" | awk '{print $NF}')
    ;;
  *.dylib)
    exports=$(nm -gU "$library" | awk '{print $NF}' | sed 's/^_//')
    ;;
  *.dll)
    exports=$(objdump -p "$library" | awk '
      /^Export Table:|^\[Ordinal\/Name Pointer\] Table/ { exported = 1; next }
      exported && $NF ~ /^sqlite3_/ { print $NF }
    ')
    ;;
  *)
    echo "Unsupported library format: $library" >&2
    exit 2
    ;;
esac

for field in database table origin; do
  for suffix in '' 16; do
    symbol="sqlite3_column_${field}_name${suffix}"
    if ! grep -Fxq "$symbol" <<<"$exports"; then
      echo "$library is missing export $symbol" >&2
      exit 1
    fi
  done
done
echo "$library exports the column metadata API"
