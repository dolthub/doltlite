#!/bin/bash

DOLTLITE="${1:-./doltlite}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TMP="$(mktemp -d)"
WRAPPER="$TMP/doltlite-fail-overflow-copy"
trap 'rm -rf "$TMP"' EXIT

if ! command -v sqlite3 >/dev/null 2>&1; then
  echo "SKIP: stock sqlite3 binary not found"
  exit 0
fi

cat > "$WRAPPER" <<'EOF'
#!/bin/bash
sql=$(cat)
if [[ "$sql" == *"SELECT dolt_commit('-A','-m','overflow copy');"* ]]; then
  echo "injected overflow copy failure" >&2
  exit 37
fi
printf '%s\n' "$sql" | "$REAL_DOLTLITE" "$@"
EOF
chmod +x "$WRAPPER"

export REAL_DOLTLITE="$DOLTLITE"
output=$(bash "$SCRIPT_DIR/doltlite_sqlite_file_oracle.sh" "$WRAPPER" 2>&1)
status=$?
FAIL=0

if [ "$status" -eq 0 ]; then
  echo "FAIL: oracle accepted failed overflow copies"
  FAIL=$((FAIL+1))
fi

for shape in textpk intpk keyless; do
  if ! grep -q "FAIL: O_attach_ov_${shape}_copy" <<<"$output"; then
    echo "FAIL: missing ${shape} copy failure"
    FAIL=$((FAIL+1))
  fi
  if grep -q "FAIL: O_attach_ov_${shape}_distinct\|FAIL: O_attach_ov_${shape}_except" <<<"$output"; then
    echo "FAIL: ${shape} post-copy checks ran after the copy failed"
    FAIL=$((FAIL+1))
  fi
done

if [ "$FAIL" -ne 0 ]; then
  echo "$output"
  exit 1
fi

echo "PASS: failed overflow copies stop before post-copy assertions"
