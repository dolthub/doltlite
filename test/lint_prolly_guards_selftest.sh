#!/usr/bin/env bash
# Structural plus behavioural checks for lint_prolly_guards.sh.

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
LINT="$HERE/lint_prolly_guards.sh"
STRIP="$HERE/lib/strip_doltlite_prolly.py"

PASS=0
FAIL=0
ERRORS=""
ok() { PASS=$((PASS + 1)); }
bad() { FAIL=$((FAIL + 1)); ERRORS="$ERRORS\nFAIL: $1\n  $2"; }

echo "=== lint_prolly_guards self-test ==="

skip_body=$(sed -n '/^skip_file()/,/^}/p' "$LINT")
if echo "$skip_body" | grep -q 'btree.c'; then
  ok
else
  bad "skip_list_mentions_btree" "skip_file() should still exclude btree.c (storage seam)"
fi
for f in btree.c vdbeaux.c wherecode.c sqliteInt.h; do
  if [ -f "$REPO/src/$f" ]; then
    ok
  else
    bad "skip_target_exists:$f" "skip_file lists $f but src/$f is gone; drop it from the skip list"
  fi
done

python3 - "$STRIP" <<'PY' || exit 2
import importlib.util, sys
spec = importlib.util.spec_from_file_location("strip", sys.argv[1])
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

cases = [
    ("plain", "int x;\n", "int x;\n"),
    ("ifdef_drop",
     "#ifdef DOLTLITE_PROLLY\nfoo();\n#endif\nint x;\n",
     "int x;\n"),
    ("ifndef_keep",
     "#ifndef DOLTLITE_PROLLY\nbar();\n#endif\n",
     "bar();\n"),
    ("if_defined_drop",
     "#if defined(DOLTLITE_PROLLY)\nfoo();\n#else\nbar();\n#endif\n",
     "bar();\n"),
    ("and_short_circuit",
     "#if defined(DOLTLITE_PROLLY) && !defined(SQLITE_TEST)\nfoo();\n#endif\nint x;\n",
     "int x;\n"),
    ("passthrough_other",
     "#ifdef SQLITE_DEBUG\nfoo();\n#endif\n",
     "#ifdef SQLITE_DEBUG\nfoo();\n#endif\n"),
    ("nested_in_drop",
     "#ifdef DOLTLITE_PROLLY\n#ifdef SQLITE_DEBUG\nfoo();\n#endif\nbar();\n#endif\nint x;\n",
     "int x;\n"),
]
fail = 0
for name, src, want in cases:
    got = mod.strip(src)
    if got != want:
        fail += 1
        print("FAIL strip:%s" % name)
        print("  got:  %r" % got)
        print("  want: %r" % want)
if fail:
    sys.exit(1)
print("stripper unit tests passed")
PY
ok

if command -v unifdef >/dev/null 2>&1; then
  py=$(python3 "$STRIP" "$REPO/src/vdbe.c")
  uni=$(unifdef -UDOLTLITE_PROLLY "$REPO/src/vdbe.c" || true)
  if [ "$py" = "$uni" ]; then
    ok
  else
    bad "matches_unifdef_vdbe" "python stripper disagrees with unifdef -UDOLTLITE_PROLLY on src/vdbe.c"
  fi
else
  ok
fi

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
cp "$REPO/src/expr.c" "$WORK/expr.c"

out=$(bash "$LINT" "$WORK" 2>&1)
rc=$?
if [ "$rc" -eq 0 ] && echo "$out" | grep -q "match"; then
  ok
else
  bad "subset_src_passes" "expected a clean pass on unmodified expr.c, got rc=$rc
$out"
fi

printf '\nint leaked_unguarded;\n' >> "$WORK/expr.c"
out=$(bash "$LINT" "$WORK" 2>&1)
rc=$?
if [ "$rc" -ne 0 ] && echo "$out" | grep -q "src/expr.c"; then
  ok
else
  bad "unguarded_edit_is_rejected" "expected a src/expr.c violation, got rc=$rc
$out"
fi

cp "$REPO/src/expr.c" "$WORK/expr.c"
printf '#ifdef DOLTLITE_PROLLY\nint leaked_guarded;\n#endif\n' >> "$WORK/expr.c"
out=$(bash "$LINT" "$WORK" 2>&1)
rc=$?
if [ "$rc" -eq 0 ]; then
  ok
else
  bad "guarded_edit_is_allowed" "expected a pass for an #ifdef DOLTLITE_PROLLY addition, got rc=$rc
$out"
fi

echo
echo "Results: $PASS passed, $FAIL failed out of $((PASS + FAIL)) tests"
if [ "$FAIL" -gt 0 ]; then
  echo -e "$ERRORS"
  exit 1
fi
exit 0
