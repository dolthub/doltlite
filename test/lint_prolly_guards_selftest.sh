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

# The upstream fetch is the one network step in lint. A flake there must
# cost a retry, not a guard violation, so shadow git and count the calls.
FETCH_WORK=$(mktemp -d)
trap 'rm -rf "$WORK" "$FETCH_WORK"' EXIT
mkdir -p "$FETCH_WORK/bin"
cat >"$FETCH_WORK/bin/git" <<'GITEOF'
#!/usr/bin/env bash
case "$1" in
  cat-file) exit 1 ;;
  fetch)
    count=0
    [ -f "$FAKE_GIT_FETCHES" ] && count=$(cat "$FAKE_GIT_FETCHES")
    count=$((count + 1))
    printf '%s\n' "$count" >"$FAKE_GIT_FETCHES"
    echo "fake fetch $count"
    [ "$count" -ge "${FAKE_GIT_FETCH_OK_AT:-0}" ] && [ "${FAKE_GIT_FETCH_OK_AT:-0}" -gt 0 ]
    exit $?
    ;;
  *) exit 0 ;;
esac
GITEOF
chmod +x "$FETCH_WORK/bin/git"

# Runs in a subshell, so the tally goes to a file the caller reads back.
run_fetch_case() {  # run_fetch_case <succeed-on-call|0> <attempts>
  rm -f "$FETCH_WORK/count"
  FAKE_GIT_FETCH_OK_AT="$1" \
  DOLTLITE_LINT_FETCH_ATTEMPTS="$2" \
  DOLTLITE_LINT_FETCH_DELAY_SECONDS=0 \
  FAKE_GIT_FETCHES="$FETCH_WORK/count" \
  PATH="$FETCH_WORK/bin:$PATH" \
    bash "$LINT" "$WORK" 2>&1
}
fetch_count() { cat "$FETCH_WORK/count" 2>/dev/null || echo 0; }

# Never succeeds: every attempt tries the shallow fetch and the full one.
out=$(run_fetch_case 0 3)
if [ "$(fetch_count)" -eq 6 ] \
 && echo "$out" | grep -q "could not fetch .* in 3 attempts"; then
  ok
else
  bad "fetch_retries_until_attempts_run_out" \
      "expected 6 fetch calls and a final message, got $(fetch_count)
$out"
fi

# Stops as soon as one succeeds.
out=$(run_fetch_case 3 3)
if [ "$(fetch_count)" -eq 3 ] && ! echo "$out" | grep -q "could not fetch"; then
  ok
else
  bad "fetch_stops_after_a_success" \
      "expected 3 fetch calls and no failure message, got $(fetch_count)
$out"
fi

# One attempt means no retry at all.
out=$(run_fetch_case 0 1)
if [ "$(fetch_count)" -eq 2 ] && ! echo "$out" | grep -q "retrying in"; then
  ok
else
  bad "fetch_attempts_are_configurable" \
      "expected 2 fetch calls and no retry notice, got $(fetch_count)
$out"
fi

echo
echo "Results: $PASS passed, $FAIL failed out of $((PASS + FAIL)) tests"
if [ "$FAIL" -gt 0 ]; then
  echo -e "$ERRORS"
  exit 1
fi
exit 0
