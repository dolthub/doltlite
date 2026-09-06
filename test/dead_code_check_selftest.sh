#!/usr/bin/env bash
# Prove the dead-code gate still fails on planted unused/duplicate-export cases.
# A clean-tree pass is the CI job's dead_code_check.sh run; this file only
# checks that the detectors fire.

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
CHECK="$HERE/dead_code_check.sh"
SCAN="$HERE/lib/dead_code_scan.py"
BUILD_DIR="${DOLTLITE_BUILD_DIR:-$ROOT/build}"
CC="${CC:-cc}"

PASS=0
FAIL=0
ERRORS=""
ok() { PASS=$((PASS+1)); }
bad() { FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $1\n  $2"; }

echo "=== dead_code_check self-test ==="

if [ ! -f "$BUILD_DIR/sqlite_cfg.h" ]; then
  echo "SETUP FAILED: $BUILD_DIR/sqlite_cfg.h missing; configure+build first" >&2
  exit 2
fi

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/src"

expect_scan_hit() {
  local name="$1" needle="$2" out rc
  out=$(python3 "$SCAN" --root "$WORK" --src-root "$WORK/src" 2>&1)
  rc=$?
  if [ "$rc" -eq 0 ]; then
    bad "$name" "scanner passed; expected a hit containing '$needle'"
  elif ! printf '%s' "$out" | grep -q -- "$needle"; then
    bad "$name" "exited $rc but never mentioned '$needle':
$out"
  else
    ok
  fi
}

# Part C: unused static inline in a header.
cat > "$WORK/src/doltlite_fixture.h" <<'EOF'
#ifndef DOLTLITE_FIXTURE_H
#define DOLTLITE_FIXTURE_H
static SQLITE_INLINE int dead_code_gate_unused_inline(int x){ return x; }
#endif
EOF
: > "$WORK/src/doltlite.c"
expect_scan_hit "header_inline_is_rejected" "dead header inline: dead_code_gate_unused_inline"

# Part E: unused header prototype.
cat > "$WORK/src/doltlite_fixture.h" <<'EOF'
#ifndef DOLTLITE_FIXTURE_H
#define DOLTLITE_FIXTURE_H
int dead_code_gate_unused_proto(void);
#endif
EOF
: > "$WORK/src/doltlite.c"
expect_scan_hit "header_prototype_is_rejected" "dead header prototype: dead_code_gate_unused_proto"

# Part F: unused header macro.
cat > "$WORK/src/doltlite_fixture.h" <<'EOF'
#ifndef DOLTLITE_FIXTURE_H
#define DOLTLITE_FIXTURE_H
#define DEAD_CODE_GATE_UNUSED_MACRO 1
#endif
EOF
: > "$WORK/src/doltlite.c"
expect_scan_hit "header_macro_is_rejected" "dead header macro: DEAD_CODE_GATE_UNUSED_MACRO"

# Part G: identical function bodies in two owned .c files.
cat > "$WORK/src/doltlite_clone_a.c" <<'EOF'
static int dead_code_gate_clone_left(int a, int b, int c){
  int t;
  if( a<0 ) a = -a;
  if( b<0 ) b = -b;
  if( c<0 ) c = -c;
  t = a*b + b*c + c*a + a + b + c + 42;
  if( t<0 ) t = -t;
  t = t + a*a + b*b + c*c;
  t = t + (a+1)*(b+1)*(c+1);
  t = t + (a-1)*(b-1)*(c-1);
  return t;
}
EOF
cat > "$WORK/src/doltlite_clone_b.c" <<'EOF'
static int dead_code_gate_clone_right(int a, int b, int c){
  int t;
  if( a<0 ) a = -a;
  if( b<0 ) b = -b;
  if( c<0 ) c = -c;
  t = a*b + b*c + c*a + a + b + c + 42;
  if( t<0 ) t = -t;
  t = t + a*a + b*b + c*c;
  t = t + (a+1)*(b+1)*(c+1);
  t = t + (a-1)*(b-1)*(c-1);
  return t;
}
EOF
rm -f "$WORK/src/doltlite_fixture.h"
expect_scan_hit "duplicate_body_is_rejected" "duplicate function body: dead_code_gate_clone_left"

# Part D: non-static helper only referenced in its defining file.
cat > "$WORK/src/doltlite.c" <<'EOF'
int dead_code_gate_should_be_static(int x){
  return x;
}
static int dead_code_gate_same_file_caller(int x){
  return dead_code_gate_should_be_static(x);
}
EOF
expect_scan_hit "should_be_static_is_rejected" "should be static: dead_code_gate_should_be_static"

# Part A: unused static in a real translation unit (compiler-driven).
cp "$ROOT/src/doltlite_add.c" "$WORK/src/doltlite_add.c"
printf '\nstatic int dead_code_gate_unused_static(void){ return 0; }\n' \
  >> "$WORK/src/doltlite_add.c"
CFLAGS=(
  -DNDEBUG -O1 -g
  -DSQLITE_ENABLE_MATH_FUNCTIONS -DSQLITE_THREADSAFE=1
  -DDOLTLITE_PROLLY=1 -DDOLTLITE_VERSION='"dev"'
  -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_RTREE
  -D_HAVE_SQLITE_CONFIG_H -DBUILD_sqlite
  "-I$BUILD_DIR" "-I$ROOT/src" -I"$ROOT/ext/rtree" -I"$ROOT/ext/icu"
  -I"$ROOT/ext/fts3" -I"$ROOT/ext/session" -I"$ROOT/ext/misc"
  -I"$ROOT/ext/blake3" -I"$ROOT/ext/ed25519" -I"$ROOT/ext/mbedtls/include"
)
A_ERR=$WORK/part_a.err
if "$CC" "${CFLAGS[@]}" -Werror=unused-function -Werror=unused-variable \
      -Werror=unused-but-set-variable -c -o /dev/null \
      "$WORK/src/doltlite_add.c" 2>"$A_ERR"; then
  bad "unused_static_is_rejected" "compiler accepted an unused static function"
elif ! grep -q 'dead_code_gate_unused_static' "$A_ERR"; then
  bad "unused_static_is_rejected" "compiler failed but did not name the planted static:
$(sed 's/^/    /' "$A_ERR")"
else
  ok
fi

# Part B: unused extern (definition, no caller) on a one-file src tree.
mkdir -p "$WORK/tiny/src"
printf 'int dead_code_gate_unused_extern(void){ return 0; }\n' \
  > "$WORK/tiny/src/doltlite.c"
B_OUT=$(
  DEAD_CODE_SKIP_PART_A=1 \
  DOLTLITE_SRC_ROOT="$WORK/tiny/src" \
  DOLTLITE_BUILD_DIR="$BUILD_DIR" \
  bash "$CHECK" 2>&1
) || true
if ! printf '%s' "$B_OUT" | grep -q 'dead (no caller): dead_code_gate_unused_extern'; then
  bad "unused_extern_is_rejected" "gate did not flag planted extern:
$B_OUT"
else
  ok
fi

echo
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ "$FAIL" -gt 0 ]; then
  printf '%b\n' "$ERRORS"
  exit 1
fi
