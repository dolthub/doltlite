#!/usr/bin/env bash
# VEC1=1 then VEC1=0 must rebuild main.o. Otherwise the object still
# references sqlite3Vec1Init after vec1.o is dropped from the link.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
tmp="$(mktemp -d "${TMPDIR:-/tmp}/doltlite-feature-stamp.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT
cd "$tmp"

cat > main.c <<'EOF'
#if defined(DOLTLITE_PROLLY) && DOLTLITE_VEC1
int sqlite3Vec1Init(void);
static int (*const sqlite3BuiltinExtensions[])(void) = { sqlite3Vec1Init };
#endif
int main(void) { return 0; }
EOF
cat > vec1.c <<'EOF'
int sqlite3Vec1Init(void) { return 0; }
EOF

cat > Makefile <<EOF
TOP := $REPO
CC ?= cc
CFLAGS :=
LIBOBJS0 = main.o
include \$(TOP)/doltlite.mk

main.o: main.c
	\$(CC) -c main.c -o main.o \$(OPT_FEATURE_FLAGS)
vec1.o: vec1.c
	\$(CC) -c vec1.c -o vec1.o
app: main.o \$(if \$(filter 1,\$(DOLTLITE_VEC1)),vec1.o)
	\$(CC) -o app main.o \$(if \$(filter 1,\$(DOLTLITE_VEC1)),vec1.o)
EOF

make DOLTLITE_PROLLY=1 DOLTLITE_VEC1=1 app
if ! ./app; then
  echo "FAIL: VEC1=1 app did not run" >&2
  exit 1
fi
if ! grep -q 'VEC1=1' .doltlite-feature-flags; then
  echo "FAIL: stamp missing VEC1=1: $(cat .doltlite-feature-flags)" >&2
  exit 1
fi

qrc=0
make DOLTLITE_PROLLY=1 DOLTLITE_VEC1=0 -q main.o || qrc=$?
if [ "$qrc" -eq 0 ]; then
  echo "FAIL: VEC1=0 left main.o up to date" >&2
  exit 1
fi
if [ "$qrc" -ne 1 ]; then
  echo "FAIL: make -q VEC1=0 main.o rc=$qrc" >&2
  exit 1
fi

make DOLTLITE_PROLLY=1 DOLTLITE_VEC1=0 app
if ! ./app; then
  echo "FAIL: VEC1=0 app did not run" >&2
  exit 1
fi
if ! grep -q 'VEC1=0' .doltlite-feature-flags; then
  echo "FAIL: stamp missing VEC1=0: $(cat .doltlite-feature-flags)" >&2
  exit 1
fi
if command -v nm >/dev/null 2>&1 && nm main.o 2>/dev/null | grep -q 'U .*sqlite3Vec1Init'; then
  echo "FAIL: VEC1=0 main.o still references sqlite3Vec1Init" >&2
  nm main.o | grep vec1 >&2 || true
  exit 1
fi

qrc=0
make DOLTLITE_PROLLY=1 DOLTLITE_VEC1=0 -q main.o || qrc=$?
if [ "$qrc" -ne 0 ]; then
  echo "FAIL: second VEC1=0 make rebuilt main.o rc=$qrc" >&2
  exit 1
fi

echo "feature flags stamp: PASS"
