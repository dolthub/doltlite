#!/usr/bin/env bash

set -euo pipefail

source_dir="${1:?Usage: build-optimized-benchmark.sh <source-directory>}"
mkdir -p "$source_dir/build"
cd "$source_dir/build"
TCL_CONFIG=$(find /usr/lib -name tclConfig.sh -print -quit 2>/dev/null)
if [ -n "$TCL_CONFIG" ]; then
  ../configure --with-tcl="$(dirname "$TCL_CONFIG")"
else
  ../configure
fi
{
  make -j2 doltlite doltlite-lib
  cc -O2 -Werror -I. -I../src \
    -o bench_timer_doltlite ../test/sysbench_timer.c \
    libdoltlite.a -lpthread -lz -lm
} 2>&1 | tee build.log
if grep -E 'warning:' build.log; then
  echo '::error::compiler warnings in optimized build'
  exit 1
fi
