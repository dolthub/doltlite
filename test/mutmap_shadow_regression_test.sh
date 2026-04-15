#!/usr/bin/env bash
set -euo pipefail

cd build

cc -g -DDOLTLITE_PROLLY=1 -DDOLTLITE_MUTMAP_SHADOW_VALIDATE=1 \
  -D_HAVE_SQLITE_CONFIG_H -I. -I../src \
  -c ../src/prolly_mutmap.c -o prolly_mutmap_shadow.o

cc -g -I. -I../src -o doltlite_regression_test_c_shadow \
  ../test/doltlite_regression_test_c.c \
  prolly_mutmap_shadow.o libdoltlite.a -lz -lpthread -lm

./doltlite_regression_test_c_shadow mutmap_differential_randomized
