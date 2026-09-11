#!/usr/bin/env bash
set -euo pipefail

mode="${1:?build or validate required}"
cache="${2:?cache directory required}"
root="$(cd "$(dirname "$0")/../.." && pwd)"
revision=db57eba95d7c412bb413da5480c8be24109a8faf
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
case "$mode" in
  build)
    fossil clone https://www.sqlite.org/sqllogictest/ "$work/source.fossil"
    mkdir -p "$cache"
    cache="$(cd "$cache" && pwd)"
    mkdir "$cache/tree"
    (
      cd "$cache/tree"
      fossil open "$work/source.fossil" "$revision"
      fossil close
      perl "$root/test/patch_sqllogictest.pl" < src/sqllogictest.c > src/sqllogictest.c.patched
      mv src/sqllogictest.c.patched src/sqllogictest.c
      grep -q 'SQLINTEGER indicator' src/slt_odbc3.c
      perl -pi -e 's/SQLINTEGER indicator/SQLLEN indicator/g' src/slt_odbc3.c
      cd src
      gcc -Werror -g -O2 -DSQLITE_NO_SYNC=1 -DSQLITE_THREADSAFE=0 \
        -DSQLITE_OMIT_LOAD_EXTENSION -c md5.c
      gcc -Werror -g -O2 -DSQLITE_NO_SYNC=1 -DSQLITE_THREADSAFE=0 \
        -DSQLITE_OMIT_LOAD_EXTENSION -c sqlite3.c
      gcc -Werror -g -O2 -o sqllogictest-stock \
        sqllogictest.c md5.o sqlite3.o -lpthread -lm -lodbc
    )
    printf '%s\n' "$revision" > "$cache/revision"
    (cd "$cache" && find tree -type f -exec shasum -a 256 {} + > MANIFEST)
    ;;
  validate) ;;
  *) exit 1 ;;
esac
test "$(cat "$cache/revision")" = "$revision"
(cd "$cache" && shasum -a 256 -c MANIFEST > /dev/null)
for name in sqllogictest.c slt_odbc3.c sqlite3.c sqlite3.h md5.o sqlite3.o; do
  test -s "$cache/tree/src/$name"
done
test -n "$(find "$cache/tree/test" -type f -name '*.test' -print -quit)"
cat > "$work/smoke.test" <<'SQL'
statement ok
CREATE TABLE cache_smoke(v INTEGER)

statement ok
INSERT INTO cache_smoke VALUES(42)

query I
SELECT v FROM cache_smoke
----
42

statement error
SELECT dolt_version()
SQL
"$cache/tree/src/sqllogictest-stock" --verify "$work/smoke.test" 2>&1 | tee "$work/smoke.log"
grep -Eq '^0 errors out of 4 tests .* - 0 skipped\.$' "$work/smoke.log"
