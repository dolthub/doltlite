#!/bin/bash
# schema_version is a function of the schema: every connection that sees the
# same schema reads the same value, however that schema was reached.
DLTEST_STRIP_CR=1
. "$(dirname "$0")/lib/doltlite_test_common.sh"

echo "=== schema_version is derived from the schema ==="
echo ""

ROOT=$(mktemp -d ./.doltlite-schema-version.XXXXXX)
trap 'rm -rf "$ROOT"' EXIT
SV="SELECT schema_version FROM pragma_schema_version;"

fresh() { "$DOLTLITE" "$1" "$SV" 2>&1; }

DB="$ROOT/peers.db"
"$DOLTLITE" "$DB" "SELECT 1;" >/dev/null 2>&1
out=$("$DOLTLITE" 2>&1 <<EOF
.open $DB
.connection 1
.open $DB
.connection 0
CREATE TABLE t(x);
SELECT 'A1|' || schema_version FROM pragma_schema_version;
.connection 1
SELECT 'B1|' || schema_version FROM pragma_schema_version;
CREATE INDEX ti ON t(x);
SELECT 'B2|' || schema_version FROM pragma_schema_version;
.connection 0
SELECT 'A2|' || schema_version FROM pragma_schema_version;
EOF
)
a1=$(printf '%s\n' "$out" | sed -n 's/^A1|//p')
b1=$(printf '%s\n' "$out" | sed -n 's/^B1|//p')
a2=$(printf '%s\n' "$out" | sed -n 's/^A2|//p')
b2=$(printf '%s\n' "$out" | sed -n 's/^B2|//p')
c=$(fresh "$DB")
if [ -n "$a1" ] && [ "$a1" = "$b1" ] && [ "$a2" = "$b2" ] \
   && [ "$a2" = "$c" ] && [ "$a1" != "$a2" ]; then
  dltest_pass
else
  dltest_fail "connections_agree_after_each_ddl" \
    "  A1=$a1 B1=$b1 A2=$a2 B2=$b2 fresh=$c"
fi

"$DOLTLITE" "$ROOT/p1.db" "CREATE TABLE t(x); CREATE TABLE u(y); CREATE INDEX ti ON t(x);" >/dev/null 2>&1
"$DOLTLITE" "$ROOT/p2.db" "CREATE TABLE u(y); CREATE TABLE t(x); CREATE TABLE z(q); DROP TABLE z; CREATE INDEX ti ON t(x);" >/dev/null 2>&1
dltest_assert_equal "same_schema_by_different_paths" \
  "$(fresh "$ROOT/p1.db")" "$(fresh "$ROOT/p2.db")"

DB="$ROOT/vc.db"
"$DOLTLITE" "$DB" "CREATE TABLE t(x); SELECT dolt_commit('-Am','c1');
SELECT dolt_branch('b'); SELECT dolt_checkout('b');
ALTER TABLE t ADD COLUMN y; SELECT dolt_commit('-am','b');
SELECT dolt_checkout('main');
CREATE TABLE m(z); SELECT dolt_commit('-Am','m');" >/dev/null 2>&1
MAIN=$(fresh "$DB")
BR=$(fresh "$DB/b")
dltest_assert_equal "checkout_reads_branch_value" \
  "$("$DOLTLITE" "$DB" "SELECT dolt_checkout('b'); $SV" 2>&1 | tail -1)" "$BR"
dltest_assert_equal "checkout_round_trip_restores_value" \
  "$("$DOLTLITE" "$DB" "SELECT dolt_checkout('b'); SELECT dolt_checkout('main'); $SV" 2>&1 | tail -1)" "$MAIN"
merged=$("$DOLTLITE" "$DB" "SELECT dolt_merge('b'); $SV" 2>&1 | tail -1)
dltest_assert_equal "merge_value_matches_fresh_connection" "$merged" "$(fresh "$DB")"
reset=$("$DOLTLITE" "$DB" "SELECT dolt_reset('--hard','HEAD~1'); $SV" 2>&1 | tail -1)
dltest_assert_equal "reset_value_matches_fresh_connection" "$reset" "$(fresh "$DB")"
dltest_assert_equal "reset_returns_to_earlier_value" "$reset" "$MAIN"

"$DOLTLITE" "$ROOT/set.db" "CREATE TABLE q(x);" >/dev/null 2>&1
before=$(fresh "$ROOT/set.db")
after=$("$DOLTLITE" "$ROOT/set.db" "PRAGMA schema_version=500; $SV" 2>&1 | tail -1)
dltest_assert_equal "assigned_value_does_not_stick" "$after" "$before"

dltest_finish
