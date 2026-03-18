#!/bin/bash
#
# SQLite parity tests for DoltLite.
#
# Runs identical SQL through both ./doltlite and the system sqlite3,
# compares output. Any difference is a FAIL.
#
DOLTLITE=./doltlite
SQLITE3=$(command -v sqlite3 2>/dev/null || echo /usr/bin/sqlite3)
PASS=0; FAIL=0; SKIP=0; ERRORS=""

if [ ! -x "$DOLTLITE" ]; then
  echo "ERROR: $DOLTLITE not found or not executable"
  exit 1
fi

if ! command -v "$SQLITE3" >/dev/null 2>&1; then
  echo "ERROR: system sqlite3 not found in PATH"
  exit 1
fi

# Detect system sqlite3 version for feature gating
SQLITE_VERSION=$("$SQLITE3" :memory: "SELECT sqlite_version();" 2>/dev/null)
SQLITE_MAJOR=$(echo "$SQLITE_VERSION" | cut -d. -f1)
SQLITE_MINOR=$(echo "$SQLITE_VERSION" | cut -d. -f2)

echo "=== DoltLite SQLite Parity Tests ==="
echo "DoltLite:       $DOLTLITE"
echo "System sqlite3: $SQLITE3 (version $SQLITE_VERSION)"
echo ""

# Feature flags based on sqlite3 version
HAS_WINDOW=0   # window functions: 3.25+
HAS_JSON=0     # json functions: 3.38+ (built-in), or 3.9+ (extension)
HAS_UPSERT=0   # upsert: 3.24+
HAS_CTE=0      # CTEs: 3.8.3+

if [ "$SQLITE_MAJOR" -gt 3 ] || { [ "$SQLITE_MAJOR" -eq 3 ] && [ "$SQLITE_MINOR" -ge 25 ]; }; then
  HAS_WINDOW=1
fi
if [ "$SQLITE_MAJOR" -gt 3 ] || { [ "$SQLITE_MAJOR" -eq 3 ] && [ "$SQLITE_MINOR" -ge 9 ]; }; then
  # json may be available as extension from 3.9; test it
  if echo "SELECT json_array(1,2,3);" | "$SQLITE3" :memory: >/dev/null 2>&1; then
    HAS_JSON=1
  fi
fi
if [ "$SQLITE_MAJOR" -gt 3 ] || { [ "$SQLITE_MAJOR" -eq 3 ] && [ "$SQLITE_MINOR" -ge 9 ]; }; then
  HAS_CTE=1
fi

# ---------------------------------------------------------------
# Test runner: compare output of identical SQL on both engines
# ---------------------------------------------------------------
run_parity() {
  local name="$1"
  local sql="$2"

  local out_dl out_sq
  out_dl=$(echo "$sql" | perl -e 'alarm(10);exec @ARGV' "$DOLTLITE" :memory: 2>&1)
  out_sq=$(echo "$sql" | perl -e 'alarm(10);exec @ARGV' "$SQLITE3" :memory: 2>&1)

  if [ "$out_dl" = "$out_sq" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $name\n  --- doltlite ---\n$out_dl\n  --- sqlite3 ---\n$out_sq\n"
  fi
}

skip_test() {
  local name="$1"
  local reason="$2"
  SKIP=$((SKIP+1))
  echo "  SKIP: $name ($reason)"
}

# ================================================================
# 1. Basic CRUD
# ================================================================
echo "--- Basic CRUD ---"

run_parity "insert_select" "
CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, val REAL);
INSERT INTO t VALUES(1,'alice',10.5);
INSERT INTO t VALUES(2,'bob',20.0);
INSERT INTO t VALUES(3,'charlie',30.75);
SELECT * FROM t ORDER BY id;
"

run_parity "update" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO t VALUES(2,'b');
INSERT INTO t VALUES(3,'c');
UPDATE t SET v='updated' WHERE id=2;
SELECT * FROM t ORDER BY id;
"

run_parity "delete" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO t VALUES(2,'b');
INSERT INTO t VALUES(3,'c');
DELETE FROM t WHERE id=2;
SELECT * FROM t ORDER BY id;
"

run_parity "replace_into" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'original');
REPLACE INTO t VALUES(1,'replaced');
SELECT * FROM t;
"

run_parity "insert_or_ignore" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'first');
INSERT OR IGNORE INTO t VALUES(1,'second');
SELECT * FROM t;
"

# ================================================================
# 2. Aggregate functions
# ================================================================
echo "--- Aggregates ---"

run_parity "count" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INTEGER);
INSERT INTO t VALUES(1,'a',10);
INSERT INTO t VALUES(2,'a',20);
INSERT INTO t VALUES(3,'b',30);
INSERT INTO t VALUES(4,'b',40);
INSERT INTO t VALUES(5,'b',50);
SELECT COUNT(*) FROM t;
"

run_parity "sum_avg" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER);
INSERT INTO t VALUES(1,10);
INSERT INTO t VALUES(2,20);
INSERT INTO t VALUES(3,30);
SELECT SUM(val), AVG(val) FROM t;
"

run_parity "min_max" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER);
INSERT INTO t VALUES(1,10);
INSERT INTO t VALUES(2,50);
INSERT INTO t VALUES(3,30);
SELECT MIN(val), MAX(val) FROM t;
"

run_parity "group_by" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INTEGER);
INSERT INTO t VALUES(1,'a',10);
INSERT INTO t VALUES(2,'a',20);
INSERT INTO t VALUES(3,'b',30);
INSERT INTO t VALUES(4,'b',40);
SELECT grp, COUNT(*), SUM(val) FROM t GROUP BY grp ORDER BY grp;
"

run_parity "group_by_having" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INTEGER);
INSERT INTO t VALUES(1,'a',10);
INSERT INTO t VALUES(2,'a',20);
INSERT INTO t VALUES(3,'b',30);
INSERT INTO t VALUES(4,'b',40);
INSERT INTO t VALUES(5,'b',50);
SELECT grp, SUM(val) FROM t GROUP BY grp HAVING SUM(val) > 50 ORDER BY grp;
"

run_parity "count_distinct" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO t VALUES(2,'a');
INSERT INTO t VALUES(3,'b');
INSERT INTO t VALUES(4,'c');
SELECT COUNT(DISTINCT v) FROM t;
"

run_parity "group_concat" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO t VALUES(2,'b');
INSERT INTO t VALUES(3,'c');
SELECT GROUP_CONCAT(v, ',') FROM t ORDER BY id;
"

run_parity "total" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER);
INSERT INTO t VALUES(1,10);
INSERT INTO t VALUES(2,20);
SELECT TOTAL(val) FROM t;
"

# ================================================================
# 3. JOIN variants
# ================================================================
echo "--- JOINs ---"

SETUP_JOIN="
CREATE TABLE a(id INTEGER PRIMARY KEY, val TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, a_id INTEGER, info TEXT);
INSERT INTO a VALUES(1,'x');
INSERT INTO a VALUES(2,'y');
INSERT INTO a VALUES(3,'z');
INSERT INTO b VALUES(1,1,'p');
INSERT INTO b VALUES(2,1,'q');
INSERT INTO b VALUES(3,2,'r');
INSERT INTO b VALUES(4,99,'s');
"

run_parity "inner_join" "$SETUP_JOIN
SELECT a.val, b.info FROM a INNER JOIN b ON a.id=b.a_id ORDER BY a.val, b.info;
"

run_parity "left_join" "$SETUP_JOIN
SELECT a.val, b.info FROM a LEFT JOIN b ON a.id=b.a_id ORDER BY a.val, b.info;
"

run_parity "cross_join" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, w TEXT);
INSERT INTO a VALUES(1,'a');
INSERT INTO a VALUES(2,'b');
INSERT INTO b VALUES(1,'x');
INSERT INTO b VALUES(2,'y');
SELECT a.v, b.w FROM a CROSS JOIN b ORDER BY a.v, b.w;
"

run_parity "self_join" "
CREATE TABLE t(id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT);
INSERT INTO t VALUES(1,NULL,'root');
INSERT INTO t VALUES(2,1,'child1');
INSERT INTO t VALUES(3,1,'child2');
SELECT c.name, p.name FROM t c LEFT JOIN t p ON c.parent_id=p.id ORDER BY c.id;
"

run_parity "multi_join" "
CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b(id INTEGER PRIMARY KEY, a_id INTEGER);
CREATE TABLE c(id INTEGER PRIMARY KEY, b_id INTEGER, val INTEGER);
INSERT INTO a VALUES(1,'x');
INSERT INTO b VALUES(1,1);
INSERT INTO c VALUES(1,1,42);
SELECT a.v, c.val FROM a JOIN b ON a.id=b.a_id JOIN c ON b.id=c.b_id;
"

# ================================================================
# 4. Subqueries
# ================================================================
echo "--- Subqueries ---"

run_parity "scalar_subquery" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER);
INSERT INTO t VALUES(1,10);
INSERT INTO t VALUES(2,20);
INSERT INTO t VALUES(3,30);
SELECT id, val, (SELECT MAX(val) FROM t) AS mx FROM t ORDER BY id;
"

run_parity "in_subquery" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT);
CREATE TABLE allowed(grp TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO t VALUES(2,'b');
INSERT INTO t VALUES(3,'c');
INSERT INTO allowed VALUES('a');
INSERT INTO allowed VALUES('c');
SELECT * FROM t WHERE grp IN (SELECT grp FROM allowed) ORDER BY id;
"

run_parity "exists_subquery" "
CREATE TABLE orders(id INTEGER PRIMARY KEY, cust_id INTEGER);
CREATE TABLE customers(id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO customers VALUES(1,'alice');
INSERT INTO customers VALUES(2,'bob');
INSERT INTO orders VALUES(1,1);
SELECT name FROM customers WHERE EXISTS (SELECT 1 FROM orders WHERE orders.cust_id=customers.id) ORDER BY name;
"

run_parity "not_in_subquery" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE exclude(v TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO t VALUES(2,'b');
INSERT INTO t VALUES(3,'c');
INSERT INTO exclude VALUES('b');
SELECT * FROM t WHERE v NOT IN (SELECT v FROM exclude) ORDER BY id;
"

run_parity "correlated_subquery" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INTEGER);
INSERT INTO t VALUES(1,'a',10);
INSERT INTO t VALUES(2,'a',20);
INSERT INTO t VALUES(3,'b',30);
SELECT id, val FROM t t1 WHERE val = (SELECT MAX(val) FROM t t2 WHERE t2.grp=t1.grp) ORDER BY id;
"

# ================================================================
# 5. Window functions
# ================================================================
echo "--- Window functions ---"

if [ "$HAS_WINDOW" -eq 1 ]; then
  run_parity "row_number" "
CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, val INTEGER);
INSERT INTO t VALUES(1,'a',10);
INSERT INTO t VALUES(2,'a',20);
INSERT INTO t VALUES(3,'b',30);
INSERT INTO t VALUES(4,'b',40);
SELECT id, grp, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) AS rn FROM t ORDER BY id;
"

  run_parity "rank_dense_rank" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER);
INSERT INTO t VALUES(1,10);
INSERT INTO t VALUES(2,20);
INSERT INTO t VALUES(3,20);
INSERT INTO t VALUES(4,30);
SELECT id, val, RANK() OVER (ORDER BY val) AS rnk, DENSE_RANK() OVER (ORDER BY val) AS drnk FROM t ORDER BY id;
"

  run_parity "lead_lag" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER);
INSERT INTO t VALUES(1,10);
INSERT INTO t VALUES(2,20);
INSERT INTO t VALUES(3,30);
INSERT INTO t VALUES(4,40);
SELECT id, val, LAG(val,1) OVER (ORDER BY id) AS prev, LEAD(val,1) OVER (ORDER BY id) AS next FROM t ORDER BY id;
"

  run_parity "sum_over" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER);
INSERT INTO t VALUES(1,10);
INSERT INTO t VALUES(2,20);
INSERT INTO t VALUES(3,30);
SELECT id, val, SUM(val) OVER (ORDER BY id) AS running FROM t ORDER BY id;
"

  run_parity "ntile" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER);
INSERT INTO t VALUES(1,10);
INSERT INTO t VALUES(2,20);
INSERT INTO t VALUES(3,30);
INSERT INTO t VALUES(4,40);
SELECT id, NTILE(2) OVER (ORDER BY id) AS tile FROM t ORDER BY id;
"
else
  skip_test "window_functions" "sqlite3 $SQLITE_VERSION < 3.25"
fi

# ================================================================
# 6. NULL handling
# ================================================================
echo "--- NULL handling ---"

run_parity "is_null" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO t VALUES(2,NULL);
INSERT INTO t VALUES(3,'c');
SELECT * FROM t WHERE v IS NULL;
"

run_parity "is_not_null" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO t VALUES(2,NULL);
INSERT INTO t VALUES(3,'c');
SELECT * FROM t WHERE v IS NOT NULL ORDER BY id;
"

run_parity "coalesce" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT);
INSERT INTO t VALUES(1,NULL,NULL,'fallback');
INSERT INTO t VALUES(2,NULL,'second',NULL);
INSERT INTO t VALUES(3,'first',NULL,NULL);
SELECT id, COALESCE(a,b,c) FROM t ORDER BY id;
"

run_parity "nullif" "
SELECT NULLIF(1,1), NULLIF(1,2), NULLIF('a','b');
"

run_parity "null_in_aggregate" "
CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER);
INSERT INTO t VALUES(1,10);
INSERT INTO t VALUES(2,NULL);
INSERT INTO t VALUES(3,30);
SELECT COUNT(*), COUNT(val), SUM(val), AVG(val) FROM t;
"

run_parity "null_comparison" "
SELECT NULL = NULL, NULL != NULL, NULL > 1, NULL IS NULL, NULL IS NOT NULL;
"

run_parity "ifnull" "
SELECT IFNULL(NULL, 'default'), IFNULL('value', 'default');
"

# ================================================================
# 7. Type coercion and affinity
# ================================================================
echo "--- Type coercion / affinity ---"

run_parity "typeof" "
SELECT typeof(1), typeof(1.5), typeof('hello'), typeof(NULL), typeof(X'00');
"

run_parity "integer_affinity" "
CREATE TABLE t(v INTEGER);
INSERT INTO t VALUES('123');
INSERT INTO t VALUES(456);
INSERT INTO t VALUES(7.0);
SELECT typeof(v), v FROM t ORDER BY rowid;
"

run_parity "text_affinity" "
CREATE TABLE t(v TEXT);
INSERT INTO t VALUES(123);
INSERT INTO t VALUES(4.56);
INSERT INTO t VALUES('hello');
SELECT typeof(v), v FROM t ORDER BY rowid;
"

run_parity "numeric_compare" "
SELECT 1 < 2, 1.0 = 1, '10' > '9', 10 > 9;
"

run_parity "real_precision" "
SELECT round(1.0/3.0, 12);
"

# ================================================================
# 8. ORDER BY with COLLATE
# ================================================================
echo "--- ORDER BY / COLLATE ---"

run_parity "order_asc_desc" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'b');
INSERT INTO t VALUES(2,'a');
INSERT INTO t VALUES(3,'c');
SELECT v FROM t ORDER BY v ASC;
SELECT v FROM t ORDER BY v DESC;
"

run_parity "collate_nocase" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'Apple');
INSERT INTO t VALUES(2,'banana');
INSERT INTO t VALUES(3,'cherry');
INSERT INTO t VALUES(4,'BANANA');
SELECT v FROM t ORDER BY v COLLATE NOCASE;
"

run_parity "order_multi_col" "
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT);
INSERT INTO t VALUES(1,1,'z');
INSERT INTO t VALUES(2,2,'a');
INSERT INTO t VALUES(3,1,'a');
INSERT INTO t VALUES(4,2,'z');
SELECT * FROM t ORDER BY a ASC, b DESC;
"

run_parity "order_nulls" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,30);
INSERT INTO t VALUES(2,NULL);
INSERT INTO t VALUES(3,10);
INSERT INTO t VALUES(4,NULL);
INSERT INTO t VALUES(5,20);
SELECT v FROM t ORDER BY v;
"

# ================================================================
# 9. LIMIT / OFFSET
# ================================================================
echo "--- LIMIT / OFFSET ---"

run_parity "limit" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
INSERT INTO t VALUES(2);
INSERT INTO t VALUES(3);
INSERT INTO t VALUES(4);
INSERT INTO t VALUES(5);
SELECT * FROM t ORDER BY id LIMIT 3;
"

run_parity "limit_offset" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
INSERT INTO t VALUES(2);
INSERT INTO t VALUES(3);
INSERT INTO t VALUES(4);
INSERT INTO t VALUES(5);
SELECT * FROM t ORDER BY id LIMIT 2 OFFSET 2;
"

run_parity "limit_zero" "
CREATE TABLE t(id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(1);
SELECT * FROM t LIMIT 0;
"

# ================================================================
# 10. CASE expressions
# ================================================================
echo "--- CASE expressions ---"

run_parity "case_simple" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,1);
INSERT INTO t VALUES(2,2);
INSERT INTO t VALUES(3,3);
SELECT id, CASE v WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM t ORDER BY id;
"

run_parity "case_searched" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,5);
INSERT INTO t VALUES(2,15);
INSERT INTO t VALUES(3,25);
SELECT id, CASE WHEN v<10 THEN 'low' WHEN v<20 THEN 'mid' ELSE 'high' END FROM t ORDER BY id;
"

run_parity "case_null" "
SELECT CASE NULL WHEN NULL THEN 'match' ELSE 'no match' END;
SELECT CASE WHEN NULL THEN 'true' ELSE 'false' END;
"

# ================================================================
# 11. Date/time functions
# ================================================================
echo "--- Date/time functions ---"

run_parity "date_func" "
SELECT date('2024-03-15');
"

run_parity "time_func" "
SELECT time('14:30:00');
"

run_parity "datetime_func" "
SELECT datetime('2024-03-15 14:30:00');
"

run_parity "julianday" "
SELECT typeof(julianday('2024-03-15'));
"

run_parity "date_modifiers" "
SELECT date('2024-03-15', '+1 day');
SELECT date('2024-03-15', '-1 month');
SELECT date('2024-03-15', 'start of month');
SELECT date('2024-03-15', 'start of year');
"

run_parity "strftime" "
SELECT strftime('%Y', '2024-03-15');
SELECT strftime('%m', '2024-03-15');
SELECT strftime('%d', '2024-03-15');
SELECT strftime('%H:%M', '2024-03-15 14:30:00');
"

run_parity "date_arithmetic" "
SELECT julianday('2024-03-15') - julianday('2024-03-01');
"

# ================================================================
# 12. JSON functions
# ================================================================
echo "--- JSON functions ---"

if [ "$HAS_JSON" -eq 1 ]; then
  run_parity "json_array" "
SELECT json_array(1,2,'three',NULL);
"

  run_parity "json_object" "
SELECT json_object('a',1,'b','two');
"

  run_parity "json_extract" "
SELECT json_extract('{\"a\":1,\"b\":[2,3]}', '\$.a');
SELECT json_extract('{\"a\":1,\"b\":[2,3]}', '\$.b[0]');
"

  run_parity "json_type" "
SELECT json_type('{\"a\":1}', '\$.a');
SELECT json_type('{\"a\":\"hello\"}', '\$.a');
"

  run_parity "json_valid" "
SELECT json_valid('{\"a\":1}');
SELECT json_valid('not json');
"

  run_parity "json_group_array" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO t VALUES(2,'b');
INSERT INTO t VALUES(3,'c');
SELECT json_group_array(v) FROM t ORDER BY id;
"

  run_parity "json_group_object" "
CREATE TABLE t(id INTEGER PRIMARY KEY, k TEXT, v INTEGER);
INSERT INTO t VALUES(1,'x',10);
INSERT INTO t VALUES(2,'y',20);
SELECT json_group_object(k,v) FROM t ORDER BY id;
"
else
  skip_test "json_functions" "sqlite3 $SQLITE_VERSION lacks JSON support"
fi

# ================================================================
# 13. UNION, INTERSECT, EXCEPT
# ================================================================
echo "--- Set operations ---"

run_parity "union" "
CREATE TABLE a(v INTEGER);
CREATE TABLE b(v INTEGER);
INSERT INTO a VALUES(1);
INSERT INTO a VALUES(2);
INSERT INTO a VALUES(3);
INSERT INTO b VALUES(2);
INSERT INTO b VALUES(3);
INSERT INTO b VALUES(4);
SELECT v FROM a UNION SELECT v FROM b ORDER BY v;
"

run_parity "union_all" "
CREATE TABLE a(v INTEGER);
CREATE TABLE b(v INTEGER);
INSERT INTO a VALUES(1);
INSERT INTO a VALUES(2);
INSERT INTO b VALUES(2);
INSERT INTO b VALUES(3);
SELECT v FROM a UNION ALL SELECT v FROM b ORDER BY v;
"

run_parity "intersect" "
CREATE TABLE a(v INTEGER);
CREATE TABLE b(v INTEGER);
INSERT INTO a VALUES(1);
INSERT INTO a VALUES(2);
INSERT INTO a VALUES(3);
INSERT INTO b VALUES(2);
INSERT INTO b VALUES(3);
INSERT INTO b VALUES(4);
SELECT v FROM a INTERSECT SELECT v FROM b ORDER BY v;
"

run_parity "except" "
CREATE TABLE a(v INTEGER);
CREATE TABLE b(v INTEGER);
INSERT INTO a VALUES(1);
INSERT INTO a VALUES(2);
INSERT INTO a VALUES(3);
INSERT INTO b VALUES(2);
INSERT INTO b VALUES(3);
INSERT INTO b VALUES(4);
SELECT v FROM a EXCEPT SELECT v FROM b ORDER BY v;
"

# ================================================================
# 14. CTEs (WITH ... AS)
# ================================================================
echo "--- CTEs ---"

if [ "$HAS_CTE" -eq 1 ]; then
  run_parity "simple_cte" "
WITH nums AS (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3)
SELECT * FROM nums ORDER BY n;
"

  run_parity "recursive_cte" "
WITH RECURSIVE cnt(x) AS (
  SELECT 1
  UNION ALL
  SELECT x+1 FROM cnt WHERE x<5
)
SELECT x FROM cnt;
"

  run_parity "cte_with_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT);
INSERT INTO t VALUES(1,NULL,'root');
INSERT INTO t VALUES(2,1,'a');
INSERT INTO t VALUES(3,1,'b');
INSERT INTO t VALUES(4,2,'c');
WITH RECURSIVE tree(id, name, depth) AS (
  SELECT id, name, 0 FROM t WHERE parent_id IS NULL
  UNION ALL
  SELECT t.id, t.name, tree.depth+1 FROM t JOIN tree ON t.parent_id=tree.id
)
SELECT id, name, depth FROM tree ORDER BY id;
"

  run_parity "multiple_ctes" "
WITH
  a AS (SELECT 1 AS x UNION ALL SELECT 2),
  b AS (SELECT x*10 AS y FROM a)
SELECT * FROM b ORDER BY y;
"
else
  skip_test "cte" "sqlite3 $SQLITE_VERSION < 3.9"
fi

# ================================================================
# 15. CAST expressions
# ================================================================
echo "--- CAST ---"

run_parity "cast_text_to_int" "
SELECT CAST('123' AS INTEGER);
"

run_parity "cast_int_to_text" "
SELECT CAST(456 AS TEXT);
"

run_parity "cast_real_to_int" "
SELECT CAST(3.7 AS INTEGER);
"

run_parity "cast_int_to_real" "
SELECT CAST(5 AS REAL);
"

run_parity "cast_text_to_real" "
SELECT CAST('3.14' AS REAL);
"

run_parity "cast_null" "
SELECT CAST(NULL AS INTEGER), CAST(NULL AS TEXT), CAST(NULL AS REAL);
"

run_parity "cast_blob" "
SELECT typeof(CAST('hello' AS BLOB));
"

# ================================================================
# Additional: string functions
# ================================================================
echo "--- String functions ---"

run_parity "length" "
SELECT length('hello'), length(''), length(NULL);
"

run_parity "upper_lower" "
SELECT upper('hello'), lower('WORLD');
"

run_parity "substr" "
SELECT substr('hello world', 1, 5);
SELECT substr('hello world', 7);
"

run_parity "trim" "
SELECT trim('  hello  '), ltrim('  hello'), rtrim('hello  ');
"

run_parity "replace_func" "
SELECT replace('hello world', 'world', 'there');
"

run_parity "instr" "
SELECT instr('hello world', 'world');
SELECT instr('hello world', 'xyz');
"

run_parity "like_glob" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'hello');
INSERT INTO t VALUES(2,'world');
INSERT INTO t VALUES(3,'help');
SELECT v FROM t WHERE v LIKE 'hel%' ORDER BY v;
SELECT v FROM t WHERE v GLOB 'hel*' ORDER BY v;
"

run_parity "hex_unhex" "
SELECT hex('ABC');
SELECT typeof(unhex('414243'));
"

run_parity "zeroblob" "
SELECT typeof(zeroblob(4)), length(zeroblob(4));
"

# ================================================================
# Additional: math/numeric functions
# ================================================================
echo "--- Math / numeric ---"

run_parity "abs" "
SELECT abs(-5), abs(5), abs(0), abs(NULL);
"

run_parity "max_min_scalar" "
SELECT max(1,2,3), min(1,2,3);
"

run_parity "random_typeof" "
SELECT typeof(random());
"

run_parity "round" "
SELECT round(3.14159, 2), round(3.5), round(-2.5);
"

# ================================================================
# Additional: misc
# ================================================================
echo "--- Miscellaneous ---"

run_parity "between" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t VALUES(1,5);
INSERT INTO t VALUES(2,10);
INSERT INTO t VALUES(3,15);
INSERT INTO t VALUES(4,20);
SELECT * FROM t WHERE v BETWEEN 10 AND 20 ORDER BY id;
"

run_parity "in_list" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO t VALUES(2,'b');
INSERT INTO t VALUES(3,'c');
INSERT INTO t VALUES(4,'d');
SELECT * FROM t WHERE v IN ('a','c','e') ORDER BY id;
"

run_parity "distinct" "
CREATE TABLE t(v TEXT);
INSERT INTO t VALUES('a');
INSERT INTO t VALUES('b');
INSERT INTO t VALUES('a');
INSERT INTO t VALUES('c');
INSERT INTO t VALUES('b');
SELECT DISTINCT v FROM t ORDER BY v;
"

run_parity "autoincrement" "
CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);
INSERT INTO t(v) VALUES('a');
INSERT INTO t(v) VALUES('b');
INSERT INTO t(v) VALUES('c');
SELECT * FROM t ORDER BY id;
"

run_parity "last_insert_rowid" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
SELECT last_insert_rowid();
"

run_parity "changes_func" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES(1,'a');
INSERT INTO t VALUES(2,'b');
INSERT INTO t VALUES(3,'c');
DELETE FROM t WHERE id > 1;
SELECT changes();
"

run_parity "empty_table" "
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
SELECT * FROM t;
SELECT COUNT(*) FROM t;
"

run_parity "multiple_tables_insert" "
CREATE TABLE t1(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE t2(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t1 VALUES(1,'a');
INSERT INTO t2 VALUES(1,'b');
SELECT t1.v, t2.v FROM t1, t2 WHERE t1.id = t2.id;
"

run_parity "insert_select_from" "
CREATE TABLE src(id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE dst(id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO src VALUES(1,'a');
INSERT INTO src VALUES(2,'b');
INSERT INTO dst SELECT * FROM src;
SELECT * FROM dst ORDER BY id;
"

run_parity "subquery_in_from" "
SELECT * FROM (SELECT 1 AS a, 2 AS b UNION ALL SELECT 3, 4) sub ORDER BY a;
"

run_parity "coalesce_chain" "
SELECT COALESCE(NULL, NULL, NULL, 'found');
SELECT COALESCE('first', NULL, 'third');
"

run_parity "nested_functions" "
SELECT upper(substr(replace('hello world','world','there'),1,5));
"

run_parity "expression_index_compat" "
SELECT 1+2, 3*4, 10/3, 10%3, -5;
"

run_parity "boolean_expressions" "
SELECT 1 AND 1, 1 AND 0, 0 OR 1, 0 OR 0, NOT 1, NOT 0;
"

run_parity "concatenation" "
SELECT 'hello' || ' ' || 'world';
"

# ================================================================
# Summary
# ================================================================
echo ""
echo "======================================="
echo "Results: $PASS passed, $FAIL failed, $SKIP skipped out of $((PASS+FAIL+SKIP)) tests"
echo "======================================="
if [ $FAIL -gt 0 ]; then
  echo ""
  echo "FAILURES:"
  echo -e "$ERRORS"
  exit 1
fi
