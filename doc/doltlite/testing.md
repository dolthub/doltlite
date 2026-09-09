# Running tests

```bash
cd build
../configure && make

# DoltLite shell suites (branch / commit / merge / remotes / …)
bash ../test/run_doltlite_tests.sh

# C unit / multiproc / stress harnesses
bash ../test/run_c_tests.sh

# Upstream SQLite TCL suite (prolly engine) — one CI bucket
bash ../test/run_testfixture.sh "SQLite regression core-sql" 300 \
  $(tr '\n' ' ' < ../test/regression-buckets/core-sql.txt)

# SQL oracles (clean stock build)
bash ../test/build_stock_reference.sh ../build-stock ./doltlite
bash ../test/sql_oracle_test.sh ./doltlite ../build-stock/sqlite3
bash ../test/vc_oracle_workspace_test.sh ./doltlite dolt

# sqllogictest (CI runners + corpus)
bash ../test/run_sqllogictest.sh \
  /path/to/sqllogictest-doltlite /path/to/sqllogictest-stock \
  /path/to/sqllogictest
```

CI wiring, coverage floors, and full bucket lists are in
[`.github/workflows/test.yml`](../../.github/workflows/test.yml) and
[AGENTS.md](../../AGENTS.md). Contract suites
(`sqlite_compatibility_contract_test.sh`, `concurrency_contract_test.sh`,
`storage_format_contract_test.sh`) gate [SQLite compatibility](sqlite-compatibility.md),
[concurrency](concurrency.md), and [storage format](storage-format.md).

Inherited TCL allowlists:
[`test/known_testfixture_divergences.txt`](../../test/known_testfixture_divergences.txt),
[`test/known_testfixture_crashes.txt`](../../test/known_testfixture_crashes.txt).
Both carry a `class=` disposition per gate; the totals are pinned by
[`test/known_testfixture_exception_ratchet.txt`](../../test/known_testfixture_exception_ratchet.txt).
