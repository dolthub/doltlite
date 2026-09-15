#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
import statistics
import subprocess
import tempfile
import time


def run(binary, database, sql):
    result = subprocess.run([str(binary), str(database)], input=sql, text=True,
                            capture_output=True, timeout=120)
    if result.returncode:
        raise RuntimeError(f"{database.name}: exit {result.returncode}: {result.stderr.strip()}")
    return result.stdout.strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('binary', type=Path)
    parser.add_argument('--repeats', type=int, default=3)
    args = parser.parse_args()
    binary = args.binary.resolve()
    if args.repeats < 1:
        parser.error('--repeats must be positive')
    with tempfile.TemporaryDirectory(prefix='doltlite-fk-scale-') as work:
        for storage in ('integer', 'composite', 'unique'):
            for parents, orphans in ((1000, 1000), (4000, 1000), (1000, 4000), (4000, 4000)):
                db = Path(work) / f'{storage}-{parents}-{orphans}.db'
                if storage == 'integer':
                    parent = 'id INTEGER PRIMARY KEY'
                    child = 'id INTEGER PRIMARY KEY, p INTEGER REFERENCES p(id)'
                    parent_values = 'x'
                    child_values = f'x,x+{parents}'
                elif storage == 'composite':
                    parent = 'id INT, k TEXT COLLATE NOCASE, PRIMARY KEY(k,id)'
                    child = 'id INTEGER PRIMARY KEY, p INT, k TEXT, FOREIGN KEY(p,k) REFERENCES p(id,k)'
                    parent_values = "x,'Key'"
                    child_values = f"x,x+{parents},'KEY'"
                else:
                    parent = 'id INTEGER PRIMARY KEY, k TEXT COLLATE NOCASE UNIQUE'
                    child = 'id INTEGER PRIMARY KEY, k TEXT REFERENCES p(k)'
                    parent_values = "x,'Key'||x"
                    child_values = f"x,'KEY'||(x+{parents})"
                run(binary, db, f'''
CREATE TABLE p({parent});
CREATE TABLE c({child});
WITH RECURSIVE s(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM s WHERE x<{parents})
INSERT INTO p SELECT {parent_values} FROM s;
PRAGMA foreign_keys=OFF;
WITH RECURSIVE s(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM s WHERE x<{orphans})
INSERT INTO c SELECT {child_values} FROM s;
PRAGMA foreign_keys=ON;
''')
                samples = []
                for _ in range(args.repeats):
                    start = time.perf_counter()
                    result = run(binary, db, "SELECT dolt_verify_constraints('--all','--output-only');")
                    samples.append(time.perf_counter() - start)
                    if result != '1':
                        raise RuntimeError(f'{db.name}: verification returned {result!r}')
                result = run(binary, db, '''
SELECT count(*) FROM dolt_constraint_violations;
SELECT count(*) FROM pragma_foreign_key_check('c');
SELECT dolt_verify_constraints('--all');
SELECT coalesce(sum(num_violations),0) FROM dolt_constraint_violations;
''')
                if result.splitlines() != ['0', str(orphans), '1', str(orphans)]:
                    raise RuntimeError(f'{db.name}: incorrect violation counts: {result!r}')
                print(json.dumps(dict(storage=storage, parents=parents, orphans=orphans,
                                      median_seconds=statistics.median(samples))), flush=True)


if __name__ == '__main__':
    main()
