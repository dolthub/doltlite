import json
from pathlib import Path

from performance_hotspot_fuzzer import Case, Profile
from performance_hotspots import (INDEX_EDIT_CACHE_KIB, INDEX_EDIT_ROWS, PAYLOAD_BYTES,
                                  index_edit_setup, scan_setup, workloads)


SEED_DIR = Path(__file__).resolve().parent / 'performance-hotspot-seeds'


def large_specs(rows=262144, edit_rows=INDEX_EDIT_ROWS):
    profile = Profile(rows, PAYLOAD_BYTES, 'integer', min(2048, rows), False,
                      65536, 2, 10000, 0, 1, rows//4)
    queries = workloads(rows)
    scan, points = queries[0][1], queries[2][1]
    cold = 'PRAGMA shrink_memory;\n'
    cases = [Case('scan_first', scan, prepare=cold),
             Case('scan_repeat', scan, prepare=scan),
             Case('point_10000', points, prepare=cold+scan),
             Case('scan_after_points', scan, prepare=cold+scan+points)]
    yield profile, cases, scan_setup(rows)

    setup = f"""CREATE TABLE orders(id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL,
      amount_cents INTEGER NOT NULL, description TEXT NOT NULL);
BEGIN;
WITH RECURSIVE c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<{rows})
INSERT INTO orders SELECT i,(i-1)%{profile.groups},i*37%100000,
                          printf('%0{PAYLOAD_BYTES}x',i) FROM c;
COMMIT;
CREATE INDEX orders_customer ON orders(customer_id);
ANALYZE;
"""
    query = f"""WITH RECURSIVE requests(i) AS (
      VALUES(0) UNION ALL SELECT i+1 FROM requests WHERE i<999
    ) SELECT (SELECT json_array(count(*),sum(amount_cents),sum(length(description)),
      sum(unicode(substr(description,1,1))+unicode(substr(description,-1,1))))
      FROM orders WHERE customer_id=requests.i*137%{profile.groups})
      FROM requests ORDER BY i;"""
    yield profile, [Case('index_scan_row_fetch', query)], setup

    profile = Profile(edit_rows, 32, 'integer', 1000, False,
                      INDEX_EDIT_CACHE_KIB, 2, 10000, 0, 1, edit_rows//4)
    case = Case('index_edit_update', 'UPDATE ie SET k=k+1 WHERE id%2=0;',
                'SELECT changes(),sum(k) FROM ie;',
                warmup='SELECT count(*) FROM ie WHERE id>0;\n'
                       'SELECT count(*) FROM ie INDEXED BY ie_k WHERE k>=0;')
    yield profile, [case], index_edit_setup(edit_rows)


def specs():
    yield from large_specs()
    grouped = {}
    for path in sorted(SEED_DIR.glob('*.json')):
        bundle = json.loads(path.read_text())
        key = Profile(**bundle['profile']), bundle['setup_sql']
        grouped.setdefault(key, []).append(Case(**bundle['case']))
    for (profile, setup), cases in grouped.items():
        yield profile, cases, setup
