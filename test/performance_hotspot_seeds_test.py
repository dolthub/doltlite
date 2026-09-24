#!/usr/bin/env python3

import json
from pathlib import Path
import sqlite3
import unittest
from unittest.mock import patch

import performance_hotspot_fuzzer as fuzzer
import performance_hotspot_search as search
import performance_hotspot_seeds as seeds
import performance_hotspots as hotspots


class SeedTests(unittest.TestCase):
    def test_large_workloads_keep_their_sizes_and_cache_regimes(self):
        specs = list(seeds.large_specs())
        self.assertEqual([p.rows for p, _, _ in specs], [262144, 262144, 1048576])
        self.assertEqual([p.cache_kib for p, _, _ in specs], [65536, 65536, 131072])
        self.assertEqual([c.name for _, cases, _ in specs for c in cases],
                         ['scan_first', 'scan_repeat', 'point_10000', 'scan_after_points',
                          'index_scan_row_fetch', 'index_edit_update'])
        self.assertIn('shrink_memory', specs[0][1][0].prepare)
        self.assertIn('SELECT sum', specs[0][1][1].prepare)
        self.assertIn('WITH RECURSIVE', specs[0][1][3].prepare)

    def test_seed_sql_results_plans_and_rollback(self):
        for p, cases, setup in seeds.large_specs(rows=64, edit_rows=128):
            with sqlite3.connect(':memory:') as db:
                db.executescript(setup)
                original = list(db.iterdump())
                for case in cases:
                    with self.subTest(case=case.name):
                        plan = '\n'.join(row[3] for row in db.execute('EXPLAIN QUERY PLAN '+case.sql))
                        if case.name == 'index_scan_row_fetch':
                            self.assertIn('SEARCH orders USING INDEX orders_customer', plan)
                        for _ in range(2):
                            for query in case.warmup.split(';'):
                                if query.strip():
                                    db.execute(query).fetchall()
                            db.execute('BEGIN')
                            for query in case.prepare.split(';'):
                                if query.strip():
                                    db.execute(query).fetchall()
                            result = db.execute(case.sql).fetchall()
                            if case.verify:
                                result = db.execute(case.verify).fetchall()
                            if case.name == 'index_edit_update':
                                self.assertEqual(result, [(64, sum((i*7919)%1000 for i in range(1,129))+64)])
                            elif case.name == 'index_scan_row_fetch':
                                expected = [list(db.execute(q).fetchone()) for q in hotspots.index_queries(64)]
                                self.assertEqual([json.loads(row[0]) for row in result], expected)
                            else:
                                expected = dict((n, e) for n, _, e in hotspots.workloads(64))[case.name]
                                self.assertEqual('|'.join(map(str, result[0])), expected)
                            db.rollback()
                            self.assertEqual(list(db.iterdump()), original)

    def test_index_cache_warmup_precedes_transaction_and_timer(self):
        profile, cases, setup = list(seeds.large_specs(rows=64, edit_rows=128))[-1]
        case = cases[0]
        script = fuzzer.unit_sql(case, True)
        self.assertLess(script.index(case.warmup), script.index('BEGIN;'))
        self.assertLess(script.index('BEGIN;'), script.index('.timer on'))
        self.assertLess(script.index('.timer off'), script.index(case.verify))
        self.assertTrue(script.endswith('ROLLBACK;\n'))
        other = fuzzer.Case(case.name, case.sql, case.verify, prepare=case.warmup)
        self.assertNotEqual(search.family_fingerprint(profile, case, {}),
                            search.family_fingerprint(profile, other, {}))

    def test_retired_corpus_moves_to_search_and_random_exploration_continues(self):
        specs = list(seeds.specs())
        self.assertEqual(len(specs), 13)
        self.assertEqual(sum(len(cases) for _, cases, _ in specs), 31)
        names = {path.stem for path in seeds.SEED_DIR.glob('*.json')}
        wide_cases = {'create_index', 'delete_batch', 'distinct', 'index_fetch',
                      'join_pk', 'point_payload', 'point_pk', 'range_pk',
                      'reverse_scan', 'scan_payload', 'sort_limit', 'update_batch'}
        narrow_cases = {'index_fetch', 'join_pk', 'point_payload', 'point_pk', 'reverse_scan'}
        self.assertEqual(names, {'wide_rows_'+name for name in wide_cases}
                               | {'narrow_rows_'+name for name in narrow_cases}
                               | {'zero_row_updates_after_delete_text_pk',
                                  'zero_row_updates_after_delete_integer_pk',
                                  'update_text_after_delete_integer_pk_file',
                                  'range_join_union_after_update_text_pk_memory',
                                  'update_text_after_delete_text_pk_memory',
                                  'scan_after_update_wide_text_pk',
                                  'issue_3249', 'issue_3250'})
        wide = [(p, cases, setup) for p, cases, setup in specs if p.payload==16384]
        self.assertEqual(len(wide), 1)
        profile, cases, setup = wide[0]
        self.assertEqual({case.name for case in cases}, wide_cases)
        self.assertEqual((profile.rows, profile.cache_kib), (16384, 16384))
        for name in names:
            bundle = json.loads((seeds.SEED_DIR/(name+'.json')).read_text())
            matches = [(p, case, sql) for p, cases, sql in specs for case in cases
                       if p == fuzzer.Profile(**bundle['profile'])
                       and case == fuzzer.Case(**bundle['case'])
                       and sql == bundle['setup_sql']]
            self.assertEqual(len(matches), 1, name)
        active = hotspots.TEST_DIR/'performance-hotspot-corpus'
        self.assertTrue(all(not (active/(name+'.json')).exists() for name in names))
        with patch.object(seeds, 'specs', return_value=iter(specs)):
            generated = search.Search(123).specs(123, nightly_seeds=True,
                                               operators=['update_blob'])
            for index, spec in enumerate(specs):
                self.assertEqual(next(generated), (index, *spec, 'retired'))
            index, profile, cases, setup, origin = next(generated)
            self.assertEqual(index, 13)
            self.assertEqual(len(cases), 4)
            self.assertEqual({case.recipe['operator'] for case in cases}, {'update_blob'})
            self.assertIn('CREATE TABLE u', setup)
            self.assertNotEqual(origin, 'retired')

    def test_multirow_results_validate_every_row_and_timer(self):
        unit = '[1,2]\n[3,4]\nRun Time: real 0.01 user 0.01 sys 0.0\n'
        output = 'WARM\n[1,2]\n[3,4]\nMEASURE\n'+unit*2+'END\n'
        self.assertEqual(fuzzer.parse_measurement(output, 2), {'ms': 20, 'result': '[1,2]\n[3,4]'})
        for bad in (output.replace('MEASURE\n[1,2]', 'MEASURE\n[1,3]'),
                    output.replace('MEASURE\n[1,2]\n[3,4]', 'MEASURE\n[3,4]\n[1,2]'),
                    output.replace('MEASURE\n[1,2]\n', 'MEASURE\n'),
                    output.replace('Run Time: real 0.01 user 0.01 sys 0.0\n', '', 1)):
            with self.assertRaises(ValueError):
                fuzzer.parse_measurement(bad, 2)

    def test_nightly_workflow_includes_seeds_within_existing_budget(self):
        workflow = (hotspots.TEST_DIR.parent/'.github/workflows/nightly-hotspots.yml').read_text()
        self.assertIn('--search --nightly-seeds', workflow)
        self.assertIn('--seconds 1800', workflow)
        self.assertIn('performance_hotspot_seeds_test.py', workflow)


if __name__ == '__main__':
    unittest.main()
