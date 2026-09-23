#!/usr/bin/env python3

from dataclasses import asdict, replace
import json
from pathlib import Path
import random
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import performance_hotspot_fuzzer as fuzzer
import performance_hotspot_issues as issues
import performance_hotspot_search as search
import performance_hotspots as retained


class SearchTests(unittest.TestCase):
    def setUp(self):
        self.profile = fuzzer.Profile(64, 32, 'integer', 16, False, 4096, 8, 10, 3, 12, 8)
        self.recipe = search.fresh(random.Random(12))

    def test_composed_sql_results_and_transaction_rollback(self):
        rng = random.Random(23)
        seen = {key: set() for key in search.CHOICES}
        for key in ('integer', 'text'):
            for operator in search.CHOICES['operator']:
                for context in search.CHOICES['context']:
                    recipe = search.fresh(rng)
                    recipe.update(operator=operator, context=context)
                    for k, v in recipe.items():
                        seen[k].add(v)
                    p = replace(self.profile, key=key, skew=bool(rng.randrange(2)))
                    case = search.generated_case(p, recipe)
                    with self.subTest(recipe=recipe, key=key), sqlite3.connect(':memory:') as db:
                        db.executescript(search.setup_sql(p, recipe))
                        original = list(db.iterdump())
                        outputs = []
                        for _ in range(2):
                            db.execute('BEGIN')
                            if case.prepare:
                                db.execute(case.prepare).fetchall()
                            result = db.execute(case.sql).fetchall()
                            if case.verify:
                                result = db.execute(case.verify).fetchall()
                            outputs.append(result)
                            self.assertEqual(len(result), 1)
                            db.rollback()
                            self.assertEqual(list(db.iterdump()), original)
                        self.assertEqual(outputs[0], outputs[1])
                        session = fuzzer.session_sql(p, case, 2)
                        self.assertNotIn('COMMIT', session)
                        if context != 'plain' or case.verify:
                            self.assertIn('BEGIN;', session)
                            self.assertIn('ROLLBACK;', session)
        for key, values in search.CHOICES.items():
            self.assertEqual(seen[key], set(values), key)

    def test_determinism_and_all_exploration_modes(self):
        a, b = search.Search(123), search.Search(123)
        a.remember(self.recipe, asdict(self.profile))
        b.remember(self.recipe, asdict(self.profile))
        modes, recipes = set(), set()
        for _ in range(100):
            choice = a.choose(self.profile)
            self.assertEqual(choice, b.choose(self.profile))
            modes.add(choice[2])
            recipes.add(search.digest(choice[1]))
        self.assertEqual(modes, {'fresh', 'mutation', 'underexplored'})
        self.assertGreater(len(recipes), 50)

    def test_secondary_ranges_and_index_order_results(self):
        for key in ('integer', 'text'):
            for width in (1, 32, 64):
                p = replace(self.profile, key=key, width=width, start=1)
                recipe = dict(self.recipe, source='table', operator='index_order',
                              predicate='group_range', expression='seq', context='plain',
                              indexes='both', direction='ASC')
                with self.subTest(key=key, width=width), sqlite3.connect(':memory:') as db:
                    db.executescript(search.setup_sql(p, recipe))
                    rows = db.execute('SELECT seq,grp,v,id FROM t').fetchall()
                    selected = sorted((row for row in rows if row[1] < max(1, p.groups*width//p.rows)),
                                      key=lambda row: (row[1], row[2], row[3]))[:width]
                    self.assertEqual(db.execute(search.generated_case(p, recipe).sql).fetchone(),
                                     (len(selected), sum(row[0] for row in selected)))

    def test_operator_filter_applies_to_mutations_and_companions(self):
        operators = ['update_pk', 'upsert_update', 'correlated_limit']
        a, b = search.Search(123), search.Search(123)
        parent = dict(self.recipe, operator='aggregate')
        for s in (a, b):
            s.remember(parent, asdict(self.profile))
        specs_a = a.specs(123, operators=operators)
        specs_b = b.specs(123, operators=operators)
        seen, origins = set(), set()
        for _ in range(30):
            spec = next(specs_a)
            self.assertEqual(spec, next(specs_b))
            origins.add(spec[-1])
            for case in spec[2]:
                seen.add(case.recipe['operator'])
        self.assertEqual(seen, set(operators))
        self.assertIn('mutation', origins)

    def test_operator_filter_requires_search(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(SystemExit) as error:
                fuzzer.main(['--doltlite', 'unused', '--sqlite', 'unused',
                             '--operator', 'update_pk', '--output', tmp])
            self.assertEqual(error.exception.code, 2)
            self.assertEqual(list(Path(tmp).iterdir()), [])

    def test_new_write_families_mutate_and_verify_the_target(self):
        operators = ('update_text', 'update_blob', 'update_pk', 'upsert_update',
                     'upsert_ignore', 'replace', 'insert_select')
        for key in ('integer', 'text'):
            p = replace(self.profile, key=key)
            for operator in operators:
                recipe = dict(self.recipe, operator=operator, predicate='all', context='plain')
                case = search.generated_case(p, recipe)
                with self.subTest(key=key, operator=operator), sqlite3.connect(':memory:') as db:
                    db.executescript(search.setup_sql(p, recipe))
                    before = db.execute('SELECT * FROM t ORDER BY id').fetchall()
                    db.execute('BEGIN')
                    db.execute(case.sql)
                    verification = db.execute(case.verify).fetchone()
                    self.assertEqual(verification[0], 0 if operator=='upsert_ignore' else p.rows)
                    self.assertEqual(verification[1], p.rows * (2 if operator=='insert_select' else 1))
                    after = db.execute('SELECT * FROM t ORDER BY id').fetchall()
                    if operator in ('upsert_ignore', 'replace'):
                        self.assertEqual(after, before)
                    else:
                        self.assertNotEqual(after, before)
                    db.rollback()
                    self.assertEqual(db.execute('SELECT * FROM t ORDER BY id').fetchall(), before)

    def test_history_and_known_issues_remain_mutation_inputs(self):
        case = search.generated_case(self.profile, self.recipe)
        bundle = {'profile': asdict(self.profile), 'case': asdict(case)}
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'history.json'
            s = search.Search(123, path, [{'bundles': [bundle]}])
            self.assertEqual(len(s.state['corpus']), 1)
            s.observe(self.profile, case, {'confirmed': True, 'plans': {'sqlite': 'SCAN t'}})
            restored = search.Search(999, path)
            self.assertEqual(restored.state, s.state)
            for k, v in self.recipe.items():
                self.assertEqual(restored.state['visits'][f'{k}:{v}'], 1)
            self.assertFalse(path.with_suffix('.tmp').exists())

    def test_underexplored_selection_uses_feature_counts(self):
        s = search.Search(1)
        recipe = dict(self.recipe, context='plain')
        unpopular = dict(recipe, context='after_delete')
        s.state['visits']['context:plain'] = 100
        with patch.object(s.rng, 'randrange', return_value=1), patch.object(search, 'fresh', side_effect=[recipe]*15+[unpopular]):
            self.assertEqual(s.choose(self.profile)[1], unpopular)
        s.state['shapes'][search.digest(unpopular)] = 10
        with patch.object(s.rng, 'randrange', return_value=1), patch.object(search, 'fresh', side_effect=[recipe]*15+[unpopular]):
            self.assertEqual(s.choose(self.profile)[1], recipe)

    def test_fingerprints_ignore_seeds_names_and_literal_offsets_but_keep_regimes(self):
        recipe = dict(self.recipe, predicate='range', operator='aggregate', source='table')
        case = search.generated_case(self.profile, recipe)
        plans = {'doltlite': 'SEARCH t', 'sqlite': 'SEARCH t'}
        fp = search.fingerprint(self.profile, case, plans)
        legacy = asdict(self.profile)
        del legacy['memory']
        self.assertEqual(fp, search.fingerprint(fuzzer.Profile(**legacy), case, plans))
        other = replace(self.profile, start=13, target=4)
        self.assertEqual(fp, search.fingerprint(other, search.generated_case(other, recipe, 3), plans))
        for p in (replace(self.profile, key='text'), replace(self.profile, payload=16384),
                  replace(self.profile, cache_kib=1), replace(self.profile, skew=True),
                  replace(self.profile, memory=True)):
            self.assertNotEqual(fp, search.fingerprint(p, search.generated_case(p, recipe), plans))
        self.assertNotEqual(fp, search.fingerprint(self.profile, case, dict(plans, sqlite='SCAN t')))

    def test_search_has_no_profile_count_ceiling(self):
        specs = search.Search(123).specs(123)
        for _ in range(130):
            index, profile, cases, setup, origin = next(specs)
            self.assertEqual(len(cases), 4)
            self.assertIn('CREATE TABLE u', setup)
            self.assertEqual(len({c.recipe['indexes'] for c in cases}), 1)
        self.assertEqual(index, 129)

    def test_deadline_saves_incomplete_case_without_confirmation(self):
        case = search.generated_case(self.profile, self.recipe)
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)/'output'
            with patch.object(fuzzer.shutil, 'copyfile'), patch.object(fuzzer.Runner, 'run', return_value='ok'), \
                 patch.object(fuzzer, 'binary_info', return_value={}), \
                 patch.object(search.Search, 'specs', return_value=iter([(0, self.profile, [case], 'setup', 'fresh')])), \
                 patch.object(fuzzer, 'measure_case', side_effect=fuzzer.BudgetExpired):
                rc = fuzzer.main(['--doltlite', 'unused', '--sqlite', 'unused', '--search', '--output', str(output)])
            self.assertEqual(rc, 0)
            report = json.loads((output/'results.json').read_text())
            self.assertIn('partial', report['status'])
            self.assertIn('incomplete', report['cases'][0])
            self.assertFalse(report['cases'][0].get('confirmed'))
            self.assertTrue((output/'p000/generated_0.json').exists())


    def test_timeout_cannot_contaminate_next_fixture(self):
        def run(command, sql=None, setup=False):
            if setup:
                Path(command[1]).write_text('pristine')
            return 'ok'

        count = 0

        def measure(runner, binaries, databases, profile, case, *args):
            nonlocal count
            self.assertEqual(databases['doltlite'].read_text(), 'pristine')
            count += 1
            if count == 1:
                databases['doltlite'].write_text('damaged')
                raise fuzzer.CaseTimeout('killed writer')
            return {'repeats': 1, 'result': '42', 'ratio': 1, 'confirmed': False, 'pairs': []}

        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(fuzzer.Runner, 'run', side_effect=run), \
                 patch.object(fuzzer, 'binary_info', return_value={}), \
                 patch.object(fuzzer, 'profile_for', return_value=self.profile), \
                 patch.object(fuzzer, 'measure_case', side_effect=measure), patch('builtins.print'):
                rc = fuzzer.main(['--doltlite', 'unused', '--sqlite', 'unused', '--profiles', '1', '--output', tmp])
            self.assertEqual(rc, 0)
            self.assertEqual(count, 17)

    def test_memory_replay_does_not_create_or_copy_disk_fixtures(self):
        profile = replace(self.profile, memory=True)
        case = search.generated_case(profile, dict(self.recipe, operator='update_text'))
        setup = search.setup_sql(profile, case.recipe)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            replay = root/'case.json'
            replay.write_text(json.dumps({'seed': 1, 'profile': asdict(profile),
                                         'case': asdict(case), 'setup_sql': setup}))
            with patch.object(fuzzer.Runner, 'run', return_value='ok') as run, \
                 patch.object(fuzzer, 'binary_info', return_value={}), \
                 patch.object(fuzzer.shutil, 'copyfile') as copy, \
                 patch.object(fuzzer, 'measure_case', return_value={
                     'repeats': 1, 'result': '42', 'ratio': 1, 'confirmed': False, 'pairs': []}) as measure:
                rc = fuzzer.main(['--doltlite', 'unused', '--sqlite', 'unused',
                                 '--replay', str(replay), '--output', str(root/'out')])
            self.assertEqual(rc, 0)
            copy.assert_not_called()
            self.assertFalse(any(call.kwargs.get('setup') for call in run.call_args_list))
            self.assertEqual(measure.call_args.kwargs['setup'], setup)
            self.assertTrue(measure.call_args.args[3].memory)
            plans = [c for c in run.call_args_list if len(c.args)>1 and 'EXPLAIN QUERY PLAN' in c.args[1]]
            self.assertEqual(len(plans), 2)
            for call in plans:
                self.assertEqual(call.args[0][1], ':memory:')
                self.assertIn(setup, call.args[1])
            sql = (root/'out/p000'/f'{case.name}.sql').read_text()
            self.assertLess(sql.index(setup), sql.index('.print WARM'))

    def test_issue_profiles_cannot_inject_sql_into_mutations(self):
        case = search.generated_case(self.profile, self.recipe)
        bad = dict(asdict(self.profile), key='integer);DROP TABLE t;--')
        s = search.Search(1, issues=[{'bundles': [{'profile': bad, 'case': asdict(case)}]}])
        self.assertEqual(s.state['corpus'], [])
        self.assertFalse(search.valid_profile(dict(asdict(self.profile), rows=10**12)))


class IssueTests(unittest.TestCase):
    def setUp(self):
        self.fp = 'a'*24
        self.issue = {'number': 10, 'url': 'https://github.com/dolthub/doltlite/issues/10',
                      'state': 'open', 'state_reason': None, 'labels': ['performance-hotspot'],
                      'fingerprints': [self.fp], 'bundles': []}
        self.profile = fuzzer.Profile(64, 32, 'integer', 16, False, 4096, 8, 10, 3, 12, 8)
        self.case = fuzzer.cases_for(self.profile)[0]
        self.bundle = {'seed': 123, 'profile': asdict(self.profile), 'case': asdict(self.case),
                       'setup_sql': fuzzer.fixture_sql(self.profile), 'expected': '2048|42',
                       'repeats': 1, 'fingerprint': self.fp}
        self.record = {'id': 'p000/scan_payload', 'fingerprint': self.fp, 'confirmed': True,
                       'ratio': 4, 'doltlite_ms': 80, 'sqlite_ms': 20, 'plans': {'sqlite': 'SCAN t', 'doltlite': 'SCAN t'},
                       'pairs': [{'sqlite_ms': 20, 'doltlite_ms': 80}]*5, 'reproducer': 'case.json'}
        self.report = {'cases': [self.record], 'min_ms': 20, 'seed': 123, 'source_commit': 'abc',
                       'harness_sha256': 'def', 'platform': 'test', 'binaries': {}}

    def test_duplicate_known_declined_and_fixed_lifecycle(self):
        self.assertEqual(issues.disposition(self.fp, [self.issue])[0], 'duplicate')
        known = dict(self.issue, labels=['known-performance-hotspot'])
        self.assertEqual(issues.disposition(self.fp, [known])[0], 'duplicate')
        fixed = dict(self.issue, state='closed', state_reason='completed')
        self.assertEqual(issues.disposition(self.fp, [fixed])[0], 'regression')
        self.assertEqual(issues.disposition(self.fp, [dict(fixed, labels=['known-performance-hotspot'])])[0], 'regression')
        declined = dict(fixed, state_reason='not_planned')
        self.assertEqual(issues.disposition(self.fp, [declined])[0], 'duplicate')
        self.assertEqual(issues.disposition('b'*24, [self.issue])[0], 'new')
        self.assertEqual(issues.disposition(self.fp, [fixed, self.issue])[0], 'duplicate')

    def test_broad_family_matching_requires_explicit_known_scope(self):
        known = dict(self.issue, families=['f'*24])
        self.assertEqual(issues.disposition('b'*24, [known], 'f'*24)[0], 'new')
        known['labels'] = ['known-performance-hotspot']
        self.assertEqual(issues.disposition('b'*24, [known], 'f'*24)[0], 'duplicate')
        self.assertEqual(issues.disposition('b'*24, [known], 'e'*24)[0], 'new')
        known.update(state='closed', state_reason='completed')
        self.assertEqual(issues.disposition('b'*24, [known], 'f'*24)[0], 'regression')

    def test_issue_body_contains_standalone_reproducer_and_raw_evidence(self):
        body = issues.issue_body(self.report, dict(self.record, family='f'*24), self.bundle, 'run-url', self.issue)
        self.assertEqual(issues.FAMILY.findall(body), [])
        self.assertEqual(issues.MARKER.findall(body), [self.fp])
        self.assertEqual(json.loads(issues.BUNDLE.search(body)[1]), self.bundle)
        self.assertIn('Recurrence of previously fixed', body)
        self.assertIn('Engine/generator commit: `abc`', body)
        self.assertIn('| 5 | 80.000 | 20.000 | 4.00× |', body)
        self.assertIn('known-performance-hotspot', body)

    def test_issue_lookup_paginates_both_labels_including_closed_issues(self):
        item = {'number': 10, 'html_url': self.issue['url'], 'state': 'closed', 'state_reason': 'completed',
                'labels': [{'name': 'performance-hotspot'}],
                'body': issues.issue_body(self.report, self.record, self.bundle, 'run')}
        with patch.object(issues, 'api', return_value=[[item], [dict(item, number=11)]]) as api:
            result = issues.read_issues('repo/name')
        self.assertEqual(len(result), 2)
        self.assertEqual(api.call_count, 2)
        for call in api.call_args_list:
            self.assertIn('state=all', call.args[1])
            self.assertTrue(call.kwargs['paginate'])
        self.assertEqual(result[0]['bundles'], [self.bundle])

    def test_publish_is_idempotent_and_api_failures_do_not_create_issues(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root/'case.json').write_text(json.dumps(self.bundle))
            (root/'results.json').write_text(json.dumps(dict(self.report, cases=[self.record, self.record])))
            with patch.object(issues, 'read_issues', return_value=[]) as read, \
                 patch.object(issues, 'api', return_value={'number': 10, 'html_url': self.issue['url']}) as api:
                issues.publish('repo/name', root, 'run')
                self.assertEqual(api.call_count, 1)
                read.return_value = [self.issue]
                issues.publish('repo/name', root, 'run')
                self.assertEqual(api.call_count, 1)
                read.side_effect = RuntimeError('API unavailable')
                with self.assertRaisesRegex(RuntimeError, 'API unavailable'):
                    issues.publish('repo/name', root, 'run')
                self.assertEqual(api.call_count, 1)

    def test_unconfirmed_cases_never_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root/'results.json').write_text(json.dumps(dict(self.report, cases=[{'timeout': 'slow'}, {'incomplete': 'budget'}])))
            with patch.object(issues, 'read_issues', return_value=[]), patch.object(issues, 'api') as api:
                issues.publish('repo/name', root, 'run')
            api.assert_not_called()

    def test_promotion_is_consumed_by_existing_pr_gate(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            replay = root/'replay.json'
            replay.write_text(json.dumps(self.bundle))
            output = root/'corpus/case.json'
            issues.main(['promote', '--replay', str(replay), '--issue', '10', '--output', str(output)])
            with patch.object(retained, 'sql', return_value='') as sql:
                fixtures = retained.prepare_retained({'baseline': 'base', 'candidate': 'new', 'stock': 'stock'}, root, root/'corpus')
                self.assertEqual(sql.call_count, 3)
                sql.return_value = 'WARM\n2048|42\nMEASURE\n2048|42\nRun Time: real 0.08 user 0.08 sys 0.0\nEND\n'
                measurements = retained.measure_retained('new', 'candidate', fixtures)
                self.assertEqual(next(iter(measurements.values())), 80000)
                self.assertEqual(retained.section_of(next(iter(measurements))), 'retained')
                sql.return_value = sql.return_value.replace('2048|42', 'wrong')
                with self.assertRaisesRegex(ValueError, 'result mismatch'):
                    retained.measure_retained('new', 'candidate', fixtures)
            with self.assertRaises(FileExistsError):
                issues.main(['promote', '--replay', str(replay), '--issue', '10', '--output', str(output)])

    def test_promoted_memory_cases_stay_in_memory(self):
        bundle = dict(self.bundle, issue=10)
        bundle['profile'] = dict(bundle['profile'], memory=True)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            corpus = root/'corpus'
            corpus.mkdir()
            (corpus/'memory.json').write_text(json.dumps(bundle))
            with patch.object(retained, 'sql') as sql:
                fixtures = retained.prepare_retained({'candidate': 'new'}, root, corpus)
                sql.assert_not_called()
                sql.return_value = 'WARM\n2048|42\nMEASURE\n2048|42\nRun Time: real 0.08 user 0.08 sys 0.0\nEND\n'
                self.assertEqual(next(iter(retained.measure_retained('new', 'candidate', fixtures).values())), 80000)
                command = sql.call_args.args
                self.assertEqual(command[1], ':memory:')
                self.assertLess(command[2].index(bundle['setup_sql']), command[2].index('.timer on'))

    def test_workflow_persists_history_and_serializes_issue_publication(self):
        workflow = (Path(__file__).resolve().parents[1]/'.github/workflows/nightly-hotspots.yml').read_text()
        self.assertIn('--search', workflow)
        self.assertIn('--seconds 1800', workflow)
        self.assertNotIn('--profiles', workflow)
        self.assertIn('actions/cache/restore@v4', workflow)
        self.assertIn('actions/cache/save@v4', workflow)
        self.assertIn('performance_hotspot_issues.py snapshot', workflow)
        self.assertIn('performance_hotspot_issues.py publish', workflow)
        self.assertIn('group: nightly-hotspots\n', workflow)
        self.assertIn('issues: write', workflow)
        self.assertNotIn('pull_request:', workflow)


if __name__ == '__main__':
    unittest.main()
