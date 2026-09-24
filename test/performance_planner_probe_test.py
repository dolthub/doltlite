#!/usr/bin/env python3

import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import performance_planner_probe as probe


class PlannerProbeTests(unittest.TestCase):
    def test_alternatives_return_equal_results(self):
        for profile in probe.profiles(3235, 8, 128):
            with self.subTest(profile=profile), sqlite3.connect(':memory:') as db:
                db.executescript(probe.fixture(profile))
                for name, variants in probe.cases(profile).items():
                    results = [db.execute(sql).fetchall() for sql in variants.values()]
                    self.assertTrue(all(result == results[0] for result in results), name)

    def test_parse_rejects_truncation_wrong_results_and_timer_counts(self):
        output = ('PLAN:q:auto\nSCAN t\nWARM:q:auto\n42\nMEASURE:q:auto\n'
                  '42\nRun Time: real 0.025 user 0.025 sys 0.000\nEND:q:auto\n')
        self.assertEqual(probe.parse_output(output, 1)['q:auto'], (['SCAN t'], ['42'], 25))
        for broken in (output.replace('END:q:auto', ''), output.replace('\n42\nRun', '\n41\nRun'),
                       output.replace('Run Time: real 0.025 user 0.025 sys 0.000\n', ''), output + output):
            with self.subTest(output=broken), self.assertRaises(ValueError):
                probe.parse_output(broken, 1)

    def test_payload_matrix_preserves_other_inputs_and_statistics(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(probe, 'provenance', return_value={'compile_options': []}), \
                 patch.object(probe, 'measure', return_value={}), \
                 patch.object(probe, 'summarize', return_value={}), patch('builtins.print'):
                probe.main(['--doltlite', 'unused', '--sqlite', 'unused', '--output', tmp,
                            '--rows', '128', '--profile-index', '7', '--payload', '1', '--payload', '32', '--payload', '1024',
                            '--case', 'group_aggregate', '--case', 'group_limit', '--case', 'group_having'])
            records = [json.loads((Path(tmp)/f'profile-{i}.json').read_text()) for i in range(3)]
        self.assertEqual(records[0]['cases'], records[1]['cases'])
        self.assertEqual(records[1]['cases'], records[2]['cases'])
        statistics = []
        base = probe.profiles(3235, 8, 128)[7]
        for record, size in zip(records, (1, 32, 1024)):
            self.assertEqual(record['profile'], dict(base, payload=size))
            with sqlite3.connect(':memory:') as db:
                db.executescript(record['setup_sql'])
                self.assertEqual(db.execute('SELECT min(length(payload)),max(length(payload)) FROM t').fetchone(),
                                 (size, size))
                statistics.append(db.execute('SELECT * FROM sqlite_stat1 ORDER BY tbl,idx').fetchall())
                for name, variants in record['cases'].items():
                    results = [db.execute(sql).fetchall() for sql in variants.values()]
                    self.assertTrue(all(result == results[0] for result in results), name)
                    if name == 'group_limit':
                        self.assertEqual(results[0], db.execute(record['cases']['group_aggregate']['auto']).fetchall()[1:2])
        self.assertEqual(statistics[0], statistics[1])
        self.assertEqual(statistics[1], statistics[2])

    def test_payload_matrix_rejects_invalid_sizes_and_replay_overrides(self):
        for args in (['--payload', '0'], ['--payload', '-1'], ['--payload', '32', '--replay', '/unused']):
            with self.subTest(args=args), patch.object(probe, 'measure') as measure, \
                 self.assertRaises(SystemExit) as error:
                probe.main(['--doltlite', 'unused', '--sqlite', 'unused', '--output', '/unused'] + args)
            self.assertEqual(error.exception.code, 2)
            measure.assert_not_called()

    def test_measure_requires_every_variant(self):
        profile = probe.profiles(3235, 1, 8)[0]
        with patch.object(probe, 'execute', return_value=''), self.assertRaises(ValueError):
            probe.measure('unused', profile, '', {'q': {'auto': 'SELECT 1;'}}, 1, 1, Path('/unused'))

    def test_summary_checks_engines_and_batch_floor(self):
        engine = {'results': {'q:auto': ['42'], 'q:primary': ['42']},
                  'values': {'q:auto': [10, 12], 'q:primary': [2, 3]},
                  'batch_ms': {'q:auto': [100, 120], 'q:primary': [20, 30]}}
        record = {'cases': {'q': {'auto': '', 'primary': ''}},
                  'engines': {'doltlite': engine, 'sqlite': json.loads(json.dumps(engine))}}
        summary = probe.summarize(record, 20)['q']
        self.assertEqual(summary['paired_ratios'], [5, 4])
        self.assertTrue(summary['adequate_batches'])
        self.assertFalse(probe.summarize(record, 21)['q']['adequate_batches'])
        record['engines']['sqlite']['results']['q:auto'] = ['wrong']
        with self.assertRaises(ValueError):
            probe.summarize(record, 20)

    def test_replay_uses_recorded_sql_and_seed(self):
        source = {'seed': 17, 'profile': probe.profiles(17, 1, 8)[0],
                  'setup_sql': 'CREATE TABLE custom(x);',
                  'cases': {'custom': {'auto': 'SELECT 19;'}}}
        with tempfile.TemporaryDirectory() as tmp:
            replay = Path(tmp) / 'replay.json'
            replay.write_text(json.dumps(source))
            with patch.object(probe, 'provenance', return_value={'compile_options': []}), \
                 patch.object(probe, 'measure', return_value={}) as measure, \
                 patch.object(probe, 'summarize', return_value={}), \
                 patch('builtins.print'):
                probe.main(['--doltlite', 'dolt', '--sqlite', 'sqlite', '--output', tmp,
                            '--replay', str(replay), '--seed', '999'])
            for call in measure.call_args_list:
                self.assertEqual(call.args[2:4], (source['setup_sql'], source['cases']))
            saved = json.loads((Path(tmp) / 'profile-0.json').read_text())
            self.assertEqual(saved['seed'], 17)

    def test_rejects_histogram_builds_on_either_side(self):
        for engine in ('doltlite', 'sqlite'):
            with self.subTest(engine=engine), tempfile.TemporaryDirectory() as tmp:
                def build(binary):
                    return {'compile_options': ['ENABLE_STAT4'] if binary == engine else []}
                with patch.object(probe, 'provenance', side_effect=build), \
                     patch.object(probe, 'measure') as measure, \
                     self.assertRaises(SystemExit) as error:
                    probe.main(['--doltlite', 'doltlite', '--sqlite', 'sqlite', '--output', tmp])
                self.assertEqual(error.exception.code, 2)
                measure.assert_not_called()


if __name__ == '__main__':
    unittest.main()
