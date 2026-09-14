import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import textwrap


github = Path(__file__).resolve().parents[1]
workflow = (github / 'workflows/nightly-heartbeat.yml').read_text()
step = re.search(r'    - name: Audit the last seven nights\n.*?      run: \|\n'
                 r'((?:        [^\n]*\n|\n)+)', workflow, re.S)
assert step
script = textwrap.dedent(step[1])
days = [f'2026-09-{n:02d}' for n in range(7, 14)]


def runs(dates, conclusion='success'):
    return [{'run_started_at': f'{day}T06:30:00Z', 'conclusion': conclusion}
            for day in dates]


with tempfile.TemporaryDirectory(prefix='nightly-heartbeat-') as tmp:
    root = Path(tmp)
    for name, body in {
        'gh': '''
import json, os, subprocess, sys
args = sys.argv[2:]
fixture = json.loads(os.environ['HEARTBEAT_FIXTURE'])
record = fixture['seed' if '/seed-ci-caches.yml' in args[0] else 'old']
data = {'workflow_runs': record['runs']} if '/runs?' in args[0] else record['metadata']
if '--jq' in args:
    sys.exit(subprocess.run(['jq', '-c', '-r', args[args.index('--jq')+1]],
                           input=json.dumps(data), text=True).returncode)
print(json.dumps(data))
''',
        'date': '''
from datetime import datetime, timedelta, timezone
import sys
value = sys.argv[sys.argv.index('-d')+1]
if value.endswith(' days') or value.endswith(' days ago'):
    offset = int(value.split()[0])
    if value.endswith('ago'): offset = -offset
    day = datetime(2026, 9, 14, tzinfo=timezone.utc) + timedelta(days=offset)
else:
    day = datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(timezone.utc)
print(day.strftime('%Y-%m-%d'))
''',
    }.items():
        path = root / name
        path.write_text(f'#!{sys.executable}\n' + textwrap.dedent(body))
        path.chmod(0o755)

    def check(name, created, scheduled, expected, state='active'):
        fixture = {
            'old': {'metadata': {'state': 'active', 'created_at': '2026-08-01T00:00:00Z'},
                    'runs': runs(days)},
            'seed': {'metadata': {'state': state, 'created_at': created},
                     'runs': scheduled},
        }
        env = dict(os.environ, PATH=f'{root}:{os.environ["PATH"]}',
                   RUNNER_TEMP=str(root), GITHUB_REPOSITORY='dolthub/doltlite',
                   GITHUB_OUTPUT=str(root / 'output'), GITHUB_ENV=str(root / 'env'),
                   HEARTBEAT_FIXTURE=json.dumps(fixture))
        for name_out in ('output', 'env'):
            (root / name_out).write_text('')
        result = subprocess.run(['bash', '-c', script], env=env, text=True,
                                capture_output=True, timeout=30)
        assert result.returncode == 0, (name, result.stderr)
        lines = (root / 'heartbeat-body.md').read_text().splitlines()
        assert len(lines) == len(expected), (name, lines)
        for needle, line in zip(expected, lines):
            assert needle in line, (name, line)
        assert ('HEARTBEAT_GAPS=1' in (root / 'env').read_text()) == bool(expected)
        print(f'PASS: {name}')

    created = '2026-09-11T11:37:04-07:00'
    check('new workflow', created, runs(days[-2:]), [])
    check('first complete day missing', created, runs(days[-1:]), ['no scheduled run** on 2026-09-12'])
    check('cancelled', created, runs(days[-2:-1]) + runs(days[-1:], 'cancelled'), ['got: cancelled'])
    check('unfinished', created, runs(days[-2:-1]) + runs(days[-1:], None), ['got: in_progress'])
    check('failure reported elsewhere', created, runs(days[-2:], 'failure'), [])
    check('successful retry', created, runs(days[-2:]) + runs(days[-1:], 'cancelled'), [])
    check('UTC creation day', '2026-09-11T23:37:04-07:00', runs(days[-1:]), [])
    check('created today', '2026-09-14T01:00:00Z', [], [])
    check('older workflow gap', '2026-08-01T00:00:00Z', runs(days[1:]), ['no scheduled run** on 2026-09-07'])
    check('disabled new workflow', created, [], ['**disabled_inactivity**'], 'disabled_inactivity')
