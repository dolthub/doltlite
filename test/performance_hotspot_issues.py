#!/usr/bin/env python3

import argparse
import json
import os
from pathlib import Path
import re
import subprocess

MARKER = re.compile(r'<!-- doltlite-hotspot:([0-9a-f]{24}) -->')
FAMILY = re.compile(r'<!-- doltlite-hotspot-family:([0-9a-f]{24}) -->')
BUNDLE = re.compile(r'<!-- hotspot-reproducer -->\s*```json\n(.*?)\n```', re.S)
LABELS = {'performance-hotspot': 'Confirmed DoltLite/SQLite performance gap awaiting investigation',
          'known-performance-hotspot': 'Investigated performance gap retained in the PR hotspot suite'}


def api(repo, endpoint, payload=None, paginate=False):
    command = ['gh', 'api', f'repos/{repo}/{endpoint}']
    if paginate:
        command += ['--paginate', '--slurp']
    if payload is not None:
        command += ['--method', 'POST', '--input', '-']
    result = subprocess.run(command, input=json.dumps(payload) if payload is not None else None,
                            capture_output=True, text=True, timeout=120)
    if result.returncode:
        raise RuntimeError(f'GitHub request failed: {result.stderr}')
    return json.loads(result.stdout)


def read_issues(repo):
    issues = {}
    for label in LABELS:
        pages = api(repo, f'issues?state=all&labels={label}&per_page=100', paginate=True)
        for page in pages:
            for issue in page:
                if 'pull_request' in issue:
                    continue
                body = issue.get('body') or ''
                bundles = []
                for match in BUNDLE.finditer(body):
                    bundles.append(json.loads(match[1]))
                issues[issue['number']] = {
                    'number': issue['number'], 'url': issue['html_url'], 'state': issue['state'],
                    'state_reason': issue.get('state_reason'),
                    'labels': [x['name'] for x in issue['labels']],
                    'fingerprints': MARKER.findall(body), 'families': FAMILY.findall(body), 'bundles': bundles,
                }
    return list(issues.values())


def disposition(fingerprint, issues, family=None):
    matches = [i for i in issues if fingerprint in i['fingerprints']
               or (family and family in i.get('families', []) and 'known-performance-hotspot' in i['labels'])]
    for issue in matches:
        if issue['state'] == 'open' or issue['state_reason'] != 'completed':
            return 'duplicate', issue
    return ('regression', matches[0]) if matches else ('new', None)


def issue_body(report, record, bundle, run_url, previous=None):
    pairs = record['pairs']
    lines = [f"<!-- doltlite-hotspot:{record['fingerprint']} -->", '',
             f"Confirmed in all {len(pairs)} fresh-connection confirmation pairs; results match stock SQLite.",
             f"Median paired slowdown: **{record['ratio']:.2f}×**. "
             f"DoltLite: {record['doltlite_ms']:.3f} ms/query; SQLite: {record['sqlite_ms']:.3f} ms/query.",
             f"SQLite batch timings use a {report['min_ms']} ms floor for the 3× confirmation test.",
             'Autocommit writes are excluded. Setup, warm-up, and rollback are untimed.', '',
             f"Run: {run_url}", f"Engine/generator commit: `{report['source_commit']}`", '',
             '| Pair | DoltLite batch ms | SQLite batch ms | Ratio |', '| --- | ---: | ---: | ---: |']
    for n, pair in enumerate(pairs, 1):
        d, s = pair['doltlite_ms'], pair['sqlite_ms']
        lines.append(f'| {n} | {d:.3f} | {s:.3f} | {d/s:.2f}× |')
    if previous:
        lines += ['', f"Recurrence of previously fixed finding: {previous['url']}"]
    provenance = {k: report[k] for k in ('seed', 'source_commit', 'harness_sha256', 'platform', 'binaries')}
    lines += ['', 'Provenance:', '```json', json.dumps(provenance, indent=2), '```']
    for arm, plan in record['plans'].items():
        lines += ['', f'{arm} query plan:', '```text', plan.strip(), '```']
    lines += ['', 'Save the following JSON as `case.json`. The setup SQL is embedded so this survives artifact expiry.',
              '```sh', 'python3 test/performance_hotspot_fuzzer.py --doltlite build/doltlite '
              '--sqlite build-stockref/sqlite3 --replay case.json --output replay-results', '```', '',
              '<!-- hotspot-reproducer -->', '```json', json.dumps(bundle, indent=2), '```', '',
              'Triage: investigate and optimize, or retain a representative reproducer in '
              '`test/performance-hotspot-corpus/` using `performance_hotspot_issues.py promote`. '
              'After that benchmark lands, add `known-performance-hotspot` and keep this issue open. '
              'Fixed cases should retain their benchmark and close as completed. '
              'Keep the fingerprint marker when editing or grouping findings.']
    if record.get('family'):
        lines += ['', f"Parameter-family ID: `{record['family']}`. To explicitly accept other parameters of this same SQL shape and access plans, "
                  'add an HTML comment containing `doltlite-hotspot-family:` immediately followed by this ID, '
                  'with one space inside each comment delimiter. This broader match applies only with the known-hotspot label.']
    body = '\n'.join(lines)
    if len(body) > 60000:
        raise ValueError('issue reproducer exceeds durable issue size limit')
    return body


def publish(repo, output, run_url):
    report = json.loads((output/'results.json').read_text())
    issues = read_issues(repo)
    proposals = []
    for record in report['cases']:
        if not record.get('confirmed'):
            continue
        status, previous = disposition(record['fingerprint'], issues, record.get('family'))
        if status == 'duplicate':
            proposals.append({'fingerprint': record['fingerprint'], 'status': status, 'url': previous['url']})
            continue
        bundle = json.loads((output/record['reproducer']).read_text())
        body = issue_body(report, record, bundle, run_url, previous)
        recipe = bundle['case'].get('recipe', {})
        description = f"{recipe['operator']}/{recipe['context']}" if recipe else bundle['case']['name']
        title = f"Performance hotspot{' regression' if previous else ''}: {record['ratio']:.1f}x {description} ({record['fingerprint'][:8]})"
        created = api(repo, 'issues', {'title': title, 'body': body, 'labels': ['performance-hotspot']})
        issues.append({'number': created['number'], 'url': created['html_url'], 'state': 'open',
                       'state_reason': None, 'labels': ['performance-hotspot'],
                       'fingerprints': [record['fingerprint']], 'bundles': [bundle]})
        proposals.append({'fingerprint': record['fingerprint'], 'status': status, 'url': created['html_url']})
        (output/'issues.json').write_text(json.dumps(proposals, indent=2)+'\n')
    (output/'issues.json').write_text(json.dumps(proposals, indent=2)+'\n')
    with (output/'summary.md').open('a') as summary:
        summary.write('\n### Hotspot issues\n\n')
        for proposal in proposals:
            summary.write(f"- {proposal['status']}: {proposal['url']} (`{proposal['fingerprint']}`)\n")


def main(argv=None):
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest='command', required=True)
    for command in ('snapshot', 'publish'):
        p = sub.add_parser(command)
        p.add_argument('--repo', default=os.environ.get('GITHUB_REPOSITORY', 'dolthub/doltlite'))
        p.add_argument('--output', required=True, type=Path)
    sub.choices['publish'].add_argument('--run-url', required=True)
    p = sub.add_parser('promote')
    p.add_argument('--replay', required=True, type=Path)
    p.add_argument('--issue', required=True, type=int)
    p.add_argument('--output', required=True, type=Path)
    args = parser.parse_args(argv)
    if args.command == 'snapshot':
        existing = {label['name'] for page in api(args.repo, 'labels?per_page=100', paginate=True) for label in page}
        for label, description in LABELS.items():
            if label not in existing:
                api(args.repo, 'labels', {'name': label, 'description': description, 'color': 'd4c5f9'})
        args.output.write_text(json.dumps(read_issues(args.repo), indent=2)+'\n')
    elif args.command == 'publish':
        publish(args.repo, args.output, args.run_url)
    else:
        bundle = json.loads(args.replay.read_text())
        if 'setup_sql' not in bundle:
            bundle['setup_sql'] = (args.replay.parent/'setup.sql').read_text()
        if (args.issue <= 0 or 'expected' not in bundle or not MARKER.fullmatch(f"<!-- doltlite-hotspot:{bundle.get('fingerprint', '')} -->")
                or type(bundle.get('repeats')) is not int or not 1 <= bundle['repeats'] <= 1024):
            parser.error('promotion requires a positive issue number and a verified reproducer with expected results')
        bundle['issue'] = args.issue
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open('x') as output:
            output.write(json.dumps(bundle, indent=2)+'\n')


if __name__ == '__main__':
    main()
