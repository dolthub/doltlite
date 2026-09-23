#!/usr/bin/env python3

import argparse
import hashlib
import json
from pathlib import Path
import platform
import random
import statistics
import subprocess
import tempfile

from performance_hotspot_fuzzer import TIMER


def fixture(p):
    key = {'integer': 'i', 'text': "printf('%016x',i)",
           'blob': "CAST(printf('%016x',i) AS BLOB)", 'composite': "printf('%016x',i)"}[p['key']]
    decl = {'integer': 'INTEGER PRIMARY KEY', 'text': 'TEXT PRIMARY KEY',
            'blob': 'BLOB PRIMARY KEY', 'composite': 'TEXT'}[p['key']]
    extra = ',PRIMARY KEY(id,seq)' if p['key'] == 'composite' else ''
    group = f"CASE WHEN i%10<9 THEN 0 ELSE i%{p['groups']} END" if p['skew'] else f"i%{p['groups']}"
    u_decl = 'TEXT PRIMARY KEY' if p['key'] == 'composite' else decl
    return f"""CREATE TABLE t(id {decl},seq INTEGER NOT NULL,grp INTEGER NOT NULL,
  v INTEGER NOT NULL,tag TEXT,payload BLOB{extra});
BEGIN;
WITH RECURSIVE c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<{p['rows']})
INSERT INTO t SELECT {key},i,{group},(i*7919)%1000000,printf('tag-%08x',i),
  CAST(printf('%0{p['payload']}d',i) AS BLOB) FROM c;
COMMIT;
CREATE INDEX t_g ON t(grp);
CREATE INDEX t_gv ON t(grp,v);
CREATE TABLE u(id {u_decl},seq INTEGER,grp INTEGER,v INTEGER);
INSERT INTO u SELECT id,seq,grp,v FROM t WHERE seq%{p['stride']}=0;
CREATE INDEX u_g ON u(grp);
ANALYZE;
"""


def cases(p):
    primary = 'NOT INDEXED' if p['key'] == 'integer' else 'INDEXED BY sqlite_autoindex_t_1'
    hints = {'auto': '', 'primary': primary, 'group': 'INDEXED BY t_g', 'group_value': 'INDEXED BY t_gv'}
    out = {}
    predicates = [('hot_group', 'grp=0')] + [
        (f'range_{pc}', f"grp<{max(1, p['groups'] * pc // 100)}") for pc in (6, 25, 50, 90)]
    for name, pred in predicates:
        out[name] = {a: f'SELECT sum(seq),sum(length(payload)) FROM t {h} WHERE {pred};'
                     for a, h in hints.items()}
    out['group_aggregate'] = {
        a: f'SELECT grp,sum(seq),sum(length(payload)) FROM t {h} GROUP BY grp ORDER BY grp;'
        for a, h in hints.items()}
    for pc in (1, 50, 100):
        limit = max(1, p['rows'] * pc // 100)
        out[f'order_{pc}'] = {
            a: f'SELECT sum(seq),sum(length(payload)) FROM '
               f'(SELECT seq,payload FROM t {h} ORDER BY grp,v LIMIT {limit});'
            for a, h in hints.items()}
    on = 't.id=u.id' + (' AND t.seq=u.seq' if p['key'] == 'composite' else '')
    predicates = [('join_group', 't.grp=0'), ('join_half', f"t.grp<{p['groups'] // 2}"), ('join_all', '1')]
    for name, pred in predicates:
        expr = 'SELECT sum(t.seq+u.v),sum(length(t.payload)) FROM '
        out[name] = {
            'auto': expr + f't JOIN u ON {on} WHERE {pred};',
            't_primary': expr + f't {primary} CROSS JOIN u WHERE {on} AND {pred};',
            't_group': expr + f't INDEXED BY t_g CROSS JOIN u WHERE {on} AND {pred};',
            'u_outer': expr + f'u CROSS JOIN t {primary} WHERE {on} AND {pred};'}
    return out


def execute(binary, database, sql):
    result = subprocess.run([str(binary), str(database)], input=sql, text=True,
                            capture_output=True, timeout=300)
    if result.returncode or result.stderr.strip():
        raise RuntimeError(result.stderr + '\n' + result.stdout[-1000:])
    return result.stdout


def parse_output(output, repeats):
    lines = iter(output.splitlines())
    measurements = {}
    for line in lines:
        if not line.startswith('PLAN:'):
            raise ValueError(f'expected plan marker: {line}')
        ident = line[5:]
        sections = []
        for marker in ('WARM:', 'MEASURE:', 'END:'):
            section = []
            for line in lines:
                if line == marker + ident:
                    break
                section.append(line)
            else:
                raise ValueError(f'missing {marker}{ident}')
            sections.append(section)
        plan, result, measured = sections
        times, actual = [], []
        for line in measured:
            match = TIMER.fullmatch(line)
            if match:
                times.append(float(match[1]) * 1000)
            else:
                actual.append(line)
        if len(times) != repeats or actual != result * repeats:
            raise ValueError(f'timing or result mismatch: {ident}')
        if ident in measurements:
            raise ValueError(f'duplicate measurement: {ident}')
        measurements[ident] = (plan, result, sum(times))
    return measurements


def measure(binary, profile, setup, queries, runs, repeats, root):
    database = ':memory:' if profile['memory'] else root / 'fixture.db'
    preamble = ('.bail on\n.headers off\n.mode list\n.nullvalue NULL\n.explain off\n'
                '.output /dev/null\nPRAGMA mmap_size=0;\n'
                f'PRAGMA cache_size=-{profile["cache_kib"]};\nPRAGMA temp_store=MEMORY;\n')
    if not profile['memory']:
        execute(binary, database, preamble + setup)
    record = {'values': {}, 'batch_ms': {}, 'plans': {}, 'results': {}}
    expected = {f'{name}:{arm}' for name, variants in queries.items() for arm in variants}
    for run in range(runs):
        script = preamble + (setup if profile['memory'] else '')
        script += '\n.output stdout\n'
        for name, variants in queries.items():
            order = list(variants)
            offset = run % len(order)
            order = order[offset:] + order[:offset]
            if run % 2:
                order.reverse()
            for arm in order:
                query, ident = variants[arm], f'{name}:{arm}'
                script += (f'.print PLAN:{ident}\nEXPLAIN QUERY PLAN {query}\n'
                           f'.print WARM:{ident}\n{query}\n.print MEASURE:{ident}\n.timer on\n'
                           + (query + '\n') * repeats + f'.timer off\n.print END:{ident}\n')
        measured = parse_output(execute(binary, database, script), repeats)
        if set(measured) != expected:
            raise ValueError('missing or unexpected measurements')
        for ident, (plan, result, elapsed) in measured.items():
            if record['results'].setdefault(ident, result) != result:
                raise ValueError(f'result changed across connections: {ident}')
            if record['plans'].setdefault(ident, plan) != plan:
                raise ValueError(f'plan changed across connections: {ident}')
            record['values'].setdefault(ident, []).append(elapsed / repeats)
            record['batch_ms'].setdefault(ident, []).append(elapsed)
        if run == 0:
            record['statistics'] = execute(binary, database, preamble
                + (setup if profile['memory'] else '') + '\n.output stdout\n'
                + 'SELECT tbl,idx,stat FROM sqlite_stat1 ORDER BY tbl,idx;\n').splitlines()
    for name, variants in queries.items():
        results = [record['results'][f'{name}:{arm}'] for arm in variants]
        if any(result != results[0] for result in results[1:]):
            raise ValueError(f'alternative plans disagree: {name}')
    return record


def provenance(binary):
    binary = Path(binary).resolve()
    return {'binary': str(binary), 'sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
            'version': execute(binary, ':memory:', 'SELECT sqlite_version();').strip(),
            'compile_options': execute(binary, ':memory:', 'PRAGMA compile_options;').splitlines()}


def summarize(record, min_batch_ms):
    output = {}
    for name, variants in record['cases'].items():
        engines = record['engines']
        for arm in variants:
            ident = f'{name}:{arm}'
            if engines['doltlite']['results'][ident] != engines['sqlite']['results'][ident]:
                raise ValueError(f'engines disagree: {ident}')
        values = engines['doltlite']['values']
        medians = {arm: statistics.median(values[f'{name}:{arm}']) for arm in variants}
        best = min(medians, key=medians.get)
        batches = engines['doltlite']['batch_ms']
        ratios = [a / max(b, 0.000001) for a, b in
                  zip(values[f'{name}:auto'], values[f'{name}:{best}'])]
        output[name] = {'best': best, 'median_ms': medians,
                        'auto_over_best': medians['auto'] / max(medians[best], 0.000001),
                        'paired_ratios': ratios,
                        'adequate_batches': min(batches[f'{name}:auto'] + batches[f'{name}:{best}']) >= min_batch_ms,
                        'sqlite_auto_ms': statistics.median(engines['sqlite']['values'][f'{name}:auto'])}
    return output


def profiles(seed, count, rows):
    rng = random.Random(seed)
    return [{'key': ['integer', 'text', 'blob', 'composite'][i % 4], 'rows': rows,
             'payload': rng.choice([32, 256, 1024]), 'groups': rng.choice([16, 256]),
             'skew': i % 4 < 2, 'cache_kib': rng.choice([4096, 16384]),
             'stride': rng.choice([2, 8, 32]), 'memory': i % 8 < 4} for i in range(count)]


def main(argv=None):
    parser = argparse.ArgumentParser(description='Compare chosen and forced read plans; replay saved SQL exactly.')
    parser.add_argument('--doltlite', required=True)
    parser.add_argument('--sqlite', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--profiles', type=int, default=8)
    parser.add_argument('--profile-index', type=int, action='append', help='Select zero-based generated profile indices')
    parser.add_argument('--rows', type=int, default=32768)
    parser.add_argument('--runs', type=int, default=5)
    parser.add_argument('--repeats', type=int, default=20)
    parser.add_argument('--seed', type=int, default=3235)
    parser.add_argument('--min-batch-ms', type=float, default=20)
    parser.add_argument('--replay', help='Previously saved profile JSON; executes its recorded setup and queries')
    parser.add_argument('--case', action='append', help='Restrict to named cases; may be repeated')
    args = parser.parse_args(argv)
    if min(args.profiles, args.rows, args.runs, args.repeats, args.min_batch_ms) <= 0:
        parser.error('counts and minimum batch duration must be positive')
    records = [{'seed': args.seed, 'profile': p, 'setup_sql': fixture(p), 'cases': cases(p)}
               for p in profiles(args.seed, args.profiles, args.rows)]
    if args.profile_index:
        if args.replay or any(i < 0 or i >= len(records) for i in args.profile_index):
            parser.error('profile indices require generated profiles and must be in range')
        records = [records[i] for i in args.profile_index]
    if args.replay:
        source = json.loads(Path(args.replay).read_text())
        records = [{key: source[key] for key in ('seed', 'profile', 'setup_sql', 'cases')}]
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    binaries = {'doltlite': args.doltlite, 'sqlite': args.sqlite}
    builds = {arm: provenance(binary) for arm, binary in binaries.items()}
    for i, record in enumerate(records):
        if args.case:
            unknown = set(args.case) - set(record['cases'])
            if unknown:
                parser.error(f'unknown cases: {sorted(unknown)}')
            record['cases'] = {name: record['cases'][name] for name in args.case}
        record.update(runs=args.runs, repeats=args.repeats, platform=platform.platform(),
                      harness_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                      builds=builds, engines={})
        for arm, binary in binaries.items():
            with tempfile.TemporaryDirectory() as tmp:
                record['engines'][arm] = measure(binary, record['profile'], record['setup_sql'],
                    record['cases'], args.runs, args.repeats, Path(tmp))
        record['summary'] = summarize(record, args.min_batch_ms)
        (output / f'profile-{i}.json').write_text(json.dumps(record, indent=2) + '\n')
        print(json.dumps({'profile': record['profile'], 'summary': record['summary']}), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
