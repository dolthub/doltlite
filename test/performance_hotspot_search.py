import hashlib
import json
import math
from pathlib import Path
import random
import re

VERSION = 2
CHOICES = {
    'source': ['table', 'join', 'exists', 'in'],
    'predicate': ['all', 'group', 'range', 'or', 'modulo', 'group_range'],
    'expression': ['v', 'seq', 'length', 'bytes'],
    'operator': ['aggregate', 'distinct', 'group', 'order', 'index_order', 'window', 'nested',
                 'update', 'delete', 'create_index', 'add_column',
                 'update_text', 'update_blob', 'update_pk', 'upsert_update',
                 'upsert_ignore', 'replace', 'insert_select',
                 'correlated_aggregate', 'correlated_limit', 'anti_join',
                 'union_all', 'intersect', 'except', 'window_frame', 'materialized'],
    'indexes': ['none', 'group', 'cover', 'both'],
    'context': ['plain', 'after_scan', 'after_points', 'after_update', 'after_delete'],
    'direction': ['ASC', 'DESC'],
}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()[:24]


def fresh(rng):
    return {key: rng.choice(values) for key, values in CHOICES.items()}


def valid_recipe(recipe):
    return isinstance(recipe, dict) and set(recipe) == set(CHOICES) and all(recipe[k] in values for k, values in CHOICES.items())


def valid_profile(profile):
    from performance_hotspot_fuzzer import Profile
    try:
        p = Profile(**profile)
    except (TypeError, KeyError):
        return False
    numbers = (p.rows, p.payload, p.groups, p.cache_kib, p.stride, p.lookups, p.target, p.start, p.width)
    return (all(type(n) is int for n in numbers) and type(p.skew) is bool and type(p.memory) is bool
            and p.key in ('integer', 'text') and 1 <= p.rows <= 262144
            and 1 <= p.payload <= 16384 and p.rows*p.payload <= 256*1024*1024
            and 1 <= p.groups <= 4096 and 1 <= p.cache_kib <= 65536
            and 1 <= p.stride <= 32 and 1 <= p.lookups <= 10000
            and 0 <= p.target < p.groups and 1 <= p.start <= p.rows
            and 1 <= p.width <= p.rows and p.start+p.width-1 <= p.rows)


def setup_sql(profile, recipe):
    from performance_hotspot_fuzzer import fixture_sql
    sql = fixture_sql(profile)
    if recipe['indexes'] in ('none', 'cover'):
        sql += 'DROP INDEX t_g;\n'
    if recipe['indexes'] in ('none', 'group'):
        sql += 'DROP INDEX t_gv;\n'
    sql += ('CREATE TABLE u(id INTEGER PRIMARY KEY, grp INTEGER, v INTEGER);\n'
            f'INSERT INTO u SELECT seq,grp,v FROM t WHERE seq%{profile.stride}=0;\n'
            'CREATE INDEX u_g ON u(grp);\nANALYZE;\n')
    return sql


def generated_case(profile, recipe, number=0):
    from performance_hotspot_fuzzer import Case, key_sql
    if not valid_recipe(recipe):
        raise ValueError('invalid SQL recipe')
    p, r = profile, recipe
    predicates = {
        'all': '1', 'group': f't.grp={p.target}',
        'range': f't.id BETWEEN {key_sql(p, p.start)} AND {key_sql(p, p.start+p.width-1)}',
        'group_range': f't.grp<{max(1, p.groups*p.width//p.rows)}',
        'or': f'(t.grp={p.target} OR t.seq%{p.stride}=0)',
        'modulo': f't.seq%{p.stride}=0',
    }
    expression = {'v': 't.v', 'seq': 't.seq', 'length': 'length(t.payload)',
                  'bytes': 'unicode(substr(CAST(t.payload AS TEXT),-1,1))'}[r['expression']]
    source, predicate = 't', predicates[r['predicate']]
    if r['source'] == 'join':
        source += ' JOIN u ON u.id=t.seq'
    elif r['source'] == 'exists':
        predicate += ' AND EXISTS(SELECT 1 FROM u WHERE u.id=t.seq AND u.v>=t.v)'
    elif r['source'] == 'in':
        predicate += ' AND t.seq IN (SELECT id FROM u WHERE grp=t.grp)'
    inner = f'SELECT {expression} AS x,t.seq AS seq,t.grp AS grp FROM {source} WHERE {predicate}'
    operator = r['operator']
    if operator == 'distinct':
        inner = f'SELECT DISTINCT x FROM ({inner})'
    elif operator == 'group':
        inner = f'SELECT sum(x) AS x FROM ({inner}) GROUP BY grp'
    elif operator == 'order':
        inner += f' ORDER BY x {r["direction"]},t.seq LIMIT {p.width}'
    elif operator == 'index_order':
        inner += f' ORDER BY t.grp {r["direction"]},t.v {r["direction"]},t.id LIMIT {p.width}'
    elif operator == 'window':
        inner = f'SELECT row_number() OVER (PARTITION BY grp ORDER BY x {r["direction"]},seq) AS x FROM ({inner})'
    elif operator == 'nested':
        inner = f'SELECT x FROM ({inner}) WHERE x%{p.stride}=0'
    elif operator in ('union_all', 'intersect', 'except'):
        compound = operator.replace('_', ' ').upper()
        inner = f'SELECT x FROM ({inner}) {compound} SELECT v FROM u'
    elif operator == 'window_frame':
        inner = (f'SELECT sum(x) OVER (PARTITION BY grp ORDER BY seq {r["direction"]} '
                 f'ROWS BETWEEN {p.stride} PRECEDING AND CURRENT ROW) AS x FROM ({inner})')
    elif operator == 'materialized':
        inner = f'WITH q AS MATERIALIZED ({inner}) SELECT a.x+b.x AS x FROM q a JOIN q b USING(seq)'
    elif operator in ('correlated_aggregate', 'correlated_limit'):
        lookup = ('SELECT sum(u.v) FROM u WHERE u.grp=t.grp' if operator == 'correlated_aggregate'
                  else f'SELECT u.v FROM u WHERE u.grp=t.grp ORDER BY u.v {r["direction"]},u.id LIMIT 1')
        inner = f'SELECT ({lookup}) AS x FROM t WHERE {predicates[r["predicate"]]} AND t.seq<={p.lookups}'
    elif operator == 'anti_join':
        inner = f'SELECT {expression} AS x FROM t LEFT JOIN u ON u.id=t.seq WHERE u.id IS NULL AND {predicates[r["predicate"]]}'
    prepare = {
        'plain': '', 'after_scan': 'SELECT sum(length(payload)),sum(v) FROM t NOT INDEXED;',
        'after_points': f'WITH RECURSIVE c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<1000) SELECT sum((SELECT v FROM t WHERE id={key_sql(p, f"1+(c.i*2654435761)%{p.rows}")})) FROM c;',
        'after_update': f'UPDATE t SET v=v+1 WHERE seq%{p.stride}=0;',
        'after_delete': f'DELETE FROM t WHERE seq%{p.stride}=0;',
    }[r['context']]
    verify = 'SELECT count(*),sum(v),sum(seq),sum(length(payload)) FROM t;'
    if operator == 'update':
        return Case(f'generated_{number}', f'UPDATE t SET v=v+1 WHERE {predicates[r["predicate"]]};', verify, prepare, r)
    if operator == 'delete':
        return Case(f'generated_{number}', f'DELETE FROM t WHERE {predicates[r["predicate"]]};', verify, prepare, r)
    if operator == 'create_index':
        return Case(f'generated_{number}', f'CREATE INDEX probe ON t(({expression.replace("t.", "")}));', verify, prepare, r)
    if operator == 'add_column':
        return Case(f'generated_{number}', 'ALTER TABLE t ADD COLUMN z INTEGER NOT NULL DEFAULT 7;', 'SELECT count(*),sum(z) FROM t;', prepare, r)
    updates = {
        'update_text': "tag=printf('updated-%08x',seq)",
        'update_blob': "payload=CAST(substr(payload,1,length(payload)-1)||'x' AS BLOB)",
        'update_pk': f'id={key_sql(p, f"seq+{p.rows}")}',
    }
    sql = None
    if operator in updates:
        sql = f'UPDATE t SET {updates[operator]} WHERE {predicates[r["predicate"]]};'
    elif operator in ('upsert_update', 'upsert_ignore', 'replace'):
        insert = 'INSERT OR REPLACE' if operator == 'replace' else 'INSERT'
        conflict = {'replace': '', 'upsert_ignore': ' ON CONFLICT(id) DO NOTHING',
                    'upsert_update': " ON CONFLICT(id) DO UPDATE SET v=excluded.v+1,tag=excluded.tag||'x'"}[operator]
        sql = f'{insert} INTO t SELECT * FROM t WHERE {predicates[r["predicate"]]}{conflict};'
    elif operator == 'insert_select':
        sql = (f'INSERT INTO t SELECT {key_sql(p, f"seq+{p.rows}")},seq+{p.rows},grp,v,tag,payload '
               f'FROM t WHERE {predicates[r["predicate"]]};')
    if sql:
        verify = ('SELECT changes(),count(*),sum(v),sum(seq),min(id),max(id),'
                  'sum(length(payload)),sum(length(tag)),'
                  'sum(unicode(substr(tag,-1,1))),sum(unicode(substr(CAST(payload AS TEXT),-1,1))) FROM t;')
        return Case(f'generated_{number}', sql, verify, prepare, r)
    return Case(f'generated_{number}', f'SELECT count(*),sum(x) FROM ({inner});', prepare=prepare, recipe=r)


def family_fingerprint(profile, case, plans):
    def normalized(sql):
        return re.sub(r'\b\d+\b', '?', ' '.join(sql.split()))
    identity = {'key': profile.key, 'sql': normalized(case.sql), 'indexes': case.recipe.get('indexes'),
                   'prepare': normalized(case.prepare), 'verify': normalized(case.verify),
                   'plans': {arm: normalized(plan) for arm, plan in plans.items()}}
    if case.warmup:
        identity['warmup'] = normalized(case.warmup)
    if profile.memory:
        identity['memory'] = True
    return digest(identity)


def fingerprint(profile, case, plans):
    size = profile.rows * (profile.payload + 64)
    identity = {
        'version': VERSION, 'family': family_fingerprint(profile, case, plans),
        'key': profile.key, 'payload': profile.payload, 'rows': profile.rows,
        'cache_pressure': max(0, math.ceil(math.log2(size/(profile.cache_kib*1024)))),
        'target_regime': 'hot' if profile.skew and profile.target == 0 else 'ordinary',
        'skew': profile.skew, 'groups': profile.groups, 'stride': profile.stride,
        'range_fraction': round(profile.width/profile.rows, 4), 'lookups': profile.lookups,
    }
    return digest(identity)


class Search:
    def __init__(self, seed, history=None, issues=()):
        self.rng = random.Random(f'hotspot-search-v{VERSION}:{seed}')
        self.history = Path(history) if history else None
        self.state = {'version': VERSION, 'visits': {}, 'shapes': {}, 'plans': {}, 'corpus': []}
        if self.history and self.history.exists():
            state = json.loads(self.history.read_text())
            if state.get('version') == VERSION:
                if not all(valid_recipe(x['recipe']) and valid_profile(x['profile']) for x in state['corpus']):
                    raise ValueError('invalid exploration corpus')
                self.state = state
                self.state.setdefault('shapes', {})
        for issue in issues:
            for bundle in issue.get('bundles', []):
                recipe = bundle.get('case', {}).get('recipe', {})
                if valid_recipe(recipe) and valid_profile(bundle.get('profile', {})):
                    self.remember(recipe, bundle['profile'])

    def remember(self, recipe, profile):
        item = {'recipe': recipe, 'profile': profile}
        corpus = self.state['corpus']
        if item in corpus:
            corpus.remove(item)
        corpus.append(item)
        del corpus[:-128]

    def choose(self, profile, operators=None):
        from performance_hotspot_fuzzer import Profile
        mode = self.rng.randrange(4)
        if mode == 0 and self.state['corpus']:
            parent = self.rng.choice(self.state['corpus'])
            recipe = dict(parent['recipe'])
            if self.rng.choice([False, True]):
                profile = Profile(**parent['profile'])
            key = self.rng.choice(list(CHOICES))
            recipe[key] = self.rng.choice([v for v in CHOICES[key] if v != recipe[key]])
            origin = 'mutation'
        elif mode == 1:
            candidates = [fresh(self.rng) for _ in range(16)]
            recipe = min(candidates, key=lambda r: (self.state['shapes'].get(digest(r), 0),
                         sum(self.state['visits'].get(f'{k}:{v}', 0) for k, v in r.items())))
            origin = 'underexplored'
        else:
            recipe, origin = fresh(self.rng), 'fresh'
        if operators and recipe['operator'] not in operators:
            recipe['operator'] = self.rng.choice(operators)
        return profile, recipe, origin

    def observe(self, profile, case, record):
        from dataclasses import asdict
        if not case.recipe:
            return
        for k, v in case.recipe.items():
            name = f'{k}:{v}'
            self.state['visits'][name] = self.state['visits'].get(name, 0) + 1
        shape = digest(case.recipe)
        self.state['shapes'][shape] = self.state['shapes'].get(shape, 0) + 1
        if len(self.state['shapes']) > 4096:
            self.state['shapes'].pop(next(iter(self.state['shapes'])))
        plan = digest(record.get('plans', {}))
        novel = plan not in self.state['plans']
        self.state['plans'][plan] = self.state['plans'].get(plan, 0) + 1
        if novel or record.get('confirmed') or (record.get('ratio') or 0) >= 2:
            self.remember(case.recipe, asdict(profile))
        if len(self.state['plans']) > 2048:
            self.state['plans'].pop(next(iter(self.state['plans'])))
        if self.history:
            self.history.parent.mkdir(parents=True, exist_ok=True)
            temp = self.history.with_suffix('.tmp')
            temp.write_text(json.dumps(self.state, sort_keys=True)+'\n')
            temp.replace(self.history)

    def specs(self, seed, nightly_seeds=False, operators=None):
        from performance_hotspot_fuzzer import profile_for
        index = 0
        if nightly_seeds:
            from performance_hotspot_seeds import specs
            for profile, cases, setup in specs():
                yield index, profile, cases, setup, 'retired'
                index += 1
        while True:
            profile, recipe, origin = self.choose(profile_for(seed, index), operators)
            cases = [generated_case(profile, recipe)]
            for number in range(1, 4):
                variant = fresh(self.rng)
                variant['indexes'] = recipe['indexes']
                if operators and variant['operator'] not in operators:
                    variant['operator'] = self.rng.choice(operators)
                cases.append(generated_case(profile, variant, number))
            yield index, profile, cases, setup_sql(profile, recipe), origin
            index += 1
