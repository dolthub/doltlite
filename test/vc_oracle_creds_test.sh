#!/bin/bash
set -euo pipefail
DOLTLITE="${1:-build/doltlite}"
DOLT="${2:-dolt}"
DOLTLITE="$(cd "$(dirname "$DOLTLITE")" && pwd)/$(basename "$DOLTLITE")"
DOLT="$(command -v "$DOLT")"
python3 - "$DOLTLITE" "$DOLT" <<'PY'
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

lite, dolt = sys.argv[1:]
passed = failed = 0
with tempfile.TemporaryDirectory(prefix='vc-oracle-creds-') as tmp:
    root = Path(tmp)
    def env(name):
        home = root / name
        home.mkdir(exist_ok=True)
        e = dict(os.environ, HOME=str(home), DOLT_ROOT_PATH=str(home),
                 DOLTLITE_CREDS_DIR=str(home / '.doltlite' / 'creds'),
                 DOLT_DISABLE_UPDATE_CHECK='1', NO_COLOR='1')
        e.pop('DOLTLITE_CREDS_KID', None)
        return e
    dt, dl, fixture = env('dolt'), env('lite'), env('fixture')
    def run(binary, args, e):
        r = subprocess.run([binary, *args], env=e, capture_output=True, text=True,
                           timeout=30)
        if r.returncode < 0:
            raise RuntimeError(f'{binary} died from signal {-r.returncode}')
        return r
    def dtcmd(*args):
        return run(dolt, ['creds', *args], dt)
    def sql(query):
        return run(lite, ['-batch', '-bail', ':memory:', query], dl)
    def require(r):
        if r.returncode:
            raise RuntimeError(r.stderr)
        return r.stdout.strip()
    def rows_dt():
        out = require(dtcmd('ls', '-v'))
        rows = []
        for line in out.splitlines():
            m = re.fullmatch(r'([* ]) ([0-9a-v]{52})\s+([0-9a-v]{45})', line)
            if m:
                rows.append((m[3], m[2], int(m[1] == '*')))
        return sorted(rows)
    def rows_dl():
        out = require(sql('SELECT kid, public_key, active FROM dolt_creds_ls() ORDER BY kid;'))
        return [(k, p, int(a)) for k, p, a in
                (line.split('|') for line in out.splitlines())] if out else []
    def compare(name):
        global passed, failed
        try:
            expected, actual = rows_dt(), rows_dl()
            if actual != expected:
                raise AssertionError(f'expected {expected!r}, got {actual!r}')
            passed += 1
            print('  PASS:', name)
        except Exception as ex:
            failed += 1
            print('  FAIL:', name, ex)
    def result(name, a, b, success):
        global passed, failed
        ok = (a.returncode == 0) == success and (b.returncode == 0) == success
        if ok:
            passed += 1
            print('  PASS:', name)
        else:
            failed += 1
            print('  FAIL:', name, 'Dolt rc=', a.returncode, 'DoltLite rc=', b.returncode,
                  b.stderr.strip())

    print('=== Credential CLI / SQL oracle ===')
    compare('empty list')
    paths = []
    for _ in range(2):
        require(run(dolt, ['creds', 'new'], fixture))
    paths = sorted((Path(fixture['HOME']) / '.dolt' / 'creds').glob('*.jwk'))
    for i, path in enumerate(paths):
        result(f'import key {i+1}', dtcmd('import', '--no-profile', str(path)),
               sql(f"SELECT dolt_creds_import('--no-profile', '{path}');"), True)
        compare(f'import {i+1} preserves active selection')
    kids = [p.stem for p in paths]
    pub = {k: p for k, p, _ in rows_dt()}
    for value in [kids[1], pub[kids[0]], pub[kids[1]], kids[0]]:
        result('use key ID or public key', dtcmd('use', value),
               sql(f"SELECT dolt_creds_use('{value}');"), True)
        compare('selection survives process restart')
    result('duplicate import', dtcmd('import', '--no-profile', str(paths[1])),
           sql(f"SELECT dolt_creds_import('--no-profile', '{paths[1]}');"), True)
    compare('duplicate import preserves active key')
    for value in ['../victim', 'invalid', '0' * 45]:
        result('invalid or missing key is rejected', dtcmd('use', value),
               sql(f"SELECT dolt_creds_use('{value}');"), False)
        compare('failed selection preserves previous key')
    result('remove by public key', dtcmd('rm', pub[kids[1]]),
           sql(f"SELECT dolt_creds_rm('{pub[kids[1]]}');"), True)
    compare('remaining active key survives removal')
    result('remove active key', dtcmd('rm', kids[0]),
           sql(f"SELECT dolt_creds_rm('{kids[0]}');"), True)
    compare('removed active key is not listed')
    result('check missing active key fails locally', dtcmd('check'),
           sql('SELECT dolt_creds_check();'), False)
    for e, binary, args in [(dt, dolt, ['creds', 'new']),
                             (dl, lite, [':memory:', 'SELECT dolt_creds_new();'])]:
        require(run(binary, args, e))
    try:
        expected, actual = rows_dt(), rows_dl()
        if len(expected) != 1 or len(actual) != 1 or expected[0][2] or actual[0][2]:
            raise AssertionError('new selected a key over a configured missing key')
        passed += 1
        print('  PASS: new does not replace a configured missing key')
    except Exception as ex:
        failed += 1
        print('  FAIL: new does not replace a configured missing key', ex)
    for name, e, binary, args in [('dolt-new', env('dolt-new'), dolt, ['creds', 'new']),
                                   ('lite-new', env('lite-new'), lite,
                                    [':memory:', 'SELECT dolt_creds_new();'])]:
        try:
            require(run(binary, args, e))
            if binary == dolt:
                active_before = require(run(dolt, ['config', '--global', '--get', 'user.creds'], e))
            else:
                active_before = require(run(lite, [':memory:',
                    'SELECT kid FROM dolt_creds_ls() WHERE active = true;'], e))
            require(run(binary, args, e))
            if binary == dolt:
                active_after = require(run(dolt, ['config', '--global', '--get', 'user.creds'], e))
            else:
                active_after = require(run(lite, [':memory:',
                    'SELECT kid FROM dolt_creds_ls() WHERE active = true;'], e))
            if not active_before or active_before != active_after:
                failed += 1
                print('  FAIL:', name, 'first key selection changed')
            else:
                passed += 1
                print('  PASS:', name, 'first key remains selected after second new')
        except Exception as ex:
            failed += 1
            print('  FAIL:', name, ex)

print(f'Results: {passed} passed, {failed} failed')
sys.exit(bool(failed))
PY
