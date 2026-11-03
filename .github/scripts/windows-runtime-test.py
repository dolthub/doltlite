import os
from pathlib import Path
import subprocess
import tempfile

repo = Path(__file__).resolve().parents[2]
checks = 0
with tempfile.TemporaryDirectory() as tmp:
    root = Path(tmp)
    dumper = root / 'objdump with spaces'
    dumper.write_text('#!/bin/sh\nprintf "%s\\n" "$IMPORTS"\nexit "${DUMP_RC:-0}"\n')
    dumper.chmod(0o755)

    def inspect(imports, *args, **env):
        return subprocess.run(
            ['bash', str(repo / 'test/windows_runtime_dependencies.sh'), *args],
            env=dict(os.environ, OBJDUMP=str(dumper), IMPORTS=imports, **env),
            capture_output=True, text=True, timeout=10)

    for library in ['KERNEL32.dll', 'msvcrt.dll', 'bcrypt.dll', 'CRYPT32.dll',
                    'WS2_32.dll', 'api-ms-win-core-synch-l1-2-0.dll']:
        result = inspect(f'\tDLL Name: {library}', 'library with spaces.dll')
        assert result.returncode == 0, result
        checks += 1
    for library in ['libgcc_s_seh-1.dll', 'LIBGCC_S_SJLJ-1.DLL', 'libwinpthread-1.dll',
                    'libstdc++-6.dll', 'libssp-0.dll', 'zlib1.dll']:
        result = inspect(f'\tDLL Name: KERNEL32.dll\n\tDLL Name: {library}', 'bad.dll')
        assert result.returncode != 0 and library in result.stdout, result
        checks += 1
    for imports, env in [('', {}), ('not a PE file', {}),
                         ('DLL Name: KERNEL32.dll', {'DUMP_RC': '42'})]:
        assert inspect(imports, 'bad.dll', **env).returncode != 0
        checks += 1
    assert inspect('DLL Name: KERNEL32.dll').returncode == 2
    checks += 1

    dumper.write_text('''#!/bin/sh
case "$2" in
  *bad.dll) echo 'DLL Name: libwinpthread-1.dll';;
  *) echo 'DLL Name: KERNEL32.dll';;
esac
''')
    for artifacts in [('good.exe', 'bad.dll'), ('bad.dll', 'good.exe')]:
        assert inspect('', *artifacts).returncode != 0
        checks += 1

    for suffix in ['.dll', '.so', '.dylib']:
        result = subprocess.run([
            'make', '-f', str(repo / 'main.mk'), '-n', f'libdoltlite{suffix}',
            f'TOP={repo}', f'T.dll={suffix}', 'LIBOBJS0=',
            'LDFLAGS.libsqlite3=', 'LDFLAGS.libdoltlite.os-specific='],
            cwd=root, capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, result
        link = next(line for line in result.stdout.splitlines() if ' -shared ' in line)
        assert ('-static' in link.split()) == (suffix == '.dll'), link
        checks += 1

for path in ['.github/workflows/release.yml', '.github/actions/checked-probes/action.yml']:
    workflow = (repo / path).read_text()
    assert 'windows_runtime_dependencies.sh' in workflow
    assert 'windows_runtime_smoke.sh build' in workflow
    checks += 1
print(f'Windows runtime dependency and link checks: {checks} passed')
