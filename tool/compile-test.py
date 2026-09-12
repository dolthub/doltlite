#!/usr/bin/env python3
from pathlib import Path
import subprocess
import sys


def build(argv):
    groups = {name: [] for name in ('--cc', '--cflags', '--sources', '--ldflags', '--output')}
    group = None
    for arg in argv:
        if arg in groups:
            group = arg
        elif group is None:
            raise ValueError(f'unexpected argument: {arg}')
        else:
            groups[group].append(arg)
    if not groups['--cc'] or not groups['--sources'] or len(groups['--output']) != 1:
        raise ValueError('compiler, sources and one output are required')

    output = groups['--output'][0]
    directory = Path('.test-objects') / Path(output).name
    directory.mkdir(parents=True, exist_ok=True)
    command = groups['--cc'] + groups['--cflags']
    objects = []
    for index, source in enumerate(groups['--sources']):
        if not source.endswith('.c'):
            objects.append(source)
            continue
        obj = str(directory / f'{index}.o')
        subprocess.run(command + ['-c', source, '-o', obj], check=True)
        objects.append(obj)
    subprocess.run(command + ['-o', output] + objects + groups['--ldflags'], check=True)


if __name__ == '__main__':
    try:
        build(sys.argv[1:])
    except subprocess.CalledProcessError as error:
        sys.exit(error.returncode if error.returncode > 0 else 128 - error.returncode)
    except (ValueError, OSError) as error:
        sys.exit(str(error))
