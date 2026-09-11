import json
import os
from pathlib import Path
import subprocess
import tempfile

helper = Path(__file__).with_name('parallel-compile.sh').resolve()
worker = '''import json, os, sys, time
number = int(sys.argv[1])
def event(kind):
    with open(os.environ['EVENT_LOG'], 'a') as log:
        log.write(json.dumps([kind, number]) + '\\n')
event('start')
open('started-' + str(number), 'w').close()
if number < int(os.environ['WORKER_JOBS']):
    deadline = time.monotonic() + 10
    while sum(name.startswith('started-') for name in os.listdir('.')) < int(os.environ['WORKER_JOBS']):
        if time.monotonic() > deadline:
            raise RuntimeError('compiler workers did not start concurrently')
        time.sleep(0.01)
time.sleep(0.05)
print('compiler output', number)
event('finish')
sys.exit(42 if number == int(os.environ['FAIL_TASK']) else 0)
'''

for jobs in (1, 2, 3):
    for failed in (-1, 0, 1, 4):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'worker.py').write_text(worker)
            script = '''set -euo pipefail
source "$HELPER"
ci_compile_init "$JOBS"
for i in 0 1 2 3 4; do ci_compile python3 worker.py "$i"; done
ci_compile_wait
printf packaged > package
'''
            result = subprocess.run(['bash'], input=script, text=True, cwd=root,
                                    env=dict(os.environ, HELPER=str(helper), JOBS=str(jobs),
                                             EVENT_LOG=str(root / 'events'), FAIL_TASK=str(failed), WORKER_JOBS=str(jobs)),
                                    capture_output=True, timeout=30)
            assert result.returncode == (0 if failed == -1 else 42), result
            assert (root / 'package').exists() == (failed == -1), result
            active = set()
            finished = set()
            peak = 0
            for kind, number in map(json.loads, (root / 'events').read_text().splitlines()):
                if kind == 'start':
                    active.add(number)
                    peak = max(peak, len(active))
                else:
                    active.remove(number)
                    finished.add(number)
                    assert f'compiler output {number}' in result.stdout, result
            assert not active and peak <= jobs, (active, peak, result)
            if failed == -1:
                assert finished == set(range(5)) and peak == jobs, (finished, peak)

print('Parallel compiler scheduling: 12 bounded concurrency/failure checks passed')
