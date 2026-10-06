import os
from pathlib import Path
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parent


def main():
    cases = [
        ('fast', '0.001', '0.010', 200, 0, 0, True),
        ('post_query_delay', '0.001', '0.010', 200, 0, 0.4, True),
        ('single_outlier', '0.050,0.001,0.001', '0.010', 200, 0, 0, True),
        ('repeated_slow', '0.050,0.050,0.001', '0.010', 200, 0, 0, False),
        ('slow', '0.010', '0.001', 200, 0, 0, False),
        ('small_slow', '0.001', '0.0001', 200, 0, 0, False),
        ('missing', '0.001', '0.010', 0, 0, 0, False),
        ('truncated', '0.001', '0.010', 199, 0, 0, False),
        ('extra', '0.001', '0.010', 201, 0, 0, False),
        ('malformed', 'bad', '0.010', 200, 0, 0, False),
        ('zero', '0.000', '0.000', 200, 0, 0, False),
        ('error', '0.001', '0.010', 200, 1, 0, False),
        ('integrity_error', '0.001', '0.010', 200, 125, 0, False),
        ('signal', '0.001', '0.010', 200, 139, 0, False),
    ]
    with tempfile.TemporaryDirectory(prefix='pushdown-timing-') as work:
        root = Path(work)
        engine = root / 'engine'
        engine.write_text('''#!/usr/bin/env python3
import os, sys, time
from pathlib import Path
sql=sys.stdin.read().splitlines()
assert sys.argv[1:]==['-bail', 'db']
assert sql[:3]==['SELECT count(*) FROM dolt_history_t WHERE id=2000;',
                 'SELECT count(*) FROM dolt_history_t;', '.timer on']
assert sql[3:]==sql[:2]*100
trace=Path(os.environ['TRACE'])
trial=int(trace.read_text()) if trace.exists() else 0
trace.write_text(str(trial+1))
for i in range(int(os.environ['TIMINGS'])):
    durations=os.environ['POINT' if i%2==0 else 'FULL'].split(',')
    duration=durations[min(trial,len(durations)-1)]
    print(f'Run Time: real {duration} user 0.000000 sys 0.000000')
time.sleep(float(os.environ['DELAY']))
sys.exit(int(os.environ['RC']))
''')
        engine.chmod(0o755)
        script = '''source "$HELPER"
history_pushdown_time "$ENGINE" db 2000
'''
        for name, point, full, count, rc, delay, success in cases:
            result = subprocess.run(['bash', '-c', script], text=True,
                                    capture_output=True, timeout=10,
                                    env=dict(os.environ,
                                             HELPER=str(ROOT / 'lib/dolt_table_pushdown_timing.sh'),
                                             ENGINE=str(engine), POINT=point, FULL=full,
                                             TIMINGS=str(count), RC=str(rc), DELAY=str(delay),
                                             TRACE=str(root / name)))
            assert (result.returncode == 0) == success, (name, result)
            if success:
                assert 'constrained=100ms unconstrained=1000ms' in result.stdout, (name, result)
        print(f'History pushdown timing: {len(cases)} checks passed')


if __name__ == '__main__':
    main()
