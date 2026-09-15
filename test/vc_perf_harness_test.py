import os
from pathlib import Path
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parent
ERROR = 'Error near line 5: Merge has 3 conflict(s). Resolve and then commit with dolt_commit.\n'
CONFLICTS = 'VC_PERF_CONFLICTS|3\nVC_PERF_DONE\n'
RESOLVED = 'VC_PERF_CONFLICTS|3\nVC_PERF_RESOLVE|0\nVC_PERF_RESOLVED|0|0\nVC_PERF_DONE\n'


def main():
    cases = [
        ('conflicts', 'conflicts', 1, CONFLICTS, ERROR, True),
        ('resolved', 'resolved', 1, RESOLVED, ERROR, True),
        ('runtime_error_prefix', 'resolved', 1, RESOLVED,
         ERROR.replace('Error near', 'Runtime error near').rstrip() + ' (1)\n', True),
        ('crlf', 'resolved', 1, RESOLVED.replace('\n', '\r\n'),
         ERROR.replace('\n', '\r\n'), True),
        ('skipped_count', 'conflicts', 1, '', ERROR, False),
        ('skipped_resolver', 'resolved', 1, CONFLICTS, ERROR, False),
        ('broken_resolver', 'resolved', 1, CONFLICTS,
         ERROR + 'Error near line 7: no such function: dolt_conflicts_resolve\n', False),
        ('no_op_resolver', 'resolved', 1, RESOLVED.replace('RESOLVED|0|0', 'RESOLVED|3|1'), ERROR, False),
        ('failed_resolver_result', 'resolved', 1, RESOLVED.replace('RESOLVE|0', 'RESOLVE|1'), ERROR, False),
        ('unexpected_error', 'resolved', 1, RESOLVED, ERROR + 'disk I/O error\n', False),
        ('wrong_error', 'resolved', 1, RESOLVED, 'database is locked\n', False),
        ('wrong_error_count', 'resolved', 1, RESOLVED, ERROR.replace('3 conflict', '2 conflict'), False),
        ('missing_merge_error', 'resolved', 0, RESOLVED, '', False),
        ('wrong_exit_status', 'resolved', 2, RESOLVED, ERROR, False),
        ('crash', 'resolved', 139, RESOLVED, ERROR, False),
        ('wrong_conflict_count', 'resolved', 1, RESOLVED.replace('CONFLICTS|3', 'CONFLICTS|2'), ERROR, False),
        ('remaining_conflict_table', 'resolved', 1, RESOLVED.replace('RESOLVED|0|0', 'RESOLVED|0|1'), ERROR, False),
        ('missing_completion', 'resolved', 1, RESOLVED.replace('VC_PERF_DONE\n', ''), ERROR, False),
        ('extra_output', 'resolved', 1, RESOLVED + 'unexpected\n', ERROR, False),
        ('ordinary_success', '0', 0, '1\n', '', True),
        ('ordinary_failure', '0', 1, '', 'failure\n', False),
        ('unknown_expectation', 'unknown', 1, CONFLICTS, ERROR, False),
    ]
    with tempfile.TemporaryDirectory(prefix='vc-perf-harness-') as work:
        root = Path(work)
        engine = root / 'engine'
        engine.write_text('''#!/usr/bin/env bash
cat > "$WORK/script.sql"
printf '%s' "$OUTPUT"
printf '%s' "$ERROR_OUTPUT" >&2
exit "$EXIT_CODE"
''')
        engine.chmod(0o755)
        script = '''set -euo pipefail
source "$HELPER"
MERGE_CHANGE_ROWS=3
us_now() { echo 1000; }
if [ "$MODE" = 0 ]; then sql='SELECT 1;'; else sql=$(vc_perf_conflict_sql "$MODE"); fi
time_sql "$ENGINE" "$WORK/db" "$sql" "$WORK/out" "$WORK/err" "$MODE"
'''
        for name, mode, rc, output, errors, success in cases:
            result = subprocess.run(['bash', '-c', script], text=True, capture_output=True,
                                    env=dict(os.environ, HELPER=str(ROOT / 'lib/vc_perf_benchmark.sh'),
                                             ENGINE=str(engine), WORK=work, MODE=mode,
                                             EXIT_CODE=str(rc), OUTPUT=output, ERROR_OUTPUT=errors),
                                    timeout=10)
            assert (result.returncode == 0) == success, (name, result)
            assert result.stdout == ('0\n' if success else ''), (name, result.stdout)
            if mode in ('conflicts', 'resolved'):
                lines = (root / 'script.sql').read_text().splitlines()
                assert "SELECT dolt_merge('feat');" in lines, (name, lines)
                assert 'ROLLBACK;' in lines, (name, lines)
                assert lines[-1] == "SELECT 'VC_PERF_DONE';", (name, lines)
                if mode == 'resolved':
                    assert "SELECT 'VC_PERF_RESOLVE|' || dolt_conflicts_resolve('--ours','t');" in lines
        print(f'VC performance harness: {len(cases)} checks passed')


if __name__ == '__main__':
    main()
