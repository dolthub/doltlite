import sqlite3
import sys

with sqlite3.connect(sys.argv[1]) as db:
    rows = db.execute('''
        SELECT displayname, state, starttime, endtime FROM jobs
        WHERE displaytype='bld' ORDER BY starttime
    ''').fetchall()
if not rows:
    sys.exit('No development build timings recorded')
start = min((row[2] for row in rows if row[2] is not None), default=0)
print('| Target | State | Start | Duration |')
print('| --- | --- | ---: | ---: |')
for name, state, begin, end in rows:
    offset = f'{(begin-start)/1000:.1f}s' if begin is not None else '—'
    duration = f'{(end-begin)/1000:.1f}s' if begin is not None and end is not None else '—'
    print(f'| {name} | {state} | {offset} | {duration} |')
