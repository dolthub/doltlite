# Using existing SQLite databases

Header-based auto-detect: stock SQLite files use the original B-tree engine;
everything else is prolly. Typical hybrid: versioned tables on the DoltLite
main DB, high-write operational tables on an attached stock SQLite file. Version
control applies only to the DoltLite-format main database.

```sql
ATTACH DATABASE '/path/to/events.sqlite' AS ops;
SELECT * FROM ops.events WHERE type='click';
SELECT * FROM threads;   -- main DB, no prefix
SELECT t.title, e.type
  FROM threads t
  JOIN ops.events e ON t.id = e.thread_id;

-- Migrate either direction
INSERT INTO threads SELECT * FROM ops.threads;
INSERT INTO ops.archive SELECT * FROM threads WHERE archived=1;
CREATE TABLE local_events AS SELECT * FROM ops.events;

DETACH DATABASE ops;
```

Auto-detect reads an existing file's header, so it cannot classify a file that
does not exist yet: a database created by DoltLite is DoltLite-format. To create
a stock SQLite file instead, open it with `doltlite_engine=sqlite`:

```
doltlite 'file:/path/to/new.sqlite?doltlite_engine=sqlite'
```

The parameter selects the engine for a database being created and is ignored
once the file has content, so it can never reinterpret an existing database.
`.backup` and `VACUUM INTO` apply it for you when the source is a stock file, so
their output is a stock file too.

`VACUUM` on a stock database rewrites pages as SQLite does; on a DoltLite
database it garbage-collects unreachable chunks. `.backup`/`.restore` and
`sqlite3_backup_*` work within either format, but not between them — there is no
defined conversion, so a mixed pair is refused rather than half-copied.
