# Logical exports with `dolt_dump`

Export a DoltLite snapshot as SQL or an ordinary SQLite database. Exports
contain table definitions and rows, indexes, views, and triggers. They do not
contain branches, commits, remotes, or the version-control graph. To preserve
that history, use a native DoltLite backup instead.

## SQL interface

```sql
-- SQL text is the default format; an explicit destination is required.
SELECT dolt_dump('--file-name', '/path/to/export.sql');

-- A database that ordinary SQLite can open directly.
SELECT dolt_dump('--file-name', '/path/to/export.sqlite',
                 '--result-format', 'sqlite');

-- Export a committed revision without checking it out.
SELECT dolt_dump('--file-name', '/path/to/release.sqlite',
                 '--result-format', 'sqlite', '--ref', 'HEAD');
```

Success returns integer `0`; failure raises a SQL error. `--result-format`
accepts `sql` (default) or `sqlite`; `-r` is its short form. `--ref` defaults
to `WORKING`, including uncommitted changes visible to this connection, even
inside its SQL transaction. `STAGED`, branch names, tags, commit hashes, and
other [commit references](refs.md) select their corresponding snapshots.
Only the source connection's `main` database is exported; attached databases
and temporary objects are excluded.

The source remains on its current branch with its staging state and SQL
transaction intact. One read snapshot covers all exported tables, so a
concurrent writer cannot produce an export mixing different revisions.

The destination must not already exist, including an empty file or a symlink.
A temporary file beside the destination is completed and synced before it is
published atomically without replacing an existing path. A failed export
removes its temporary file; a process crash may leave a `-dump-<random>` file.
The SQL file-export interface requires a native Unix or Windows filesystem
VFS; custom VFSes and WebAssembly should use the embedded APIs below.

To replay a SQL export with stock SQLite:

```sh
sqlite3 restored.sqlite < export.sql
```

## Embedded C API

Both APIs are declared in `doltlite.h`, exported by `libdoltlite`, and included
in the amalgamation. They run synchronously in process and need no shell or
child process.

```c
int doltlite_dump_sql(
    sqlite3 *source, const char *ref,
    int (*write)(void *context, const void *bytes, int count),
    void *context, char **error);

int doltlite_export_sqlite(
    sqlite3 *source, const char *ref, sqlite3 *destination, char **error);
```

Pass `NULL` for `ref` to select `WORKING`. Both return `SQLITE_OK` on success
or a SQLite error code on failure. An optional error string is allocated by
SQLite and must be freed with `sqlite3_free()`. Errors are also available on
the source connection through `sqlite3_errmsg()`.

For a direct database export, open an **empty ordinary SQLite** destination
using this library:

```c
sqlite3 *destination = NULL;
char *error = NULL;
int rc = sqlite3_open_v2(
    "file:/path/to/new.sqlite?doltlite_engine=sqlite", &destination,
    SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
if (rc == SQLITE_OK) {
    rc = doltlite_export_sqlite(source, NULL, destination, &error);
}
/* Handle rc and error before releasing them. */
sqlite3_free(error);
sqlite3_close(destination);
```

The destination must be outside a transaction and must not be used
concurrently. The API writes inside a transaction, rolls back on failure,
and restores its foreign-key setting and authorizer. The application owns
the destination connection and file, including cleanup or publication;
unlike the SQL filename interface, this API does not manage file paths.
It does not accept a connection from a separately linked SQLite library.
Register required application collations and functions on the destination
before exporting.

For SQL text, the callback receives UTF-8 bytes in chunks of at most 16 KiB.
The bytes are valid only for that callback invocation and need not end at a
statement or character boundary. Return `SQLITE_OK` to continue, or a SQLite
error code to abort. The exporter does not buffer the entire dump or build
a SQL string containing an entire row. For example, a file sink can use:

```c
static int write_sql(void *context, const void *bytes, int count) {
    return fwrite(bytes, 1, count, (FILE *)context) == (size_t)count
        ? SQLITE_OK : SQLITE_IOERR_WRITE;
}
/* FILE *output is opened and owned by the application. */
int rc = doltlite_dump_sql(source, "HEAD", write_sql, output, &error);
```

Discard all SQL already delivered if the call fails. Check the sink's own
flush/close result too. Callbacks must not re-enter either participating
connection. Other connections can continue to read or write the source.

Swift applications can execute the same `SELECT dolt_dump(...)` statement
through their existing binding, or call these C APIs through `CDoltlite`
when using an SDK artifact containing this API. Neither approach launches
the CLI. Older SDK artifacts need an updated DoltLite library before they
can use the new SQL function or C symbols.

## Compatibility and limitations

Exports preserve values, generated-column definitions, AUTOINCREMENT
counters, and the selected revision's `user_version` and `application_id`.
Triggers are installed after copying rows so their side effects are not
repeated. Materialized user-space system tables such as `dolt_docs`,
`dolt_ignore`, and `dolt_tests` are ordinary exported tables; virtual
version-control surfaces and internal operation state are excluded.

SQLite FTS3/4/5 and R-tree tables can be exported when their modules are
available in the destination SQLite build. DoltLite-specific virtual tables,
including `vec1`, and nonlocal table bindings fail explicitly. Views or
triggers that depend on DoltLite functions also fail instead of producing a
silently unusable export. Other missing modules, functions, or collations
are reported with the affected object where available. The SQLite build
that later opens or replays an export must provide the extensions and
application-defined functions/collations required by its schema.

Logical export does not change the DoltLite storage format.
