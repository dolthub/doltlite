# Remotes

Git-like push / fetch / pull / clone between databases.

## Filesystem Remotes

```sql
SELECT dolt_remote('add', 'origin', 'file:///path/to/remote.doltlite');
SELECT dolt_push('origin', 'main');
SELECT dolt_push('origin', 'v1.0');       -- push one tag
SELECT dolt_push('origin', '--tags');     -- push all tags
SELECT dolt_clone('file:///path/to/source.doltlite');
SELECT dolt_clone('--lazy', 'file:///path/to/source.doltlite');
SELECT dolt_clone('--lazy', '--revision', 'release~1',
                  'file:///path/to/source.doltlite');
SELECT dolt_fetch('origin', 'main');
SELECT dolt_pull('origin', 'main');   -- fetch, then fast-forward or merge
SELECT * FROM dolt_remotes;
```

A push, including a force push, is refused if the target branch has
uncommitted working or staged changes. Commit or reset the target database
before retrying; a clean working set does not block a push.

A lazy clone installs refs and records `origin` without copying the reachable
chunk graph. It enables origin-backed reads on its current connection. To
reopen the clone in another process, opt in before the B-tree opens:

```sh
doltlite 'file:/path/to/lazy.db?lazy_origin=1'
```

Missing chunks are fetched and cached as queries need them. Fetch and
fast-forward pull remain refs-only while the connection is origin-enabled. A
divergent lazy pull is refused until the store is fully materialized.
Opening without `lazy_origin=1` leaves cached chunks available, but an
uncached miss fails with a hash-named error instead of contacting `origin`.
The optional `--revision` value resolves a branch, tag, commit hash, or
ancestor expression once during the lazy clone. A branch selects its working
set; every other revision opens a read-only detached snapshot at that commit.

`dolt_pull` fetches the named remote branch, then integrates it into the
current local branch. It fast-forwards when the current tip is an ancestor of
the remote tip and otherwise three-way merges like `dolt_merge`. The branch
name selects the remote ref; a same-named non-current local branch is neither
created nor moved. Fetch and pull also install remote tags whose commits have
been fetched, replacing same-named local tags when the remote value differs.

## HTTP Remotes

Same ops as filesystem remotes; the URL includes the database name:

```sql
SELECT dolt_remote('add', 'origin', 'http://myserver:8080/mydb.db');
SELECT dolt_push('origin', 'main');
SELECT dolt_clone('http://myserver:8080/mydb.db');
SELECT dolt_clone('--lazy', 'http://myserver:8080/mydb.db');
```
