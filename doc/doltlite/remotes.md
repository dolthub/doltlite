# Remotes

Git-like push / fetch / pull / clone between databases.

## Filesystem Remotes

```sql
SELECT dolt_remote('add', 'origin', 'file:///path/to/remote.doltlite');
SELECT dolt_push('origin', 'main');
SELECT dolt_push('origin', 'v1.0');       -- push one tag
SELECT dolt_push('origin', '--tags');     -- push all tags
SELECT dolt_push('--all', 'origin');      -- every local branch, not tags
SELECT dolt_fetch('origin', 'main');
SELECT dolt_fetch('--prune', 'origin');   -- drop tracking refs the remote deleted
SELECT dolt_pull('origin', 'main');   -- fetch, then fast-forward or merge
SELECT dolt_pull('--prune', 'origin', 'main'); -- prune is allowed with a branch
SELECT * FROM dolt_remotes;
```

A `file://` URL is stored with an absolute path. `file://rel.db` is recorded
as `file:///<directory>/rel.db`, and `dolt_clone` records `origin` the same
way, so push, fetch, and pull open that file from any working directory.

Clone into an empty database:

```sql
SELECT dolt_clone('file:///path/to/source.doltlite');
```

```sql
SELECT dolt_clone('--remote', 'upstream', '--single-branch',
                  'file:///path/to/source.doltlite');
```

```sql
SELECT dolt_clone('--lazy', '--revision', 'main~1',
                  'file:///path/to/source.doltlite');
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

`dolt_push('--all', remote)` pushes every local branch. It does not push
tags, and the remote name is required. `dolt_fetch('--prune', remote)` drops
tracking refs for branches the remote has deleted, and is refused when a
branch or refspec is also given. `dolt_pull('--prune')` does that same prune
and is allowed together with a branch.

`dolt_clone('--remote', name, url)` records that remote name instead of
`origin`. `dolt_clone('--single-branch', url)` tracks and checks out only the
default branch, or the `--branch` name when that branch exists.

`dolt_pull()` and `dolt_fetch()` with no remote name use `origin` when that
remote exists. When it does not, and exactly one remote is configured, they
use that remote. `dolt_pull()` and `dolt_pull(remote)` fetch every branch on
that remote, then integrate the current branch's upstream into the current
local branch.
`dolt_pull(remote, branch)` fetches only that named branch. Either form
fast-forwards when the current tip is an ancestor of the remote tip and
otherwise three-way merges like `dolt_merge`. A merge commit is recorded as
`Merge branch '<branch>' of <url> into <local>`, using the remote's stored
URL with no quotes around it. The branch name selects the
remote ref; a same-named non-current local branch is neither created nor
moved. Fetch and pull also install remote tags whose commits have been
fetched, replacing same-named local tags when the remote value differs.

## HTTP Remotes

Same ops as filesystem remotes; the URL includes the database name:

```sql
SELECT dolt_remote('add', 'backup', 'http://myserver:8080/mydb.db');
SELECT dolt_push('backup', 'main');
```

```sql
SELECT dolt_clone('--lazy', 'http://myserver:8080/mydb.db');
```
