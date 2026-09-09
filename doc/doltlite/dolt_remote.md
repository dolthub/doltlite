# Remotes: dolt_remote, dolt_push, dolt_fetch, dolt_pull, dolt_clone

Sync commits and refs between databases over the filesystem or HTTP. Dolt:
[dolt_remote](https://docs.dolthub.com/sql-reference/version-control/dolt-sql-procedures#dolt_remote),
[dolt_push](https://docs.dolthub.com/sql-reference/version-control/dolt-sql-procedures#dolt_push),
[dolt_clone](https://docs.dolthub.com/sql-reference/version-control/dolt-sql-procedures#dolt_clone).

## Synopsis

```sql
SELECT dolt_clone('http://host:8080/mydb.db');   -- into an empty database; records origin
SELECT dolt_remote('add', 'backup', 'file:///data/remote.db');
SELECT dolt_remote('remove', 'backup');
SELECT dolt_push('origin', 'main');
SELECT dolt_push('origin', 'main', '--force');
SELECT dolt_push('origin', 'v1.0');              -- one tag
SELECT dolt_push('origin', '--tags');
SELECT dolt_fetch('origin');                     -- all branches
SELECT dolt_fetch('origin', 'main');
SELECT dolt_pull('origin', 'main');
SELECT * FROM dolt_remotes;
SELECT * FROM dolt_remote_branches;
```

```sql
SELECT dolt_clone('--lazy', '--revision', 'v1.0', 'file:///data/src.db');
```

## Functions

| Function | Arguments | Notes |
|---|---|---|
| `dolt_remote` | `'add', name, url` or `'remove', name` | `file://` or `http(s)://...` URL naming the database file |
| `dolt_push` | `remote, branch [, '--force']` or `remote, tag` or `remote, '--tags'` | Non-fast-forward is refused without `--force` |
| `dolt_fetch` | `remote [, branch]` | Updates `remotes/<remote>/<branch>` tracking refs and tags |
| `dolt_pull` | `remote, branch` | Fetch, then fast-forward or three-way merge into the current branch |
| `dolt_clone` | `['--lazy'] ['--revision', rev] url` | Only into an empty database; records `origin` |

All return `0` on success. `dolt_pull` behaves like [dolt_merge](dolt_merge.md)
when it cannot fast-forward, conflicts included.

| Error | Cause |
|---|---|
| `usage: dolt_remote(action, name [, url])`, `url required for add`, `remote already exists`, `unknown action: use 'add' or 'remove'` | `dolt_remote` arguments |
| `remote not found` | unknown remote name |
| `push failed: branch or tag not found` | local ref does not exist |
| `not a fast-forward of the remote branch (use force to overwrite)` | remote moved; add `--force` |
| `fetch failed: branch not found on remote` | `dolt_fetch`/`dolt_pull` of a missing branch |
| `database is not empty — clone into a fresh database` | `dolt_clone` into a database with tables or commits |
| `clone failed` | source unreachable or not a DoltLite database |
| `DoltLite remotes are disabled in this build` | built with `DOLTLITE_ENABLE_REMOTES=0` |

## Behaviour

- Remotes carry commits, tags, and branch refs. Working sets never travel:
  cloned branches start clean, and a push is refused while the **target**
  database's branch has uncommitted changes.
- A lazy clone installs refs without copying chunks and fetches them on
  demand from `origin`. Reopen it with `?lazy_origin=1` to keep that
  behaviour in a new process; `--revision` picks the branch to check out, or a
  read-only snapshot for a tag or commit. A divergent pull on a lazy clone is
  refused until the store is fully materialized.
- Fetch and pull install remote tags whose commits were fetched, replacing a
  same-named local tag when the remote value differs.
- Pushes to an HTTP remote are validated under the server's lock, so a stale
  push is rejected rather than overwriting a peer's ref.

## Tables

| Table | Columns |
|---|---|
| `dolt_remotes` | `name`, `url`, `fetch_specs`, `params` |
| `dolt_remote_branches` | `name` (`remotes/origin/main`), `hash`, `latest_committer`, `latest_committer_email`, `latest_commit_date`, `latest_commit_message` |

## See also

[remotes.md](remotes.md) for URL forms and lazy clones,
[remotesrv.md](remotesrv.md) for serving, [dolt_creds.md](dolt_creds.md) for
authentication, `test/vc_oracle_remotes_test.sh`,
`test/vc_oracle_fetch_pull_convergence_test.sh`.
