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
SELECT dolt_push('origin', 'main:feature/x');    -- local main, remote branch feature/x
SELECT dolt_push('origin', 'HEAD:hh');           -- checked-out branch, remote branch hh
SELECT dolt_push('origin', 'v1.0');              -- one tag
SELECT dolt_push('origin', '--tags');
SELECT dolt_push('--all', 'origin');             -- every local branch, not tags
SELECT dolt_fetch('origin');                     -- all branches
SELECT dolt_fetch('origin', 'main');
SELECT dolt_fetch('--prune', 'origin');          -- drop tracking refs the remote deleted
SELECT dolt_fetch('origin', 'refs/heads/*:refs/remotes/origin/*');
SELECT dolt_pull();                              -- every branch and tag, then merge upstream
SELECT dolt_pull('origin');
SELECT dolt_pull('origin', 'main');              -- fetch only main
SELECT dolt_pull('--prune', 'origin', 'main');   -- prune is allowed with a branch
SELECT * FROM dolt_remotes;
SELECT * FROM dolt_remote_branches;
```

```sql
SELECT dolt_remote('add', 'origin', 'file:///data/remote.db');
SELECT dolt_fetch('--prune', 'origin', 'main'); -- error: prune refuses a refspec
SELECT dolt_push('--all', 'origin', 'main'); -- error: refspec
SELECT dolt_push('--all'); -- error: remote required
SELECT dolt_push('--all', '--tags', 'origin'); -- error: tags
```

```sql
SELECT dolt_clone('--lazy', '--revision', 'v1.0', 'file:///data/src.db');
```

```sql
SELECT dolt_clone('--remote', 'upstream', 'http://host:8080/mydb.db');
```

```sql
SELECT dolt_clone('--single-branch', 'http://host:8080/mydb.db');
```

```sql
SELECT dolt_clone('--remote', 'upstream', '--single-branch', '--branch', 'main',
                  'http://host:8080/mydb.db');
```

```sql
SELECT dolt_clone('--remote', 'bad/name', 'http://host:8080/mydb.db'); -- error: invalid name
```

## Functions

| Function | Arguments | Notes |
|---|---|---|
| `dolt_remote` | `'add', name, url` or `'remove', name` | `file://` or `http(s)://...` URL naming the database file. A `file://` path is stored absolute |
| `dolt_push` | `remote, branch [, '--force']` or `remote, 'src:dst'` or `remote, tag` or `remote, '--tags'` or `'--all', remote` | Non-fast-forward is refused without `--force`. `src:dst` pushes revision `src` as remote branch `dst`. `HEAD` as `src` is the checked-out branch's tip; any other `src` is a revision. `--all` pushes every local branch and not tags; the remote name is required |
| `dolt_fetch` | `remote [, branch or refspec] [, '--prune']` | Updates `remotes/<remote>/<branch>` tracking refs and tags. A refspec maps `refs/heads/<pat>` onto `refs/remotes/<name>/<pat>`. `--prune` / `-p` drops tracking refs the remote no longer has, and is refused together with a refspec |
| `dolt_pull` | `[remote [, branch]]` or `'--prune'` | With no branch, fetches every remote branch and tag, then fast-forwards or merges the current branch. With a branch, fetches only that branch. `--prune` / `-p` is allowed with a branch and drops tracking refs the remote no longer has |
| `dolt_clone` | `['--lazy'] ['--revision', rev] ['--branch', name] ['--remote', name] ['--single-branch'] url` | Only into an empty database. `--remote` names the remote (default `origin`). `--single-branch` tracks and checks out one branch |

All return `0` on success. `dolt_pull` behaves like [dolt_merge](dolt_merge.md)
when it cannot fast-forward, conflicts included.

| Error | Cause |
|---|---|
| `usage: dolt_remote(action, name [, url])`, `url required for add`, `remote already exists`, `unknown action: use 'add' or 'remove'` | `dolt_remote` arguments |
| `remote not found` | unknown remote name |
| `push failed: branch or tag not found` | push source does not resolve |
| `--prune option cannot be provided with a ref spec` | `dolt_fetch --prune` was given a branch or refspec. `dolt_pull --prune` still accepts a branch |
| `fatal: --all can't be combined with refspecs` | `dolt_push --all` was given a branch or other refspec |
| `--all cannot be combined with --tags` | `dolt_push --all` was combined with `--tags` |
| `remote required` | `dolt_push --all` or `dolt_push --tags` was missing the remote name |
| `remote name invalid` | `dolt_clone --remote` used an empty name or a name `dolt_remote add` would reject |
| `invalid branch name` | `src:dst` destination fails the branch name rules |
| `unsupported mapping: '<refspec>'` | `src:dst` destination is under `refs/tags/` |
| `invalid fetch spec: '<spec>'` | fetch refspec is not a branch mapped onto a remote-tracking branch, or a glob matches nothing |
| `fetch failed: invalid ref spec: '<branch>'` | `dolt_fetch` refspec names a branch the remote does not have. Nothing is fetched |
| `not a fast-forward of the remote branch (use force to overwrite)` | remote moved; add `--force` |
| `fetch failed: branch not found on remote` | `dolt_pull` of a missing branch |
| `cannot merge with uncommitted changes` | working or staged changes, other than an untracked table matched by `dolt_ignore`, on a fast-forward, a merge, or a pull that is already up to date. A local branch that is already ahead of the remote keeps those changes and the pull returns 0 |
| `database is not empty — clone into a fresh database` | `dolt_clone` into a database with tables or commits |
| `clone failed (graph changed during install; retry)` | a peer collected the synced chunks before the clone installed its refs. Retry the clone |
| `clone failed` | source unreachable or not a DoltLite database |
| `DoltLite remotes are disabled in this build` | built with `DOLTLITE_ENABLE_REMOTES=0` |

## Behaviour

- `dolt_push(remote, 'src:dst')` pushes revision `src` to remote branch
  `dst`. A `refs/heads/` prefix on either side is that branch's name. `HEAD`
  (or `refs/heads/HEAD`) as `src` is the checked-out branch's tip. Any other
  `src` is a revision: a branch, a tag, a commit hash, or a parent walk such
  as `HEAD~1`. `dst` follows
  the same rules as [dolt_branch](dolt_branch.md) and is refused with
  `invalid branch name` before the remote is opened, including a destination
  of `HEAD`. `src:refs/tags/...` is refused with
  `unsupported mapping: '<refspec>'`. An argument with no colon that starts
  with `refs/tags/` still pushes that tag.
- `dolt_fetch(remote, 'refs/heads/<pat>:refs/remotes/<name>/<pat>')` fetches
  each remote branch matching `<pat>` into `remotes/<name>/<mapped>`.
  `<pat>` is one branch name or a single `*`, and the same `*` on the
  destination is the captured text. `remotes/<name>/<pat>` is that mapping
  without the `refs/` prefix. A destination that is not a remote-tracking
  ref, such as `main:mm`, a star on only one side, or a glob that matches
  no branch, is `invalid fetch spec: '<spec>'`. An argument with no colon
  is still one branch name, and a `refs/heads/` prefix on that argument is
  stripped. Each refspec is checked before any tracking ref is updated. A
  branch the remote does not have, whether written as `<name>`,
  `refs/heads/<name>`, or an exact
  `refs/heads/<name>:refs/remotes/<remote>/<name>` mapping, is
  `fetch failed: invalid ref spec: '<name>'` and leaves every tracking ref
  where it was.
- `dolt_fetch` and `dolt_pull` do not hold the local database write lock
  while chunks download. A concurrent write waits only while a fetched batch
  is committed and while the tracking refs are installed. That commit reloads
  refs a peer wrote after this connection's snapshot before it publishes.
  A download that fails before that install leaves the tracking refs where
  they were.
- `dolt_clone` checks out the remote's default branch. That is `main` when
  the remote has it, otherwise `master`, otherwise the branch name that sorts
  first. The first push into an empty remote records its branch. A later push
  moves that default to `main`, to `master` while `main` is absent, or to a
  new branch whose name sorts first. Calling `dolt_default_branch` on the
  remote keeps the named branch, including when that name currently sorts
  first. A later push of `main`, or of `master` while `main` is absent, still
  takes over.
- Remotes carry commits, tags, and branch refs. Working sets never travel:
  cloned branches start clean, and a push is refused while the **target**
  database's branch has uncommitted changes.
- `dolt_push(remote, ':branch')` deletes that remote branch and its local
  tracking ref. The remote default branch cannot be deleted. Deleting a
  branch that is already absent returns 0, including when this database
  never fetched a tracking ref for it. Dolt errors in that case; the remote
  branch list is unchanged either way. A read-only connection, or another
  connection holding a write transaction, is refused before the remote ref
  moves.
- A lazy clone installs refs without copying chunks and fetches them on
  demand from `origin`. Reopen it with `?lazy_origin=1` to keep that
  behaviour in a new process; `--revision` picks the branch to check out, or a
  read-only snapshot for a tag or commit. A divergent pull on a lazy clone is
  refused until the store is fully materialized.
- Fetch and pull install remote tags whose commits were fetched, replacing a
  same-named local tag when the remote value differs.
- `dolt_push('--all', remote)` pushes every local branch to that remote.
  Tags are not included; use `--tags` for those. `--force` applies to each
  branch. The remote name is required. A branch or other refspec is
  `fatal: --all can't be combined with refspecs`, and `--tags` is
  `--all cannot be combined with --tags`. A non-fast-forward among several
  branches is reported with the other rejected names, the same way a
  multi-ref push is.
- `dolt_fetch --prune` (or `-p`) with no refspec drops tracking refs whose
  branch the remote no longer has. The same flag with a branch or refspec is
  refused and drops nothing.
- `dolt_pull()` and `dolt_fetch()` with no remote name use `origin` when that
  remote exists. When it does not, and exactly one remote is configured, they
  use that remote. That is the name `dolt_clone('--remote', name, url)` stored.
- `dolt_pull()` and `dolt_pull(remote)` fetch every branch on that remote,
  and the tags those fetches install, then fast-forward or merge the current
  branch from the same remote. `dolt_pull(remote, branch)` fetches only that
  branch. Without `--prune`, other tracking refs stay put and a tag whose
  commit was not fetched stays absent. A branch that exists only on the remote becomes a
  tracking ref and is not created locally. `--prune` / `-p` is allowed with
  or without a branch. After the fetch succeeds, tracking refs for branches
  the remote no longer has are dropped, including when the pull is already
  up to date, the local branch is ahead, or a later merge is refused.
- `dolt_clone` records the remote as `origin` unless `--remote` names it.
  The name is trimmed and then checked with the same rules as
  `dolt_remote('add', ...)`. Without `--single-branch`, every remote branch
  is tracked and the checkout branch is the only local branch (`--branch`
  when it names one, otherwise the default). `--single-branch` limits both
  the local branch and the tracking ref to that checkout branch.
- A pull that cannot fast-forward merges the commit just fetched onto the
  tracking ref, including `--no-ff` and `--squash`. The merge commit message
  is `Merge branch '<branch>' of <url> into <local>`, with the remote's
  stored URL and no quotes around it. A local branch named
  `<remote>/<branch>` does not take that commit's place. Uncommitted changes
  refuse that merge the same way they refuse a fast-forward. An untracked
  table matched by [dolt_ignore](dolt_ignore.md) is not one of those changes,
  and the pull leaves it in the working set. The branch tip moves only once
  that working catalog is ready to commit with it. If the pulled commit
  already contains the ignored table's name, the pull refuses and the branch
  stays put. An edit of a tracked table still refuses, including one whose
  name matches `dolt_ignore`, and so does an uncommitted edit of `dolt_ignore`
  itself.
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
