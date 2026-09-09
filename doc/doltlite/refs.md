# Revision syntax

Every version-control function and table that takes a revision accepts the
same spellings. Resolution is case-sensitive.

| Spelling | Meaning |
|---|---|
| `main`, `feature/x` | Branch tip |
| `v1.0` | Tag |
| 40 lowercase hex chars | Commit hash. Prefixes are not accepted. |
| `HEAD` | Tip of the connection's branch. `head` is an error. |
| `origin/main`, `remotes/origin/main`, `refs/remotes/origin/main` | Remote tracking branch, present after `dolt_clone` or `dolt_fetch` |
| `<rev>~N` | Nth first-parent ancestor. `~` alone is `~1`. |
| `<rev>^N` | Nth parent of a merge commit. `^` alone is `^1`; `^0` is an error. |
| `WORKING` | The working set: committed data plus staged and unstaged edits |
| `STAGED` | The staged catalog, or `HEAD` when nothing is staged |

Suffixes chain and apply left to right: `HEAD~2^2`, `main~1~1`. A suffix needs
a base, so `~1` alone is an error. When one name is both a branch and a tag,
the branch wins. Lookup order is `HEAD`, hash, branch, tracking branch, tag.

`WORKING` and `STAGED` are case-insensitive and name a catalog, not a commit.
They work wherever a snapshot is compared or read (`dolt_diff_*`,
`dolt_diff_stat`, `dolt_diff_summary`, `dolt_schema_diff`, `dolt_patch`,
`dolt_at_<table>`, `dolt_hashof_table`, `dolt_hashof_db`) and are rejected by
anything that needs a commit (`dolt_hashof`, `dolt_log`, `dolt_history_<table>`,
`dolt_merge_base`, `dolt_checkout`). A detached connection has neither.

## Ranges

| Range | Meaning |
|---|---|
| `a..b` | In `dolt_log`: commits reachable from `b` but not `a`. In diffs: from `a` to `b`. |
| `a...b` | In `dolt_log`: commits on either side but not both. In diffs: from the merge base of `a` and `b` to `b`. |

Both sides are required and a range may hold one operator, so `..main` and
`a..b..c` are errors.

| Surface | Accepts |
|---|---|
| `dolt_log(rev)`, `dolt_log(range)` | one argument; no two-argument form |
| `dolt_diff_<table>(from, to)`, `dolt_diff_<table>(range)` | one revision alone is an error |
| `dolt_schema_diff`, `dolt_patch` | `(from, to)` or `(range)` |
| `dolt_diff_stat`, `dolt_diff_summary` | `(from, to)` only |
| `dolt_history_<table>(rev)`, `dolt_at_<table>(rev)` | one revision |
| `dolt_branch(name, rev)`, `dolt_checkout('-b', name, rev)`, `dolt_tag(name, rev)`, `dolt_reset('--hard', rev)` | one revision as start point |

`dolt_checkout(tag)` and `dolt_checkout(hash)` are refused, as in Dolt: the
error suggests `dolt_checkout(rev, '-b', name)`. To read a commit without a
branch, open it by path.

## Opening a revision by path

The database path may carry a revision after `@` or `/`. The file is the
longest existing prefix, so branch names may contain `/`.

```
doltlite my.db@feature       # branch: read-write, working set of that branch
doltlite my.db/feature/x     # same, slash form
doltlite my.db/v1.0          # tag: read-only detached snapshot
doltlite 'my.db/main~1'      # ancestor: read-only detached snapshot
```

The same string works in `sqlite3_open()` and every binding. Detached
connections return `NULL` from `active_branch()`, fail writes with
`attempt to write a readonly database`, and stay pinned even if a peer moves
or deletes the ref. A missing revision fails the open:
`branch or revision "x" not found`.

## Names

Branch and tag names follow git's rules. Rejected: empty; `HEAD`, `head`,
`WORKING`, `STAGED`, `-`, `@`; 40 hex characters; a leading or trailing `/`;
a trailing `.`; a component starting with `.` or ending in `.lock`; and any of
space, control characters, `:`, `?`, `[`, `\`, `^`, `~`, `*`, `..`, `@{`.
Dots and slashes are otherwise fine: `release/1.2` is a valid branch or tag.

## Errors

| Message | Cause |
|---|---|
| `invalid ref spec` | unknown name, short hash, empty string, or a catalog name where a commit is required |
| `invalid ancestor spec` | `~`/`^` without a base, `^0`, or walking past the root |
| `invalid dolt_log revision: <spec>` | `dolt_log` given a malformed range or a non-commit |
| `dolt_diff_<t> requires a '..' or '...' revision range` | `dolt_diff_<table>` given a single revision |
| `start point not found` | `-b` start point does not resolve |
| `invalid branch name`, `invalid tag name` | name rule violation |
