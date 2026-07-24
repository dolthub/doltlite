<p align="center">
  <img src="doltlite-logo.png" alt="Doltlite" width="600">
</p>

# Doltlite

A SQLite fork that replaces the B-tree storage engine with a content-addressed
[prolly tree](https://docs.dolthub.com/architecture/storage-engine/prolly-tree),
enabling Git-like version control on a SQL database. Everything
above SQLite's `btree.h` interface (VDBE, query planner, parser) is untouched.
Everything below it -- the pager and on-disk format -- is replaced with a
prolly tree engine backed by a single-file content-addressed chunk store.

[Why DoltLite?](https://www.dolthub.com/blog/2026-04-27-why-doltlite/) DoltLite
can be embedded in any language enabling local-first use cases for [Dolt](https://github.com/dolthub/dolt/).

You can read more about DoltLite, including its 
[origin story](https://www.dolthub.com/blog/2026-03-24-a-week-in-gas-town/), 
on the [DoltHub blog](https://www.dolthub.com/blog/?tags=doltlite).

## Install

Prebuilt binaries: [github.com/dolthub/doltlite/releases](https://github.com/dolthub/doltlite/releases).

Each install method places the same set of files (paths shown for `/usr/local`):

- `bin/doltlite`, `bin/doltlite-remotesrv` — the CLI shell and remote sync server
- `include/doltlite.h` — embedding header (the SQLite C API, under our name; `#include <doltlite.h>`)
- `include/doltlite_remotesrv.h` — in-process remote server API
- `lib/libdoltlite.a` — static library
- `lib/libdoltlite.{so,dylib}` — shared library

### macOS (Apple Silicon) / Linux (x86_64 or arm64)

```
sudo bash -c 'curl -fsSL https://github.com/dolthub/doltlite/releases/latest/download/install.sh | bash'
```

### Debian / Ubuntu

`.deb` packages ship for both `amd64` and `arm64`. Substitute `$ARCH` below:

```
VER=$(curl -fsSL https://api.github.com/repos/dolthub/doltlite/releases/latest | jq -r .tag_name | sed 's/^v//')
ARCH=amd64   # or arm64
BASE=https://github.com/dolthub/doltlite/releases/download/v${VER}
wget ${BASE}/libdoltlite0_${VER}_${ARCH}.deb ${BASE}/doltlite_${VER}_${ARCH}.deb
sudo dpkg -i libdoltlite0_*.deb doltlite_*.deb
```

Add `libdoltlite-dev_${VER}_${ARCH}.deb` for the header and static library.

### Windows

Download `doltlite-tools-win-x64-<ver>.zip` from
[releases](https://github.com/dolthub/doltlite/releases), extract `doltlite.exe`, add to `PATH`.

## Bindings

Language-specific wrappers around `libdoltlite`. Each one exposes the full `sqlite3_*` C API plus the dolt version-control functions.

| Language | Distribution | Source |
|---|---|---|
| Python | `pip install doltlite` | [dolthub/doltlite-python](https://github.com/dolthub/doltlite-python) |
| Ruby | `gem install doltlite` | [dolthub/doltlite-ruby](https://github.com/dolthub/doltlite-ruby) |
| Node.js / Bun | `npm install @dolthub/doltlite` | [dolthub/doltlite-node](https://github.com/dolthub/doltlite-node) |
| Browser / WASM | `npm install @dolthub/doltlite-wasm` | this repo ([`packaging/npm`](packaging/npm), built from [`ext/wasm`](ext/wasm)) |
| Swift (iOS / macOS) | SwiftPM: `https://github.com/dolthub/doltlite-swift` | [dolthub/doltlite-swift](https://github.com/dolthub/doltlite-swift) (XCFramework built by [`packaging/swift`](packaging/swift)) |
| Android | Gradle: `com.dolthub:doltlite-android` | [dolthub/doltlite-android](https://github.com/dolthub/doltlite-android) (AAR + JNA) |

## Building

### macOS / Linux

```
cd build
../configure
make
./doltlite :memory:
```

### Windows (MSYS2 / MINGW64)

```
pacman -S mingw-w64-x86_64-gcc mingw-w64-x86_64-zlib make tcl
mkdir -p build && cd build
../configure
make doltlite.exe
./doltlite.exe :memory:
```

To verify the engine:

```sql
SELECT doltlite_engine();
-- prolly
```

To build stock SQLite instead (for comparison):

```
make DOLTLITE_PROLLY=0 sqlite3
```

### WebAssembly (`ext/wasm`)

Doltlite vendors SQLite's `ext/wasm` build and defaults it to the Doltlite
engine path in this repo. Build the top-level generated SQLite files first,
then build the wasm package:

```bash
./configure
make sqlite3.c sqlite3.h sqlite3ext.h
make -C ext/wasm
```

That produces the browser-consumable artifacts under
[`ext/wasm/jswasm`](ext/wasm/jswasm), including:

- `sqlite3.js`
- `sqlite3.mjs`
- `sqlite3.wasm`

To build the upstream SQLite wasm path instead of Doltlite's split build:

```bash
make -C ext/wasm DOLTLITE_WASM=0
```

To package the generated wasm distribution as a zip:

```bash
make -C ext/wasm dist
```

## Using as a C Library

Doltlite exposes the full SQLite C API (`sqlite3_open`, `sqlite3_exec`,
`sqlite3_prepare_v2`, ...) through `doltlite.h`. Existing C programs port
by changing `#include "sqlite3.h"` to `#include <doltlite.h>` and linking
against `libdoltlite` instead of `libsqlite3` — no other source changes —
to get version control. The build produces `libdoltlite.a` (static) and
`libdoltlite.dylib`/`.so` (shared) with the full prolly tree engine and all
Dolt functions included.

Loadable-extension authors use `doltliteext.h` (the rebranded `sqlite3ext.h`,
shipped in the amalgamation zip alongside `doltlite.c`/`doltlite.h`).

```bash
cd build
../configure
make doltlite-lib   # builds libdoltlite.a and libdoltlite.dylib/.so
```

Compile and link your program:

```bash
# Static link (recommended — single binary, no runtime deps)
gcc -o myapp myapp.c -I/path/to/build libdoltlite.a -lpthread -lz

# Dynamic link
gcc -o myapp myapp.c -I/path/to/build -L/path/to/build -ldoltlite -lpthread -lz
```

The API is the standard [SQLite C API](https://sqlite.org/cintro.html) —
`sqlite3_open`, `sqlite3_exec`, `sqlite3_prepare_v2`, etc. Dolt features are
called as SQL functions (`dolt_commit`, `dolt_branch`, `dolt_merge`, ...) and
virtual tables (`dolt_log`, `dolt_diff_<table>`, `dolt_workspace_<table>`,
`dolt_history_<table>`, ...).

### Quickstart Examples

Complete working examples that demonstrate commits, branches, merges,
point-in-time queries, diffs, and tags. Each example does the same thing
in a different language.

**C** ([`examples/quickstart.c`](examples/quickstart.c)) — based on the
[SQLite quickstart](https://sqlite.org/quickstart.html):

```bash
cd build
gcc -o quickstart ../examples/quickstart.c -I. libdoltlite.a -lpthread -lz
./quickstart
```

**Python** ([`examples/quickstart.py`](examples/quickstart.py)) — uses the
standard `sqlite3` module, zero code changes. The
[`doltlite`](https://github.com/dolthub/doltlite-python) package bundles a
precompiled libdoltlite and handles loading it ahead of the stdlib SQLite:

```bash
pip install doltlite
python3 examples/quickstart.py
```

If you'd rather wire up the preload yourself (e.g. against a libdoltlite
you just built locally):

```bash
cd build
# Linux:
LD_PRELOAD=./libdoltlite.so python3 ../examples/quickstart.py
# macOS: see https://github.com/dolthub/doltlite-python for the
# install_name-shim + DYLD_INSERT_LIBRARIES recipe — macOS's two-level
# namespace means a plain LD_PRELOAD / ctypes preload won't redirect
# _sqlite3's symbol lookups.
```

All of the above paths require a Python whose `_sqlite3` module is loaded
as a shared extension that links to a separate `libsqlite3`. The
following Pythons do **not** qualify, regardless of which loading recipe
you use:

- **python-build-standalone** interpreters — the default for `uv python
  install`, `mise`, and Rye; `_sqlite3` is built into the interpreter.
- **python.org installer (macOS framework build)** — `_sqlite3` is built
  with SQLite statically linked in; only `/usr/lib/libSystem.B.dylib`
  shows as a dependency.
- **Apple's system Python** — same framework-style static link.

Use distro Python, Homebrew Python, pyenv-built Python, or conda Python
instead. For `uv`:

```bash
uv venv --python /opt/homebrew/bin/python3   # or /usr/bin/python3, etc.
```

**Go** ([`examples/go/main.go`](examples/go/main.go)) — uses
[mattn/go-sqlite3](https://github.com/mattn/go-sqlite3) with the `libsqlite3`
build tag:

```bash
cd examples/go
CGO_CFLAGS="-I../../build" CGO_LDFLAGS="../../build/libdoltlite.a -lz -lpthread" \
    go build -tags libsqlite3 -o quickstart .
./quickstart
```

## Dolt Features

Version control operations are exposed as SQL functions and virtual tables.

### The basic commit loop

#### Configuration

```sql
-- Set committer name and email (per-session)
SELECT dolt_config('user.name', 'Tim Sehn');
SELECT dolt_config('user.email', 'tim@dolthub.com');

-- Read current config
SELECT dolt_config('user.name');
-- Tim Sehn
```

All commit-creating operations (`dolt_commit`, `dolt_merge`, `dolt_cherry_pick`,
`dolt_revert`) use these values. The `--author` flag on `dolt_commit` overrides
the session config for a single commit. Config is per-connection and not
persisted — set it at the start of each session.

#### Staging and Committing

```sql
-- Stage specific tables or all changes
SELECT dolt_add('users');
SELECT dolt_add('-A');

-- Commit staged changes
SELECT dolt_commit('-m', 'Add users table');

-- Stage and commit in one step
SELECT dolt_commit('-A', '-m', 'Initial commit');

-- Shorthand (compound flags, like git commit -am)
SELECT dolt_commit('-am', 'Initial commit');

-- Commit with author
SELECT dolt_commit('-m', 'Fix data', '--author', 'Alice <alice@example.com>');
```

#### Status

```sql
-- Working/staged changes
SELECT * FROM dolt_status;
-- table_name | staged | status
-- users      | 1      | modified
-- orders     | 0      | new table
```

#### Workspace Tables

`dolt_workspace_<table>` exposes row-level working/staged changes and lets
you stage or unstage individual edits by updating `staged`:

```sql
SELECT id, staged, diff_type, to_id, to_rating, to_confidence,
       from_rating, from_confidence
  FROM dolt_workspace_ratings;

UPDATE dolt_workspace_ratings
   SET staged = TRUE
 WHERE to_confidence > from_confidence;

SELECT dolt_commit('-m', 'accept higher-confidence edits');
```

`DELETE FROM dolt_workspace_<table>` discards unstaged working-set edits
(restores the staged/HEAD side of those rows). Staged workspace rows cannot
be deleted; unstage them first.

#### Ignoring Tables (`dolt_ignore`)

Tables matching a pattern in `dolt_ignore` stay in the working set
but are skipped by `dolt_add` and hidden from `dolt_status`. Create
the table once per repo, then `INSERT` patterns:

```sql
CREATE TABLE dolt_ignore(
  pattern TEXT NOT NULL,
  ignored TINYINT NOT NULL,
  PRIMARY KEY(pattern)
);
INSERT INTO dolt_ignore VALUES ('tmp_*', 1);    -- ignore tmp_* tables
INSERT INTO dolt_ignore VALUES ('tmp_keep', 0); -- un-ignore a specific name
```

Patterns use `*` / `%` for zero-or-more and `?` for exactly one;
everything else is literal. Most-specific pattern wins (longest
literal count); equal-specificity disagreements error out.

### Inspecting what's there

#### Diff

Several ways to ask what changed:

```sql
-- Which tables changed across the commit history?
SELECT * FROM dolt_diff WHERE table_name = 'users';

-- Row- and cell-level change counts between two refs (commits, branches, tags)
SELECT * FROM dolt_diff_stat('v1.0', 'HEAD');
SELECT * FROM dolt_diff_stat('v1.0', 'HEAD', 'users');  -- narrow to one table

-- High-level per-table classification: added / dropped / renamed / modified
SELECT * FROM dolt_diff_summary('v1.0', 'HEAD');

-- Schema-level diff (tables, views, indexes)
SELECT * FROM dolt_schema_diff('v1.0', 'v2.0');

-- Turn a diff into ordered, executable SQLite statements. The result has
-- Dolt's statement_order, from_commit_hash, to_commit_hash, table_name,
-- diff_type, and statement columns. Schema changes that SQLite cannot express
-- with ALTER TABLE are emitted as an ordered table rebuild.
SELECT * FROM dolt_patch('v1.0', 'v2.0');
SELECT * FROM dolt_patch('v1.0', 'v2.0', 'users');
SELECT * FROM dolt_patch('v1.0..v2.0');
SELECT * FROM dolt_patch('main...feature', 'users');

-- Generate only data statements; numbering is compacted just as in Dolt.
SELECT statement FROM dolt_patch('HEAD', 'WORKING')
 WHERE diff_type = 'data'
 ORDER BY statement_order;

-- Row-level history for a single table: every INSERT / UPDATE / DELETE
-- that was ever committed, with real per-column to_/from_ pairs plus
-- commit metadata and a diff_type. One virtual table per user table,
-- auto-registered on each commit. Filter by to_commit (including the
-- special 'WORKING' value for staged + working changes) or from_commit
-- to narrow to a specific slice.
SELECT * FROM dolt_diff_users;
-- to_id | to_name | to_email | to_commit | to_commit_date |
--   from_id | from_name | from_email | from_commit | from_commit_date |
--   diff_type

SELECT diff_type, to_name, to_email, to_commit
  FROM dolt_diff_users
  WHERE to_id = 42;

SELECT * FROM dolt_diff_users WHERE to_commit = 'WORKING';  -- staged+working

-- TVF form: compare the table snapshots at two refs. Equivalent to
-- Dolt's dolt_diff(from_ref, to_ref, table) TVF — the table name
-- rides in the module name (SQLite TVFs declare a static schema at
-- xConnect, so the per-table column list can't move into the
-- argument list) and the two refs come through as positional args.
SELECT * FROM dolt_diff_users('HEAD~1', 'HEAD');
SELECT * FROM dolt_diff_users('v1.0', 'WORKING');

-- Two dots also compare the endpoint snapshots. Three dots compare the
-- merge base to the right endpoint.
SELECT * FROM dolt_diff_users('main..feature');
SELECT * FROM dolt_diff_users('main...feature');

-- Restrict the commit-attributed system table to a history range.
SELECT d.*
  FROM dolt_diff_users AS d
  JOIN dolt_log('v1.0..HEAD') AS l ON l.commit_hash = d.to_commit;
```

#### Log and History

```sql
-- Commit history
SELECT * FROM dolt_log;
SELECT * FROM dolt_log('feature');
SELECT * FROM dolt_log('main..feature');
-- commit_hash | committer | email | date | message
```

Two per-table virtual tables for time travel:

```sql
-- Every version of every row across all commits
SELECT * FROM dolt_history_users WHERE id = 42;

-- The table as it existed at a specific commit / branch / tag
SELECT * FROM dolt_at_users('abc123...');
SELECT * FROM dolt_at_users('feature');
SELECT * FROM dolt_at_users('v1.0');
```

#### Blame (dolt_blame\_&lt;table&gt;)

For each live row, the most recent commit that introduced its current
value:

```sql
SELECT * FROM dolt_blame_users;
-- id | commit | commit_date | committer | email | message
```

Walks history first-parent from HEAD; at linear commits a row is
blamed if it differs from first-parent, at merge commits if it
differs from the merge base. Schema-only changes (`ALTER TABLE ADD
COLUMN`) don't update blame.

#### Schema History (dolt_schemas)

Projection of views and triggers from `sqlite_schema`. This is the Dolt-style
surface for browsing non-table schema objects. Because `sqlite_schema` lives
in the branch-scoped catalog, `dolt_schemas` is version-controlled per branch
just like user tables — switching branches with `dolt_checkout` will show the
views and triggers defined on that branch:

```sql
CREATE VIEW active_users AS SELECT * FROM users WHERE active = 1;
CREATE TRIGGER audit_users AFTER UPDATE ON users
  BEGIN INSERT INTO audit VALUES(new.id, 'updated'); END;
SELECT dolt_commit('-Am', 'Add view and trigger');

SELECT * FROM dolt_schemas;
-- type    | name         | fragment                                  | extra | sql_mode
-- view    | active_users | CREATE VIEW active_users AS SELECT ...    |       |
-- trigger | audit_users  | CREATE TRIGGER audit_users AFTER UPDATE...|       |
```

Rows are filtered to `type IN ('view','trigger')` — ordinary tables and
indexes are not reported here. Use `sqlite_schema` directly (or
`dolt_schema_diff`) if you need the full schema surface.

### Undoing on one branch

#### Reset

```sql
SELECT dolt_reset('--soft');   -- unstage all, keep working changes
SELECT dolt_reset('--hard');   -- discard all uncommitted changes
```

#### Revert

Create a new commit that undoes the changes from a specific commit:

```sql
SELECT dolt_revert('abc123...');
-- Returns new commit hash, or "Revert completed with N conflict(s)"
```

Revert computes the inverse of the target commit's changes and applies
them to the current HEAD. The new commit message is
`Revert '<original message>'`. Cannot revert the initial commit.

### Parallel development

#### Branching (Per-Session)

Each connection tracks its own active branch. Branch state (active branch
name, HEAD commit, staged catalog hash) lives in the `Btree` struct
(per-connection). Each connection gets its own `BtShared` and chunk store.

```sql
-- Create a branch at current HEAD
SELECT dolt_branch('feature');

-- Switch to it (fails if uncommitted changes exist)
SELECT dolt_checkout('feature');

-- See current branch
SELECT active_branch();

-- List all branches
SELECT * FROM dolt_branches;
-- name | hash | latest_committer | latest_committer_email
-- | latest_commit_date | latest_commit_message | remote | branch | dirty

-- Delete a branch
SELECT dolt_branch('-d', 'feature');
```

##### Opening a Specific Branch

By default, Doltlite opens a database on the `main` branch. To connect
directly to another branch, put the branch name in the database target:

```bash
./doltlite my.db@feature
./doltlite my.db/feature
```

The same syntax works through the SQLite C API and language bindings, since
the branch is parsed from the database filename passed to `sqlite3_open()`:

```c
sqlite3_open("my.db@feature", &db);
sqlite3_open("my.db/feature", &db);
```

After opening, `active_branch()` reflects the selected branch:

```sql
SELECT active_branch();
-- feature
```

#### Tags

Immutable named pointers to commits:

```sql
SELECT dolt_tag('v1.0');                  -- tag HEAD
SELECT dolt_tag('v1.0', 'abc123...');     -- tag specific commit
SELECT dolt_tag('-d', 'v1.0');            -- delete tag
SELECT * FROM dolt_tags;                  -- list tags
```

#### Merge

Three-way merge of another branch into the current branch. Merges at the
**row level** — non-conflicting changes to different rows of the same table
are auto-merged. Conflicts (same row modified on both branches) are detected
and stored for resolution.

```sql
SELECT dolt_merge('feature');
-- Returns commit hash (clean merge), or "Merge completed with N conflict(s)"
```

#### Merge Status

Always one row. When nothing is merging, `is_merging` is 0 and the rest is NULL.

```sql
SELECT * FROM dolt_merge_status;
-- is_merging | source  | source_commit | target          | unmerged_tables
-- 1          | feature | 0f470f8440... | refs/heads/main | orders, users
```

`unmerged_tables` is the name-ordered union of tables with data conflicts,
constraint violations, or schema conflicts. Merge state lives in the working
set, so a connection that did not run the merge still reports it — but it
recovers `source` from the branch pointing at the merge commit, falling back to
the hash when none matches.

#### Conflicts

View and resolve merge conflicts:

```sql
-- View which tables have conflicts (summary)
SELECT * FROM dolt_conflicts;
-- table | num_conflicts
-- users | 2

-- View individual conflict rows for a table. Columns are
-- (from_root_ish, base_<col>..., our_<col>..., our_diff_type,
--  their_<col>..., their_diff_type, dolt_conflict_id) — one base/our/their
-- triple per user column, plus per-side diff_type and a stable
-- dolt_conflict_id for resolution.
SELECT * FROM dolt_conflicts_users;

-- Resolve individual conflicts by deleting them (keeps current working value)
DELETE FROM dolt_conflicts_users WHERE dolt_conflict_id = 5;

-- Or resolve all conflicts for a table at once
SELECT dolt_conflicts_resolve('--ours', 'users');   -- keep our values
SELECT dolt_conflicts_resolve('--theirs', 'users'); -- take their values

-- Commit is blocked while conflicts exist
SELECT dolt_commit('-A', '-m', 'msg');
-- Error: "cannot commit: unresolved merge conflicts"
```

#### Constraint Violations on Merge

Merges apply cell-by-cell at the prolly layer and don't run
referential actions inline. After the merge, doltlite walks the
merged state and records any row that violates a foreign key,
unique index, or CHECK constraint into per-table
`dolt_constraint_violations_<table>` vtables. A summary vtable
`dolt_constraint_violations` reports `(table, num_violations)`.

```sql
-- Summary of post-merge violations
SELECT * FROM dolt_constraint_violations;
-- table | num_violations
-- child | 1

-- Inspect violations for a specific table
SELECT violation_type, pk, violation_info
  FROM dolt_constraint_violations_child;
-- foreign key | 2 | {"Columns":["v1"],"ReferencedTable":"parent",...}

-- Resolve by deleting the offending rows from the base table,
-- or clear the violation entry directly:
DELETE FROM dolt_constraint_violations_child WHERE pk = 2;
```

`violation_type` values match Dolt: `foreign key`, `unique index`,
`check constraint`. For foreign-key and CHECK violations the
offending row remains in the base table; for unique-index
violations the loser (highest rowid) is evicted from the base
table into the violations vtable so the remaining value
stays unique. `dolt_commit` refuses to proceed while any row
remains in `dolt_constraint_violations_*`; pass `--force` to
bypass the guard. Re-scan anytime with `SELECT dolt_verify_constraints([--all] [--output-only] [table...]);` (returns 0 or 1).

#### Cherry-Pick

Apply the changes from a specific commit onto the current branch:

```sql
SELECT dolt_cherry_pick('abc123...');
-- Returns new commit hash, or "Cherry-pick completed with N conflict(s)"
```

Cherry-pick works by computing the diff between the target commit and its
parent, then applying that diff to the current HEAD as a three-way merge.
Conflicts are handled the same way as `dolt_merge`.

#### Rebase

Replay the current branch's commits on top of an upstream:

```sql
SELECT dolt_rebase('main');
-- "Successfully rebased and updated refs/heads/feat"
```

Atomic: any conflict or error during the replay restores the branch
to its pre-rebase state. Interactive mode lets you edit the plan
before applying it:

```sql
SELECT dolt_rebase('-i', 'main');
-- Creates a working branch dolt_rebase_<orig> and a dolt_rebase
-- table with one row per commit (default action: pick). Edit with
-- normal SQL: action in ('pick','drop','reword','squash','fixup'),
-- change commit_message, or reorder with fractional rebase_order.

UPDATE dolt_rebase SET action='drop'   WHERE commit_message='debug';
UPDATE dolt_rebase SET action='squash' WHERE commit_message='fixup';
SELECT dolt_rebase('--continue');  -- apply the edited plan
SELECT dolt_rebase('--abort');     -- throw the working branch away
```

#### Merge Base

Find the common ancestor of two commits:

```sql
SELECT dolt_merge_base('abc123...', 'def456...');
```

### Introspection and ops

#### Content-Addressed Hashes

Doltlite exposes the content-address of any ref, table, or the whole
database as a scalar SQL function. The decentralized use case is
simple: two peers that compute the same hash are at the exact same
state — no row-level diff required, no metadata roundtrip.

```sql
-- Commit hash for a branch, tag, raw hash, HEAD, or HEAD~N / HEAD^N
SELECT dolt_hashof('main');
SELECT dolt_hashof('HEAD~2');

-- Table root hash. One-arg form reflects uncommitted working edits.
SELECT dolt_hashof_table('users');
SELECT dolt_hashof_table('users', 'main');

-- Whole-catalog hash — changes iff any table root moves or a table
-- is created/dropped.
SELECT dolt_hashof_db();
SELECT dolt_hashof_db('HEAD');
```

All results are 40-char lowercase hex (20-byte prolly hash).

The `_table` and `_db` variants are history-independent: any two
rowsets that reduce to the same `(key, value)` set hash identically
regardless of insert order, transient deletions, commit chain, or
branch. See `test/vc_oracle_hashof_test.sh` for the property tests.

#### Garbage Collection

Remove unreachable chunks from the store to reclaim space:

```sql
SELECT dolt_gc();
-- "12 chunks removed, 45 chunks kept"
```

Stop-the-world mark-and-sweep: walks all branches, tags, commit
history, catalogs, and prolly tree nodes to find reachable chunks,
then rewrites the file with only live data. Safe and idempotent.

#### Remotes

Doltlite supports Git-like remotes for pushing, fetching, pulling, and cloning
between databases.

##### Filesystem Remotes

```sql
-- Add a remote
SELECT dolt_remote('add', 'origin', 'file:///path/to/remote.doltlite');

-- Push a branch
SELECT dolt_push('origin', 'main');

-- Clone a remote database
SELECT dolt_clone('file:///path/to/source.doltlite');

-- Fetch updates
SELECT dolt_fetch('origin', 'main');

-- Pull (fetch + fast-forward)
SELECT dolt_pull('origin', 'main');

-- List remotes
SELECT * FROM dolt_remotes;
```

##### HTTP Remotes

```sql
-- Add an HTTP remote (URL includes database name)
SELECT dolt_remote('add', 'origin', 'http://myserver:8080/mydb.db');

-- All operations work identically to file:// remotes
SELECT dolt_push('origin', 'main');
SELECT dolt_clone('http://myserver:8080/mydb.db');
SELECT dolt_fetch('origin', 'main');
SELECT dolt_pull('origin', 'main');
```

##### Remote Server (`doltlite-remotesrv`)

> [!WARNING]
> Plain HTTP remotes are unencrypted and unauthenticated. The server binds to
> `127.0.0.1` by default; use `--cert`, `--key`, and `--auth-keys` before
> exposing it to a network, or place it behind a reverse proxy that provides
> equivalent TLS and authentication.

Doltlite includes a standalone HTTP server for serving databases over the
network. Build it alongside doltlite:

```
cd build
make doltlite-remotesrv
```

Start serving a directory of databases:

```
./doltlite-remotesrv -p 8080 /path/to/databases/
# To bind to all interfaces (e.g. behind a TLS-terminating reverse proxy):
./doltlite-remotesrv -p 8080 --bind 0.0.0.0 /path/to/databases/

# Serve HTTPS and require authorized client credentials:
./doltlite-remotesrv -p 8443 --bind 0.0.0.0 \
  --cert server.crt --key server.key \
  --auth-keys /path/to/authorized-keys --audience db.example.com \
  /path/to/databases/
```

HTTPS clients verify the server certificate and hostname using the system
trust store. Set `DOLTLITE_CA_FILE` for a private CA. Client credentials live
in `~/.doltlite/creds` and can be created with `SELECT dolt_creds_new();`.
HTTP and HTTPS requests have a 30-second deadline by default. Set
`DOLTLITE_HTTP_TIMEOUT_MS` to a positive number of milliseconds to tune it.

Every `.db` file in that directory becomes accessible at
`http://host:8080/filename.db` or its configured HTTPS URL. The server supports
push, fetch, pull, and clone — multiple clients can collaborate on the same
databases.

The server is also embeddable as a library (`doltliteServeAsync` in
`doltlite_remotesrv.h`) for applications that want to host remotes in-process.
Transfers are content-addressed — only chunks the remote doesn't already
have are sent.

#### Version String

```sql
SELECT dolt_version();
-- e.g. "v0.10.6"
```

Zero-arg scalar returning the build's version string (from
`git describe` at compile time). Useful for peer negotiation in
decentralized setups, bug-report ergonomics, and migrations
that branch on engine version. Matches Dolt's `DOLT_VERSION()`
argcount contract.

## Using Existing SQLite Databases

Doltlite can ATTACH standard SQLite databases alongside its own prolly-tree
storage. This lets you keep versioned tables in doltlite and high-write
operational tables in standard SQLite, queried through a single connection.

Doltlite detects the file format automatically from the header — no
configuration needed. Standard SQLite files route to SQLite's original B-tree
engine; everything else uses the prolly tree.

### Basic ATTACH

```sql
-- Attach a standard SQLite database
ATTACH DATABASE '/path/to/events.sqlite' AS ops;

-- Query it (prefix table names with the alias)
SELECT * FROM ops.events WHERE type='click';

-- Main db tables need no prefix
SELECT * FROM threads;

-- Detach when done
DETACH DATABASE ops;
```

### Cross-Database JOINs

```sql
-- Join doltlite (versioned) tables with SQLite (attached) tables
SELECT t.title, e.type
FROM threads t
JOIN ops.events e ON t.id = e.thread_id;
```

### Migrating Data Between Formats

```sql
-- Copy from SQLite into doltlite (now versioned)
INSERT INTO threads SELECT * FROM ops.threads;

-- Copy from doltlite into SQLite (for export)
INSERT INTO ops.archive SELECT * FROM threads WHERE archived=1;

-- One-step copy with CREATE TABLE...AS
CREATE TABLE local_events AS SELECT * FROM ops.events;
```

### Hybrid Storage Pattern

Use doltlite for tables that benefit from version control, and standard SQLite
for high-throughput tables that don't need history:

```sql
-- Main DB: doltlite (versioned)
CREATE TABLE config(key TEXT PRIMARY KEY, val TEXT);
SELECT dolt_commit('-am', 'Add config table');

-- Attached: standard SQLite (high-write, no versioning overhead)
ATTACH DATABASE 'telemetry.sqlite' AS tel;
CREATE TABLE tel.events(seq INTEGER PRIMARY KEY, kind TEXT, payload TEXT);

-- Hot write path goes to standard SQLite
INSERT INTO tel.events VALUES(1, 'pageview', '{"url":"/home"}');

-- Analytics spans both databases
SELECT c.val, count(e.seq)
FROM config c
JOIN tel.events e ON e.kind = c.key
GROUP BY c.key;

-- Version control only applies to main db
SELECT * FROM dolt_diff WHERE table_name='config';
```

## Per-Session Branching Architecture

Each connection gets its own `Btree` / `BtShared` pair and independently
tracks branch name, HEAD commit, and staged catalog hash, so different
connections can sit on different branches at the same time. Each branch's
working catalog lives in its own chunk, so one branch's autocommit can
never corrupt another branch's reads. Writes and commit-graph mutations
are serialized through an exclusive file-level lock (matching SQLite's
standard behavior); reads are concurrent.

## Performance

### Sysbench OLTP Benchmarks: Doltlite vs SQLite

Doltlite retains SQLite's SQL engine and C API while replacing its storage
engine, so the natural question is: what does version control cost?

Every PR runs sysbench-style benchmarks comparing doltlite against stock SQLite:
the int-key suite in [`test/sysbench_compare.sh`](test/sysbench_compare.sh), plus
TEXT, BLOB, and composite-primary-key variants. CI enforces a 2.5x ceiling on
individual read/write ratios and a 2x ceiling on section averages, with wider
autocommit-write ceilings for expected durability variance. The per-release
numbers are published with each release on the
[GitHub releases page](https://github.com/dolthub/doltlite/releases).

### Algorithmic Complexity

All numbers below have automated assertions in CI (`test/doltlite_perf.sh` and `test/doltlite_structural.sh`).

- **O(log n) Point Operations** -- SELECT, UPDATE, and DELETE by primary key are O(log n), essentially constant time from 1K to 1M rows. Tested and asserted at 1K, 100K, and 1M rows.
- **O(n log n) Bulk Insert** -- Bulk INSERT inside BEGIN/COMMIT scales as O(n log n). 1M rows inserts in ~2 seconds. CTE-based inserts also scale linearly (5M rows in 11s).
- **O(changes) Diff** -- `dolt_diff` between two commits is proportional to the number of changed rows, not the table size. A single-row diff on a 1M-row table takes the same time as on a 1K-row table (~30ms).
- **Structural Sharing** -- The prolly tree provides structural sharing between versions. Changing 1 row in a 10K-row table adds only 1.9% to the file size (5.2KB on 273KB). Branch creation with 1 new row adds ~10% overhead.
- **Garbage Collection** -- `dolt_gc()` reclaims orphaned chunks. Deleting a branch with 1000 unique rows and running GC reclaims 53% of file size. GC is idempotent and preserves all reachable data.

## Running Tests

### SQLite Tcl Test Suite

`testfixture` is built from real doltlite (the prolly + version-control engine,
not stock SQLite) and runs the upstream SQLite TCL suite.

The TCL suite is gated in two layers:

- Passing upstream files are split into five CI buckets under
  [`test/regression-buckets`](test/regression-buckets): `core-sql`,
  `ddl-schema`, `storage`, `fault-fuzz`, and `ported`. The `ported` bucket holds
  doltlite-native rewrites of upstream tests whose original synchronization or
  raw-file assumptions do not apply to prolly storage.
- Remaining inherited-file gaps are tracked in
  [`test/known_testfixture_divergences.txt`](test/known_testfixture_divergences.txt)
  and [`test/known_testfixture_crashes.txt`](test/known_testfixture_crashes.txt).
  The dominant classes are doltlite's **rowid / primary-key identity** semantics
  (non-INTEGER primary key tables are stored as PK-keyed WITHOUT ROWID tables),
  raw SQLite page-format probes (`hexio_write`, file-format bytes,
  rootpage/page-count assertions), unsupported non-UTF-8 database encodings,
  custom-collation indexes, pager/WAL/rollback-journal lock-state expectations,
  VACUUM/page-layout behavior, planner counters such as `sqlite_search_count`,
  and a small number of diagnostic text differences.

`test/run_testfixture.sh` enforces these lists **bidirectionally**: every actual
failure/crash must be listed, and every listed entry must still fail/crash. That
catches both new regressions and stale allowlist entries. The lists are still an
audit backlog rather than proof of correctness: suspicious entries should get a
reproducible GitHub issue, native regression coverage, or a precise comment
explaining why the divergence is intentional. CI runs the bucket sweep on every
PR in [`.github/workflows/test.yml`](.github/workflows/test.yml).

To rerun the gated buckets locally:

```bash
cd build
bash ../test/run_testfixture.sh "SQLite regression core-sql" 300 \
  $(tr '\n' ' ' < ../test/regression-buckets/core-sql.txt)
bash ../test/run_testfixture.sh "SQLite regression ddl-schema" 300 \
  $(tr '\n' ' ' < ../test/regression-buckets/ddl-schema.txt)
bash ../test/run_testfixture.sh "SQLite regression storage" 300 \
  $(tr '\n' ' ' < ../test/regression-buckets/storage.txt)
bash ../test/run_testfixture.sh "SQLite regression fault-fuzz" 300 \
  $(tr '\n' ' ' < ../test/regression-buckets/fault-fuzz.txt)
bash ../test/run_testfixture.sh "SQLite regression ported" 300 \
  $(tr '\n' ' ' < ../test/regression-buckets/ported.txt)
```

To audit only the known-divergence and expected-crash files:

```bash
cd build
awk 'BEGIN{n=0} /^[[:space:]]*#/ || /^[[:space:]]*$/ {next} {print $1}' \
  ../test/known_testfixture_divergences.txt \
  ../test/known_testfixture_crashes.txt | sort -u > /tmp/doltlite-known-tests.txt
DIVERGENCE_FILE=$PWD/../test/known_testfixture_divergences.txt \
CRASH_FILE=$PWD/../test/known_testfixture_crashes.txt \
  bash ../test/run_testfixture.sh known-audit 120 $(cat /tmp/doltlite-known-tests.txt)
```

### Doltlite Shell Tests

60+ test suites covering all features:

```bash
# Run all suites
cd build
bash ../test/run_doltlite_tests.sh

# Run individual suites
bash ../test/doltlite_parity.sh          # SQLite compatibility (119 tests)
bash ../test/doltlite_commit.sh          # Commits and log
bash ../test/doltlite_staging.sh         # Add, status, staging
bash ../test/doltlite_workspace.sh       # Workspace tables and partial row staging
bash ../test/doltlite_branch.sh          # Branching and checkout
bash ../test/doltlite_merge.sh           # Three-way merge
bash ../test/doltlite_attach_sqlite.sh   # ATTACH standard SQLite databases
```

### Source Coverage

CI builds the non-amalgamated engine once with LLVM source-coverage
instrumentation. The existing Linux correctness jobs consume that build and
run the complete SQLite Tcl regression buckets, differential oracles,
deterministic shell and C suites, and the credential/TLS/HTTP remote
integration suites. Each parallel job uploads a small pool of raw profiles; a
final job merges them and publishes line, branch, and function coverage for
the DoltLite-owned `src/` implementation files. The workflow fails if an owned
source file with executable lines receives zero line coverage.

Timing and scale gates, concurrency and fault stress, sanitizers, and
platform-specific jobs keep their optimized or specialized builds. They do not
contribute profiles. macOS and Windows still run their platform checks, but
deterministic Linux correctness tests are not repeated outside the
instrumented jobs.

The aggregate source-coverage percentages are informational; no percentage
floor is currently set. CI updates a single coverage comment on the pull
request. Its linked artifact contains an HTML report, aggregate summary,
per-file TSV, merged LLVM profile, and LCOV data. To produce a local report
with Clang, LLVM tools, and Dolt:

```bash
mkdir build-coverage
cd build-coverage
CC=clang \
  CFLAGS="-O1 -g -fprofile-instr-generate -fcoverage-mapping" \
  LDFLAGS="-fprofile-instr-generate" \
  ../configure
make -j2 DOLTLITE_PROLLY_CHECK=1 \
  doltlite doltlite-remotesrv doltlite-lib testfixture \
  doltlite-c-tests-build doltlite-regression-test-c-build
make DOLTLITE_PROLLY=0 sqlite3
cd ..
mkdir -p coverage-profiles
export LLVM_PROFILE_FILE="$PWD/coverage-profiles/%8m.profraw"
export DOLTLITE_REGRESSION_PREBUILT=1
DOLTLITE_BUILD_DIR="$PWD/build-coverage" \
  DOLTLITE_SUITE_SET=coverage \
  bash test/run_doltlite_tests.sh
bash test/run_c_tests.sh build-coverage coverage
# Run the Tcl regression buckets and differential oracles as in test.yml.
bash test/source_coverage_report.sh \
  build-coverage coverage-profiles coverage-report
```

The report command accepts optional percentage floors through
`DOLTLITE_COVERAGE_MIN_LINES`, `DOLTLITE_COVERAGE_MIN_BRANCHES`, and
`DOLTLITE_COVERAGE_MIN_FUNCTIONS`. They are intentionally unset until the
informational baseline is stable enough to ratchet.

### Differential Oracle Tests

Doltlite ships a suite of differential oracle tests that run the same SQL
through doltlite and stock sqlite3, or through doltlite and Dolt for
version-control behavior, and compare results byte-for-byte. Each
script is focused on a SQL feature surface: savepoints, foreign keys, UPSERT,
generated columns, WITHOUT ROWID, large BLOBs at chunk boundaries, ATTACH
cross-engine queries, TEMP tables, triggers, dot-commands, FTS5, workspace
staging, or version-control operations. Scenarios are written to hit
storage-layer edge cases.

```bash
cd build
bash ../test/sql_oracle_test.sh ./doltlite ./sqlite3
bash ../test/oracle_savepoints_test.sh ./doltlite ./sqlite3
bash ../test/oracle_foreign_keys_test.sh ./doltlite ./sqlite3
bash ../test/oracle_upsert_test.sh ./doltlite ./sqlite3
bash ../test/oracle_generated_columns_test.sh ./doltlite ./sqlite3
bash ../test/oracle_without_rowid_test.sh ./doltlite ./sqlite3
bash ../test/oracle_large_blobs_test.sh ./doltlite ./sqlite3
bash ../test/oracle_attach_test.sh ./doltlite ./sqlite3
bash ../test/oracle_temp_tables_test.sh ./doltlite ./sqlite3
bash ../test/oracle_triggers_test.sh ./doltlite ./sqlite3
bash ../test/oracle_fts5_test.sh ./doltlite ./sqlite3
bash ../test/vc_oracle_workspace_test.sh ./doltlite dolt
```

A separate CI job builds the same suite with
`-fsanitize=address,undefined` and runs every oracle under ASan/UBSan to
catch memory and undefined-behavior bugs before they reach master.
The sanitizer workflow also runs SQLite's shared-connection and
shared-database thread tests plus Doltlite's concurrent HTTP remote suite
under TSan.

### SQL Logic Test Suite

doltlite runs the full [sqllogictest](https://www.sqlite.org/sqllogictest/)
corpus — the same 5.7M-statement set SQLite uses — against **real doltlite** and
stock SQLite. `test/run_sqllogictest.sh` compares doltlite-only divergences
against `test/known_sqllogictest_divergences.txt`.
Run `bash test/run_sqllogictest.sh <doltlite-runner> <stock-runner> <corpus-dir>`
locally (requires Fossil for the upstream corpus).

### Concurrent Branch Tests

C tests that verify cross-branch isolation — two connections on different
branches both write and read without corrupting each other:

```bash
cd build
gcc -o cross_branch_test ../test/cross_branch_test.c \
    -I. -I../src libdoltlite.a -lz -lpthread
./cross_branch_test
```

## Architecture

Doltlite implements the same prolly tree design as
[Dolt](https://github.com/dolthub/dolt) — content-addressed immutable
nodes with rolling-hash-determined boundaries — adapted for SQLite's
constraints and C implementation. The prolly tree engine lives in
`src/prolly_*.c`, the feature-level implementations of `dolt_*` SQL
functions and vtables live in `src/doltlite_*.c`, and `src/prolly_btree.c`
is the integration point where prolly dispatches against SQLite's
`btree.h` API.

For a deeper side-by-side of Dolt and DoltLite storage internals, see
[Dolt vs DoltLite Storage Comparison](https://www.dolthub.com/blog/2026-07-08-dolt-doltlite-storage-comp/).
