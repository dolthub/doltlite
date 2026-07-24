# AGENTS.md

Guidance for AI coding agents working in this repository.

## Project nature

DoltLite is a fork of SQLite that replaces the B-tree storage engine with a
content-addressed [prolly tree](https://docs.dolthub.com/architecture/storage-engine/prolly-tree),
giving a SQL database Git-like version control (branch / commit / merge / diff)
while staying embeddable as a single library. Everything **above** SQLite's
`btree.h` interface — tokenizer, parser, planner, VDBE — is upstream SQLite and
is left untouched. Everything **below** it — the pager and on-disk format — is
replaced by a prolly-tree engine backed by a single-file, content-addressed
chunk store.

DoltLite is developed on **Git** and hosted on **GitHub**
(<https://github.com/dolthub/doltlite>). Unlike upstream SQLite, it **uses
pull requests** and **accepts agentic contributions** — opening PRs is the
normal workflow (see *PR / git workflow* below).

### Licensing and file headers

DoltLite is licensed **Apache-2.0** (`LICENSE.md`). This differs from upstream
SQLite, which is public domain.

- **Upstream SQLite files** (`src/btree.c`, `src/vdbe.c`, `src/where*.c`, …)
  keep their public-domain "blessing" comment. Preserve it unchanged; never add
  a license header to them.
- **DoltLite source** (`src/doltlite_*.c`, `src/prolly_*.c`, `src/chunk_*.c`,
  all guarded by `#ifdef DOLTLITE_PROLLY`) carries **no** per-file license
  header. Match the surrounding file — do not add one.
- No issue or PR numbers in code comments; those references belong in commit
  messages and PR descriptions.

## Comments: minimal by default

**Do not add explanatory comments.** The comment bar in this codebase is very
high — aim for effectively none. Only keep a comment that is *load-bearing*:
one that captures a non-obvious invariant, a subtle correctness reason, or a
"why it must be this way" that the code cannot express on its own. Everything
else — restating what the code does, section banners, narration of obvious
steps, TODO chatter — makes the code worse and must be stripped.

Agents over-comment by default; consciously resist it. Write code that reads
like the surrounding DoltLite source (which is nearly comment-free), and when
in doubt, leave the comment out.

## Build

The build uses [autosetup](https://msteveb.github.io/autosetup/) from a
separate `build/` directory.

```bash
cd build
../configure
make doltlite                    # CLI shell — the prolly engine (default)
make doltlite-lib                # libdoltlite.a + .so/.dylib + doltlite.h
make DOLTLITE_PROLLY=0 sqlite3   # stock SQLite, for oracle/perf comparison
```

- On macOS with Homebrew, link needs `LIBRARY_PATH=/opt/homebrew/lib`.
- The test harness runs **`build/doltlite`**. Rebuild `build/` after any `src/`
  change or you validate a stale engine. A repo-root `./doltlite` or `./sqlite3`
  built over the prolly `.o` files is DoltLite-in-disguise; to get *real* stock
  SQLite for comparison, build with `DOLTLITE_PROLLY=0` in a clean directory.
- `make lint` runs the layering and raw-file-I/O guards over `src/`.

## Source layout

- **Upstream SQLite core** — `src/*.c` (parser, planner, `vdbe.c`, `where*.c`,
  …), untouched above `btree.h`. Master header `src/sqliteInt.h`.
- **Storage engine** — `src/prolly_*.c` (prolly-tree node, cursor, mutmap,
  diff, three-way merge, chunker, cache, hashing) and `src/chunk_*.c`
  (content-addressed chunk store, WAL, refs, staging, file format).
- **Version-control surfaces** — `src/doltlite_*.c`, roughly one file per
  feature: `doltlite_commit` (wire format), `doltlite_commit_cmd` /
  `doltlite_add` / `doltlite_reset` / `doltlite_merge_cmd` /
  `doltlite_cherry_pick` / `doltlite_revert` / `doltlite_rebase` /
  `doltlite_config` (SQL commands), `doltlite_core` (txn seal, catalog flush,
  create-commit helpers), `doltlite_cmd` (shared command scaffolding: option
  errors, peer-branch BUSY, conflict/CV txn-mode outcomes), `doltlite_branch`
  (branch/checkout), `doltlite_merge` (catalog orchestration in
  `doltlite_merge`, pass1/pass2 in `doltlite_merge_pass1` /
  `doltlite_merge_pass2`, rows in `doltlite_merge_rows`, schema IR in
  `doltlite_merge_schema`, plus `doltlite_merge_constraints`),
  `doltlite_diff` / `doltlite_diff_stat` /
  `doltlite_diff_table`, `doltlite_log`, `doltlite_history`, `doltlite_blame`,
  `doltlite_tag`, `doltlite_conflicts`, `doltlite_constraint_violations` /
  `doltlite_verify_constraints`,
  `doltlite_schemas` / `doltlite_schema_diff`, `doltlite_patch`,
  `doltlite_status`, `doltlite_workspace`, `doltlite_ignore`, `doltlite_hashof`,
  `doltlite_gc`, `doltlite_remote` / `doltlite_http_remote` /
  `doltlite_remotesrv`. Internal header: `src/doltlite_internal.h`; surfaces are
  registered in `src/doltlite.c`.

## The version-control surface (vtables + functions)

DoltLite exposes version control as SQLite **virtual tables** (system tables:
`dolt_log`, `dolt_diff`, `dolt_diff_<table>`, `dolt_conflicts`,
`dolt_constraint_violations`, `dolt_status`, …) and **scalar functions / TVFs**
(`SELECT dolt_commit(...)`, `SELECT dolt_merge(...)`, `SELECT dolt_branch(...)`).
Dolt exposes the same operations as stored procedures (`CALL dolt_commit(...)`).

**This vtable form, and DoltLite-flavored column names, are intentional** — not
conformance bugs. When comparing against Dolt, only **row-level semantics** must
match; do not file conformance issues over the vtable/function shape or column
naming.

### Dolt is the reference implementation

[Dolt](https://github.com/dolthub/dolt) is the authority on **what** every
version-control operation should do. When a behavior is ambiguous, when
DoltLite and Dolt disagree, or when implementing a new VC surface, **read the
Dolt source** (Go) to determine the correct semantics — result shape, diff/
merge/conflict rules, `dolt_*` system-table columns, error conditions — and
match it. It's the reference for the oracle suites and for the engine itself.
Dolt may be checked out locally as a sibling of this repo; otherwise consult it
on GitHub.

## Testing

Several independent layers. A change under `src/` should run the relevant ones;
`dolt` on `PATH` is required for the oracle suites.

- **`test/run_doltlite_tests.sh`** — DoltLite-native shell suites
  (`doltlite_*.sh`) covering branch/commit/diff/merge/checkout/etc. behavior and
  parity. Runs `build/doltlite`. This layer is separate from the regression and
  C tests and catches branch / default-branch / parity bugs they miss —
  **always run it for version-control changes.**
- **`test/vc_oracle_*_test.sh`** — Dolt oracle suites: run identical SQL against
  DoltLite (`SELECT dolt_x`) and real Dolt (`CALL dolt_x`) and diff the
  normalized output. Invoke as `bash test/vc_oracle_X_test.sh build/doltlite dolt`.
  CI auto-runs anything matching `vc_oracle_*_test.sh` / `oracle_*_test.sh`, so a
  new suite with that name needs no wiring. Compare semantics, not vtable shape.
- **Regression buckets** — `test/regression-buckets/*.txt` list the inherited
  and ported upstream `*.test` files gated via `testfixture`. `testfixture` is
  built **with** `SQLITE_TEST` (the CLI is not), so don't `#ifdef SQLITE_TEST`-
  guard a correctness fix, and verify fixes in `testfixture`, not only the CLI.
  Every `doltlite_*.test` suite must appear in a bucket or
  `test/lint_orphaned_suites.sh` fails.
- **`test/run_c_tests.sh`** — C unit tests (concurrency, crash recovery,
  serialize determinism, chunk-store locking, …). Locally it builds the tests it
  gates and flags binaries older than `src/`; under CI it runs the build phase's
  artifacts untouched. Either way *not built* and *stale* are counted separately
  from *failed*, so a skipped or out-of-date binary can never read as a pass.
- **`test/run_sqllogictest.sh`** — at **full pass / 100% parity**.
  `test/known_sqllogictest_divergences.txt` is **empty and must stay empty**: a
  new divergence means fix the engine, never add a known-divergence entry.

Rules that override convenience:

- **Never delete a test or disable a check.** If a test or assertion fails, fix
  the change, not the guardrail.
- For an observable bug, prove the fix with a **fail-before / pass-after** test
  (fails on the unfixed engine, passes with the fix). Latent/defensive fixes may
  legitimately have no such test.
- When local validation would take more than ~15 min, push a PR and let CI run
  the buckets/corpus/suites in parallel; spot-check locally.

## PR / git workflow

- Branch off a **freshly pulled `master`**; open the PR **against `master`**.
  Do not stack PRs — a stacked branch leaves commits dangling when its base
  merges first; rebase onto `master` instead.
- **Stage by explicit path** (`git add src/foo.c test/bar.sh`). `git add -A` /
  `git add .` sweep in hundreds of untracked build artifacts (`*.o`, `tsrc/*.c`,
  `./doltlite`, `./sqlite3`, `testfixture`).
- Commit or push only when asked. End commit messages and PR bodies with the
  trailers the harness expects.

## Version-control correctness invariants

Subtle rules that are easy to break silently — hold them when touching VC code:

- **Re-confirm under the lock.** Multi-step ops (merge / cherry-pick / revert /
  pull) must re-read HEAD under the lock immediately before advancing a ref, or
  they clobber a concurrent peer.
- **Scoped ref installs.** A pushed ref update must go through the scoped-refs
  validator (only the declared branch may change); never install a pushed blob
  wholesale. Clone install is unscoped by design.
- **Canonical catalog.** Schema commits adopt the canonical catalog; the live
  catalog must always equal the persisted one. Constructed catalog arrays
  (staging / merge / `-am`) adopt the single master root paired by `iTable==1`,
  never by name.
- **Ref resolution** for commit / branch / `HEAD` / `WORKING` / `STAGED` goes
  through `doltliteResolveCatalogHashForRef` — reuse it, don't re-derive.

## Move fast

Format stability, concurrency-contract documentation, and external integration
tests are currently deprioritized — the project is not ready to lock those down.
Prefer fixing the engine and proving it with tests over documenting constraints.
