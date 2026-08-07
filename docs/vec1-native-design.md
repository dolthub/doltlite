# Version-controlled vector search in doltlite: vec1 syntax, Dolt semantics

Status: Phase 1 implemented (this PR); Phases 2 and 3 are design. All
empirical claims below were verified on 2026-08-07 against doltlite master
(with #2020–#2023) and vec1 trunk check-in `ecb12ac26e` (v0.7), with stock
SQLite 3.53.2 as the oracle.

## Directive

Match the syntax of the official SQLite vector extension —
[vec1](https://sqlite.org/vec1/doc/trunk/doc/vec1.md) — rather than inventing
a doltlite-specific surface. vec1 is developed by the SQLite team in its own
fossil repository (single ~11k-line `vec1.c`, not yet in the amalgamation),
uses IVFADC with optional OPQ/PQ compression, and exposes:

```sql
CREATE VIRTUAL TABLE embeddings USING vec1(vector, category, price);
INSERT INTO embeddings(rowid, vector, category, price) VALUES (:id, :f32blob, :c, :p);
SELECT vec1_train(vector, '{nbucket: 1024, codesize: 16, distance: "cos"}') FROM training_set;
INSERT INTO embeddings(cmd, arg) VALUES ('rebuild', :model);
SELECT rowid FROM embeddings(:query, '{k: 100}')
 WHERE category = 3
 ORDER BY vec1_cos_distance(:query, vector) LIMIT 10;
```

## What we verified empirically

vec1 works on doltlite **out of the box** — no code changes, loaded as a
dylib. Every shadow table uses `INTEGER PRIMARY KEY` (no sqlite-vec-style
typeless `rowid PRIMARY KEY`), and its only incremental-blob use is
read-only. Verified working: inserts, flat and trained/bucketed (nbucket=16)
KNN, metadata filtering, incremental inserts into a trained index, commit,
branch isolation, historical KNN on old branches, `integrity_check`.

Shadow layout (all ordinary rowid tables):

| Table | Contents |
|---|---|
| `%_base(id, vector, c0…)` | raw vectors + metadata, one row per rowid |
| `%_idx(id, bucket, first, last, val)` | segmented index arrays (rowids + codes/vectors), LSM-ish |
| `%_model(id, val)` | trained model blob |
| `%_meta(id, val)` | metadata arrays parallel to `%_idx` segments |
| `%_config(id, val)` | persistent config |

Key behaviors, each verified against the stock oracle where marked:

1. **`rebuild` migrates vectors out of `%_base`** (stock-identical). After
   rebuild, `%_base.vector` holds 1-byte placeholders and the float data
   lives only in `%_idx` segment blobs. Inserts into a built index migrate
   immediately. `%_base` is *not* an authoritative store after rebuild.
2. **Rebuild is deterministic.** Identical final content inserted in
   different orders produces byte-identical `%_idx`/`%_meta`. Combined with
   content-addressed chunk storage, a rebuild over unchanged data costs
   **zero** new storage (measured: recompact after delete = 0 bytes).
3. **Merge conflict surface is small but the failure mode is data loss.**
   Disjoint inserts on two branches: `%_base` row-merges cleanly; conflicts
   land only on the derived `%_idx`/`%_meta` tail segments. But resolving
   `--ours`/`--theirs` **discards the losing side's migrated vector bytes
   irrecoverably** — base has only placeholders, KNN returns wrong rows,
   rebuild fails (`unexpected vector size in t_base: 1`), `integrity_check`
   fails. Verified end to end.
4. **Storage profile** (5000×128d float32, ~2.7MB): +1 insert costs ~5KB
   (raw base) / ~5.9KB (built index — only a small tail segment changes);
   1 delete costs ~2.1MB (segment rewrite); full rebuild rewrites ~2.6MB
   but dedups to zero if content is unchanged.

## The design in one sentence

Keep `%_base` authoritative forever — never migrate vectors out of it — and
everything Dolt-like (row-level merge, structural sharing, lossless
conflict resolution, history independence) follows from machinery doltlite
already has.

## Phased plan

### Phase 1 — vendor vec1 (implemented in this PR)

vec1 is compiled into doltlite the way fts5 and rtree are: `ext/vec1/vec1.c`
in the object builds, the MSVC build, and the doltlite amalgamation,
registered per-connection in the built-in extension list under
`DOLTLITE_PROLLY` (no enable flag, no `.load`; stock amalgamations are
unchanged). Users get vec1 syntax with per-branch versioned vector search
today. Documented caveats: merge requires the rebuild workflow below;
concurrent branch writes to a *built* index risk the loss mode in finding 3.

The vendored file stays byte-identical to upstream outside fenced
`doltlite:` blocks, which carry four things: the `VEC1_STATIC`/init glue,
warning suppression for the strict `-Werror` CI flavors, amalgamation
collision fixes (the `i8` typedef, a few helper macros), and one behavior
fix — an **`xShadowName` implementation**. Upstream leaves that method
unset, so `sqlite3IsShadowTableOf()` cannot recognize vec1's shadow tables
and every by-name shadow carrier (staging, table-level checkout) silently
skipped them; the vec1 suite caught this as a checkout that adopted
nothing. Worth offering upstream.

Tests landed as a dedicated suite, `test/doltlite_vec1_vc.sh` (a separate
suite rather than a section in the vtab battery: vec1's train/rebuild
workflows need fixtures the generic matrix doesn't have). It covers the VC
surfaces plus the load-bearing properties: raw-base row-merge, the
conflict + source-table-rebuild recovery from finding 3, the determinism
of finding 2 (so upstream changes that break determinism fail loudly when
we re-vendor), trained-model KNN, table-level checkout, clone/pull, gc,
and drop/restore.

Open question for this phase: vec1 is v0.7 and pre-amalgamation; upstream
may still change the shadow schema. Vendoring pins check-in `ecb12ac26e`;
re-vendoring is a deliberate, tested act (same as the Aug 1 upstream merge
discipline), with the fenced blocks reapplied.

### Phase 2 — keep `%_base` authoritative (small patch, transforms the semantics)

A compile-time doltlite mode for vec1 (candidate name
`VEC1_KEEP_BASE`): inserts write the raw vector to `%_base` *and* the
index; rebuild reads from `%_base` and does not blank it. Storage cost:
one extra copy of the raw vectors (~the size of the data itself; the index
segments usually carry compressed PQ codes anyway when trained).

What this buys, given the verified findings:

- **Lossless merge, mechanically.** `%_base` row-merges (finding 3's clean
  half). `%_idx`/`%_meta`/`%_config` conflicts become *resolvable by
  either side + rebuild* with zero data loss, because base is always the
  source of truth. The recovery is `dolt_conflicts_resolve(<either>);
  INSERT INTO t(cmd,arg) VALUES('rebuild', <model>);` — deterministic
  (finding 2), so both merge parents resolving identically converge to
  identical indexes (history independence).
- **Structural sharing.** Per-row base entries share across commits like
  any prolly table (finding 4: ~5KB per added vector, independent of
  corpus size). Index churn from rebuilds dedups to zero when unchanged.
- **True conflicts only where they belong**: same-rowid vector edits on
  both branches conflict in `%_base` — a real semantic conflict the user
  should resolve.

Upstream angle: `keep_base` is a legitimate general feature (it is what
makes vec1 backup/replication-friendly too). Worth offering upstream; until
then it is a small carried patch (the migration happens in a handful of
sites — insert path, rebuild reader, and the base-write of placeholders).

### Phase 3 (optional end-state) — merge-aware derived shadows

With Phase 2, the remaining friction is that users must resolve derived-
table conflicts and run rebuild by hand after merges. Phase 3 teaches
doltlite's merge that a vtab's derived shadows (`%_idx`, `%_meta`,
`%_config`) can be auto-resolved (take either side) and queues a rebuild —
making `dolt_merge` on vector tables fully automatic. This generalizes to
FTS (`%_data` etc.) and is the doltlite answer to "index merge" without
per-module merge code: **derived shadows merge by rebuild, authoritative
shadows merge by row.** Requires a per-module declaration of which shadows
are derived (a small registry keyed by module name, or an xShadowName-like
extension method we define).

A full native reimplementation of vec1's interface over bespoke prolly
trees (proximity-map style) is *not* proposed: Phase 2 already achieves
row-level merge, sharing, and determinism with a fraction of the code and
zero surface drift, and vec1's segment design is already incremental-
friendly on content-addressed storage. Revisit only if the raw-vector
duplication or the rebuild-on-merge cost proves unacceptable at scale.

## What doltlite infrastructure this leans on

- #2020 rowid-shape exemption (not needed by vec1 itself, but keeps the
  extension ecosystem working), #2021 vtab schema rows through merge,
  #2022 merge transaction sealing, #2023 (+#2026) table-level vtab
  checkout with shadow index reconcile, #2024 vtab×VC battery as the
  regression net.

## Test plan

- `test/doltlite_vec1_vc.sh` (Phase 1, landed): vec1 across the VC
  surfaces, the built-index conflict + source-table-rebuild recovery, and
  insert-order determinism of `%_idx`/`%_meta`. The finding-3 loss case is
  documented here rather than pinned as a test; Phase 2 turns the recovery
  into a lossless assertion that no longer needs a source table.
- Still open: recompact-after-noop → zero chunk growth as a suite check
  (verified by hand); fuzzing — vec1 shadow blobs are untrusted on-disk
  inputs once vendored, so the fuzz-vc-blobs corpus should grow
  `%_idx`/`%_model` mutations.
- Oracle discipline: every divergence suspicion runs against stock
  SQLite + vec1 first (finding 1 was almost misfiled as doltlite
  corruption).

## Recommendation

Phase 1 (landed) plus Phase 2 deliver the ask with real Dolt semantics:
vec1 syntax, versioned + branchable vector search, lossless merges via
deterministic rebuild, and structural sharing measured at ~5KB per vector
per commit. Phase 3 is polish to schedule on demand.
