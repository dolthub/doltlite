# Version-controlled vector search in doltlite: vec1 syntax, Dolt semantics

Status: all three phases done. Phase 1 shipped the vendored extension,
Phase 2 turned out to be upstream behavior under PQ configurations, and
Phase 3 made dolt_merge absorb derived-shadow conflicts itself. All
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

1. **`rebuild` migrates vectors out of `%_base` in uncompressed
   configurations** (stock-identical). After rebuild, `%_base.vector`
   holds the bucket number and the float data lives only in `%_idx`
   segment blobs; inserts into a built index migrate immediately, and
   `%_base` is *not* an authoritative store. With PQ compression
   (`codesize > 0`) the raw vectors stay in `%_base` — the property
   Phase 2 builds on.
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
today. Documented caveat: with *uncompressed* index configurations,
merge requires the rebuild workflow below, and concurrent branch writes to
a built index risk the loss mode in finding 3 — PQ configurations are
exempt (see Phase 2).

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

### Phase 2 — keep `%_base` authoritative (achieved by configuration, no patch)

The original plan here was a carried patch (`VEC1_KEEP_BASE`) forcing raw
vectors to stay in `%_base`. Implementation reading made it moot: **vec1
already keeps `%_base` authoritative whenever PQ compression is on**
(`codesize > 0`). The vector-to-index migration only happens for
uncompressed configurations, where the index segments store full vectors
and `%_base` would be a duplicate; with PQ, the segments hold only codes,
`%_base` retains the raw vectors (reranking needs them), and delete/update
re-derive the bucket by quantizing against the model. Verified end to end
on doltlite and pinned in the suite:

- **Lossless merge, mechanically.** `%_base` row-merges with full vector
  fidelity. Branch inserts landing in different buckets merge with *zero
  conflicts* (disjoint segment rows). Same-bucket inserts conflict only on
  the derived `%_idx` tail segment; resolving with **either** side and
  rebuilding from the merged base recovers both branches' vectors — no
  source table, no discipline. Rebuild determinism (finding 2) means both
  merge parents resolving independently converge to identical indexes.
- **Structural sharing.** Per-row base entries share across commits like
  any prolly table (finding 4: ~5KB per added vector, independent of
  corpus size). Index churn from rebuilds dedups to zero when unchanged.
- **True conflicts only where they belong**: same-rowid vector edits on
  both branches conflict in `%_base` — a real semantic conflict the user
  should resolve.

**The guidance is therefore a configuration rule**: versioned vector
tables should use trained PQ configurations (`codesize > 0`; training
needs ≥512 vectors). Uncompressed configurations (`index:"flat"`, or
`nbucket` without `codesize`) migrate vectors into the index and carry the
finding-3 loss caveat — appropriate only for small corpora where the
source-table drop-and-rebuild workflow is trivial anyway. A carried
`VEC1_KEEP_BASE` patch decoupling keep-base from compression remains
possible for those configs but is backlog, not a phase.

### Phase 3 (implemented) — merge-aware derived shadows

`dolt_merge` now absorbs vec1 derived-shadow conflicts itself: when every
conflict in a merge is a rebuildable vec1 shadow (`%_idx`, `%_meta`,
`%_model`, `%_config`) of a table whose `%_base` still holds raw vectors,
the merge keeps ours' rows (exactly what `--ours` resolution leaves),
surfaces no conflicts, and rebuilds each owner from the merged base and
stored model while the merged catalog is live — then commits as a clean
merge. **Derived shadows merge by rebuild, authoritative shadows merge by
row.**

Deliberate guardrails, each pinned in the suite:

- **Uncompressed builds stay loud.** Their `%_base` holds bucket numbers,
  not vectors (finding 1), so auto-resolution would silently lose the
  discarded side's data. The gate is `typeof(vector)` on the base rows.
- **Any non-derived conflict disables the automation** for the whole
  merge: a user-table conflict keeps every conflict, shadows included,
  reported for manual resolution — today's semantics.
- **Only `dolt_merge`.** Cherry-pick and rebase pass a null rebuild list
  and keep loud conflicts, conservatively.

The policy lives as a small registry in the merge (vec1 suffixes + the
model-based rebuild statement). FTS could join later, but only for
content-ful tables — contentless fts5 cannot rebuild — so it is not wired
here.

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
  documented here rather than pinned as a test; the PQ scenarios pin the
  lossless recovery that needs no source table.
- Still open: recompact-after-noop → zero chunk growth as a suite check
  (verified by hand); fuzzing — vec1 shadow blobs are untrusted on-disk
  inputs once vendored, so the fuzz-vc-blobs corpus should grow
  `%_idx`/`%_model` mutations.
- Oracle discipline: every divergence suspicion runs against stock
  SQLite + vec1 first (finding 1 was almost misfiled as doltlite
  corruption).

## Recommendation

Phases 1 and 2 deliver the ask with real Dolt semantics: vec1 syntax,
versioned + branchable vector search, lossless merges via deterministic
rebuild under PQ configurations, and structural sharing measured at ~5KB
per vector per commit. Tell users one rule — train with `codesize > 0` —
and the versioning story has no asterisks — with Phase 3, `dolt_merge`
on PQ vector tables needs no manual steps at all.
