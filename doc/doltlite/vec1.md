# Versioned vec1

DoltLite vendors the SQLite team's [vec1](https://sqlite.org/vec1)
extension the way it vendors fts5: compiled in, no `.load`. The SQL
surface is vec1's, not a DoltLite dialect. Vector tables still commit,
branch, diff, clone, and push like any other table.

Usage lives in the README **Vector Search** section. This note is the
merge and storage contract behind that section.

## The configuration rule

Train with PQ compression (`codesize > 0`; training needs ≥ 512
vectors). That is the versioned configuration.

```sql
SELECT vec1_train(vector, '{nbucket: 64, codesize: 8, distance: "cos"}')
  FROM embeddings_base;
INSERT INTO embeddings(cmd, arg) VALUES ('rebuild', :model);
```

vec1 already keeps raw vectors in `%_base` whenever PQ is on: index
segments store codes, reranking reads the base. Uncompressed builds
(`index:"flat"`, or `nbucket` without `codesize`) migrate vectors out
of `%_base` into `%_idx` segments and leave placeholders behind.
Independent branch inserts then conflict on those derived segments;
`--ours` / `--theirs` discards the losing side's vector bytes, KNN
returns wrong rows, and rebuild fails.

A carried patch to keep uncompressed `%_base` authoritative is
backlog. Until then, uncompressed indexes are for small corpora where
drop-and-rebuild is cheap.

## Shadow tables

All ordinary `INTEGER PRIMARY KEY` tables:

| Table | Contents |
|---|---|
| `%_base(id, vector, c0…)` | raw vectors + metadata (authoritative under PQ) |
| `%_idx(id, bucket, first, last, val)` | segmented index arrays |
| `%_model(id, val)` | trained model blob |
| `%_meta(id, val)` | metadata arrays parallel to `%_idx` |
| `%_config(id, val)` | persistent config |

Rebuild is deterministic: the same final content, inserted in any
order, produces byte-identical `%_idx` / `%_meta`. Unchanged rebuilds
dedup to zero new chunks.

## Merge

`dolt_merge` treats vec1 shadows as derived state when `%_base` still
holds raw vectors:

- `%_base` merges row by row. Same-rowid vector edits are real
  conflicts. Disjoint inserts usually are not.
- If every remaining conflict is a rebuildable vec1 shadow
  (`%_idx`, `%_meta`, `%_model`, `%_config`) of such a table, the
  merge keeps ours' shadow rows, surfaces no conflicts, rebuilds each
  owner from the merged base and stored model, and commits clean.
- Uncompressed builds stay loud (`typeof(vector)` on the base rows
  is not a float blob). Auto-resolution would drop the other side's
  data.
- Any non-derived conflict disables the automation for the whole
  merge. User-table conflicts keep every conflict, shadows included.
- Cherry-pick and rebase do not auto-rebuild. Conflicts stay visible.

## Vendoring

`ext/vec1/vec1.c` stays byte-identical to upstream outside fenced
`doltlite:` blocks: `VEC1_STATIC` / init glue, `-Werror` warning
suppression, amalgamation collision fixes, and an `xShadowName`
implementation. Upstream leaves that method unset, so
`sqlite3IsShadowTableOf()` cannot see vec1 shadows and staging /
table-level checkout would skip them.

Re-vendoring is a deliberate, tested act. The current pin and the VC
surface live in `test/doltlite_vec1_vc.sh`.
