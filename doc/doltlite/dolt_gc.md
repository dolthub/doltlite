# dolt_gc

Reclaim chunks no branch, tag, commit, or working set can reach. Dolt:
[dolt_gc](https://docs.dolthub.com/sql-reference/version-control/dolt-sql-procedures#dolt_gc).

## Synopsis

```sql
SELECT dolt_gc();
-- 12 chunks removed, 45 chunks kept
VACUUM;                            -- same thing
VACUUM INTO '/path/compact.db';    -- compacted copy, source untouched
```

## Behaviour

Stop-the-world mark and sweep over every ref, commit, catalog, working set,
and prolly node, then a rewrite of the file holding only live chunks. Safe to
run at any time and idempotent. Deleted history becomes unreachable when its
last ref goes (`dolt_branch('-D')`, `dolt_tag('-d')`, `dolt_reset('--hard')`),
so space returns on the next `dolt_gc`. Collection is manual: unreachable
chunks occupy space until then. Automatic chunk-WAL checkpoints are separate
from GC.

`VACUUM` on a DoltLite database runs the same collection; `PRAGMA
wal_checkpoint` does too. `VACUUM INTO` writes a compacted DoltLite-format
copy; `:memory:` as the destination is refused. On an in-memory database the
result is `0 chunks removed, 0 chunks kept (in-memory)`.

| Error | Cause |
|---|---|
| `gc requires exclusive access`, `database is locked by another connection` | a peer holds a write transaction; retry after it commits |
| `cannot VACUUM from within a transaction` | as in SQLite |
| `attempt to write a readonly database` | `query_only` or `immutable=1` |

An open reader in another process finishes safely while GC runs; readers
never see a half-rewritten file.

## See also

[concurrency.md](concurrency.md), [storage-format.md](storage-format.md),
`test/doltlite_vacuum.sh`.
