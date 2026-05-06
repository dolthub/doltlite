## Fixed

- Savepoint / transaction parity with Dolt is much tighter across version-control operations. Successful and error-path behavior now matches Dolt for `dolt_branch`, `dolt_checkout`, `dolt_tag`, `dolt_add`, `dolt_reset`, `dolt_commit`, `dolt_push`, `dolt_fetch`, `dolt_pull`, and `dolt_clone`, including when those operations should invalidate savepoints versus remain rollback-safe.
- Merge / replay rollback state is now durable and reopen-safe. `dolt_merge`, `dolt_cherry_pick`, and `dolt_revert` no longer leave stale conflicts, constraint violations, or half-applied working-set state behind after rollback, reconnect, or crash-recovery scenarios.
- Interactive rebase got a full round of savepoint and reopen fixes. `dolt_rebase('-i', ...)`, `dolt_rebase('--continue')`, and `dolt_rebase('--abort')` now clean up temporary rebase branches correctly, preserve or seal savepoints in the same places Dolt does, and reopen on the correct branch with the correct durable state after success, conflict aborts, constraint aborts, and explicit transactions.
- Conflict resolution is now properly rollback-aware. `dolt_conflicts_resolve('--ours', ...)`, `dolt_conflicts_resolve('--theirs', ...)`, and conflict-resolution cleanup paths restore conflict state correctly on savepoint rollback instead of leaking resolved state through the transaction boundary.
- `dolt_checkout('-b', ..., 'origin/<branch>')` now resolves fetched tracking refs correctly after `dolt_fetch`.
- `dolt_clone()` now refreshes the active `main` schema like `ATTACH`, so statements later in the same SQL execution see cloned tables and schema changes immediately.
- Default-branch and working-set durability are more robust: delete / rename of `main` is rejected, checkout rollback preserves the original branch state, and branch/default-branch persistence after VC operations is more consistent across reopen.
- Crash recovery tests for large batched commits now enforce the real committed-prefix invariant and avoid timing-based flakes.

## Performance

- Deferred per-table mutmaps now drain through the existing savepoint-aware flush path once they reach 64K pending edits, preventing unbounded in-memory growth during large write bursts while preserving `ROLLBACK TO` semantics.

## Added

- Connection targets can now select a branch directly when opening a database, making branch-scoped sessions available at connect time instead of requiring a follow-up checkout.
- The cross-engine oracle suite now exceeds 1,000 scenarios, with new coverage for triggers, savepoints, error recovery, conflict resolution, remote operations, clone semantics, and broad feature-interaction matrices.
- Added dedicated regressions for clone transaction behavior so `dolt_clone()` is locked to `ATTACH`-style semantics going forward.

**Full Changelog**: https://github.com/dolthub/doltlite/compare/v0.8.1...v0.8.2
