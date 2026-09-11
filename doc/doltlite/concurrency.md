# Concurrency

DoltLite supports multiple connections and processes on one database file, but
it is not a free-for-all multi-writer server. Coordination is explicit: a
**graph lock** sidecar serializes durable writers, write transactions pin a
chunk-store snapshot, and multi-step version-control ops re-check HEAD under
the lock before advancing a branch tip.

For a DoltLite-format main database, the concurrency contract is:

<!-- contract: conn.per_session_branch -->
- **Per-connection branch selection.** Each connection holds its own active
  branch and session view of HEAD and staging (see
  per-session branching below). The
  uncommitted working set belongs to the branch, so another connection that
  selects that branch recovers it. Two connections may sit on different
  branches of the same file at once.
<!-- contract: conn.peer_deleted_branch -->
- **Peer-deleted branches.** A connection parked on a branch that a peer
  deletes may keep reading its snapshot, but writes fail and name the missing
  branch. Checking out an existing branch recovers the connection without
  recreating the deleted branch.
<!-- contract: reader.readonly_never_blocks_writer -->
- **A read-only connection never blocks a writer.** Reads, `dolt_status`
  included, take no exclusive lock on a connection opened read-only, so
  polling status from a second process cannot make a writer's `INSERT` or
  `dolt_commit` fail. Such a connection still sees a peer's uncommitted and
  committed work.
<!-- contract: writer.cross_thread_transaction -->
- **One durable writer at a time.** A connection that holds an explicit write
  transaction owns the graph lock. A peer that tries to begin a concurrent
  write gets `SQLITE_BUSY` (or a retryable busy class) until the owner
  commits or rolls back. Version-control commands report that same code, so a
  retry policy keyed on `SQLITE_BUSY` covers `dolt_branch` and `dolt_tag` as
  well as an ordinary `INSERT`. After the lock is free, the peer can retry
  successfully. In serialized threading mode, sequential calls from different
  threads may continue and finish the same transaction.
- **Snapshot-safe write upgrades.** A transaction that has established a read
  snapshot cannot upgrade to a writer after a peer advances the store; the
  upgrade returns `SQLITE_BUSY_SNAPSHOT` instead of mixing catalogs. Once a
  write transaction begins, it holds the graph lock and pins its snapshot
  until commit or rollback.
<!-- contract: txn.add_isolation -->
<!-- contract: txn.reader_close_upgrade -->
<!-- contract: reader.follows_restore -->
- **Readers stay live.** A reader can see already-committed data while another
  process holds an uncommitted write. Running `dolt_add` inside that write
  transaction does not publish its rows or staged state, and rolling the
  transaction back restores its prior staging state. A peer that only opens,
  reads, and closes the database does not prevent a live read transaction from
  upgrading to a writer. An idle connection follows a peer's completed
  `.restore` or backup replacement, while an unrelated file moved over the path
  remains read-only. An open iterator completes safely while another process
  runs GC. Readers do not create SQLite `-wal`/`-shm` sidecars.
- **Multi-process commits are CAS-safe.** A process that races `dolt_commit`
  against a peer either wins a clean tip advance or loses with a busy /
  conflict outcome. The loser's stale tip must not clobber the winner's
  commit. Sequential multiproc commits both land; forked SQL transaction
  writers leave consistent table and index state.
- **VC ops re-confirm HEAD under the lock.** Merge, cherry-pick, and revert use
  locked compare-and-advance; pull and rebase use operation-specific locked
  branch expectations. A peer commit between planning and ref update yields
  `SQLITE_BUSY` instead of a lost update.
- **Remote ref installs are serialized.** HTTP pushes refresh the remote refs
  under the graph lock before validating and installing either conditional or
  plain ref updates. A stale push is rejected instead of replacing a peer's
  ref update. A push also refuses to replace a target branch with uncommitted
  working or staged changes.
- **GC cooperates with writers.** `dolt_gc` / `VACUUM` may be deferred or
  report busy while a writer holds the graph lock; after the writer finishes,
  GC completes without dropping reachable data. Multiproc GC-vs-commit and
  GC-vs-GC races leave committed rows intact.
- **Conflicts are never durable.** A conflicted merge exists only inside the
  transaction that produced it. Commit is refused while conflicts remain;
  nothing conflicted is left on disk for a later connection to inherit.
  Constraint violations still persist (see Constraint Violations on Merge).

The machine-readable contract and its test mapping live in
[`test/concurrency_contract.tsv`](../../test/concurrency_contract.tsv). Multiproc and
multi-connection C harnesses (`multi_process_*`, `concurrent_*`) are the
behavioral oracles; the contract test asserts that every claim still points at
a real check or source needle. Nightly stress soaks those harnesses for hours;
PR CI runs them at shorter budgets via `test/run_c_tests.sh` and
`build-test`.

## Per-session branching

Each connection selects a branch independently and recovers that branch's
working set when it checks it out. There is no `dolt_stash`: checkout does not
shelve uncommitted work between branches.
