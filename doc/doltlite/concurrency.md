# Concurrency

DoltLite supports multiple connections and processes on one database file, but
it is not a free-for-all multi-writer server. Coordination is explicit: a
**graph lock** sidecar serializes durable writers, write transactions pin a
chunk-store snapshot, and a version-control command that changes the
database runs as one write transaction.

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
  `dolt_commit` fail. A writer's `COMMIT` waits for that reader, up to the
  writer's busy timeout, before it publishes. Such a connection still sees a
  peer's uncommitted and committed work.
<!-- contract: writer.vacuum_into_destination -->
- **`VACUUM INTO` reserves its destination.** The destination is held for the
  whole operation, so a peer cannot commit a database at that path between the
  emptiness check and the output. One side is refused: `VACUUM INTO` reports
  `output file is in use by another connection` if the destination is already
  held, and a peer that arrives while it runs is refused instead. Neither side
  reports success while overwriting the other's committed data. An
  already-populated destination is still refused with `output file already
  exists`.
<!-- contract: writer.commit_busy_retry -->
<!-- contract: writer.cross_thread_transaction -->
- **One durable writer at a time.** A connection that holds an explicit write
  transaction owns the graph lock. A peer that tries to begin a concurrent
  write gets `SQLITE_BUSY` (or a retryable busy class) until the owner
  commits or rolls back. Version-control commands report that same code, so a
  retry policy keyed on `SQLITE_BUSY` covers `dolt_branch` and `dolt_tag` as
  well as an ordinary `INSERT`. `dolt_commit` honors the busy handler while
  waiting for the graph lock; a refused commit preserves earlier successful
  autocommit writes in the working set. After the lock is free, the peer can retry
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
<!-- contract: reader.status_snapshot -->
- **Readers stay live.** A reader can see already-committed data while another
  process holds an uncommitted write. A `dolt_status` query uses its active
  reader snapshot without acquiring the graph writer lock, including on a
  read-only connection. Running `dolt_add` inside that write
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
- **A version-control command is one write transaction.** In autocommit, a
  command that changes the database (`dolt_add`, `dolt_commit`, `dolt_merge`,
  `dolt_cherry_pick`, `dolt_revert`, `dolt_reset`, `dolt_clean`,
  `dolt_checkout`, `dolt_rebase`, `dolt_branch`, `dolt_tag`,
  `dolt_default_branch`, `dolt_conflicts_resolve`,
  `dolt_verify_constraints`) takes the graph lock when it starts, waiting
  for it under the busy handler, and holds it until it finishes. It reads
  one snapshot, no peer write lands while it runs, and it publishes once,
  when it finishes, so a command that fails has taken no effect. The
  exception is a rebase step that fails and aborts the rebase: it publishes
  the abort. A peer that tries gets `SQLITE_BUSY`, or waits up to its busy
  timeout, and succeeds once the command is done. A long merge therefore
  blocks other writers for its whole run, as a long write transaction does
  in SQLite. Inside `BEGIN` or a `SAVEPOINT`, a command joins the caller's
  transaction and holds the graph lock while it runs, so no peer writes
  between its steps.
- **`dolt_clean` and `dolt_reset` do not wait to start.** Either may change
  nothing, and such a call must not block on a peer. If a peer holds the lock
  when one begins, it does not wait for it: a call that changes nothing
  completes without blocking, and one that does write starts over as a
  write transaction that waits for the lock under the busy handler, like
  any other command. A command whose wait times out reports `SQLITE_BUSY`.
- **`dolt_gc` and the remote commands run outside a command transaction.**
  `dolt_gc` needs the lock free, and the remote commands must not hold the
  local lock during a transfer. They take the lock to publish and re-check
  HEAD and the working set before they do, so a peer commit in between
  yields `SQLITE_BUSY` instead of a lost update. Pull also uses
  operation-specific locked branch expectations.
- **A working set is never replaced unread.** Publishing a branch's working
  set is refused with `SQLITE_BUSY_SNAPSHOT` unless the working set it
  replaces is the one this connection last read or wrote. A peer's write
  that this connection has not seen is never overwritten. The check runs at
  publish time, under the lock, for every caller.
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

## Several branches in one transaction

`ATTACH 'file.db/feature' AS f` on a connection that already has `file.db`
open on another branch shares that file's store instead of opening it again.
One transaction can then write both branches: `COMMIT` publishes both
branches' working sets as one root, and `ROLLBACK`, or a failed `COMMIT`,
undoes both. [dolt_nonlocal_tables](dolt_nonlocal_tables.md) uses the same
mechanism.

- Each branch's working set gets its own publish-time check. If a peer wrote
  either branch after this transaction read it, the stale write is refused
  with `database is locked` (`SQLITE_BUSY_SNAPSHOT`) and the peer's rows
  survive.
- `dolt_commit` is refused while another branch of the store has uncommitted
  writes in the transaction, as in Dolt. Other version-control commands run;
  one that commits the caller's transaction publishes every branch's writes
  in the same root.
- `dolt_checkout`, and deleting, renaming, or force-updating a branch, are
  refused for a branch attached in the connection.
- Sharing applies to a different, existing branch of a writable file. Attaching
  a branch the connection already has open, a tag or commit, or a read-only
  connection still opens a separate store, which writes like a peer.
