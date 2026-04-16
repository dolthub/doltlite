# The DoltHub Break Room Incident

> At 14:23 UTC, the body of one Dr. E. Xample was discovered beside the coffee
> machine, clutching a half-eaten croissant and a `sqlite3` man page.
> Foul play is suspected. You are the lead investigator.
> Your evidence log is a doltlite database.

This is a doltlite tutorial disguised as a whodunit. Every SQL block is
runnable — copy-paste into `./doltlite case.db` and the story unfolds
for real. The companion test script `test/doltlite_detective_demo_test.sh`
runs the same SQL with assertions, so the demo never drifts from what
the engine actually does.


## Chapter 1: Opening the case file

Every investigation starts with a schema.

```sql
CREATE TABLE suspects (
  id       INTEGER PRIMARY KEY,
  name     TEXT NOT NULL,
  role     TEXT,
  motive   TEXT
);

CREATE TABLE locations (
  id       INTEGER PRIMARY KEY,
  name     TEXT NOT NULL
);

CREATE TABLE sightings (
  id          INTEGER PRIMARY KEY,
  suspect_id  INTEGER NOT NULL REFERENCES suspects(id),
  location_id INTEGER NOT NULL REFERENCES locations(id),
  at_time     TEXT NOT NULL,
  source      TEXT
);

CREATE TABLE evidence (
  id            INTEGER PRIMARY KEY,
  description   TEXT NOT NULL,
  implicates_id INTEGER REFERENCES suspects(id),
  found_at      TEXT NOT NULL
);
```

The initial canvass fills in our cast of characters:

```sql
INSERT INTO suspects VALUES
  (1, 'The Butler',    'household staff',    'inheritance'),
  (2, 'Dr. Plum',      'visiting scholar',   'academic rivalry'),
  (3, 'Ms. Scarlet',   'lab assistant',      'workplace grievance'),
  (4, 'The Ficus',     'decorative plant',   'photosynthesis');

INSERT INTO locations VALUES
  (1, 'Break room'),
  (2, 'Lab 2'),
  (3, 'Rooftop'),
  (4, 'Server room');

SELECT dolt_commit('-Am', 'Initial canvass');
```

`-Am` stages everything and commits in one shot. The database now has a
root commit. `dolt_log` will show two entries: this commit and the
automatic "Initialize data repository" commit that every doltlite
database starts with.


## Chapter 2: Two theories, two branches

Two investigators, two hypotheses. Each works in isolation on a branch
so neither contaminates the other's evidence log.

```sql
SELECT dolt_branch('theory/butler');
SELECT dolt_checkout('theory/butler');

INSERT INTO sightings VALUES
  (1, 1, 1, '2026-04-15T14:15:00Z', 'coffee machine cam'),
  (2, 1, 1, '2026-04-15T14:22:00Z', 'coffee machine cam');

INSERT INTO evidence VALUES
  (1, 'Butler seen polishing a suspicious croissant', 1, '2026-04-15T15:00:00Z');

SELECT dolt_commit('-Am', 'Butler: opportunity + means');
```

Meanwhile, on the other side of the precinct:

```sql
SELECT dolt_checkout('main');
SELECT dolt_branch('theory/ficus');
SELECT dolt_checkout('theory/ficus');

INSERT INTO sightings VALUES
  (3, 4, 1, '2026-04-15T00:00:00Z', 'janitor');

INSERT INTO evidence VALUES
  (2, 'Ficus placed directly above victim at time of death', 4, '2026-04-15T15:05:00Z'),
  (3, 'Traces of photosynthate on the croissant', 4, '2026-04-15T16:00:00Z');

SELECT dolt_commit('-Am', 'Ficus: opportunity, no alibi');
```

Both branches share the same cast of suspects and locations (inherited
from `main`), but their evidence tables diverge from the moment they
branched. Neither investigator sees the other's work until a merge.

You can verify what each branch knows about the case:

```sql
SELECT count(*) FROM dolt_at_evidence('theory/butler');
-- 1

SELECT count(*) FROM dolt_at_evidence('theory/ficus');
-- 2
```


## Chapter 3: Where do we disagree

Before merging, you want to know how the two theories differ.
`dolt_diff_stat` gives a quick summary:

```sql
SELECT table_name, rows_added, rows_deleted, rows_modified
  FROM dolt_diff_stat('theory/butler', 'theory/ficus');
```

For a row-level view, use the per-table diff vtable with two refs:

```sql
SELECT diff_type, to_id, to_description
  FROM dolt_diff_evidence('theory/butler', 'theory/ficus');
```

This returns every evidence row that exists in `theory/ficus` but not in
`theory/butler` (added), vice versa (deleted), or both but with
different values (modified). The diff is between any two refs — branches,
tags, commit hashes — not just adjacent commits.


## Chapter 4: Who added this clue

The ficus theory has enough evidence to merge into `main`. Let's do that
and then ask blame who contributed what.

```sql
SELECT dolt_checkout('main');
SELECT dolt_merge('theory/ficus');
```

Now `main` has the ficus evidence. `dolt_blame_<table>` tells you, for
every live row, which commit introduced it:

```sql
SELECT id, message, committer
  FROM dolt_blame_evidence;
```

Each row in the blame result corresponds to a row in the evidence table.
The `message` and `committer` columns point at the commit that last
touched that row — so if someone later edits the evidence description,
blame will update to reflect the editing commit, not the original insert.


## Chapter 5: What did we believe at 3pm yesterday

A new lead comes in overnight. You add it:

```sql
INSERT INTO evidence VALUES
  (4, 'Partial fingerprint on the coffee pot', NULL, '2026-04-16T09:00:00Z');

SELECT dolt_commit('-Am', 'Partial print added');
```

But later you need to know what the case file looked like before this
commit — what did we believe yesterday? Use `dolt_at_<table>` with a
ref:

```sql
-- current state
SELECT count(*) FROM evidence;
-- 3

-- state one commit ago
SELECT count(*) FROM dolt_at_evidence('HEAD~1');
-- 2
```

`HEAD~1` means "one commit before HEAD." You can also pass a full commit
hash, a branch name, or a tag. `dolt_history_<table>` gives you every
version of every row across the full commit history:

```sql
SELECT id, description, commit_hash, committer
  FROM dolt_history_evidence
  ORDER BY commit_date DESC;
```


## Chapter 6: The witness lied

The partial fingerprint turns out to be fabricated — the lab report was
faxed from a suspicious number. You want to undo that specific commit
without losing everything that came after it:

```sql
SELECT dolt_revert('HEAD');
```

`dolt_revert` creates a new commit that inverses the target. The
evidence table is back to two rows, but the full history — including
the reverted commit and the revert itself — is preserved in
`dolt_log`. Nothing is destroyed; the investigation is auditable.

If instead you wanted to pull one good commit from a discarded branch
(say, a single witness statement from a theory you otherwise abandoned),
that's `dolt_cherry_pick('commit_hash')`.


## Chapter 7: The detectives agree

The butler theory has developed new evidence independently. Let's merge
it in:

```sql
SELECT dolt_branch('theory/butler_v2');
SELECT dolt_checkout('theory/butler_v2');

INSERT INTO evidence VALUES
  (5, 'Butler cannot account for whereabouts 14:10-14:25', 1, '2026-04-16T11:00:00Z');

SELECT dolt_commit('-Am', 'Butler alibi gap');

SELECT dolt_checkout('main');
SELECT dolt_merge('theory/butler_v2');
```

The merge is clean: `theory/butler_v2` added a row, `main` didn't touch
the same data, so doltlite does a three-way merge and creates a merge
commit automatically. The evidence table now has entries from both
theories — the combined case file.


## Chapter 8: The detectives disagree

Not every merge is clean. If two investigators independently rewrite the
same evidence row, doltlite can't pick a winner automatically — that's a
row-level merge conflict.

Set it up:

```sql
SELECT dolt_branch('theory/revised_1');
SELECT dolt_checkout('theory/revised_1');

UPDATE evidence SET description='Butler seen polishing the murder weapon' WHERE id=5;
SELECT dolt_commit('-Am', 'Detective A rewords clue');

SELECT dolt_checkout('main');

UPDATE evidence SET description='Butler observed in suspicious coffee-related activity' WHERE id=5;
SELECT dolt_commit('-Am', 'Detective B rewords clue');

SELECT dolt_merge('theory/revised_1');
-- Merge has 1 conflict(s). Resolve and then commit with dolt_commit.
```

The merge halts. Inspect the damage:

```sql
SELECT * FROM dolt_conflicts;
-- evidence | 1

SELECT our_v, their_v, our_diff_type
  FROM dolt_conflicts_evidence;
```

Resolve by keeping your version:

```sql
SELECT dolt_conflicts_resolve('--ours', 'evidence');
SELECT dolt_commit('-m', 'Resolved: kept our wording');
```

Or delete individual conflict rows from `dolt_conflicts_evidence` for
fine-grained control. Either way, `dolt_commit` refuses to proceed
until every conflict is resolved — you can't accidentally commit a
half-merged case file.


## Chapter 9: Signing off the case file

Before handing the case file to the prosecution, you want a tamper-proof
fingerprint. `dolt_hashof_db` produces a single 40-character hex hash
over the entire database state:

```sql
SELECT dolt_hashof_db();
-- e.g. 872d5b060f294519e9cafc7ad97a2578aa8cdb98
```

The hash is content-addressed and history-independent: two databases that
contain the same rows hash identically, regardless of how they got there
(different insert order, transient deletions, different branch
structure). If forensics has a copy and their hash matches yours, you're
looking at the same data.

Per-table hashes work the same way:

```sql
SELECT dolt_hashof_table('evidence');
```

Insert a row and delete it — the hash returns to its original value,
because the content is back to what it was. This is structural sharing
at work: the prolly tree deduplicates at the chunk level, so "same
data" literally means "same bytes on disk."


## Chapter 10: Forensics hands you their evidence

The forensics lab has their own database — a plain SQLite file, no
version control. They give you a file called `forensics.db` with a
`fingerprints` table. You want to pull it into the case file without
copying data by hand.

```sql
ATTACH DATABASE 'forensics.db' AS forensics;

-- Check what they have
SELECT * FROM forensics.fingerprints;

-- Join across the two databases
SELECT s.name, f.pattern, f.found_on
  FROM forensics.fingerprints f
  JOIN suspects s ON s.id = f.suspect_id;
```

To bring the data under version control, migrate it:

```sql
CREATE TABLE fingerprints AS SELECT * FROM forensics.fingerprints;
SELECT dolt_commit('-Am', 'Import forensics fingerprint data');
```

The `fingerprints` table is now a first-class versioned table in the
case file. Future changes to it show up in `dolt_diff`, `dolt_blame`,
and `dolt_history` like any other table. The original SQLite file is
untouched.

This is the unique doltlite move: any SQLite database can be ATTACHed
read-only, JOINed against versioned data, and selectively migrated in.
Your version-controlled case file coexists with legacy databases that
weren't designed for version control.


## The verdict

The Butler did it. Obviously. Plants can't commit crimes.

```sql
SELECT name, motive FROM suspects WHERE id = 1;
-- The Butler | inheritance
```

Here's everything you used to close the case:

| What you did | SQL |
|---|---|
| First commit | `SELECT dolt_commit('-Am', 'msg')` |
| Branch a theory | `SELECT dolt_branch('theory/butler')` |
| Switch theories | `SELECT dolt_checkout('theory/ficus')` |
| Compare theories | `SELECT * FROM dolt_diff_evidence('ref1', 'ref2')` |
| Quick diff summary | `SELECT * FROM dolt_diff_stat('ref1', 'ref2')` |
| Who added this clue | `SELECT * FROM dolt_blame_evidence` |
| What did we know then | `SELECT * FROM dolt_at_evidence('HEAD~3')` |
| Full row history | `SELECT * FROM dolt_history_evidence` |
| Undo a bad commit | `SELECT dolt_revert('HEAD')` |
| Merge two theories | `SELECT dolt_merge('theory/butler')` |
| Resolve a conflict | `SELECT dolt_conflicts_resolve('--ours', 'evidence')` |
| Tamper-proof fingerprint | `SELECT dolt_hashof_db()` |
| Import a plain SQLite DB | `ATTACH 'file.db' AS x; CREATE TABLE t AS SELECT * FROM x.t` |
| Commit log | `SELECT * FROM dolt_log` |
| Tag a milestone | `SELECT dolt_tag('v1.0')` |
| Cherry-pick one commit | `SELECT dolt_cherry_pick('hash')` |

The full test script that runs this demo end-to-end:
`test/doltlite_detective_demo_test.sh`
