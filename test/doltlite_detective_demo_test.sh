#!/bin/bash
#
# Runnable companion to doc/doltlite/demo.md — "The DoltHub Break
# Room Incident."
#
# Every SQL block in the demo appears here in the same order, with
# an assertion on its output. If the demo drifts from reality — a
# vtable column gets renamed, a function signature changes, a
# result shape shifts — this test fails, and the demo gets updated
# in the same PR that made the underlying change.
#
# Run: bash test/doltlite_detective_demo_test.sh [path/to/doltlite]
#

set -u

DOLTLITE="${1:-./doltlite}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
DB="$TMPROOT/case.db"
FORENSICS="$TMPROOT/forensics.db"

pass=0; fail=0; FAILED_NAMES=""

pass_name() { pass=$((pass+1)); echo "  PASS: $1"; }
fail_name() {
  fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES $1"
  echo "  FAIL: $1"
}

expect_eq() {
  local name="$1" want="$2" got="$3"
  if [ "$want" = "$got" ]; then pass_name "$name"
  else
    fail_name "$name"
    echo "    want: |$want|"
    echo "    got:  |$got|"
  fi
}

expect_match() {
  local name="$1" pattern="$2" got="$3"
  if echo "$got" | grep -qE "$pattern"; then pass_name "$name"
  else
    fail_name "$name"
    echo "    pattern: |$pattern|"
    echo "    got:     |$got|"
  fi
}

dl() { "$DOLTLITE" "$DB" "$1" 2>&1; }
dl_bulk() { "$DOLTLITE" "$DB" >/dev/null 2>&1; }

echo "=== The DoltHub Break Room Incident — demo test ==="
echo ""

# ── Chapter 1: Opening the case file ─────────────────────
echo "--- Chapter 1: Opening the case file ---"
rm -f "$DB"

"$DOLTLITE" "$DB" >/dev/null 2>&1 <<'SQL'
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
SQL

N_SUSPECTS=$(dl "SELECT count(*) FROM suspects;")
expect_eq "chapter1_suspects_count" "4" "$N_SUSPECTS"

N_COMMITS=$(dl "SELECT count(*) FROM dolt_log;")
expect_eq "chapter1_commit_count" "2" "$N_COMMITS"

echo ""

# ── Chapter 2: Two theories, two branches ────────────────
echo "--- Chapter 2: Two theories, two branches ---"

"$DOLTLITE" "$DB" >/dev/null 2>&1 <<'SQL'
SELECT dolt_branch('theory/butler');
SELECT dolt_checkout('theory/butler');
INSERT INTO sightings VALUES
  (1, 1, 1, '2026-04-15T14:15:00Z', 'coffee machine cam'),
  (2, 1, 1, '2026-04-15T14:22:00Z', 'coffee machine cam');
INSERT INTO evidence VALUES
  (1, 'Butler seen polishing a suspicious croissant', 1, '2026-04-15T15:00:00Z');
SELECT dolt_commit('-Am', 'Butler: opportunity + means');
SELECT dolt_checkout('main');
SELECT dolt_branch('theory/ficus');
SELECT dolt_checkout('theory/ficus');
INSERT INTO sightings VALUES
  (3, 4, 1, '2026-04-15T00:00:00Z', 'janitor');
INSERT INTO evidence VALUES
  (2, 'Ficus placed directly above victim at time of death', 4, '2026-04-15T15:05:00Z'),
  (3, 'Traces of photosynthate on the croissant', 4, '2026-04-15T16:00:00Z');
SELECT dolt_commit('-Am', 'Ficus: opportunity, no alibi');
SELECT dolt_checkout('main');
SQL

BRANCHES=$(dl "SELECT name FROM dolt_branches ORDER BY name;")
expect_match "chapter2_branches_present" "main.*theory/butler.*theory/ficus" "$(echo $BRANCHES)"

BUTLER_EV=$(dl "SELECT count(*) FROM dolt_at_evidence('theory/butler');")
expect_eq "chapter2_butler_evidence_count" "1" "$BUTLER_EV"

FICUS_EV=$(dl "SELECT count(*) FROM dolt_at_evidence('theory/ficus');")
expect_eq "chapter2_ficus_evidence_count" "2" "$FICUS_EV"

echo ""

# ── Chapter 3: Where do we disagree ──────────────────────
echo "--- Chapter 3: Where do we disagree ---"

STAT=$(dl "SELECT rows_added FROM dolt_diff_stat('theory/butler', 'theory/ficus') WHERE table_name='evidence';")
expect_eq "chapter3_diff_stat_evidence_added" "2" "$STAT"

DIFF_ROWS=$(dl "SELECT count(*) FROM dolt_diff_evidence('theory/butler', 'theory/ficus');")
expect_eq "chapter3_diff_evidence_rowcount" "3" "$DIFF_ROWS"

echo ""

# ── Chapter 4: Who added this clue ───────────────────────
echo "--- Chapter 4: Who added this clue ---"

# Merge theory/ficus into main so we have a populated evidence table
# with real commit metadata to blame against.
"$DOLTLITE" "$DB" >/dev/null 2>&1 <<'SQL'
SELECT dolt_checkout('main');
SELECT dolt_merge('theory/ficus');
SQL

BLAME_ROWS=$(dl "SELECT count(*) FROM dolt_blame_evidence;")
expect_eq "chapter4_blame_row_count" "2" "$BLAME_ROWS"

BLAME_MSG=$(dl "SELECT message FROM dolt_blame_evidence WHERE id=2;")
expect_match "chapter4_blame_attributes_ficus_commit" "Ficus" "$BLAME_MSG"

echo ""

# ── Chapter 5: What did we believe at 3pm yesterday ──────
echo "--- Chapter 5: Time travel ---"

# Add new evidence on main, then query what we believed before it.
"$DOLTLITE" "$DB" >/dev/null 2>&1 <<'SQL'
INSERT INTO evidence VALUES
  (4, 'Partial fingerprint on the coffee pot', NULL, '2026-04-16T09:00:00Z');
SELECT dolt_commit('-Am', 'Partial print added');
SQL

NOW_COUNT=$(dl "SELECT count(*) FROM evidence;")
expect_eq "chapter5_current_evidence_count" "3" "$NOW_COUNT"

THEN_COUNT=$(dl "SELECT count(*) FROM dolt_at_evidence('HEAD~1');")
expect_eq "chapter5_historical_evidence_count" "2" "$THEN_COUNT"

echo ""

# ── Chapter 6: The witness lied ──────────────────────────
echo "--- Chapter 6: The witness lied ---"

# Revert the partial-print commit (HEAD).
"$DOLTLITE" "$DB" >/dev/null 2>&1 "SELECT dolt_revert('HEAD');"

AFTER_REVERT=$(dl "SELECT count(*) FROM evidence;")
expect_eq "chapter6_revert_removed_evidence" "2" "$AFTER_REVERT"

FINGERPRINT_GONE=$(dl "SELECT count(*) FROM evidence WHERE id=4;")
expect_eq "chapter6_revert_targeted_correct_row" "0" "$FINGERPRINT_GONE"

echo ""

# ── Chapter 7: The detectives agree ──────────────────────
echo "--- Chapter 7: Clean merge ---"

# Start a fresh parallel theory that touches only sightings (so merge
# is clean versus main's current state), then merge.
"$DOLTLITE" "$DB" >/dev/null 2>&1 <<'SQL'
SELECT dolt_branch('theory/butler_v2');
SELECT dolt_checkout('theory/butler_v2');
INSERT INTO evidence VALUES
  (5, 'Butler cannot account for whereabouts 14:10-14:25', 1, '2026-04-16T11:00:00Z');
SELECT dolt_commit('-Am', 'Butler alibi gap');
SELECT dolt_checkout('main');
SELECT dolt_merge('theory/butler_v2');
SQL

MERGED_COUNT=$(dl "SELECT count(*) FROM evidence;")
expect_eq "chapter7_clean_merge_evidence_count" "3" "$MERGED_COUNT"

echo ""

# ── Chapter 8: The detectives disagree ───────────────────
echo "--- Chapter 8: Row-level merge conflict ---"

# Two branches modify the same evidence row differently → conflict.
"$DOLTLITE" "$DB" >/dev/null 2>&1 <<'SQL'
SELECT dolt_branch('theory/revised_1');
SELECT dolt_checkout('theory/revised_1');
UPDATE evidence SET description='Butler seen polishing the murder weapon' WHERE id=5;
SELECT dolt_commit('-Am', 'Detective A rewords clue');
SELECT dolt_checkout('main');
UPDATE evidence SET description='Butler observed in suspicious coffee-related activity' WHERE id=5;
SELECT dolt_commit('-Am', 'Detective B rewords clue');
SELECT dolt_merge('theory/revised_1');
SQL

CONFLICT_COUNT=$(dl "SELECT num_conflicts FROM dolt_conflicts WHERE \"table\"='evidence';")
expect_eq "chapter8_conflict_detected" "1" "$CONFLICT_COUNT"

# Resolve by taking our side.
"$DOLTLITE" "$DB" >/dev/null 2>&1 "SELECT dolt_conflicts_resolve('--ours','evidence');"

RESOLVED=$(dl "SELECT count(*) FROM dolt_conflicts;")
expect_eq "chapter8_conflict_resolved" "0" "$RESOLVED"

KEPT_OUR_TEXT=$(dl "SELECT description FROM evidence WHERE id=5;")
expect_match "chapter8_resolution_kept_our_text" "suspicious coffee" "$KEPT_OUR_TEXT"

echo ""

# ── Chapter 9: Signing off the case file ─────────────────
echo "--- Chapter 9: Fingerprinting ---"

"$DOLTLITE" "$DB" >/dev/null 2>&1 "SELECT dolt_commit('-Am','Post-conflict reconciliation');"

DB_HASH=$(dl "SELECT dolt_hashof_db();")
expect_match "chapter9_db_hash_is_hex" "^[0-9a-f]{40}$" "$DB_HASH"

EVIDENCE_HASH_A=$(dl "SELECT dolt_hashof_table('evidence');")

# Insert then delete a throwaway row — the table hash must be
# identical to before (history-independence).
"$DOLTLITE" "$DB" >/dev/null 2>&1 <<'SQL'
INSERT INTO evidence VALUES (99, 'ignore me', NULL, '2026-04-16T12:00:00Z');
DELETE FROM evidence WHERE id=99;
SQL
EVIDENCE_HASH_B=$(dl "SELECT dolt_hashof_table('evidence');")
expect_eq "chapter9_table_hash_history_independent" "$EVIDENCE_HASH_A" "$EVIDENCE_HASH_B"

echo ""

# ── Chapter 10: Forensics hands you their evidence ───────
echo "--- Chapter 10: ATTACH a stock SQLite database ---"

rm -f "$FORENSICS"
SQLITE3=$(command -v sqlite3 || echo /usr/bin/sqlite3)
"$SQLITE3" "$FORENSICS" <<'SQL'
CREATE TABLE fingerprints(id INTEGER PRIMARY KEY, suspect_id INTEGER, pattern TEXT, found_on TEXT);
INSERT INTO fingerprints VALUES
  (1, 1, 'whorl',    'coffee pot'),
  (2, 4, 'none',     'victim sleeve'),
  (3, 1, 'swirl',    'croissant wrapper');
SQL

# ATTACH stock sqlite → JOIN across → migrate in → commit.
ATTACHED_COUNT=$("$DOLTLITE" "$DB" "ATTACH DATABASE '$FORENSICS' AS forensics; SELECT count(*) FROM forensics.fingerprints;" 2>&1 | tail -1)
expect_eq "chapter10_attach_read_count" "3" "$ATTACHED_COUNT"

"$DOLTLITE" "$DB" >/dev/null 2>&1 <<SQL
ATTACH DATABASE '$FORENSICS' AS forensics;
CREATE TABLE fingerprints AS SELECT * FROM forensics.fingerprints;
SELECT dolt_commit('-Am','Import forensics fingerprint data');
SQL

MIGRATED=$(dl "SELECT count(*) FROM fingerprints;")
expect_eq "chapter10_migrated_row_count" "3" "$MIGRATED"

BUTLER_PRINTS=$(dl "SELECT count(*) FROM fingerprints f JOIN suspects s ON s.id=f.suspect_id WHERE s.name='The Butler';")
expect_eq "chapter10_cross_table_join_works" "2" "$BUTLER_PRINTS"

echo ""
echo "======================================="
echo "Results: $pass passed, $fail failed"
echo "======================================="
if [ $fail -gt 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
