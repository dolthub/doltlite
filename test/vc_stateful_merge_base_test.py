#!/usr/bin/env python3
"""The stateful fuzzer's merge model must agree with the engine on histories
with several best common ancestors, where the engine merges from a virtual
base built from all of them. Fail-before: the model read one ancestor from
dolt_merge_base() and predicted an undone edit would reappear."""
import importlib.util
import pathlib
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("fuzz", ROOT / "vc_stateful_fuzzer.py")
fuzz = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fuzz)

DOLTLITE = str(pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "./doltlite").resolve())
failures = 0

CRISS_CROSS = """
CREATE TABLE kv(id INTEGER PRIMARY KEY, v TEXT, n INT);
INSERT INTO kv VALUES(1,'base',0),(2,'base',0);
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('side');
UPDATE kv SET v='left' WHERE id=1;
SELECT dolt_commit('-am','left');
SELECT dolt_tag('left_tip');
SELECT dolt_checkout('side');
UPDATE kv SET v='right' WHERE id=2;
SELECT dolt_commit('-am','right');
SELECT dolt_checkout('main');
SELECT dolt_merge('side','--no-ff','-m','merge-right');
SELECT dolt_checkout('side');
SELECT dolt_merge('left_tip','--no-ff','-m','merge-left');
SELECT dolt_checkout('main');
"""

# The cross merges resolve a real conflict, so the two best common ancestors
# themselves conflict and the engine refuses to build a virtual base.
CONFLICTING_BASES = """
CREATE TABLE kv(id INTEGER PRIMARY KEY, v TEXT, n INT);
INSERT INTO kv VALUES(1,'base',0);
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('side');
UPDATE kv SET v='left' WHERE id=1;
SELECT dolt_commit('-am','left');
SELECT dolt_tag('left_tip');
SELECT dolt_checkout('side');
UPDATE kv SET v='right' WHERE id=1;
SELECT dolt_commit('-am','right');
SELECT dolt_tag('right_tip');
SELECT dolt_checkout('main');
BEGIN;
SELECT dolt_merge('right_tip');
SELECT dolt_conflicts_resolve('--ours','kv');
SELECT dolt_commit('-am','merge-right');
COMMIT;
SELECT dolt_checkout('side');
BEGIN;
SELECT dolt_merge('left_tip');
SELECT dolt_conflicts_resolve('--ours','kv');
SELECT dolt_commit('-am','merge-left');
COMMIT;
SELECT dolt_checkout('main');
UPDATE kv SET n=1;
SELECT dolt_commit('-am','main-after');
"""


def check(name, setup, ours, theirs, setup_conflicts=0):
    """setup_conflicts: merges in the setup that stop on a conflict, which
    the setup then resolves inside its transaction."""
    global failures
    with tempfile.TemporaryDirectory() as tmp:
        db = str(pathlib.Path(tmp) / "m.db")
        rc, output = fuzz.run_script(DOLTLITE, db, setup, name + "_setup")
        if output.count("Merge has 1 conflict(s)") != setup_conflicts or (
                setup_conflicts == 0 and rc != 0):
            print("FAIL: %s setup\n%s" % (name, output))
            failures += 1
            return
        ours_rows = fuzz.query_committed_rows(DOLTLITE, db, ours)
        theirs_rows = fuzz.query_committed_rows(DOLTLITE, db, theirs)
        predicted, conflict, ff = fuzz.merge_prediction(
            DOLTLITE, db, ours, ours_rows, theirs, theirs_rows)
        rc, output = fuzz.run_script(
            DOLTLITE, fuzz.db_for_branch(db, ours),
            "SELECT dolt_merge(%s);" % fuzz.sql_quote(theirs), name + "_merge")
        actual = fuzz.query_rows(DOLTLITE, db, ours)
        if conflict:
            ok = rc != 0 and fuzz.refused(output, fuzz.MERGE_ROLLED_BACK) \
                and actual == ours_rows
        else:
            ok = rc == 0 and actual == predicted
        print("%s: %s predicted=%r conflict=%s actual=%r" % (
            "PASS" if ok else "FAIL", name, predicted, conflict, actual))
        if not ok:
            print(output)
            failures += 1


check("criss_cross_undo_main", CRISS_CROSS + """
UPDATE kv SET v='base';
SELECT dolt_commit('-am','undo-both');
""", "main", "side")
check("criss_cross_undo_side", CRISS_CROSS + """
SELECT dolt_checkout('side');
UPDATE kv SET v='base';
SELECT dolt_commit('-am','undo-both');
SELECT dolt_checkout('main');
""", "main", "side")
check("criss_cross_edit_both", CRISS_CROSS + """
UPDATE kv SET n=5 WHERE id=1;
SELECT dolt_commit('-am','main-n');
SELECT dolt_checkout('side');
UPDATE kv SET n=7 WHERE id=2;
SELECT dolt_commit('-am','side-n');
SELECT dolt_checkout('main');
""", "main", "side")
check("single_base", """
CREATE TABLE kv(id INTEGER PRIMARY KEY, v TEXT, n INT);
INSERT INTO kv VALUES(1,'base',0),(2,'base',0);
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('side');
UPDATE kv SET v='left' WHERE id=1;
SELECT dolt_commit('-am','left');
SELECT dolt_checkout('side');
UPDATE kv SET v='right' WHERE id=2;
SELECT dolt_commit('-am','right');
SELECT dolt_checkout('main');
""", "main", "side")
check("fast_forward", """
CREATE TABLE kv(id INTEGER PRIMARY KEY, v TEXT, n INT);
INSERT INTO kv VALUES(1,'base',0);
SELECT dolt_commit('-Am','base');
SELECT dolt_branch('side');
SELECT dolt_checkout('side');
UPDATE kv SET v='side';
SELECT dolt_commit('-am','side');
SELECT dolt_checkout('main');
""", "main", "side")
check("conflicting_bases", CONFLICTING_BASES, "main", "side", setup_conflicts=2)


def check_fetched():
    """Commits only a fetch brought in are reached by no local branch; the
    model must still find their parents to place the merge base."""
    global failures
    with tempfile.TemporaryDirectory() as tmp:
        remote = str(pathlib.Path(tmp) / "r.db")
        local = str(pathlib.Path(tmp) / "l.db")
        fuzz.run_sql(DOLTLITE, remote, """
CREATE TABLE kv(id INTEGER PRIMARY KEY, v TEXT, n INT);
INSERT INTO kv VALUES(1,'base',0),(2,'base',0);
SELECT dolt_commit('-Am','base');
""", "fetched_remote")
        fuzz.run_sql(DOLTLITE, local, (
            "SELECT dolt_remote('add','origin',%s);\n"
            "SELECT dolt_fetch('origin');\n"
            "SELECT dolt_reset('--hard','origin/main');\n"
            "UPDATE kv SET v='local' WHERE id=1;\n"
            "SELECT dolt_commit('-am','local');\n") % fuzz.sql_quote("file://" + remote),
            "fetched_local")
        fuzz.run_sql(DOLTLITE, remote, """
UPDATE kv SET v='remote' WHERE id=2;
SELECT dolt_commit('-am','remote-1');
UPDATE kv SET n=9 WHERE id=2;
SELECT dolt_commit('-am','remote-2');
""", "fetched_remote_more")
        fuzz.run_sql(DOLTLITE, local, "SELECT dolt_fetch('origin');", "fetched_fetch")
        ours_rows = fuzz.query_committed_rows(DOLTLITE, local, "main")
        theirs_rows = fuzz.query_revision_rows(DOLTLITE, local, "origin/main")
        predicted, conflict, ff = fuzz.merge_prediction(
            DOLTLITE, local, "main", ours_rows, "origin/main", theirs_rows)
        rc, output = fuzz.run_script(
            DOLTLITE, local, "SELECT dolt_merge('origin/main');", "fetched_merge")
        actual = fuzz.query_rows(DOLTLITE, local, "main")
        ok = not conflict and rc == 0 and actual == predicted
        print("%s: fetched_tracking_ref predicted=%r actual=%r" % (
            "PASS" if ok else "FAIL", predicted, actual))
        if not ok:
            print(output)
            failures += 1


check_fetched()

print("Results: %s" % ("all passed" if failures == 0 else "%d failed" % failures))
sys.exit(1 if failures else 0)
