#!/usr/bin/env python3
import importlib.util
import pathlib
import shutil
import sys
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("fuzzer", ROOT / "vc_stateful_fuzzer.py")
fuzzer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fuzzer)
DOLTLITE = str(pathlib.Path(sys.argv.pop(1)).resolve())


class MergeChoice:
    def __init__(self, kind):
        self.branches = iter(("main", "side"))
        self.kind = kind

    def choice(self, choices):
        branch = next(self.branches)
        assert branch in choices
        return branch

    def randrange(self, limit):
        assert 0 <= self.kind < limit
        return self.kind


class PullChoice:
    def choice(self, choices):
        assert "main" in choices
        return "main"

    def randrange(self, limit):
        assert limit == 2
        return 1


class GraphModelTest(unittest.TestCase):
    def model(self, parents, rows, objects=None, dates=None):
        return fuzzer.MergeBaseModel(
            parents,
            lambda h: (rows[h], set((objects or {}).get(h, ()))),
            lambda h: (dates or {}).get(h, 0))

    def test_all_best_bases_exclude_older_common_ancestors(self):
        parents = {"r": (), "a": ("r",), "b": ("r",),
                   "x": ("a", "b", "r"), "y": ("b", "a")}
        rows = {"r": {1: (0, 0)}, "a": {1: (1, 0)}, "b": {1: (0, 2)}}
        m = self.model(parents, rows)
        base = m.base("x", "y")
        self.assertEqual(m.snapshot(base), ({1: (1, 2)}, set()))
        self.assertEqual(m.ancestors(base), {base, "a", "b", "r"})
        self.assertEqual(m.base("y", "x"), base)

    def test_three_bases_fold_through_the_virtual_parents(self):
        parents = {"r": (), "a": ("r",), "b": ("r",), "c": ("r",),
                   "x": ("a", "b", "c"), "y": ("c", "a", "b")}
        rows = {"r": {}, "a": {1: (1, 0)}, "b": {2: (2, 0)}, "c": {3: (3, 0)}}
        objects = {"a": {("view", "v")}, "b": {("trigger", "t")}}
        m = self.model(parents, rows, objects, {"a": 3, "b": 1, "c": 2})
        base = m.base("x", "y")
        self.assertEqual(base, (("b", "c"), "a"))
        self.assertEqual(m.snapshot(base),
                         ({1: (1, 0), 2: (2, 0), 3: (3, 0)},
                          {("view", "v"), ("trigger", "t")}))

    def test_equal_dates_break_ties_by_hash(self):
        m = self.model({"r": (), "b": ("r",), "a": ("r",),
                        "x": ("b", "a"), "y": ("a", "b")},
                       {"r": {}, "a": {}, "b": {}})
        self.assertEqual(m.base("x", "y"), ("a", "b"))

    def test_conflicting_bases_cannot_supply_a_success_prediction(self):
        m = self.model({"r": (), "a": ("r",), "b": ("r",),
                        "x": ("a", "b"), "y": ("b", "a")},
                       {"r": {1: (0, 0)}, "a": {1: (1, 0)}, "b": {1: (2, 0)}})
        with self.assertRaises(fuzzer.VirtualBaseConflict):
            m.base("x", "y")

    def test_unrelated_roots_are_not_an_empty_successful_base(self):
        m = self.model({"a": (), "b": ()}, {"a": {}, "b": {}})
        with self.assertRaisesRegex(AssertionError, "no common ancestor"):
            m.base("a", "b")


class MergePredictionTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="vc-merge-model-")
        self.addCleanup(self.tmp.cleanup)
        self.db = str(pathlib.Path(self.tmp.name) / "db")

    def sql(self, sql):
        return fuzzer.run_sql(DOLTLITE, self.db, sql, "model_test_setup")

    def history(self, levels=1, objects=False):
        statements = ["""
            CREATE TABLE kv(id INTEGER PRIMARY KEY,v TEXT,n INT);
            INSERT INTO kv VALUES(1,'base',0),(2,'base',0);
            SELECT dolt_commit('-Am','base');
            SELECT dolt_branch('side');
        """]
        for level in range(1, levels + 1):
            statements.append("SELECT dolt_checkout('main');"
                              "UPDATE kv SET v='left%d' WHERE id=1;" % level)
            if objects and level == 1:
                statements.append("""
                    CREATE VIEW v_main AS SELECT * FROM kv;
                    CREATE TRIGGER tr_main AFTER INSERT ON kv BEGIN SELECT 1; END;
                """)
            statements.append("SELECT dolt_commit('-am','left%d');"
                     "SELECT dolt_tag('left%d');"
                     "SELECT dolt_checkout('side');"
                     "UPDATE kv SET v='right%d' WHERE id=2;" % (level, level, level))
            if objects and level == 1:
                statements.append("""
                    CREATE VIEW v_side AS SELECT * FROM kv;
                    CREATE TRIGGER tr_side AFTER INSERT ON kv BEGIN SELECT 1; END;
                """)
            statements.append("SELECT dolt_commit('-am','right%d');"
                     "SELECT dolt_checkout('main');"
                     "SELECT dolt_merge('side','--no-ff');"
                     "SELECT dolt_checkout('side');"
                     "SELECT dolt_merge('left%d','--no-ff');"
                     "SELECT dolt_checkout('main');" % (level, level))
        self.sql("\n".join(statements))

    def rows(self, branch):
        return fuzzer.query_rows(DOLTLITE, self.db, branch)

    def prediction(self, source="side"):
        return fuzzer.merge_prediction(
            DOLTLITE, self.db, "main", self.rows("main"), source, self.rows(source))

    def test_undo_both_sides_of_recursive_bases(self):
        for levels in (1, 2, 3):
            with self.subTest(levels=levels):
                self.db = str(pathlib.Path(self.tmp.name) / ("levels%d" % levels))
                self.history(levels)
                self.sql("UPDATE kv SET v='base'; SELECT dolt_commit('-am','undo both');")
                expected = {1: ("base", 0), 2: ("base", 0)}
                self.assertEqual(self.prediction(), (expected, False, False))
                self.sql("SELECT dolt_merge('side');")
                self.assertEqual(self.rows("main"), expected)

    def test_merge_modes_use_recursive_schema_base(self):
        for kind in range(5):
            with self.subTest(kind=kind):
                self.db = str(pathlib.Path(self.tmp.name) / ("mode%d" % kind))
                self.history(objects=True)
                self.sql("""
                    DROP VIEW v_main; DROP VIEW v_side;
                    DROP TRIGGER tr_main; DROP TRIGGER tr_side;
                    SELECT dolt_commit('-am','drop both');
                """)
                model = {b: fuzzer.clean_state(self.rows(b)) for b in ("main", "side")}
                fuzzer.merge_branch(DOLTLITE, self.db, list(model), model, MergeChoice(kind))
                self.assertEqual(fuzzer.schema_objects(DOLTLITE, self.db, "main"), [])

    def test_fast_forward_and_already_up_to_date(self):
        self.sql("""
            CREATE TABLE kv(id INT PRIMARY KEY,v TEXT,n INT);
            INSERT INTO kv VALUES(1,'base',0);
            SELECT dolt_commit('-Am','base'); SELECT dolt_branch('side');
            SELECT dolt_checkout('side'); INSERT INTO kv VALUES(2,'side',1);
            SELECT dolt_commit('-am','side'); SELECT dolt_checkout('main');
        """)
        expected = {1: ("base", 0), 2: ("side", 1)}
        self.assertEqual(self.prediction(), (expected, False, True))
        self.sql("SELECT dolt_merge('side');")
        self.assertEqual(self.prediction(), (expected, False, False))

    def test_final_row_conflict_is_not_a_success(self):
        self.history()
        self.sql("""
            UPDATE kv SET v='ours' WHERE id=1; SELECT dolt_commit('-am','ours');
            SELECT dolt_checkout('side'); UPDATE kv SET v='theirs' WHERE id=1;
            SELECT dolt_commit('-am','theirs'); SELECT dolt_checkout('main');
        """)
        self.assertTrue(self.prediction()[1])
        before = self.rows("main")
        rc, output = fuzzer.run_script(DOLTLITE, self.db, "SELECT dolt_merge('side');", "conflict")
        self.assertNotEqual(rc, 0)
        self.assertIn("conflict", output)
        self.assertEqual(self.rows("main"), before)

    def test_three_best_common_ancestors(self):
        self.sql("""
            CREATE TABLE kv(id INT PRIMARY KEY,v TEXT,n INT);
            INSERT INTO kv VALUES(1,'base',0),(2,'base',0),(3,'base',0);
            SELECT dolt_commit('-Am','base');
            SELECT dolt_branch('b'); SELECT dolt_branch('c');
            UPDATE kv SET v='a' WHERE id=1;
            SELECT dolt_commit('-am','a'); SELECT dolt_tag('a');
            SELECT dolt_checkout('b'); UPDATE kv SET v='b' WHERE id=2;
            SELECT dolt_commit('-am','b'); SELECT dolt_tag('b_tip');
            SELECT dolt_checkout('c'); UPDATE kv SET v='c' WHERE id=3;
            SELECT dolt_commit('-am','c'); SELECT dolt_tag('c_tip');
            SELECT dolt_checkout('main');
            SELECT dolt_merge('b'); SELECT dolt_merge('c');
            SELECT dolt_checkout('b');
            SELECT dolt_merge('c_tip'); SELECT dolt_merge('a');
            SELECT dolt_checkout('main'); UPDATE kv SET v='base';
            SELECT dolt_commit('-am','undo all three');
        """)
        expected = {i: ("base", 0) for i in (1, 2, 3)}
        self.assertEqual(self.prediction("b"), (expected, False, False))
        self.sql("SELECT dolt_merge('b');")
        self.assertEqual(self.rows("main"), expected)

    def test_head_outside_branch_and_tag_roots(self):
        self.sql("""
            CREATE TABLE kv(id INT PRIMARY KEY,v TEXT,n INT);
            INSERT INTO kv VALUES(1,'base',0),(2,'base',0);
            SELECT dolt_commit('-Am','base'); SELECT dolt_branch('side');
            UPDATE kv SET v='ours' WHERE id=1; SELECT dolt_commit('-am','ours');
            SELECT dolt_checkout('side'); UPDATE kv SET v='theirs' WHERE id=2;
            SELECT dolt_commit('-am','theirs'); SELECT dolt_checkout('main');
        """)
        source = fuzzer.branch_hash(DOLTLITE, self.db, "side")
        source_rows = self.rows("side")
        self.sql("SELECT dolt_branch('-D','side');")
        predicted = fuzzer.merge_prediction(DOLTLITE, self.db, "main", self.rows("main"),
                                             source, source_rows)
        expected = {1: ("ours", 0), 2: ("theirs", 0)}
        self.assertEqual(predicted, (expected, False, False))
        self.sql("SELECT dolt_merge('%s');" % source)
        self.assertEqual(self.rows("main"), expected)

    def test_base_dates_are_read_outside_main_history(self):
        self.history()
        left = fuzzer.branch_hash(DOLTLITE, self.db, "main")
        right = fuzzer.branch_hash(DOLTLITE, self.db, "side")
        root = fuzzer.query_scalar(DOLTLITE, self.db, "main",
                                   "SELECT commit_hash FROM dolt_log WHERE message='base';", "base")
        self.sql("SELECT dolt_branch('target'); SELECT dolt_reset('--hard','%s');" % root)
        graph = fuzzer.read_merge_model(DOLTLITE, self.db, left, right)
        base = graph.base(left, right)
        self.assertIsInstance(base, tuple)
        for commit in graph.parents[base]:
            expected = fuzzer.query_scalar(DOLTLITE, self.db, "target",
                                            "SELECT date FROM dolt_log WHERE commit_hash='%s';"
                                            % commit, "base_date")
            self.assertTrue(expected)
            self.assertEqual(graph.date(commit), expected)

    def test_virtual_base_conflict_preserves_the_working_set(self):
        self.sql("""
            CREATE TABLE kv(id INT PRIMARY KEY,v TEXT,n INT);
            INSERT INTO kv VALUES(1,'base',0);
            SELECT dolt_commit('-Am','base'); SELECT dolt_branch('side');
            UPDATE kv SET v='ours'; SELECT dolt_commit('-am','ours');
            SELECT dolt_tag('ours'); SELECT dolt_checkout('side');
            UPDATE kv SET v='theirs'; SELECT dolt_commit('-am','theirs');
            SELECT dolt_checkout('main');
        """)
        for branch, source in (("main", "side"), ("side", "ours")):
            rc, output = fuzzer.run_script(
                DOLTLITE, fuzzer.db_for_branch(self.db, branch),
                "BEGIN;\nSELECT dolt_merge('%s');\n"
                "SELECT dolt_conflicts_resolve('--ours','kv');\n"
                "SELECT dolt_commit('-am','resolved');\nCOMMIT;" % source,
                "resolved_cross_merge")
            self.assertNotEqual(rc, 0)
            self.assertIn("Merge has 1 conflict", output)
        left = fuzzer.branch_hash(DOLTLITE, self.db, "main")
        right = fuzzer.branch_hash(DOLTLITE, self.db, "side")
        graph = fuzzer.read_merge_model(DOLTLITE, self.db, left, right)
        with self.assertRaises(fuzzer.VirtualBaseConflict):
            graph.base(left, right)
        self.assertTrue(self.prediction()[1])
        before = fuzzer.kv_state(DOLTLITE, self.db, "main")
        rc, output = fuzzer.run_script(DOLTLITE, self.db, "SELECT dolt_merge('side');",
                                      "virtual_conflict")
        self.assertNotEqual(rc, 0)
        self.assertIn("conflict", output)
        self.assertEqual(fuzzer.kv_state(DOLTLITE, self.db, "main"), before)
        self.assertEqual(fuzzer.branch_hash(DOLTLITE, self.db, "main"), left)

    def test_pull_uses_the_recursive_prediction(self):
        self.history()
        self.sql("UPDATE kv SET v='base'; SELECT dolt_commit('-am','undo both');")
        remote = pathlib.Path(self.tmp.name) / "remote.db"
        shutil.copyfile(self.db, remote)
        self.sql("SELECT dolt_remote('add','origin',%s);"
                 "SELECT dolt_push('--force','origin','side:main');" % fuzzer.sql_quote(remote.as_uri()))
        source = fuzzer.branch_hash(DOLTLITE, self.db, "side")
        source_rows = self.rows("side")
        model = {"main": fuzzer.clean_state(self.rows("main"))}
        pushed = {"main": (source, source_rows)}
        fuzzer.remote_operation(DOLTLITE, self.db, ["main"], model, pushed, PullChoice(), 0, "pull")
        expected = {1: ("base", 0), 2: ("base", 0)}
        self.assertEqual(model["main"], fuzzer.clean_state(expected))
        self.assertEqual(fuzzer.kv_state(DOLTLITE, self.db, "main"), fuzzer.clean_state(expected))

    def test_missing_row_still_fails_the_verdict(self):
        self.history()
        self.sql("UPDATE kv SET v='base'; SELECT dolt_commit('-am','undo both');")
        expected, conflict, _ = self.prediction()
        self.assertFalse(conflict)
        self.sql("SELECT dolt_merge('side');")
        expected.pop(1)
        with self.assertRaisesRegex(AssertionError, "matched no predicted outcome"):
            fuzzer.expect_one_of(DOLTLITE, self.db, "main", {},
                                 [("merged", fuzzer.clean_state(expected))], "bad model", "")


if __name__ == "__main__":
    unittest.main()
