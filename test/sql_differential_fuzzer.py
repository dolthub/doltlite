#!/usr/bin/env python3
"""Random SQL for differential testing against stock SQLite.

No dolt_*, no rowid identity, no physical row order. Results are aggregates
or totally ordered. Interleave reads with writes in BEGIN/SAVEPOINT.

Usage: sql_differential_fuzzer.py SEED [--include-<group>]... [--all]
Groups: large-ints desc expr agg setops cte window joins writesel ddl
        constraints triggers returning generated fkeys
"""

import random
import sys

GROUPS = ["large-ints", "desc", "expr", "agg", "setops", "cte", "window",
          "joins", "writesel", "ddl", "constraints", "triggers",
          "returning", "generated", "fkeys"]

# Integers past 2^53 (and INT64 extremes) use a longer numeric sort key.
LARGE_INTS = [
    9007199254740992, 9007199254740993, 9007199254740994, 9007199254740995,
    -9007199254740992, -9007199254740993, -9007199254740994,
    18014398509481983, 18014398509481984, 18014398509481985,
    9223372036854775807, 9223372036854775806,
    -9223372036854775808, -9223372036854775807,
]

TEXTS = ["''", "'a'", "'A'", "'ab'", "'AB '", "'b'", "'z'", "'zz'",
         "'a' || char(0) || 'b'", "'  pad  '", "x'00'", "x'0001'", "x'ff'"]

# quote() so 2, 2.0, and '2' stay distinguishable.
Q = "coalesce(quote(%s), 'N')"


class Gen:
    def __init__(self, seed, groups):
        self.r = random.Random(seed)
        self.g = set(groups)
        self.out = []
        self.in_txn = False
        self.savepoints = []
        self.ncols = 3          # k/j, a, b -- grows if ddl adds one
        self.added_col = False
        self.has_child = False  # fkeys built a table referencing t

    def on(self, name):
        return name in self.g

    def emit(self, s):
        self.out.append(s)

    def int_val(self):
        if self.on("large-ints") and self.r.random() < 0.35:
            return str(self.r.choice(LARGE_INTS))
        return str(self.r.randint(-40, 40))

    def text_val(self):
        return self.r.choice(TEXTS)

    def val(self, kind="any"):
        if kind == "int":
            return self.int_val()
        if kind == "text":
            return self.text_val()
        r = self.r.random()
        if r < 0.15:
            return "NULL"
        if r < 0.5:
            return self.int_val()
        if r < 0.8:
            return self.text_val()
        return "%.3f" % self.r.uniform(-50, 50)

    def expr(self):
        if not self.on("expr"):
            return self.val()
        r = self.r.random()
        if r < 0.2:
            return "CASE WHEN a IS NULL THEN %s WHEN b > %s THEN %s ELSE %s END" % (
                self.val(), self.text_val(), self.val(), self.val())
        if r < 0.35:
            return "coalesce(a, %s)" % self.val()
        if r < 0.5:
            return "substr(coalesce(quote(b),'x'), 1, %d)" % self.r.randint(1, 4)
        if r < 0.6:
            return "CAST(%s AS %s)" % (self.val(),
                                       self.r.choice(["INTEGER", "TEXT", "REAL"]))
        if r < 0.7:
            return "length(coalesce(quote(b),''))"
        if r < 0.8:
            return "nullif(%s, %s)" % (self.val(), self.val())
        if r < 0.9:
            return "abs(length(coalesce(quote(a),'')) - %d)" % self.r.randint(0, 5)
        return self.val()

    def schema(self):
        coll = self.r.choice(["", "", " COLLATE NOCASE", " COLLATE RTRIM"])
        shape = self.r.choice([
            "int_pk", "int_pk", "text_pk", "numeric_pk",
            "composite_pk", "no_pk", "unique_only",
        ])
        self.key_kind = "int"
        wr = ""
        extra = ""
        if self.on("constraints"):
            extra = self.r.choice(
                ["", "", ", CHECK (a IS NULL OR length(coalesce(quote(a),'')) < 40)",
                 ", CHECK (k IS NOT NULL)"])
        bdecl = "b TEXT%s" % coll
        if self.on("constraints") and self.r.random() < 0.25:
            bdecl = "b TEXT%s DEFAULT 'dflt'" % coll
        # Generated column recomputed while a scan of the row is open.
        self.gen_kind = ""
        if self.on("generated"):
            self.gen_kind = self.r.choice(["", "VIRTUAL", "STORED"])
            if self.gen_kind:
                bdecl += (", g AS (length(coalesce(quote(b),'')) + "
                          "coalesce(a, 0)) %s" % self.gen_kind)
        if shape == "int_pk":
            cols = "k INTEGER PRIMARY KEY, a, %s" % bdecl
        elif shape == "text_pk":
            cols = "k TEXT PRIMARY KEY%s, a, %s" % (coll, bdecl)
            self.key_kind = "text"
            wr = " WITHOUT ROWID" if self.r.random() < 0.5 else ""
        elif shape == "numeric_pk":
            cols = "k NUMERIC PRIMARY KEY, a, %s" % bdecl
            wr = " WITHOUT ROWID" if self.r.random() < 0.5 else ""
        elif shape == "composite_pk":
            cols = "k INTEGER, j TEXT%s, a, %s, PRIMARY KEY(k, j)" % (coll, bdecl)
            wr = " WITHOUT ROWID" if self.r.random() < 0.5 else ""
        elif shape == "unique_only":
            cols = "k INTEGER UNIQUE, a, %s" % bdecl
        else:
            cols = "k INTEGER, a, %s" % bdecl
        self.shape = shape
        self.emit("CREATE TABLE t(%s%s)%s;" % (cols, extra, wr))

        if self.r.random() < 0.7:
            d = " DESC" if (self.on("desc") and self.r.random() < 0.5) else ""
            self.emit("CREATE INDEX i_a ON t(a%s);" % d)
        if self.r.random() < 0.4:
            self.emit("CREATE INDEX i_b ON t(b, a);")
        if self.r.random() < 0.3 and shape != "unique_only":
            self.emit("CREATE UNIQUE INDEX i_u ON t(b, k);")
        if self.on("ddl"):
            if self.r.random() < 0.4:
                # Partial index: rows enter and leave as a changes.
                self.emit("CREATE INDEX i_p ON t(a) WHERE a IS NOT NULL;")
            if self.r.random() < 0.4:
                self.emit("CREATE INDEX i_e ON t(length(coalesce(quote(b),'')));")
            if self.r.random() < 0.3:
                self.emit("CREATE VIEW v AS SELECT k, a, b FROM t "
                          "WHERE a IS NOT NULL;")
        if self.on("joins"):
            self.emit("CREATE TABLE s(id INTEGER PRIMARY KEY, v, w TEXT);")
            for i in range(self.r.randint(2, 6)):
                self.emit("INSERT INTO s(id, v, w) VALUES(%d, %s, %s);"
                          % (i, self.val(), self.text_val()))
        if self.on("triggers"):
            # No UNIQUE on n: trigger INSERT OR IGNORE still aborts under a plain INSERT.
            self.emit("CREATE TABLE u(n INTEGER, tag TEXT);")
            # Trigger bodies write the pending map while t is still being scanned.
            self.emit("CREATE TRIGGER tr_i AFTER INSERT ON t BEGIN "
                      "INSERT INTO u(n, tag) "
                      "VALUES(length(coalesce(quote(NEW.b),'')), 'i'); END;")
            self.emit("CREATE TRIGGER tr_u AFTER UPDATE ON t BEGIN "
                      "INSERT INTO u(n, tag) "
                      "VALUES(length(coalesce(quote(NEW.a),'')) + 40, 'u'); END;")
            self.emit("CREATE TRIGGER tr_d AFTER DELETE ON t BEGIN "
                      "DELETE FROM u WHERE n = length(coalesce(quote(OLD.b),'')); "
                      "END;")

        if self.on("generated") and self.gen_kind and self.r.random() < 0.5:
            self.emit("CREATE INDEX i_g ON t(g);")
        if self.on("fkeys") and self.shape in (
                "int_pk", "text_pk", "numeric_pk", "unique_only"):
            # FK cascade writes another table while the parent's scan is open.
            self.has_child = True
            action = self.r.choice(["CASCADE", "SET NULL", "RESTRICT"])
            ktype = "TEXT" if self.key_kind == "text" else "INTEGER"
            self.emit("CREATE TABLE ch(cid INTEGER PRIMARY KEY, fk %s "
                      "REFERENCES t(k) ON DELETE %s ON UPDATE %s, note TEXT);"
                      % (ktype, action, action))
            self.emit("PRAGMA foreign_keys=ON;")

    def child_write(self):
        """Child write. ORDER BY is load-bearing: LIMIT 1 without it is plan-dependent."""
        if self.r.random() < 0.7:
            self.emit("INSERT OR IGNORE INTO ch(cid, fk, note) "
                      "SELECT %d, k, %s FROM t WHERE %s ORDER BY %s LIMIT 1;"
                      % (self.r.randint(1, 60), self.text_val(), self.pred(),
                         Q % "k"))
        else:
            self.emit("DELETE FROM ch WHERE cid = %d;" % self.r.randint(1, 60))

    def returning(self):
        """RETURNING of one row: SQLite does not order RETURNING and cannot take ORDER BY."""
        cols = "coalesce(quote(k),'N'), coalesce(quote(a),'N'), " \
               "coalesce(quote(b),'N')"
        r = self.r.random()
        if r < 0.4 or self.shape == "no_pk":
            kc, kv = self.key_args()
            self.emit("INSERT OR REPLACE INTO t(%s, a, b) VALUES(%s, %s, %s) "
                      "RETURNING %s;"
                      % (kc, kv, self.val(), self.val("text"), cols))
            return
        eq = self.key_eq()
        if r < 0.7:
            self.emit("UPDATE t SET b = %s WHERE %s RETURNING %s;"
                      % (self.val("text"), eq, cols))
        else:
            self.emit("DELETE FROM t WHERE %s RETURNING %s;" % (eq, cols))

    def key_eq(self):
        if self.shape == "composite_pk":
            return "k = %s AND j = %s" % (self.int_val(), self.text_val())
        kv = self.text_val() if self.key_kind == "text" else self.int_val()
        return "k = %s" % kv

    def key_args(self):
        if self.shape == "composite_pk":
            return "k, j", "%s, %s" % (self.int_val(), self.text_val())
        if self.key_kind == "text":
            return "k", self.text_val()
        return "k", self.int_val()

    def pred(self):
        r = self.r.random()
        if self.shape == "composite_pk" and r < 0.2:
            return "k = %s AND j = %s" % (self.int_val(), self.text_val())
        if r < 0.24:
            kv = self.text_val() if self.key_kind == "text" else self.int_val()
            return "k = %s" % kv
        if r < 0.4:
            return "k > %s AND k < %s" % (self.int_val(), self.int_val())
        if r < 0.5:
            return "a IS NULL"
        if r < 0.6:
            return "b > %s" % self.text_val()
        if r < 0.66:
            return "a > %s AND a < %s" % (self.val(), self.val())
        if r < 0.72:
            return "k IN (%s, %s)" % (self.int_val(), self.int_val())
        if not self.on("expr"):
            return "a = %s" % self.val()
        if r < 0.78:
            return "b LIKE %s" % self.r.choice(["'a%'", "'%z'", "'_b'", "'%'"])
        if r < 0.83:
            return "b GLOB %s" % self.r.choice(["'a*'", "'[a-c]*'", "'*'"])
        if r < 0.88:
            return "k BETWEEN %s AND %s" % (self.int_val(), self.int_val())
        if r < 0.93:
            return "a IN (SELECT a FROM t WHERE b > %s)" % self.text_val()
        if r < 0.97:
            return "EXISTS (SELECT 1 FROM t AS t2 WHERE t2.b = t.b AND %s)" % (
                "t2.a IS NOT NULL")
        return "a = %s" % self.val()

    def insert(self):
        kc, kv = self.key_args()
        verb = self.r.choice(["INSERT OR IGNORE", "INSERT OR REPLACE",
                              "INSERT OR IGNORE", "INSERT OR ROLLBACK"])
        if verb == "INSERT OR ROLLBACK" and self.in_txn:
            verb = "INSERT OR IGNORE"
        if self.on("writesel") and self.r.random() < 0.25:
            # One statement, many pending writes.
            rows = ", ".join("(%s, %s, %s)" % (self.key_args()[1], self.val(),
                                               self.val("text"))
                             for _ in range(self.r.randint(2, 4)))
            self.emit("%s INTO t(%s, a, b) VALUES %s;" % (verb, kc, rows))
            return
        self.emit("%s INTO t(%s, a, b) VALUES(%s, %s, %s);"
                  % (verb, kc, kv, self.val(), self.val("text")))

    def upsert(self):
        if self.shape == "no_pk":
            self.insert()
            return
        kc, kv = self.key_args()
        target = "k, j" if self.shape == "composite_pk" else "k"
        if self.r.random() < 0.3:
            self.emit("INSERT INTO t(%s, a, b) VALUES(%s, %s, %s) "
                      "ON CONFLICT(%s) DO NOTHING;"
                      % (kc, kv, self.val(), self.val("text"), target))
            return
        self.emit("INSERT INTO t(%s, a, b) VALUES(%s, %s, %s) "
                  "ON CONFLICT(%s) DO UPDATE SET b = %s WHERE %s;"
                  % (kc, kv, self.val(), self.val("text"), target,
                     self.val("text"), self.r.choice(["1", "excluded.b > t.b"])))

    def insert_select(self):
        # Same-table INSERT SELECT scans a pending map the statement is filling.
        kc, _ = self.key_args()
        if self.shape == "composite_pk":
            sel = "k + %d, coalesce(quote(j),'x'), a, b" % self.r.randint(1, 50)
            order = ", ".join(Q % col for col in ("k", "j", "a", "b"))
        else:
            base = "coalesce(quote(k),'x') || 'x'" if self.key_kind == "text" \
                else "k + %d" % self.r.randint(1, 50)
            sel = "%s, a, b" % base
            order = ", ".join(Q % col for col in ("k", "a", "b"))
        self.emit("INSERT OR IGNORE INTO t(%s, a, b) "
                  "SELECT %s FROM t WHERE %s ORDER BY %s;"
                  % (kc, sel, self.pred(), order))

    def update(self):
        col = self.r.choice(["a", "b"])
        if self.on("writesel") and self.r.random() < 0.25:
            self.emit("UPDATE t SET %s = (SELECT %s FROM t AS t2 "
                      "ORDER BY %s LIMIT 1) WHERE %s;"
                      % (col, "max(coalesce(quote(t2.b),''))",
                         "1", self.pred()))
            return
        rhs = self.expr() if col == "a" else \
            ("coalesce(quote(%s),'x')" % self.r.choice(["a", "b"])
             if self.on("expr") and self.r.random() < 0.3 else self.val("text"))
        self.emit("UPDATE t SET %s = %s WHERE %s;" % (col, rhs, self.pred()))

    def delete(self):
        if self.on("writesel") and self.r.random() < 0.2:
            self.emit("DELETE FROM t WHERE b IN "
                      "(SELECT b FROM t WHERE %s);" % self.pred())
            return
        self.emit("DELETE FROM t WHERE %s;" % self.pred())

    def ddl_step(self):
        r = self.r.random()
        if r < 0.3 and not self.added_col:
            # Column added mid-transaction; later rows must carry it.
            self.added_col = True
            self.emit("ALTER TABLE t ADD COLUMN c%s;"
                      % (" DEFAULT 7" if self.r.random() < 0.5 else ""))
        elif r < 0.55:
            # Index over whatever is currently pending.
            self.emit("CREATE INDEX IF NOT EXISTS i_t ON t(b, a, k);")
        elif r < 0.7:
            self.emit("DROP INDEX IF EXISTS i_t;")
        elif r < 0.85:
            self.emit("REINDEX t;")
        else:
            self.emit("ANALYZE t;")

    def full_read(self, tbl="t"):
        cols = ["k", "a", "b"] + (["c"] if self.added_col else [])
        if self.shape == "composite_pk":
            cols.insert(1, "j")
        proj = " || '/' || ".join(Q % c for c in cols)
        self.emit("SELECT group_concat(q, '|') FROM (SELECT %s AS q FROM %s "
                  "ORDER BY 1);" % (proj, tbl))

    def read(self):
        r = self.r.random()
        if r < 0.16:
            self.emit("SELECT count(*), count(a), count(b) FROM t WHERE %s;"
                      % self.pred())
        elif r < 0.3:
            self.emit("SELECT group_concat(q, '|') FROM (SELECT %s || '/' || %s "
                      "AS q FROM t WHERE %s ORDER BY 1);"
                      % (Q % "k", Q % "a", self.pred()))
        elif r < 0.38:
            self.emit("SELECT min(k), max(k), count(DISTINCT b) FROM t;")
        elif r < 0.46:
            d = self.r.choice(["ASC", "DESC"])
            self.emit("SELECT group_concat(%s, '|') FROM (SELECT a FROM t "
                      "ORDER BY a %s, %s, %s);"
                      % (Q % "a", d, Q % "k", Q % "b"))
        elif r < 0.52:
            self.emit("SELECT count(*) FROM t WHERE a IN "
                      "(SELECT a FROM t WHERE %s);" % self.pred())
        elif self.on("agg") and r < 0.62:
            having = " HAVING count(*) > 1" if self.r.random() < 0.5 else ""
            self.emit("SELECT group_concat(g, '|') FROM (SELECT "
                      "min(%s) || ':' || count(*) AS g FROM t "
                      "GROUP BY b%s ORDER BY 1);" % (Q % "b", having))
        elif self.on("agg") and r < 0.68:
            self.emit("SELECT count(*), sum(length(coalesce(quote(a),''))), "
                      "count(DISTINCT b), max(length(coalesce(quote(b),''))) "
                      "FROM t WHERE %s;" % self.pred())
        elif self.on("agg") and r < 0.72:
            self.emit("SELECT group_concat(q,'|') FROM (SELECT DISTINCT %s AS q "
                      "FROM t ORDER BY 1);" % (Q % "b"))
        elif self.on("setops") and r < 0.78:
            op = self.r.choice(["UNION", "UNION ALL", "INTERSECT", "EXCEPT"])
            self.emit("SELECT group_concat(q,'|') FROM (SELECT %s AS q FROM t "
                      "WHERE %s %s SELECT %s AS q FROM t WHERE %s ORDER BY 1);"
                      % (Q % "b", self.pred(), op, Q % "b", self.pred()))
        elif self.on("cte") and r < 0.82:
            self.emit("WITH w AS (SELECT k, a, b FROM t WHERE %s) "
                      "SELECT count(*), count(DISTINCT b) FROM w;" % self.pred())
        elif self.on("cte") and r < 0.85:
            # Recursive CTE uses an ephemeral queue, not t.
            self.emit("WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL "
                      "SELECT i+1 FROM n WHERE i < 5) "
                      "SELECT count(*) FROM n JOIN t ON "
                      "length(coalesce(quote(t.b),'')) = n.i;")
        elif self.on("window") and r < 0.89:
            self.emit("SELECT group_concat(q,'|') FROM (SELECT %s || ':' || "
                      "row_number() OVER (ORDER BY %s, %s) AS q FROM t "
                      "ORDER BY 1);" % (Q % "b", Q % "b", Q % "k"))
        elif self.on("window") and r < 0.91:
            self.emit("SELECT group_concat(q,'|') FROM (SELECT %s || ':' || "
                      "count(*) OVER (PARTITION BY b) AS q FROM t ORDER BY 1);"
                      % (Q % "k"))
        elif self.on("joins") and r < 0.96:
            j = self.r.choice(["JOIN", "LEFT JOIN"])
            self.emit("SELECT group_concat(q,'|') FROM (SELECT %s || '/' || %s "
                      "AS q FROM t %s s ON length(coalesce(quote(t.b),'')) = s.id "
                      "ORDER BY 1);" % (Q % "t.k", Q % "s.w", j))
        elif self.on("joins") and r < 0.98:
            # Self-join: two cursors over one pending map.
            self.emit("SELECT count(*) FROM t AS x JOIN t AS y "
                      "ON x.b = y.b AND %s;"
                      % self.r.choice(["x.a IS NOT y.a", "x.a IS NOT NULL"]))
        else:
            self.emit("SELECT group_concat(q,'|') FROM (SELECT %s AS q FROM t "
                      "ORDER BY %s, %s LIMIT %d OFFSET %d);"
                      % (Q % "b", Q % "b", Q % "k",
                         self.r.randint(1, 5), self.r.randint(0, 3)))

    def tail_reads(self):
        for d in ("ASC", "DESC"):
            self.emit("SELECT group_concat(%s, '|') FROM (SELECT a FROM t "
                      "ORDER BY a %s, %s, %s);"
                      % (Q % "a", d, Q % "k", Q % "b"))
        if self.on("joins"):
            self.emit("SELECT group_concat(q,'|') FROM (SELECT %s || '/' || %s "
                      "AS q FROM s ORDER BY 1);" % (Q % "id", Q % "w"))
        if self.on("triggers"):
            self.emit("SELECT coalesce(group_concat(q,'|'),'none') FROM "
                      "(SELECT %s || '=' || %s AS q FROM u ORDER BY 1);"
                      % (Q % "n", Q % "tag"))
        if self.on("ddl"):
            self.emit("SELECT count(*) FROM sqlite_master WHERE type='index';")
        if self.on("generated") and self.gen_kind:
            self.emit("SELECT group_concat(q,'|') FROM (SELECT %s || '/' || %s "
                      "AS q FROM t ORDER BY 1);" % (Q % "g", Q % "k"))
            self.emit("SELECT count(*) FROM t WHERE g > 2;")
        if self.has_child:
            self.emit("SELECT coalesce(group_concat(q,'|'),'none') FROM "
                      "(SELECT %s || '/' || %s AS q FROM ch ORDER BY 1);"
                      % (Q % "cid", Q % "fk"))
            self.emit("PRAGMA foreign_key_check;")

    def open_txn(self):
        self.emit("BEGIN;")
        self.in_txn = True

    def close_txn(self):
        # Rollback is as much of the contract as commit.
        self.emit(self.r.choice(["COMMIT;", "COMMIT;", "ROLLBACK;"]))
        self.in_txn = False
        self.savepoints = []

    def savepoint(self):
        name = "sp%d" % (len(self.savepoints) + 1)
        self.savepoints.append(name)
        self.emit("SAVEPOINT %s;" % name)

    def release_savepoint(self):
        name = self.savepoints.pop()
        if self.r.random() < 0.5:
            self.emit("ROLLBACK TO %s;" % name)
        self.emit("RELEASE %s;" % name)

    def body(self):
        for _ in range(self.r.randint(10, 30)):
            r = self.r.random()
            if not self.in_txn and r < 0.28:
                self.open_txn()
            elif self.in_txn and self.savepoints and r < 0.10:
                self.release_savepoint()
            elif self.in_txn and r < 0.20:
                self.savepoint()
            elif self.in_txn and r < 0.29:
                self.close_txn()
            elif r < 0.42:
                self.insert()
            elif r < 0.48:
                self.upsert() if self.on("triggers") else self.insert()
            elif r < 0.52 and self.on("writesel"):
                self.insert_select()
            elif r < 0.62:
                self.update()
            elif r < 0.70:
                self.delete()
            elif r < 0.73 and self.on("ddl"):
                self.ddl_step()
            elif r < 0.76 and self.on("returning"):
                self.returning()
            elif r < 0.79 and self.has_child:
                self.child_write()
            else:
                self.read()
        while self.savepoints:
            self.release_savepoint()
        if self.in_txn:
            self.close_txn()

    def run(self):
        self.schema()
        for _ in range(self.r.randint(2, 10)):
            self.insert()
        self.body()
        self.tail_reads()
        self.full_read()
        if self.on("ddl"):
            self.emit("SELECT count(*) FROM t;")
        self.emit("PRAGMA integrity_check;")
        return "\n".join(self.out)


def main():
    if len(sys.argv) < 2:
        sys.stderr.write("usage: %s SEED [--include-<group>]... [--all]\n"
                         "groups: %s\n" % (sys.argv[0], " ".join(GROUPS)))
        return 2
    seed = int(sys.argv[1])
    flags = sys.argv[2:]
    if "--all" in flags:
        groups = list(GROUPS)
    else:
        groups = [g for g in GROUPS if ("--include-%s" % g) in flags]
    unknown = [f for f in flags
               if f != "--all" and f[len("--include-"):] not in GROUPS]
    if unknown:
        sys.stderr.write("unknown flag(s): %s\n" % " ".join(unknown))
        return 2
    print(Gen(seed, groups).run())
    return 0


if __name__ == "__main__":
    sys.exit(main())
