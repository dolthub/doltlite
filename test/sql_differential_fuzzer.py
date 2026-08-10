#!/usr/bin/env python3
"""Emit a randomized SQL script for differential testing against stock SQLite.

The script is printed on stdout and is valid for both engines: no dolt_*
surfaces, and nothing that depends on rowid identity or physical row order,
both of which diverge by design. Every result is ordered by declared columns
so a comparison only sees storage and query semantics.

The axis this exists for is transaction shape. Explicit BEGIN/SAVEPOINT blocks
with reads interleaved between writes are where the merged-cursor state machine
lives, and the bugs found there (a one-pass scan skipping pending rows, a
deferred seek serving a stale payload, a resumed scan re-seeking to a stale key)
were all found by randomized differential runs rather than by any suite.

Usage: sql_differential_fuzzer.py SEED [--include-large-ints] [--include-desc]
"""

import random
import sys

# Integers beyond 2^53 that no double represents exactly get a longer numeric
# sort key, and INT64 extremes are in that class too. Off by default: see the
# --include-desc note below and the runner's --help.
LARGE_INTS = [
    9007199254740992, 9007199254740993, 9007199254740994, 9007199254740995,
    -9007199254740992, -9007199254740993, -9007199254740994,
    18014398509481984, 18014398509481985,
    9223372036854775807, 9223372036854775806,
    -9223372036854775808, -9223372036854775807,
]

TEXTS = ["''", "'a'", "'A'", "'ab'", "'AB '", "'b'", "'z'", "'zz'",
         "'a' || char(0) || 'b'", "'  pad  '", "x'00'", "x'0001'", "x'ff'"]


class Gen:
    def __init__(self, seed, large_ints, desc):
        self.r = random.Random(seed)
        self.large_ints = large_ints
        self.desc = desc
        self.out = []
        self.depth = 0          # open SAVEPOINTs
        self.in_txn = False
        self.savepoints = []

    def emit(self, s):
        self.out.append(s)

    # ---- values ---------------------------------------------------------
    def int_val(self):
        if self.large_ints and self.r.random() < 0.35:
            return str(self.r.choice(LARGE_INTS))
        return str(self.r.randint(-40, 40))

    def text_val(self):
        return self.r.choice(TEXTS)

    def val(self, kind):
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

    # ---- schema ---------------------------------------------------------
    def schema(self):
        coll = self.r.choice(["", "", " COLLATE NOCASE", " COLLATE RTRIM"])
        shape = self.r.choice([
            "int_pk", "int_pk", "text_pk", "numeric_pk",
            "composite_pk", "no_pk", "unique_only",
        ])
        self.key_kind = "int"
        wr = ""
        if shape == "int_pk":
            cols = "k INTEGER PRIMARY KEY, a, b TEXT%s" % coll
        elif shape == "text_pk":
            cols = "k TEXT PRIMARY KEY%s, a, b TEXT" % coll
            self.key_kind = "text"
            wr = " WITHOUT ROWID" if self.r.random() < 0.5 else ""
        elif shape == "numeric_pk":
            cols = "k NUMERIC PRIMARY KEY, a, b TEXT%s" % coll
            wr = " WITHOUT ROWID" if self.r.random() < 0.5 else ""
        elif shape == "composite_pk":
            cols = "k INTEGER, j TEXT%s, a, b TEXT, PRIMARY KEY(k, j)" % coll
            wr = " WITHOUT ROWID" if self.r.random() < 0.5 else ""
        elif shape == "unique_only":
            cols = "k INTEGER UNIQUE, a, b TEXT%s" % coll
        else:
            cols = "k INTEGER, a, b TEXT%s" % coll
        self.shape = shape
        self.emit("CREATE TABLE t(%s)%s;" % (cols, wr))
        if self.r.random() < 0.7:
            d = " DESC" if (self.desc and self.r.random() < 0.5) else ""
            self.emit("CREATE INDEX i_a ON t(a%s);" % d)
        if self.r.random() < 0.4:
            self.emit("CREATE INDEX i_b ON t(b, a);")
        if self.r.random() < 0.3 and self.shape != "unique_only":
            self.emit("CREATE UNIQUE INDEX i_u ON t(b, k);")

    def key_args(self):
        """Return (column list, value list) for the key columns."""
        if self.shape == "composite_pk":
            return "k, j", "%s, %s" % (self.int_val(), self.text_val())
        if self.key_kind == "text":
            return "k", self.text_val()
        return "k", self.int_val()

    # ---- statements -----------------------------------------------------
    def insert(self):
        kc, kv = self.key_args()
        verb = self.r.choice(["INSERT OR IGNORE", "INSERT OR REPLACE",
                              "INSERT OR IGNORE", "INSERT OR ROLLBACK"])
        if verb == "INSERT OR ROLLBACK" and self.in_txn:
            verb = "INSERT OR IGNORE"
        self.emit("%s INTO t(%s, a, b) VALUES(%s, %s, %s);"
                  % (verb, kc, kv, self.val("any"), self.val("text")))

    def update(self):
        pred = self.pred()
        col = self.r.choice(["a", "b"])
        self.emit("UPDATE t SET %s = %s WHERE %s;"
                  % (col, self.val("any" if col == "a" else "text"), pred))

    def delete(self):
        self.emit("DELETE FROM t WHERE %s;" % self.pred())

    def pred(self):
        r = self.r.random()
        if self.shape == "composite_pk" and r < 0.25:
            return "k = %s AND j = %s" % (self.int_val(), self.text_val())
        if r < 0.3:
            kv = self.text_val() if self.key_kind == "text" else self.int_val()
            return "k = %s" % kv
        if r < 0.5:
            lo = self.int_val()
            hi = self.int_val()
            return "k > %s AND k < %s" % (lo, hi)
        if r < 0.65:
            return "a IS NULL"
        if r < 0.8:
            return "b > %s" % self.text_val()
        if r < 0.9:
            return "a = %s" % self.val("any")
        return "k IN (%s, %s)" % (self.int_val(), self.int_val())

    def read(self):
        r = self.r.random()
        if r < 0.3:
            self.emit("SELECT count(*), count(a), count(b) FROM t WHERE %s;"
                      % self.pred())
        elif r < 0.5:
            self.emit("SELECT group_concat(q, '|') FROM (SELECT "
                      "coalesce(quote(k), 'NK') || '/' || coalesce(quote(a), 'NA')"
                      " AS q FROM t WHERE %s ORDER BY 1);" % self.pred())
        elif r < 0.65:
            self.emit("SELECT min(k), max(k) FROM t;")
        elif r < 0.8:
            self.emit("SELECT group_concat(q, '|') FROM (SELECT quote(b) AS q "
                      "FROM t ORDER BY b, k);")
        else:
            self.emit("SELECT count(*) FROM t WHERE a IN "
                      "(SELECT a FROM t WHERE %s);" % self.pred())

    def full_read(self):
        self.emit("SELECT group_concat(q, '|') FROM (SELECT "
                  "coalesce(quote(k), 'NK') || '/' || coalesce(quote(a), 'NA')"
                  " || '/' || coalesce(quote(b), 'NB') AS q FROM t "
                  "ORDER BY 1);")

    # ---- transaction shaping -------------------------------------------
    def open_txn(self):
        self.emit("BEGIN;")
        self.in_txn = True

    def close_txn(self):
        # A rollback must restore the pre-transaction state; that is as much
        # of the contract as a commit is.
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
        else:
            self.emit("RELEASE %s;" % name)

    def body(self):
        for _ in range(self.r.randint(6, 22)):
            r = self.r.random()
            if not self.in_txn and r < 0.3:
                self.open_txn()
            elif self.in_txn and self.savepoints and r < 0.12:
                self.release_savepoint()
            elif self.in_txn and r < 0.22:
                self.savepoint()
            elif self.in_txn and r < 0.32:
                self.close_txn()
            elif r < 0.55:
                self.insert()
            elif r < 0.68:
                self.update()
            elif r < 0.78:
                self.delete()
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
        self.full_read()
        self.emit("PRAGMA integrity_check;")
        return "\n".join(self.out)


def main():
    if len(sys.argv) < 2:
        sys.stderr.write("usage: %s SEED [--include-large-ints] [--include-desc]\n"
                         % sys.argv[0])
        return 2
    seed = int(sys.argv[1])
    flags = sys.argv[2:]
    print(Gen(seed,
              "--include-large-ints" in flags,
              "--include-desc" in flags).run())
    return 0


if __name__ == "__main__":
    sys.exit(main())
