#!/usr/bin/env python3
"""
Doltlite Quickstart — Python

Demonstrates Dolt version control features using Python's standard sqlite3
module. Doltlite is a drop-in replacement for SQLite, so no code changes
are needed — just swap the library at runtime.

Run (from the build/ directory):
    LD_PRELOAD=./libdoltlite.so python3 ../examples/quickstart.py
"""
import sqlite3
import os
import sys

DB_PATH = sys.argv[1] if len(sys.argv) > 1 else "quickstart.db"


def main():
    # Start fresh (remove DB, WAL, and journal files)
    for f in [DB_PATH, DB_PATH + "-wal", DB_PATH + "-journal"]:
        if os.path.exists(f):
            os.remove(f)

    db = sqlite3.connect(DB_PATH)
    cur = db.cursor()

    # ---- 1. Create schema and first commit ----
    print("== Create schema and first commit ==\n")
    cur.executescript("""
        CREATE TABLE employees(
            id INTEGER PRIMARY KEY,
            first_name TEXT,
            last_name TEXT
        );
        INSERT INTO employees VALUES(0, 'Tim',   'Sehn');
        INSERT INTO employees VALUES(1, 'Brian', 'Hendriks');
        INSERT INTO employees VALUES(2, 'Aaron', 'Son');
    """)
    cur.execute("SELECT dolt_commit('-A', '-m', 'Initial schema and data')")
    print(f"  commit: {cur.fetchone()[0]}\n")

    print("Log after first commit:")
    for row in cur.execute("SELECT commit_hash, message FROM dolt_log"):
        print(f"  {row[1]} ({row[0][:12]}...)")
    print()

    # ---- 2. Create a branch and make changes ----
    print("== Branch and modify ==\n")
    cur.execute("SELECT dolt_branch('feature')")
    cur.execute("SELECT dolt_checkout('feature')")

    cur.executescript("""
        UPDATE employees SET first_name='Timothy' WHERE id=0;
        INSERT INTO employees VALUES(3, 'Daylon', 'Wilkins');
    """)
    cur.execute("SELECT dolt_commit('-A', '-m', 'Rename Tim, add Daylon')")
    print(f"  commit: {cur.fetchone()[0]}\n")

    # ---- 3. Point-in-time query ----
    print("== Point-in-time query (main vs feature) ==\n")

    print("Employees on feature (current branch):")
    for row in cur.execute("SELECT id, first_name, last_name FROM employees ORDER BY id"):
        print(f"  {row[0]}: {row[1]} {row[2]}")
    print()

    print("Employees on main (without switching):")
    for row in cur.execute("SELECT * FROM dolt_at_employees('main') ORDER BY id"):
        print(f"  {row[0]}: {row[1]} {row[2]}")
    print()

    # ---- 4. Merge ----
    print("== Merge feature into main ==\n")
    cur.execute("SELECT dolt_checkout('main')")
    cur.execute("SELECT dolt_merge('feature')")

    print("Employees on main after merge:")
    for row in cur.execute("SELECT id, first_name, last_name FROM employees ORDER BY id"):
        print(f"  {row[0]}: {row[1]} {row[2]}")
    print()

    # ---- 5. Audit log ----
    print("== Audit log: all changes to employees ==\n")
    for row in cur.execute(
        "SELECT diff_type, to_id, to_first_name, to_last_name FROM dolt_diff_employees"
    ):
        print(f"  {row[0]:10s} id={row[1]} {row[2]} {row[3]}")
    print()

    # ---- 6. Commit log ----
    print("== Full commit log ==\n")
    for row in cur.execute("SELECT commit_hash, message FROM dolt_log"):
        print(f"  {row[0][:12]}... {row[1]}")
    print()

    # ---- 7. Tag ----
    print("== Tag current state ==\n")
    cur.execute("SELECT dolt_tag('v1.0')")
    for row in cur.execute("SELECT name, hash FROM dolt_tags"):
        print(f"  {row[0]}: {row[1][:12]}...")
    print()

    db.close()
    # Clean up DB, WAL, and journal files
    for f in [DB_PATH, DB_PATH + "-wal", DB_PATH + "-journal"]:
        if os.path.exists(f):
            os.remove(f)
    print("Done.")


if __name__ == "__main__":
    main()
