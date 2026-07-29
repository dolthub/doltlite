#!/usr/bin/env python3
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time


def sql_quote(s):
    return "'" + s.replace("'", "''") + "'"


def db_for_branch(db_path, branch):
    return db_path if branch == "main" else db_path + "/" + branch


def run_sql(doltlite, db_path, sql, label, timeout=20, allowed_errors=()):
    p = subprocess.run(
        [doltlite, db_path],
        input=sql,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
    )
    out = p.stdout.strip()
    err = p.stderr.strip()
    combined = out + "\n" + err
    if p.returncode != 0:
        if any(needle in combined for needle in allowed_errors):
            return None
        raise RuntimeError(
            "%s failed rc=%d\nSQL:\n%s\nstdout:\n%s\nstderr:\n%s"
            % (label, p.returncode, sql, out, err)
        )
    if "ERROR:" in out or "ERROR:" in err:
        if any(needle in combined for needle in allowed_errors):
            return None
        raise RuntimeError(
            "%s returned error\nSQL:\n%s\nstdout:\n%s\nstderr:\n%s"
            % (label, sql, out, err)
        )
    return out


def query_rows(doltlite, db_path, branch):
    sql = (
        ".mode list\n"
        ".separator |\n"
        "SELECT id, v, n FROM kv ORDER BY id;\n"
    )
    out = run_sql(doltlite, db_for_branch(db_path, branch), sql, "query_rows")
    rows = {}
    if not out:
        return rows
    for line in out.splitlines():
        parts = line.split("|")
        if len(parts) != 3:
            raise RuntimeError("unexpected row output for %s: %r" % (branch, line))
        rows[int(parts[0])] = (parts[1], int(parts[2]))
    return rows


def query_scalar(doltlite, db_path, branch, sql, label):
    out = run_sql(doltlite, db_for_branch(db_path, branch), sql, label)
    lines = [x for x in out.splitlines() if x.strip()]
    return lines[-1].strip() if lines else ""


def assert_rows(doltlite, db_path, branch, model):
    actual = query_rows(doltlite, db_path, branch)
    expected = model[branch]["working"]
    if actual != expected:
        raise AssertionError(
            "row model mismatch on %s\nexpected=%r\nactual=%r"
            % (branch, expected, actual)
        )


def assert_hash_shape(doltlite, db_path, branch):
    h = query_scalar(
        doltlite,
        db_path,
        branch,
        "SELECT dolt_hashof_table('kv');",
        "hash_shape",
    )
    if len(h) != 40 or any(c not in "0123456789abcdef" for c in h):
        raise AssertionError("bad table hash for %s: %r" % (branch, h))


def assert_clean_commit_stable(doltlite, db_path, branch):
    h1 = query_scalar(doltlite, db_path, branch, "SELECT dolt_hashof_db();", "hash_before")
    h2 = query_scalar(doltlite, db_path, branch, "SELECT dolt_hashof_db();", "hash_after")
    if h1 != h2:
        raise AssertionError("db hash changed across reopen on %s: %s != %s" % (branch, h1, h2))


def check_invariants(doltlite, db_path, branches, model, rng):
    branch = rng.choice(branches)
    assert_rows(doltlite, db_path, branch, model)
    assert_hash_shape(doltlite, db_path, branch)
    assert_clean_commit_stable(doltlite, db_path, branch)

    if rng.randrange(4) == 0:
        out = query_scalar(doltlite, db_path, branch, "PRAGMA integrity_check;", "integrity")
        if out != "ok":
            raise AssertionError("integrity_check on %s returned %r" % (branch, out))


def mutate_branch(doltlite, db_path, branch, model, rng, step):
    branch_num = 0 if branch == "main" else int(branch[1:])
    key_base = branch_num * 10000
    key = key_base + rng.randrange(1, 80)
    action = rng.choice(("insert", "update", "delete"))

    if action == "delete":
        sql = "DELETE FROM kv WHERE id=%d;" % key
        model[branch]["working"].pop(key, None)
    else:
        val = "%s_%05d_%04d" % (branch, step, rng.randrange(10000))
        n = rng.randrange(1000000)
        sql = (
            "INSERT INTO kv(id, v, n) VALUES(%d, %s, %d) "
            "ON CONFLICT(id) DO UPDATE SET v=excluded.v, n=excluded.n;"
            % (key, sql_quote(val), n)
        )
        model[branch]["working"][key] = (val, n)

    run_sql(doltlite, db_for_branch(db_path, branch), sql, "mutate_%s" % branch)
    model[branch]["dirty"] = model[branch]["working"] != model[branch]["committed"]


def commit_branch(doltlite, db_path, branch, model, step):
    if not model[branch]["dirty"]:
        return
    msg = "stateful %s %d" % (branch, step)
    run_sql(
        doltlite,
        db_for_branch(db_path, branch),
        "SELECT dolt_commit('-A','-m',%s);" % sql_quote(msg),
        "commit_%s" % branch,
    )
    model[branch]["committed"] = dict(model[branch]["working"])
    model[branch]["dirty"] = False


def reset_branch(doltlite, db_path, branch, model):
    run_sql(
        doltlite,
        db_for_branch(db_path, branch),
        "SELECT dolt_reset('--hard');",
        "reset_%s" % branch,
    )
    model[branch]["working"] = dict(model[branch]["committed"])
    model[branch]["dirty"] = False


def create_branch(doltlite, db_path, branches, model, rng):
    name = "b%d" % (len(branches))
    source = rng.choice(branches)
    commit_branch(doltlite, db_path, source, model, len(branches))
    run_sql(
        doltlite,
        db_for_branch(db_path, source),
        "SELECT dolt_branch(%s);" % sql_quote(name),
        "branch_%s_from_%s" % (name, source),
    )
    actual = query_rows(doltlite, db_path, name)
    model[name] = {
        "working": dict(actual),
        "committed": dict(actual),
        "dirty": False,
    }
    branches.append(name)


def merge_branch(doltlite, db_path, branches, model, rng):
    if len(branches) < 2:
        return
    target = rng.choice(branches)
    source_choices = [b for b in branches if b != target]
    source = rng.choice(source_choices)
    commit_branch(doltlite, db_path, target, model, 0)
    commit_branch(doltlite, db_path, source, model, 0)
    run_sql(
        doltlite,
        db_for_branch(db_path, target),
        "SELECT dolt_merge(%s);" % sql_quote(source),
        "merge_%s_into_%s" % (source, target),
        timeout=30,
        # An autocommit merge that conflicts is rolled back whole, so the model
        # below still matches: the target keeps the state it had before.
        allowed_errors=("cannot merge: conflicts detected",),
    )
    merged = query_rows(doltlite, db_path, target)
    model[target]["working"] = dict(merged)
    model[target]["committed"] = dict(merged)
    model[target]["dirty"] = False


def setup_repo(doltlite, db_path):
    run_sql(
        doltlite,
        db_path,
        (
            "CREATE TABLE kv(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);\n"
            "INSERT INTO kv VALUES(0, 'base', 0);\n"
            "SELECT dolt_commit('-A','-m','init');\n"
        ),
        "setup",
    )


def main():
    doltlite = sys.argv[1] if len(sys.argv) > 1 else "./doltlite"
    seconds = int(os.environ.get("DOLTLITE_VC_STATEFUL_SECONDS", "600"))
    seed = int(os.environ.get("DOLTLITE_VC_STATEFUL_SEED", str(int(time.time()))))
    rng = random.Random(seed)
    tmp = tempfile.mkdtemp(prefix="doltlite-stateful-")
    db_path = os.path.join(tmp, "stateful.db")
    deadline = time.time() + seconds
    step = 0
    branches = ["main"]
    model = {
        "main": {
            "working": {0: ("base", 0)},
            "committed": {0: ("base", 0)},
            "dirty": False,
        }
    }

    print("stateful vc fuzzer: seed=%d seconds=%d db=%s" % (seed, seconds, db_path))
    try:
        setup_repo(doltlite, db_path)
        while time.time() < deadline:
            step += 1
            op = rng.choice(
                ("mutate", "mutate", "mutate", "commit", "reset", "branch", "merge", "gc")
            )
            branch = rng.choice(branches)
            if op == "mutate":
                mutate_branch(doltlite, db_path, branch, model, rng, step)
            elif op == "commit":
                commit_branch(doltlite, db_path, branch, model, step)
            elif op == "reset":
                reset_branch(doltlite, db_path, branch, model)
            elif op == "branch" and len(branches) < 8:
                create_branch(doltlite, db_path, branches, model, rng)
            elif op == "merge":
                merge_branch(doltlite, db_path, branches, model, rng)
            elif op == "gc":
                run_sql(doltlite, db_path, "SELECT dolt_gc();", "gc", timeout=30)

            check_invariants(doltlite, db_path, branches, model, rng)
            if step % 25 == 0:
                print("  steps=%d branches=%d" % (step, len(branches)))

        for b in list(branches):
            commit_branch(doltlite, db_path, b, model, step)
            assert_rows(doltlite, db_path, b, model)
            assert_hash_shape(doltlite, db_path, b)
        print("OK: stateful vc fuzzer completed %d steps across %d branches" % (step, len(branches)))
        return 0
    except Exception as e:
        print("FAIL: stateful vc fuzzer seed=%d step=%d" % (seed, step), file=sys.stderr)
        print(str(e), file=sys.stderr)
        return 1
    finally:
        if os.environ.get("DOLTLITE_VC_STATEFUL_KEEP_DB") != "1":
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
