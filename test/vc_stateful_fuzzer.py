#!/usr/bin/env python3
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time

# Autocommit merge refusals roll back whole; dolt_pull's merge half is the same. Prefix matches those errors.
MERGE_ROLLED_BACK = ("cannot merge:",)


def sql_quote(s):
    return "'" + s.replace("'", "''") + "'"


def db_for_branch(db_path, branch):
    return db_path if branch == "main" else db_path + "/" + branch


def run_sql(doltlite, db_path, sql, label, timeout=20, allowed_errors=()):
    if os.environ.get("DOLTLITE_VC_STATEFUL_TRACE") == "1":
        print("TRACE %s db=%s sql=%r" % (label, db_path, sql), file=sys.stderr, flush=True)
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


def query_revision_rows(doltlite, db_path, revision):
    sql = (
        ".mode list\n"
        ".separator |\n"
        "SELECT id, v, n FROM dolt_at_kv(%s) ORDER BY id;\n"
        % sql_quote(revision)
    )
    out = run_sql(doltlite, db_path, sql, "query_revision_rows")
    rows = {}
    if not out:
        return rows
    for line in out.splitlines():
        parts = line.split("|")
        if len(parts) != 3:
            raise RuntimeError("unexpected revision row output: %r" % line)
        rows[int(parts[0])] = (parts[1], int(parts[2]))
    return rows


def query_committed_rows(doltlite, db_path, branch):
    sql = (
        ".mode list\n"
        ".separator |\n"
        "SELECT id, v, n FROM dolt_at_kv('HEAD') ORDER BY id;\n"
    )
    out = run_sql(
        doltlite,
        db_for_branch(db_path, branch),
        sql,
        "query_committed_rows",
    )
    rows = {}
    if not out:
        return rows
    for line in out.splitlines():
        parts = line.split("|")
        if len(parts) != 3:
            raise RuntimeError("unexpected committed row output: %r" % line)
        rows[int(parts[0])] = (parts[1], int(parts[2]))
    return rows


def query_scalar(doltlite, db_path, branch, sql, label):
    out = run_sql(doltlite, db_for_branch(db_path, branch), sql, label)
    lines = [x for x in out.splitlines() if x.strip()]
    return lines[-1].strip() if lines else ""


def query_list(doltlite, db_path, branch, sql, label):
    out = run_sql(doltlite, db_for_branch(db_path, branch), sql, label)
    return [line.strip() for line in out.splitlines() if line.strip()]


def query_schema(doltlite, db_path, branch):
    return query_list(
        doltlite,
        db_path,
        branch,
        (
            ".mode list\n"
            ".separator |\n"
            "SELECT type, name, coalesce(sql, '') FROM sqlite_schema "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name;\n"
        ),
        "query_schema",
    )


def status_counts(doltlite, db_path, branch):
    out = query_scalar(
        doltlite,
        db_path,
        branch,
        "SELECT coalesce(sum(staged),0) || '|' || count(*) FROM dolt_status;",
        "status_counts",
    )
    staged, total = out.split("|")
    return int(staged), int(total)


def new_branch_state(rows):
    return {
        "working": dict(rows),
        "staged": dict(rows),
        "committed": dict(rows),
    }


def sync_vc_result(doltlite, db_path, branch, model):
    working = query_rows(doltlite, db_path, branch)
    committed = query_committed_rows(doltlite, db_path, branch)
    model[branch] = {
        "working": dict(working),
        "staged": dict(working),
        "committed": dict(committed),
    }


def assert_rows(doltlite, db_path, branch, model):
    actual = query_rows(doltlite, db_path, branch)
    expected = model[branch]["working"]
    if actual != expected:
        raise AssertionError(
            "row model mismatch on %s\nexpected=%r\nactual=%r"
            % (branch, expected, actual)
        )
    committed = query_committed_rows(doltlite, db_path, branch)
    expected_committed = model[branch]["committed"]
    if committed != expected_committed:
        raise AssertionError(
            "HEAD model mismatch on %s\nexpected=%r\nactual=%r"
            % (branch, expected_committed, committed)
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


def assert_refs(doltlite, db_path, branches, tags):
    actual_branches = query_list(
        doltlite,
        db_path,
        "main",
        "SELECT name FROM dolt_branches ORDER BY name;",
        "branch_refs",
    )
    if actual_branches != sorted(branches):
        raise AssertionError(
            "branch refs mismatch\nexpected=%r\nactual=%r"
            % (sorted(branches), actual_branches)
        )
    actual_tags = query_list(
        doltlite,
        db_path,
        "main",
        "SELECT tag_name FROM dolt_tags ORDER BY tag_name;",
        "tag_refs",
    )
    if actual_tags != sorted(tags):
        raise AssertionError(
            "tag refs mismatch\nexpected=%r\nactual=%r"
            % (sorted(tags), actual_tags)
        )


def check_invariants(doltlite, db_path, branches, tags, model, rng):
    for branch in branches:
        assert_rows(doltlite, db_path, branch, model)
    branch = rng.choice(branches)
    assert_hash_shape(doltlite, db_path, branch)
    assert_clean_commit_stable(doltlite, db_path, branch)
    schema = query_schema(doltlite, db_path, branch)
    if schema != query_schema(doltlite, db_path, branch):
        raise AssertionError("schema changed across reopen on %s" % branch)

    if rng.randrange(4) == 0:
        out = query_scalar(doltlite, db_path, branch, "PRAGMA integrity_check;", "integrity")
        if out != "ok":
            raise AssertionError("integrity_check on %s returned %r" % (branch, out))
    if rng.randrange(4) == 0:
        assert_refs(doltlite, db_path, branches, tags)
    assert_related_consistent(doltlite, db_path, branch)
    if rng.randrange(4) == 0:
        assert_shadow_tables_consistent(doltlite, db_path, branch)
    if rng.randrange(8) == 0:
        assert_reindex_preserves_answers(doltlite, db_path, branch)


def branch_base(branch):
    return (0 if branch == "main" else int(branch[1:])) * 10000


def mutate_related(doltlite, db_path, branch, rng, step):
    """Write the foreign key, index and shadow-table shapes on one branch."""
    base = branch_base(branch)
    grp = base + rng.randrange(4)
    key = base + 100 + rng.randrange(60)
    action = rng.choice(
        ("insert_null", "insert_parent", "flip_to_parent", "flip_to_null",
         "delete_child", "duplicate", "doc_insert", "doc_delete")
    )
    parent = rng.randrange(1, SEED_PARENTS + 1)
    text = "row %s %05d doc" % (branch, step)

    if action == "insert_null":
        sql = (
            "INSERT INTO child(id, parent_id, grp, name, body) "
            "VALUES(%d, NULL, %d, 'n%d', %s) "
            "ON CONFLICT(id) DO UPDATE SET parent_id=NULL, grp=excluded.grp, "
            "name=excluded.name, body=excluded.body;"
            % (key, grp, key, sql_quote(text))
        )
    elif action == "insert_parent":
        sql = (
            "INSERT INTO child(id, parent_id, grp, name, body) "
            "VALUES(%d, %d, %d, 'n%d', %s) "
            "ON CONFLICT(id) DO UPDATE SET parent_id=excluded.parent_id, "
            "grp=excluded.grp, name=excluded.name, body=excluded.body;"
            % (key, parent, grp, key, sql_quote(text))
        )
    elif action == "flip_to_parent":
        sql = "UPDATE child SET parent_id=%d WHERE id=%d;" % (parent, key)
    elif action == "flip_to_null":
        sql = "UPDATE child SET parent_id=NULL WHERE id=%d;" % key
    elif action == "delete_child":
        sql = "DELETE FROM child WHERE id=%d;" % key
    elif action == "doc_insert":
        # fts5 has no UPSERT; replace the row instead.
        rowid = base + 200 + rng.randrange(40)
        sql = (
            "DELETE FROM docs WHERE rowid=%d;\n"
            "INSERT INTO docs(rowid, body) VALUES(%d, %s);"
            % (rowid, rowid, sql_quote(text))
        )
    elif action == "doc_delete":
        sql = "DELETE FROM docs WHERE rowid=%d;" % (base + 200 + rng.randrange(40))
    else:
        # A second row with the same partial-index key must still be refused,
        # whatever merges and rebases did to the index in between.
        # One row per (branch, grp) owns the duplicate probe name, so seeding
        # it is idempotent however many times this action runs.
        seed_id = base + 900 + (grp - base)
        run_sql(
            doltlite,
            db_for_branch(db_path, branch),
            "INSERT INTO child(id, parent_id, grp, name, body) "
            "VALUES(%d, NULL, %d, 'dup%d', 'seed') "
            "ON CONFLICT(id) DO UPDATE SET parent_id=NULL, grp=excluded.grp, "
            "name=excluded.name;" % (seed_id, grp, grp),
            "dup_seed_%s" % branch,
        )
        out = run_sql(
            doltlite,
            db_for_branch(db_path, branch),
            "INSERT INTO child(id, parent_id, grp, name, body) "
            "VALUES(%d, NULL, %d, 'dup%d', 'clash');" % (seed_id + 50, grp, grp),
            "dup_clash_%s" % branch,
            allowed_errors=("UNIQUE constraint failed",),
        )
        if out is not None:
            raise AssertionError(
                "duplicate partial-index key accepted on %s (grp=%d)" % (branch, grp)
            )
        return

    run_sql(
        doltlite,
        db_for_branch(db_path, branch),
        sql,
        "mutate_related_%s" % branch,
        allowed_errors=("UNIQUE constraint failed",),
    )


def assert_related_consistent(doltlite, db_path, branch):
    """Indexes must answer what a scan answers, and no constraint may fire.

    A row the partial predicate excludes has no business in the index, and a
    stale entry only shows up when something walks the whole index, which is
    what the foreign-key and constraint checks do.
    """
    pairs = (
        ("SELECT coalesce(group_concat(id),'') FROM "
         "(SELECT id FROM child WHERE parent_id IS NOT NULL ORDER BY id);",
         "SELECT coalesce(group_concat(id),'') FROM "
         "(SELECT id FROM child NOT INDEXED WHERE parent_id IS NOT NULL ORDER BY id);"),
        ("SELECT coalesce(group_concat(id),'') FROM "
         "(SELECT id FROM child WHERE parent_id IS NULL ORDER BY id);",
         "SELECT coalesce(group_concat(id),'') FROM "
         "(SELECT id FROM child NOT INDEXED WHERE parent_id IS NULL ORDER BY id);"),
        ("SELECT count(*) FROM child WHERE grp >= 0;",
         "SELECT count(*) FROM child NOT INDEXED WHERE grp >= 0;"),
    )
    for indexed_sql, scan_sql in pairs:
        via_index = query_scalar(doltlite, db_path, branch, indexed_sql, "idx_read")
        via_scan = query_scalar(doltlite, db_path, branch, scan_sql, "scan_read")
        if via_index != via_scan:
            raise AssertionError(
                "index and scan disagree on %s\nsql=%s\nindex=%r scan=%r"
                % (branch, indexed_sql, via_index, via_scan)
            )

    fk = query_scalar(
        doltlite,
        db_path,
        branch,
        "SELECT count(*) FROM pragma_foreign_key_check;",
        "foreign_key_check",
    )
    if fk != "0":
        raise AssertionError("foreign_key_check reported %s rows on %s" % (fk, branch))

    violations = query_scalar(
        doltlite,
        db_path,
        branch,
        "SELECT count(*) FROM dolt_constraint_violations;",
        "constraint_violations",
    )
    if violations != "0":
        raise AssertionError(
            "%s constraint violations recorded on %s" % (violations, branch)
        )


def assert_shadow_tables_consistent(doltlite, db_path, branch):
    """fts5 keeps its content, index and docsize shadow tables in step; its own
    integrity command is the check for all three at once."""
    run_sql(
        doltlite,
        db_for_branch(db_path, branch),
        "INSERT INTO docs(docs) VALUES('integrity-check');",
        "fts_integrity_%s" % branch,
    )
    matched = query_scalar(
        doltlite,
        db_path,
        branch,
        "SELECT count(*) FROM docs WHERE docs MATCH 'doc';",
        "fts_match",
    )
    stored = query_scalar(
        doltlite,
        db_path,
        branch,
        "SELECT count(*) FROM docs WHERE body LIKE '%doc%';",
        "fts_scan",
    )
    if matched != stored:
        raise AssertionError(
            "fts5 index and content disagree on %s: match=%s content=%s"
            % (branch, matched, stored)
        )


def assert_reindex_preserves_answers(doltlite, db_path, branch):
    """Rebuilding every index must not change a single answer."""
    probe = (
        "SELECT coalesce(group_concat(id),'') FROM "
        "(SELECT id FROM child WHERE parent_id IS NOT NULL ORDER BY id);"
    )
    before = query_scalar(doltlite, db_path, branch, probe, "reindex_before")
    run_sql(doltlite, db_for_branch(db_path, branch), "REINDEX;", "reindex_%s" % branch)
    after = query_scalar(doltlite, db_path, branch, probe, "reindex_after")
    if before != after:
        raise AssertionError(
            "REINDEX changed answers on %s\nbefore=%r after=%r" % (branch, before, after)
        )


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


def transaction_branch(doltlite, db_path, branch, model, step, outcome):
    branch_num = 0 if branch == "main" else int(branch[1:])
    key_base = branch_num * 10000 + step * 2 + 1000
    pre_value = "pre_tx_%s_%05d" % (branch, step)
    tx_value = "in_tx_%s_%05d" % (branch, step)
    pre_sql = (
        "INSERT INTO kv(id, v, n) VALUES(%d, %s, %d) "
        "ON CONFLICT(id) DO UPDATE SET v=excluded.v, n=excluded.n;"
        % (key_base, sql_quote(pre_value), step)
    )
    run_sql(doltlite, db_for_branch(db_path, branch), pre_sql, "pre_transaction_%s" % branch)
    model[branch]["working"][key_base] = (pre_value, step)

    before = {name: dict(rows) for name, rows in model[branch].items()}
    before_schema = query_schema(doltlite, db_path, branch)
    before_status = status_counts(doltlite, db_path, branch)
    table = "tx_aux_%d" % step
    statements = [
        "BEGIN;",
        (
            "INSERT INTO kv(id, v, n) VALUES(%d, %s, %d) "
            "ON CONFLICT(id) DO UPDATE SET v=excluded.v, n=excluded.n;"
            % (key_base + 1, sql_quote(tx_value), step + 1)
        ),
        "CREATE TABLE %s(id INTEGER PRIMARY KEY, payload TEXT);" % table,
        "INSERT INTO %s VALUES(1, %s);" % (table, sql_quote(tx_value)),
        "CREATE INDEX %s_idx ON %s(payload);" % (table, table),
        "SELECT dolt_add('-A');",
    ]
    expected = dict(before["working"])
    expected[key_base + 1] = (tx_value, step + 1)
    if outcome == "rollback":
        statements.append("ROLLBACK;")
    elif outcome == "commit":
        statements.append("COMMIT;")
    else:
        statements.append(
            "SELECT dolt_commit('-m',%s);"
            % sql_quote("stateful transaction %s %d" % (branch, step))
        )
    run_sql(
        doltlite,
        db_for_branch(db_path, branch),
        "\n".join(statements),
        "transaction_%s_%s" % (outcome, branch),
        timeout=30,
    )

    schema = query_schema(doltlite, db_path, branch)
    status = status_counts(doltlite, db_path, branch)
    if outcome == "rollback":
        model[branch] = before
        if schema != before_schema:
            raise AssertionError("transaction rollback kept schema changes on %s" % branch)
        if status != before_status:
            raise AssertionError("transaction rollback kept staged changes on %s" % branch)
    else:
        model[branch]["working"] = dict(expected)
        model[branch]["staged"] = dict(expected)
        if outcome == "vc_commit":
            model[branch]["committed"] = dict(expected)
            if status != (0, 0):
                raise AssertionError("VC commit left status entries on %s" % branch)
        elif status[0] != status[1]:
            raise AssertionError("transaction commit lost staged changes on %s" % branch)
        if schema == before_schema:
            raise AssertionError("transaction commit lost schema changes on %s" % branch)
    assert_rows(doltlite, db_path, branch, model)


def add_branch(doltlite, db_path, branch, model, all_tables):
    arg = "-A" if all_tables else "kv"
    run_sql(
        doltlite,
        db_for_branch(db_path, branch),
        "SELECT dolt_add(%s);" % sql_quote(arg),
        "add_%s" % branch,
    )
    model[branch]["staged"] = dict(model[branch]["working"])


def commit_branch(doltlite, db_path, branch, model, step, stage_all=True):
    staged, total = status_counts(doltlite, db_path, branch)
    if (stage_all and total == 0) or (not stage_all and staged == 0):
        return
    msg = "stateful %s %d" % (branch, step)
    args = "'-A','-m',%s" % sql_quote(msg) if stage_all else "'-m',%s" % sql_quote(msg)
    # A failed statement rolls back -A's restage; complete it before treating as a no-op.
    out = run_sql(
        doltlite,
        db_for_branch(db_path, branch),
        "SELECT dolt_commit(%s);" % args,
        "commit_%s" % branch,
        allowed_errors=("nothing to commit",) if stage_all else (),
    )
    if out is None:
        run_sql(
            doltlite,
            db_for_branch(db_path, branch),
            "SELECT dolt_add('-A');",
            "restage_%s" % branch,
        )
        sync_vc_result(doltlite, db_path, branch, model)
        return
    expected = model[branch]["working"] if stage_all else model[branch]["staged"]
    committed = query_committed_rows(doltlite, db_path, branch)
    if committed != expected:
        raise AssertionError(
            "commit model mismatch on %s\nexpected=%r\nactual HEAD=%r"
            % (branch, expected, committed)
        )
    if stage_all:
        model[branch]["staged"] = dict(model[branch]["working"])
    model[branch]["committed"] = dict(committed)
    model[branch]["staged"] = dict(committed)


def reset_branch(doltlite, db_path, branch, model, hard):
    flag = "--hard" if hard else "--soft"
    committed = query_committed_rows(doltlite, db_path, branch)
    if committed != model[branch]["committed"]:
        raise AssertionError(
            "pre-reset HEAD model mismatch on %s\nexpected=%r\nactual HEAD=%r"
            % (branch, model[branch]["committed"], committed)
        )
    run_sql(
        doltlite,
        db_for_branch(db_path, branch),
        "SELECT dolt_reset(%s);" % sql_quote(flag),
        "reset_%s" % branch,
    )
    if hard:
        working = query_rows(doltlite, db_path, branch)
        if working != committed:
            raise AssertionError(
                "hard reset did not restore HEAD on %s\nHEAD=%r\nworking=%r"
                % (branch, committed, working)
            )
        model[branch]["working"] = dict(committed)
        model[branch]["staged"] = dict(committed)
    # --soft without a ref moves nothing, including staged. Mixed reset (no flag) is not exercised.


def create_branch(doltlite, db_path, branches, model, rng, name, via_checkout):
    source = rng.choice(branches)
    commit_branch(doltlite, db_path, source, model, len(branches))
    function = "dolt_checkout('-b',%s)" % sql_quote(name) if via_checkout else "dolt_branch(%s)" % sql_quote(name)
    run_sql(
        doltlite,
        db_for_branch(db_path, source),
        "SELECT %s;" % function,
        "branch_%s_from_%s" % (name, source),
    )
    actual = query_rows(doltlite, db_path, name)
    model[name] = new_branch_state(actual)
    branches.append(name)


def copy_branch(doltlite, db_path, branches, model, rng, name):
    source = rng.choice(branches)
    commit_branch(doltlite, db_path, source, model, len(branches))
    run_sql(
        doltlite,
        db_path,
        "SELECT dolt_branch('-c',%s,%s);" % (sql_quote(source), sql_quote(name)),
        "copy_%s_from_%s" % (name, source),
    )
    model[name] = new_branch_state(query_rows(doltlite, db_path, name))
    branches.append(name)


def rename_branch(doltlite, db_path, branches, model, rng, name):
    candidates = [branch for branch in branches if branch != "main"]
    if not candidates:
        return False
    old = rng.choice(candidates)
    commit_branch(doltlite, db_path, old, model, len(branches))
    run_sql(
        doltlite,
        db_path,
        "SELECT dolt_branch('-m',%s,%s);" % (sql_quote(old), sql_quote(name)),
        "rename_%s_to_%s" % (old, name),
    )
    branches[branches.index(old)] = name
    model[name] = model.pop(old)
    return True


def delete_branch(doltlite, db_path, branches, model, rng):
    candidates = [branch for branch in branches if branch != "main"]
    if len(candidates) < 2:
        return
    branch = rng.choice(candidates)
    run_sql(
        doltlite,
        db_path,
        "SELECT dolt_branch('-D',%s);" % sql_quote(branch),
        "delete_%s" % branch,
    )
    branches.remove(branch)
    del model[branch]


def checkout_branch(doltlite, db_path, branches, model, rng, step):
    if len(branches) < 2:
        return
    source = rng.choice(branches)
    target = rng.choice([branch for branch in branches if branch != source])
    commit_branch(doltlite, db_path, source, model, step)
    out = run_sql(
        doltlite,
        db_for_branch(db_path, source),
        "SELECT dolt_checkout(%s); SELECT active_branch();" % sql_quote(target),
        "checkout_%s_from_%s" % (target, source),
    )
    if out.splitlines()[-1].strip() != target:
        raise AssertionError("checkout did not activate %s: %r" % (target, out))


def connect_branch(doltlite, db_path, branches, model, rng, step):
    target = rng.choice(branches)
    source = rng.choice(branches)
    commit_branch(doltlite, db_path, source, model, step)
    out = run_sql(
        doltlite,
        db_for_branch(db_path, source),
        "SELECT dolt_connect_branch(%s); SELECT active_branch();" % sql_quote(target),
        "connect_%s_from_%s" % (target, source),
    )
    if out.splitlines()[-1].strip() != target:
        raise AssertionError("connect did not activate %s: %r" % (target, out))


def mutate_schema(doltlite, db_path, branch, rng, step):
    schema = query_schema(doltlite, db_path, branch)
    tables = [line.split("|", 2)[1] for line in schema if line.startswith("table|aux_")]
    indexes = [line.split("|", 2)[1] for line in schema if line.startswith("index|aux_idx_")]
    actions = ["create_table"]
    if tables:
        actions.extend((
            "add_column", "rename_table", "drop_table",
            "create_index", "create_partial_index",
        ))
    if indexes:
        actions.append("drop_index")
    action = rng.choice(actions)
    name = "aux_%d" % step
    if action == "create_table":
        sql = "CREATE TABLE %s(id INTEGER PRIMARY KEY, payload TEXT, n INTEGER);" % name
    elif action == "add_column":
        sql = "ALTER TABLE %s ADD COLUMN c%d TEXT;" % (rng.choice(tables), step)
    elif action == "rename_table":
        sql = "ALTER TABLE %s RENAME TO %s;" % (rng.choice(tables), name)
    elif action == "drop_table":
        sql = "DROP TABLE %s;" % rng.choice(tables)
    elif action == "create_index":
        sql = "CREATE INDEX aux_idx_%d ON %s(payload);" % (step, rng.choice(tables))
    elif action == "create_partial_index":
        sql = (
            "CREATE INDEX aux_idx_%d ON %s(payload) WHERE payload IS NOT NULL;"
            % (step, rng.choice(tables))
        )
    else:
        sql = "DROP INDEX %s;" % rng.choice(indexes)
    run_sql(doltlite, db_for_branch(db_path, branch), sql, "ddl_%s_%s" % (action, branch))
    changed = query_schema(doltlite, db_path, branch)
    if changed == schema or changed != query_schema(doltlite, db_path, branch):
        raise AssertionError("DDL %s did not persist on %s" % (action, branch))


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
        allowed_errors=MERGE_ROLLED_BACK,
    )
    sync_vc_result(doltlite, db_path, target, model)


def cherry_pick_branch(doltlite, db_path, branches, model, rng, step):
    if len(branches) < 2:
        return
    target = rng.choice(branches)
    source = rng.choice([branch for branch in branches if branch != target])
    commit_branch(doltlite, db_path, target, model, step)
    commit_branch(doltlite, db_path, source, model, step)
    run_sql(
        doltlite,
        db_for_branch(db_path, target),
        "SELECT dolt_cherry_pick(%s);" % sql_quote(source),
        "cherry_pick_%s_onto_%s" % (source, target),
        timeout=30,
        allowed_errors=(
            "conflict",
            "nothing to commit",
            "already exists",
            "cherry-pick of",
            "cherry-picking a merge commit",
        ),
    )
    sync_vc_result(doltlite, db_path, target, model)


def revert_branch(doltlite, db_path, branch, model, step):
    commit_branch(doltlite, db_path, branch, model, step)
    count = int(
        query_scalar(
            doltlite,
            db_path,
            branch,
            "SELECT count(*) FROM dolt_log;",
            "revert_log_count",
        )
    )
    if count < 3:
        return
    run_sql(
        doltlite,
        db_for_branch(db_path, branch),
        "SELECT dolt_revert('HEAD');",
        "revert_%s" % branch,
        timeout=30,
        allowed_errors=("conflict", "nothing to commit"),
    )
    sync_vc_result(doltlite, db_path, branch, model)


def rebase_branch(doltlite, db_path, branches, model, rng, step):
    if len(branches) < 2:
        return
    branch = rng.choice(branches)
    upstream = rng.choice([name for name in branches if name != branch])
    commit_branch(doltlite, db_path, branch, model, step)
    commit_branch(doltlite, db_path, upstream, model, step)
    run_sql(
        doltlite,
        db_for_branch(db_path, branch),
        "SELECT dolt_rebase(%s);" % sql_quote(upstream),
        "rebase_%s_onto_%s" % (branch, upstream),
        timeout=30,
        allowed_errors=("conflict", "rebase aborted", "didn't identify any commits"),
    )
    sync_vc_result(doltlite, db_path, branch, model)


def create_tag(doltlite, db_path, branch, tags, name):
    run_sql(
        doltlite,
        db_for_branch(db_path, branch),
        "SELECT dolt_tag(%s);" % sql_quote(name),
        "tag_%s" % name,
    )
    tags.append(name)


def delete_tag(doltlite, db_path, tags, rng):
    if not tags:
        return
    name = rng.choice(tags)
    run_sql(
        doltlite,
        db_path,
        "SELECT dolt_tag('-d',%s);" % sql_quote(name),
        "delete_tag_%s" % name,
    )
    tags.remove(name)


def detached_revision(doltlite, db_path, branches, tags, model, rng, step):
    branch = rng.choice(branches)
    commit_branch(doltlite, db_path, branch, model, step)
    head = query_scalar(
        doltlite,
        db_path,
        "main",
        "SELECT dolt_hashof(%s);" % sql_quote(branch),
        "detached_head_hash",
    )
    revisions = [head, branch + "~0"]
    if tags:
        revisions.append(rng.choice(tags))
    revision = rng.choice(revisions)
    expected_hash = query_scalar(
        doltlite,
        db_path,
        "main",
        "SELECT dolt_hashof(%s);" % sql_quote(revision),
        "detached_revision_hash",
    )
    expected_rows = query_revision_rows(doltlite, db_path, revision)
    separator = rng.choice(("/", "@"))
    detached_path = db_path + separator + revision
    out = run_sql(
        doltlite,
        detached_path,
        (
            ".mode list\n"
            ".separator |\n"
            "SELECT 'STATE', IFNULL(active_branch(),'NULL'), dolt_hashof('HEAD');\n"
            "SELECT 'ROW', id, v, n FROM kv ORDER BY id;\n"
        ),
        "detached_read_%s" % revision,
    )
    lines = out.splitlines()
    if not lines or lines[0] != "STATE|NULL|" + expected_hash:
        raise AssertionError("bad detached state for %s: %r" % (revision, out))
    actual_rows = {}
    for line in lines[1:]:
        parts = line.split("|")
        if len(parts) != 4 or parts[0] != "ROW":
            raise AssertionError("bad detached row for %s: %r" % (revision, line))
        actual_rows[int(parts[1])] = (parts[2], int(parts[3]))
    if actual_rows != expected_rows:
        raise AssertionError(
            "detached row mismatch for %s\nexpected=%r\nactual=%r"
            % (revision, expected_rows, actual_rows)
        )

    write = run_sql(
        doltlite,
        detached_path,
        "UPDATE kv SET n=n WHERE 0;",
        "detached_write_%s" % revision,
        allowed_errors=("read-only", "readonly"),
    )
    if write is not None:
        raise AssertionError("detached write unexpectedly succeeded for %s" % revision)

    target = rng.choice(branches)
    out = run_sql(
        doltlite,
        detached_path,
        (
            "SELECT 'BEFORE|' || IFNULL(active_branch(),'NULL');\n"
            "SELECT dolt_checkout(%s);\n"
            "SELECT 'AFTER|' || active_branch();\n"
            "UPDATE kv SET n=n WHERE 0;\n"
        )
        % sql_quote(target),
        "detached_reattach_%s_to_%s" % (revision, target),
    )
    if "BEFORE|NULL" not in out.splitlines() or "AFTER|" + target not in out.splitlines():
        raise AssertionError("detached reattach failed for %s: %r" % (revision, out))
    assert_rows(doltlite, db_path, target, model)


def remote_operation(doltlite, db_path, branches, model, pushed, rng, step, op):
    if op == "pull":
        candidates = [branch for branch in branches if branch in pushed]
        if not candidates:
            return
        branch = rng.choice(candidates)
    else:
        branch = rng.choice(branches)
    commit_branch(doltlite, db_path, branch, model, step)
    if op == "push":
        run_sql(
            doltlite,
            db_for_branch(db_path, branch),
            "SELECT dolt_push('origin',%s);" % sql_quote(branch),
            "push_%s" % branch,
            timeout=30,
            allowed_errors=("non-fast-forward", "not a fast-forward"),
        )
        pushed.add(branch)
    elif pushed:
        remote_branch = branch if op == "pull" else rng.choice(sorted(pushed))
        run_sql(
            doltlite,
            db_for_branch(db_path, branch),
            "SELECT dolt_%s('origin',%s);" % (op, sql_quote(remote_branch)),
            "%s_%s" % (op, remote_branch),
            timeout=30,
            allowed_errors=(
                "conflict",
                "diverged",
                "non-fast-forward",
            )
            + MERGE_ROLLED_BACK,
        )
        if op == "pull":
            sync_vc_result(doltlite, db_path, branch, model)


def reset_remote_config(doltlite, db_path, remote_path):
    run_sql(
        doltlite,
        db_path,
        (
            "SELECT dolt_remote('remove','origin'); "
            "SELECT dolt_remote('add','origin',%s);" % sql_quote("file://" + remote_path)
        ),
        "reset_remote_config",
    )


# kv is modelled row by row. The related tables below are not: they carry the
# shapes whose index maintenance runs outside the VDBE (a foreign key, a plain
# index, two partial indexes and an fts5 virtual table's shadow tables) and are
# checked by self-consistency instead. Every branch writes a disjoint id and
# grp range and only ever points parent_id at a parent seeded in the first
# commit, so no version-control operation can produce a legitimate constraint
# violation: any violation the engine reports is a bug.
SEED_PARENTS = 8

RELATED_SCHEMA = (
    "CREATE TABLE parent(id INTEGER PRIMARY KEY, label TEXT);\n"
    "CREATE TABLE child(\n"
    "  id INTEGER PRIMARY KEY,\n"
    "  parent_id INTEGER REFERENCES parent(id),\n"
    "  grp INTEGER NOT NULL,\n"
    "  name TEXT NOT NULL,\n"
    "  body TEXT\n"
    ");\n"
    "CREATE INDEX child_grp ON child(grp);\n"
    "CREATE UNIQUE INDEX child_with_parent ON child(grp, parent_id, name)\n"
    "  WHERE parent_id IS NOT NULL;\n"
    "CREATE UNIQUE INDEX child_without_parent ON child(grp, name)\n"
    "  WHERE parent_id IS NULL;\n"
    "CREATE VIRTUAL TABLE docs USING fts5(body);\n"
)


def setup_repo(doltlite, db_path, remote_path):
    run_sql(
        doltlite,
        db_path,
        (
            "CREATE TABLE kv(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);\n"
            "INSERT INTO kv VALUES(0, 'base', 0);\n"
            + RELATED_SCHEMA
            + "INSERT INTO parent SELECT value, 'p' || value "
              "FROM generate_series(1, %d);\n" % SEED_PARENTS
            + "SELECT dolt_commit('-A','-m','init');\n"
        ),
        "setup",
    )
    shutil.copyfile(db_path, remote_path)
    remote_url = "file://" + remote_path
    run_sql(
        doltlite,
        db_path,
        "SELECT dolt_remote('add','origin',%s);" % sql_quote(remote_url),
        "setup_remote",
    )


OPERATIONS = (
    ["mutate"] * 4
    + ["mutate_related"] * 3
    + ["ddl"] * 3
    + [
        "add",
        "commit_staged",
        "commit_all",
        "transaction_rollback",
        "transaction_commit",
        "transaction_vc_commit",
        "reset_soft",
        "reset_hard",
        "branch_create",
        "checkout_new",
        "branch_copy",
        "branch_rename",
        "branch_delete",
        "checkout",
        "connect_branch",
        "tag_create",
        "tag_delete",
        "detached_revision",
        "merge",
        "cherry_pick",
        "revert",
        "rebase",
        "push",
        "fetch",
        "pull",
        "remote_config",
        "verify_constraints",
        "reindex",
        "gc",
    ]
)


def main():
    doltlite = sys.argv[1] if len(sys.argv) > 1 else "./doltlite"
    seconds = int(os.environ.get("DOLTLITE_VC_STATEFUL_SECONDS", "600"))
    seed = int(os.environ.get("DOLTLITE_VC_STATEFUL_SEED", str(int(time.time()))))
    rng = random.Random(seed)
    tmp = tempfile.mkdtemp(prefix="doltlite-stateful-")
    db_path = os.path.join(tmp, "stateful.db")
    remote_path = os.path.join(tmp, "remote.db")
    deadline = time.time() + seconds
    step = 0
    branches = ["main"]
    tags = []
    pushed = {"main"}
    model = {"main": new_branch_state({0: ("base", 0)})}
    next_branch = 1
    next_tag = 1
    operation_cycle = []
    op_counts = {}
    current_op = "setup"

    print(
        "stateful vc fuzzer: seed=%d seconds=%d db=%s" % (seed, seconds, db_path),
        flush=True,
    )
    try:
        setup_repo(doltlite, db_path, remote_path)
        while time.time() < deadline:
            step += 1
            if not operation_cycle:
                operation_cycle = list(OPERATIONS)
                rng.shuffle(operation_cycle)
            op = operation_cycle.pop()
            current_op = op
            op_counts[op] = op_counts.get(op, 0) + 1
            branch = rng.choice(branches)
            if op == "mutate":
                mutate_branch(doltlite, db_path, branch, model, rng, step)
            elif op == "mutate_related":
                mutate_related(doltlite, db_path, branch, rng, step)
            elif op == "reindex":
                assert_reindex_preserves_answers(doltlite, db_path, branch)
            elif op == "ddl":
                mutate_schema(doltlite, db_path, branch, rng, step)
            elif op == "add":
                add_branch(doltlite, db_path, branch, model, rng.randrange(2) == 0)
            elif op == "commit_staged":
                commit_branch(doltlite, db_path, branch, model, step, False)
            elif op == "commit_all":
                commit_branch(doltlite, db_path, branch, model, step)
            elif op.startswith("transaction_"):
                transaction_branch(
                    doltlite,
                    db_path,
                    branch,
                    model,
                    step,
                    op[len("transaction_"):],
                )
            elif op == "reset_soft":
                reset_branch(doltlite, db_path, branch, model, False)
            elif op == "reset_hard":
                reset_branch(doltlite, db_path, branch, model, True)
            elif op in ("branch_create", "checkout_new") and len(branches) < 10:
                name = "b%d" % next_branch
                next_branch += 1
                create_branch(
                    doltlite,
                    db_path,
                    branches,
                    model,
                    rng,
                    name,
                    op == "checkout_new",
                )
            elif op == "branch_copy" and len(branches) < 10:
                name = "b%d" % next_branch
                next_branch += 1
                copy_branch(doltlite, db_path, branches, model, rng, name)
            elif op == "branch_rename":
                name = "b%d" % next_branch
                if rename_branch(doltlite, db_path, branches, model, rng, name):
                    next_branch += 1
            elif op == "branch_delete":
                delete_branch(doltlite, db_path, branches, model, rng)
            elif op == "checkout":
                checkout_branch(doltlite, db_path, branches, model, rng, step)
            elif op == "connect_branch":
                connect_branch(doltlite, db_path, branches, model, rng, step)
            elif op == "tag_create":
                create_tag(doltlite, db_path, branch, tags, "tag%d" % next_tag)
                next_tag += 1
            elif op == "tag_delete":
                delete_tag(doltlite, db_path, tags, rng)
            elif op == "detached_revision":
                detached_revision(doltlite, db_path, branches, tags, model, rng, step)
            elif op == "merge":
                merge_branch(doltlite, db_path, branches, model, rng)
            elif op == "cherry_pick":
                cherry_pick_branch(doltlite, db_path, branches, model, rng, step)
            elif op == "revert":
                revert_branch(doltlite, db_path, branch, model, step)
            elif op == "rebase":
                rebase_branch(doltlite, db_path, branches, model, rng, step)
            elif op in ("push", "fetch", "pull"):
                remote_operation(doltlite, db_path, branches, model, pushed, rng, step, op)
            elif op == "remote_config":
                reset_remote_config(doltlite, db_path, remote_path)
            elif op == "verify_constraints":
                run_sql(
                    doltlite,
                    db_for_branch(db_path, branch),
                    "SELECT dolt_verify_constraints('--all','--output-only');",
                    "verify_constraints_%s" % branch,
                    timeout=30,
                )
            elif op == "gc":
                run_sql(doltlite, db_path, "SELECT dolt_gc();", "gc", timeout=30)

            check_invariants(doltlite, db_path, branches, tags, model, rng)
            if step % 25 == 0:
                print(
                    "  steps=%d branches=%d operations=%d"
                    % (step, len(branches), len(op_counts)),
                    flush=True,
                )

        for b in list(branches):
            commit_branch(doltlite, db_path, b, model, step)
            assert_rows(doltlite, db_path, b, model)
            assert_hash_shape(doltlite, db_path, b)
        assert_refs(doltlite, db_path, branches, tags)
        print(
            "OK: stateful vc fuzzer completed %d steps across %d branches; operations=%s"
            % (step, len(branches), ",".join(sorted(op_counts))),
            flush=True,
        )
        return 0
    except Exception as e:
        fail = "FAIL: stateful vc fuzzer seed=%d step=%d op=%s\n%s" % (
            seed,
            step,
            current_op,
            e,
        )
        print(fail, flush=True)
        print(fail, file=sys.stderr, flush=True)
        return 1
    finally:
        if os.environ.get("DOLTLITE_VC_STATEFUL_KEEP_DB") != "1":
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
