#!/usr/bin/env python3
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time

# Autocommit merge refusals roll back whole; dolt_pull's merge half is the same.
# UNIQUE/CHECK/FK on the extra shape tables also land here: autocommit merge
# with conflicts or constraint violations is not a fuzzer failure.
MERGE_ROLLED_BACK = (
    "cannot merge:",
    "conflict",
    "constraint violation",
    "transaction rolled back",
)


def sql_quote(s):
    return "'" + s.replace("'", "''") + "'"


def resolve_engine(path):
    """Windows CI wraps the engine in a bash script Python cannot CreateProcess.

    platform-test.yml sets DOLTLITE to doltlite-no-msys-conv and the PE path in
    DOLTLITE_SYSTEM. Prefer that, then a sibling doltlite.exe, then `path`.
    """
    system = os.environ.get("DOLTLITE_SYSTEM")
    if system:
        return system
    if not path:
        return path
    if path.lower().endswith(".exe"):
        return path
    sibling = os.path.join(os.path.dirname(os.path.abspath(path)) or ".", "doltlite.exe")
    if os.path.isfile(sibling):
        return sibling
    return path


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
    for table in ("kv",) + SHAPE_TABLES:
        h = query_scalar(
            doltlite,
            db_path,
            branch,
            "SELECT dolt_hashof_table(%s);" % sql_quote(table),
            "hash_shape_%s" % table,
        )
        if len(h) != 40 or any(c not in "0123456789abcdef" for c in h):
            raise AssertionError("bad table hash for %s %s: %r" % (branch, table, h))


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


def assert_generated_indexes(doltlite, db_path, branch):
    """VIRTUAL/STORED generated indexes must contain every table row."""
    for table, index in (("t_gen", "t_gen_g"), ("t_int", "t_int_g")):
        via_scan = query_scalar(
            doltlite, db_path, branch,
            "SELECT count(*) FROM %s NOT INDEXED;" % table,
            "gen_scan_%s" % table,
        )
        via_idx = query_scalar(
            doltlite, db_path, branch,
            "SELECT count(*) FROM %s INDEXED BY %s;" % (table, index),
            "gen_idx_%s" % table,
        )
        if via_scan != via_idx:
            raise AssertionError(
                "generated index %s on %s disagrees with the table: "
                "index=%s scan=%s" % (index, branch, via_idx, via_scan)
            )


def check_invariants(doltlite, db_path, branches, tags, model, rng):
    for branch in branches:
        assert_rows(doltlite, db_path, branch, model)
        assert_generated_indexes(doltlite, db_path, branch)
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
    assert_generated_columns(doltlite, db_path, branch)
    assert_views_query(doltlite, db_path, branch)
    if rng.randrange(6) == 0:
        assert_triggers_fire(doltlite, db_path, branch)
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


def secondary_index_hashes(doltlite, db_path, branch):
    """Hash of every CREATE INDEX / UNIQUE INDEX, labeled by name.

    Clustered PK tables (TEXT / composite / WITHOUT ROWID) are the table
    itself. ADD COLUMN without a default leaves omitted-NULL encoding in those
    trees; REINDEX rewrites the NULLs and dolt_hashof_table / dolt_hashof_db
    move even though SELECT * does not. Secondary indexes have no such
    rewrite, so a hash that moves is a stale entry.
    """
    sql = (
        ".mode list\n"
        ".separator |\n"
        "SELECT name, dolt_hashof_index(name) FROM sqlite_schema\n"
        " WHERE type='index' AND sql IS NOT NULL ORDER BY name;\n"
    )
    out = run_sql(
        doltlite, db_for_branch(db_path, branch), sql, "secondary_index_hashes"
    )
    hashes = {}
    for line in (out or "").splitlines():
        parts = line.split("|", 1)
        if len(parts) != 2:
            continue
        hashes[parts[0]] = parts[1]
    return hashes


def logical_table_dumps(doltlite, db_path, branch):
    """SELECT * of every fuzzed table, so a clustered PK rewrite cannot hide."""
    dumps = {}
    for table in ("kv", "child") + SHAPE_TABLES:
        sql = (
            ".headers off\n"
            ".mode list\n"
            ".separator |\n"
            "SELECT * FROM %s ORDER BY 1, 2, 3;\n" % table
        )
        dumps[table] = run_sql(
            doltlite, db_for_branch(db_path, branch), sql, "dump_%s" % table
        ) or ""
    return dumps


def format_hash_diff(before, after):
    keys = sorted(set(before) | set(after))
    lines = []
    for key in keys:
        b = before.get(key)
        a = after.get(key)
        if b == a:
            continue
        lines.append("%s before=%s after=%s" % (key, b, a))
    return "\n".join(lines) if lines else "(no labeled object changed)"


def assert_reindex_preserves_answers(doltlite, db_path, branch):
    """Rebuilding an index that is already right must change nothing.

    REINDEX writes the index the engine believes the rows imply, so a hash that
    moves means what was stored did not match the rows: an entry missing, or an
    entry a partial predicate excludes. The answers a query returns are the
    weaker half of this check, because a stale entry can hide behind a range
    scan that never visits it.
    """
    probe = (
        "SELECT coalesce(group_concat(id),'') FROM "
        "(SELECT id FROM child WHERE parent_id IS NOT NULL ORDER BY id);"
    )
    before_rows = query_scalar(doltlite, db_path, branch, probe, "reindex_before")
    before_hash = secondary_index_hashes(doltlite, db_path, branch)
    before_dump = logical_table_dumps(doltlite, db_path, branch)
    run_sql(doltlite, db_for_branch(db_path, branch), "REINDEX;", "reindex_%s" % branch)
    after_rows = query_scalar(doltlite, db_path, branch, probe, "reindex_after")
    after_hash = secondary_index_hashes(doltlite, db_path, branch)
    after_dump = logical_table_dumps(doltlite, db_path, branch)
    if before_rows != after_rows:
        raise AssertionError(
            "REINDEX changed answers on %s\nbefore=%r after=%r"
            % (branch, before_rows, after_rows)
        )
    if before_hash != after_hash:
        raise AssertionError(
            "REINDEX changed an index hash on %s, so the stored index did not "
            "match its rows\n%s"
            % (branch, format_hash_diff(before_hash, after_hash))
        )
    if before_dump != after_dump:
        raise AssertionError(
            "REINDEX changed table rows on %s\n%s"
            % (branch, format_hash_diff(before_dump, after_dump))
        )


def schema_objects(doltlite, db_path, branch):
    """Sorted (type, name) pairs of the views and triggers in the working schema."""
    rows = query_list(
        doltlite,
        db_path,
        branch,
        ".mode list\n.separator |\nSELECT type || '|' || name FROM sqlite_schema "
        "WHERE type IN ('view','trigger') ORDER BY type, name;\n",
        "schema_objects",
    )
    return sorted(tuple(r.split("|", 1)) for r in rows)


def mutate_schema_objects(doltlite, db_path, branch, rng, step):
    """Create, replace or drop a view or trigger on one branch."""
    objects = schema_objects(doltlite, db_path, branch)
    prefix = "_%s_" % branch
    views = [n for t, n in objects if t == "view" and prefix in n]
    triggers = [n for t, n in objects if t == "trigger" and prefix in n]
    actions = ["create_view", "create_trigger"]
    if views:
        actions.extend(("drop_view", "replace_view"))
    if triggers:
        actions.extend(("drop_trigger", "replace_trigger"))
    action = rng.choice(actions)
    vname = "v%s%d" % (prefix, step)
    tname = "tr%s%d" % (prefix, step)
    selects = (
        "SELECT id, v FROM kv WHERE id > %d" % rng.randrange(1000),
        "SELECT id FROM child WHERE parent_id IS NULL",
        "SELECT c.id, p.id AS pid FROM child c JOIN parent p ON p.id = c.parent_id",
        "SELECT count(*) AS n FROM kv",
    )
    events = ("AFTER INSERT ON child", "AFTER UPDATE ON child",
              "AFTER DELETE ON child", "AFTER INSERT ON parent")

    if action == "create_view":
        sql = "CREATE VIEW %s AS %s;" % (vname, rng.choice(selects))
        expect = ("view", vname, True)
    elif action == "replace_view":
        vname = rng.choice(views)
        sql = "DROP VIEW %s; CREATE VIEW %s AS %s;" % (vname, vname, rng.choice(selects))
        expect = ("view", vname, True)
    elif action == "drop_view":
        vname = rng.choice(views)
        sql = "DROP VIEW %s;" % vname
        expect = ("view", vname, False)
    elif action == "create_trigger":
        event = rng.choice(events)
        ref = "old" if "DELETE" in event else "new"
        sql = (
            "CREATE TRIGGER %s %s BEGIN INSERT INTO audit(id, note) "
            "SELECT %s.id, %s WHERE NOT EXISTS (SELECT 1 FROM audit WHERE id = %s.id); END;"
            % (tname, event, ref, sql_quote(tname), ref)
        )
        expect = ("trigger", tname, True)
    elif action == "replace_trigger":
        tname = rng.choice(triggers)
        event = rng.choice(events)
        ref = "old" if "DELETE" in event else "new"
        sql = (
            "DROP TRIGGER %s; CREATE TRIGGER %s %s BEGIN INSERT INTO audit(id, note) "
            "SELECT %s.id, %s WHERE NOT EXISTS (SELECT 1 FROM audit WHERE id = %s.id); END;"
            % (tname, tname, event, ref, sql_quote(tname + "b"), ref)
        )
        expect = ("trigger", tname, True)
    else:
        tname = rng.choice(triggers)
        sql = "DROP TRIGGER %s;" % tname
        expect = ("trigger", tname, False)

    run_sql(doltlite, db_for_branch(db_path, branch), sql, "schema_object_%s_%s" % (action, branch))
    after = schema_objects(doltlite, db_path, branch)
    present = (expect[0], expect[1]) in after
    if present != expect[2]:
        raise AssertionError(
            "%s on %s did not take: %s %s present=%s\nschema=%r"
            % (action, branch, expect[0], expect[1], present, after)
        )
    if after == objects and action.startswith("create"):
        raise AssertionError("%s on %s left the schema unchanged" % (action, branch))
    if action.startswith("create"):
        rows = query_list(
            doltlite,
            db_path,
            branch,
            "SELECT table_name || '|' || staged FROM dolt_status WHERE table_name='dolt_schemas';",
            "schema_object_status",
        )
        if not rows:
            raise AssertionError(
                "%s on %s left dolt_status without a dolt_schemas row" % (action, branch)
            )


def assert_views_query(doltlite, db_path, branch):
    """Every view must still answer: its tables and columns are never dropped."""
    for kind, name in schema_objects(doltlite, db_path, branch):
        if kind != "view":
            continue
        query_scalar(
            doltlite,
            db_path,
            branch,
            "SELECT count(*) FROM %s;" % name,
            "view_query_%s" % name,
        )


def assert_triggers_fire(doltlite, db_path, branch):
    """A probe insert into child must reach audit exactly when an AFTER INSERT
    trigger on child is defined, and the probe is removed afterwards."""
    base = branch_base(branch)
    probe = base + 9500
    triggers = query_list(
        doltlite,
        db_path,
        branch,
        ".mode list\n.separator |\nSELECT name FROM sqlite_schema WHERE type='trigger' "
        "AND tbl_name='child' AND sql LIKE '%AFTER INSERT%';\n",
        "insert_triggers",
    )
    run_sql(
        doltlite,
        db_for_branch(db_path, branch),
        "DELETE FROM audit WHERE id=%d; DELETE FROM child WHERE id=%d;" % (probe, probe),
        "probe_clear_%s" % branch,
    )
    run_sql(
        doltlite,
        db_for_branch(db_path, branch),
        "INSERT INTO child(id, parent_id, grp, name, body) VALUES(%d, NULL, %d, 'probe', 'probe doc');"
        % (probe, base + 9),
        "probe_insert_%s" % branch,
    )
    fired = query_scalar(
        doltlite, db_path, branch,
        "SELECT count(*) FROM audit WHERE id=%d;" % probe, "probe_audit",
    )
    run_sql(
        doltlite,
        db_for_branch(db_path, branch),
        "DELETE FROM child WHERE id=%d; DELETE FROM audit WHERE id=%d;" % (probe, probe),
        "probe_cleanup_%s" % branch,
    )
    expected = "1" if triggers else "0"
    if fired != expected:
        raise AssertionError(
            "trigger probe on %s: audit rows=%s with insert triggers=%r"
            % (branch, fired, triggers)
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


def aux_create_sql(name, rng):
    templates = (
        "CREATE TABLE {n}(id INTEGER PRIMARY KEY, payload TEXT, n INTEGER);",
        "CREATE TABLE {n}(k TEXT PRIMARY KEY, payload TEXT, n INTEGER);",
        "CREATE TABLE {n}(a INTEGER NOT NULL, b INTEGER NOT NULL, payload TEXT, PRIMARY KEY(a, b));",
        "CREATE TABLE {n}(k TEXT PRIMARY KEY, payload TEXT, n INTEGER) WITHOUT ROWID;",
        "CREATE TABLE {n}(id INTEGER NOT NULL, payload TEXT, n INTEGER, PRIMARY KEY(id DESC)) WITHOUT ROWID;",
        "CREATE TABLE {n}(id INTEGER PRIMARY KEY, n INTEGER, g INTEGER GENERATED ALWAYS AS (n + 1) STORED);",
        "CREATE TABLE {n}(id INTEGER PRIMARY KEY, n INTEGER CHECK (n >= 0), payload TEXT);",
        "CREATE TABLE {n}(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id), payload TEXT);",
    )
    return rng.choice(templates).format(n=name)


def extra_columns(doltlite, db_path, branch, table):
    cols = query_list(
        doltlite,
        db_path,
        branch,
        "SELECT name FROM pragma_table_info(%s) ORDER BY cid;" % sql_quote(table),
        "table_info_%s" % table,
    )
    return [c for c in cols if c.startswith("xcol_")]


def data_columns(doltlite, db_path, branch, table):
    cols = query_list(
        doltlite,
        db_path,
        branch,
        "SELECT name FROM pragma_table_info(%s) WHERE pk=0 ORDER BY cid;"
        % sql_quote(table),
        "data_cols_%s" % table,
    )
    return [c for c in cols if c not in ("g",) and not c.startswith("xcol_")]


def mutate_schema(doltlite, db_path, branch, rng, step):
    schema = query_schema(doltlite, db_path, branch)
    tables = [line.split("|", 2)[1] for line in schema if line.startswith("table|aux_")]
    indexes = [line.split("|", 2)[1] for line in schema if line.startswith("index|aux_idx_")]
    shape_targets = list(SHAPE_TABLES) + tables
    actions = ["create_table"]
    if tables:
        actions.extend((
            "add_column", "add_column_default", "add_column_notnull",
            "rename_table", "drop_table",
            "create_index", "create_partial_index", "add_unique",
        ))
    if extra_columns(doltlite, db_path, branch, rng.choice(shape_targets) if shape_targets else "kv"):
        actions.extend(("drop_column", "rename_column"))
    else:
        actions.extend(("add_column_default", "add_column_notnull"))
    if indexes:
        actions.append("drop_index")
    action = rng.choice(actions)
    name = "aux_%d" % step
    sql = ""
    if action == "create_table":
        sql = aux_create_sql(name, rng)
    elif action == "add_column":
        sql = "ALTER TABLE %s ADD COLUMN xcol_%d TEXT;" % (rng.choice(shape_targets), step)
    elif action == "add_column_default":
        sql = (
            "ALTER TABLE %s ADD COLUMN xcol_%d TEXT DEFAULT %s;"
            % (rng.choice(shape_targets), step, sql_quote("d%d" % step))
        )
    elif action == "add_column_notnull":
        sql = (
            "ALTER TABLE %s ADD COLUMN xcol_%d INTEGER NOT NULL DEFAULT 0;"
            % (rng.choice(shape_targets), step)
        )
    elif action == "drop_column":
        target = rng.choice(shape_targets)
        extras = extra_columns(doltlite, db_path, branch, target)
        if extras:
            sql = "ALTER TABLE %s DROP COLUMN %s;" % (target, rng.choice(extras))
    elif action == "rename_column":
        target = rng.choice(shape_targets)
        extras = extra_columns(doltlite, db_path, branch, target)
        if extras:
            sql = "ALTER TABLE %s RENAME COLUMN %s TO xcol_%d;" % (
                target, rng.choice(extras), step,
            )
    elif action == "rename_table":
        sql = "ALTER TABLE %s RENAME TO %s;" % (rng.choice(tables), name)
    elif action == "drop_table":
        sql = "DROP TABLE %s;" % rng.choice(tables)
    elif action == "create_index":
        target = rng.choice(tables)
        cols = data_columns(doltlite, db_path, branch, target) or extra_columns(
            doltlite, db_path, branch, target
        )
        if cols:
            sql = "CREATE INDEX aux_idx_%d ON %s(%s);" % (step, target, cols[0])
    elif action == "create_partial_index":
        target = rng.choice(tables)
        cols = data_columns(doltlite, db_path, branch, target)
        if cols:
            sql = (
                "CREATE INDEX aux_idx_%d ON %s(%s) WHERE %s IS NOT NULL;"
                % (step, target, cols[0], cols[0])
            )
    elif action == "add_unique":
        target = rng.choice(tables)
        cols = extra_columns(doltlite, db_path, branch, target)
        if cols:
            sql = "CREATE UNIQUE INDEX aux_idx_%d ON %s(%s);" % (step, target, cols[0])
    else:
        sql = "DROP INDEX %s;" % rng.choice(indexes)
    if not sql:
        return
    run_sql(
        doltlite,
        db_for_branch(db_path, branch),
        sql,
        "ddl_%s_%s" % (action, branch),
        allowed_errors=(
            "duplicate column",
            "no such column",
            "cannot drop",
            "UNIQUE constraint failed",
            "error in view",
        ),
    )
    changed = query_schema(doltlite, db_path, branch)
    if sql and changed == schema and action not in (
        "drop_column", "rename_column", "add_unique",
    ):
        # drop/rename/unique can no-op when there is no extra column.
        if action == "create_table" or action.startswith("add_column"):
            raise AssertionError("DDL %s did not persist on %s" % (action, branch))
    if changed != query_schema(doltlite, db_path, branch):
        raise AssertionError("DDL %s did not persist on %s" % (action, branch))


def mutate_shapes(doltlite, db_path, branch, rng, step):
    """Write a disjoint key range into every PK shape in the same repo."""
    table = rng.choice(SHAPE_TABLES)
    key = branch_base(branch) + rng.randrange(1, 80)
    action = rng.choice(("insert", "update", "delete"))
    val = "%s_%05d" % (branch, step)
    n = rng.randrange(1000)
    k = "k%d" % key
    if action == "delete":
        if table in ("t_text", "t_wor"):
            sql = "DELETE FROM %s WHERE k=%s;" % (table, sql_quote(k))
        elif table == "t_comp":
            sql = "DELETE FROM t_comp WHERE a=%d AND b=%d;" % (key, branch_base(branch))
        else:
            sql = "DELETE FROM %s WHERE id=%d;" % (table, key)
    elif table == "t_int":
        sql = (
            "INSERT INTO t_int(id, v, n) VALUES(%d, %s, %d) "
            "ON CONFLICT(id) DO UPDATE SET v=excluded.v, n=excluded.n;"
            % (key, sql_quote(val), n)
        )
    elif table == "t_text":
        sql = (
            "INSERT INTO t_text(k, v, n) VALUES(%s, %s, %d) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v, n=excluded.n;"
            % (sql_quote(k), sql_quote(val), n)
        )
    elif table == "t_comp":
        sql = (
            "INSERT INTO t_comp(a, b, v, n) VALUES(%d, %d, %s, %d) "
            "ON CONFLICT(a, b) DO UPDATE SET v=excluded.v, n=excluded.n;"
            % (key, branch_base(branch), sql_quote(val), n)
        )
    elif table == "t_wor":
        sql = (
            "INSERT INTO t_wor(k, v, n) VALUES(%s, %s, %d) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v, n=excluded.n;"
            % (sql_quote(k), sql_quote(val), n)
        )
    elif table == "t_desc":
        sql = (
            "INSERT INTO t_desc(id, v, n) VALUES(%d, %s, %d) "
            "ON CONFLICT(id) DO UPDATE SET v=excluded.v, n=excluded.n;"
            % (key, sql_quote(val), n)
        )
    else:
        sql = (
            "INSERT INTO t_gen(id, n) VALUES(%d, %d) "
            "ON CONFLICT(id) DO UPDATE SET n=excluded.n;"
            % (key, n)
        )
    run_sql(
        doltlite,
        db_for_branch(db_path, branch),
        sql,
        "mutate_shape_%s_%s" % (table, branch),
        allowed_errors=("UNIQUE constraint failed", "CHECK constraint failed"),
    )


def assert_generated_columns(doltlite, db_path, branch):
    """STORED and VIRTUAL generated columns must match their expressions."""
    for table, sql in GENERATED_CHECKS:
        bad = query_scalar(doltlite, db_path, branch, sql, "generated_%s" % table)
        if bad != "0":
            raise AssertionError(
                "generated column drifted on %s %s: %s rows disagree"
                % (branch, table, bad)
            )


def wrap_vc_rollback(doltlite, db_path, branch, model):
    """BEGIN; dolt_add; ROLLBACK must leave rows and status untouched."""
    before_rows = query_rows(doltlite, db_path, branch)
    before_status = status_counts(doltlite, db_path, branch)
    run_sql(
        doltlite,
        db_for_branch(db_path, branch),
        "BEGIN; SELECT dolt_add('-A'); ROLLBACK;",
        "wrap_vc_rollback_%s" % branch,
    )
    after_rows = query_rows(doltlite, db_path, branch)
    after_status = status_counts(doltlite, db_path, branch)
    if after_rows != before_rows:
        raise AssertionError("ROLLBACK of dolt_add changed rows on %s" % branch)
    if after_status != before_status:
        raise AssertionError("ROLLBACK of dolt_add changed status on %s" % branch)
    assert_rows(doltlite, db_path, branch, model)


def merge_branch(doltlite, db_path, branches, model, rng):
    if len(branches) < 2:
        return
    target = rng.choice(branches)
    source_choices = [b for b in branches if b != target]
    source = rng.choice(source_choices)
    commit_branch(doltlite, db_path, target, model, 0)
    commit_branch(doltlite, db_path, source, model, 0)
    ours = set(schema_objects(doltlite, db_path, target))
    theirs = set(schema_objects(doltlite, db_path, source))
    base_hash = query_scalar(
        doltlite, db_path, target,
        "SELECT coalesce(dolt_merge_base(%s, %s), '');" % (sql_quote(target), sql_quote(source)),
        "merge_base",
    )
    base = set(schema_objects(doltlite, db_path, base_hash)) if base_hash else None
    out = run_sql(
        doltlite,
        db_for_branch(db_path, target),
        "SELECT dolt_merge(%s);" % sql_quote(source),
        "merge_%s_into_%s" % (source, target),
        timeout=30,
        allowed_errors=MERGE_ROLLED_BACK,
    )
    sync_vc_result(doltlite, db_path, target, model)
    if out is not None and base is not None:
        # Three-way rules on entryless objects. Names are branch-scoped, so no
        # two sides ever define one name differently: an object on both sides
        # survives, one added on a single side survives, and one that a single
        # side deleted since the base is the only thing that may go.
        after = set(schema_objects(doltlite, db_path, target))
        must_keep = (ours & theirs) | ((ours ^ theirs) - base)
        missing = must_keep - after
        resurrected = (base - ours - theirs) & after
        if missing or resurrected:
            raise AssertionError(
                "merge %s into %s: dropped=%r resurrected=%r"
                % (source, target, sorted(missing), sorted(resurrected))
            )


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
        ) + MERGE_ROLLED_BACK,
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
        allowed_errors=(
            "conflict",
            "rebase aborted",
            "didn't identify any commits",
        ) + MERGE_ROLLED_BACK,
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

# Several PK shapes live in the same repo so one run exercises integer, TEXT,
# composite, WITHOUT ROWID and DESC keys together. Generated columns are
# checked after every operation with `g IS NOT <expr>`.
SHAPE_TABLES = ("t_int", "t_text", "t_comp", "t_wor", "t_desc", "t_gen")

SHAPE_SCHEMA = (
    "CREATE TABLE t_int(\n"
    "  id INTEGER PRIMARY KEY,\n"
    "  v TEXT,\n"
    "  n INTEGER,\n"
    "  g INTEGER GENERATED ALWAYS AS (n + 1) STORED\n"
    ");\n"
    "CREATE INDEX t_int_g ON t_int(g);\n"
    "CREATE TABLE t_text(k TEXT PRIMARY KEY, v TEXT, n INTEGER);\n"
    "CREATE TABLE t_comp(\n"
    "  a INTEGER NOT NULL,\n"
    "  b INTEGER NOT NULL,\n"
    "  v TEXT,\n"
    "  n INTEGER,\n"
    "  PRIMARY KEY(a, b)\n"
    ");\n"
    "CREATE TABLE t_wor(k TEXT PRIMARY KEY, v TEXT, n INTEGER) WITHOUT ROWID;\n"
    # PRIMARY KEY(id DESC) on a rowid table corrupts merge; WITHOUT ROWID is the
    # DESC clustered-PK shape that still round-trips.
    "CREATE TABLE t_desc(\n"
    "  id INTEGER NOT NULL,\n"
    "  v TEXT,\n"
    "  n INTEGER,\n"
    "  PRIMARY KEY(id DESC)\n"
    ") WITHOUT ROWID;\n"
    "CREATE TABLE t_gen(\n"
    "  id INTEGER PRIMARY KEY,\n"
    "  n INTEGER,\n"
    "  g INTEGER GENERATED ALWAYS AS (n * 2) VIRTUAL\n"
    ");\n"
    "CREATE INDEX t_gen_g ON t_gen(g);\n"
    "INSERT INTO t_int(id, v, n) VALUES(0, 'base', 0);\n"
    "INSERT INTO t_text VALUES('k0', 'base', 0);\n"
    "INSERT INTO t_comp VALUES(0, 0, 'base', 0);\n"
    "INSERT INTO t_wor VALUES('k0', 'base', 0);\n"
    "INSERT INTO t_desc VALUES(0, 'base', 0);\n"
    "INSERT INTO t_gen(id, n) VALUES(0, 0);\n"
)

GENERATED_CHECKS = (
    ("t_int", "SELECT count(*) FROM t_int WHERE g IS NOT (n + 1);"),
    ("t_gen", "SELECT count(*) FROM t_gen WHERE g IS NOT (n * 2);"),
)

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
    "CREATE TABLE audit(id INTEGER PRIMARY KEY, note TEXT NOT NULL);\n"
    "CREATE VIEW v_seed AS SELECT id, v FROM kv;\n"
    "CREATE TRIGGER tr_seed AFTER INSERT ON child BEGIN\n"
    "  INSERT INTO audit(id, note) SELECT new.id, 'seed'\n"
    "  WHERE NOT EXISTS (SELECT 1 FROM audit WHERE id = new.id);\n"
    "END;\n"
)

# Views and triggers are entryless catalog rows: they have no prolly tree of
# their own and travel through commits, merges and resets as sqlite_master text.
# Names are branch-scoped so two branches never define the same name, and every
# definition reads only kv.id, kv.v, child.id and parent.id, columns no
# operation ever drops or renames; a view that cannot be queried is therefore a
# bug, not a stale definition. Triggers write to audit keyed by the branch-scoped
# child id, so their side effects merge without collision and a probe insert can
# check that the triggers present in the schema actually fire. Their bodies
# insert only when the row is absent rather than OR REPLACE: an upsert that
# fires a trigger overrides the trigger's own conflict clause (stock SQLite does
# the same), so OR REPLACE inside a trigger can still raise UNIQUE.


def setup_repo(doltlite, db_path, remote_path=None):
    run_sql(
        doltlite,
        db_path,
        (
            "CREATE TABLE kv(id INTEGER PRIMARY KEY, v TEXT, n INTEGER);\n"
            "INSERT INTO kv VALUES(0, 'base', 0);\n"
            + RELATED_SCHEMA
            + SHAPE_SCHEMA
            + "INSERT INTO parent SELECT value, 'p' || value "
              "FROM generate_series(1, %d);\n" % SEED_PARENTS
            + "SELECT dolt_commit('-A','-m','init');\n"
        ),
        "setup",
    )
    if not remote_path:
        return
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
    + ["mutate_shapes"] * 3
    + ["schema_objects"] * 2
    + ["ddl"] * 3
    + ["wrap_vc_rollback"]
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


def setup_check(doltlite, db_path):
    setup_repo(doltlite, db_path)
    names = query_list(
        doltlite,
        db_path,
        "main",
        "SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name;",
        "setup_tables",
    )
    missing = [n for n in SHAPE_TABLES if n not in names]
    if missing:
        raise AssertionError("setup missing shape tables: %r have=%r" % (missing, names))
    assert_generated_columns(doltlite, db_path, "main")
    print("setup_tables " + " ".join(names))
    print("generated_ok")
    return 0


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    flags = [a for a in sys.argv[1:] if a.startswith("--")]
    doltlite = resolve_engine(args[0] if args else "./doltlite")
    setup_db = args[1] if len(args) > 1 else None
    if "--setup-check" in flags:
        tmp = None
        db_path = setup_db
        if not db_path:
            tmp = tempfile.mkdtemp(prefix="doltlite-shape-")
            db_path = os.path.join(tmp, "stateful.db")
        try:
            return setup_check(doltlite, db_path)
        finally:
            if tmp and os.environ.get("DOLTLITE_VC_STATEFUL_KEEP_DB") != "1":
                shutil.rmtree(tmp, ignore_errors=True)
    seconds = int(os.environ.get("DOLTLITE_VC_STATEFUL_SECONDS", "600"))
    max_steps = int(os.environ.get("DOLTLITE_VC_STATEFUL_STEPS", "0"))
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
        "stateful vc fuzzer: seed=%d seconds=%d steps=%s db=%s"
        % (seed, seconds, max_steps or "unlimited", db_path),
        flush=True,
    )
    try:
        setup_repo(doltlite, db_path, remote_path)
        while time.time() < deadline:
            if max_steps and step >= max_steps:
                break
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
            elif op == "mutate_shapes":
                mutate_shapes(doltlite, db_path, branch, rng, step)
            elif op == "wrap_vc_rollback":
                wrap_vc_rollback(doltlite, db_path, branch, model)
            elif op == "schema_objects":
                mutate_schema_objects(doltlite, db_path, branch, rng, step)
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
