// Package-level smoke test for the doltlite crate. Runs from a consumer
// project against the packaged crate, covering the surface end to end: the
// bundled engine, every value type, error paths, and a version-control round
// trip (commit, branch, checkout, merge, dolt_log reads).

use doltlite::{Connection, OpenFlags, Value};

fn main() -> std::process::ExitCode {
    let mut failures = 0usize;
    let mut check = |name: &str, got: String, want: String| {
        if got == want {
            println!("  PASS: {name}");
        } else {
            failures += 1;
            println!("  FAIL: {name}\n    want: {want}\n    got:  {got}");
        }
    };

    let dir = std::env::temp_dir();
    let db_path = dir.join(format!("doltlite-rs-smoke-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(dir.join(format!(
        ".doltlite-rs-smoke-{}.db-lock",
        std::process::id()
    )));

    let db = Connection::open(&db_path).expect("open");
    check(
        "engine version reports",
        (!doltlite::version().is_empty()).to_string(),
        "true".into(),
    );

    db.execute_batch("CREATE TABLE t(pk INTEGER PRIMARY KEY, s TEXT, f REAL, b BLOB)")
        .expect("create");
    let n = db
        .execute(
            "INSERT INTO t(pk, s, f, b) VALUES (?, ?, ?, ?)",
            &[
                Value::from(1),
                Value::from("one"),
                Value::from(1.5),
                Value::from(vec![0u8, 255, 16]),
            ],
        )
        .expect("insert");
    check("insert changes", n.to_string(), "1".into());
    check(
        "last_insert_rowid",
        db.last_insert_rowid().to_string(),
        "1".into(),
    );

    db.execute(
        "INSERT INTO t(pk, s, f, b) VALUES (?, ?, ?, ?)",
        &[
            Value::from(2),
            Value::from("twö"),
            Value::from(2.25),
            Value::from(vec![1u8, 0, 2]),
        ],
    )
    .expect("insert 2");
    db.execute(
        "INSERT INTO t(pk, s) VALUES (?, ?)",
        &[Value::from(3), Value::Null],
    )
    .expect("insert 3");

    let row = db
        .query_row("SELECT s, f, b FROM t WHERE pk = 2", &[])
        .expect("query")
        .expect("row");
    check(
        "text round trip (utf8)",
        row.get(0).unwrap().as_str().unwrap().to_string(),
        "twö".into(),
    );
    check(
        "float round trip",
        row.get(1).unwrap().as_f64().unwrap().to_string(),
        "2.25".into(),
    );
    check(
        "blob round trip",
        format!("{:?}", row.get(2).unwrap().as_blob().unwrap()),
        format!("{:?}", [1u8, 0, 2]),
    );
    check(
        "named column access",
        row.get_named("s").unwrap().as_str().unwrap().to_string(),
        "twö".into(),
    );

    let bad = db
        .query_row("SELECT CAST(X'FF61' AS TEXT), CAST(X'610062' AS TEXT)", &[])
        .expect("q")
        .expect("row");
    check(
        "invalid utf8 text keeps its bytes",
        format!("{:?}", bad.get(0).unwrap()),
        format!("{:?}", Value::Blob(vec![0xFF, 0x61])),
    );
    check(
        "text with an embedded NUL stays text",
        format!("{:?}", bad.get(1).unwrap()),
        format!("{:?}", Value::Text("a\0b".into())),
    );

    let nullrow = db
        .query_row("SELECT s FROM t WHERE pk = 3", &[])
        .expect("q")
        .expect("row");
    check(
        "null round trip",
        nullrow.get(0).unwrap().is_null().to_string(),
        "true".into(),
    );
    check(
        "no rows is None",
        db.query_row("SELECT s FROM t WHERE pk = 99", &[])
            .expect("q")
            .is_none()
            .to_string(),
        "true".into(),
    );
    check(
        "query returns all rows",
        db.query("SELECT pk FROM t ORDER BY pk", &[])
            .expect("q")
            .len()
            .to_string(),
        "3".into(),
    );

    match db.query_row("SELECT * FROM missing_table", &[]) {
        Ok(_) => check("bad SQL errors".into(), "ok".into(), "error".into()),
        Err(e) => check(
            "bad SQL errors",
            e.message.contains("missing_table").to_string(),
            "true".into(),
        ),
    }

    // Version control is plain SQL on the same connection.
    let commit = db
        .query_row("SELECT dolt_commit('-A', '-m', 'first commit')", &[])
        .expect("commit")
        .expect("row");
    check(
        "dolt_commit",
        (!commit.get(0).unwrap().as_str().unwrap().is_empty()).to_string(),
        "true".into(),
    );
    let scalar = |sql: &str| -> String {
        db.query_row(sql, &[])
            .expect("q")
            .and_then(|r| r.get(0).cloned())
            .map(|v| match v {
                Value::Integer(i) => i.to_string(),
                Value::Text(t) => t,
                other => format!("{other:?}"),
            })
            .unwrap_or_default()
    };
    let engine_version = scalar("SELECT dolt_version()");
    check(
        "dolt_version is stamped, not the amalgamation placeholder",
        (engine_version.starts_with('v') && engine_version != "doltlite-amalgamation").to_string(),
        "true".into(),
    );
    if let Ok(want) = std::env::var("DOLTLITE_EXPECT_VERSION") {
        check("dolt_version matches the crate version", engine_version, want);
    }

    check("dolt_branch", scalar("SELECT dolt_branch('feature')"), "0".into());
    check(
        "dolt_checkout feature",
        scalar("SELECT dolt_checkout('feature')"),
        "0".into(),
    );
    db.execute("UPDATE t SET s = 'branched' WHERE pk = 1", &[])
        .expect("update");
    check(
        "dolt_commit on branch",
        (!scalar("SELECT dolt_commit('-A', '-m', 'feature work')").is_empty()).to_string(),
        "true".into(),
    );
    check(
        "dolt_checkout main",
        scalar("SELECT dolt_checkout('main')"),
        "0".into(),
    );
    check("main unchanged", scalar("SELECT s FROM t WHERE pk = 1"), "one".into());
    scalar("SELECT dolt_merge('feature')");
    check(
        "dolt_merge fast-forwards",
        scalar("SELECT s FROM t WHERE pk = 1"),
        "branched".into(),
    );
    check(
        "dolt_log sees both commits",
        (scalar("SELECT count(*) FROM dolt_log").parse::<i64>().unwrap() >= 2).to_string(),
        "true".into(),
    );

    drop(db);

    let before = std::fs::read(&db_path).expect("read database");
    let db = Connection::open_with_flags(&db_path, OpenFlags::READ_ONLY).expect("read-only open");
    check(
        "read-only reads existing data",
        db.query_row("SELECT count(*) FROM t", &[])
            .expect("read-only query")
            .expect("row")
            .get(0)
            .unwrap()
            .as_i64()
            .unwrap()
            .to_string(),
        "3".into(),
    );
    for sql in [
        "INSERT INTO t(pk) VALUES(99)",
        "CREATE TABLE forbidden(id INTEGER PRIMARY KEY)",
        "SELECT dolt_branch('forbidden')",
    ] {
        check(
            "read-only refuses mutation",
            db.execute_batch(sql)
                .err()
                .map(|e| e.code)
                .unwrap_or(0)
                .to_string(),
            "8".into(),
        );
    }
    drop(db);
    check(
        "read-only preserves bytes",
        (std::fs::read(&db_path).unwrap() == before).to_string(),
        "true".into(),
    );

    let uri = format!("file:{}?mode=ro", db_path.display());
    let db = Connection::open_with_flags(uri, OpenFlags::READ_ONLY | OpenFlags::URI)
        .expect("read-only URI open");
    db.query_row("SELECT * FROM t", &[]).expect("URI query");
    drop(db);
    for flags in [
        OpenFlags::CREATE,
        OpenFlags::READ_ONLY | OpenFlags::READ_WRITE,
    ] {
        check(
            "invalid open flags refused",
            Connection::open_with_flags(&db_path, flags)
                .err()
                .map(|e| e.code)
                .unwrap_or(0)
                .to_string(),
            "21".into(),
        );
    }
    let db = Connection::open_with_flags(&db_path, OpenFlags::READ_WRITE).expect("existing open");
    db.execute_batch("INSERT INTO t(pk) VALUES(99)")
        .expect("existing write");
    drop(db);
    let _ = std::fs::remove_file(&db_path);

    for flags in [OpenFlags::READ_ONLY, OpenFlags::READ_WRITE] {
        check(
            "missing file refused",
            Connection::open_with_flags(&db_path, flags)
                .err()
                .map(|e| e.code)
                .unwrap_or(0)
                .to_string(),
            "14".into(),
        );
        check(
            "missing file not created",
            db_path.exists().to_string(),
            "false".into(),
        );
    }
    let db = Connection::open_with_flags(&db_path, OpenFlags::READ_WRITE | OpenFlags::CREATE)
        .expect("explicit create");
    db.execute_batch("CREATE TABLE saved(id INTEGER PRIMARY KEY)")
        .expect("create saved");
    drop(db);
    let valid = std::fs::read(&db_path).unwrap();
    for cut in [168, 500, 1000] {
        for flags in [
            OpenFlags::READ_ONLY,
            OpenFlags::READ_WRITE,
            OpenFlags::READ_WRITE | OpenFlags::CREATE,
        ] {
            std::fs::write(&db_path, &valid[..cut]).unwrap();
            let code = match Connection::open_with_flags(&db_path, flags) {
                Ok(db) => db
                    .execute_batch("SELECT count(*) FROM sqlite_schema")
                    .err()
                    .map(|e| e.code)
                    .unwrap_or(0),
                Err(e) => e.code,
            };
            check("truncated file refused", code.to_string(), "11".into());
            check(
                "truncated file preserved",
                (std::fs::read(&db_path).unwrap() == valid[..cut]).to_string(),
                "true".into(),
            );
        }
    }
    let _ = std::fs::remove_file(&db_path);

    if failures == 0 {
        println!("smoke-test: all checks passed");
        std::process::ExitCode::SUCCESS
    } else {
        println!("smoke-test: {failures} failure(s)");
        std::process::ExitCode::FAILURE
    }
}
