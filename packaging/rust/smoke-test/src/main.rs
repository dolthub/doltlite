// Package-level smoke test for the doltlite crate. Runs from a consumer
// project against the packaged crate, covering the surface end to end: the
// bundled engine, every value type, error paths, and a version-control round
// trip (commit, branch, checkout, merge, dolt_log reads).

use doltlite::{Connection, Value};

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
    let _ = std::fs::remove_file(&db_path);

    if failures == 0 {
        println!("smoke-test: all checks passed");
        std::process::ExitCode::SUCCESS
    } else {
        println!("smoke-test: {failures} failure(s)");
        std::process::ExitCode::FAILURE
    }
}
