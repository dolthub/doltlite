// Package-level smoke test for DoltHub.Doltlite. Runs from a consumer
// project against the installed package, covering the surface end to end:
// provider init, DDL/DML through Microsoft.Data.Sqlite.Core, parameters and
// column values across every type, error paths, and a version-control round
// trip (commit, branch, checkout, merge, dolt_log reads).

using DoltHub.Doltlite;
using Microsoft.Data.Sqlite;

int failures = 0;

void Check(string name, object? got, object? want)
{
    bool ok = Equals(got, want) ||
        (got is byte[] g && want is byte[] w && g.AsSpan().SequenceEqual(w));
    if (ok)
    {
        Console.WriteLine($"  PASS: {name}");
    }
    else
    {
        failures++;
        Console.WriteLine($"  FAIL: {name}\n    want: {want}\n    got:  {got}");
    }
}

Doltlite.Init();

string dbPath = Path.Combine(
    Path.GetTempPath(), $"doltlite-net-smoke-{Environment.ProcessId}.db");
foreach (string f in new[] { dbPath, $"{dbPath}-lock" })
{
    File.Delete(f);
}

using (var conn = new SqliteConnection($"Data Source={dbPath}"))
{
    conn.Open();

    object? Scalar(string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteScalar();
    }
    int Exec(string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteNonQuery();
    }

    Check("engine version reports", ((string?)Scalar("SELECT sqlite_version()"))?.Length > 0, true);

    Exec("CREATE TABLE t(pk INTEGER PRIMARY KEY, s TEXT, f REAL, b BLOB)");
    Check("insert changes", Exec("INSERT INTO t(pk, s, f, b) VALUES (1, 'one', 1.5, x'00ff10')"), 1);

    using (var ins = conn.CreateCommand())
    {
        ins.CommandText = "INSERT INTO t(pk, s, f, b) VALUES ($pk, $s, $f, $b)";
        ins.Parameters.AddWithValue("$pk", 2);
        ins.Parameters.AddWithValue("$s", "twö");
        ins.Parameters.AddWithValue("$f", 2.25);
        ins.Parameters.AddWithValue("$b", new byte[] { 1, 0, 2 });
        Check("parameterized insert", ins.ExecuteNonQuery(), 1);
    }
    using (var ins = conn.CreateCommand())
    {
        ins.CommandText = "INSERT INTO t(pk, s) VALUES ($pk, $s)";
        ins.Parameters.AddWithValue("$pk", 3);
        ins.Parameters.AddWithValue("$s", DBNull.Value);
        ins.ExecuteNonQuery();
    }

    using (var sel = conn.CreateCommand())
    {
        sel.CommandText = "SELECT s, f, b FROM t WHERE pk = 2";
        using var r = sel.ExecuteReader();
        Check("row exists", r.Read(), true);
        Check("text round trip (utf8)", r.GetString(0), "twö");
        Check("float round trip", r.GetDouble(1), 2.25);
        Check("blob round trip", (byte[])r.GetValue(2), new byte[] { 1, 0, 2 });
    }
    Check("null round trip", Scalar("SELECT s FROM t WHERE pk = 3"), DBNull.Value);
    Check("int round trip", Scalar("SELECT pk FROM t WHERE pk = 1"), 1L);

    try
    {
        Scalar("SELECT * FROM missing_table");
        Check("bad SQL throws", "no exception", "exception");
    }
    catch (SqliteException e)
    {
        Check("bad SQL throws", e.Message.Contains("missing_table"), true);
    }

    // Version control is plain SQL through the same connection.
    Check("dolt_commit", ((string?)Scalar("SELECT dolt_commit('-A', '-m', 'first commit')"))?.Length > 0, true);
    Check("dolt_branch", Scalar("SELECT dolt_branch('feature')"), 0L);
    Check("dolt_checkout feature", Scalar("SELECT dolt_checkout('feature')"), 0L);
    Exec("UPDATE t SET s = 'branched' WHERE pk = 1");
    Check("dolt_commit on branch", ((string?)Scalar("SELECT dolt_commit('-A', '-m', 'feature work')"))?.Length > 0, true);
    Check("dolt_checkout main", Scalar("SELECT dolt_checkout('main')"), 0L);
    Check("main unchanged", Scalar("SELECT s FROM t WHERE pk = 1"), "one");
    Scalar("SELECT dolt_merge('feature')");
    Check("dolt_merge fast-forwards", Scalar("SELECT s FROM t WHERE pk = 1"), "branched");
    Check("dolt_log sees both commits", (long)Scalar("SELECT count(*) FROM dolt_log")! >= 2, true);
}

File.Delete(dbPath);

Console.WriteLine(failures == 0
    ? "smoke-test: all checks passed"
    : $"smoke-test: {failures} failure(s)");
return failures == 0 ? 0 : 1;
