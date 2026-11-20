// Package-level smoke test for DoltHub.Doltlite. Runs from a consumer
// project against the installed package, covering the surface end to end:
// provider init, DDL/DML through Microsoft.Data.Sqlite.Core, parameters and
// column values across every type, error paths, and a version-control round
// trip (commit, branch, checkout, merge, dolt_log reads).

using DoltHub.Doltlite;
using Microsoft.Data.Sqlite;
using System.Diagnostics;
using System.Reflection;

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

object? QueryScalar(SqliteConnection connection, string sql, string? message = null)
{
    using var command = connection.CreateCommand();
    command.CommandText = sql;
    if (message is not null)
    {
        command.Parameters.AddWithValue("$message", message);
    }
    return command.ExecuteScalar();
}

void CheckEngine(SqliteConnection connection)
{
    Check("DoltLite engine is active", QueryScalar(connection, "SELECT doltlite_engine()"), "prolly");
    string? version = (string?)QueryScalar(connection, "SELECT dolt_version()");
    string? expected = Environment.GetEnvironmentVariable("DOLTLITE_TEST_ENGINE_VERSION");
    if (string.IsNullOrEmpty(expected))
    {
        Check("DoltLite version reports", version?.Length > 0, true);
    }
    else
    {
        Check("DoltLite version matches the release", version, expected);
    }
}

const string FeatureCommit = "feature work";
if (args is ["--reopen", var path, var expectedCommit])
{
    Doltlite.Init();
    var settings = new SqliteConnectionStringBuilder
    {
        DataSource = path,
        Mode = SqliteOpenMode.ReadOnly,
        Pooling = false,
    };
    using var connection = new SqliteConnection(settings.ToString());
    connection.Open();
    CheckEngine(connection);
    Check("reopened row count", QueryScalar(connection, "SELECT count(*) FROM t"), 3L);
    Check("reopened merged row", QueryScalar(connection, "SELECT s FROM t WHERE pk = 1"), "branched");
    Check("reopened UTF-8 text", QueryScalar(connection, "SELECT s FROM t WHERE pk = 2"), "twö");
    Check("reopened blob", QueryScalar(connection, "SELECT b FROM t WHERE pk = 2"), new byte[] { 1, 0, 2 });
    Check("reopened NULL", QueryScalar(connection, "SELECT s FROM t WHERE pk = 3"), DBNull.Value);
    Check("reopened commit hash",
        QueryScalar(connection, "SELECT commit_hash FROM dolt_log WHERE message = $message", FeatureCommit),
        expectedCommit);
    return failures == 0 ? 0 : 1;
}

// A library that loads but is not an engine must say so, not fail as a null
// dereference inside the provider. Runs before Init() since the provider is
// process-wide and set once; the harness builds the decoy.
string? decoy = Environment.GetEnvironmentVariable("DOLTLITE_TEST_DECOY");
if (string.IsNullOrEmpty(decoy))
{
    Console.WriteLine("  SKIP: non-engine library is rejected (no decoy built)");
}
else
{
    Environment.SetEnvironmentVariable("DOLTLITE_NET_LIB", decoy);
    try
    {
        Doltlite.Init();
        Check("non-engine library is rejected", "no exception", "exception");
    }
    catch (EntryPointNotFoundException e)
    {
        Check("non-engine library is rejected",
            e.Message.Contains("does not export"), true);
    }
    Environment.SetEnvironmentVariable("DOLTLITE_NET_LIB", null);
}

Doltlite.Init();

string dbPath = Path.Combine(
    Path.GetTempPath(), $"doltlite-net-smoke-{Environment.ProcessId}.db");
string[] dbFiles = { dbPath, $"{dbPath}-lock" };

void Cleanup()
{
    // Disposing a SqliteConnection returns it to the pool rather than
    // closing the handle, and Windows refuses to delete a file still open.
    SqliteConnection.ClearAllPools();
    foreach (string f in dbFiles)
    {
        try
        {
            File.Delete(f);
        }
        catch (IOException)
        {
        }
    }
}

Cleanup();

string? featureCommit;
using (var conn = new SqliteConnection($"Data Source={dbPath}"))
{
    conn.Open();

    object? Scalar(string sql) => QueryScalar(conn, sql);
    int Exec(string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteNonQuery();
    }

    CheckEngine(conn);

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
    featureCommit = (string?)QueryScalar(conn, "SELECT dolt_commit('-A', '-m', $message)", FeatureCommit);
    Check("dolt_commit on branch", featureCommit?.Length > 0, true);
    Check("dolt_checkout main", Scalar("SELECT dolt_checkout('main')"), 0L);
    Check("main unchanged", Scalar("SELECT s FROM t WHERE pk = 1"), "one");
    Scalar("SELECT dolt_merge('feature')");
    Check("dolt_merge fast-forwards", Scalar("SELECT s FROM t WHERE pk = 1"), "branched");
    Check("dolt_log sees both commits", (long)Scalar("SELECT count(*) FROM dolt_log")! >= 2, true);
}

SqliteConnection.ClearAllPools();
if (!string.IsNullOrEmpty(featureCommit))
{
    var launch = new ProcessStartInfo("dotnet") { UseShellExecute = false };
    launch.ArgumentList.Add(Assembly.GetExecutingAssembly().Location);
    launch.ArgumentList.Add("--reopen");
    launch.ArgumentList.Add(dbPath);
    launch.ArgumentList.Add(featureCommit);
    using var child = Process.Start(launch) ?? throw new InvalidOperationException("Could not start the reopen probe");
    if (child.WaitForExit(30_000))
    {
        Check("fresh process reopened the file and commit", child.ExitCode, 0);
    }
    else
    {
        child.Kill();
        child.WaitForExit();
        Check("fresh process reopened the file and commit", "timed out", 0);
    }
}

Cleanup();

Console.WriteLine(failures == 0
    ? "smoke-test: all checks passed"
    : $"smoke-test: {failures} failure(s)");
return failures == 0 ? 0 : 1;
