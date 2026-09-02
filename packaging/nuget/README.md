# DoltHub.Doltlite

[DoltLite](https://github.com/dolthub/doltlite) for .NET: SQLite with
Git-style version control — branch, commit, merge, and diff your relational
data. This package bundles the `libdoltlite` native engine per platform and
plugs it into [SQLitePCLRaw](https://github.com/ericsink/SQLitePCL.raw), the
layer the .NET SQLite ecosystem already builds on — so
`Microsoft.Data.Sqlite.Core`, EF Core's Sqlite provider, and Dapper work
unchanged on top of it.

## Install

```sh
dotnet add package DoltHub.Doltlite
dotnet add package Microsoft.Data.Sqlite.Core
```

Use `Microsoft.Data.Sqlite.Core`, not `Microsoft.Data.Sqlite`: the non-Core
package pins SQLitePCLRaw to its own bundled stock engine.

## Use

```csharp
using DoltHub.Doltlite;
using Microsoft.Data.Sqlite;

Doltlite.Init(); // once at startup, before the first connection

using var conn = new SqliteConnection("Data Source=app.db");
conn.Open();

var create = conn.CreateCommand();
create.CommandText = "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)";
create.ExecuteNonQuery();

// Version control is plain SQL on the same connection.
var commit = conn.CreateCommand();
commit.CommandText = "SELECT dolt_commit('-A', '-m', 'add users table')";
commit.ExecuteScalar();
```

With EF Core, configure the provider as usual
(`options.UseSqlite("Data Source=app.db")`) after calling `Doltlite.Init()`,
and reach version control through
`db.Database.ExecuteSqlRaw("SELECT dolt_commit(...)")`. Every DoltLite
feature — branches, merges, the `dolt_log`/`dolt_diff`/`dolt_branches`
tables, remotes — is reachable through SQL; see the
[DoltLite documentation](https://github.com/dolthub/doltlite).

## Requirements

- .NET 6.0 or later.
- Bundled native engines: `linux-x64`, `linux-arm64`, `osx-arm64`, and
  `win-x64`. For anything else, set `DOLTLITE_NET_LIB` to the path of a
  `libdoltlite` shared library built from source.

## Source

The package source lives in
[dolthub/doltlite](https://github.com/dolthub/doltlite) under
`packaging/nuget/`; send issues and pull requests there.
