# Remote server (`doltlite-remotesrv`)

> [!WARNING]
> The server binds to `127.0.0.1` by default. Bound anywhere else, it warns at
> startup about each protection left unconfigured: `--cert`/`--key` for TLS, and
> `--auth-keys` plus `--audience` for clients. TLS authenticates only the server;
> without client auth, anyone may read *and push*. Configure both or use a proxy.

Standalone HTTP server for a directory of databases (`make doltlite-remotesrv`
in `build/`):

```
./doltlite-remotesrv -p 8080 /path/to/databases/
./doltlite-remotesrv -p 8080 --bind 0.0.0.0 /path/to/databases/   # all interfaces
./doltlite-remotesrv -p 8443 --bind 0.0.0.0 \
  --cert server.crt --key server.key \
  --auth-keys /path/to/authorized-keys --audience db.example.com \
  /path/to/databases/
```

Each `.db` is at `http://host:8080/filename.db` (or the HTTPS URL). Clients use
the system trust store (`DOLTLITE_CA_FILE` for a private CA); credentials live
in `~/.doltlite/creds` (`SELECT dolt_creds_new();`). Authorize one without
copying its private seed by exporting its public JWK directly into the server's
key directory:

```sql
SELECT dolt_creds_export('<credential-id>', '/path/to/authorized-keys');
```

With no directory argument, `dolt_creds_export('<credential-id>')` returns
the public JWK. The server rejects private credential files in `--auth-keys`.
Select the client key with `dolt_creds_use('<credential-id>')`, or use
`dolt_creds_bind(remote_or_https_url, key)` to bind a key to this server's
repository URL. See [credential management](dolt_creds.md) for the commands
and selection precedence.

With TLS and `--auth-keys` enabled, each request carries the key selected by
`dolt_creds_use` or `dolt_creds_bind`. For HTTPS writes to a server that
permits anonymous access, select `'anonymous'` explicitly; public reads can
proceed without a key.

Default HTTP timeout is 30s (`DOLTLITE_HTTP_TIMEOUT_MS`). The server's
`--timeout-ms` (default 30 seconds) bounds the TLS handshake and the request
body. Request headers must arrive within 2 seconds, or within `--timeout-ms`
when that is shorter, and an idle connection does not take a worker.
Embeddable as
`doltliteServeAsync` in `doltlite_remotesrv.h`. Transfers are content-addressed.
JWT, TLS, and credential-store details:
[doc/doltlite/auth.md](auth.md).

## Pre-receive hooks

On Linux and macOS, `--pre-receive PROGRAM` runs an executable before a push
changes a branch or tag. Use it to enforce server-side push policies:

```sh
./doltlite-remotesrv --pre-receive /path/to/pre-receive \
  --pre-receive-timeout-ms 5000 /path/to/databases/
```

Give the executable's path; the server does not search `PATH` or interpret a
shell command. A script needs a shebang and executable permission
(`chmod +x /path/to/pre-receive`). The program runs with no arguments and empty
stdin. It inherits the server's environment, with these variables set for the
single ref being updated:

| Variable | Value |
|---|---|
| `DOLTLITE_DB` | Database name from the URL path, such as `app.db`. |
| `DOLTLITE_DB_PATH` | Absolute path to the database file. |
| `DOLTLITE_REF` | `refs/heads/<branch>` or `refs/tags/<tag>`. |
| `DOLTLITE_BRANCH` | Branch name, empty for a tag. |
| `DOLTLITE_TAG` | Tag name, empty for a branch. |
| `DOLTLITE_OLD` | Current commit hash, empty when the ref does not exist. |
| `DOLTLITE_NEW` | Proposed commit hash, empty when deleting the ref. |
| `DOLTLITE_FORCE` | `1` when the push's force flag is set, otherwise `0`. Tag pushes currently set it to `1`. |
| `DOLTLITE_KID` | Verified bearer token's key ID with `--auth-keys`, otherwise empty. |

For example, save this as `/path/to/pre-receive` to prevent force-pushes and
deletion of `main`, while allowing updates to other refs:

```sh
#!/bin/sh
if [ "$DOLTLITE_REF" = refs/heads/main ]; then
  if [ "$DOLTLITE_FORCE" = 1 ] || [ -z "$DOLTLITE_NEW" ]; then
    echo "main cannot be force-pushed or deleted"
    exit 1
  fi
fi
exit 0
```

Exit status `0` allows the ref update. Any other exit status, failure to execute,
or termination by a signal rejects it and leaves the ref unchanged. Write a
rejection reason to stdout: its first line, limited to 159 bytes and sanitized
for the response, is returned to the client as HTTP `403` with code `rejected`.
`dolt_push` displays that reason. An empty reason becomes
`push refused by the pre-receive hook`.

The hook timeout defaults to 10,000 ms. Override it with a positive
`--pre-receive-timeout-ms` value, which requires `--pre-receive`. On timeout,
the server kills the hook's process group and rejects the push with
`pre-receive hook timed out`. Windows refuses `--pre-receive` at startup.

The hook runs after ref-scope, fast-forward, and target-graph validation, while
the server holds the database's store lock and before installing the ref.
`DOLTLITE_OLD` is therefore the commit being replaced; another push cannot move
it during the hook. Invalid pushes and pushes that change nothing do not run
the hook. Other requests needing the same database lock wait for it, so keep
hooks short.

For non-deletion pushes, the incoming commit is already readable by hash from
another connection to `DOLTLITE_DB_PATH`. For example, a hook can inspect its
commit message using an installed `doltlite` executable:

```sh
doltlite -readonly "$DOLTLITE_DB_PATH" \
  "SELECT message FROM dolt_log('$DOLTLITE_NEW') LIMIT 1;"
```

Use `dolt_at_<table>('<new>')` to inspect proposed rows or
`dolt_diff_<table>('<old>','<new>')` to compare existing and proposed commits.
Plain `dolt_log` still follows the current branch history until the ref is
installed. See [history queries](refs.md) and [row diffs](dolt_diff.md).

For an embedded server, pass a `DoltliteServeHooks` containing `zPreReceive`
and `preReceiveTimeoutMs` to `doltliteServeAsyncHooks(opts, hooks)` from
`doltlite_remotesrv.h`. A nonpositive timeout selects the 10,000 ms default.
