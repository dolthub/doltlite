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
