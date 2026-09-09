# Remote server (`doltlite-remotesrv`)

> [!WARNING]
> The server binds to `127.0.0.1` by default. Bound anywhere else, it warns at
> startup about each protection left unconfigured: `--cert`/`--key` for TLS, and
> `--auth-keys` plus `--audience` for authentication. These are independent — TLS encrypts but does
> not authenticate, and without `--auth-keys` every client that can reach the port
> may read the served databases *and push to them*. Configure both, or place the
> server behind a reverse proxy that provides equivalent TLS and authentication.

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
SELECT dolt_creds('export', '<credential-id>', '/path/to/authorized-keys');
```

With no directory argument, `dolt_creds('export', '<credential-id>')` returns
the public JWK. The server rejects private credential files in `--auth-keys`.
Default HTTP timeout is 30s (`DOLTLITE_HTTP_TIMEOUT_MS`). Embeddable as
`doltliteServeAsync` in `doltlite_remotesrv.h`. Transfers are content-addressed.
JWT, TLS, and credential-store details:
[doc/doltlite/auth.md](auth.md).
