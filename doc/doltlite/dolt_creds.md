# dolt_creds and dolt_creds_new

Credentials for authenticated HTTP remotes: an Ed25519 key pair stored
locally, whose public half a server or DoltHub authorizes.

## Synopsis

```sql
SELECT dolt_creds_new();
SELECT dolt_creds('list');
SELECT dolt_creds('export', '<credential-id>');                 -- public JWK
SELECT dolt_creds('export', '<credential-id>', '/srv/authorized-keys');
SELECT dolt_creds('rm', '<credential-id>');
```

## Behaviour

`dolt_creds_new()` creates a credential and returns one multiline message with
its id, public key, and DoltHub registration URL. `dolt_creds()` with no
arguments reports the state: `no credentials; run SELECT dolt_creds_new()`
until one exists.

| Action | Effect |
|---|---|
| `'list'` | Every stored credential id |
| `'export', id` | The public key as a JWK |
| `'export', id, dir` | Write the public JWK into `dir`, the form `doltlite-remotesrv --auth-keys` reads. The private seed never leaves the store. |
| `'rm', id` | Delete |

Unknown id: `no such credential`. Anything else prints the usage line.

| Setting | Default |
|---|---|
| `DOLTLITE_CREDS_DIR` | `~/.doltlite/creds` |
| `DOLTLITE_CREDS_KID` | the only credential, or the one named |

Remote calls pick up the credential automatically and sign a JWT per request;
the server rejects private credential files placed in `--auth-keys`.

## See also

[auth.md](auth.md) for the token format and server checks,
[remotesrv.md](remotesrv.md), [dolt_remote.md](dolt_remote.md).
