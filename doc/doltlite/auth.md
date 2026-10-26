# DoltLite remote authentication

DoltLite's HTTP sync supports authenticated HTTPS remotes, including hosted
DoltHub and DoltLab. It uses Dolt's Ed25519 identity and JWT contract, supports
importing Dolt credentials, and uses the same DoltHub credentials page.

## Dolt authentication contract

DoltHub authenticates each remote request with a short-lived bearer JWT signed
by the user's Ed25519 private key. The matching public key is registered to the
account. The server (`ld`, `go/libraries/remoteauth`) verifies:

- **Key:** Ed25519. `kid = base32(sha512_224(pubkey))` using alphabet
  `0123456789abcdefghijklmnopqrstuv` with no padding. SHA-512/224 uses its own
  FIPS-180-4 initialization vectors; it is not truncated SHA-512.
- **JWT header:** `{"alg":"EdDSA","kid":"<kid>","dolt_token_version":"2023.01"}`
- **JWT claims:** `iss:"dolt-client.dolthub.com"`,
  `sub:"doltClientCredentials/<kid>"`, `aud:["<remote-api audience>"]`,
  `iat:<now>`, and `exp:<now+30s>`.
- **Transport:** `Authorization: Bearer <jwt>`, regenerated for each request.

The audience normally equals the remote host. A
`doltliteremoteapi.<suffix>` host uses `doltremoteapi.<suffix>` to match
DoltHub's existing audience. `DOLT_OVERRIDE_GRPC_JWT_AUDIENCE` overrides both.

DoltLite **mints** those claims, including `iat` and `exp`. Clients attach
the JWT only on HTTPS remotes; plain `http://` requests never send
`Authorization`.

## What `doltlite-remotesrv` verifies

`--auth-keys` requires `--audience`. Verification accepts either a string
`aud` or a Dolt-shaped one-element (or multi-element) array and rejects a
missing audience rather than treating it as "any".

The remotesrv verifier checks:

- header `alg` is `EdDSA` and `dolt_token_version` is `2023.01`
- `kid` is present and the matching public JWK is in `--auth-keys`
- Ed25519 signature over the JWT signing input
- `iss`, `sub`, and `aud` as above
- `exp` is present and the token is within a 60-second clock-skew window
- optional `iat` and `nbf` claims are valid integers and no more than 60 seconds
  ahead of the server clock
- `exp` is not before `iat` or `nbf`, and an `iat`-bearing token has at most the
  30-second lifetime DoltLite mints
- a token without `iat` has no more than 90 seconds until `exp`, preserving
  compatibility with Dolt credentials while rejecting far-future expirations

The `Authorization` scheme must be the six characters `Bearer ` (capital
B, one space). `bearer` is rejected. Extra spaces after `Bearer ` are
allowed.

## Credential interface

Credentials use a separate `~/.doltlite/creds/` store rather than
`~/.dolt/creds`. Each key is a mode-0600 JWK file named `<kid>.jwk`:

```json
{"kty":"OKP","crv":"Ed25519","x":"<base64url-public-key>","d":"<base64url-seed>"}
```

DoltLite exposes credential management through SQL because it is an embeddable
SQLite library and shell, not a subcommand-oriented CLI:

```sql
SELECT dolt_creds_new();
SELECT kid, public_key, active, error FROM dolt_creds_ls() ORDER BY kid;
SELECT dolt_creds_use('<kid>');
```

`dolt_creds_new()` stores a key and returns its ID, public key, and DoltHub
registration URL. The first generated or imported key becomes the default
when no selection is configured; creating another preserves that selection.
Register the public key on the account, then select it with `dolt_creds_use`.
`active` is a Boolean column (`1` / `0`) for the connection's selected key,
without a remote URL binding.

HTTPS operations (`dolt_push`, `dolt_fetch`, `dolt_pull`, `dolt_clone`, and lazy
reads) use the connection override, environment override, remote URL binding,
persisted default, or sole local key, in that order. They keep the selected
identity for the operation and sign a new JWT for each request. Ambiguous or
invalid selections fail locally. Public reads can use no credential; anonymous
HTTPS writes require an explicit `'anonymous'` selection.

Use `dolt_creds_use(key, 'session')` for a connection-specific identity and
`dolt_creds_bind(remote_or_https_url, key)` for a repository on a different
account. Defaults and bindings live in the credential directory's `config`
file, outside database history. They are not cloned or pushed.

The [credential command reference](dolt_creds.md) covers listing, selection,
bindings, import, public export, removal, and compatibility commands.

The following environment variables customize credential and TLS behavior:

| Variable | Purpose |
|---|---|
| `DOLTLITE_CREDS_DIR` | Override the credential directory. |
| `DOLTLITE_CREDS_KID` | Override URL bindings and the default key; a connection override takes precedence. |
| `DOLTLITE_LOGIN_URL` | Override the URL returned by `dolt_creds_new()`. |
| `DOLT_OVERRIDE_GRPC_JWT_AUDIENCE` | Override the JWT audience. |
| `DOLTLITE_CA_FILE` | Use a specific CA bundle, including a private CA. |

Without `DOLTLITE_CA_FILE`, HTTPS uses the operating system trust store and
verifies the server certificate and hostname.

## Implementation

| Piece | Location |
|---|---|
| Ed25519 key generation, signing, and verification | `ext/ed25519/` |
| SHA-512/224, encodings, JWK storage, JWT signing and verification | `src/doltlite_creds.c` |
| Public credential API | `src/doltlite_creds.h` |
| TLS client and server over vendored mbedTLS | `ext/mbedtls/`, `src/doltlite_tls.c` |
| HTTPS requests and per-request bearer tokens | `src/doltlite_http_remote.c` |
| SQL credential functions and remote operations | `src/doltlite_remote_sql.c` |
| Server-side bearer verification | `src/doltlite_remotesrv.c` |

The credential API supports key generation, deterministic construction from a
seed, key import, save/load/list/remove operations, persistent selection and
URL bindings, JWT creation, and bearer verification.
Heap results use SQLite's allocator (`sqlite3_malloc`); free them with
`sqlite3_free` (or `doltliteCredsFree` / `doltliteCredsFreeList`). Remote
credential selection is internal to the HTTPS transport; embedders use the
same remote operations rather than attaching credentials through a separate
remote-setter API.

The TLS implementation uses vendored **mbedTLS** for client and server
connections, certificate-chain validation, hostname verification, system trust
store loading, and private-CA overrides. Ed25519 remains a separate vendored
dependency so JWT compatibility does not depend on TLS-library Ed25519 support.

## Tests

- `test/creds_kat_test.sh` covers key derivation, encoding, deterministic JWTs,
  signing, JWK persistence, listing, removal, and path validation.
- `test/creds_verify_test.sh` covers bearer verification, claims, signatures,
  authorized-key lookup, and rejection paths.
- `test/tls_test.sh` covers TLS connection and certificate validation against
  the system trust store and an untrusted CA.
- `test/vc_oracle_creds_test.sh` compares credential lifecycle and selection
  with Dolt's CLI.
- `test/doltlite_creds.test` and `test/creds_connection_test.c` cover SQL
  selection and connection isolation.
- `test/remote_https_auth_test.sh` exercises authenticated HTTPS push and clone,
  85-key selection, override precedence, concurrent URL bindings, anonymous
  access, lazy reads, incorrect CAs, multi-chunk transfer, and
  Authorization-header parsing.
- `test/creds_path_test.sh` covers the SQL credential lifecycle and configured
  credential directory.
