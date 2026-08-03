# DoltLite remote authentication

DoltLite's HTTP sync supports authenticated HTTPS remotes, including hosted
DoltHub and DoltLab. It is wire-compatible with `dolt login` credentials: the
same Ed25519 key format, JWT contract, and DoltHub credentials page.

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
SELECT dolt_creds();
SELECT dolt_creds('list');
SELECT dolt_creds('rm', '<kid>');
```

`dolt_creds_new()` stores a key and returns its ID, public key, and DoltHub
approval URL. After the user approves that key, `dolt_push`, `dolt_fetch`,
`dolt_pull`, and `dolt_clone` authenticate automatically. HTTPS remotes load
the selected credential and generate a new JWT for each request.

The following environment variables customize credential and TLS behavior:

| Variable | Purpose |
|---|---|
| `DOLTLITE_CREDS_DIR` | Override the credential directory. |
| `DOLTLITE_CREDS_KID` | Select a credential instead of the default key. |
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
seed, save/load/list/remove operations, JWT creation, and bearer verification.
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
- `test/remote_https_auth_test.sh` exercises authenticated HTTPS push and clone,
  missing and unauthorized credentials, incorrect CAs, plaintext compatibility,
  multi-chunk transfer, and Authorization-header parsing.
- `test/creds_path_test.sh` covers the SQL credential lifecycle and configured
  credential directory.
