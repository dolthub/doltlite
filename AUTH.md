# doltlite remote authentication — design

Adds DoltHub-compatible authentication to doltlite's HTTP sync so pushes/pulls
against hosted DoltHub (and DoltLab) can authenticate as a user. Wire-compatible
with `dolt login` credentials: the same ed25519 key format, the same JWT, and the
same DoltHub credentials page.

## How dolt auth works (the contract we must match)

DoltHub authenticates every remote request with a short-lived **bearer JWT** signed
by the user's ed25519 private key; the matching public key is registered to the
account. The server (`ld`, `go/libraries/remoteauth`) verifies:

- **Key:** ed25519. `kid = base32(sha512_224(pubkey))` using alphabet
  `0123456789abcdefghijklmnopqrstuv` (no padding, 45 chars). **SHA-512/224 is a
  distinct variant** (its own FIPS-180-4 IVs — *not* a truncation of SHA-512); the
  DoltHub site re-derives the same `kid` from the approved public key, so we must
  compute it identically.
- **JWT header:** `{"alg":"EdDSA","kid":"<kid>","dolt_token_version":"2023.01"}`
- **JWT claims:** `iss:"dolt-client.dolthub.com"`, `sub:"doltClientCredentials/<kid>"`,
  `aud:["<remote-api audience>"]`, `iat:<now>`, `exp:<now+30s>`
- **Transport:** `Authorization: Bearer <jwt>`, regenerated per request (30s expiry).

`aud` must equal the server's expected audience (the remote host, e.g.
`doltremoteapi.dolthub.com`; dev `doltremoteapi.awsdev.ld-corp.com`), overridable —
mirrors dolt's `DOLT_OVERRIDE_GRPC_JWT_AUDIENCE`.

## Decisions

1. **TLS:** vendor a small TLS library (see "Library choices"). A bearer JWT over
   plaintext leaks the token, and hosted DoltHub is HTTPS-only, so `https://` support
   is required, not optional.
2. **Credential store:** a separate `~/.doltlite/creds/` directory (not shared with
   `~/.dolt/creds`). One JWK file per key: `~/.doltlite/creds/<kid>.jwk`, mode 0600,
   dolt-compatible JWK: `{"kty":"OKP","crv":"Ed25519","x":<b64url pub>,"d":<b64url priv>}`.
3. **Interface:** SQL functions (matching `dolt_push`/`dolt_remote`), backed by a C
   credential API. doltlite is the SQLite shell — it has no subcommand CLI — so there
   is deliberately **no** `doltlite login` argv command.
4. **Login flow:** manual key-add, no polling. `dolt_creds_new()` prints the public key
   and an approval URL; the user adds it on DoltHub; the next push just works. No
   `WhoAmI`/gRPC and **no `ld`-side change** — this is entirely doltlite-repo work.

## Interface

SQL functions (registered in `src/doltlite_remote_sql.c`):

- `dolt_creds_new()` — generate an ed25519 keypair, store it, and return a result
  with the public key and the approval URL:
  `https://dolthub.com/settings/credentials#<base32(pubkey)>` (the page reads the key
  from the `#fragment`). The user copies/opens it and adds the key on DoltHub.
- `dolt_creds()` / `dolt_creds('list')` — list stored keys and their `kid`.
- `dolt_creds('rm', '<kid>')` — delete a key. (optional first cut)
- `dolt_config('remotes.default_host' | 'creds.login_url' | 'remotes.audience', …)` —
  reuse the existing `dolt_config` function for endpoint/audience settings; also honor
  env overrides (`DOLTLITE_CREDS_DIR`, `DOLT_OVERRIDE_GRPC_JWT_AUDIENCE`).

`dolt_push`/`dolt_fetch`/`dolt_clone` gain auth automatically: they resolve the
credential for the remote's host and sign a bearer JWT per request. If no key is
approved yet, the request returns 401/permission-denied — clear feedback, no polling.

C API (new `src/doltlite_creds.h`), used by the SQL layer and by embedders
(doltlite-node, etc.) that supply credentials without a shell:

```c
typedef struct DoltliteCreds DoltliteCreds;
int   doltliteCredsGenerate(DoltliteCreds **out);                       // new ed25519 key
int   doltliteCredsLoad(const char *dir, const char *kid, DoltliteCreds **out);
int   doltliteCredsLoadDefault(const char *dir, DoltliteCreds **out);   // single/most-recent key
int   doltliteCredsSave(const DoltliteCreds *, const char *dir);        // writes <kid>.jwk 0600
const char *doltliteCredsKid(const DoltliteCreds *);                    // base32(sha512_224(pub))
char *doltliteCredsPubKeyB32(const DoltliteCreds *);                    // for the approval URL
int   doltliteCredsBearerToken(const DoltliteCreds *, const char *audience, char **jwtOut);
void  doltliteCredsFree(DoltliteCreds *);

// Attach auth to a remote (per-request signing; token regenerated at 30s expiry):
void  doltliteRemoteSetCreds(DoltliteRemote *, const DoltliteCreds *, const char *audience);
// Or supply a raw bearer token directly (embedders that mint their own):
void  doltliteRemoteSetBearerToken(DoltliteRemote *, const char *token);
```

## Components / where they live

| Piece | Location | Notes |
|---|---|---|
| ed25519 (keygen/sign) + SHA-512 | `ext/ed25519/` (vendored) | Mirrors `ext/blake3/`. |
| SHA-512/224 (for `kid`) | `src/doltlite_creds.c` | SHA-512 core + FIPS-180-4 /224 IVs, 28-byte output. |
| base64url + base32(dolt alphabet) | `src/doltlite_creds.c` | Small, self-contained. |
| JWK read/write, creds store | `src/doltlite_creds.c/.h` | `~/.doltlite/creds/<kid>.jwk`, 0600. |
| JWT assembler (EdDSA) | `src/doltlite_creds.c` | Exact header/claims above; tiny JSON writer. |
| TLS client (`https://`) | `ext/<tls>/` + `src/doltlite_http_remote.c` | New; replaces raw socket when scheme is https. |
| `Authorization: Bearer` header | `src/doltlite_http_remote.c` | Extend `httpRequest()` + `HttpRemote`; sign per request. |
| Credential threading | `src/doltlite_remote_sql.c`, `doltliteHttpRemoteOpen` | Resolve creds by remote host; attach to remote. |
| SQL functions | `src/doltlite_remote_sql.c` | `dolt_creds_new` / `dolt_creds`, registered in `doltliteRemoteSqlRegister`. |

## Library choices (last low-level decisions)

- **ed25519:** vendor a compact public-domain implementation (e.g. `orlp/ed25519`,
  which bundles its own SHA-512) into `ext/ed25519/`, matching the `ext/blake3` pattern.
  Do **not** rely on the TLS lib for Ed25519 (support is inconsistent).
- **TLS:** **BearSSL** (recommended) — small, MIT, allocation-free, built for
  embedding; ship a CA-bundle converted to BearSSL trust anchors for chain validation.
  Alternative: **mbedTLS** — larger but easier X.509/CA handling if BearSSL cert wiring
  proves fiddly.

## Staged implementation (each step independently testable)

1. **Crypto + creds foundation** (no networking): vendor ed25519; implement
   SHA-512/224, base64url, base32; `DoltliteCreds` generate/save/load; `kid` and
   pubkey-b32 derivation; JWK read/write. **Tests:** KAT for `kid` derivation and JWT
   header/claims; ed25519 sign→verify round-trip; cross-check a `kid` against dolt's
   algorithm for a fixed pubkey.
2. **JWT signer:** `doltliteCredsBearerToken` producing the exact EdDSA token.
   **Test:** verify a doltlite-signed token with the ed25519 public key (and, ideally,
   cross-verify in Go with go-jose to prove wire-compat with `ld`).
3. **TLS transport:** vendor the TLS lib; add `https://` to the HTTP client with cert
   validation. **Test:** GET against a known HTTPS host.
4. **Auth threading:** `Authorization: Bearer` per request; resolve creds by host in
   `doltliteHttpRemoteOpen` and push/fetch/clone.
5. **SQL functions:** `dolt_creds_new` / `dolt_creds`; wire config/audience.
6. **End-to-end:** push/pull a doltlite db to dev DoltHub with an approved key.

## Testing note

doltlite already has a KAT harness pattern (`test/blake3_kat_test.sh`). Add an analogous
`test/creds_kat_test.sh` for `kid` derivation and JWT structure. The strongest
compatibility check is cross-verifying a doltlite-signed JWT in Go against `ld`'s
`remoteauth` expectations (ed25519 pubkey, issuer/audience/subject, `dolt_token_version`
header) — this can live in the `doltlite-go` module.
