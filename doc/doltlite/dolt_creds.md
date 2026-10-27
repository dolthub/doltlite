# Credential management (`dolt_creds_*`)

Manage local Ed25519 keys for authenticated HTTPS remotes. A server or
DoltHub account must authorize a key's public half before it can authenticate.
Creating or importing a key does not register it with a server.

## Commands

| Command | Effect / result |
|---|---|
| `dolt_creds_new()` | Generate and store a key; return its ID, public key, and DoltHub registration URL. |
| `dolt_creds_ls()` | Table-valued function listing local keys and the active selection. |
| `dolt_creds_use(key)` | Persist the default key; return `0`. |
| `dolt_creds_use(key, 'session')` | Select a key for this SQLite connection only; return `0`. |
| `dolt_creds_bind(remote_or_https_url, key)` | Persist a credential binding for a remote URL; return `0`. |
| `dolt_creds_import(filename)` | Import a private JWK file, accepting Dolt and DoltLite layouts; return `0`. |
| `dolt_creds_export(key [, directory])` | Return a public JWK, or write it into an authorized-keys directory and return a confirmation. |
| `dolt_creds_rm(key [, key ...])` | Delete local keys; return `0`. |

`key` accepts either a credential ID (`kid`) or its base32 public key.
`dolt_creds_use` and `dolt_creds_bind` also accept `'anonymous'`.

The first generated or imported key becomes the persisted default when no
selection is configured. Later keys preserve that selection, including a
selection whose key file has been deleted.

## Create, list, and select

```sql
SELECT dolt_creds_new();
SELECT kid, public_key, active, error FROM dolt_creds_ls() ORDER BY kid;
SELECT kid FROM dolt_creds_ls() WHERE active = true;
SELECT dolt_creds_use('<credential-id>');
SELECT dolt_creds_use('<credential-id>', 'session');
```

`dolt_creds_ls()` returns these columns:

| Column | Meaning |
|---|---|
| `kid` | Credential ID. |
| `public_key` | Base32 public key, or `NULL` if the credential cannot be loaded. |
| `active` | Boolean (`1` / `0`) identifying the current connection's selected key, without applying a remote URL binding. |
| `error` | `NULL` for a valid key, or a credential-loading error. |

Listing reads local state. An anonymous selection or an unavailable selected
key leaves every row's `active` value false.

## Selection and remote bindings

HTTPS operations select credentials in this order:

1. Connection override from `dolt_creds_use(key, 'session')`.
2. `DOLTLITE_CREDS_KID` environment override.
3. Binding for the remote URL.
4. Persisted default from `dolt_creds_use(key)`.
5. The sole local key, for compatibility with an existing single-key store.

The selected identity stays fixed throughout a remote operation. Connection
overrides last until replaced or the connection closes; they do not change
the persisted default or another connection's selection. Changing the
persisted default does not replace a connection override.

Bind a URL to use a different account for that repository:

```sql
SELECT dolt_creds_bind('https://doltliteremoteapi.dolthub.com/owner/repo', '<credential-id>');
```

An existing HTTPS remote name also works, for example
`SELECT dolt_creds_bind('origin', '<credential-id>');`. The binding follows its
URL, including the repository path, rather than the remote's name. URL matching
normalizes host case, the default HTTPS port, and trailing slashes. Bindings
are machine-local and apply to other databases using the same URL.

Multiple keys without a selection, or a missing, unreadable, or invalid
selected key, cause a local error before an authenticated request is sent.
Choose a valid key with `dolt_creds_use`.

Public reads can proceed without a credential when none is available. For a
server that permits anonymous HTTPS writes, explicitly select
`dolt_creds_use('anonymous')`, use the `'session'` scope, or bind that URL to
`'anonymous'`. Plain HTTP never sends credentials.

## Import, export, and remove

Replace `<credential-file>` with the path to a private JWK file. Import
preserves its key identity and validates agreement between its public and
private halves. The optional Dolt-compatible form is
`dolt_creds_import('--no-profile', filename)`; imports only change the local
credential store and selection, without updating the commit author.

```sql
SELECT dolt_creds_import('<credential-file>');
SELECT dolt_creds_export('<credential-id>');
SELECT dolt_creds_export('<credential-id>', '/srv/authorized-keys');
SELECT dolt_creds_rm('<credential-id>');
```

Export always contains the public key only. A directory export creates
`<kid>.jwk` for `doltlite-remotesrv --auth-keys` and refuses to overwrite an
existing file. The server rejects private JWK files in that directory.

Removing a key does not select a replacement or clear references to it. Use
`dolt_creds_use` and update affected bindings before the next operation that
would select the removed key. `dolt_creds_rm` checks every name before it
deletes any key. An unknown name returns an error and leaves every named key
in place. A repeated name in one call is removed once.

## Storage and compatibility

Keys live in `~/.doltlite/creds/<kid>.jwk`; `DOLTLITE_CREDS_DIR` overrides that
directory. The `config` file there stores the default and URL bindings.
These settings are outside the versioned database and are neither cloned nor
pushed. See [auth.md](auth.md) for TLS and JWT settings.

The existing `dolt_creds()` / `dolt_creds('list')` text listing remains
available and marks the active key with `*`. Existing `'rm'` / `'remove'` and
`'export'` actions remain supported; `'use'`, `'bind'`, and `'import'`
dispatch to the corresponding named functions.

## See also

[auth.md](auth.md), [remotesrv.md](remotesrv.md), [dolt_remote.md](dolt_remote.md).
