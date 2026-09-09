# Storage format

DoltLite does **not** use the SQLite page format. Primary databases are a
single content-addressed chunk-store file (magic `DLTC` / `0x444C5443`) with
prolly-tree chunks, a WAL of chunk/root records, and refs for branches and
tags. Stock SQLite files are still detected and opened for ordinary SQL (see
[Using Existing SQLite Databases](sqlite-files.md)); version
control requires a DoltLite-format file.

## Frozen format version 12 (beta)

Chunk-store version **12** is the on-disk format frozen for the DoltLite beta.
Version 12 includes every nested format written into the store, including:

| Layer | Constant | Value |
|---|---|---|
| Chunk-store header | `CHUNK_STORE_VERSION` | **12** |
| Working-set blob | `WS_FORMAT_VERSION` | **v5** |
| Catalog entries | `CATALOG_FORMAT_V5` | **0x46** |
| Refs blob | refs serializer | **v7** |
| Commit blob | `DOLTLITE_COMMIT_V2` | **v2** |

- **Writers** stamp version 12 and emit the nested formats above.
- **Readers** require an exact `CHUNK_STORE_VERSION` match. A different version
  returns `SQLITE_NOTADB`; there is no silent reinterpretation or automatic
  rewrite on open.
- Every file produced by a beta or later version-12 release remains readable
  and writable by later version-12 builds. An incompatible change to any nested
  format requires a `CHUNK_STORE_VERSION` bump even when that format has its own
  marker.
- Bumping `CHUNK_STORE_VERSION` requires updating this section, adding a corpus
  entry under [`test/format-corpus/`](../../test/format-corpus/), updating
  [`test/storage_format_contract.tsv`](../../test/storage_format_contract.tsv), and
  documenting whether version 12 is open-only, migrated, or refused.

The frozen version-12 file and its generation recipe live in
[`test/format-corpus/v12/`](../../test/format-corpus/v12/). The machine-readable
contract is
[`test/storage_format_contract.tsv`](../../test/storage_format_contract.tsv); CI runs
[`test/storage_format_contract_test.sh`](../../test/storage_format_contract_test.sh)
to verify the fixture, read and extend it, run GC, reject other header versions,
and keep evidence needles live.
