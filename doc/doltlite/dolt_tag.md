# dolt_tag and dolt_tags

Named, immutable pointers to commits. Dolt:
[dolt_tag](https://docs.dolthub.com/sql-reference/version-control/dolt-sql-procedures#dolt_tag),
[dolt_tags](https://docs.dolthub.com/sql-reference/version-control/dolt-system-tables#dolt_tags).

## Synopsis

```sql
SELECT dolt_tag('v2.1');                                   -- HEAD
SELECT dolt_tag('v2.2', 'HEAD~2');                          -- commit revision
SELECT dolt_tag('v3.0', 'main', '-m', 'release', '--author', 'Ann <ann@example.com>');
SELECT dolt_tag('-d', 'v2.1');
SELECT * FROM dolt_tags;
```

## Options

| Option | Meaning |
|---|---|
| `name [, rev]` | Create `name` at a commit revision (default `HEAD`) |
| `-m`, `--message` | Tag message |
| `--author 'Name <email>'` | Tagger; default is the connection's `dolt_config` identity |
| `-d`, `--delete name` | Delete |

Returns `0`. Tags are durable immediately, even inside `BEGIN`, and are pushed
with `dolt_push(remote, tag)` or `dolt_push(remote, '--tags')`. Name rules are
in [refs.md](refs.md).

| Error | Cause |
|---|---|
| `tag name required` | no arguments |
| `tag already exists` | duplicate name; delete first |
| `tag not found` | `-d` of an unknown tag |
| `invalid tag name` | name rule violation |

## dolt_tags

`tag_name`, `tag_hash`, `tagger`, `email`, `date`, `message`.

## See also

[dolt_branch.md](dolt_branch.md), [dolt_remote.md](dolt_remote.md),
`test/vc_oracle_tags_test.sh`, `test/vc_oracle_remote_tags_test.sh`.
