#!/usr/bin/env python3
"""Report content hashes stored more than once in a DoltLite chunk-store file."""
import argparse
import json
import struct
import sys
from collections import defaultdict

MANIFEST_SIZE = 168
INDEX_ENTRY_SIZE = 32
HASH_SIZE = 20
WAL_TAG_CHUNK = 0x01
WAL_TAG_ROOT = 0x02


def read_manifest(f):
    f.seek(0)
    buf = f.read(MANIFEST_SIZE)
    if len(buf) < MANIFEST_SIZE:
        raise SystemExit("file too short for manifest")
    magic = struct.unpack_from("<I", buf, 0)[0]
    version = struct.unpack_from("<I", buf, 4)[0]
    if magic != 0x444C5443:
        raise SystemExit(f"bad magic 0x{magic:08x} (not a DoltLite chunk store)")
    return {
        "version": version,
        "n_chunks": struct.unpack_from("<I", buf, 28)[0],
        "index_offset": struct.unpack_from("<q", buf, 32)[0],
        "index_size": struct.unpack_from("<I", buf, 40)[0],
        "wal_offset": struct.unpack_from("<q", buf, 84)[0],
    }


def scan_index(f, manifest):
    records = []
    index_size = manifest["index_size"]
    if index_size == 0:
        return records
    if index_size % INDEX_ENTRY_SIZE != 0:
        raise SystemExit("index size is not a multiple of index entry size")

    f.seek(manifest["index_offset"])
    buf = f.read(index_size)
    if len(buf) != index_size:
        raise SystemExit("short read while reading index")

    for off in range(0, len(buf), INDEX_ENTRY_SIZE):
        h = buf[off:off + HASH_SIZE].hex()
        file_offset = struct.unpack_from("<q", buf, off + HASH_SIZE)[0]
        size = struct.unpack_from("<I", buf, off + HASH_SIZE + 8)[0]
        records.append({
            "hash": h,
            "source": "index",
            "record_offset": file_offset,
            "data_size": size,
        })
    return records


def scan_wal(f, manifest):
    records = []
    wal_offset = manifest["wal_offset"]
    if wal_offset == 0:
        return records

    f.seek(0, 2)
    eof = f.tell()
    pos = wal_offset
    f.seek(pos)

    while pos < eof:
        tag = f.read(1)
        if not tag:
            break
        pos += 1

        if tag[0] == WAL_TAG_CHUNK:
            h = f.read(HASH_SIZE)
            length_buf = f.read(4)
            if len(h) != HASH_SIZE or len(length_buf) != 4:
                break
            n = struct.unpack("<I", length_buf)[0]
            records.append({
                "hash": h.hex(),
                "source": "wal",
                "record_offset": pos - 1,
                "data_size": n,
            })
            f.seek(n, 1)
            pos += HASH_SIZE + 4 + n
        elif tag[0] == WAL_TAG_ROOT:
            f.seek(MANIFEST_SIZE, 1)
            pos += MANIFEST_SIZE
        else:
            break

    return records


def summarize(records, manifest):
    by_hash = defaultdict(list)
    for rec in records:
        by_hash[rec["hash"]].append(rec)

    duplicates = {
        h: recs for h, recs in by_hash.items()
        if len(recs) > 1
    }
    duplicate_records = sum(len(recs) - 1 for recs in duplicates.values())
    max_records_per_hash = max((len(recs) for recs in by_hash.values()), default=0)

    return {
        "manifest_version": manifest["version"],
        "manifest_index_chunks": manifest["n_chunks"],
        "physical_records": len(records),
        "distinct_hashes": len(by_hash),
        "duplicate_hashes": len(duplicates),
        "duplicate_records": duplicate_records,
        "max_records_per_hash": max_records_per_hash,
        "index_records": sum(1 for rec in records if rec["source"] == "index"),
        "wal_records": sum(1 for rec in records if rec["source"] == "wal"),
        "duplicates": {
            h: recs for h, recs in sorted(duplicates.items())
        },
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dbfile")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--fail-on-dups", action="store_true")
    args = ap.parse_args()

    with open(args.dbfile, "rb") as f:
        manifest = read_manifest(f)
        records = scan_index(f, manifest) + scan_wal(f, manifest)

    stats = summarize(records, manifest)
    if args.json:
        print(json.dumps(stats, indent=2))
    else:
        print(f"DoltLite chunk store: {args.dbfile}")
        print(f"  physical records:     {stats['physical_records']}")
        print(f"  distinct hashes:       {stats['distinct_hashes']}")
        print(f"  duplicate hashes:      {stats['duplicate_hashes']}")
        print(f"  duplicate records:     {stats['duplicate_records']}")
        print(f"  max records per hash:  {stats['max_records_per_hash']}")
        print(f"  index/wal records:     {stats['index_records']} / {stats['wal_records']}")

    if args.fail_on_dups and stats["duplicate_hashes"] != 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
