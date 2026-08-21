#!/usr/bin/env python3
"""Chunk size distribution in a DoltLite chunk-store file. Layout: src/chunk_store.h."""
import argparse
import json
import struct
import sys
from statistics import mean, median, stdev

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
    n_chunks = struct.unpack_from("<I", buf, 28)[0]
    i_index_offset = struct.unpack_from("<q", buf, 32)[0]
    n_index_size = struct.unpack_from("<I", buf, 40)[0]
    i_wal_offset = struct.unpack_from("<q", buf, 84)[0]
    return {
        "version": version,
        "n_chunks": n_chunks,
        "index_offset": i_index_offset,
        "index_size": n_index_size,
        "wal_offset": i_wal_offset,
    }


def read_index(f, manifest):
    if manifest["index_size"] == 0:
        return []
    f.seek(manifest["index_offset"])
    buf = f.read(manifest["index_size"])
    sizes = []
    for off in range(0, len(buf), INDEX_ENTRY_SIZE):
        size = struct.unpack_from("<I", buf, off + HASH_SIZE + 8)[0]
        sizes.append(size)
    return sizes


def read_wal_chunks(f, manifest):
    if manifest["wal_offset"] == 0:
        return []
    f.seek(0, 2)
    eof = f.tell()
    f.seek(manifest["wal_offset"])
    sizes = []
    while f.tell() < eof:
        tag = f.read(1)
        if not tag:
            break
        if tag[0] == WAL_TAG_CHUNK:
            f.read(HASH_SIZE)
            length_buf = f.read(4)
            if len(length_buf) < 4:
                break
            n = struct.unpack("<I", length_buf)[0]
            sizes.append(n)
            f.seek(n, 1)
        elif tag[0] == WAL_TAG_ROOT:
            f.seek(MANIFEST_SIZE, 1)
        else:
            break
    return sizes


def percentile(sorted_xs, p):
    if not sorted_xs:
        return 0
    k = (len(sorted_xs) - 1) * p / 100.0
    f_idx = int(k)
    c_idx = min(f_idx + 1, len(sorted_xs) - 1)
    return sorted_xs[f_idx] + (sorted_xs[c_idx] - sorted_xs[f_idx]) * (k - f_idx)


def histogram(sizes, n_buckets=20):
    if not sizes:
        return []
    lo, hi = min(sizes), max(sizes)
    if lo == hi:
        return [(lo, hi, len(sizes))]
    width = (hi - lo) / n_buckets
    counts = [0] * n_buckets
    for s in sizes:
        i = min(int((s - lo) / width), n_buckets - 1)
        counts[i] += 1
    return [(int(lo + i * width), int(lo + (i + 1) * width), counts[i])
            for i in range(n_buckets)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dbfile")
    ap.add_argument("--min", type=int, default=512,
                    help="prolly chunk MIN clamp (default 512)")
    ap.add_argument("--max", type=int, default=16384,
                    help="prolly chunk MAX clamp (default 16384)")
    ap.add_argument("--target", type=int, default=4096,
                    help="prolly chunk target / Weibull L (default 4096)")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    with open(args.dbfile, "rb") as f:
        manifest = read_manifest(f)
        index_sizes = read_index(f, manifest)
        wal_sizes = read_wal_chunks(f, manifest)

    all_sizes = index_sizes + wal_sizes
    if not all_sizes:
        raise SystemExit("no chunks found")

    # Prolly chunks are >= MIN; smaller ones are admin objects.
    prolly_sizes = [s for s in all_sizes if s >= args.min]
    admin_sizes = [s for s in all_sizes if s < args.min]
    sorted_p = sorted(prolly_sizes)

    stats = {
        "total_chunks": len(all_sizes),
        "index_chunks": len(index_sizes),
        "wal_chunks": len(wal_sizes),
        "admin_chunks": len(admin_sizes),
        "prolly_chunks": len(prolly_sizes),
        "min_observed": min(all_sizes),
        "max_observed": max(all_sizes),
    }
    if prolly_sizes:
        stats.update({
            "prolly_mean": mean(prolly_sizes),
            "prolly_median": median(prolly_sizes),
            "prolly_stdev": stdev(prolly_sizes) if len(prolly_sizes) > 1 else 0.0,
            "prolly_min": min(prolly_sizes),
            "prolly_max": max(prolly_sizes),
            "prolly_p10": percentile(sorted_p, 10),
            "prolly_p50": percentile(sorted_p, 50),
            "prolly_p90": percentile(sorted_p, 90),
            "prolly_p99": percentile(sorted_p, 99),
        })

    if args.json:
        print(json.dumps(stats, indent=2))
        return

    print(f"DoltLite chunk store: {args.dbfile}")
    print(f"  manifest version:   {manifest['version']}")
    print(f"  total chunks:       {stats['total_chunks']}"
          f"  (index={stats['index_chunks']}, wal={stats['wal_chunks']})")
    print(f"  admin chunks (<{args.min}B): {stats['admin_chunks']}")
    print(f"  prolly chunks (>={args.min}B): {stats['prolly_chunks']}")
    if not prolly_sizes:
        return

    print()
    print("Prolly chunk size distribution:")
    print(f"  mean:    {stats['prolly_mean']:8.1f} B"
          f"   (target {args.target}, "
          f"theoretical Weibull mean ~3713)")
    print(f"  median:  {stats['prolly_median']:8.1f} B")
    print(f"  stdev:   {stats['prolly_stdev']:8.1f} B"
          f"   (Weibull theoretical ~1041)")
    print(f"  min:     {stats['prolly_min']:8d} B   (clamp {args.min})")
    print(f"  max:     {stats['prolly_max']:8d} B   (clamp {args.max})")
    print(f"  p10/p50/p90/p99: {stats['prolly_p10']:.0f} / "
          f"{stats['prolly_p50']:.0f} / "
          f"{stats['prolly_p90']:.0f} / "
          f"{stats['prolly_p99']:.0f}")

    print()
    print("Histogram (prolly chunks):")
    hist = histogram(prolly_sizes, n_buckets=20)
    max_count = max(h[2] for h in hist) or 1
    for lo, hi, cnt in hist:
        bar = "#" * int(40 * cnt / max_count)
        print(f"  [{lo:5d}, {hi:5d}) {cnt:4d} {bar}")


if __name__ == "__main__":
    main()
