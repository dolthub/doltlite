#!/usr/bin/env python3
"""Pull real v12 blobs out of test/format-corpus/v12/seed.db into test/fuzz-corpus.

The tracked seeds stay. This adds files the decoders accept: working sets,
commits, catalogs, refs, prolly nodes, a WAL prefix, a flat index, and a
database prefix. Re-run after the format corpus is regenerated.
"""

import pathlib
import struct
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
SEED = ROOT / "test" / "format-corpus" / "v12" / "seed.db"
CORPUS = ROOT / "test" / "fuzz-corpus"

HASH = 20
WAL_HDR = 1 + HASH + 4
MANIFEST = 168
NODE_MAGIC = bytes.fromhex("444f4e50")
WS_LEN = {2: 102, 3: 207, 4: 227, 5: 311}


def u32(buf, off):
    return struct.unpack_from("<I", buf, off)[0]


def u64(buf, off):
    return struct.unpack_from("<q", buf, off)[0]


def put_u32(buf, off, val):
    struct.pack_into("<I", buf, off, val)


def put_i64(buf, off, val):
    struct.pack_into("<q", buf, off, val)


def walk(data):
    pos = MANIFEST
    while pos + WAL_HDR <= len(data):
        tag = data[pos]
        if tag == 1:
            ln = u32(data, pos + 1 + HASH)
            body = pos + WAL_HDR
            if ln > len(data) - body:
                break
            yield ("chunk", pos, body, ln, data[pos + 1 : pos + 1 + HASH])
            pos = body + ln
        elif tag == 2:
            end = pos + 1 + MANIFEST
            if end > len(data):
                break
            yield ("root", pos, end, 0, b"")
            pos = end
        else:
            break


def write(rel, blob):
    path = CORPUS / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(blob)
    print(f"  {rel} ({len(blob)} bytes)")


def main():
    data = SEED.read_bytes()
    if u32(data, 0) != 0x444C5443 or u32(data, 4) != 12:
        sys.exit("seed.db is not a v12 chunk store")

    nodes = {0: None, 1: None, 2: None}
    catalogs = []
    commits = []
    refs = []
    working = []
    conflicts = []
    violations = []
    chunks_for_index = []
    root_ends = []
    internal_at = None

    for kind, pos, body, ln, digest in walk(data):
        if kind == "root":
            root_ends.append(body)
            continue
        blob = data[body : body + ln]
        if ln >= 8 and blob[:4] == NODE_MAGIC:
            level = blob[4]
            cur = nodes.get(level)
            if cur is None or ln < len(cur):
                nodes[level] = blob
            if level > 0 and internal_at is None:
                internal_at = body + ln
        elif ln in WS_LEN.values() and blob and blob[0] in WS_LEN and WS_LEN[blob[0]] == ln:
            if blob not in working and len(working) < 2:
                working.append(blob)
        elif blob[:1] == b"\x46":
            if not catalogs or ln < len(catalogs[0]):
                catalogs = [blob]
        elif blob[:1] == b"\x07":
            if not refs or ln < len(refs[0]):
                refs = [blob]
        elif blob[:4] == b"DLC\x01":
            if not conflicts or ln < len(conflicts[0]):
                conflicts = [blob]
        elif blob[:4] == b"DCV\x01":
            if not violations or ln < len(violations[0]):
                violations = [blob]
        elif (blob[:1] == b"\x02" and ln not in WS_LEN.values()
              and ln > 8 and blob[1] <= 16):
            commits.append(blob)
        if len(chunks_for_index) < 32 and pos < 256 * 1024:
            chunks_for_index.append((digest, pos + WAL_HDR - 4, ln))

    print("writing seeds from", SEED.name)
    for level, blob in sorted(nodes.items()):
        if blob is not None:
            write(f"prolly_node/v12_level{level}", blob)
    if catalogs:
        write("deserialize_catalog/v12", catalogs[0])
    if refs:
        write("deserialize_refs/v12", refs[0])
    if working:
        for i, blob in enumerate(working):
            write(f"working_set/v12_{i}", blob)
    if commits:
        commits.sort(key=len)
        write("commit/v12", commits[0])
    if conflicts:
        write("conflicts/v12", conflicts[0])
    if violations:
        write("constraint_violations/v12", violations[0])

    cut = root_ends[0] if root_ends else MANIFEST
    for end in root_ends:
        if end > 64 * 1024:
            break
        cut = end
        if internal_at is not None and end >= internal_at:
            break
    # The replay harness places these bytes at file offset 1, so the seed is
    # the WAL only. Chunk hashes do not depend on that offset.
    write("replaywal/v12_prefix", data[MANIFEST:cut])
    write("database/v12_prefix", data[:cut])

    entries = []
    index_at = cut
    for digest, offset, ln in chunks_for_index:
        if offset < MANIFEST or offset + 4 + ln > index_at:
            continue
        entries.append((digest, offset, ln))
    entries.sort()
    uniq = []
    prev = None
    for digest, offset, ln in entries:
        if digest == prev:
            continue
        prev = digest
        uniq.append((digest, offset, ln))
    if uniq:
        image = bytearray(data[:index_at])
        for digest, offset, ln in uniq:
            image += digest
            image += struct.pack("<qI", offset, ln)
        put_u32(image, 28, len(uniq))
        put_i64(image, 32, index_at)
        put_u32(image, 40, len(uniq) * 32)
        put_i64(image, 84, MANIFEST)
        write("read_index/v12_flat", bytes(image))


if __name__ == "__main__":
    main()
