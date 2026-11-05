# Prolly algorithm proofs

This package contains unbounded, kernel-checked Lean proofs of an abstract model
of DoltLite's prolly algorithms. Keys, tree depth, node fanout, row counts, value
sizes, and edit histories have no exploration bound. Values are generic; the
ordered-key model uses mathematical integers. The package has no third-party
Lean dependencies.

These are proofs about the model, **not a formal refinement proof of the C
implementation**. Passing this check does not establish that the shipped C
engine satisfies these theorems. The source mapping below identifies the code
whose behavior the model specifies; that correspondence still requires review
and implementation tests.

## Run

Install [Lean's toolchain manager](https://lean-lang.org/install/manual/), then:

```sh
cd formal/prolly
lake --wfail build
lake env lean Prolly/Audit.lean
cd ../..
python3 test/prolly_proofs_selftest.py
```

`lean-toolchain` pins Lean 4.35.0. CI also verifies the Linux toolchain archive's
SHA-256 digest. The audit scans every declaration in `DoltLite.Prolly`, including
transitive dependencies, and permits only Lean's standard `propext`,
`Classical.choice`, and `Quot.sound` axioms. Custom axioms, `sorryAx`, and
compiler-trusted proof shortcuts are rejected. Warnings fail the build. The
explicit audit command runs the audit even if its module was already built.

## Properties and source correspondence

| Model | Proven properties | C correspondence |
| --- | --- | --- |
| `OrderedMap.lean` | Insert/overwrite and delete affect exactly their key; edits preserve strict key order; arbitrary edit histories agree with an independent functional-map replay; last write wins; independent updates commute; sorted maps with equal lookups have identical rows. | `prolly_mutmap.c`, `prolly_mutate.c` |
| `Tree.lean` | Inclusive maximum-child-key routing agrees with lookup in flattened rows for every valid tree; flattened rows are strictly ordered; recursive subtree counts equal flattened row counts; tree contents compose with edit replay. | `prolly_node.c` search, `prolly_cursor.c` routing/counting |
| `Chunker.lean` | Chunking preserves every row in order and emits no empty chunks; carrying chunker state across arbitrary input batches gives identical chunks; histories with equal final maps give identical leaf chunks and the same deterministic content address. | `prolly_chunker.c` add/flush/finish, `prolly_mutate.c` rebuild |
| `DiffMerge.lean` | Diff is absent exactly for equal values; applying a diff reconstructs the new map; reversing changes gives the reverse diff; three-way fast merge is symmetric, preserves unilateral edits and identical edits, and declines exactly the divergent changes requiring fallback. | `prolly_diff.c`, `prolly_three_way_merge.c` (`fmResolveAndEmit`) |
| `Examples.lean` | A valid, inhabited two-child tree includes negative keys, an inclusive fence lookup, and an absent key between children; malformed fence metadata is rejected; deletion and delete/modify fallback cases are checked. | Concrete witnesses and boundary cases for the model |

`Audit.lean` checks the theorem inventory and trust dependencies. The self-test
first checks the unchanged package, then checks isolated copies with deliberate
bugs: a lost overwrite, a missed deletion, exclusive fence routing, a discarded
completed chunk, and a conflict silently resolved to ours. Every broken model
must fail verification. Additional controls must reject a custom axiom and an
unfinished proof. These controls test the verification gate, not the C engine.

## Model boundaries

- A tree is an immutable ordered multiway tree. Every child fence equals a key
  in that child, bounds all its keys from above, and precedes every later child's
  keys. Empty internal nodes and empty children are excluded. Equal leaf depth,
  node capacity, canonical root collapse, and incremental subtree reuse are not
  modeled.
- Integer keys represent the ordered key domain. The model does not prove the
  C int64 or blob-key encodings, comparison routines, SQL affinities, or
  collations correct.
- Chunk boundaries are supplied by an arbitrary deterministic predicate on the
  current pending chunk. The theorems hold for every such predicate, but do not
  establish that C's byte accounting, Weibull decisions, or multi-level tree
  construction implement a particular predicate correctly. The content-address
  theorem concerns leaf-chunk content and a deterministic addressing function;
  it does not prove the C root hash or cryptographic collision resistance.
- Diff and fast merge use decidable equality on abstract values. A missing row
  is `none`. A successful deletion is `some none` in `mergeValue`; outer `none`
  means the fast path must decline and use the full merge path. This does not
  prove the SQL/schema/cell-level conflict resolver or commit-graph merge-base
  selection. Diffs are specified pointwise; enumeration and hash-based pruning
  in the C diff iterator are not modeled.
- Edit replay abstracts the mutable map's data structure and its undo/savepoint
  implementation.
- Memory safety, allocation failure, node serialization, sparse/prefix caches,
  file I/O, durability, concurrency, and the chunk store are outside this model.
  No C source or storage format is changed by this package.

The `Prolly proofs` workflow checks the package on relevant pull requests and
master pushes. Changes to the model or mapped C algorithms must preserve this
scope distinction: running abstract proofs against a changed C checkout is not
an automatic check that the C code refines the model.
