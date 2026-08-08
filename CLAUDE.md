# docstack-store — native document store

Read `@../specs/02-docstack-store.md` and `@../specs/adr/0001-document-level-seam.md`
before changing anything here.

## Hard rules

- **Never parse a revision tree.** Trees are opaque blobs: store them, return them.
  Merge semantics belong to `pouchdb-merge`, in JS.
- **Never depend on `permetic-core`.** Permetic registration lives in
  `docstack-permetic`. This module must be usable without a WebView.
- `bulkWrite` is one atomic transaction, sequences allocated inside it.
- Sequences are monotonic. Gaps fine, reordering never — replication checkpoints
  on them.
- Readers are never blocked by the writer.
- Attachment refcounts change in the same transaction as the write that
  references them.

## Structure

```
ac.onyx.docstack.store
  DocumentStore     the interface the dispatcher calls
  dispatcher/       envelope decode -> DocumentStore, generated from the contract
  engine/           RocksDB implementation (ADR-0003, provisional)
  attachments/      digest-keyed blobs, refcounts, filesystem spill
```

## Testing

The in-memory and engine implementations run the same suite. Build the in-memory
one first — the dispatcher and the JS adapter are developed against it.

Crash consistency and concurrent-read-under-write are not optional tests. They are
where this module actually fails.
