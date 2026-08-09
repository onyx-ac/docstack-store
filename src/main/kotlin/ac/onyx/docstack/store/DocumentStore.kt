package ac.onyx.docstack.store

import kotlinx.coroutines.flow.Flow

/**
 * A revision tree as PouchDB serialised it. Native never parses this — merge
 * semantics live in `pouchdb-merge`, in JS. See ADR-0001.
 */
public typealias OpaqueRevTree = String

public data class StoreInfo(
    public val docCount: Int,
    public val updateSeq: Long,
)

public data class StoredDoc(
    public val id: String,
    public val rev: String,
    public val seq: Long,
    public val deleted: Boolean,
    /** Absent when the body was compacted away or the doc is deleted. */
    public val body: Map<String, Any?>? = null,
    /** Non-winning leaf revisions. Only populated by [DocumentStore.allDocs] with `includeConflicts`. */
    public val conflicts: List<String>? = null,
)

public data class LocalDoc(
    public val rev: String,
    public val body: Map<String, Any?>,
)

public data class RevTreeEntry(
    public val id: String,
    public val tree: OpaqueRevTree?,
    public val winningRev: String?,
    public val seq: Long,
    public val deleted: Boolean,
)

public data class WriteOp(
    public val id: String,
    public val rev: String,
    /** Full tree after the JS-side merge. Replaces whatever is stored. */
    public val tree: OpaqueRevTree,
    public val winningRev: String,
    public val deleted: Boolean,
    public val body: Map<String, Any?>? = null,
    /** Digests referenced by this revision. Native maintains refcounts. */
    public val attachmentDigests: List<String>? = null,
    /**
     * The document's winning rev at the moment the JS-side merge that produced
     * [tree] was computed, or `null` if the caller believed the document didn't
     * exist yet. [DocumentStore.bulkWrite] rejects (per-op, not batch-wide) an op
     * whose current winning rev has since moved on from this - a lost race between
     * two concurrent writers to the same id, rather than the second writer's merge
     * silently overwriting the first's.
     */
    public val expectedPrevWinningRev: String? = null,
)

public data class WriteResult(
    public val id: String,
    public val rev: String,
    public val seq: Long,
)

public data class AllDocsOptions(
    public val startkey: String? = null,
    public val endkey: String? = null,
    public val inclusiveEnd: Boolean = true,
    public val keys: List<String>? = null,
    public val limit: Int? = null,
    public val skip: Int = 0,
    public val descending: Boolean = false,
    public val includeBody: Boolean = false,
    /** Include non-winning leaf revisions so JS can report conflicts. */
    public val includeConflicts: Boolean = false,
    public val deleted: Boolean = false,
)

public data class AllDocsResult(
    public val totalRows: Int,
    public val offset: Int,
    public val rows: List<StoredDoc>,
)

public data class ChangesOptions(
    public val since: Long = 0,
    public val limit: Int? = null,
    public val descending: Boolean = false,
    public val includeBody: Boolean = false,
    public val docIds: List<String>? = null,
)

public data class ChangesResult(
    public val lastSeq: Long,
    public val results: List<StoredDoc>,
)

public data class BulkGetRequest(
    public val id: String,
    public val rev: String? = null,
)

public data class RevsDiffEntry(
    public val missing: List<String>,
    public val possibleAncestors: List<String>? = null,
)

/**
 * The interface the dispatcher calls. Answers the `storage` capability.
 *
 * Engine-agnostic: in-memory and RocksDB-backed implementations run the same
 * conformance suite ([DocumentStoreConformanceTest]). Never parses a revision
 * tree (ADR-0001) and has no carrier branch (ADR-0002) — carriers live above
 * this interface, in the dispatcher.
 */
public interface DocumentStore {
    public suspend fun info(db: String): StoreInfo

    /** [rev] omitted -> winning revision. */
    public suspend fun getDoc(db: String, id: String, rev: String? = null): StoredDoc

    /** Read phase of `_bulkDocs`. One crossing for the whole batch. */
    public suspend fun getRevTrees(db: String, ids: List<String>): List<RevTreeEntry>

    /**
     * Write phase of `_bulkDocs`. One crossing: ops whose [WriteOp.expectedPrevWinningRev]
     * still matches the document's current winning rev commit together, as one
     * atomic transaction. Any op that's gone stale (a concurrent writer got there
     * first) is skipped and reported as `null` at that position - positionally
     * aligned with [ops] - rather than failing the whole call. Matches CouchDB's own
     * per-doc partial-failure `_bulkDocs` semantics.
     */
    public suspend fun bulkWrite(db: String, ops: List<WriteOp>): List<WriteResult?>

    public suspend fun allDocs(db: String, options: AllDocsOptions): AllDocsResult

    public suspend fun changes(db: String, options: ChangesOptions): ChangesResult

    /**
     * Live changes from [since] onward: replays anything already committed past
     * [since], then continues with live writes, with no gap and no duplicate
     * across the join point. Collector cancellation is the unsubscribe.
     */
    public fun subscribeChanges(db: String, since: Long): Flow<StoredDoc>

    /**
     * Overridden rather than emulated by PouchDB core: replication calls this
     * constantly and the default implementation is O(n) round trips.
     */
    public suspend fun revsDiff(db: String, revsByDocId: Map<String, List<String>>): Map<String, RevsDiffEntry>

    public suspend fun bulkGet(db: String, requests: List<BulkGetRequest>): List<StoredDoc>

    /** Deletes bodies for the given revisions and stores the rewritten tree, in one transaction. */
    public suspend fun compact(db: String, id: String, revs: List<String>, tree: OpaqueRevTree)

    /** null when no such local doc exists. */
    public suspend fun getLocal(db: String, id: String): LocalDoc?

    /**
     * Returns the new rev, formatted `"0-N"` (matching PouchDB/CouchDB's own
     * local-doc convention). [prevRev] omitted means the doc must not already
     * exist. Throws [IllegalStateException] if [prevRev] doesn't match the
     * currently stored rev - including "expected a rev, found none" and vice
     * versa. Unlike regular docs, native decides this conflict itself: local
     * docs never replicate, so ADR-0001's "native never decides revision
     * semantics" doesn't apply to them.
     */
    public suspend fun putLocal(db: String, id: String, doc: Map<String, Any?>, prevRev: String? = null): String

    /** Throws [IllegalStateException] on a [prevRev] mismatch, [NoSuchElementException] if no such local doc exists. */
    public suspend fun removeLocal(db: String, id: String, prevRev: String)

    /**
     * Attachment bodies use a binary side-channel, not the JSON envelope — see
     * ADR-0002.
     */
    public suspend fun getAttachment(db: String, digest: String): ByteArray

    public suspend fun putAttachment(db: String, digest: String, data: ByteArray)

    /** Removes every family for [db]. Idempotent. */
    public suspend fun destroy(db: String)

    public suspend fun close(db: String)
}
