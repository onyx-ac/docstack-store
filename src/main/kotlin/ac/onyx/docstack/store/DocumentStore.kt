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

    /** Write phase of `_bulkDocs`. One crossing, one atomic transaction. */
    public suspend fun bulkWrite(db: String, ops: List<WriteOp>): List<WriteResult>

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

    public suspend fun getLocal(db: String, id: String): Map<String, Any?>?

    /** Returns the new rev. */
    public suspend fun putLocal(db: String, id: String, doc: Map<String, Any?>): String

    public suspend fun removeLocal(db: String, id: String)

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
