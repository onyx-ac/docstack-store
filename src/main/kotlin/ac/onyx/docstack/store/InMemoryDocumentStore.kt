package ac.onyx.docstack.store

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * In-memory [DocumentStore]. Written first so the dispatcher and the JS adapter
 * can be developed and tested against it (spec 02 task 2); the RocksDB-backed
 * implementation (task 4) runs the same conformance suite.
 *
 * Data structures are chosen so a reader is never blocked by a writer: reads go
 * straight to [ConcurrentSkipListMap]/[ConcurrentHashMap], which support safe,
 * weakly-consistent concurrent iteration with no lock. A single [Mutex] guards
 * only the write path (`bulkWrite`, `compact`, `putLocal`/`removeLocal`,
 * `destroy`), serializing writers among themselves for atomicity and monotonic
 * sequence allocation.
 *
 * Known, deliberate limitation: [WriteOp.attachmentDigests] lists digests a
 * revision *references*, but the contract gives no way to know which digests a
 * *superseded* revision referenced (only the new tree is passed). Refcounts
 * therefore only increment here; decrement-on-supersede and filesystem spill are
 * real, deferred work (spec 02 task 6), not something this store's inputs can
 * fully solve.
 */
public class InMemoryDocumentStore : DocumentStore {

    private data class RevisionRecord(
        val body: Map<String, Any?>?,
        val deleted: Boolean,
        val attachmentDigests: List<String>,
    )

    private data class DocRecord(
        val tree: OpaqueRevTree,
        val winningRev: String,
        val seq: Long,
        val revisions: Map<String, RevisionRecord>,
    ) {
        val winningRevision: RevisionRecord? get() = revisions[winningRev]
    }

    private data class LocalRecord(val rev: String, val body: Map<String, Any?>)

    private data class AttachmentRecord(val data: ByteArray?, val refCount: Int)

    private class DbState {
        val docs = ConcurrentSkipListMap<String, DocRecord>()
        val bySeq = ConcurrentSkipListMap<Long, String>()
        val local = ConcurrentHashMap<String, LocalRecord>()
        val attachments = ConcurrentHashMap<String, AttachmentRecord>()
        val seqCounter = AtomicLong(0)
        val latestSeq = MutableStateFlow(0L)
    }

    private val databases = ConcurrentHashMap<String, DbState>()
    private val mutex = Mutex()

    private fun existingDb(db: String): DbState? = databases[db]

    private fun dbForWrite(db: String): DbState = databases.computeIfAbsent(db) { DbState() }

    override suspend fun info(db: String): StoreInfo {
        val database = existingDb(db) ?: return StoreInfo(docCount = 0, updateSeq = 0)
        return StoreInfo(docCount = database.docs.size, updateSeq = database.seqCounter.get())
    }

    override suspend fun getDoc(db: String, id: String, rev: String?): StoredDoc {
        val record = existingDb(db)?.docs?.get(id) ?: throw NoSuchElementException("doc not found: $db/$id")
        val targetRev = rev ?: record.winningRev
        val revRecord = record.revisions[targetRev]
            ?: throw NoSuchElementException("revision not found: $db/$id@$targetRev")
        return StoredDoc(id = id, rev = targetRev, seq = record.seq, deleted = revRecord.deleted, body = revRecord.body)
    }

    override suspend fun getRevTrees(db: String, ids: List<String>): List<RevTreeEntry> {
        val database = existingDb(db)
        return ids.map { id ->
            val record = database?.docs?.get(id)
            if (record == null) {
                RevTreeEntry(id = id, tree = null, winningRev = null, seq = 0, deleted = true)
            } else {
                RevTreeEntry(
                    id = id,
                    tree = record.tree,
                    winningRev = record.winningRev,
                    seq = record.seq,
                    deleted = record.winningRevision?.deleted ?: true,
                )
            }
        }
    }

    override suspend fun bulkWrite(db: String, ops: List<WriteOp>): List<WriteResult> = mutex.withLock {
        val database = dbForWrite(db)
        val results = ArrayList<WriteResult>(ops.size)
        for (op in ops) {
            val seq = database.seqCounter.incrementAndGet()
            val existing = database.docs[op.id]
            if (existing != null) {
                database.bySeq.remove(existing.seq)
            }
            val revisions = (existing?.revisions ?: emptyMap()) +
                (op.rev to RevisionRecord(op.body, op.deleted, op.attachmentDigests ?: emptyList()))
            database.docs[op.id] = DocRecord(
                tree = op.tree,
                winningRev = op.winningRev,
                seq = seq,
                revisions = revisions,
            )
            database.bySeq[seq] = op.id
            op.attachmentDigests?.forEach { digest ->
                database.attachments.compute(digest) { _, existingAttachment ->
                    existingAttachment?.copy(refCount = existingAttachment.refCount + 1)
                        ?: AttachmentRecord(data = null, refCount = 1)
                }
            }
            results += WriteResult(id = op.id, rev = op.rev, seq = seq)
        }
        database.latestSeq.value = database.seqCounter.get()
        results
    }

    override suspend fun allDocs(db: String, options: AllDocsOptions): AllDocsResult {
        val database = existingDb(db) ?: return AllDocsResult(totalRows = 0, offset = options.skip, rows = emptyList())

        val selected: List<Pair<String, DocRecord>> = if (options.keys != null) {
            options.keys.mapNotNull { key -> database.docs[key]?.let { key to it } }
        } else {
            val ordered = if (options.descending) database.docs.descendingMap() else database.docs
            ordered.entries.asSequence()
                .filter { (key, _) ->
                    val afterStart = options.startkey == null || key >= options.startkey
                    val beforeEnd = options.endkey == null ||
                        if (options.inclusiveEnd) key <= options.endkey else key < options.endkey
                    afterStart && beforeEnd
                }
                .map { it.key to it.value }
                .toList()
        }

        val filtered = selected.filter { (_, record) ->
            val winningDeleted = record.winningRevision?.deleted ?: true
            options.deleted || !winningDeleted
        }

        val totalRows = filtered.size
        val skipped = filtered.drop(options.skip)
        val paged = if (options.limit != null) skipped.take(options.limit) else skipped

        val rows = paged.map { (id, record) ->
            val revRecord = record.winningRevision
            StoredDoc(
                id = id,
                rev = record.winningRev,
                seq = record.seq,
                deleted = revRecord?.deleted ?: true,
                body = if (options.includeBody) revRecord?.body else null,
                conflicts = if (options.includeConflicts) {
                    (record.revisions.keys - record.winningRev).toList().takeIf { it.isNotEmpty() }
                } else {
                    null
                },
            )
        }

        return AllDocsResult(totalRows = totalRows, offset = options.skip, rows = rows)
    }

    override suspend fun changes(db: String, options: ChangesOptions): ChangesResult {
        val database = existingDb(db) ?: return ChangesResult(lastSeq = options.since, results = emptyList())

        val tail = database.bySeq.tailMap(options.since, false)
        var entries = (if (options.descending) tail.descendingMap() else tail).entries.asSequence()
        if (options.docIds != null) {
            val idSet = options.docIds.toSet()
            entries = entries.filter { it.value in idSet }
        }
        var list = entries.toList()
        if (options.limit != null) list = list.take(options.limit)

        val results = list.mapNotNull { (seq, id) ->
            val record = database.docs[id] ?: return@mapNotNull null
            val revRecord = record.winningRevision
            StoredDoc(
                id = id,
                rev = record.winningRev,
                seq = seq,
                deleted = revRecord?.deleted ?: true,
                body = if (options.includeBody) revRecord?.body else null,
            )
        }
        return ChangesResult(lastSeq = results.lastOrNull()?.seq ?: options.since, results = results)
    }

    override fun subscribeChanges(db: String, since: Long): Flow<StoredDoc> = flow {
        val database = dbForWrite(db)
        var lastSeen = since
        database.latestSeq.collect { latest ->
            if (latest > lastSeen) {
                val batch = database.bySeq.tailMap(lastSeen, false)
                for ((seq, id) in batch) {
                    val record = database.docs[id] ?: continue
                    // Guard against a stale entry: bulkWrite prunes the old seq on
                    // overwrite, so this should never fire, but a reader must never
                    // trust an index it didn't take a lock to read.
                    if (record.seq != seq) continue
                    val revRecord = record.winningRevision
                    emit(StoredDoc(id, record.winningRev, seq, revRecord?.deleted ?: true, revRecord?.body))
                }
                lastSeen = latest
            }
        }
    }

    override suspend fun revsDiff(db: String, revsByDocId: Map<String, List<String>>): Map<String, RevsDiffEntry> {
        val database = existingDb(db)
        return revsByDocId.mapValues { (id, revs) ->
            val known = database?.docs?.get(id)?.revisions?.keys ?: emptySet()
            RevsDiffEntry(missing = revs.filterNot { it in known })
        }
    }

    override suspend fun bulkGet(db: String, requests: List<BulkGetRequest>): List<StoredDoc> {
        val database = existingDb(db) ?: return emptyList()
        return requests.mapNotNull { req ->
            val record = database.docs[req.id] ?: return@mapNotNull null
            val rev = req.rev ?: record.winningRev
            val revRecord = record.revisions[rev] ?: return@mapNotNull null
            StoredDoc(id = req.id, rev = rev, seq = record.seq, deleted = revRecord.deleted, body = revRecord.body)
        }
    }

    override suspend fun compact(db: String, id: String, revs: List<String>, tree: OpaqueRevTree): Unit = mutex.withLock {
        val database = existingDb(db) ?: return@withLock
        val existing = database.docs[id] ?: return@withLock
        val remainingRevisions = existing.revisions - revs.toSet()
        database.docs[id] = existing.copy(tree = tree, revisions = remainingRevisions)
    }

    override suspend fun getLocal(db: String, id: String): Map<String, Any?>? = existingDb(db)?.local?.get(id)?.body

    override suspend fun putLocal(db: String, id: String, doc: Map<String, Any?>): String = mutex.withLock {
        val database = dbForWrite(db)
        val nextRevNumber = (database.local[id]?.rev?.substringBefore('-')?.toIntOrNull() ?: 0) + 1
        val rev = "$nextRevNumber-local"
        database.local[id] = LocalRecord(rev, doc)
        rev
    }

    override suspend fun removeLocal(db: String, id: String): Unit = mutex.withLock {
        existingDb(db)?.local?.remove(id)
        Unit
    }

    override suspend fun getAttachment(db: String, digest: String): ByteArray {
        val record = existingDb(db)?.attachments?.get(digest)
        return record?.data ?: throw NoSuchElementException("attachment not found: $db/$digest")
    }

    override suspend fun putAttachment(db: String, digest: String, data: ByteArray) {
        val database = dbForWrite(db)
        database.attachments.compute(digest) { _, existing -> AttachmentRecord(data, existing?.refCount ?: 0) }
    }

    override suspend fun destroy(db: String): Unit = mutex.withLock {
        databases.remove(db)
        Unit
    }

    override suspend fun close(db: String) {
        // No engine-level resource to release for an in-memory store.
    }
}
