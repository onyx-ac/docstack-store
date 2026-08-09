package ac.onyx.docstack.store.engine

import ac.onyx.docstack.store.AllDocsOptions
import ac.onyx.docstack.store.AllDocsResult
import ac.onyx.docstack.store.BulkGetRequest
import ac.onyx.docstack.store.ChangesOptions
import ac.onyx.docstack.store.ChangesResult
import ac.onyx.docstack.store.DocumentStore
import ac.onyx.docstack.store.LocalDoc
import ac.onyx.docstack.store.OpaqueRevTree
import ac.onyx.docstack.store.RevTreeEntry
import ac.onyx.docstack.store.RevsDiffEntry
import ac.onyx.docstack.store.StoreInfo
import ac.onyx.docstack.store.StoredDoc
import ac.onyx.docstack.store.WriteOp
import ac.onyx.docstack.store.WriteResult
import ac.onyx.docstack.store.toJsonElement
import ac.onyx.docstack.store.toKotlin
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import org.rocksdb.ColumnFamilyDescriptor
import org.rocksdb.ColumnFamilyHandle
import org.rocksdb.ColumnFamilyOptions
import org.rocksdb.DBOptions
import org.rocksdb.RocksDB
import org.rocksdb.WriteBatch
import org.rocksdb.WriteOptions

/**
 * RocksDB-backed [DocumentStore] (ADR-0003, spec 02 tasks 4-5). Runs the exact same
 * [ac.onyx.docstack.store.DocumentStoreConformanceTest] suite
 * [ac.onyx.docstack.store.InMemoryDocumentStore] does, per `docstack-store/CLAUDE.md`.
 *
 * Topology: one RocksDB directory per `db` name under [baseDir], each with its own
 * `docs`/`revs`/`trees`/`seq`/`local`/`attachments` column families (spec 02's
 * storage layout table) plus RocksDB's mandatory `default`. Opened lazily on first
 * touch, `createIfMissing` always true — a real store creating its file the first
 * time it's opened (including from a bare [info] call) is normal PouchDB-adapter
 * behavior. [destroy] is then exactly "close the handle, delete the directory."
 *
 * Sequence numbers are not a separately-persisted counter: on open, the in-process
 * [AtomicLong] is seeded from the `seq` column family's last key (`seekToLast`, 0 if
 * empty) — the `seq` CF's own max key is the counter's source of truth, so no `meta`
 * column family is needed beyond what spec 02's layout table already lists.
 *
 * `allDocs`/`changes`/`revsDiff` scan the relevant column family in full rather than
 * using native RocksDB range-seek optimizations for `startkey`/`endkey`/`descending` —
 * simpler, correct, and matches what [ac.onyx.docstack.store.InMemoryDocumentStore]
 * already does (filter-then-paginate in Kotlin, not a true range view). The 10k-document
 * benchmark spec 02's Verification section calls for is separate, heavier work this
 * task doesn't fold in.
 *
 * Attachment refcounts: same carried-forward limitation as
 * [ac.onyx.docstack.store.InMemoryDocumentStore] — the contract gives native no way to
 * know which digests a *superseded* revision referenced, so decrement-on-supersede is
 * deferred (spec 02 task 6) regardless of engine. This class stores/retrieves
 * attachment bytes correctly; it does not persist or act on a refcount at all.
 */
public class RocksDbDocumentStore(private val baseDir: File) : DocumentStore {

    private companion object {
        private val libraryLoaded = AtomicBoolean(false)
        private val COLUMN_FAMILY_NAMES = listOf("docs", "revs", "trees", "seq", "local", "attachments")
    }

    init {
        if (libraryLoaded.compareAndSet(false, true)) {
            RocksDB.loadLibrary()
        }
    }

    private class Handles(
        val default: ColumnFamilyHandle,
        val docs: ColumnFamilyHandle,
        val revs: ColumnFamilyHandle,
        val trees: ColumnFamilyHandle,
        val seq: ColumnFamilyHandle,
        val local: ColumnFamilyHandle,
        val attachments: ColumnFamilyHandle,
    ) {
        fun all(): List<ColumnFamilyHandle> = listOf(default, docs, revs, trees, seq, local, attachments)
    }

    private class OpenDb(val rocksDb: RocksDB, val handles: Handles, val seqCounter: AtomicLong, val latestSeq: MutableStateFlow<Long>)

    private val json = Json { ignoreUnknownKeys = true }
    private val databases = ConcurrentHashMap<String, OpenDb>()

    /** Serializes writers among themselves (atomicity, sequence allocation) and
     * db-open bookkeeping. Never taken by a read method — RocksDB's LSM design keeps
     * reads lock-free against an in-flight `WriteBatch` commit. */
    private val mutex = Mutex()

    @Serializable
    private data class DocsValue(val winningRev: String, val seq: Long, val deleted: Boolean)

    @Serializable
    private data class RevsValue(val body: JsonElement?, val deleted: Boolean, val attachmentDigests: List<String>)

    @Serializable
    private data class LocalValue(val rev: String, val body: JsonElement)

    private fun idKey(id: String): ByteArray = id.toByteArray(Charsets.UTF_8)

    private fun revKey(id: String, rev: String): ByteArray = idKey(id) + byteArrayOf(0) + rev.toByteArray(Charsets.UTF_8)

    private fun seqKey(seq: Long): ByteArray {
        val bytes = ByteArray(8)
        for (i in 0 until 8) bytes[i] = (seq shr (8 * (7 - i))).toByte()
        return bytes
    }

    private fun seqFromKey(bytes: ByteArray): Long {
        var value = 0L
        for (b in bytes) value = (value shl 8) or (b.toLong() and 0xFF)
        return value
    }

    private fun ByteArray.hasPrefix(prefix: ByteArray): Boolean {
        if (size < prefix.size) return false
        for (i in prefix.indices) if (this[i] != prefix[i]) return false
        return true
    }

    /** All still-known revision ids for [id] — a prefix scan over `revs`, the only
     * place native must enumerate a doc's non-winning leaf revisions (`allDocs` with
     * `includeConflicts`, `revsDiff`). */
    private fun revisionKeysForDoc(d: OpenDb, id: String): List<String> {
        val prefix = idKey(id) + byteArrayOf(0)
        val results = mutableListOf<String>()
        d.rocksDb.newIterator(d.handles.revs).use { it ->
            it.seek(prefix)
            while (it.isValid) {
                val key = it.key()
                if (!key.hasPrefix(prefix)) break
                results += String(key, prefix.size, key.size - prefix.size, Charsets.UTF_8)
                it.next()
            }
        }
        return results
    }

    private fun readLastSeq(rocksDb: RocksDB, seqHandle: ColumnFamilyHandle): Long =
        rocksDb.newIterator(seqHandle).use { it ->
            it.seekToLast()
            if (it.isValid) seqFromKey(it.key()) else 0L
        }

    private fun openDb(db: String): OpenDb {
        val dir = File(baseDir, db)
        dir.mkdirs()
        val descriptorNames = listOf(RocksDB.DEFAULT_COLUMN_FAMILY) + COLUMN_FAMILY_NAMES.map { it.toByteArray(Charsets.UTF_8) }
        val descriptors = descriptorNames.map { ColumnFamilyDescriptor(it, ColumnFamilyOptions()) }
        val handleList = ArrayList<ColumnFamilyHandle>(descriptors.size)
        val options = DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)
        val rocksDb = RocksDB.open(options, dir.absolutePath, descriptors, handleList)
        val handles = Handles(
            default = handleList[0],
            docs = handleList[1],
            revs = handleList[2],
            trees = handleList[3],
            seq = handleList[4],
            local = handleList[5],
            attachments = handleList[6],
        )
        val lastSeq = readLastSeq(rocksDb, handles.seq)
        val openDb = OpenDb(rocksDb, handles, AtomicLong(lastSeq), MutableStateFlow(lastSeq))
        databases[db] = openDb
        return openDb
    }

    private fun closeOpenDb(d: OpenDb) {
        d.handles.all().forEach { it.close() }
        d.rocksDb.close()
    }

    private suspend fun dbFor(db: String): OpenDb {
        databases[db]?.let { return it }
        return mutex.withLock {
            databases[db]?.let { return@withLock it }
            withContext(Dispatchers.IO) { openDb(db) }
        }
    }

    override suspend fun info(db: String): StoreInfo {
        val d = dbFor(db)
        return withContext(Dispatchers.IO) {
            var count = 0
            d.rocksDb.newIterator(d.handles.docs).use { it ->
                it.seekToFirst()
                while (it.isValid) {
                    if (!json.decodeFromString<DocsValue>(String(it.value(), Charsets.UTF_8)).deleted) count++
                    it.next()
                }
            }
            StoreInfo(docCount = count, updateSeq = d.seqCounter.get())
        }
    }

    override suspend fun getDoc(db: String, id: String, rev: String?): StoredDoc {
        val d = dbFor(db)
        return withContext(Dispatchers.IO) {
            val docsBytes = d.rocksDb.get(d.handles.docs, idKey(id)) ?: throw NoSuchElementException("doc not found: $db/$id")
            val docsValue = json.decodeFromString<DocsValue>(String(docsBytes, Charsets.UTF_8))
            val targetRev = rev ?: docsValue.winningRev
            val revBytes = d.rocksDb.get(d.handles.revs, revKey(id, targetRev))
                ?: throw NoSuchElementException("revision not found: $db/$id@$targetRev")
            val revValue = json.decodeFromString<RevsValue>(String(revBytes, Charsets.UTF_8))
            @Suppress("UNCHECKED_CAST")
            StoredDoc(id = id, rev = targetRev, seq = docsValue.seq, deleted = revValue.deleted, body = revValue.body?.toKotlin() as? Map<String, Any?>)
        }
    }

    override suspend fun getRevTrees(db: String, ids: List<String>): List<RevTreeEntry> {
        val d = dbFor(db)
        return withContext(Dispatchers.IO) {
            ids.map { id ->
                val docsBytes = d.rocksDb.get(d.handles.docs, idKey(id))
                if (docsBytes == null) {
                    RevTreeEntry(id = id, tree = null, winningRev = null, seq = 0, deleted = true)
                } else {
                    val docsValue = json.decodeFromString<DocsValue>(String(docsBytes, Charsets.UTF_8))
                    val treeBytes = d.rocksDb.get(d.handles.trees, idKey(id))
                    RevTreeEntry(
                        id = id,
                        tree = treeBytes?.let { String(it, Charsets.UTF_8) },
                        winningRev = docsValue.winningRev,
                        seq = docsValue.seq,
                        deleted = docsValue.deleted,
                    )
                }
            }
        }
    }

    override suspend fun bulkWrite(db: String, ops: List<WriteOp>): List<WriteResult?> {
        val d = dbFor(db)
        return mutex.withLock {
            withContext(Dispatchers.IO) {
                val batch = WriteBatch()
                val results = ArrayList<WriteResult?>(ops.size)
                // A batch that touches the same id twice (e.g. two edits to one doc in
                // one _bulkDocs call) must have the second op see the first op's write -
                // but the batch isn't committed to RocksDB until the end, so reads
                // within this loop go through this map first, not straight to the DB.
                val pending = HashMap<String, DocsValue?>()
                try {
                    for (op in ops) {
                        val existing = if (pending.containsKey(op.id)) {
                            pending[op.id]
                        } else {
                            d.rocksDb.get(d.handles.docs, idKey(op.id))?.let {
                                json.decodeFromString<DocsValue>(String(it, Charsets.UTF_8))
                            }
                        }
                        if (existing?.winningRev != op.expectedPrevWinningRev) {
                            results += null
                            continue
                        }
                        val seq = d.seqCounter.incrementAndGet()
                        if (existing != null) {
                            batch.delete(d.handles.seq, seqKey(existing.seq))
                        }
                        val docsValue = DocsValue(winningRev = op.winningRev, seq = seq, deleted = op.deleted)
                        batch.put(d.handles.docs, idKey(op.id), json.encodeToString(docsValue).toByteArray(Charsets.UTF_8))
                        pending[op.id] = docsValue

                        val revsValue = RevsValue(
                            body = op.body?.toJsonElement(),
                            deleted = op.deleted,
                            attachmentDigests = op.attachmentDigests ?: emptyList(),
                        )
                        batch.put(d.handles.revs, revKey(op.id, op.rev), json.encodeToString(revsValue).toByteArray(Charsets.UTF_8))
                        batch.put(d.handles.trees, idKey(op.id), op.tree.toByteArray(Charsets.UTF_8))
                        batch.put(d.handles.seq, seqKey(seq), idKey(op.id))
                        results += WriteResult(id = op.id, rev = op.rev, seq = seq)
                    }
                    d.rocksDb.write(WriteOptions(), batch)
                } finally {
                    batch.close()
                }
                d.latestSeq.value = d.seqCounter.get()
                results
            }
        }
    }

    override suspend fun allDocs(db: String, options: AllDocsOptions): AllDocsResult {
        val d = dbFor(db)
        return withContext(Dispatchers.IO) {
            val all: List<Pair<String, DocsValue>> = if (options.keys != null) {
                options.keys.mapNotNull { key ->
                    d.rocksDb.get(d.handles.docs, idKey(key))?.let { key to json.decodeFromString<DocsValue>(String(it, Charsets.UTF_8)) }
                }
            } else {
                val scanned = mutableListOf<Pair<String, DocsValue>>()
                d.rocksDb.newIterator(d.handles.docs).use { it ->
                    it.seekToFirst()
                    while (it.isValid) {
                        scanned += String(it.key(), Charsets.UTF_8) to json.decodeFromString<DocsValue>(String(it.value(), Charsets.UTF_8))
                        it.next()
                    }
                }
                var filtered = scanned.asSequence()
                if (options.startkey != null) filtered = filtered.filter { it.first >= options.startkey }
                if (options.endkey != null) {
                    filtered = if (options.inclusiveEnd) {
                        filtered.filter { it.first <= options.endkey }
                    } else {
                        filtered.filter { it.first < options.endkey }
                    }
                }
                val list = filtered.toList()
                if (options.descending) list.reversed() else list
            }

            val filtered = all.filter { (_, value) -> options.deleted || !value.deleted }
            val totalRows = filtered.size
            val skipped = filtered.drop(options.skip)
            val paged = if (options.limit != null) skipped.take(options.limit) else skipped

            val rows = paged.map { (id, docsValue) ->
                @Suppress("UNCHECKED_CAST")
                val body = if (options.includeBody) {
                    d.rocksDb.get(d.handles.revs, revKey(id, docsValue.winningRev))?.let {
                        json.decodeFromString<RevsValue>(String(it, Charsets.UTF_8)).body?.toKotlin() as? Map<String, Any?>
                    }
                } else {
                    null
                }
                val conflicts = if (options.includeConflicts) {
                    (revisionKeysForDoc(d, id) - docsValue.winningRev).takeIf { it.isNotEmpty() }
                } else {
                    null
                }
                StoredDoc(id = id, rev = docsValue.winningRev, seq = docsValue.seq, deleted = docsValue.deleted, body = body, conflicts = conflicts)
            }
            AllDocsResult(totalRows = totalRows, offset = options.skip, rows = rows)
        }
    }

    override suspend fun changes(db: String, options: ChangesOptions): ChangesResult {
        val d = dbFor(db)
        return withContext(Dispatchers.IO) {
            val scanned = mutableListOf<Pair<Long, String>>()
            d.rocksDb.newIterator(d.handles.seq).use { it ->
                it.seek(seqKey(options.since + 1))
                while (it.isValid) {
                    scanned += seqFromKey(it.key()) to String(it.value(), Charsets.UTF_8)
                    it.next()
                }
            }
            var list = if (options.descending) scanned.reversed() else scanned
            if (options.docIds != null) {
                val idSet = options.docIds.toSet()
                list = list.filter { it.second in idSet }
            }
            if (options.limit != null) list = list.take(options.limit)

            val results = list.mapNotNull { (seq, id) ->
                val docsBytes = d.rocksDb.get(d.handles.docs, idKey(id)) ?: return@mapNotNull null
                val docsValue = json.decodeFromString<DocsValue>(String(docsBytes, Charsets.UTF_8))
                @Suppress("UNCHECKED_CAST")
                val body = if (options.includeBody) {
                    d.rocksDb.get(d.handles.revs, revKey(id, docsValue.winningRev))?.let {
                        json.decodeFromString<RevsValue>(String(it, Charsets.UTF_8)).body?.toKotlin() as? Map<String, Any?>
                    }
                } else {
                    null
                }
                StoredDoc(id = id, rev = docsValue.winningRev, seq = seq, deleted = docsValue.deleted, body = body)
            }
            ChangesResult(lastSeq = results.lastOrNull()?.seq ?: options.since, results = results)
        }
    }

    override fun subscribeChanges(db: String, since: Long): Flow<StoredDoc> = flow {
        val d = dbFor(db)
        var lastSeen = since
        d.latestSeq.collect { latest ->
            if (latest > lastSeen) {
                val batch = withContext(Dispatchers.IO) {
                    val results = mutableListOf<StoredDoc>()
                    d.rocksDb.newIterator(d.handles.seq).use { it ->
                        it.seek(seqKey(lastSeen + 1))
                        while (it.isValid) {
                            val seq = seqFromKey(it.key())
                            if (seq > latest) break
                            val id = String(it.value(), Charsets.UTF_8)
                            val docsBytes = d.rocksDb.get(d.handles.docs, idKey(id))
                            if (docsBytes != null) {
                                val docsValue = json.decodeFromString<DocsValue>(String(docsBytes, Charsets.UTF_8))
                                // A stale seq CF entry must never be trusted without the
                                // write mutex; bulkWrite prunes the old entry on overwrite
                                // in the same batch, so this should never fire in practice.
                                if (docsValue.seq == seq) {
                                    @Suppress("UNCHECKED_CAST")
                                    val body = d.rocksDb.get(d.handles.revs, revKey(id, docsValue.winningRev))?.let {
                                        json.decodeFromString<RevsValue>(String(it, Charsets.UTF_8)).body?.toKotlin() as? Map<String, Any?>
                                    }
                                    results += StoredDoc(id = id, rev = docsValue.winningRev, seq = seq, deleted = docsValue.deleted, body = body)
                                }
                            }
                            it.next()
                        }
                    }
                    results
                }
                for (item in batch) emit(item)
                lastSeen = latest
            }
        }
    }

    override suspend fun revsDiff(db: String, revsByDocId: Map<String, List<String>>): Map<String, RevsDiffEntry> {
        val d = dbFor(db)
        return withContext(Dispatchers.IO) {
            revsByDocId.mapValues { (id, revs) ->
                val known = revisionKeysForDoc(d, id).toSet()
                RevsDiffEntry(missing = revs.filterNot { it in known })
            }
        }
    }

    override suspend fun bulkGet(db: String, requests: List<BulkGetRequest>): List<StoredDoc> {
        val d = dbFor(db)
        return withContext(Dispatchers.IO) {
            requests.mapNotNull { req ->
                val docsBytes = d.rocksDb.get(d.handles.docs, idKey(req.id)) ?: return@mapNotNull null
                val docsValue = json.decodeFromString<DocsValue>(String(docsBytes, Charsets.UTF_8))
                val rev = req.rev ?: docsValue.winningRev
                val revBytes = d.rocksDb.get(d.handles.revs, revKey(req.id, rev)) ?: return@mapNotNull null
                val revValue = json.decodeFromString<RevsValue>(String(revBytes, Charsets.UTF_8))
                @Suppress("UNCHECKED_CAST")
                StoredDoc(id = req.id, rev = rev, seq = docsValue.seq, deleted = revValue.deleted, body = revValue.body?.toKotlin() as? Map<String, Any?>)
            }
        }
    }

    override suspend fun compact(db: String, id: String, revs: List<String>, tree: OpaqueRevTree) {
        val d = dbFor(db)
        mutex.withLock {
            withContext(Dispatchers.IO) {
                val batch = WriteBatch()
                try {
                    for (rev in revs) batch.delete(d.handles.revs, revKey(id, rev))
                    batch.put(d.handles.trees, idKey(id), tree.toByteArray(Charsets.UTF_8))
                    d.rocksDb.write(WriteOptions(), batch)
                } finally {
                    batch.close()
                }
            }
        }
    }

    override suspend fun getLocal(db: String, id: String): LocalDoc? {
        val d = dbFor(db)
        return withContext(Dispatchers.IO) {
            val bytes = d.rocksDb.get(d.handles.local, idKey(id)) ?: return@withContext null
            val value = json.decodeFromString<LocalValue>(String(bytes, Charsets.UTF_8))
            @Suppress("UNCHECKED_CAST")
            LocalDoc(value.rev, value.body.toKotlin() as Map<String, Any?>)
        }
    }

    override suspend fun putLocal(db: String, id: String, doc: Map<String, Any?>, prevRev: String?): String {
        val d = dbFor(db)
        return mutex.withLock {
            withContext(Dispatchers.IO) {
                val existingBytes = d.rocksDb.get(d.handles.local, idKey(id))
                val existing = existingBytes?.let { json.decodeFromString<LocalValue>(String(it, Charsets.UTF_8)) }
                if (existing?.rev != prevRev) {
                    throw IllegalStateException("local doc conflict: $db/$id (expected rev $prevRev, found ${existing?.rev})")
                }
                val nextRevNumber = (existing?.rev?.substringAfter('-')?.toIntOrNull() ?: 0) + 1
                val rev = "0-$nextRevNumber"
                val value = LocalValue(rev = rev, body = doc.toJsonElement())
                d.rocksDb.put(d.handles.local, idKey(id), json.encodeToString(value).toByteArray(Charsets.UTF_8))
                rev
            }
        }
    }

    override suspend fun removeLocal(db: String, id: String, prevRev: String) {
        val d = dbFor(db)
        mutex.withLock {
            withContext(Dispatchers.IO) {
                val existingBytes = d.rocksDb.get(d.handles.local, idKey(id))
                    ?: throw NoSuchElementException("local doc not found: $db/$id")
                val existing = json.decodeFromString<LocalValue>(String(existingBytes, Charsets.UTF_8))
                if (existing.rev != prevRev) {
                    throw IllegalStateException("local doc conflict: $db/$id (expected rev $prevRev, found ${existing.rev})")
                }
                d.rocksDb.delete(d.handles.local, idKey(id))
            }
        }
    }

    override suspend fun getAttachment(db: String, digest: String): ByteArray {
        val d = dbFor(db)
        return withContext(Dispatchers.IO) {
            d.rocksDb.get(d.handles.attachments, idKey(digest)) ?: throw NoSuchElementException("attachment not found: $db/$digest")
        }
    }

    override suspend fun putAttachment(db: String, digest: String, data: ByteArray) {
        val d = dbFor(db)
        withContext(Dispatchers.IO) { d.rocksDb.put(d.handles.attachments, idKey(digest), data) }
    }

    override suspend fun destroy(db: String) {
        mutex.withLock {
            withContext(Dispatchers.IO) {
                databases.remove(db)?.let { closeOpenDb(it) }
                File(baseDir, db).deleteRecursively()
            }
        }
    }

    override suspend fun close(db: String) {
        mutex.withLock {
            withContext(Dispatchers.IO) {
                databases.remove(db)?.let { closeOpenDb(it) }
            }
        }
    }
}
