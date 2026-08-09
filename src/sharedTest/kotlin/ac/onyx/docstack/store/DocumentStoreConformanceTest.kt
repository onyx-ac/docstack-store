package ac.onyx.docstack.store

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * Runs against every [DocumentStore] implementation — in-memory here, the
 * RocksDB-backed one (spec 02 task 4) later — per `docstack-store/CLAUDE.md`:
 * "The in-memory and engine implementations run the same suite." Covers spec 02's
 * "Semantics that must hold."
 *
 * Lives in `src/sharedTest`, wired into both the `test` (JVM) and `androidTest`
 * (on-device) source sets in `build.gradle.kts`, because the RocksDB-backed subclass
 * only runs on-device (native `.so`) while the in-memory one only needs the plain
 * JVM. Plain JUnit 4, not Jupiter: Android instrumented tests don't support JUnit 5
 * without extra tooling this project isn't otherwise using, and JUnit 4 runs
 * unmodified in both places, so this one abstract class serves both engines with
 * zero duplication.
 */
public abstract class DocumentStoreConformanceTest {

    protected abstract fun createStore(): DocumentStore

    private fun writeOp(
        id: String,
        rev: String,
        winningRev: String = rev,
        deleted: Boolean = false,
        body: Map<String, Any?>? = mapOf("hello" to id),
        tree: OpaqueRevTree = "tree:$id:$rev",
        attachmentDigests: List<String>? = null,
        expectedPrevWinningRev: String? = null,
    ) = WriteOp(id, rev, tree, winningRev, deleted, body, attachmentDigests, expectedPrevWinningRev)

    @Test
    public fun bulkWrite_isOneAtomicUnit_withContiguousIncreasingSequenceRange(): Unit = runTest {
        val store = createStore()
        val results = store.bulkWrite(
            "db1",
            listOf(writeOp("doc1", "1-a"), writeOp("doc2", "1-a"), writeOp("doc3", "1-a")),
        )

        assertEquals(3, results.size)
        val seqs = results.map { it!!.seq }
        assertEquals("sequence should already be increasing in result order", seqs.sorted(), seqs)
        assertEquals("sequences must be unique", seqs.toSet().size, seqs.size)
        assertEquals("sequence range must be contiguous", (seqs.size - 1).toLong(), seqs.last() - seqs.first())
    }

    @Test
    public fun sequences_areMonotonic_acrossMultipleBulkWriteCalls(): Unit = runTest {
        val store = createStore()
        val first = store.bulkWrite("db1", listOf(writeOp("doc1", "1-a")))
        val second = store.bulkWrite("db1", listOf(writeOp("doc2", "1-a")))
        assertTrue(second.first()!!.seq > first.first()!!.seq)
    }

    @Test
    public fun allDocs_completesCorrectly_whileWritesLandUnderneathIt(): Unit = runBlocking {
        // Real threads (Dispatchers.Default), not runTest's virtual scheduler -
        // the point is exercising genuine concurrent mutation against the
        // lock-free read path, not just logically-interleaved coroutines on one
        // thread.
        val store = createStore()
        val initialCount = 500
        store.bulkWrite("db1", (0 until initialCount).map { writeOp("doc-%05d".format(it), "1-a") })

        val writer = async(Dispatchers.Default) {
            repeat(200) { i -> store.bulkWrite("db1", listOf(writeOp("extra-%05d".format(i), "1-a"))) }
        }
        val reader = async(Dispatchers.Default) { store.allDocs("db1", AllDocsOptions()) }

        writer.await()
        val result = reader.await()

        val ids = result.rows.map { it.id }
        assertEquals("no duplicate ids", ids.distinct().size, ids.size)
        assertEquals("allDocs must return lexically ordered ids", ids.sorted(), ids)
        assertTrue("must see at least the pre-existing documents", result.rows.size >= initialCount)
    }

    @Test
    public fun allDocs_withIncludeConflicts_returnsNonWinningLeafRevisions(): Unit = runTest {
        val store = createStore()
        store.bulkWrite("db1", listOf(writeOp("doc1", "1-a", winningRev = "1-a")))
        store.bulkWrite("db1", listOf(writeOp("doc1", "1-b", winningRev = "1-b", expectedPrevWinningRev = "1-a")))

        val result = store.allDocs("db1", AllDocsOptions(includeConflicts = true))
        val row = result.rows.single { it.id == "doc1" }

        assertEquals("1-b", row.rev)
        assertEquals(listOf("1-a"), row.conflicts)
    }

    @Test
    public fun allDocs_totalRows_isTheWholeDatabasesNonDeletedCount_regardlessOfQuery(): Unit = runTest {
        val store = createStore()
        store.bulkWrite(
            "db1",
            (0 until 10).map { writeOp("doc-%02d".format(it), "1-a") },
        )
        store.bulkWrite("db1", listOf(writeOp("doc-03", "2-b", deleted = true, expectedPrevWinningRev = "1-a")))

        assertEquals("no query restriction", 9, store.allDocs("db1", AllDocsOptions()).totalRows)
        assertEquals(
            "startkey/endkey must not change total_rows",
            9,
            store.allDocs("db1", AllDocsOptions(startkey = "doc-05")).totalRows,
        )
        assertEquals(
            "keys must not change total_rows",
            9,
            store.allDocs("db1", AllDocsOptions(keys = listOf("doc-00", "doc-03"))).totalRows,
        )
        assertEquals(
            "skip/limit must not change total_rows",
            9,
            store.allDocs("db1", AllDocsOptions(skip = 2, limit = 1)).totalRows,
        )
    }

    @Test
    public fun compact_deletesNamedRevisionBodies_andStoresTheRewrittenTree(): Unit = runTest {
        val store = createStore()
        store.bulkWrite("db1", listOf(writeOp("doc1", "1-a", winningRev = "1-a")))
        store.bulkWrite("db1", listOf(writeOp("doc1", "1-b", winningRev = "1-b", expectedPrevWinningRev = "1-a")))

        store.compact("db1", "doc1", listOf("1-a"), tree = "tree:doc1:compacted")

        var threw = false
        try {
            store.getDoc("db1", "doc1", "1-a")
        } catch (e: NoSuchElementException) {
            threw = true
        }
        assertTrue("compacted revision must no longer be retrievable", threw)

        val current = store.getDoc("db1", "doc1")
        assertEquals("1-b", current.rev)
        val trees = store.getRevTrees("db1", listOf("doc1"))
        assertEquals("tree:doc1:compacted", trees.single().tree)
    }

    @Test
    public fun info_docCount_excludesDocsWhoseWinningRevisionIsDeleted(): Unit = runTest {
        val store = createStore()
        store.bulkWrite(
            "db1",
            listOf(writeOp("doc1", "1-a"), writeOp("doc2", "1-a"), writeOp("doc3", "1-a")),
        )
        assertEquals(3, store.info("db1").docCount)

        store.bulkWrite("db1", listOf(writeOp("doc1", "2-b", deleted = true, expectedPrevWinningRev = "1-a")))
        assertEquals("a deleted doc's winning revision must not count", 2, store.info("db1").docCount)
    }

    @Test
    public fun bulkWrite_rejectsAnOp_whoseExpectedPrevWinningRevHasGoneStale(): Unit = runTest {
        val store = createStore()
        store.bulkWrite("db1", listOf(writeOp("doc1", "1-a")))

        // Two "writers" that both read the doc before either wrote - only the one
        // whose expectation still matches may commit; the other lost the race.
        val results = store.bulkWrite(
            "db1",
            listOf(
                writeOp("doc1", "2-b", expectedPrevWinningRev = "1-a"),
                writeOp("doc1", "2-c", expectedPrevWinningRev = "1-a"),
            ),
        )

        assertEquals("2-b", results[0]?.rev)
        assertEquals("the second writer's assumption was stale by the time its op ran", null, results[1])
        assertEquals("2-b", store.getDoc("db1", "doc1").rev)
    }

    @Test
    public fun bulkWrite_chainsSequentialEditsToTheSameId_withinOneBatch(): Unit = runTest {
        val store = createStore()

        // A single batch can legally contain more than one edit to the same id
        // (real replication payloads do this) - the second op's expectation is
        // against the first op's write, not the pre-batch state.
        val results = store.bulkWrite(
            "db1",
            listOf(
                writeOp("doc1", "1-a", expectedPrevWinningRev = null),
                writeOp("doc1", "2-b", expectedPrevWinningRev = "1-a"),
            ),
        )

        assertTrue("both ops touch the same id but chain correctly, so both apply", results.all { it != null })
        assertEquals("2-b", store.getDoc("db1", "doc1").rev)
    }

    @Test
    public fun destroy_isIdempotent_andScopedToItsOwnDb(): Unit = runTest {
        val store = createStore()
        store.bulkWrite("db1", listOf(writeOp("doc1", "1-a")))
        store.bulkWrite("db2", listOf(writeOp("doc1", "1-a")))

        store.destroy("db1")
        store.destroy("db1") // second call must not throw

        assertEquals(0, store.info("db1").docCount)
        assertEquals("an untouched db must be unaffected", 1, store.info("db2").docCount)
    }

    @Test
    public fun subscribeChanges_replaysMissedWrites_thenContinuesLive_noGapOrDuplicate(): Unit = runTest {
        val store = createStore()
        val before = store.bulkWrite("db1", listOf(writeOp("doc1", "1-a")))
        val sinceSeq = before.first()!!.seq - 1 // subscribe from just before the first write

        val collected = mutableListOf<StoredDoc>()
        val collector = launch {
            store.subscribeChanges("db1", sinceSeq).take(3).toList(collected)
        }

        yield() // let the collector reach its replay point before more writes land
        store.bulkWrite("db1", listOf(writeOp("doc2", "1-a")))
        store.bulkWrite("db1", listOf(writeOp("doc3", "1-a")))

        collector.join()

        val ids = collected.map { it.id }
        assertEquals(listOf("doc1", "doc2", "doc3"), ids)
        assertEquals("no duplicate across the replay/live join point", ids.distinct(), ids)
    }

    @Test
    public fun putLocal_andRemoveLocal_detectConflictingWrites(): Unit = runTest {
        val store = createStore()

        val rev1 = store.putLocal("db1", "local1", mapOf("k" to "v1"))
        assertEquals("0-1", rev1)

        var threwOnStalePut = false
        try {
            store.putLocal("db1", "local1", mapOf("k" to "v2")) // prevRev omitted, but a doc already exists
        } catch (e: IllegalStateException) {
            threwOnStalePut = true
        }
        assertTrue("put without the current rev must conflict", threwOnStalePut)

        val rev2 = store.putLocal("db1", "local1", mapOf("k" to "v2"), rev1)
        assertEquals("0-2", rev2)

        var threwOnStaleRemove = false
        try {
            store.removeLocal("db1", "local1", rev1) // stale rev
        } catch (e: IllegalStateException) {
            threwOnStaleRemove = true
        }
        assertTrue("remove with a stale rev must conflict", threwOnStaleRemove)

        store.removeLocal("db1", "local1", rev2)
        assertEquals(null, store.getLocal("db1", "local1"))

        var threwOnMissingRemove = false
        try {
            store.removeLocal("db1", "local1", rev2)
        } catch (e: NoSuchElementException) {
            threwOnMissingRemove = true
        }
        assertTrue("remove of an already-removed doc must throw not-found", threwOnMissingRemove)
    }
}
