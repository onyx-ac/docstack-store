package ac.onyx.docstack.store

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * Runs against every [DocumentStore] implementation — in-memory here, the
 * RocksDB-backed one (spec 02 task 4) later — per `docstack-store/CLAUDE.md`:
 * "The in-memory and engine implementations run the same suite." Covers spec 02's
 * "Semantics that must hold."
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
    ) = WriteOp(id, rev, tree, winningRev, deleted, body, attachmentDigests)

    @Test
    fun `bulkWrite is one atomic unit with a contiguous increasing sequence range`() = runTest {
        val store = createStore()
        val results = store.bulkWrite(
            "db1",
            listOf(writeOp("doc1", "1-a"), writeOp("doc2", "1-a"), writeOp("doc3", "1-a")),
        )

        assertEquals(3, results.size)
        val seqs = results.map { it.seq }
        assertEquals(seqs.sorted(), seqs, "sequence should already be increasing in result order")
        assertEquals(seqs.toSet().size, seqs.size, "sequences must be unique")
        assertEquals((seqs.size - 1).toLong(), seqs.last() - seqs.first(), "sequence range must be contiguous")
    }

    @Test
    fun `sequences are monotonic across multiple bulkWrite calls`() = runTest {
        val store = createStore()
        val first = store.bulkWrite("db1", listOf(writeOp("doc1", "1-a")))
        val second = store.bulkWrite("db1", listOf(writeOp("doc2", "1-a")))
        assertTrue(second.first().seq > first.first().seq)
    }

    @Test
    fun `allDocs completes correctly while writes land underneath it`() = runBlocking {
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
        assertEquals(ids.distinct().size, ids.size, "no duplicate ids")
        assertEquals(ids.sorted(), ids, "allDocs must return lexically ordered ids")
        assertTrue(result.rows.size >= initialCount, "must see at least the pre-existing documents")
    }

    @Test
    fun `allDocs with includeConflicts returns non-winning leaf revisions`() = runTest {
        val store = createStore()
        store.bulkWrite("db1", listOf(writeOp("doc1", "1-a", winningRev = "1-a")))
        store.bulkWrite("db1", listOf(writeOp("doc1", "1-b", winningRev = "1-b")))

        val result = store.allDocs("db1", AllDocsOptions(includeConflicts = true))
        val row = result.rows.single { it.id == "doc1" }

        assertEquals("1-b", row.rev)
        assertEquals(listOf("1-a"), row.conflicts)
    }

    @Test
    fun `compact deletes named revision bodies and stores the rewritten tree`() = runTest {
        val store = createStore()
        store.bulkWrite("db1", listOf(writeOp("doc1", "1-a", winningRev = "1-a")))
        store.bulkWrite("db1", listOf(writeOp("doc1", "1-b", winningRev = "1-b")))

        store.compact("db1", "doc1", listOf("1-a"), tree = "tree:doc1:compacted")

        var threw = false
        try {
            store.getDoc("db1", "doc1", "1-a")
        } catch (e: NoSuchElementException) {
            threw = true
        }
        assertTrue(threw, "compacted revision must no longer be retrievable")

        val current = store.getDoc("db1", "doc1")
        assertEquals("1-b", current.rev)
        val trees = store.getRevTrees("db1", listOf("doc1"))
        assertEquals("tree:doc1:compacted", trees.single().tree)
    }

    @Test
    fun `destroy is idempotent and scoped to its own db`() = runTest {
        val store = createStore()
        store.bulkWrite("db1", listOf(writeOp("doc1", "1-a")))
        store.bulkWrite("db2", listOf(writeOp("doc1", "1-a")))

        store.destroy("db1")
        store.destroy("db1") // second call must not throw

        assertEquals(0, store.info("db1").docCount)
        assertEquals(1, store.info("db2").docCount, "an untouched db must be unaffected")
    }

    @Test
    fun `subscribeChanges replays missed writes then continues live, with no gap or duplicate`() = runTest {
        val store = createStore()
        val before = store.bulkWrite("db1", listOf(writeOp("doc1", "1-a")))
        val sinceSeq = before.first().seq - 1 // subscribe from just before the first write

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
        assertEquals(ids.distinct(), ids, "no duplicate across the replay/live join point")
    }
}
