package ac.onyx.docstack.store.dispatcher

import ac.onyx.docstack.store.AllDocsOptions
import ac.onyx.docstack.store.AllDocsResult
import ac.onyx.docstack.store.BulkGetRequest
import ac.onyx.docstack.store.ChangesOptions
import ac.onyx.docstack.store.ChangesResult
import ac.onyx.docstack.store.DocumentStore
import ac.onyx.docstack.store.InMemoryDocumentStore
import ac.onyx.docstack.store.OpaqueRevTree
import ac.onyx.docstack.store.RevTreeEntry
import ac.onyx.docstack.store.RevsDiffEntry
import ac.onyx.docstack.store.StoreInfo
import ac.onyx.docstack.store.StoredDoc
import ac.onyx.docstack.store.WriteOp
import ac.onyx.docstack.store.WriteResult
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * Round-trips every [StorageDispatcher.DISPATCHED_METHODS] entry through real JSON
 * args, per spec 02 task 3 ("Dispatcher... envelope decode, method dispatch, error
 * mapping to BridgeErrorCode, cancellation") — see `android/specs/02-docstack-store.md`.
 */
public class StorageDispatcherTest {

    private fun request(method: String, vararg args: kotlinx.serialization.json.JsonElement) =
        BridgeRequest(v = 1, id = "req-1", capability = "storage", method = method, args = args.toList())

    private fun ok(response: BridgeResponse): BridgeResponse.Ok {
        assertTrue("expected Ok, got $response", response is BridgeResponse.Ok)
        return response as BridgeResponse.Ok
    }

    private fun err(response: BridgeResponse): BridgeResponse.Err {
        assertTrue("expected Err, got $response", response is BridgeResponse.Err)
        return response as BridgeResponse.Err
    }

    @Test
    fun `info`() = runTest {
        val dispatcher = StorageDispatcher(InMemoryDocumentStore())
        dispatcher.dispatch(request("bulkWrite", JsonPrimitive("db1"), JsonArray(listOf(writeOpJson("doc1", "1-a")))))

        val response = ok(dispatcher.dispatch(request("info", JsonPrimitive("db1"))))
        val body = response.value.jsonObject
        assertEquals(1, body.getValue("docCount").jsonPrimitive.long.toInt())
        assertEquals(1L, body.getValue("updateSeq").jsonPrimitive.long)
    }

    @Test
    fun `getDoc with rev omitted returns the winning revision`() = runTest {
        val dispatcher = StorageDispatcher(InMemoryDocumentStore())
        dispatcher.dispatch(request("bulkWrite", JsonPrimitive("db1"), JsonArray(listOf(writeOpJson("doc1", "1-a")))))

        val response = ok(dispatcher.dispatch(request("getDoc", JsonPrimitive("db1"), JsonPrimitive("doc1"))))
        val body = response.value.jsonObject
        assertEquals("1-a", body.getValue("rev").jsonPrimitive.content)
        assertEquals("doc1", body.getValue("body").jsonObject.getValue("hello").jsonPrimitive.content)
    }

    @Test
    fun `getRevTrees returns the tree and winning rev per id`() = runTest {
        val dispatcher = StorageDispatcher(InMemoryDocumentStore())
        dispatcher.dispatch(request("bulkWrite", JsonPrimitive("db1"), JsonArray(listOf(writeOpJson("doc1", "1-a")))))

        val response = ok(dispatcher.dispatch(request("getRevTrees", JsonPrimitive("db1"), JsonArray(listOf(JsonPrimitive("doc1"))))))
        val entry = response.value.jsonArray.single().jsonObject
        assertEquals("tree:doc1:1-a", entry.getValue("tree").jsonPrimitive.content)
        assertEquals("1-a", entry.getValue("winningRev").jsonPrimitive.content)
    }

    @Test
    fun `bulkWrite returns one WriteResult per op with allocated sequences`() = runTest {
        val dispatcher = StorageDispatcher(InMemoryDocumentStore())
        val ops = JsonArray(listOf(writeOpJson("doc1", "1-a"), writeOpJson("doc2", "1-a")))

        val response = ok(dispatcher.dispatch(request("bulkWrite", JsonPrimitive("db1"), ops)))
        val results = response.value.jsonArray
        assertEquals(2, results.size)
        assertEquals("doc1", results[0].jsonObject.getValue("id").jsonPrimitive.content)
        assertEquals(1L, results[0].jsonObject.getValue("seq").jsonPrimitive.long)
    }

    @Test
    fun `allDocs with default options returns every non-deleted doc`() = runTest {
        val dispatcher = StorageDispatcher(InMemoryDocumentStore())
        dispatcher.dispatch(
            request("bulkWrite", JsonPrimitive("db1"), JsonArray(listOf(writeOpJson("doc1", "1-a"), writeOpJson("doc2", "1-a")))),
        )

        val response = ok(dispatcher.dispatch(request("allDocs", JsonPrimitive("db1"), JsonObject(emptyMap()))))
        val body = response.value.jsonObject
        assertEquals(2, body.getValue("totalRows").jsonPrimitive.long.toInt())
        assertEquals(2, body.getValue("rows").jsonArray.size)
    }

    @Test
    fun `changes returns writes since the given seq`() = runTest {
        val dispatcher = StorageDispatcher(InMemoryDocumentStore())
        dispatcher.dispatch(request("bulkWrite", JsonPrimitive("db1"), JsonArray(listOf(writeOpJson("doc1", "1-a")))))

        val response = ok(dispatcher.dispatch(request("changes", JsonPrimitive("db1"), JsonObject(mapOf("since" to JsonPrimitive(0L))))))
        val body = response.value.jsonObject
        assertEquals(1, body.getValue("results").jsonArray.size)
        assertEquals(1L, body.getValue("lastSeq").jsonPrimitive.long)
    }

    @Test
    fun `revsDiff reports revisions the store does not have`() = runTest {
        val dispatcher = StorageDispatcher(InMemoryDocumentStore())
        dispatcher.dispatch(request("bulkWrite", JsonPrimitive("db1"), JsonArray(listOf(writeOpJson("doc1", "1-a")))))

        val map = JsonObject(mapOf("doc1" to JsonArray(listOf(JsonPrimitive("1-a"), JsonPrimitive("1-b")))))
        val response = ok(dispatcher.dispatch(request("revsDiff", JsonPrimitive("db1"), map)))
        val missing = response.value.jsonObject.getValue("doc1").jsonObject.getValue("missing").jsonArray
        assertEquals(listOf("1-b"), missing.map { it.jsonPrimitive.content })
    }

    @Test
    fun `bulkGet returns the requested revisions`() = runTest {
        val dispatcher = StorageDispatcher(InMemoryDocumentStore())
        dispatcher.dispatch(request("bulkWrite", JsonPrimitive("db1"), JsonArray(listOf(writeOpJson("doc1", "1-a")))))

        val requests = JsonArray(listOf(JsonObject(mapOf("id" to JsonPrimitive("doc1")))))
        val response = ok(dispatcher.dispatch(request("bulkGet", JsonPrimitive("db1"), requests)))
        assertEquals(1, response.value.jsonArray.size)
        assertEquals("1-a", response.value.jsonArray.single().jsonObject.getValue("rev").jsonPrimitive.content)
    }

    @Test
    fun `compact removes the named revision`() = runTest {
        val store = InMemoryDocumentStore()
        val dispatcher = StorageDispatcher(store)
        dispatcher.dispatch(request("bulkWrite", JsonPrimitive("db1"), JsonArray(listOf(writeOpJson("doc1", "1-a")))))
        dispatcher.dispatch(request("bulkWrite", JsonPrimitive("db1"), JsonArray(listOf(writeOpJson("doc1", "1-b")))))

        val response = ok(
            dispatcher.dispatch(
                request(
                    "compact",
                    JsonPrimitive("db1"),
                    JsonPrimitive("doc1"),
                    JsonArray(listOf(JsonPrimitive("1-a"))),
                    JsonPrimitive("tree:doc1:compacted"),
                ),
            ),
        )
        assertEquals(JsonNull, response.value)

        val missing = err(dispatcher.dispatch(request("getDoc", JsonPrimitive("db1"), JsonPrimitive("doc1"), JsonPrimitive("1-a"))))
        assertEquals(BridgeErrorCode.NOT_FOUND, missing.error.code)
    }

    @Test
    fun `putLocal then getLocal round-trips the body`() = runTest {
        val dispatcher = StorageDispatcher(InMemoryDocumentStore())
        val body = JsonObject(mapOf("k" to JsonPrimitive("v")))

        val putResponse = ok(dispatcher.dispatch(request("putLocal", JsonPrimitive("db1"), JsonPrimitive("local1"), body)))
        assertEquals("1-local", putResponse.value.jsonPrimitive.content)

        val getResponse = ok(dispatcher.dispatch(request("getLocal", JsonPrimitive("db1"), JsonPrimitive("local1"))))
        assertEquals("v", getResponse.value.jsonObject.getValue("k").jsonPrimitive.content)
    }

    @Test
    fun `removeLocal clears the doc`() = runTest {
        val dispatcher = StorageDispatcher(InMemoryDocumentStore())
        dispatcher.dispatch(request("putLocal", JsonPrimitive("db1"), JsonPrimitive("local1"), JsonObject(emptyMap())))

        val removeResponse = ok(dispatcher.dispatch(request("removeLocal", JsonPrimitive("db1"), JsonPrimitive("local1"))))
        assertEquals(JsonNull, removeResponse.value)

        val getResponse = ok(dispatcher.dispatch(request("getLocal", JsonPrimitive("db1"), JsonPrimitive("local1"))))
        assertEquals(JsonNull, getResponse.value)
    }

    @Test
    fun `destroy empties the named db only`() = runTest {
        val dispatcher = StorageDispatcher(InMemoryDocumentStore())
        dispatcher.dispatch(request("bulkWrite", JsonPrimitive("db1"), JsonArray(listOf(writeOpJson("doc1", "1-a")))))
        dispatcher.dispatch(request("bulkWrite", JsonPrimitive("db2"), JsonArray(listOf(writeOpJson("doc1", "1-a")))))

        dispatcher.dispatch(request("destroy", JsonPrimitive("db1")))

        val info1 = ok(dispatcher.dispatch(request("info", JsonPrimitive("db1")))).value.jsonObject
        val info2 = ok(dispatcher.dispatch(request("info", JsonPrimitive("db2")))).value.jsonObject
        assertEquals(0, info1.getValue("docCount").jsonPrimitive.long.toInt())
        assertEquals(1, info2.getValue("docCount").jsonPrimitive.long.toInt())
    }

    @Test
    fun `close is a no-op that still returns Ok`() = runTest {
        val dispatcher = StorageDispatcher(InMemoryDocumentStore())
        val response = ok(dispatcher.dispatch(request("close", JsonPrimitive("db1"))))
        assertEquals(JsonNull, response.value)
    }

    @Test
    fun `getDoc for a missing doc maps NoSuchElementException to NOT_FOUND`() = runTest {
        val dispatcher = StorageDispatcher(InMemoryDocumentStore())
        val response = err(dispatcher.dispatch(request("getDoc", JsonPrimitive("db1"), JsonPrimitive("missing"))))
        assertEquals(BridgeErrorCode.NOT_FOUND, response.error.code)
    }

    @Test
    fun `a malformed arg maps to INVALID_ARGUMENT`() = runTest {
        val dispatcher = StorageDispatcher(InMemoryDocumentStore())
        // bulkWrite's second arg must be an array of WriteOp objects, not a bare string.
        val response = err(dispatcher.dispatch(request("bulkWrite", JsonPrimitive("db1"), JsonPrimitive("not-an-array"))))
        assertEquals(BridgeErrorCode.INVALID_ARGUMENT, response.error.code)
    }

    @Test
    fun `an unknown method name maps to INTERNAL`() = runTest {
        val dispatcher = StorageDispatcher(InMemoryDocumentStore())
        val response = err(dispatcher.dispatch(request("notAMethod", JsonPrimitive("db1"))))
        assertEquals(BridgeErrorCode.INTERNAL, response.error.code)
    }

    @Test
    fun `dispatch rethrows cancellation rather than mapping it to an error response`() = runTest {
        val gate = CompletableDeferred<Unit>()
        val dispatcher = StorageDispatcher(SuspendingUntil(gate))

        var caught: Throwable? = null
        val job = launch {
            try {
                dispatcher.dispatch(request("getDoc", JsonPrimitive("db1"), JsonPrimitive("doc1")))
            } catch (e: CancellationException) {
                caught = e
                throw e
            }
        }
        yield()
        job.cancelAndJoin()

        assertTrue("expected dispatch to propagate cancellation, got $caught", caught is CancellationException)
    }

    @Test
    fun `DISPATCHED_METHODS matches the 14 request-response methods on StorageCapability`() {
        // index.d.ts lines 300-343. subscribeChanges/getAttachment/putAttachment are
        // direct passthroughs instead — see StorageDispatcher's class doc.
        val expected = setOf(
            "info", "getDoc", "getRevTrees", "bulkWrite", "allDocs", "changes",
            "revsDiff", "bulkGet", "compact", "getLocal", "putLocal", "removeLocal",
            "destroy", "close",
        )
        assertEquals(expected, StorageDispatcher.DISPATCHED_METHODS)
    }

    private fun writeOpJson(id: String, rev: String) = JsonObject(
        mapOf(
            "id" to JsonPrimitive(id),
            "rev" to JsonPrimitive(rev),
            "tree" to JsonPrimitive("tree:$id:$rev"),
            "winningRev" to JsonPrimitive(rev),
            "deleted" to JsonPrimitive(false),
            "body" to JsonObject(mapOf("hello" to JsonPrimitive(id))),
        ),
    )

    /** Suspends [DocumentStore.getDoc] on [gate] forever; every other member is
     * unused by the one test that needs a store which never resolves on its own —
     * [InMemoryDocumentStore] resolves synchronously and can't exercise cancellation
     * mid-flight. */
    private class SuspendingUntil(private val gate: CompletableDeferred<Unit>) : DocumentStore {
        override suspend fun info(db: String): StoreInfo = error("unused")
        override suspend fun getDoc(db: String, id: String, rev: String?): StoredDoc {
            gate.await()
            error("unreachable")
        }
        override suspend fun getRevTrees(db: String, ids: List<String>): List<RevTreeEntry> = error("unused")
        override suspend fun bulkWrite(db: String, ops: List<WriteOp>): List<WriteResult> = error("unused")
        override suspend fun allDocs(db: String, options: AllDocsOptions): AllDocsResult = error("unused")
        override suspend fun changes(db: String, options: ChangesOptions): ChangesResult = error("unused")
        override fun subscribeChanges(db: String, since: Long): Flow<StoredDoc> = error("unused")
        override suspend fun revsDiff(db: String, revsByDocId: Map<String, List<String>>): Map<String, RevsDiffEntry> = error("unused")
        override suspend fun bulkGet(db: String, requests: List<BulkGetRequest>): List<StoredDoc> = error("unused")
        override suspend fun compact(db: String, id: String, revs: List<String>, tree: OpaqueRevTree) = error("unused")
        override suspend fun getLocal(db: String, id: String): Map<String, Any?>? = error("unused")
        override suspend fun putLocal(db: String, id: String, doc: Map<String, Any?>): String = error("unused")
        override suspend fun removeLocal(db: String, id: String) = error("unused")
        override suspend fun getAttachment(db: String, digest: String): ByteArray = error("unused")
        override suspend fun putAttachment(db: String, digest: String, data: ByteArray) = error("unused")
        override suspend fun destroy(db: String) = error("unused")
        override suspend fun close(db: String) = error("unused")
    }
}
