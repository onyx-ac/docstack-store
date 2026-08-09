package ac.onyx.docstack.store.dispatcher

import ac.onyx.docstack.store.AllDocsOptions
import ac.onyx.docstack.store.AllDocsResult
import ac.onyx.docstack.store.BulkGetRequest
import ac.onyx.docstack.store.ChangesOptions
import ac.onyx.docstack.store.ChangesResult
import ac.onyx.docstack.store.DocumentStore
import ac.onyx.docstack.store.RevTreeEntry
import ac.onyx.docstack.store.RevsDiffEntry
import ac.onyx.docstack.store.StoreInfo
import ac.onyx.docstack.store.StoredDoc
import ac.onyx.docstack.store.WriteOp
import ac.onyx.docstack.store.WriteResult
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.Flow
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long

/**
 * The one dispatcher on the Kotlin side (ADR-0002): turns a carrier-agnostic
 * [BridgeRequest] into a [DocumentStore] call and a [BridgeResponse]. Shared,
 * unmodified, by the WebView carrier (`docstack-permetic`) and the headless carrier
 * (`docstack-headless`) — this class has no carrier branch in it.
 *
 * [dispatch] handles the 14 [DocumentStore] methods that fit the plain
 * request/response envelope shape. `subscribeChanges`/`getAttachment`/
 * `putAttachment` don't: subscriptions stream unsolicited [BridgeEvent]s over time
 * (subscription-id allocation and cancellation-by-id are spec 01 task 2's job, a
 * different module) and attachment bodies use a binary side-channel, not JSON
 * (ADR-0002's carve-out; the wire format is spec 04 task 2's job). Those three are
 * exposed here as direct typed passthroughs so this dispatcher is still the single
 * object mirroring the whole `StorageCapability` surface, without inventing
 * bookkeeping that belongs to modules this one must not depend on.
 */
public class StorageDispatcher(private val store: DocumentStore) {

    public companion object {
        /** The methods [dispatch] handles. Everything else in `StorageCapability`
         * (`subscribeChanges`, `getAttachment`, `putAttachment`) is a direct
         * passthrough on this class instead. See `index.d.ts` lines 300-343. */
        public val DISPATCHED_METHODS: Set<String> = setOf(
            "info", "getDoc", "getRevTrees", "bulkWrite", "allDocs", "changes",
            "revsDiff", "bulkGet", "compact", "getLocal", "putLocal", "removeLocal",
            "destroy", "close",
        )
    }

    public suspend fun dispatch(request: BridgeRequest): BridgeResponse {
        val value: JsonElement
        try {
            value = when (request.method) {
                "info" -> encodeStoreInfo(store.info(request.args[0].asString()))

                "getDoc" -> encodeStoredDoc(
                    store.getDoc(request.args[0].asString(), request.args[1].asString(), request.args.getOrNull(2).asStringOrNull()),
                )

                "getRevTrees" -> JsonArray(
                    store.getRevTrees(request.args[0].asString(), request.args[1].asStringList())
                        .map { encodeRevTreeEntry(it) },
                )

                "bulkWrite" -> JsonArray(
                    store.bulkWrite(request.args[0].asString(), request.args[1].jsonArray.map { decodeWriteOp(it) })
                        .map { encodeWriteResult(it) },
                )

                "allDocs" -> encodeAllDocsResult(
                    store.allDocs(request.args[0].asString(), decodeAllDocsOptions(request.args[1])),
                )

                "changes" -> encodeChangesResult(
                    store.changes(request.args[0].asString(), decodeChangesOptions(request.args[1])),
                )

                "revsDiff" -> encodeRevsDiff(
                    store.revsDiff(request.args[0].asString(), decodeRevsByDocId(request.args[1])),
                )

                "bulkGet" -> JsonArray(
                    store.bulkGet(request.args[0].asString(), decodeBulkGetRequests(request.args[1]))
                        .map { encodeStoredDoc(it) },
                )

                "compact" -> {
                    store.compact(
                        request.args[0].asString(),
                        request.args[1].asString(),
                        request.args[2].asStringList(),
                        request.args[3].asString(),
                    )
                    JsonNull
                }

                "getLocal" -> store.getLocal(request.args[0].asString(), request.args[1].asString()).toJsonElement()

                "putLocal" -> JsonPrimitive(
                    store.putLocal(
                        request.args[0].asString(),
                        request.args[1].asString(),
                        request.args[2].asBodyOrNull() ?: emptyMap(),
                    ),
                )

                "removeLocal" -> {
                    store.removeLocal(request.args[0].asString(), request.args[1].asString())
                    JsonNull
                }

                "destroy" -> {
                    store.destroy(request.args[0].asString())
                    JsonNull
                }

                "close" -> {
                    store.close(request.args[0].asString())
                    JsonNull
                }

                else -> return BridgeResponse.Err(
                    request.v,
                    request.id,
                    BridgeError(
                        BridgeErrorCode.INTERNAL,
                        "unknown storage method: ${request.method} (contract drift — a client on a matching contract version never sends this)",
                    ),
                )
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: NoSuchElementException) {
            return BridgeResponse.Err(request.v, request.id, BridgeError(BridgeErrorCode.NOT_FOUND, e.message ?: "not found"))
        } catch (e: IllegalArgumentException) {
            return BridgeResponse.Err(request.v, request.id, BridgeError(BridgeErrorCode.INVALID_ARGUMENT, e.message ?: "invalid argument"))
        } catch (e: Exception) {
            return BridgeResponse.Err(request.v, request.id, BridgeError(BridgeErrorCode.INTERNAL, e.message ?: "internal error"))
        }
        return BridgeResponse.Ok(request.v, request.id, value)
    }

    /** Streams live changes. Not part of [dispatch] — see class doc. */
    public fun subscribeChanges(db: String, since: Long): Flow<StoredDoc> = store.subscribeChanges(db, since)

    /** Binary side-channel, not the JSON envelope (ADR-0002). Not part of [dispatch]. */
    public suspend fun getAttachment(db: String, digest: String): ByteArray = store.getAttachment(db, digest)

    /** Binary side-channel, not the JSON envelope (ADR-0002). Not part of [dispatch]. */
    public suspend fun putAttachment(db: String, digest: String, data: ByteArray): Unit = store.putAttachment(db, digest, data)
}

/* -------------------------------------------------------------------------- */
/* Arg decode helpers                                                          */
/* -------------------------------------------------------------------------- */

private fun JsonElement.asString(): String = jsonPrimitive.content

private fun JsonElement?.asStringOrNull(): String? = if (this == null || this is JsonNull) null else jsonPrimitive.content

private fun JsonElement.asStringList(): List<String> = jsonArray.map { it.asString() }

private fun JsonElement?.asStringListOrNull(): List<String>? = if (this == null || this is JsonNull) null else asStringList()

private fun JsonElement?.asBooleanOr(default: Boolean): Boolean = if (this == null || this is JsonNull) default else jsonPrimitive.boolean

private fun JsonElement?.asIntOrNull(): Int? = if (this == null || this is JsonNull) null else jsonPrimitive.int

private fun JsonElement?.asIntOr(default: Int): Int = asIntOrNull() ?: default

private fun JsonElement?.asBodyOrNull(): Map<String, Any?>? {
    if (this == null || this is JsonNull) return null
    @Suppress("UNCHECKED_CAST")
    return toKotlin() as Map<String, Any?>
}

/** [JsonObject.get] throws nothing on a missing key; a decoder needs the field to
 * exist. Distinct from [NoSuchElementException] on purpose — a malformed request is
 * [BridgeErrorCode.INVALID_ARGUMENT], not [BridgeErrorCode.NOT_FOUND]. */
private fun JsonObject.require(key: String): JsonElement = this[key] ?: throw IllegalArgumentException("missing required field: $key")

private fun decodeWriteOp(element: JsonElement): WriteOp {
    val o = element.jsonObject
    return WriteOp(
        id = o.require("id").asString(),
        rev = o.require("rev").asString(),
        tree = o.require("tree").asString(),
        winningRev = o.require("winningRev").asString(),
        deleted = o.require("deleted").jsonPrimitive.boolean,
        body = o["body"].asBodyOrNull(),
        attachmentDigests = o["attachmentDigests"].asStringListOrNull(),
    )
}

private fun decodeAllDocsOptions(element: JsonElement): AllDocsOptions {
    val o = element.jsonObject
    return AllDocsOptions(
        startkey = o["startkey"].asStringOrNull(),
        endkey = o["endkey"].asStringOrNull(),
        inclusiveEnd = o["inclusiveEnd"].asBooleanOr(true),
        keys = o["keys"].asStringListOrNull(),
        limit = o["limit"].asIntOrNull(),
        skip = o["skip"].asIntOr(0),
        descending = o["descending"].asBooleanOr(false),
        includeBody = o["includeBody"].asBooleanOr(false),
        includeConflicts = o["includeConflicts"].asBooleanOr(false),
        deleted = o["deleted"].asBooleanOr(false),
    )
}

private fun decodeChangesOptions(element: JsonElement): ChangesOptions {
    val o = element.jsonObject
    return ChangesOptions(
        since = o["since"]?.takeIf { it !is JsonNull }?.jsonPrimitive?.long ?: 0,
        limit = o["limit"].asIntOrNull(),
        descending = o["descending"].asBooleanOr(false),
        includeBody = o["includeBody"].asBooleanOr(false),
        docIds = o["docIds"].asStringListOrNull(),
    )
}

private fun decodeBulkGetRequests(element: JsonElement): List<BulkGetRequest> = element.jsonArray.map {
    val o = it.jsonObject
    BulkGetRequest(id = o.require("id").asString(), rev = o["rev"].asStringOrNull())
}

private fun decodeRevsByDocId(element: JsonElement): Map<String, List<String>> =
    element.jsonObject.entries.associate { (id, revs) -> id to revs.asStringList() }

/* -------------------------------------------------------------------------- */
/* Result encode helpers                                                       */
/* -------------------------------------------------------------------------- */

private fun encodeStoreInfo(info: StoreInfo): JsonElement = JsonObject(
    mapOf("docCount" to JsonPrimitive(info.docCount), "updateSeq" to JsonPrimitive(info.updateSeq)),
)

private fun encodeStoredDoc(doc: StoredDoc): JsonElement = JsonObject(
    mapOf(
        "id" to JsonPrimitive(doc.id),
        "rev" to JsonPrimitive(doc.rev),
        "seq" to JsonPrimitive(doc.seq),
        "deleted" to JsonPrimitive(doc.deleted),
        "body" to doc.body.toJsonElement(),
        "conflicts" to doc.conflicts.toJsonElement(),
    ),
)

private fun encodeRevTreeEntry(entry: RevTreeEntry): JsonElement = JsonObject(
    mapOf(
        "id" to JsonPrimitive(entry.id),
        "tree" to entry.tree.toJsonElement(),
        "winningRev" to entry.winningRev.toJsonElement(),
        "seq" to JsonPrimitive(entry.seq),
        "deleted" to JsonPrimitive(entry.deleted),
    ),
)

private fun encodeWriteResult(result: WriteResult): JsonElement = JsonObject(
    mapOf("id" to JsonPrimitive(result.id), "rev" to JsonPrimitive(result.rev), "seq" to JsonPrimitive(result.seq)),
)

private fun encodeAllDocsResult(result: AllDocsResult): JsonElement = JsonObject(
    mapOf(
        "totalRows" to JsonPrimitive(result.totalRows),
        "offset" to JsonPrimitive(result.offset),
        "rows" to JsonArray(result.rows.map { encodeStoredDoc(it) }),
    ),
)

private fun encodeChangesResult(result: ChangesResult): JsonElement = JsonObject(
    mapOf(
        "lastSeq" to JsonPrimitive(result.lastSeq),
        "results" to JsonArray(result.results.map { encodeStoredDoc(it) }),
    ),
)

private fun encodeRevsDiff(diff: Map<String, RevsDiffEntry>): JsonElement = JsonObject(
    diff.mapValues { (_, entry) ->
        JsonObject(
            buildMap {
                put("missing", JsonArray(entry.missing.map { JsonPrimitive(it) }))
                entry.possibleAncestors?.let { put("possibleAncestors", JsonArray(it.map { a -> JsonPrimitive(a) })) }
            },
        )
    },
)
