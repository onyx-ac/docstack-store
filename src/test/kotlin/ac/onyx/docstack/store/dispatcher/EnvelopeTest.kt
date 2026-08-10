package ac.onyx.docstack.store.dispatcher

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.Assert.assertEquals
import org.junit.Test

/**
 * [BridgeResponse]'s wire shape must match `permetic-web/src/index.d.ts`'s discriminated
 * union exactly - `{v, id, ok: true, value}` / `{v, id, ok: false, error}`, a boolean `ok`
 * field, not kotlinx.serialization's default `type`-tagged polymorphism. Found and fixed
 * while designing spec 04's `HeadlessCarrier` (needs to cross this through Zipline).
 */
public class EnvelopeTest {

    @Test
    fun `Ok encodes with a boolean ok discriminator, not a type tag`() {
        val response: BridgeResponse = BridgeResponse.Ok(v = 1, id = "req-1", value = JsonPrimitive("hi"))
        val json = Json.encodeToJsonElement(BridgeResponseSerializer, response).jsonObject

        assertEquals(JsonPrimitive(1), json["v"])
        assertEquals(JsonPrimitive("req-1"), json["id"])
        assertEquals(JsonPrimitive(true), json["ok"])
        assertEquals(JsonPrimitive("hi"), json["value"])
        assertEquals(null, json["type"])
    }

    @Test
    fun `Err encodes with a boolean ok discriminator, not a type tag`() {
        val response: BridgeResponse = BridgeResponse.Err(
            v = 1,
            id = "req-2",
            error = BridgeError(code = BridgeErrorCode.NOT_FOUND, message = "missing"),
        )
        val json = Json.encodeToJsonElement(BridgeResponseSerializer, response).jsonObject

        assertEquals(JsonPrimitive(false), json["ok"])
        assertEquals("NOT_FOUND", json["error"]!!.jsonObject["code"]!!.jsonPrimitive.content)
        assertEquals(null, json["type"])
    }

    @Test
    fun `Ok and Err round-trip through encode then decode`() {
        val ok: BridgeResponse = BridgeResponse.Ok(v = 1, id = "req-1", value = JsonPrimitive(42))
        val err: BridgeResponse = BridgeResponse.Err(
            v = 1,
            id = "req-2",
            error = BridgeError(code = BridgeErrorCode.CONFLICT, message = "boom"),
        )

        val decodedOk = Json.decodeFromString(BridgeResponseSerializer, Json.encodeToString(BridgeResponseSerializer, ok))
        val decodedErr = Json.decodeFromString(BridgeResponseSerializer, Json.encodeToString(BridgeResponseSerializer, err))

        assertEquals(ok, decodedOk)
        assertEquals(err, decodedErr)
    }
}
