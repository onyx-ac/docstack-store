package ac.onyx.docstack.store.dispatcher

import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.descriptors.element
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.JsonDecoder
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonEncoder
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.decodeFromJsonElement
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put

/**
 * Local mirror of the Transport section of `permetic-web/src/index.d.ts`. No shared
 * Kotlin module for the contract exists yet and `docstack-store` must not depend on
 * `permetic-core` to borrow its copy, so this is hand-mirrored under the same
 * "contract drift is a compile error" discipline as [ac.onyx.docstack.store.DocumentStore]
 * mirrors `StorageCapability`. `docstack-permetic` reconciles this copy with
 * `permetic-core`'s at the boundary once that module exists.
 */
public const val CONTRACT_VERSION: Int = 1

@Serializable
public data class BridgeRequest(
    public val v: Int,
    /** Correlation id, unique per in-flight request. */
    public val id: String,
    public val capability: String,
    public val method: String,
    public val args: List<JsonElement>,
)

/**
 * Wire shape is `permetic-web/src/index.d.ts`'s `{v, id, ok: true, value} | {v, id, ok:
 * false, error}` - a boolean `ok` discriminator, not a `type` tag. kotlinx.serialization's
 * default sealed-interface polymorphism produces a `type` wrapper instead, so this needs
 * [BridgeResponseSerializer] rather than a bare `@Serializable` on the sealed interface.
 */
@Serializable(with = BridgeResponseSerializer::class)
public sealed interface BridgeResponse {
    public val v: Int
    public val id: String

    public data class Ok(
        override val v: Int,
        override val id: String,
        public val value: JsonElement,
    ) : BridgeResponse

    public data class Err(
        override val v: Int,
        override val id: String,
        public val error: BridgeError,
    ) : BridgeResponse
}

public object BridgeResponseSerializer : KSerializer<BridgeResponse> {
    override val descriptor: SerialDescriptor =
        buildClassSerialDescriptor("ac.onyx.docstack.store.dispatcher.BridgeResponse") {
            element<Int>("v")
            element<String>("id")
            element<Boolean>("ok")
            element("value", JsonElement.serializer().descriptor, isOptional = true)
            element("error", BridgeError.serializer().descriptor, isOptional = true)
        }

    override fun serialize(encoder: Encoder, value: BridgeResponse) {
        val jsonEncoder = encoder as? JsonEncoder
            ?: error("BridgeResponse can only be serialized to JSON")
        val element = when (value) {
            is BridgeResponse.Ok -> buildJsonObject {
                put("v", value.v)
                put("id", value.id)
                put("ok", true)
                put("value", value.value)
            }
            is BridgeResponse.Err -> buildJsonObject {
                put("v", value.v)
                put("id", value.id)
                put("ok", false)
                put("error", jsonEncoder.json.encodeToJsonElement(BridgeError.serializer(), value.error))
            }
        }
        jsonEncoder.encodeJsonElement(element)
    }

    override fun deserialize(decoder: Decoder): BridgeResponse {
        val jsonDecoder = decoder as? JsonDecoder
            ?: error("BridgeResponse can only be deserialized from JSON")
        val element = jsonDecoder.decodeJsonElement().jsonObject
        val v = element.getValue("v").jsonPrimitive.content.toInt()
        val id = element.getValue("id").jsonPrimitive.content
        val ok = element.getValue("ok").jsonPrimitive.content.toBoolean()
        return if (ok) {
            BridgeResponse.Ok(v, id, element.getValue("value"))
        } else {
            val error = jsonDecoder.json.decodeFromJsonElement(BridgeError.serializer(), element.getValue("error"))
            BridgeResponse.Err(v, id, error)
        }
    }
}

@Serializable
public data class BridgeError(
    public val code: BridgeErrorCode,
    public val message: String,
    /** Never contains a stack trace in release builds. */
    public val details: Map<String, JsonElement>? = null,
)

@Serializable
public enum class BridgeErrorCode {
    UNAVAILABLE,
    NOT_FOUND,
    CONFLICT,
    UNAUTHENTICATED,
    PERMISSION_DENIED,
    CANCELLED,
    NETWORK,
    INVALID_ARGUMENT,
    INTERNAL,
}
