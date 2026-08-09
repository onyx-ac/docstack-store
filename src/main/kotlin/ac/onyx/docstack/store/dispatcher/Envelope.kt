package ac.onyx.docstack.store.dispatcher

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement

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
