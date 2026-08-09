package ac.onyx.docstack.store.dispatcher

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.doubleOrNull

/**
 * Doc bodies are `Map<String, Any?>` ([ac.onyx.docstack.store.DocumentStore] never
 * interprets their structure), so they cannot round-trip through
 * kotlinx.serialization's data-class (de)serialization the way [BridgeError] or
 * [BridgeRequest] do. These two functions are the manual bridge between arbitrary
 * JSON and the `Any?` shape [DocumentStore]'s methods actually take and return.
 */
public fun JsonElement.toKotlin(): Any? = when (this) {
    is JsonNull -> null
    is JsonObject -> entries.associate { (key, value) -> key to value.toKotlin() }
    is JsonArray -> map { it.toKotlin() }
    is JsonPrimitive -> when {
        !isString && this.booleanOrNull != null -> booleanOrNull
        !isString && this.longOrNull != null -> longOrNull
        !isString && this.doubleOrNull != null -> doubleOrNull
        else -> content
    }
}

@Suppress("UNCHECKED_CAST")
public fun Any?.toJsonElement(): JsonElement = when (this) {
    null -> JsonNull
    is JsonElement -> this
    is String -> JsonPrimitive(this)
    is Boolean -> JsonPrimitive(this)
    is Int -> JsonPrimitive(this)
    is Long -> JsonPrimitive(this)
    is Double -> JsonPrimitive(this)
    is Float -> JsonPrimitive(this)
    is Map<*, *> -> JsonObject((this as Map<String, Any?>).mapValues { (_, v) -> v.toJsonElement() })
    is List<*> -> JsonArray(map { it.toJsonElement() })
    else -> throw IllegalArgumentException("cannot encode value of type ${this::class} to JSON")
}
