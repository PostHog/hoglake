package com.posthog.hoglake

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.StreamWriteFeature
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinFeature
import com.fasterxml.jackson.module.kotlin.registerKotlinModule

/**
 * The one wire JSON configuration: snake_case with ISO-8601 date-times
 * and base64 byte fields, per openapi/hoglake.yaml.
 *
 * Defined once because it was defined five times — App.module, three
 * tests, and the maintenance-run ledger, each claiming in a comment to
 * mirror the others "exactly". None of those comments was enforced and
 * two had already drifted: a fuzz target that configures its own mapper
 * only fuzzes production while its comment stays true, and the ledger's
 * copy had lost the ISO-8601 date setting it claimed to have.
 *
 * It lives at the top level rather than in `api` because it is not only
 * the API's: the maintenance ledger stores wire-shaped payloads on
 * purpose, and persistence must not have to import from `api` to say so.
 *
 * [KotlinFeature.StrictNullChecks] is the load-bearing one. Kotlin's
 * non-null ELEMENT types (`List<AlterOpDto>`) are erased at the Jackson
 * boundary by default, so a JSON `null` inside such a list binds happily
 * and only fails later, when the mapping code dereferences it — a bare
 * NullPointerException from the route handler, which ErrorMapping does
 * not map, i.e. a 500 on a malformed request.
 *
 * The rule, not the list, is the thing to remember: EVERY request
 * collection with a non-null element type that is mapped after binding
 * had this shape. At the time of the fix that was nine, across three
 * endpoints — `ops`, `fields`, `sort_fields` (alter); `appends`,
 * `deletes`, and the nested `files` of each (commit); `column_stats`
 * (nested in a file registration); and `columns` (create table). They
 * are listed to show the spread, not as an inventory to maintain: the
 * setting is global, so a tenth added tomorrow is covered the day it is
 * added, and nothing here needs updating.
 *
 * With the feature on, Jackson refuses the null during binding with an
 * InvalidNullException naming the property, inside ContentNegotiation,
 * so the wire answer is a 400 with a usable message. Element types that
 * are DECLARED nullable are unaffected — `partition_values:
 * List<String?>`, where a null partition value is a real value, and
 * `type_params: Map<String, Any?>`.
 */
fun ObjectMapper.configureHoglakeWire(): ObjectMapper =
    apply {
        registerKotlinModule { enable(KotlinFeature.StrictNullChecks) }
        registerModule(JavaTimeModule())
        propertyNamingStrategy = PropertyNamingStrategies.SNAKE_CASE
        disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        setSerializationInclusion(JsonInclude.Include.NON_NULL)
        // Decimal bound tokens are PLAIN notation at every scale
        // (BigDecimal.toPlainString): BigDecimal.toString would flip a
        // decimal(38,38) bound to "1E-38", but the documented convention
        // (openapi FileColumnStats: "unscaled x 10^-scale, e.g. '1.50'")
        // is a plain JSON number token, and the cross-language wire
        // oracle renders plain tokens too. BigDecimal is the only wire
        // type this touches — every other number serializes unchanged.
        factory.enable(StreamWriteFeature.WRITE_BIGDECIMAL_AS_PLAIN.mappedFeature())
    }

/**
 * Built once, not per call. An ObjectMapper is thread-safe after
 * configuration, and building one costs a Kotlin-module reflection scan —
 * which the commit path paid on EVERY idempotent commit, because
 * commitFingerprint asked for a fresh mapper each time. Treat the
 * returned instance as immutable; a caller that needs different settings
 * adds its own instance here, next to this one.
 */
private val WIRE_MAPPER: ObjectMapper = ObjectMapper().configureHoglakeWire()

/** The wire mapper carrying [configureHoglakeWire], for services, tests and tools. */
fun wireObjectMapper(): ObjectMapper = WIRE_MAPPER

/**
 * The same wire shape, but tolerant of properties it does not know.
 *
 * Request parsing must stay STRICT: an unknown property in a request body
 * is a client error and has to surface as a 400, so [wireObjectMapper] —
 * which the API uses — keeps Jackson's default
 * FAIL_ON_UNKNOWN_PROPERTIES.
 *
 * A payload this service STORED is a different thing. A commit receipt's
 * `request` column was written by some replica of this service, and
 * during a rolling deploy that replica may be running a NEWER version
 * than the one reading it back: two API replicas, one already carrying a
 * field the other has never heard of. Decoding such a receipt strictly
 * throws UnrecognizedPropertyException inside the commit tail, under the
 * per-catalog lock, which the error mapping turns into a 500 — the
 * replay contract broken by nothing worse than a deploy being half done.
 * Stored payloads therefore decode leniently: an unknown property is a
 * newer replica's field, and skipping it leaves the known fields — which
 * is exactly what the fingerprint compares.
 */
private val STORED_PAYLOAD_MAPPER: ObjectMapper =
    ObjectMapper().configureHoglakeWire()
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)

/** Decoder for payloads this service wrote (see [STORED_PAYLOAD_MAPPER]), never for request bodies. */
fun storedPayloadObjectMapper(): ObjectMapper = STORED_PAYLOAD_MAPPER
