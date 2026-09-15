package com.posthog.hoglake.api

import com.fasterxml.jackson.annotation.JsonInclude
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
 * Defined once because it was defined three times — App.module plus two
 * tests, each claiming in a comment to mirror it "exactly". A fuzz target
 * that configures its own mapper is only fuzzing production while those
 * comments stay true, and nothing made them stay true.
 *
 * [KotlinFeature.StrictNullChecks] is the load-bearing one. Kotlin's
 * non-null ELEMENT types (`List<AlterOpDto>`) are erased at the Jackson
 * boundary by default, so a JSON `null` inside such a list binds happily
 * and only fails later, when the mapping code dereferences it — a bare
 * NullPointerException from the route handler, which ErrorMapping does
 * not map, i.e. a 500 on a malformed request. Five request lists had that
 * hole (`ops`, `fields`, `sort_fields`, `appends`, `deletes`).
 *
 * With the feature on, Jackson refuses the null during binding with an
 * InvalidNullException naming the property, inside ContentNegotiation,
 * so the wire answer is a 400 with a usable message. Element types that
 * are DECLARED nullable — `partition_values: List<String?>`, where a null
 * partition value is a real value — are unaffected.
 */
fun ObjectMapper.configureHoglakeWire(): ObjectMapper =
    apply {
        registerKotlinModule { enable(KotlinFeature.StrictNullChecks) }
        registerModule(JavaTimeModule())
        propertyNamingStrategy = PropertyNamingStrategies.SNAKE_CASE
        disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        setSerializationInclusion(JsonInclude.Include.NON_NULL)
    }

/** A standalone mapper carrying [configureHoglakeWire], for tests and tools. */
fun wireObjectMapper(): ObjectMapper = ObjectMapper().configureHoglakeWire()
