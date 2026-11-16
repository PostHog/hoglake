package com.posthog.hoglake.store

import com.fasterxml.jackson.core.JacksonException
import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.core.StreamReadFeature
import com.fasterxml.jackson.core.StreamWriteFeature
import com.fasterxml.jackson.core.async.ByteArrayFeeder
import java.io.ByteArrayOutputStream

/**
 * The one change an alias makes to a remote schema's answer
 * ([MountHostClient.schemaNamespace], [MountHostClient.schemaTable]), made
 * while the answer streams: [field] of the top-level JSON object must be the
 * string [from], the schema's name on its host, and becomes [to], its local
 * name. Every other token is copied as it was read, a number by its text, so
 * `1e10`, `-0.0` and `1.10` stay as the host wrote them. Whitespace is
 * dropped and string escapes are written anew; no value changes.
 *
 * Refused, with what [refuse] makes of the reason: anything but one JSON
 * object, a key twice in one object, anything after the object, and [field]
 * missing or not [from]. So is an answer past Jackson's default limits (a
 * string of 20,000,000 characters, a number of 1,000 digits, 1,000 levels).
 * Fed a chunk at a time ([feed], then [end]), each call returning the bytes
 * it completed, so it holds one chunk's output and one token at most, never
 * the answer. Not thread-safe; one per answer.
 */
internal class NamespaceFieldRewrite(
    private val field: String,
    private val from: String,
    private val to: String,
    private val refuse: (what: String) -> Throwable,
) : AutoCloseable {
    private val parser = FACTORY.createNonBlockingByteArrayParser()
    private val feeder = parser.nonBlockingInputFeeder as ByteArrayFeeder
    private val output = ByteArrayOutputStream()
    private val generator = FACTORY.createGenerator(output)
    private var depth = 0
    private var atField = false
    private var rewritten = false
    private var done = false

    /** The last byte fed that is not JSON whitespace; whether it was a comma before this chunk; no token yet. */
    private var last: Byte = 0
    private var afterComma = false
    private var chunkStart = false
    private var previous: JsonToken? = null

    /** Parse [length] bytes of [bytes] from [offset]; returns the rewritten bytes they complete. */
    fun feed(
        bytes: ByteArray,
        offset: Int = 0,
        length: Int = bytes.size,
    ): ByteArray {
        afterComma = last == COMMA
        chunkStart = true
        val end = (offset + length downTo offset + 1).firstOrNull { bytes[it - 1] !in WHITESPACE }
        if (end != null) last = bytes[end - 1]
        return drain { feeder.feedInput(bytes, offset, offset + length) }
    }

    /** The answer has ended: the bytes still to send, once the object is known to be whole. */
    fun end(): ByteArray {
        val rest = drain { feeder.endOfInput() }
        if (!done) throw refuse("a body that is not a whole JSON object")
        return rest
    }

    private inline fun drain(input: () -> Unit): ByteArray {
        try {
            input()
            while (true) {
                val token = parser.nextToken()
                if (token == null || token == JsonToken.NOT_AVAILABLE) break
                copy(token)
            }
            generator.flush()
        } catch (e: JacksonException) {
            // The parser's message quotes the host's bytes; only its kind goes on.
            throw refuse("a body that is not valid JSON (${e.javaClass.simpleName})")
        }
        return output.toByteArray().also { output.reset() }
    }

    private fun copy(token: JsonToken) {
        if (done || (depth == 0 && token != JsonToken.START_OBJECT)) throw refuse("a body that is not one JSON object")
        // Jackson's async parser closes `{"a":1,` and `{"a":` with a `}` from the next chunk, where in one chunk it
        // refuses them. The first leaves a `}` as a chunk's first token after a comma; the second, a key with no value.
        if (token == JsonToken.END_OBJECT && (chunkStart && afterComma || previous == JsonToken.FIELD_NAME)) {
            throw refuse("a body that is not valid JSON (an object ends after a comma or a key)")
        }
        chunkStart = false
        previous = token
        when {
            atField -> {
                if (token != JsonToken.VALUE_STRING || parser.text != from) {
                    throw refuse("an object whose $field is not the schema's name on the host")
                }
                generator.writeString(to)
                rewritten = true
            }
            token.isNumeric -> generator.writeNumber(parser.text)
            else -> generator.copyCurrentEvent(parser)
        }
        atField = token == JsonToken.FIELD_NAME && depth == 1 && parser.currentName() == field
        if (token.isStructStart) depth++
        if (token.isStructEnd) depth--
        if (token.isStructEnd && depth == 0) {
            if (!rewritten) throw refuse("an object without $field")
            done = true
        }
    }

    override fun close() {
        parser.close()
        generator.close()
    }

    private companion object {
        const val COMMA = ','.code.toByte()
        val WHITESPACE = " \t\r\n".map { it.code.toByte() }

        /** Each key once in every object; nothing written on close that the answer did not hold. */
        val FACTORY: JsonFactory =
            JsonFactory.builder()
                .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                .disable(StreamWriteFeature.AUTO_CLOSE_CONTENT)
                .build()
    }
}
