package com.posthog.hoglake.fuzz

import com.code_intelligence.jazzer.api.FuzzedDataProvider
import com.code_intelligence.jazzer.junit.FuzzTest
import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.core.StreamReadFeature
import com.posthog.hoglake.store.NamespaceFieldRewrite
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.util.Random

/**
 * Fuzz target (docs/fuzzing.md layer 4): the one-field rewrite an alias
 * makes to a remote schema's answer ([NamespaceFieldRewrite]), fed in
 * chunks of 1 to 64 bytes as a host's body arrives.
 *
 * Contracts under test, each against ground truth:
 *
 *  - **generated answers.** A model built from the input, written out with
 *    random whitespace and random `\u` escapes (a key or a value spelled
 *    with escapes is still that key or value). Accepted exactly when the
 *    model is one object, with no key twice in any object, whose top-level
 *    `name` is the string `shared`; the output's tokens are then the
 *    model's, a number by its text, with that one value `posthog`. Nested
 *    `name`s, other strings that hold `shared`, and numbers a re-encoding
 *    would change (`1e10`, `-0.0`, `1.10`) are where a wrong rewrite shows.
 *  - **arbitrary bytes.** Refused only with the rewrite's own refusal,
 *    whose reason is one of its fixed phrases (never a parser message,
 *    which quotes the host's bytes). Accepted only when Jackson's blocking
 *    parser reads the bytes as one such object, and then the output's
 *    tokens are the input's with that one value changed. Bytes it reads so
 *    are never refused; checked for bytes with no zero, since a zero is how
 *    the blocking parser comes to read another encoding.
 *
 * A refused answer's output is dropped whole, as the relay drops it (the
 * 502 before the response starts, the cut connection after), so the
 * output judged here is always a whole answer.
 *
 * **Input layout.** Two bytes off the front: the mode (low bit: 0 a
 * generated answer, 1 arbitrary bytes) and the seed of the chunk sizes.
 * The rest is the generator's tape, or the answer itself.
 */
class NamespaceFieldRewriteFuzzTest {
    @FuzzTest(maxDuration = "60s")
    fun rewriteChangesOnlyTheTopLevelField(data: FuzzedDataProvider) {
        val header = data.consumeBytes(2)
        if (header.size < 2) return
        val chunks = Random(header[1].toLong())
        val rest = data.consumeRemainingAsBytes()
        if (header[0].toInt() and 1 == 0) {
            generated(Tape(rest), chunks)
        } else {
            arbitrary(rest.copyOf(minOf(rest.size, MAX_BYTES)), chunks)
        }
    }

    private class Refused(what: String) : RuntimeException(what)

    /** What the rewrite sends for [bytes], or null when it refused, as it may only with [Refused]. */
    private fun rewrite(
        bytes: ByteArray,
        chunks: Random,
    ): ByteArray? {
        val out = ByteArrayOutputStream()
        try {
            NamespaceFieldRewrite(FIELD, FROM, TO) { Refused(it) }.use { rewrite ->
                var at = 0
                while (at < bytes.size) {
                    val n = minOf(1 + chunks.nextInt(64), bytes.size - at)
                    out.write(rewrite.feed(bytes, at, n))
                    at += n
                }
                out.write(rewrite.end())
            }
        } catch (refused: Refused) {
            val reason = refused.message.orEmpty()
            check(REASONS.any { reason.startsWith(it) } && reason.length < 100) { "a refusal not in its own words" }
            return null
        }
        return out.toByteArray()
    }

    // ---- arbitrary bytes ------------------------------------------------------

    private fun arbitrary(
        bytes: ByteArray,
        chunks: Random,
    ) {
        val output = rewrite(bytes, chunks)
        val input =
            try {
                tokens(bytes)
            } catch (_: IOException) {
                null
            }
        val sound = input != null && oneObject(input) && fieldValue(input).let { it > 0 && input[it] == "s:$FROM" }
        if (output != null) {
            check(sound) { "an answer was accepted that is not one object whose name is the host's" }
            val expected = input!!.toMutableList().also { it[fieldValue(input)] = "s:$TO" }
            check(tokens(output) == expected) { "the output is not the answer with only its name changed" }
        } else {
            check(!sound || bytes.any { it == 0.toByte() }) { "one object whose name is the host's was refused" }
        }
    }

    /** The tokens of [bytes] as text: a key, a string, a number by its text, else the token itself. */
    private fun tokens(bytes: ByteArray): List<String> =
        STRICT.createParser(bytes).use { parser ->
            generateSequence { parser.nextToken() }.map { token ->
                when (token) {
                    JsonToken.FIELD_NAME -> "k:" + parser.currentName()
                    JsonToken.VALUE_STRING -> "s:" + parser.text
                    JsonToken.VALUE_NUMBER_INT, JsonToken.VALUE_NUMBER_FLOAT -> "n:" + parser.text
                    else -> token.asString()
                }
            }.toList()
        }

    /** Whether [tokens] are one object and nothing after it. */
    private fun oneObject(tokens: List<String>): Boolean {
        if (tokens.firstOrNull() != "{") return false
        var depth = 0
        for ((i, token) in tokens.withIndex()) {
            depth += nesting(token)
            if (depth == 0) return i == tokens.lastIndex
        }
        return false
    }

    /** The index of the top-level [FIELD]'s value in [tokens], or -1. */
    private fun fieldValue(tokens: List<String>): Int {
        var depth = 0
        for ((i, token) in tokens.withIndex()) {
            if (depth == 1 && token == "k:$FIELD") return i + 1
            depth += nesting(token)
        }
        return -1
    }

    private fun nesting(token: String) =
        when (token) {
            "{", "[" -> 1
            "}", "]" -> -1
            else -> 0
        }

    // ---- generated answers ----------------------------------------------------

    private sealed interface Json

    private data class Obj(val members: List<Pair<String, Json>>) : Json

    private data class Arr(val items: List<Json>) : Json

    private data class Str(val value: String) : Json

    private data class Num(val text: String) : Json

    private data class Lit(val text: String) : Json

    /** The generator's choices, read off the input; zeros once it runs out. */
    private class Tape(private val bytes: ByteArray) {
        private var at = 0

        fun next(bound: Int): Int = if (bound <= 1 || at >= bytes.size) 0 else (bytes[at++].toInt() and 0xff) % bound
    }

    private fun generated(
        tape: Tape,
        chunks: Random,
    ) {
        // Mostly one object with `name` the host's name; a choice of the tape breaks each rule.
        val model = if (tape.next(16) == 0) value(tape, 1) else Obj(members(tape, 0, field = true))
        val text = StringBuilder()
        render(model, tape, text)
        val trailing = tape.next(16)
        when (trailing) {
            0 -> render(value(tape, 1), tape, text)
            1 -> text.append('x')
        }
        val sound =
            trailing > 1 && model is Obj && unique(model) && model.members.any { it == FIELD to Str(FROM) }
        val output = rewrite(text.toString().toByteArray(Charsets.UTF_8), chunks)
        check((output != null) == sound) {
            if (sound) "a sound answer was refused" else "an answer that must be refused was accepted"
        }
        if (output != null) {
            val expected = (model as Obj).members.map { (key, value) -> key to if (key == FIELD) Str(TO) else value }
            check(tokens(output) == tokensOf(Obj(expected))) { "the output is not the answer with one value changed" }
        }
    }

    /** Distinct keys, sometimes one twice; at the top ([field]), mostly a `name` that is the host's name. */
    private fun members(
        tape: Tape,
        depth: Int,
        field: Boolean = false,
    ): List<Pair<String, Json>> {
        val keys = KEYS.filter { !field || it != FIELD }.toMutableList()
        val members = MutableList(tape.next(5)) { keys.removeAt(tape.next(keys.size)) to value(tape, depth + 1) }
        if (field) {
            when (tape.next(8)) {
                0 -> {}
                1 -> members.add(tape.next(members.size + 1), FIELD to value(tape, depth + 1))
                else -> members.add(tape.next(members.size + 1), FIELD to Str(FROM))
            }
        }
        if (members.isNotEmpty() && tape.next(10) == 0) members.add(members[tape.next(members.size)])
        return members
    }

    private fun value(
        tape: Tape,
        depth: Int,
    ): Json =
        when (tape.next(if (depth >= 3) 3 else 5)) {
            0 -> Str(STRINGS[tape.next(STRINGS.size)])
            1 -> Num(number(tape))
            2 -> Lit(LITERALS[tape.next(LITERALS.size)])
            3 -> Obj(members(tape, depth))
            else -> Arr(List(tape.next(4)) { value(tape, depth + 1) })
        }

    /** One of the spellings a re-encoding would change, or a number the tape spells by the JSON grammar. */
    private fun number(tape: Tape): String {
        if (tape.next(2) == 0) return NUMBERS[tape.next(NUMBERS.size)]
        return buildString {
            if (tape.next(2) == 0) append('-')
            val digits = tape.next(25)
            if (digits == 0) append('0') else append('1' + tape.next(9))
            repeat(maxOf(digits - 1, 0)) { append('0' + tape.next(10)) }
            if (tape.next(3) == 0) {
                append('.')
                repeat(1 + tape.next(6)) { append('0' + tape.next(10)) }
            }
            if (tape.next(3) == 0) {
                append(if (tape.next(2) == 0) 'e' else 'E')
                when (tape.next(3)) {
                    0 -> append('+')
                    1 -> append('-')
                }
                repeat(1 + tape.next(4)) { append('0' + tape.next(10)) }
            }
        }
    }

    private fun render(
        json: Json,
        tape: Tape,
        out: StringBuilder,
    ) {
        out.append(SPACES[tape.next(SPACES.size)])
        when (json) {
            is Obj -> {
                out.append('{')
                for ((i, member) in json.members.withIndex()) {
                    if (i > 0) out.append(',')
                    out.append(SPACES[tape.next(SPACES.size)])
                    string(member.first, tape, out)
                    out.append(SPACES[tape.next(SPACES.size)]).append(':')
                    render(member.second, tape, out)
                }
                out.append(SPACES[tape.next(SPACES.size)]).append('}')
            }
            is Arr -> {
                out.append('[')
                for ((i, item) in json.items.withIndex()) {
                    if (i > 0) out.append(',')
                    render(item, tape, out)
                }
                out.append(SPACES[tape.next(SPACES.size)]).append(']')
            }
            is Str -> string(json.value, tape, out)
            is Num -> out.append(json.text)
            is Lit -> out.append(json.text)
        }
        out.append(SPACES[tape.next(SPACES.size)])
    }

    /** [value] as a JSON string; a surrogate is always escaped, any other character when the tape says so. */
    private fun string(
        value: String,
        tape: Tape,
        out: StringBuilder,
    ) {
        out.append('"')
        for (c in value) {
            when {
                c == '"' || c == '\\' -> out.append('\\').append(c)
                c < ' ' || c.isSurrogate() || tape.next(8) == 0 -> out.append("\\u%04x".format(c.code))
                else -> out.append(c)
            }
        }
        out.append('"')
    }

    private fun unique(json: Json): Boolean =
        when (json) {
            is Obj ->
                json.members.map { it.first }.toSet().size == json.members.size &&
                    json.members.all { unique(it.second) }
            is Arr -> json.items.all(::unique)
            else -> true
        }

    private fun tokensOf(json: Json): List<String> =
        when (json) {
            is Obj -> listOf("{") + json.members.flatMap { (key, value) -> listOf("k:$key") + tokensOf(value) } + "}"
            is Arr -> listOf("[") + json.items.flatMap(::tokensOf) + "]"
            is Str -> listOf("s:${json.value}")
            is Num -> listOf("n:${json.text}")
            is Lit -> listOf(json.text)
        }

    private companion object {
        const val FIELD = "name"
        const val FROM = "shared"
        const val TO = "posthog"
        const val MAX_BYTES = 64 * 1024

        val STRICT: JsonFactory = JsonFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build()

        /** The rewrite's own reasons, and only these: none quotes the answer. */
        val REASONS =
            listOf(
                "a body that is not valid JSON (",
                "a body that is not one JSON object",
                "a body that is not a whole JSON object",
                "an object whose $FIELD is not the schema's name on the host",
                "an object without $FIELD",
            )

        val KEYS = listOf(FIELD, "namespace", "Name", "remote", "table_uuid", "", "é", "😀", "o", "n")
        val STRINGS =
            listOf(
                FROM, TO, "", "Shared", "shared ", "sh\"ared", "back\\slash", "/", "é", Char(0x2028).toString(),
                "😀", "\uD800", "line\nbreak", "\u0000", FIELD, "x".repeat(70),
            )
        val NUMBERS =
            listOf(
                "0", "-0", "1e10", "1E+10", "-0.0", "1.10", "1E-7", "2.5e+3", "0.000", "1e400", "4.9e-325",
                "123456789012345678901234567890", "9223372036854775808", "-9223372036854775809",
            )
        val LITERALS = listOf("true", "false", "null")
        val SPACES = listOf("", "", "", " ", "\n", "\t", "\r\n  ")
    }
}
