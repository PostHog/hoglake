package com.posthog.hoglake.store

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream

/** The alias's one-field rewrite on its own, fed the answer split at every byte. */
class NamespaceFieldRewriteTest {
    private class Refused(what: String) : RuntimeException(what)

    /** [input] rewritten when fed in two chunks cut at [split]. */
    private fun rewrite(
        input: String,
        split: Int,
    ): String {
        val bytes = input.toByteArray()
        NamespaceFieldRewrite("name", "shared", "posthog") { Refused(it) }.use { rewrite ->
            val out = ByteArrayOutputStream()
            out.write(rewrite.feed(bytes, 0, split))
            out.write(rewrite.feed(bytes, split, bytes.size - split))
            out.write(rewrite.end())
            return out.toString(Charsets.UTF_8)
        }
    }

    private fun splits(input: String) = 0..input.toByteArray().size

    @Test
    fun `only the top-level field changes, and every number keeps its text`() {
        val input =
            """{"name":"shared","n":[1e10,-0.0,-0,1.10,1E-7,2.5e+3,123456789012345678901234567890],""" +
                """"o":{"name":"shared"},"s":"shared","t":"a, ","é":[true,false,null,{}]}"""
        val expected = input.replaceFirst("\"shared\"", "\"posthog\"")
        for (split in splits(input)) {
            assertThat(rewrite(input, split)).describedAs("split at %d", split).isEqualTo(expected)
        }
        // Whitespace is dropped and escapes are written anew; no value changes. A key and a value spelled with
        // escapes are that key and that value.
        val u = "\\" + "u00"
        val spaced = """ { "s" : "${u}e9\/\n" , "n${u}61me" : "sh${u}61red" } """
        for (split in splits(spaced)) {
            assertThat(rewrite(spaced, split)).isEqualTo("{\"s\":\"é/\\n\",\"name\":\"posthog\"}")
        }
    }

    @Test
    fun `an answer that is not an object is refused at its first token, before any of it is sent`() {
        for (head in listOf("[", "null ", "\"shared\" ", "7 ", "  [{\"name\":\"shared\"}")) {
            NamespaceFieldRewrite("name", "shared", "posthog") { Refused(it) }.use { rewrite ->
                assertThatThrownBy { rewrite.feed(head.toByteArray()) }.describedAs(head)
                    .isInstanceOf(Refused::class.java)
            }
        }
    }

    @Test
    fun `anything but one object whose field is the host's name is refused, wherever the answer is cut`() {
        for (input in listOf(
            "",
            "  ",
            "null",
            "\"shared\"",
            """[{"name":"shared"}]""",
            """{"name":"other"}""",
            """{"name":"SHARED"}""",
            """{"name":7}""",
            """{"name":{"name":"shared"}}""",
            """{"o":{"name":"shared"}}""",
            """{"name":"shared","name":"shared"}""",
            """{"name":"shared","o":{"a":1,"a":1}}""",
            """{"name":"shared"} {"name":"shared"}""",
            """{"name":"shared"}x""",
            """{"name":"shared"},""",
            """{"name":"shared"""",
            // Jackson's async parser closes these when a chunk ends after the comma or the colon; the rewrite
            // works round it.
            """{"name":"shared",}""",
            "{\"name\":\"shared\",\"a\":1, \n }",
            """{"name":"shared","o":{"a":1,  }}""",
            """{"name":"shared","a":}""",
            """{"name":"shared","o":{"a" : }}""",
            """{"name":}""",
            """{"name":"shared","n":01}""",
        )) {
            for (split in splits(input)) {
                assertThatThrownBy { rewrite(input, split) }.describedAs("%s split at %d", input, split)
                    .isInstanceOf(Refused::class.java)
            }
        }
    }
}
