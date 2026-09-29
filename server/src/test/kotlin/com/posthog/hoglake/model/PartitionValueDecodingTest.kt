package com.posthog.hoglake.model

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.io.File
import java.util.Locale

/**
 * [PartitionValueDecoding] against the vectors `webui/test/partitions.test.ts`
 * asserts of `decodeValue`.
 *
 * ONE FILE, TWO READERS. There are two implementations of one rule —
 * the server's, because `filter=` and the `partition` sort happen
 * before paging, and the browser's, because the files tab decodes a
 * tuple it already holds. The vectors live in
 * `src/test/resources/vectors/partition_decode_vectors.json` and BOTH
 * this test and `webui/test/partitions.test.ts` read that file and pin
 * its `count`, so a transform changed on one side reds the other side's
 * suite directly rather than only when someone remembers to copy a
 * literal across. `.github/workflows/webui.yml` lists the vectors
 * directory among its triggers so a change to it runs both suites.
 *
 * THE ONE DELIBERATE DIFFERENCE is the null value, which is therefore
 * NOT in the shared table: the webui renders it as the literal string
 * "null" because a table cell has to show something, and this returns
 * null, because the listing SORTS nulls first and matches them with an
 * empty filter. Each side pins its own half.
 *
 * Unit test on purpose: no Docker, no container, so it reds in the fast
 * lane within a second of the mistake.
 */
class PartitionValueDecodingTest {
    private companion object {
        /**
         * The shared table, parsed — not restated. AGENT.md: "a parity
         * test must PARSE the other side's artifact or share a fixture;
         * a test that restates the constant it claims to mirror asserts
         * only that the file compiles." `webui/test/partitions.test.ts`
         * reads this same file.
         */
        val VECTORS: JsonNode =
            jacksonObjectMapper().readTree(File(PATH))

        const val PATH = "src/test/resources/vectors/partition_decode_vectors.json"

        @JvmStatic
        fun vectors(): List<Arguments> =
            VECTORS.get("vectors").map { v ->
                Arguments.of(
                    v.get("transform").asText(),
                    v.get("raw").asText(),
                    v.get("transform_param")?.asInt(),
                    v.get("expected").asText(),
                )
            }
    }

    @Test
    fun `the shared vector file's count matches its contents`() {
        // Pinned on BOTH sides. Without it, a vector added to the file
        // and to only one side's expectations passes here and there.
        assertThat(VECTORS.get("vectors").size()).isEqualTo(VECTORS.get("count").asInt())
    }

    @ParameterizedTest(name = "{0}({1}) = {3}")
    @MethodSource("vectors")
    fun `decodes the shared vectors`(
        transform: String,
        raw: String,
        param: Int?,
        expected: String,
    ) {
        assertThat(PartitionValueDecoding.decode(transform, raw, param)).isEqualTo(expected)
    }

    @Test
    fun `a null stored value decodes to null, not to the string the console shows`() {
        // The one divergence, and it is deliberate: the listing sorts
        // nulls first and matches them with an EMPTY filter, so a
        // decoder that produced "null" would sort them among the n's
        // and match the filter `nul`. The webui renders "null" in the
        // cell, which is a display choice on top of this.
        assertThat(PartitionValueDecoding.decode("month", null)).isNull()
        assertThat(PartitionValueDecoding.decode("identity", null)).isNull()
        assertThat(PartitionValueDecoding.decode("bucket", null, 16)).isNull()
    }

    @Test
    fun `the decoded form is ASCII whatever the JVM's default locale is`() {
        // `String.format` without a Locale uses
        // Locale.getDefault(FORMAT), so a pod whose LANG is ar_SA once
        // rendered "٢٠٢٦-٠٤" here — which would break every ASCII
        // filter a user types and sort partitions by Arabic-Indic code
        // points, silently, on CI runners that are all en_US.
        val original = Locale.getDefault()
        try {
            Locale.setDefault(Locale.forLanguageTag("ar-SA"))
            assertThat(PartitionValueDecoding.decode("month", "675")).isEqualTo("2026-04")
            assertThat(PartitionValueDecoding.decode("hour", "25")).isEqualTo("1970-01-02T01")
            assertThat(PartitionValueDecoding.decode("day", "20713")).isEqualTo("2026-09-17")
            assertThat(PartitionValueDecoding.decode("year", "56")).isEqualTo("2026")
        } finally {
            Locale.setDefault(original)
        }
    }
}
