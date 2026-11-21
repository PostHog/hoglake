package com.posthog.hoglake.service

import com.posthog.hoglake.model.HoglakeException
import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * [CatalogService.validateDataPath] is the one rule a data_path passes on
 * both backends before it is stored or sent to hoglite. The character
 * rule matters most on Hoglite: the value rides in the
 * `X-Hoglite-Data-Path` header, which Go writes as UTF-8 and Java's
 * HttpClient reads as ISO-8859-1, so anything but printable ASCII fails
 * to round-trip and the create refuses its own echo (#668).
 */
class DataPathValidationTest {
    @Test
    fun `a data_path is printable ASCII, whatever hoglite would accept`() {
        for (ok in listOf("s3://b", "s3://b/", "s3://b/a-b_c.d~e!(x)", "s3://b/%C3%A9", "s3://b/a/b/c")) {
            assertThatCode { CatalogService.validateDataPath(ok) }.describedAs(ok).doesNotThrowAnyException()
        }
        // hoglite admits these (valid UTF-8, no space, no control; a zero-width
        // space is neither to Java); hoglake does not.
        for (bad in listOf("s3://b/café", "s3://b/日本", "s3://b/x\u200by")) {
            assertThatThrownBy { CatalogService.validateDataPath(bad) }
                .describedAs(bad)
                .isInstanceOf(HoglakeException.Validation::class.java)
                .hasMessageContaining("printable ASCII")
        }
        // The older refusals keep their own messages.
        assertThatThrownBy { CatalogService.validateDataPath("s3://b/a b") }
            .hasMessageContaining("whitespace or control")
        assertThatThrownBy { CatalogService.validateDataPath("s3://b/a\u0007") }
            .hasMessageContaining("whitespace or control")
        assertThatThrownBy { CatalogService.validateDataPath("gs://b/a") }
            .hasMessageContaining("s3://<bucket>")
        assertThatThrownBy { CatalogService.validateDataPath("s3://b/../a") }
            .hasMessageContaining("'.' or '..' segments")
    }
}
