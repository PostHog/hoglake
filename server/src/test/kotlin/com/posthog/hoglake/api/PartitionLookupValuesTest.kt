package com.posthog.hoglake.api

import com.posthog.hoglake.model.PartitionLookupInvalidRequest
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class PartitionLookupValuesTest {
    @Test
    fun `preserves ordered raw values including null empty and literal null`() {
        assertThat(parsePartitionLookupValues("""[null,"","null","01","a:b","a,[]","東京"]"""))
            .containsExactly(null, "", "null", "01", "a:b", "a,[]", "東京")
    }

    @Test
    fun `rejects coercion partial JSON trailing tokens and oversized input`() {
        listOf(
            "", "null", "{}", "[]", "[1]", "[true]", "[{}]", "[[]]", "[\"x\"] []", "[null,]",
            "[\"${"x".repeat(
                8193,
            )}\"]",
            " ".repeat(65537), "[${List(65) { "null" }.joinToString(",")}]",
        ).forEach {
                raw ->
            assertThatThrownBy {
                parsePartitionLookupValues(
                    raw,
                )
            }.isInstanceOf(PartitionLookupInvalidRequest::class.java)
        }
    }
}
