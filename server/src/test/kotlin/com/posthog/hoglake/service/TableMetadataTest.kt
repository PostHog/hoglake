package com.posthog.hoglake.service

import com.posthog.hoglake.model.HoglakeException
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class TableMetadataTest {
    @Test
    fun `custom metadata enforces bounded strings and reserves configuration names`() {
        TableMetadata.validateComment(null)
        TableMetadata.validateComment("")
        TableMetadata.validateComment("x".repeat(16384))
        TableMetadata.validateProperties(mapOf("owner.team" to "x".repeat(4096)))
        for (key in listOf("", "Owner", "hoglake.location", "trino.foo", "location", "sorted_by", "x".repeat(129))) {
            assertThatThrownBy { TableMetadata.validateProperties(mapOf(key to "value")) }
                .isInstanceOf(HoglakeException.Validation::class.java)
        }
        assertThatThrownBy { TableMetadata.validateProperties((0..100).associate { "k$it" to "value" }) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { TableMetadata.validateProperties(mapOf("key" to "x".repeat(4097))) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { TableMetadata.validateProperties(mapOf("key" to "\u0000")) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { TableMetadata.validateComment("x".repeat(16385)) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { TableMetadata.validateComment("\u0000") }
            .isInstanceOf(HoglakeException.Validation::class.java)
    }
}
