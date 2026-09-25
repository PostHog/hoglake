package com.posthog.hoglake.model

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * The split_offsets contract every writer shares, checked on its edges:
 * each accepted list sits exactly on a boundary whose one-step-over twin
 * is refused, so flipping any comparison (`<` to `<=`, `>=` to `>`) in
 * [SplitOffsets.violation] reds here. The contract is the Trino
 * connector's `isUsable` — non-empty, strictly increasing, first >= 0,
 * last < file_size_bytes — plus the row-group cap.
 */
class SplitOffsetsTest {
    @Test
    fun `boundaries of the contract`() {
        assertThat(SplitOffsets.violation(listOf(0L), 1)).isNull()
        assertThat(SplitOffsets.violation(listOf(4L, 5L, 99L), 100)).isNull()

        assertThat(SplitOffsets.violation(emptyList(), 100)).contains("at least one")
        assertThat(SplitOffsets.violation(listOf(-1L, 4L), 100)).contains("negative")
        assertThat(SplitOffsets.violation(listOf(4L, 4L), 100)).contains("strictly increasing")
        assertThat(SplitOffsets.violation(listOf(4L, 50L, 20L), 100))
            .contains("strictly increasing").contains("entry 2 (20)")
        assertThat(SplitOffsets.violation(listOf(4L, 100L), 100)).contains("file_size_bytes")
        assertThat(SplitOffsets.violation(listOf(0L), 0)).contains("file_size_bytes")
    }

    @Test
    fun `the row-group cap is inclusive`() {
        val atCap = List(SplitOffsets.MAX_ROW_GROUPS) { it.toLong() }
        assertThat(SplitOffsets.violation(atCap, Long.MAX_VALUE)).isNull()
        assertThat(SplitOffsets.violation(atCap + SplitOffsets.MAX_ROW_GROUPS.toLong(), Long.MAX_VALUE))
            .contains("maximum ${SplitOffsets.MAX_ROW_GROUPS}")
    }

    @Test
    fun `a registration's list is validated only when present, as a 422`() {
        val base = FileRegistration(path = "s3://b/f.parquet", recordCount = 1, fileSizeBytes = 100)
        base.validateSplitOffsets()
        base.copy(splitOffsets = listOf(4L, 50L)).validateSplitOffsets()
        assertThatThrownBy { base.copy(splitOffsets = listOf(50L, 4L)).validateSplitOffsets() }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("invalid split_offsets for s3://b/f.parquet")
    }
}
