package com.posthog.hoglake.service

import com.posthog.hoglake.model.HoglakeException
import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/** Arithmetic supplement; actual mixed-state/backend/route inputs live in the shared parity suite. */
class ScanColumnStatsEntryCapTest {
    @Test
    fun `provided count is multiplied in Long before comparing the cap`() {
        assertThatThrownBy {
            enforceScanColumnStatsEntryCap(215_000L, (1L..10_000L).toSet(), ScanService.ColumnStatsRequest())
        }.isInstanceOf(HoglakeException.Validation::class.java).hasMessageContaining("2150000000")
    }

    @Test
    fun `exact cap and visible requested intersection retain their boundary`() {
        assertThatCode {
            enforceScanColumnStatsEntryCap(10_000L, (1L..100L).toSet(), ScanService.ColumnStatsRequest())
            enforceScanColumnStatsEntryCap(1_000_001L, setOf(1), ScanService.ColumnStatsRequest(setOf(999)))
        }.doesNotThrowAnyException()
        assertThatThrownBy {
            enforceScanColumnStatsEntryCap(1_000_001L, setOf(1), ScanService.ColumnStatsRequest(setOf(1, 999)))
        }.isInstanceOf(HoglakeException.Validation::class.java).hasMessageContaining("1000001")
    }
}
