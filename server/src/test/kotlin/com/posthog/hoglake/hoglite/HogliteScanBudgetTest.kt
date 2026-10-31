package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.HoglakeException
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class HogliteScanBudgetTest {
    @Test
    fun `requested metadata has its own cumulative memory ceiling`() {
        val budget = HogliteScanBudget()
        budget.add(1, HogliteCatalogStore.SCAN_MAX_METADATA_BYTES - 1)
        budget.add(1, 1)
        assertThatThrownBy { budget.add(1, 1) }
            .isInstanceOf(HoglakeException.Validation::class.java).hasMessageContaining("requested metadata")
    }

    @Test
    fun `file count accepts the ceiling and refuses the next row`() {
        val budget = HogliteScanBudget()
        repeat(HogliteCatalogStore.SCAN_MAX_FILES) { budget.add(1) }
        assertThatThrownBy { budget.add(1) }
            .isInstanceOf(HoglakeException.Validation::class.java).hasMessageContaining("1000000 files")
    }

    @Test
    fun `byte count is cumulative and refuses the next byte beyond the ceiling`() {
        val budget = HogliteScanBudget()
        budget.add(HogliteCatalogStore.SCAN_MAX_BYTES - 1)
        budget.add(1)
        assertThatThrownBy { budget.add(1) }
            .isInstanceOf(HoglakeException.Validation::class.java).hasMessageContaining("536870912 bytes")
    }
}
