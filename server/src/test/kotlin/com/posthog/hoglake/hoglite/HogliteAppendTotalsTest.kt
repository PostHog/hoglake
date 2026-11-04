package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test

/** The per-table totals a commit writes: checked sums, as in CommitService. */
class HogliteAppendTotalsTest {
    private fun file(bytes: Long) = FileRegistration(path = "s3://b/f-$bytes", recordCount = 1, fileSizeBytes = bytes)

    @Test
    fun `the totals upsert follows a guard that keeps the running byte total exact`() {
        val tx = HogliteTx()

        HogliteCommit.addAppendTotals(tx, 7, "ns.t", 3, listOf(file(10), file(5)))

        val (guard, upsert) = tx.statements()
        assertThat(guard.sql).contains("FROM table_totals WHERE table_id = ? AND file_size_bytes > ?")
        assertThat(guard.args).containsExactly(7L, Long.MAX_VALUE - 15)
        assertThat(guard.expectScalarInt).isZero()
        assertThat(upsert.sql).startsWith("INSERT INTO table_totals")
        assertThat(upsert.args).containsExactly(7L, 3L, 2L, 15L)
        assertThat(tx.classify(HogliteGuardFailure(0, "guard failed")))
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("file_size_bytes total overflows for ns.t")
    }

    @Test
    fun `a byte sum that overflows is a Validation and adds nothing to the transaction`() {
        val tx = HogliteTx()

        val failure =
            catchThrowable {
                HogliteCommit.addAppendTotals(tx, 7, "ns.t", 2, listOf(file(Long.MAX_VALUE), file(1)))
            }

        assertThat(failure)
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("file_size_bytes sum overflows for ns.t")
        assertThat(tx.size).isZero()
    }
}
