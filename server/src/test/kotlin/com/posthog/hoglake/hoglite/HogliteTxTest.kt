package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.HoglakeException
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * The guard-index → typed-exception mapping is the contract every OCC
 * refusal on the hoglite backend flows through: hoglite's 409 carries
 * only a statement index, and [HogliteTx.classify] is where it becomes
 * hoglake's error model.
 */
class HogliteTxTest {
    @Test
    fun `a guard failure decodes to the exception the statement was registered with`() {
        val tx = HogliteTx()
        tx.add("INSERT INTO files(id) VALUES (1)")
        tx.expectZero("SELECT COUNT(*) FROM tables WHERE name = 't'") {
            HoglakeException.NotFound("table 'ns.t' in catalog 'c'")
        }
        tx.affectOne("UPDATE meta SET next_snapshot = next_snapshot + 1") {
            HoglakeException.CommitConflict("head moved")
        }

        assertThat(tx.classify(HogliteGuardFailure(1, "guard failed")))
            .isInstanceOf(HoglakeException.NotFound::class.java)
            .hasMessageContaining("ns.t")
        assertThat(tx.classify(HogliteGuardFailure(2, "guard failed")))
            .isInstanceOf(HoglakeException.CommitConflict::class.java)
            .hasMessageContaining("head moved")
    }

    @Test
    fun `a failure on an unguarded statement is still a typed conflict, never a crash`() {
        val tx = HogliteTx()
        tx.add("INSERT INTO files(id) VALUES (1)")
        assertThat(tx.classify(HogliteGuardFailure(0, "guard failed")))
            .isInstanceOf(HoglakeException.CommitConflict::class.java)
            .hasMessageContaining("store bug")
    }

    @Test
    fun `expectScalar pins a counter`() {
        val tx = HogliteTx()
        tx.expectScalar(7L, "SELECT next_snapshot FROM meta WHERE id = 1") {
            HoglakeException.CommitConflict("head moved")
        }
        val statements = tx.statements()
        assertThat(statements).hasSize(1)
        assertThat(statements[0].expectScalarInt).isEqualTo(7L)
        assertThat(statements[0].expectAffected).isNull()
    }

    @Test
    fun `statement order is preserved and indices are stable`() {
        val tx = HogliteTx()
        val a = tx.add("SELECT 1")
        val b = tx.expectOne("SELECT COUNT(*) FROM meta") { HoglakeException.NotFound("meta") }
        val c = tx.add("SELECT 2")
        assertThat(listOf(a, b, c)).containsExactly(0, 1, 2)
        assertThat(tx.statements().map { it.sql }).containsExactly(
            "SELECT 1",
            "SELECT COUNT(*) FROM meta",
            "SELECT 2",
        )
    }
}
