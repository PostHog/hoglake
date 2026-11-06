package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.model.TableAppend
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * Namespace containment on the live engine: the schema's columns, CHECK
 * and partial unique index, the mark and clear guards, the gate's read,
 * the commit check and the drop refusal. The PostgreSQL behavior is in
 * NamespaceContainmentIntegrationTest.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteNamespaceContainmentIntegrationTest {
    private lateinit var engine: HogliteConformanceEngine

    private val store get() = engine.store

    private val team42 = NamespaceContainment("team_id", 42)

    @BeforeAll
    fun start() {
        engine = HogliteConformanceEngine()
    }

    @AfterAll
    fun stop() {
        if (::engine.isInitialized) engine.close()
    }

    private fun catalog(): String {
        val c = HogliteConformanceEngine.uniqueName("contain")
        store.createCatalog(c, "s3://hoglake-it/$c", null, null)
        return c
    }

    private fun append(
        c: String,
        namespace: String,
        rest: String,
    ) = store.commit(
        c,
        CommitRequest(
            appends =
                listOf(TableAppend(namespace, "t", listOf(FileRegistration("s3://hoglake-it/$c/$rest", 10, 100, 50)))),
        ),
    )

    private fun refused(block: () -> Unit) =
        assertThatThrownBy(block).isInstanceOf(HoglakeException.ContainmentViolation::class.java)

    @Test
    fun `a contained namespace owns its prefix through mark, commits, clear and drop`() {
        val c = catalog()
        store.createNamespace(c, "team_42", null, null)
        store.createNamespace(c, "main", null, null)
        store.createTable(c, "main", "t", listOf(ColumnDef("id", ColType.LONG)), null, null)
        append(c, "main", "data/main/t/a.parquet")

        assertThat(store.setNamespaceContainment(c, "team_42", team42, "op").containment).isEqualTo(team42)
        // What the namespace gate reads, and what the namespace reads return.
        assertThat(store.liveContainment(c, "team_42")).isEqualTo(team42)
        assertThat(store.liveContainment(c, "main")).isNull()
        assertThat(store.liveContainment(HogliteConformanceEngine.uniqueName("absent"), "team_42")).isNull()
        assertThat(store.getNamespace(c, "team_42").containment).isEqualTo(team42)
        assertThat(store.listNamespaces(c, null, null).single { it.name == "team_42" }.containment).isEqualTo(team42)
        // Marking again is a no-op. A second namespace cannot take the same value.
        assertThat(store.setNamespaceContainment(c, "team_42", team42, null).containment).isEqualTo(team42)
        store.createNamespace(c, "team_x", null, null)
        assertThatThrownBy { store.setNamespaceContainment(c, "team_x", team42, null) }
            .isInstanceOf(HoglakeException.AlreadyExists::class.java)
        // A namespace that holds a table cannot be marked.
        assertThatThrownBy { store.setNamespaceContainment(c, "main", NamespaceContainment("team_id", 1), null) }
            .isInstanceOf(HoglakeException.NamespaceNotEmpty::class.java)

        store.createTable(c, "team_42", "t", listOf(ColumnDef("id", ColType.LONG)), null, null)
        append(c, "team_42", "data/team_42/t/b.parquet")
        refused { append(c, "team_42", "data/main/t/c.parquet") }
        refused { append(c, "main", "data/team_42/t/d.parquet") }
        refused { append(c, "main", "data/team%5F42/t/e.parquet") }
        refused { append(c, "main", "d%61ta/team_42/t/f.parquet") }
        append(c, "main", "data/team_43/t/g.parquet")

        // Clear needs an empty namespace. Drop needs an operator.
        assertThatThrownBy { store.setNamespaceContainment(c, "team_42", null, null) }
            .isInstanceOf(HoglakeException.NamespaceNotEmpty::class.java)
        store.dropTable(c, "team_42", "t", null, "op", "drop")
        assertThatThrownBy { store.dropNamespace(c, "team_42", null, "me", "drop", mayDropContained = false) }
            .isInstanceOf(HoglakeException.Forbidden::class.java)
        assertThat(store.setNamespaceContainment(c, "team_42", null, null).containment).isNull()
        assertThat(store.liveContainment(c, "team_42")).isNull()
        store.dropNamespace(c, "team_42", null, "me", "drop", mayDropContained = false)
    }

    @Test
    fun `a foreign file under the prefix refuses the mark, in a literal or an escaped spelling`() {
        val c = catalog()
        store.createNamespace(c, "main", null, null)
        store.createNamespace(c, "team_7", null, null)
        store.createNamespace(c, "team_8", null, null)
        store.createNamespace(c, "team_9", null, null)
        store.createTable(c, "main", "t", listOf(ColumnDef("id", ColType.LONG)), null, null)
        append(c, "main", "data/team_7/t/a.parquet")
        append(c, "main", "data/team%5F8/t/b.parquet")
        append(c, "main", "data/team_90/t/c.parquet")

        for (ns in listOf("team_7", "team_8")) {
            assertThatThrownBy { store.setNamespaceContainment(c, ns, NamespaceContainment("team_id", 7), null) }
                .`as`(ns)
                .isInstanceOf(HoglakeException.CommitConflict::class.java)
                .hasMessageContaining("is already registered or claimed under its prefix")
        }
        assertThat(store.setNamespaceContainment(c, "team_9", NamespaceContainment("team_id", 9), null).containment)
            .isEqualTo(NamespaceContainment("team_id", 9))
    }
}
