package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.service.columnsByFieldId
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/** Stored stats resolve against a field-id map that the caller builds once per read. */
class HogliteStoredStatsTest {
    private val store = HogliteCatalogStore(HogliteClient("http://127.0.0.1:1", sleeper = {}), { "t-$it" })

    private fun col(
        id: Long,
        name: String,
        type: ColType,
        children: List<Column> = emptyList(),
    ) = Column(id, 0, ColumnDef(name, type), children)

    private val byId =
        columnsByFieldId(
            listOf(
                col(1, "a", ColType.LONG),
                col(2, "s", ColType.STRUCT, listOf(col(3, "x", ColType.INT))),
                col(4, "v", ColType.VARIANT),
            ),
        )

    // Field 99 is a dropped column: it is not in the visible forest.
    private val statsJson =
        jacksonObjectMapper().writeValueAsString(
            listOf(1L, 2L, 3L, 4L, 99L).map { ColumnStats(it, 5, 0, null, null, null, null) },
        )

    @Test
    fun `only visible leaves resolve, with their dotted paths`() {
        val resolved = store.resolveStoredStats(statsJson, byId, null)

        assertThat(resolved.map { it.fieldId to it.path }).containsExactly(1L to "a", 3L to "s.x")
        assertThat(resolved.map { it.type }).containsExactly(ColType.LONG, ColType.INT)
    }

    @Test
    fun `a wanted set narrows the result, and absent stats resolve to nothing`() {
        assertThat(store.resolveStoredStats(statsJson, byId, setOf(3L)).map { it.path }).containsExactly("s.x")
        assertThat(store.resolveStoredStats(null, byId, null)).isEmpty()
    }
}
