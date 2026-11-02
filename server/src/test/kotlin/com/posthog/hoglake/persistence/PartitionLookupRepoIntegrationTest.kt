package com.posthog.hoglake.persistence

import com.posthog.hoglake.testing.PartitionLookupFixture
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PartitionLookupRepoIntegrationTest {
    private val db = PgTestSupport.freshDatabase(productionSession = true)

    @AfterAll
    fun close() = db.close()

    @Test
    fun `equally capped keys choose sparse id coverage and finish in two windows`() {
        val f = PartitionLookupFixture(db)
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                    path, record_count, file_size_bytes, row_id_start, spec_id)
                SELECT :catalog, g, :table, 0, 's3://fixture/skew/' || g, 10, 100, g * 10, :spec
                FROM generate_series(1, 10001) g
                """,
            ).bind("catalog", f.catalogId).bind("table", f.tableId).bind("spec", f.specId).execute()
            h.createUpdate(
                """
                INSERT INTO hog_file_partition_value (catalog_id, data_file_id, key_index, value)
                SELECT :catalog, g, 0, 'month' FROM generate_series(1, 10001) g
                UNION ALL
                SELECT :catalog, g, 1, CASE WHEN g % 1000 = 0 OR g = 10001 THEN 'team' ELSE 'other' END
                FROM generate_series(1, 10001) g
                """,
            ).bind("catalog", f.catalogId).execute()
        }

        fun lookup(
            after: Long,
            aggregates: Boolean,
        ) = db.jdbi.withHandleUnchecked { h ->
            PartitionLookupRepo.lookup(
                h, f.catalogId, f.tableId, f.specId, listOf("month", "team"), db.head(f.name),
                10002, after, 10, 100, aggregates,
            )
        }
        val first = lookup(-1, true)
        assertThat(first.aggregates.reason).isEqualTo("candidate_scan_limit")
        assertThat(first.page.candidateCount).isEqualTo(11)
        assertThat(first.page.lastExamined).isEqualTo(10000)
        assertThat(first.page.files.map { it.dataFileId }).containsExactlyElementsOf((1L..10L).map { it * 1000 })
        val second = lookup(first.page.lastExamined!!, false)
        assertThat(second.aggregates.reason).isEqualTo("continuation")
        assertThat(second.page.candidateCount).isEqualTo(1)
        assertThat(second.page.files.map { it.dataFileId }).containsExactly(10001)
    }

    @Test
    fun `combined result keeps totals independent of page size and aggregate overflow`() {
        val f = PartitionLookupFixture(db)
        f.append(*List(3) { listOf(null, "") }.toTypedArray())

        fun lookup() =
            db.jdbi.withHandleUnchecked { h ->
                PartitionLookupRepo.lookup(
                    h, f.catalogId, f.tableId, f.specId, listOf(null, ""), db.head(f.name),
                    4, -1, 3, 1, true,
                )
            }
        val exact = lookup()
        assertThat(exact.aggregates.totals!!.fileCount).isEqualTo(3)
        assertThat(exact.aggregates.totals!!.physicalRows).isEqualTo(30)
        assertThat(exact.page.files.map { it.dataFileId }).containsExactly(1, 2)
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate("UPDATE hog_data_file SET record_count = :max WHERE catalog_id = :catalog")
                .bind("max", Long.MAX_VALUE).bind("catalog", f.catalogId).execute()
        }
        val overflow = lookup()
        assertThat(overflow.aggregates.reason).isEqualTo("overflow")
        assertThat(overflow.aggregates.totals).isNull()
        assertThat(overflow.page.files.map { it.dataFileId }).containsExactly(1, 2)
    }
}
