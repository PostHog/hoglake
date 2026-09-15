package com.posthog.hoglake.service

import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.stats.IcebergSingleValue
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.atomic.AtomicInteger

/**
 * ALTER behaviour for the DuckLake scalar-parity types, against a real
 * catalog: the promotion ladders, the partition-transform type gates,
 * and the promote-time stats re-encode — which is now driven by the
 * facade mapping rather than a hardcoded pair of promotions, so "which
 * promotions leave bounds alone" is a claim worth proving with bytes.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AlterScalarTypeIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val alter = AlterService(db.jdbi)
    private val catalogs = CatalogService(db.jdbi)
    private val counter = AtomicInteger(0)

    @AfterAll
    fun tearDown() = db.close()

    /** Fresh catalog + namespace + a table with exactly [columns]. */
    private fun fixture(vararg columns: ColumnDef): Triple<String, String, Long> {
        val cat = "scalar-alter-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", columns.toList())
        return Triple(cat, "ns", catalogs.getCatalog(cat).catalogId)
    }

    private fun fieldId(
        cat: String,
        name: String,
    ): Long = catalogs.getTable(cat, "ns", "t").columns.single { it.def.name == name }.fieldId

    // ---- the promotion ladders --------------------------------------------

    @Test
    fun `the signed and unsigned integer ladders walk end to end`() {
        val (cat, ns, _) =
            fixture(
                ColumnDef("a", ColType.INT8),
                ColumnDef("b", ColType.UINT8),
            )
        alter.alterTable(cat, ns, "t", listOf(AlterOp.PromoteColumn("a", ColType.INT16)))
        alter.alterTable(cat, ns, "t", listOf(AlterOp.PromoteColumn("a", ColType.INT)))
        alter.alterTable(cat, ns, "t", listOf(AlterOp.PromoteColumn("a", ColType.LONG)))
        alter.alterTable(cat, ns, "t", listOf(AlterOp.PromoteColumn("b", ColType.UINT16)))
        alter.alterTable(cat, ns, "t", listOf(AlterOp.PromoteColumn("b", ColType.LONG)))
        val live = catalogs.getTable(cat, ns, "t").columns.associate { it.def.name to it.def.type }
        assertThat(live["a"]).isEqualTo(ColType.LONG)
        assertThat(live["b"]).isEqualTo(ColType.LONG)
    }

    @Test
    fun `the timestamp precision ladder walks seconds to millis to micros`() {
        val (cat, ns, _) = fixture(ColumnDef("ts", ColType.TIMESTAMP_S))
        alter.alterTable(cat, ns, "t", listOf(AlterOp.PromoteColumn("ts", ColType.TIMESTAMP_MS)))
        alter.alterTable(cat, ns, "t", listOf(AlterOp.PromoteColumn("ts", ColType.TIMESTAMP)))
        assertThat(catalogs.getTable(cat, ns, "t").columns.single().def.type)
            .isEqualTo(ColType.TIMESTAMP)
    }

    @Test
    fun `the Iceberg-illegal promotions are refused with the types named`() {
        val (cat, ns, _) =
            fixture(
                ColumnDef("u", ColType.UINT32),
                ColumnDef("ts", ColType.TIMESTAMP),
                ColumnDef("j", ColType.JSON),
            )
        // long -> decimal(20,0) is not an Iceberg evolution, so hoglake is
        // stricter than DuckLake here on purpose.
        assertThatThrownBy {
            alter.alterTable(cat, ns, "t", listOf(AlterOp.PromoteColumn("u", ColType.UINT64)))
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("'uint32'")
            .hasMessageContaining("'uint64'")
        // timestamp -> timestamp_ns changes the mapped type AND would
        // change the unit of every already-stored bound.
        assertThatThrownBy {
            alter.alterTable(cat, ns, "t", listOf(AlterOp.PromoteColumn("ts", ColType.TIMESTAMP_NS)))
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("'timestamp_ns'")
        assertThatThrownBy {
            alter.alterTable(cat, ns, "t", listOf(AlterOp.PromoteColumn("j", ColType.STRING)))
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("'json'")
    }

    // ---- promote-time stats re-encode --------------------------------------

    @Test
    fun `a promotion inside one mapped Iceberg type leaves the bounds bytes alone`() {
        // int8 -> int16 -> int all map to Iceberg int, so the 4-byte bound
        // is already correct at every step. A re-encode here would be a
        // bug (it would have to invent a width).
        val (cat, ns, catalogId) = fixture(ColumnDef("a", ColType.INT8))
        val field = fieldId(cat, "a")
        seedBounds(catalogId, cat, field, IcebergSingleValue.encodeInt(-7), IcebergSingleValue.encodeInt(9))

        alter.alterTable(cat, ns, "t", listOf(AlterOp.PromoteColumn("a", ColType.INT16)))
        alter.alterTable(cat, ns, "t", listOf(AlterOp.PromoteColumn("a", ColType.INT)))

        val (lower, upper) = readBounds(catalogId, field)
        assertThat(lower).isEqualTo(IcebergSingleValue.encodeInt(-7))
        assertThat(upper).isEqualTo(IcebergSingleValue.encodeInt(9))
    }

    @Test
    fun `a promotion out of the int mapping widens the bounds, unsigned values included`() {
        val (cat, ns, catalogId) = fixture(ColumnDef("b", ColType.UINT16))
        val field = fieldId(cat, "b")
        seedBounds(
            catalogId,
            cat,
            field,
            IcebergSingleValue.encodeInt(0),
            IcebergSingleValue.encodeInt(65_535),
        )

        alter.alterTable(cat, ns, "t", listOf(AlterOp.PromoteColumn("b", ColType.LONG)))

        val (lower, upper) = readBounds(catalogId, field)
        assertThat(lower).isEqualTo(IcebergSingleValue.encodeLong(0L))
        assertThat(upper).isEqualTo(IcebergSingleValue.encodeLong(65_535L))
    }

    @Test
    fun `the timestamp precision ladder leaves micros bounds untouched`() {
        // All three map to Iceberg timestamp and all three STORE micros,
        // so the declared precision change is metadata only.
        val (cat, ns, catalogId) = fixture(ColumnDef("ts", ColType.TIMESTAMP_S))
        val field = fieldId(cat, "ts")
        val lo = IcebergSingleValue.encodeTimestampMicros(-1_500_000L)
        val hi = IcebergSingleValue.encodeTimestampMicros(2_000_000L)
        seedBounds(catalogId, cat, field, lo, hi)

        alter.alterTable(cat, ns, "t", listOf(AlterOp.PromoteColumn("ts", ColType.TIMESTAMP_MS)))
        alter.alterTable(cat, ns, "t", listOf(AlterOp.PromoteColumn("ts", ColType.TIMESTAMP)))

        val (lower, upper) = readBounds(catalogId, field)
        assertThat(lower).isEqualTo(lo)
        assertThat(upper).isEqualTo(hi)
    }

    // ---- partition-transform type gates ------------------------------------

    @Test
    fun `every timestamp precision is a valid temporal transform source`() {
        for (type in listOf(ColType.TIMESTAMP_S, ColType.TIMESTAMP_MS, ColType.TIMESTAMP_NS)) {
            val (cat, ns, _) = fixture(ColumnDef("ts", type))
            val field = fieldId(cat, "ts")
            for (transform in listOf(Transform.YEAR, Transform.MONTH, Transform.DAY, Transform.HOUR)) {
                val info =
                    alter.alterTable(
                        cat,
                        ns,
                        "t",
                        listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(field, transform)))),
                    )
                assertThat(info.partitionSpec!!.fields.single().transform)
                    .describedAs("%s on %s", transform.wire, type.wire)
                    .isEqualTo(transform)
            }
        }
    }

    @Test
    fun `a temporal transform on a non-temporal type is still refused`() {
        val (cat, ns, _) = fixture(ColumnDef("u", ColType.UINT64))
        val field = fieldId(cat, "u")
        assertThatThrownBy {
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(field, Transform.DAY)))),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("'uint64'")
    }

    @Test
    fun `bucket accepts every new type except json`() {
        for (type in listOf(
            ColType.INT8, ColType.INT16, ColType.UINT8, ColType.UINT16,
            ColType.UINT32, ColType.UINT64,
            ColType.TIMESTAMP_S, ColType.TIMESTAMP_MS, ColType.TIMESTAMP_NS,
        )) {
            val (cat, ns, _) = fixture(ColumnDef("v", type))
            val field = fieldId(cat, "v")
            val info =
                alter.alterTable(
                    cat,
                    ns,
                    "t",
                    listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(field, Transform.BUCKET, 8)))),
                )
            assertThat(info.partitionSpec!!.fields.single().transformParam)
                .describedAs(type.wire)
                .isEqualTo(8)
        }
    }

    @Test
    fun `bucket is refused on json and on the Iceberg-unbucketable types`() {
        // json: two documents equal as JSON hash differently, so bucketing
        // would scatter equal values and prune wrong. boolean/float/double:
        // the Iceberg spec excludes them outright.
        for (type in listOf(ColType.JSON, ColType.BOOLEAN, ColType.FLOAT, ColType.DOUBLE)) {
            val (cat, ns, _) = fixture(ColumnDef("v", type))
            val field = fieldId(cat, "v")
            assertThatThrownBy {
                alter.alterTable(
                    cat,
                    ns,
                    "t",
                    listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(field, Transform.BUCKET, 8)))),
                )
            }.describedAs(type.wire)
                .isInstanceOf(HoglakeException.Validation::class.java)
                .hasMessageContaining("'${type.wire}'")
                .hasMessageContaining("bucket")
        }
    }

    @Test
    fun `identity partitioning on a json column is allowed - the one transform it supports`() {
        val (cat, ns, _) = fixture(ColumnDef("v", ColType.JSON))
        val field = fieldId(cat, "v")
        val info =
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(field, Transform.IDENTITY)))),
            )
        assertThat(info.partitionSpec!!.fields.single().transform).isEqualTo(Transform.IDENTITY)
    }

    // ---- fixtures ----------------------------------------------------------

    /** One data file with one stats row for [field], so promote has work to do. */
    private fun seedBounds(
        catalogId: Long,
        cat: String,
        field: Long,
        lower: ByteArray,
        upper: ByteArray,
    ) {
        val tableId = catalogs.getTable(cat, "ns", "t").tableId
        db.jdbi.withHandleUnchecked { h ->
            h.execute(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                    path, record_count, file_size_bytes, row_id_start)
                VALUES (?, 1, ?, 2, 's3://bucket/$cat.parquet', 10, 100, 0)
                """,
                catalogId,
                tableId,
            )
            h.execute(
                """
                INSERT INTO hog_file_column_stats
                    (catalog_id, data_file_id, field_id, value_count, null_count, lower_bound, upper_bound)
                VALUES (?, 1, ?, 10, 0, ?, ?)
                """,
                catalogId,
                field,
                lower,
                upper,
            )
        }
    }

    private fun readBounds(
        catalogId: Long,
        field: Long,
    ): Pair<ByteArray, ByteArray> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT lower_bound, upper_bound FROM hog_file_column_stats
                WHERE catalog_id = ? AND data_file_id = 1 AND field_id = ?
                """,
            ).bind(0, catalogId)
                .bind(1, field)
                .map { rs, _ -> rs.getBytes("lower_bound") to rs.getBytes("upper_bound") }
                .one()
        }
}
