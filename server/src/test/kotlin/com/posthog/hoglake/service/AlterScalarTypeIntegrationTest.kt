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
    fun `the signed and unsigned ladders walk to their own ends`() {
        val (cat, ns, _) =
            fixture(
                ColumnDef("a", ColType.INT8),
                ColumnDef("b", ColType.UINT8),
            )
        // Signed: int8 -> int16 -> int -> long.
        alter.alterTable(cat, ns, "t", listOf(AlterOp.PromoteColumn("a", ColType.INT16)))
        alter.alterTable(cat, ns, "t", listOf(AlterOp.PromoteColumn("a", ColType.INT)))
        alter.alterTable(cat, ns, "t", listOf(AlterOp.PromoteColumn("a", ColType.LONG)))
        // Unsigned: uint8 -> uint16 -> uint32, and STOPS there. DuckLake
        // offers uint32 -> uint64, but uint64 maps to decimal(20,0) and
        // long -> decimal is not an Iceberg evolution.
        alter.alterTable(cat, ns, "t", listOf(AlterOp.PromoteColumn("b", ColType.UINT16)))
        alter.alterTable(cat, ns, "t", listOf(AlterOp.PromoteColumn("b", ColType.UINT32)))
        val live = catalogs.getTable(cat, ns, "t").columns.associate { it.def.name to it.def.type }
        assertThat(live["a"]).isEqualTo(ColType.LONG)
        assertThat(live["b"]).isEqualTo(ColType.UINT32)
    }

    @Test
    fun `uint8 reaches uint32 in one step as well as two`() {
        val (cat, ns, _) = fixture(ColumnDef("b", ColType.UINT8))
        alter.alterTable(cat, ns, "t", listOf(AlterOp.PromoteColumn("b", ColType.UINT32)))
        assertThat(catalogs.getTable(cat, ns, "t").columns.single().def.type)
            .isEqualTo(ColType.UINT32)
    }

    @Test
    fun `promotions outside DuckLake's table are refused even when value-preserving`() {
        val (cat, ns, _) =
            fixture(
                ColumnDef("u8", ColType.UINT8),
                ColumnDef("u32", ColType.UINT32),
                ColumnDef("ts", ColType.TIMESTAMP_S),
            )
        // uint8 -> int loses nothing (255 fits int32), but DuckLake has no
        // unsigned -> signed rung at all, and a hoglake catalog must not
        // accept DDL a DuckLake client would reject.
        assertThatThrownBy {
            alter.alterTable(cat, ns, "t", listOf(AlterOp.PromoteColumn("u8", ColType.INT)))
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("'uint8'")
            .hasMessageContaining("'int'")
        // The timestamp precision ladder is pure metadata and still not
        // something DuckLake offers.
        assertThatThrownBy {
            alter.alterTable(cat, ns, "t", listOf(AlterOp.PromoteColumn("ts", ColType.TIMESTAMP_MS)))
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("'timestamp_s'")
        // And uint32 -> long, which used to be allowed here on a
        // same-mapped-type argument DuckLake's table does not share.
        assertThatThrownBy {
            alter.alterTable(cat, ns, "t", listOf(AlterOp.PromoteColumn("u32", ColType.LONG)))
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("'uint32'")
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
        // uint16 -> uint32 is int -> long in Iceberg terms, so the 4-byte
        // bound must become 8 bytes even though neither type name says
        // "long". Keying the re-encode on the MAPPED type is what gets
        // this right without a special case.
        val (cat, ns, catalogId) = fixture(ColumnDef("b", ColType.UINT16))
        val field = fieldId(cat, "b")
        seedBounds(
            catalogId,
            cat,
            field,
            IcebergSingleValue.encodeInt(0),
            IcebergSingleValue.encodeInt(65_535),
        )

        alter.alterTable(cat, ns, "t", listOf(AlterOp.PromoteColumn("b", ColType.UINT32)))

        val (lower, upper) = readBounds(catalogId, field)
        assertThat(lower).isEqualTo(IcebergSingleValue.encodeLong(0L))
        assertThat(upper).isEqualTo(IcebergSingleValue.encodeLong(65_535L))
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
    fun `bucket accepts the new small integer widths`() {
        for (type in listOf(ColType.INT8, ColType.INT16, ColType.UINT8, ColType.UINT16)) {
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
    fun `bucket is refused outside the Iceberg hash domain`() {
        // boolean/float/double really are outside Appendix B's hash domain.
        for (type in listOf(ColType.BOOLEAN, ColType.FLOAT, ColType.DOUBLE)) {
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
                .hasMessageContaining("excludes it from the bucket hash domain")
        }
    }

    @Test
    fun `the json bucket refusal blames JSON's byte forms, not the Iceberg spec`() {
        // json maps to Iceberg string, and Iceberg buckets strings
        // perfectly well — so "the spec excludes it" was simply false,
        // and a caller who knew the spec would have read it as a hoglake
        // bug. The real reason is that JSON has no canonical byte form,
        // which makes the hash depend on the writer rather than the value.
        val (cat, ns, _) = fixture(ColumnDef("v", ColType.JSON))
        val field = fieldId(cat, "v")
        assertThatThrownBy {
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(field, Transform.BUCKET, 8)))),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("'json'")
            .hasMessageContaining("equal")
            .hasMessageContaining("different bytes")
            .hasMessageContaining("which writer serialized the value")
            // The claim that was wrong must not come back.
            .hasMessageNotContaining("spec excludes")
    }

    @Test
    fun `bucketable types are an explicit allowlist, so a new ColType is not bucketable`() {
        // The set used to be `entries - exclusions`, which made every
        // future type bucketable by default — the opposite of the
        // admit-deliberately policy. Pinned by CONTENTS so that adding a
        // type to the vocabulary cannot quietly add it here too.
        assertThat(AlterService.BUCKETABLE_TYPES).containsExactlyInAnyOrder(
            ColType.INT8, ColType.INT16, ColType.INT, ColType.LONG,
            ColType.UINT8, ColType.UINT16, ColType.DECIMAL, ColType.DATE,
            ColType.TIME, ColType.TIMESTAMP, ColType.TIMESTAMPTZ,
            ColType.STRING, ColType.UUID_T, ColType.BINARY,
        )
        // The fail-closed property, stated directly: every member of the
        // vocabulary that is NOT named above must be refused.
        val notBucketable = ColType.entries.toSet() - AlterService.BUCKETABLE_TYPES
        assertThat(notBucketable).containsExactlyInAnyOrder(
            ColType.BOOLEAN, ColType.FLOAT, ColType.DOUBLE, ColType.JSON, ColType.VARIANT,
            ColType.UINT32, ColType.UINT64,
            ColType.TIMESTAMP_S, ColType.TIMESTAMP_MS, ColType.TIMESTAMP_NS,
        )
        assertThat(AlterService.BUCKETABLE_TYPES + notBucketable)
            .describedAs("every type is classified exactly once")
            .isEqualTo(ColType.entries.toSet())
    }

    @Test
    fun `bucket is refused on the mapped-hash types, naming that as the reason`() {
        // uint32, uint64 and the three timestamp precisions are excluded
        // for a DIFFERENT reason from json's, and the message has to say
        // which: Iceberg hashes the MAPPED type's representation, hoglake
        // has no cross-language contract for that hash, and the server
        // never computes bucket values itself — it only stores the
        // strings clients send. A generic "not bucketable" would send a
        // caller hunting for a syntax error.
        val expected =
            mapOf(
                ColType.UINT32 to "long",
                ColType.UINT64 to "decimal",
                ColType.TIMESTAMP_S to "timestamp",
                ColType.TIMESTAMP_MS to "timestamp",
                ColType.TIMESTAMP_NS to "timestamp_ns",
            )
        for ((type, mapped) in expected) {
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
                .hasMessageContaining("MAPPED type")
                .hasMessageContaining("'$mapped'")
                .hasMessageContaining("cross-language contract")
        }
    }

    @Test
    fun `the mapped-hash types still take identity and the temporal transforms`() {
        // Withdrawing bucket must not withdraw everything: these columns
        // stay partitionable, just not by hash.
        for (type in listOf(ColType.UINT32, ColType.UINT64, ColType.TIMESTAMP_NS)) {
            val (cat, ns, _) = fixture(ColumnDef("v", type))
            val field = fieldId(cat, "v")
            val info =
                alter.alterTable(
                    cat,
                    ns,
                    "t",
                    listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(field, Transform.IDENTITY)))),
                )
            assertThat(info.partitionSpec!!.fields.single().transform)
                .describedAs(type.wire)
                .isEqualTo(Transform.IDENTITY)
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
