package com.posthog.hoglake.hydrator

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.stats.IcebergSingleValue
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.testing.TestImages
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.containers.MinIOContainer
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files

/**
 * The regression fence for the unsigned-bounds bug class, walked end to
 * end through the real services rather than asserted on a hand-built
 * footer.
 *
 * The scenario is ordinary, which is the point. A client declares a
 * column as `long` and writes it the way pyarrow and DuckDB natively
 * write unsigned 32-bit data — parquet INT32 with INT(32, unsigned) —
 * then registers with DEFERRED stats, so the HYDRATOR, not the writer,
 * produces the bounds. Nothing here is a type error: a long column's
 * domain contains every uint32 value, and the file is exactly what a
 * foreign writer emits.
 *
 * Read with a sign extension, the file's max of 4294967295 became -1:
 * an upper bound BELOW the lower bound, which every range pruner reads
 * as "no rows can match" — the file silently disappears from scans while
 * every count and status field still looks healthy.
 *
 * (This used to arrive via a uint32 -> long PROMOTION. That rung left
 * the matrix when PROMOTIONS was pinned to DuckLake's documented set,
 * but the defect never depended on it: the hazard is a foreign file's
 * annotation, and declaring the column long up front reaches it just as
 * directly.)
 */
@Tag("integration")
class UnsignedPromotionHydrationIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val jdbi get() = db.jdbi

    private val catalogs by lazy { CatalogService(jdbi) }
    private val commits by lazy { CommitService(jdbi) }
    private val hydrator by lazy { Hydrator(jdbi, store) }

    @AfterEach
    fun tearDown() = db.close()

    private companion object {
        const val BUCKET = "hoglake-unsigned-test"

        /** The unsigned domain's edges: 0, the sign boundary, and the max. */
        val VALUES = listOf(0, Int.MIN_VALUE, -1) // bits for 0, 2^31, 2^32-1
        const val MIN_UNSIGNED = 0L
        const val MAX_UNSIGNED = 4_294_967_295L

        val minio: MinIOContainer by lazy { TestImages.minio().also { it.start() } }

        val store: ObjectStore by lazy {
            ObjectStore(
                endpoint = minio.s3URL,
                region = "us-east-1",
                accessKey = minio.userName,
                secretKey = minio.password,
                pathStyle = true,
            ).also { it.createBucket(BUCKET) }
        }

        /**
         * A file shaped exactly like pyarrow's or DuckDB's native uint32
         * output: INT32 physical, INT(32, unsigned) logical. parquet-java
         * computes this chunk's min/max in UNSIGNED order because of that
         * annotation, so the stored max really is the 0xFFFFFFFF bits.
         */
        fun unsignedInt32Parquet(fieldId: Int): ByteArray {
            val schema =
                MessageType(
                    "hoglake_test",
                    Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                        .`as`(LogicalTypeAnnotation.intType(32, false))
                        .id(fieldId)
                        .named("v"),
                )
            val tmp = Files.createTempFile("hoglake-unsigned", ".parquet")
            return try {
                Files.deleteIfExists(tmp)
                val factory = SimpleGroupFactory(schema)
                ExampleParquetWriter.builder(LocalOutputFile(tmp))
                    .withType(schema)
                    .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                    .build()
                    .use { w -> for (bits in VALUES) w.write(factory.newGroup().append("v", bits)) }
                Files.readAllBytes(tmp)
            } finally {
                Files.deleteIfExists(tmp)
            }
        }

        fun footerSizeOf(bytes: ByteArray): Long =
            ByteBuffer.wrap(bytes, bytes.size - 8, 4).order(ByteOrder.LITTLE_ENDIAN).int.toLong()
    }

    @Test
    fun `a foreign unsigned int32 file hydrates correctly under a long column`() {
        val cat = "unsigned-long"
        catalogs.createCatalog(cat, "s3://$BUCKET/")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("v", ColType.LONG)))
        val fieldId = catalogs.getTable(cat, "ns", "t").columns.single().fieldId

        val bytes = unsignedInt32Parquet(Math.toIntExact(fieldId))
        val path = "s3://$BUCKET/t/unsigned.parquet"
        store.put(path, bytes)
        commits.commit(
            cat,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "t",
                            listOf(
                                FileRegistration(
                                    path = path,
                                    recordCount = VALUES.size.toLong(),
                                    fileSizeBytes = bytes.size.toLong(),
                                    footerSize = footerSizeOf(bytes),
                                    // Deferred stats: the HYDRATOR decodes this
                                    // footer, which is the path under test.
                                    columnStats = null,
                                ),
                            ),
                        ),
                    ),
            ),
        )

        assertThat(hydrator.runOnce()).isEqualTo(1)

        val (lower, upper) = boundsOf(cat, fieldId)
        assertThat(lower).describedAs("lower bound was written").isNotNull()
        assertThat(upper).describedAs("upper bound was written").isNotNull()

        // Decode under the LIVE column type, exactly as a pruner does.
        val live = catalogs.getTable(cat, "ns", "t").columns.single().def.type
        val lo = IcebergSingleValue.decode(live, lower!!) as Long
        val hi = IcebergSingleValue.decode(live, upper!!) as Long

        // The invariant that the bug broke, asserted first because it is
        // the one that makes the file vanish from scans.
        assertThat(lo)
            .describedAs("lower bound must not exceed the upper bound")
            .isLessThanOrEqualTo(hi)
        assertThat(lo).isEqualTo(MIN_UNSIGNED)
        assertThat(hi).describedAs("0xFFFFFFFF must zero-extend, not become -1").isEqualTo(MAX_UNSIGNED)
    }

    @Test
    fun `the same file under a uint32 column produces byte-identical bounds`() {
        // The control. uint32 and long share a mapped Iceberg type, so
        // the DECLARED type must not change a single byte of the stored
        // bound — if it did, the two columns would prune differently over
        // identical data, and the "bounds are the mapped type's encoding"
        // invariant would be false.
        val cat = "unsigned-uint32"
        catalogs.createCatalog(cat, "s3://$BUCKET/")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("v", ColType.UINT32)))
        val fieldId = catalogs.getTable(cat, "ns", "t").columns.single().fieldId

        val bytes = unsignedInt32Parquet(Math.toIntExact(fieldId))
        val path = "s3://$BUCKET/t2/unsigned.parquet"
        store.put(path, bytes)
        commits.commit(
            cat,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "t",
                            listOf(
                                FileRegistration(
                                    path = path,
                                    recordCount = VALUES.size.toLong(),
                                    fileSizeBytes = bytes.size.toLong(),
                                    footerSize = footerSizeOf(bytes),
                                    columnStats = null,
                                ),
                            ),
                        ),
                    ),
            ),
        )
        assertThat(hydrator.runOnce()).isEqualTo(1)

        val (lower, upper) = boundsOf(cat, fieldId)
        assertThat(IcebergSingleValue.decode(ColType.UINT32, lower!!)).isEqualTo(MIN_UNSIGNED)
        assertThat(IcebergSingleValue.decode(ColType.UINT32, upper!!)).isEqualTo(MAX_UNSIGNED)

        // The same bytes the long column stored for the same file.
        assertThat(lower).isEqualTo(IcebergSingleValue.encode(ColType.LONG, MIN_UNSIGNED))
        assertThat(upper).isEqualTo(IcebergSingleValue.encode(ColType.LONG, MAX_UNSIGNED))
    }

    /** The one live stats row's bounds, straight from the hydrator's output. */
    private fun boundsOf(
        catalog: String,
        fieldId: Long,
    ): Pair<ByteArray?, ByteArray?> =
        jdbi.withHandle<Pair<ByteArray?, ByteArray?>, Exception> { h ->
            h.createQuery(
                """
                SELECT s.lower_bound, s.upper_bound
                FROM hog_file_column_stats s
                JOIN hog_catalog c ON c.catalog_id = s.catalog_id
                WHERE c.name = :catalog AND s.field_id = :fieldId
                """,
            )
                .bind("catalog", catalog)
                .bind("fieldId", fieldId)
                .map { rs, _ -> rs.getBytes("lower_bound") to rs.getBytes("upper_bound") }
                .one()
        }
}
