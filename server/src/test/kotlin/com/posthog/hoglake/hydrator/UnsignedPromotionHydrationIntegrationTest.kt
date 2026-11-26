package com.posthog.hoglake.hydrator

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.stats.IcebergSingleValue
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.testing.TestImages
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.containers.MinIOContainer
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

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

        /** A parquet file of [schema] holding [rows], written without field ids unless [schema] has them. */
        fun parquetOf(
            schema: MessageType,
            rows: (SimpleGroupFactory) -> List<Group>,
        ): ByteArray {
            val tmp = Files.createTempFile("hoglake-promote", ".parquet")
            return try {
                Files.deleteIfExists(tmp)
                ExampleParquetWriter.builder(LocalOutputFile(tmp))
                    .withType(schema)
                    .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                    .build()
                    .use { w -> rows(SimpleGroupFactory(schema)).forEach(w::write) }
                Files.readAllBytes(tmp)
            } finally {
                Files.deleteIfExists(tmp)
            }
        }

        /** An id-less file of one INT32 column `v`. */
        fun idlessInts(vararg values: Int): ByteArray =
            parquetOf(MessageType("hoglake_test", Types.optional(PrimitiveType.PrimitiveTypeName.INT32).named("v"))) {
                    factory ->
                values.map { factory.newGroup().append("v", it) }
            }
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

    @Test
    fun `an id-less file pending across an int to long promote gets 8-byte bounds`() {
        // The promote re-encodes only stats that exist; this file gets its
        // own in the sweep, bound by name at its begin_snapshot, where v
        // is still an INT. Its bounds must be the live LONG's width.
        val cat = "idless-promote"
        catalogs.createCatalog(cat, "s3://$BUCKET/")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("v", ColType.INT)))
        val fieldId = catalogs.getTable(cat, "ns", "t").columns.single().fieldId

        commitDeferred(cat, mapOf("s3://$BUCKET/t3/idless.parquet" to idlessInts(-3, 7)))
        AlterService(jdbi).alterTable(cat, "ns", "t", listOf(AlterOp.PromoteColumn("v", ColType.LONG)))

        assertThat(hydrator.runOnce()).isEqualTo(1)

        val (lower, upper) = boundsOf(cat, fieldId)
        assertThat(lower).isEqualTo(IcebergSingleValue.encode(ColType.LONG, -3L))
        assertThat(upper).isEqualTo(IcebergSingleValue.encode(ColType.LONG, 7L))
    }

    @Test
    fun `an id-less file pending across a nested float to double promote gets 8-byte bounds`() {
        // The struct leaf takes its live type by field id at any depth, and
        // float widens like int: a retype of top-level columns only, or of
        // ints only, leaves these bounds at 4 bytes under a live DOUBLE.
        val cat = "idless-nested-promote"
        catalogs.createCatalog(cat, "s3://$BUCKET/")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(
            cat,
            "ns",
            "t",
            listOf(ColumnDef("s", ColType.STRUCT, children = listOf(ColumnDef("f", ColType.FLOAT)))),
        )
        val fieldId =
            catalogs.getTable(cat, "ns", "t").columns.single().children.single().fieldId

        val schema =
            MessageType(
                "hoglake_test",
                Types.optionalGroup()
                    .addField(Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT).named("f"))
                    .named("s"),
            )
        val bytes =
            parquetOf(schema) { factory ->
                listOf(1.5f, -2.25f).map { v -> factory.newGroup().also { it.addGroup("s").append("f", v) } }
            }
        commitDeferred(cat, mapOf("s3://$BUCKET/t4/idless-nested.parquet" to bytes))
        AlterService(jdbi).alterTable(cat, "ns", "t", listOf(AlterOp.PromoteColumn("s.f", ColType.DOUBLE)))

        assertThat(hydrator.runOnce()).isEqualTo(1)

        val (lower, upper) = boundsOf(cat, fieldId)
        assertThat(lower).isEqualTo(IcebergSingleValue.encode(ColType.DOUBLE, -2.25))
        assertThat(upper).isEqualTo(IcebergSingleValue.encode(ColType.DOUBLE, 1.5))
    }

    @Test
    fun `a promote that commits mid-sweep still leaves the sweep's bounds at 8 bytes`() {
        // The race the promote stats fence closes (#729): file a is bound
        // under INT and its 4-byte bounds written, then the promote commits
        // before the sweep does, so its stats scan never sees them. The
        // sweep's re-check before its COMMIT must widen them; file b, read
        // after the promote, binds the LONG directly.
        val cat = "promote-mid-sweep"
        catalogs.createCatalog(cat, "s3://$BUCKET/")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("v", ColType.INT)))
        val fieldId = catalogs.getTable(cat, "ns", "t").columns.single().fieldId
        val a = "s3://$BUCKET/t5/a.parquet"
        val b = "s3://$BUCKET/t5/b.parquet"
        commitDeferred(cat, linkedMapOf(a to idlessInts(-3, 7), b to idlessInts(1, 2)))

        val fetched = mutableListOf<String>()
        val racing =
            object : ObjectStore(minio.s3URL, "us-east-1", minio.userName, minio.password, true) {
                fun fetching(pathUri: String) {
                    if (pathUri == b && a in fetched && b !in fetched) {
                        // On another connection, bounded: a promote that waited
                        // on this sweep would never return and fail here.
                        CompletableFuture.runAsync {
                            AlterService(
                                jdbi,
                            ).alterTable(cat, "ns", "t", listOf(AlterOp.PromoteColumn("v", ColType.LONG)))
                        }.get(30, TimeUnit.SECONDS)
                    }
                    fetched += pathUri
                }

                override fun get(pathUri: String): ByteArray {
                    fetching(pathUri)
                    return super.get(pathUri)
                }

                override fun getTail(
                    pathUri: String,
                    startInclusive: Long,
                ): ByteArray {
                    fetching(pathUri)
                    return super.getTail(pathUri, startInclusive)
                }
            }

        assertThat(Hydrator(jdbi, racing).runOnce()).isEqualTo(2)

        assertThat(fetched).containsExactly(a, b)
        assertLongBounds(boundsOf(cat, fieldId, a), -3L, 7L)
        assertLongBounds(boundsOf(cat, fieldId, b), 1L, 2L)
    }

    @Test
    fun `a promote that reaches its stats scan during a sweep's re-check waits for the sweep's commit`() {
        // The other order: the sweep holds the fence for its re-check and has
        // not committed its 4-byte row, which the promote's scan cannot see
        // yet. The promote must wait for that COMMIT, then widen the row.
        val cat = "promote-waits"
        catalogs.createCatalog(cat, "s3://$BUCKET/")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("v", ColType.INT)))
        val fieldId = catalogs.getTable(cat, "ns", "t").columns.single().fieldId
        val path = "s3://$BUCKET/t6/a.parquet"
        commitDeferred(cat, mapOf(path to idlessInts(-3, 7)))
        val (catalogId, dataFileId) =
            jdbi.withHandle<Pair<Long, Long>, Exception> { h ->
                h.createQuery("SELECT catalog_id, data_file_id FROM hog_data_file WHERE path = :path")
                    .bind("path", path)
                    .map { rs, _ -> rs.getLong("catalog_id") to rs.getLong("data_file_id") }
                    .one()
            }

        lateinit var promote: CompletableFuture<Void>
        jdbi.useTransaction<Exception> { h ->
            // The sweep's tail, by hand: the shared fence, then its row.
            Locks.acquirePromoteStatsFenceShared(h, catalogId)
            h.createUpdate(
                """
                INSERT INTO hog_file_column_stats
                    (catalog_id, data_file_id, field_id, value_count, null_count, lower_bound, upper_bound)
                VALUES (:catalogId, :dataFileId, :fieldId, 2, 0, :lower, :upper)
                """,
            )
                .bind("catalogId", catalogId)
                .bind("dataFileId", dataFileId)
                .bind("fieldId", fieldId)
                .bind("lower", IcebergSingleValue.encode(ColType.INT, -3))
                .bind("upper", IcebergSingleValue.encode(ColType.INT, 7))
                .execute()
            promote =
                CompletableFuture.runAsync {
                    AlterService(jdbi).alterTable(cat, "ns", "t", listOf(AlterOp.PromoteColumn("v", ColType.LONG)))
                }
            await().atMost(Duration.ofSeconds(30)).until { fenceWaiters(catalogId) == 1 }
            assertThat(promote).isNotDone()
        }
        promote.get(30, TimeUnit.SECONDS)

        assertLongBounds(boundsOf(cat, fieldId), -3L, 7L)
    }

    @Test
    fun `a sweep whose re-check reaches a promote's stats scan waits for the promote's commit`() {
        // The hydrator's side of the fence: the promote holds it and has
        // run its stats scan, which could not see the sweep's uncommitted
        // 4-byte row, and its LONG is not committed yet either, so the
        // sweep binds the INT. The sweep's re-check must wait for that
        // COMMIT and then read the LONG; a re-check without the fence, or
        // with the live read before it, commits the 4-byte row.
        val cat = "sweep-waits"
        catalogs.createCatalog(cat, "s3://$BUCKET/")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("v", ColType.INT)))
        val fieldId = catalogs.getTable(cat, "ns", "t").columns.single().fieldId
        commitDeferred(cat, mapOf("s3://$BUCKET/t7/a.parquet" to idlessInts(-3, 7)))
        val catalogId = catalogs.getCatalog(cat).catalogId

        lateinit var sweep: CompletableFuture<Int>
        jdbi.useTransaction<Exception> { h ->
            // The promote, by hand: the exclusive fence, then the column's
            // INT row ended and its LONG row begun at the next snapshot.
            Locks.acquirePromoteStatsFence(h, catalogId)
            val snapshot =
                h.createQuery("SELECT last_snapshot_id + 1 FROM hog_catalog WHERE catalog_id = :catalogId")
                    .bind("catalogId", catalogId)
                    .mapTo(Long::class.javaObjectType)
                    .one()
            h.createUpdate(
                """
                UPDATE hog_column SET end_snapshot = :snapshot
                WHERE catalog_id = :catalogId AND field_id = :fieldId AND end_snapshot IS NULL
                """,
            )
                .bind("snapshot", snapshot)
                .bind("catalogId", catalogId)
                .bind("fieldId", fieldId)
                .execute()
            h.createUpdate(
                """
                INSERT INTO hog_column (catalog_id, table_id, field_id, begin_snapshot, name, col_type,
                                        type_params, nullable, ordinal, parent_field_id)
                SELECT catalog_id, table_id, field_id, :snapshot, name, 'long',
                       type_params, nullable, ordinal, parent_field_id
                FROM hog_column
                WHERE catalog_id = :catalogId AND field_id = :fieldId AND end_snapshot = :snapshot
                """,
            )
                .bind("snapshot", snapshot)
                .bind("catalogId", catalogId)
                .bind("fieldId", fieldId)
                .execute()
            sweep = CompletableFuture.supplyAsync { hydrator.runOnce() }
            await().atMost(Duration.ofSeconds(30)).until { fenceWaiters(catalogId) == 1 }
            assertThat(sweep).isNotDone()
        }

        assertThat(sweep.get(30, TimeUnit.SECONDS)).isEqualTo(1)
        assertLongBounds(boundsOf(cat, fieldId), -3L, 7L)
    }

    /** [bounds] are [lower]..[upper] in the 8-byte LONG encoding. */
    private fun assertLongBounds(
        bounds: Pair<ByteArray?, ByteArray?>,
        lower: Long,
        upper: Long,
    ) {
        assertThat(bounds.first).isEqualTo(IcebergSingleValue.encode(ColType.LONG, lower))
        assertThat(bounds.second).isEqualTo(IcebergSingleValue.encode(ColType.LONG, upper))
    }

    /** Sessions waiting on [catalogId]'s promote stats fence. */
    private fun fenceWaiters(catalogId: Long): Int =
        jdbi.withHandle<Int, Exception> { h ->
            h.createQuery(
                """
                SELECT count(*) FROM pg_locks
                WHERE locktype = 'advisory' AND NOT granted
                  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
                  AND classid::bigint = :cls AND objid::bigint = :catalogId AND objsubid = 1
                """,
            )
                .bind("cls", Locks.PROMOTE_STATS_FENCE_LOCK_CLASS.toLong())
                .bind("catalogId", catalogId)
                .mapTo(Int::class.javaObjectType)
                .one()
        }

    /**
     * Registers [files] (path -> bytes, in order, [rows] rows each) in one
     * append to `ns.t`, stats deferred to the hydrator.
     */
    private fun commitDeferred(
        catalog: String,
        files: Map<String, ByteArray>,
        rows: Long = 2,
    ) {
        for ((path, bytes) in files) store.put(path, bytes)
        commits.commit(
            catalog,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "t",
                            files.map { (path, bytes) ->
                                FileRegistration(path, rows, bytes.size.toLong(), footerSizeOf(bytes))
                            },
                        ),
                    ),
            ),
        )
    }

    /** The one live stats row's bounds, straight from the hydrator's output. */
    private fun boundsOf(
        catalog: String,
        fieldId: Long,
        path: String? = null,
    ): Pair<ByteArray?, ByteArray?> =
        jdbi.withHandle<Pair<ByteArray?, ByteArray?>, Exception> { h ->
            h.createQuery(
                """
                SELECT s.lower_bound, s.upper_bound
                FROM hog_file_column_stats s
                JOIN hog_catalog c ON c.catalog_id = s.catalog_id
                JOIN hog_data_file f ON f.catalog_id = s.catalog_id AND f.data_file_id = s.data_file_id
                WHERE c.name = :catalog AND s.field_id = :fieldId
                  AND (CAST(:path AS text) IS NULL OR f.path = :path)
                """,
            )
                .bind("catalog", catalog)
                .bind("fieldId", fieldId)
                .bind("path", path)
                .map { rs, _ -> rs.getBytes("lower_bound") to rs.getBytes("upper_bound") }
                .one()
        }
}
