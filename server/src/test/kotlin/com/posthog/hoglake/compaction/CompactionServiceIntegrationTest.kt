package com.posthog.hoglake.compaction

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ChangeKind
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.CleanupService
import com.posthog.hoglake.service.ExpiryService
import com.posthog.hoglake.service.RemovalStore
import com.posthog.hoglake.service.ScanService
import com.posthog.hoglake.service.VerifyService
import com.posthog.hoglake.stats.IcebergSingleValue
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.testing.TestImages
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.MinIOContainer
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

/**
 * End-to-end compaction against real parquet + puffin in MinIO: rewrite
 * correctness (explicit row ids across non-adjacent inputs, DV
 * application, sort-order application, field ids, heterogeneous-schema
 * groups under the live schema), commit semantics (end-snapshotted
 * inputs AND their dead DVs, time travel, aggregates, the
 * table_compacted change, the staging-ticket lifecycle), typed stats
 * aggregation, the changefeed-exclusion contract, the plan-to-commit
 * races (input death, DV appearance, DV supersession), orphan
 * reclamation via the cleanup drain, verify composition, and the
 * expiry lifecycle of compacted-away inputs.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CompactionServiceIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val alter = AlterService(db.jdbi)
    private val scans = ScanService(db.jdbi)
    private val verify = VerifyService(db.jdbi)
    private val counter = AtomicInteger(0)

    // KB-scale ladder for the test fixtures' parquet files (~0.7-1 KiB
    // each): T=4 includes 512/2048/8192. The 2048 quota needs THREE files
    // (2 x ~800B < 2048 < 3 x ~800B), so groups still contain all three.
    private val cfg = CompactionConfig(targetBytes = 8192, tierTarget = 4, maxGroupsPerRun = 10)
    private val svc by lazy { CompactionService(db.jdbi, store, cfg) }
    private val cleanup by lazy { CleanupService(db.jdbi, removalStore) }

    private companion object {
        const val BUCKET = "hoglake-compaction-test"

        val minio: MinIOContainer by lazy {
            TestImages.minio().also { it.start() }
        }

        val store: ObjectStore by lazy {
            ObjectStore(
                endpoint = minio.s3URL,
                region = "us-east-1",
                accessKey = minio.userName,
                secretKey = minio.password,
                pathStyle = true,
            ).also { it.createBucket(BUCKET) }
        }

        val removalStore: RemovalStore by lazy {
            RemovalStore(
                endpoint = minio.s3URL,
                region = "us-east-1",
                accessKey = minio.userName,
                secretKey = minio.password,
                pathStyle = true,
            )
        }
    }

    @AfterAll
    fun tearDown() = db.close()

    // ---- parquet helpers ---------------------------------------------------

    // Table shape for the fixture: id long (field 1, required),
    // name string (2, optional), score double (3, optional).
    private val schema: MessageType =
        Types.buildMessage()
            .addField(Types.required(PrimitiveTypeName.INT64).id(1).named("id"))
            .addField(
                Types.optional(PrimitiveTypeName.BINARY)
                    .`as`(LogicalTypeAnnotation.stringType()).id(2).named("name"),
            )
            .addField(Types.optional(PrimitiveTypeName.DOUBLE).id(3).named("score"))
            .named("t")

    private data class TestRow(val id: Long, val name: String?, val score: Double?)

    private fun parquetBytes(rows: List<TestRow>): ByteArray =
        customParquetBytes(
            schema,
            rows.map { r ->
                { g: Group ->
                    g.add("id", r.id)
                    r.name?.let { g.add("name", it) }
                    r.score?.let { g.add("score", it) }
                }
            },
        )

    private fun customParquetBytes(
        fileSchema: MessageType,
        rows: List<(Group) -> Unit>,
    ): ByteArray {
        val tmp = Files.createTempFile("compact-e2e", ".parquet")
        try {
            Files.delete(tmp)
            val factory = SimpleGroupFactory(fileSchema)
            ExampleParquetWriter.builder(LocalOutputFile(tmp))
                .withType(fileSchema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
                .use { w ->
                    for (fill in rows) {
                        val g = factory.newGroup()
                        fill(g)
                        w.write(g)
                    }
                }
            return Files.readAllBytes(tmp)
        } finally {
            Files.deleteIfExists(tmp)
        }
    }

    private data class OutRow(val id: Long, val name: String?, val score: Double?, val rowId: Long)

    private fun readParquet(bytes: ByteArray): Pair<MessageType, List<OutRow>> {
        var fileSchema: MessageType? = null
        val out = mutableListOf<OutRow>()
        readGroups(bytes) { s, g ->
            fileSchema = s

            fun has(name: String) = g.getFieldRepetitionCount(s.getFieldIndex(name)) > 0
            out +=
                OutRow(
                    id = g.getLong(s.getFieldIndex("id"), 0),
                    name = if (has("name")) g.getString(s.getFieldIndex("name"), 0) else null,
                    score = if (has("score")) g.getDouble(s.getFieldIndex("score"), 0) else null,
                    rowId = g.getLong(s.getFieldIndex(ParquetRewriter.ROW_ID_COLUMN), 0),
                )
        }
        return fileSchema!! to out
    }

    private fun readGroups(
        bytes: ByteArray,
        consume: (MessageType, Group) -> Unit,
    ) {
        val tmp = Files.createTempFile("compact-read", ".parquet")
        try {
            Files.write(tmp, bytes)
            ParquetFileReader.open(LocalInputFile(tmp)).use { reader ->
                val fileSchema = reader.footer.fileMetaData.schema
                val columnIO = ColumnIOFactory().getColumnIO(fileSchema)
                var pages = reader.readNextRowGroup()
                while (pages != null) {
                    val rr = columnIO.getRecordReader(pages, GroupRecordConverter(fileSchema))
                    repeat(Math.toIntExact(pages.rowCount)) { consume(fileSchema, rr.read()) }
                    pages = reader.readNextRowGroup()
                }
            }
        } finally {
            Files.deleteIfExists(tmp)
        }
    }

    /** Every output row's _hog_row_id, in physical order. */
    private fun readRowIds(bytes: ByteArray): List<Long> {
        val ids = mutableListOf<Long>()
        readGroups(bytes) { s, g -> ids += g.getLong(s.getFieldIndex(ParquetRewriter.ROW_ID_COLUMN), 0) }
        return ids
    }

    /** Upload a real puffin DV and register it against [dataFileId]. */
    private fun registerDv(
        cat: String,
        dataFileId: Long,
        path: String,
        positions: List<Long>,
    ) {
        val bytes = PuffinTestFiles.deletionVector(positions)
        store.put(path, bytes)
        commits.commit(
            cat,
            CommitRequest(
                readSnapshot = catalogs.getCatalog(cat).headSnapshotId,
                deletes =
                    listOf(
                        TableDeletes(
                            "ns",
                            "t",
                            listOf(
                                DeleteFileRegistration(dataFileId, path, positions.size.toLong(), bytes.size.toLong()),
                            ),
                        ),
                    ),
            ),
        )
    }

    private data class RemovalRow(
        val path: String,
        val reason: String,
        val drainedOutcome: String?,
    )

    private fun removalRows(cat: String): List<RemovalRow> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT r.path, r.reason, r.drained_outcome FROM hog_file_removal r
                JOIN hog_catalog c ON c.catalog_id = r.catalog_id
                WHERE c.name = :cat ORDER BY r.removal_id
                """,
            )
                .bind("cat", cat)
                .map { rs, _ ->
                    RemovalRow(rs.getString("path"), rs.getString("reason"), rs.getString("drained_outcome"))
                }
                .list()
        }

    private fun assertVerifyPasses(cat: String) {
        val report = verify.runOnce(cat)
        assertThat(report.status)
            .describedAs("verify checks: " + report.checks.joinToString { "${it.check}=${it.violations}" })
            .isEqualTo("pass")
    }

    // ---- fixture -----------------------------------------------------------

    private class Fixture(
        val cat: String,
        val tableId: Long,
        val fileIds: List<Long>,
        val paths: List<String>,
        val dvPath: String?,
        val appendSnapshot: Long,
        val dvSnapshot: Long,
    )

    /**
     * Catalog with table ns.t sorted by score ASC NULLS_LAST; three
     * 5-row files (row-id ranges 0..4 / 5..9 / 10..14) with shipped
     * typed stats. With [dvOnMiddle], a REAL puffin DV on the middle
     * file deleting positions 1 and 3 (row ids 6 and 8) — the group
     * mixes DV-bearing and DV-free inputs.
     */
    @Test
    fun `a malformed pre-existing stats row cannot reach the compaction output`() {
        // The THIRD door. Commit and the hydrator both sanitize; this
        // path wrote hog_file_column_stats directly, so a row stored
        // before the sanitizer existed could be merged into a new
        // file's metadata and stay live for another compaction
        // generation. Readers prune on these.
        val fx = fixture(dvOnMiddle = false)

        // A MIXED population, which is the only one that tests what the
        // comment claims. Corrupting every input makes input-repair and
        // output-repair indistinguishable: both produce null bounds, so
        // the assertion passes with the input-level repair deleted.
        //
        // Corrupt exactly ONE of the three inputs. The true range across
        // them is -10..5. With the input repaired, the bad file
        // contributes nothing and the merge is null (its counts no
        // longer cover the field). WITHOUT it, min(-10, 500) = -10 and
        // max(5, 100) = 100 — a pair that is NOT inverted, so
        // StatsSanity sees nothing wrong and it is stored live. Every
        // pruner then reads `lower = -10, upper = 100` and drops the
        // file for `WHERE id < -10`... and worse, an inverted-looking
        // pair would at least have been caught. A plausible wrong answer
        // beats a detectable one, which is why this needs its own test.
        val victim =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT min(f.data_file_id) FROM hog_data_file f
                      JOIN hog_catalog c ON c.catalog_id = f.catalog_id
                     WHERE c.name = :cat
                    """,
                ).bind("cat", fx.cat).mapTo(Long::class.java).one()
            }
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                UPDATE hog_file_column_stats s
                   SET lower_bound = :lo, upper_bound = :hi, null_count = value_count + 7
                  FROM hog_catalog c
                 WHERE c.catalog_id = s.catalog_id AND c.name = :cat
                   AND s.field_id = 1 AND s.data_file_id = :victim
                """,
            )
                .bind("cat", fx.cat)
                .bind("victim", victim)
                .bind("lo", longLe(500))
                .bind("hi", longLe(100))
                .execute()
        }

        val result = svc.runOnce(fx.cat, cfg)
        assertThat(result.groupsCompacted).isEqualTo(1)

        val output = catalogs.listFiles(fx.cat, "ns", "t").single()
        val row =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT value_count, null_count, lower_bound, upper_bound
                      FROM hog_file_column_stats s
                      JOIN hog_catalog c ON c.catalog_id = s.catalog_id
                     WHERE c.name = :cat AND s.data_file_id = :fileId AND s.field_id = 1
                    """,
                )
                    .bind("cat", fx.cat)
                    .bind("fileId", output.dataFileId)
                    .map { rs, _ ->
                        listOf(
                            rs.getLong("value_count"),
                            rs.getLong("null_count"),
                        ) to (rs.getBytes("lower_bound") to rs.getBytes("upper_bound"))
                    }
                    .one()
            }
        val (counts, bounds) = row
        assertThat(counts[1])
            .describedAs("null_count must not exceed the value_count it is a subset of")
            .isLessThanOrEqualTo(counts[0])
        // The merged bound must not be the plausible-looking blend of a
        // sound file's minimum with a corrupt file's maximum.
        assertThat(bounds.first to bounds.second)
            .describedAs("a repaired input contributes no bound, so the merge has none")
            .isEqualTo(null to null)
        // The inverted pair is DROPPED, not narrowed: nothing in the row
        // says which of the two was the wrong one, so keeping either
        // would be picking at random — and the kept one would prune.
        assertThat(bounds.first).describedAs("inverted lower bound must not survive").isNull()
        assertThat(bounds.second).describedAs("inverted upper bound must not survive").isNull()
    }

    private fun longLe(v: Long): ByteArray =
        java.nio.ByteBuffer.allocate(8).order(java.nio.ByteOrder.LITTLE_ENDIAN).putLong(v).array()

    private fun fixture(dvOnMiddle: Boolean = true): Fixture {
        val cat = "compact-e2e-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(
            cat,
            "ns",
            "t",
            listOf(
                ColumnDef("id", ColType.LONG, nullable = false),
                ColumnDef("name", ColType.STRING),
                ColumnDef("score", ColType.DOUBLE),
            ),
        )
        alter.alterTable(
            cat,
            "ns",
            "t",
            listOf(AlterOp.SetSortOrder(listOf(SortFieldDef(3, SortDirection.ASC, NullOrder.NULLS_LAST)))),
        )

        // id values deliberately include NEGATIVES: a raw binary min/max of
        // the little-endian encodings would get these bounds wrong.
        val f1 =
            listOf(
                TestRow(-10, "e", 5.0),
                TestRow(-8, "d", 3.0),
                TestRow(-6, null, null),
                TestRow(-4, "b", 8.0),
                TestRow(-2, "a", 1.0),
            )
        val f2 = (0L until 5L).map { TestRow(it, "mid-$it", it.toDouble()) }
        val f3 =
            listOf(
                TestRow(1, "j", 2.0),
                TestRow(2, "i", 9.0),
                TestRow(3, "h", 4.0),
                TestRow(4, null, null),
                TestRow(5, "f", 0.5),
            )

        fun register(
            name: String,
            rows: List<TestRow>,
        ): FileRegistration {
            val bytes = parquetBytes(rows)
            val path = "s3://$BUCKET/$cat/data/ns/t/$name.parquet"
            store.put(path, bytes)
            val nonNullScores = rows.mapNotNull { it.score }
            val nonNullNames = rows.mapNotNull { it.name }
            return FileRegistration(
                path = path,
                recordCount = rows.size.toLong(),
                fileSizeBytes = bytes.size.toLong(),
                columnStats =
                    listOf(
                        ColumnStats(
                            fieldId = 1,
                            valueCount = rows.size.toLong(),
                            nullCount = 0,
                            nanCount = null,
                            sizeBytes = 40,
                            lowerBound = IcebergSingleValue.encodeLong(rows.minOf { it.id }),
                            upperBound = IcebergSingleValue.encodeLong(rows.maxOf { it.id }),
                        ),
                        ColumnStats(
                            fieldId = 2,
                            valueCount = rows.size.toLong(),
                            nullCount = rows.count { it.name == null }.toLong(),
                            nanCount = null,
                            sizeBytes = 50,
                            lowerBound = IcebergSingleValue.encodeString(nonNullNames.min()),
                            upperBound = IcebergSingleValue.encodeString(nonNullNames.max()),
                        ),
                        ColumnStats(
                            fieldId = 3,
                            valueCount = rows.size.toLong(),
                            nullCount = rows.count { it.score == null }.toLong(),
                            nanCount = 0,
                            sizeBytes = 40,
                            lowerBound = IcebergSingleValue.encodeDouble(nonNullScores.min()),
                            upperBound = IcebergSingleValue.encodeDouble(nonNullScores.max()),
                        ),
                    ),
            )
        }

        val regs = listOf(register("f1", f1), register("f2", f2), register("f3", f3))
        val appendSnap =
            commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", regs)))).snapshotId
        val fileIds =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT f.data_file_id FROM hog_data_file f
                    JOIN hog_catalog c ON c.catalog_id = f.catalog_id
                    WHERE c.name = :cat ORDER BY f.row_id_start
                    """,
                )
                    .bind("cat", cat)
                    .mapTo(Long::class.java)
                    .list()
            }
        var dvPath: String? = null
        if (dvOnMiddle) {
            // REAL puffin DV on the middle file: positions 1 and 3 of f2,
            // i.e. row ids 6 and 8 die when the group compacts.
            dvPath = "s3://$BUCKET/$cat/dv/f2.puffin"
            registerDv(cat, fileIds[1], dvPath, listOf(1L, 3L))
        }
        val tableId = catalogs.getTable(cat, "ns", "t").tableId
        return Fixture(
            cat,
            tableId,
            fileIds,
            regs.map { it.path },
            dvPath,
            appendSnap,
            catalogs.getCatalog(cat).headSnapshotId,
        )
    }

    // ---- the big one -------------------------------------------------------

    @Test
    fun `compacts a DV-bearing group end to end - vectors applied, ids preserved, sorted output`() {
        val fx = fixture()
        val result = svc.runOnce(fx.cat, cfg)
        assertThat(result.groupsCompacted).isEqualTo(1)
        assertThat(result.filesIn).isEqualTo(3)
        assertThat(result.filesOut).isEqualTo(1)
        assertThat(result.skippedConflicts).isZero()
        assertThat(result.dvSuperseded).isZero()
        assertThat(result.unconvertibleSchema).isZero()
        assertThat(result.bytesOut).isGreaterThan(0)

        val compactionSnap = catalogs.getCatalog(fx.cat).headSnapshotId

        // -- commit semantics: ALL inputs (the DV-bearing one included)
        //    end-snapshotted at S, visible at S-1.
        val filesAtHead = catalogs.listFiles(fx.cat, "ns", "t")
        assertThat(filesAtHead).hasSize(1)
        val output = filesAtHead.single()
        assertThat(output.explicitRowIds).isTrue()
        assertThat(output.path).contains("/data/ns/t/compacted-")
        assertThat(output.recordCount).isEqualTo(13) // 15 gross - 2 deleted
        assertThat(output.rowIdStart).isEqualTo(0) // min surviving id; positional meaning void
        assertThat(output.beginSnapshot).isEqualTo(compactionSnap)
        // A DV'd input makes the inputs' registered counts wrong for the
        // survivor set: honest 'pending', hydrator fills from the footer.
        assertThat(output.statsState.wire).isEqualTo("pending")
        val before = catalogs.listFiles(fx.cat, "ns", "t", snapshot = compactionSnap - 1)
        assertThat(before.map { it.path }).containsExactlyElementsOf(fx.paths)
        assertThat(before.none { it.explicitRowIds }).isTrue()

        // -- aggregates: gross 15 before, 13 after (the deleted rows are
        //    physically gone from the visible file set).
        assertThat(catalogs.getTable(fx.cat, "ns", "t", snapshot = compactionSnap - 1).recordCount).isEqualTo(15)
        val aggAfter = catalogs.getTable(fx.cat, "ns", "t")
        assertThat(aggAfter.recordCount).isEqualTo(13)
        assertThat(aggAfter.fileCount).isEqualTo(1)

        // -- exactly one table_compacted change row on the snapshot.
        val (page, _) = catalogs.listSnapshots(fx.cat, compactionSnap - 1, 1)
        val changes = page.single().changes
        assertThat(changes).hasSize(1)
        assertThat(changes.single().kind).isEqualTo(ChangeKind.TABLE_COMPACTED)
        assertThat(changes.single().objectId).isEqualTo(fx.tableId)

        // -- the applied DV died with its file: the scan is one explicit
        //    file with NO delete file.
        val scan = scans.planScan(fx.cat, "ns", "t")
        assertThat(scan).hasSize(1)
        assertThat(scan.single().dataFile.explicitRowIds).isTrue()
        assertThat(scan.single().deleteFile).isNull()

        // -- the physical output: sorted by score ASC NULLS_LAST, ids
        //    preserved across the inputs, holes where the DV deleted
        //    (row ids 6 and 8 are gone FOREVER).
        val (outSchema, rows) = readParquet(store.get(output.path))
        assertThat(outSchema.getType("id").id.intValue()).isEqualTo(1)
        assertThat(outSchema.getType("name").id.intValue()).isEqualTo(2)
        assertThat(outSchema.getType("score").id.intValue()).isEqualTo(3)
        assertThat(outSchema.getType(ParquetRewriter.ROW_ID_COLUMN).id.intValue())
            .isEqualTo(ParquetRewriter.ROW_ID_FIELD_ID)
        assertThat(rows.map { it.score })
            .containsExactly(0.0, 0.5, 1.0, 2.0, 2.0, 3.0, 4.0, 4.0, 5.0, 8.0, 9.0, null, null)
        assertThat(rows.map { it.rowId })
            .containsExactly(5L, 14L, 4L, 7L, 10L, 1L, 9L, 12L, 0L, 3L, 11L, 2L, 13L)

        // -- the staging ticket settled as 'registered' in the group's
        //    commit; nothing is left undrained.
        val removals = removalRows(fx.cat)
        assertThat(removals).hasSize(1)
        assertThat(removals.single().path).isEqualTo(output.path)
        assertThat(removals.single().reason).isEqualTo("compaction_staging")
        assertThat(removals.single().drainedOutcome).isEqualTo("registered")

        // -- verify composes with explicit-row-id holes: all green.
        assertVerifyPasses(fx.cat)

        // -- a second run finds nothing left to do.
        val again = svc.runOnce(fx.cat, cfg)
        assertThat(again.groupsCompacted).isZero()
        assertThat(again.skippedConflicts).isZero()
    }

    @Test
    fun `a DV-free group aggregates typed stats from the inputs`() {
        val fx = fixture(dvOnMiddle = false)
        val result = svc.runOnce(fx.cat, cfg)
        assertThat(result.groupsCompacted).isEqualTo(1)
        assertThat(result.filesIn).isEqualTo(3)

        val output = catalogs.listFiles(fx.cat, "ns", "t").single()
        assertThat(output.explicitRowIds).isTrue()
        assertThat(output.recordCount).isEqualTo(15)
        assertThat(output.statsState.wire).isEqualTo("provided")

        // -- typed stats aggregation: signed long bounds span the negative
        //    inputs; string/double bounds merge correctly; counts sum.
        val stats =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT field_id, value_count, null_count, lower_bound, upper_bound
                    FROM hog_file_column_stats s
                    JOIN hog_catalog c ON c.catalog_id = s.catalog_id
                    WHERE c.name = :cat AND s.data_file_id = :fileId
                    """,
                )
                    .bind("cat", fx.cat)
                    .bind("fileId", output.dataFileId)
                    .map { rs, _ ->
                        rs.getLong("field_id") to
                            Triple(
                                rs.getLong("value_count") to rs.getLong("null_count"),
                                rs.getBytes("lower_bound"),
                                rs.getBytes("upper_bound"),
                            )
                    }
                    .list()
                    .toMap()
            }
        assertThat(stats).containsOnlyKeys(1L, 2L, 3L)
        assertThat(stats[1L]!!.first).isEqualTo(15L to 0L)
        assertThat(stats[1L]!!.second).isEqualTo(IcebergSingleValue.encodeLong(-10))
        assertThat(stats[1L]!!.third).isEqualTo(IcebergSingleValue.encodeLong(5))
        assertThat(stats[2L]!!.first).isEqualTo(15L to 2L)
        assertThat(stats[2L]!!.second).isEqualTo(IcebergSingleValue.encodeString("a"))
        assertThat(stats[2L]!!.third).isEqualTo(IcebergSingleValue.encodeString("mid-4"))
        assertThat(stats[3L]!!.first).isEqualTo(15L to 2L)
        assertThat(stats[3L]!!.second).isEqualTo(IcebergSingleValue.encodeDouble(0.0))
        assertThat(stats[3L]!!.third).isEqualTo(IcebergSingleValue.encodeDouble(9.0))
        assertVerifyPasses(fx.cat)
    }

    @Test
    fun `a wrong-width stats bound never wedges the sweep - the group compacts with that bound null`() {
        // Pinned regression (bug hunt #5, belt-and-braces half): a bound
        // that does not decode under the live type (a 4-byte residue from
        // a pre-fix promote, or a racing hydrator's stale-typed upsert)
        // used to throw out of mergeBound EVERY sweep — a poison group
        // wedged until its inputs expired. It must instead be treated as
        // absent: the group compacts, that column's merged bound is null.
        val fx = fixture(dvOnMiddle = false)
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                """
                UPDATE hog_file_column_stats SET lower_bound = ?
                WHERE catalog_id = (SELECT catalog_id FROM hog_catalog WHERE name = ?)
                  AND data_file_id = ? AND field_id = 1
                """,
                IcebergSingleValue.encodeInt(-10),
                fx.cat,
                fx.fileIds[0],
            )
        }

        val result = svc.runOnce(fx.cat, cfg)
        assertThat(result.groupsCompacted).isEqualTo(1) // not wedged

        val output = catalogs.listFiles(fx.cat, "ns", "t").single { it.explicitRowIds }
        val stats =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT field_id, lower_bound, upper_bound FROM hog_file_column_stats s
                    JOIN hog_catalog c ON c.catalog_id = s.catalog_id
                    WHERE c.name = :cat AND s.data_file_id = :fileId
                    """,
                )
                    .bind("cat", fx.cat)
                    .bind("fileId", output.dataFileId)
                    .map { rs, _ ->
                        rs.getLong("field_id") to Pair(rs.getBytes("lower_bound"), rs.getBytes("upper_bound"))
                    }
                    .list()
                    .toMap()
            }
        // The poisoned side is null (honest absence), the clean side merged.
        assertThat(stats[1L]!!.first).isNull()
        assertThat(stats[1L]!!.second).isEqualTo(IcebergSingleValue.encodeLong(5))
        // Untouched columns merged normally.
        assertThat(stats[2L]!!.first).isEqualTo(IcebergSingleValue.encodeString("a"))
        assertThat(stats[3L]!!.second).isEqualTo(IcebergSingleValue.encodeDouble(9.0))
    }

    @Test
    fun `a mixed-width bound pair from a promotion race merges to null, not to garbage`() {
        // The race AlterService documents: the promote-time re-encode
        // rewrites existing 4-byte bounds to 8, but a hydrator already
        // in flight under the OLD type can land another 4-byte row
        // AFTER it. The merge then sees one bound of each width for the
        // same column, and decoding a 4-byte payload as the live 8-byte
        // type is not a near miss — it is an exception, which used to
        // wedge the group on every sweep until its inputs expired.
        //
        // Exercised on a NEW type so the backstop is known to cover the
        // parity set too: uint8 -> uint32 is int -> long in Iceberg
        // terms, so it is a width-changing promotion exactly like
        // int -> long, just with neither name saying "long".
        val fx = fixture(dvOnMiddle = false)
        val added =
            alter.alterTable(
                fx.cat,
                "ns",
                "t",
                listOf(AlterOp.AddColumn(ColumnDef("small", ColType.UINT8))),
            )
        val field = added.columns.single { it.def.name == "small" }.fieldId

        // Pre-promotion bounds on every input, in the 4-byte int encoding.
        db.jdbi.useHandleUnchecked { h ->
            for (fileId in fx.fileIds) {
                h.execute(
                    """
                    INSERT INTO hog_file_column_stats
                        (catalog_id, data_file_id, field_id, value_count, null_count,
                         lower_bound, upper_bound)
                    VALUES ((SELECT catalog_id FROM hog_catalog WHERE name = ?), ?, ?, 5, 0, ?, ?)
                    """,
                    fx.cat,
                    fileId,
                    field,
                    IcebergSingleValue.encodeInt(1),
                    IcebergSingleValue.encodeInt(200),
                )
            }
        }

        alter.alterTable(
            fx.cat,
            "ns",
            "t",
            listOf(AlterOp.PromoteColumn("small", ColType.UINT32)),
        )

        // The promote widened all of them; now simulate the racing
        // hydrator by putting ONE back to the pre-promotion width.
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                """
                UPDATE hog_file_column_stats SET lower_bound = ?, upper_bound = ?
                WHERE catalog_id = (SELECT catalog_id FROM hog_catalog WHERE name = ?)
                  AND data_file_id = ? AND field_id = ?
                """,
                IcebergSingleValue.encodeInt(1),
                IcebergSingleValue.encodeInt(200),
                fx.cat,
                fx.fileIds[0],
                field,
            )
        }

        val result = svc.runOnce(fx.cat, cfg)
        assertThat(result.groupsCompacted).describedAs("the sweep is not wedged").isEqualTo(1)

        val output = catalogs.listFiles(fx.cat, "ns", "t").single { it.explicitRowIds }
        val merged =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT lower_bound, upper_bound FROM hog_file_column_stats s
                    JOIN hog_catalog c ON c.catalog_id = s.catalog_id
                    WHERE c.name = :cat AND s.data_file_id = :fileId AND s.field_id = :field
                    """,
                )
                    .bind("cat", fx.cat)
                    .bind("fileId", output.dataFileId)
                    .bind("field", field)
                    .map { rs, _ -> rs.getBytes("lower_bound") to rs.getBytes("upper_bound") }
                    .one()
            }
        // Honest absence, not a bound merged from a payload that was
        // reinterpreted at the wrong width.
        assertThat(merged.first).describedAs("mixed-width merge yields no lower bound").isNull()
        assertThat(merged.second).describedAs("mixed-width merge yields no upper bound").isNull()

        // The columns that were consistent still merged normally — one
        // poisoned field must not null the whole file's stats.
        val others =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT count(*) FROM hog_file_column_stats s
                    JOIN hog_catalog c ON c.catalog_id = s.catalog_id
                    WHERE c.name = :cat AND s.data_file_id = :fileId
                      AND s.field_id <> :field AND s.lower_bound IS NOT NULL
                    """,
                )
                    .bind("cat", fx.cat)
                    .bind("fileId", output.dataFileId)
                    .bind("field", field)
                    .mapTo(Long::class.java)
                    .one()
            }
        assertThat(others).describedAs("unaffected columns keep their merged bounds").isGreaterThan(0)
    }

    @Test
    fun `bound-merge is unsigned for uint64 and byte-ordered for json`() {
        // The two new types whose merge is NOT its natural JVM ordering.
        // uint64 decodes to a BigInteger, so the winner must be picked by
        // magnitude across the 2^63 boundary where a signed long would
        // flip; json decodes to a String, which must compare as UTF-8
        // BYTES (String.compareTo is UTF-16 unit order, and the two
        // disagree above the BMP).
        val fx = fixture(dvOnMiddle = false)
        val added =
            alter.alterTable(
                fx.cat,
                "ns",
                "t",
                listOf(
                    AlterOp.AddColumn(ColumnDef("big", ColType.UINT64)),
                    AlterOp.AddColumn(ColumnDef("doc", ColType.JSON)),
                ),
            )
        val bigField = added.columns.single { it.def.name == "big" }.fieldId
        val docField = added.columns.single { it.def.name == "doc" }.fieldId

        // Per input file: a uint64 bound pair straddling 2^63, and a json
        // pair whose UTF-8 order differs from UTF-16 order. The first
        // file's uint64 lower is written NON-MINIMALLY (a redundant
        // leading sign byte), which is what a sloppy writer produces and
        // which the merge must re-minimalise on re-encode.
        val twoPow63 = java.math.BigInteger.ONE.shiftLeft(63)
        val uintBounds =
            listOf(
                byteArrayOf(0, 0, 0, 0, 0, 0, 0, 0, 7) to twoPow63.toByteArray(),
                twoPow63.toByteArray() to twoPow63.add(java.math.BigInteger.TEN).toByteArray(),
                java.math.BigInteger.valueOf(9).toByteArray() to
                    java.math.BigInteger.ONE.shiftLeft(64).subtract(java.math.BigInteger.ONE).toByteArray(),
            )
        // The orders only diverge ABOVE the BMP, so the discriminating
        // pair needs an astral codepoint: U+1F600 is F0 9F 98 80 in
        // UTF-8, above U+FFFD's EF BF BD, but its UTF-16 lead surrogate
        // D83D is BELOW FFFD. A String.compareTo merge would pick the
        // replacement character as the upper bound; the byte order picks
        // the emoji.
        val jsonBounds =
            listOf(
                """{"a":1}""" to """{"z":1}""",
                """{"b":1}""" to """{"😀":1}""",
                """{"c":1}""" to """{"�":1}""",
            )
        db.jdbi.useHandleUnchecked { h ->
            for ((i, fileId) in fx.fileIds.withIndex()) {
                val rows =
                    listOf(
                        bigField to uintBounds[i],
                        docField to
                            (
                                IcebergSingleValue.encodeString(jsonBounds[i].first) to
                                    IcebergSingleValue.encodeString(jsonBounds[i].second)
                            ),
                    )
                for ((field, pair) in rows) {
                    h.execute(
                        """
                        INSERT INTO hog_file_column_stats
                            (catalog_id, data_file_id, field_id, value_count, null_count,
                             lower_bound, upper_bound)
                        VALUES ((SELECT catalog_id FROM hog_catalog WHERE name = ?), ?, ?, 5, 0, ?, ?)
                        """,
                        fx.cat,
                        fileId,
                        field,
                        pair.first,
                        pair.second,
                    )
                }
            }
        }

        assertThat(svc.runOnce(fx.cat, cfg).groupsCompacted).isEqualTo(1)

        val output = catalogs.listFiles(fx.cat, "ns", "t").single { it.explicitRowIds }
        val merged =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT field_id, lower_bound, upper_bound FROM hog_file_column_stats s
                    JOIN hog_catalog c ON c.catalog_id = s.catalog_id
                    WHERE c.name = :cat AND s.data_file_id = :fileId
                    """,
                )
                    .bind("cat", fx.cat)
                    .bind("fileId", output.dataFileId)
                    .map { rs, _ ->
                        rs.getLong("field_id") to Pair(rs.getBytes("lower_bound"), rs.getBytes("upper_bound"))
                    }
                    .list()
                    .toMap()
            }

        // uint64: smallest is 7, largest 2^64-1 — a signed merge would
        // have called 2^63 and above negative and picked 7 as the max.
        assertThat(merged[bigField]!!.first)
            .describedAs("uint64 lower re-minimalises the 9-byte input")
            .isEqualTo(java.math.BigInteger.valueOf(7).toByteArray())
        assertThat(merged[bigField]!!.second)
            .isEqualTo(java.math.BigInteger.ONE.shiftLeft(64).subtract(java.math.BigInteger.ONE).toByteArray())

        // json: byte order puts the two-byte UTF-8 sequence on top.
        assertThat(merged[docField]!!.first).isEqualTo(IcebergSingleValue.encodeString("""{"a":1}"""))
        assertThat(merged[docField]!!.second)
            .describedAs("json upper is the UTF-8 byte winner, not the UTF-16 one")
            .isEqualTo(IcebergSingleValue.encodeString("""{"😀":1}"""))
    }

    @Test
    fun `changefeed replays original files and never the compacted output`() {
        val fx = fixture()
        svc.runOnce(fx.cat, cfg)
        val head = catalogs.getCatalog(fx.cat).headSnapshotId

        // Full replay across the compaction: the ORIGINAL three files in
        // their original row-id ranges; the compacted file never appears.
        val plan = catalogs.changes(fx.cat, "ns", "t", fromSnapshot = 0)
        assertThat(plan.toSnapshot).isEqualTo(head)
        assertThat(plan.files.map { it.path }).containsExactlyElementsOf(fx.paths)
        assertThat(plan.files.map { it.rowIdStart }).containsExactly(0L, 5L, 10L)
        assertThat(plan.files.none { it.explicitRowIds }).isTrue()
        assertThat(plan.deleteFiles).hasSize(1) // the DV feed is unaffected

        // A consumer strictly spanning only the compaction snapshot sees
        // NO new files at all.
        val tail = catalogs.changes(fx.cat, "ns", "t", fromSnapshot = fx.dvSnapshot)
        assertThat(tail.files).isEmpty()
        assertThat(tail.deleteFiles).isEmpty()
    }

    // ---- plan-to-commit races ----------------------------------------------

    @Test
    fun `a DV appearing after planning skips the group and cleanup reclaims the staged output`() {
        val fx = fixture()
        val plan = svc.planTable(fx.cat, "ns", "t", cfg)
        val group = plan.groups.single()
        assertThat(group.files.map { it.dataFileId }).containsExactlyElementsOf(fx.fileIds)

        // The race: after planning, a client registers a DV against f3
        // (planned DV-free). The rewrite applied the PLANNED vectors, so
        // committing would resurrect this delete.
        val f3DvPath = "s3://$BUCKET/${fx.cat}/dv/f3.puffin"
        registerDv(fx.cat, fx.fileIds[2], f3DvPath, listOf(0L))
        val headBefore = catalogs.getCatalog(fx.cat).headSnapshotId

        assertThat(svc.compactPlannedGroup(fx.cat, "ns", "t", group))
            .isEqualTo(CompactionService.GroupOutcome.SkippedDvSuperseded)

        // No snapshot, no metadata change, inputs untouched, the new DV live.
        assertThat(catalogs.getCatalog(fx.cat).headSnapshotId).isEqualTo(headBefore)
        assertThat(catalogs.listFiles(fx.cat, "ns", "t").map { it.path })
            .containsExactlyElementsOf(fx.paths)
        val f3Scan = scans.planScan(fx.cat, "ns", "t").single { it.dataFile.dataFileId == fx.fileIds[2] }
        assertThat(f3Scan.deleteFile!!.path).isEqualTo(f3DvPath)

        // The staged output is an undrained claim ticket whose object
        // exists — the NORMAL cleanup drain reclaims it (liveness check
        // passes: the path was never registered).
        val staged = removalRows(fx.cat).single { it.reason == "compaction_staging" }
        assertThat(staged.drainedOutcome).isNull()
        assertThat(removalStore.exists(staged.path)).isTrue()
        val drained = cleanup.runOnce(fx.cat, batchSize = 100)
        assertThat(drained.removed).isEqualTo(1)
        assertThat(drained.stillReferenced).isZero()
        assertThat(removalStore.exists(staged.path)).isFalse()
        assertThat(removalRows(fx.cat).single { it.reason == "compaction_staging" }.drainedOutcome)
            .isEqualTo("deleted")
        assertVerifyPasses(fx.cat)
    }

    @Test
    fun `a superseded DV after planning skips the group and the next run applies the grown vector`() {
        val fx = fixture()
        val plan = svc.planTable(fx.cat, "ns", "t", cfg)
        val group = plan.groups.single()

        // The race: f2's DV grows (supersession) after planning — the
        // grown vector also kills position 4 (row id 9).
        val grownPath = "s3://$BUCKET/${fx.cat}/dv/f2-grown.puffin"
        registerDv(fx.cat, fx.fileIds[1], grownPath, listOf(1L, 3L, 4L))
        val headBefore = catalogs.getCatalog(fx.cat).headSnapshotId

        assertThat(svc.compactPlannedGroup(fx.cat, "ns", "t", group))
            .isEqualTo(CompactionService.GroupOutcome.SkippedDvSuperseded)
        assertThat(catalogs.getCatalog(fx.cat).headSnapshotId).isEqualTo(headBefore)

        // The newer vector is preserved and live.
        val f2Scan = scans.planScan(fx.cat, "ns", "t").single { it.dataFile.dataFileId == fx.fileIds[1] }
        assertThat(f2Scan.deleteFile!!.path).isEqualTo(grownPath)
        assertThat(f2Scan.deleteFile!!.deleteCount).isEqualTo(3)

        // A fresh sweep re-plans against the grown vector and compacts:
        // row ids 6, 8 AND 9 are gone.
        val rerun = svc.runOnce(fx.cat, cfg)
        assertThat(rerun.groupsCompacted).isEqualTo(1)
        val output = catalogs.listFiles(fx.cat, "ns", "t").single()
        assertThat(output.recordCount).isEqualTo(12)
        assertThat(readRowIds(store.get(output.path)))
            .containsExactlyInAnyOrder(0L, 1L, 2L, 3L, 4L, 5L, 7L, 10L, 11L, 12L, 13L, 14L)
        assertVerifyPasses(fx.cat)
    }

    @Test
    fun `an input end-snapshotted after planning skips the group as a conflict`() {
        val fx = fixture()
        val plan = svc.planTable(fx.cat, "ns", "t", cfg)
        val group = plan.groups.single()

        // Simulate a concurrent compactor/expiry killing f1 after planning.
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                """
                UPDATE hog_data_file SET end_snapshot = begin_snapshot + 1
                WHERE catalog_id = (SELECT catalog_id FROM hog_catalog WHERE name = ?)
                  AND data_file_id = ?
                """,
                fx.cat,
                fx.fileIds[0],
            )
        }
        val headBefore = catalogs.getCatalog(fx.cat).headSnapshotId
        assertThat(svc.compactPlannedGroup(fx.cat, "ns", "t", group))
            .isEqualTo(CompactionService.GroupOutcome.SkippedConflict)
        assertThat(catalogs.getCatalog(fx.cat).headSnapshotId).isEqualTo(headBefore)
    }

    // ---- staging-ticket lifecycle ------------------------------------------

    @Test
    fun `a committed group's output survives the cleanup drain`() {
        val fx = fixture()
        svc.runOnce(fx.cat, cfg)
        val output = catalogs.listFiles(fx.cat, "ns", "t").single()

        val drained = cleanup.runOnce(fx.cat, batchSize = 100)
        assertThat(drained.removed).isZero()
        assertThat(drained.missing).isZero()
        assertThat(drained.stillReferenced).isZero()
        assertThat(removalStore.exists(output.path)).isTrue()
        assertThat(removalRows(fx.cat).single().drainedOutcome).isEqualTo("registered")
    }

    // ---- DV pathology ------------------------------------------------------

    @Test
    fun `an all-deleted group commits an empty output and verify stays green`() {
        val cat = "compact-empty-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(
            cat,
            "ns",
            "t",
            listOf(
                ColumnDef("id", ColType.LONG, nullable = false),
                ColumnDef("name", ColType.STRING),
                ColumnDef("score", ColType.DOUBLE),
            ),
        )
        val regs =
            listOf(
                listOf(TestRow(1, "a", 1.0), TestRow(2, "b", 2.0)),
                listOf(TestRow(3, "c", 3.0)),
            ).mapIndexed { i, rows ->
                val bytes = parquetBytes(rows)
                val path = "s3://$BUCKET/$cat/data/ns/t/e$i.parquet"
                store.put(path, bytes)
                FileRegistration(path, rows.size.toLong(), bytes.size.toLong())
            }
        commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", regs))))
        val fileIds =
            catalogs.listFiles(cat, "ns", "t").sortedBy { it.rowIdStart }.map { it.dataFileId }
        // Every row of every input dies.
        registerDv(cat, fileIds[0], "s3://$BUCKET/$cat/dv/e0.puffin", listOf(0L, 1L))
        registerDv(cat, fileIds[1], "s3://$BUCKET/$cat/dv/e1.puffin", listOf(0L))

        val result = svc.runOnce(cat, cfg.copy(targetBytes = 2048, tierTarget = 2))
        assertThat(result.groupsCompacted).isEqualTo(1)
        val output = catalogs.listFiles(cat, "ns", "t").single()
        assertThat(output.explicitRowIds).isTrue()
        assertThat(output.recordCount).isZero()
        assertThat(output.rowIdStart).isZero() // min input start: diagnostics only
        assertThat(readRowIds(store.get(output.path))).isEmpty()
        assertThat(scans.planScan(cat, "ns", "t").single().deleteFile).isNull()
        assertVerifyPasses(cat)
    }

    @Test
    fun `an unreadable input fails its group in the open - failed_groups counts it, other groups compact`() {
        // The "silently chokes on S3" regression guard: a group whose
        // input object is missing (NoSuchKey, hiccup, never uploaded)
        // must be COUNTED — a sweep whose groups all fail can never
        // present as a green no-op in the run ledger.
        val cat = "compact-fail-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        val cols =
            listOf(
                ColumnDef("id", ColType.LONG, nullable = false),
                ColumnDef("name", ColType.STRING),
                ColumnDef("score", ColType.DOUBLE),
            )
        catalogs.createTable(cat, "ns", "fine", cols)
        catalogs.createTable(cat, "ns", "gone", cols)

        fun file(
            name: String,
            rows: List<TestRow>,
            put: Boolean = true,
        ): FileRegistration {
            val bytes = parquetBytes(rows)
            val path = "s3://$BUCKET/$cat/data/$name.parquet"
            if (put) store.put(path, bytes)
            return FileRegistration(path, rows.size.toLong(), bytes.size.toLong())
        }

        commits.commit(
            cat,
            CommitRequest(
                appends =
                    listOf(
                        // The healthy group: two real files.
                        TableAppend(
                            "ns",
                            "fine",
                            listOf(
                                file("g1", listOf(TestRow(1, "a", 1.0))),
                                file("g2", listOf(TestRow(2, "b", 2.0))),
                            ),
                        ),
                        // The broken group: one real file + one phantom
                        // (registered, never uploaded — NoSuchKey at read).
                        TableAppend(
                            "ns",
                            "gone",
                            listOf(
                                file("real", listOf(TestRow(3, "c", 3.0))),
                                file("phantom", listOf(TestRow(4, "d", 4.0)), put = false),
                            ),
                        ),
                    ),
            ),
        )

        val result = svc.runOnce(cat, cfg.copy(targetBytes = 2048, tierTarget = 2))
        assertThat(result.groupsCompacted).isEqualTo(1)
        assertThat(result.failedGroups).isEqualTo(1)
        // The phantom stays live (nothing was end-snapshotted): the
        // group retries next run.
        assertThat(catalogs.listFiles(cat, "ns", "gone")).hasSize(2)
        assertThat(catalogs.listFiles(cat, "ns", "fine")).hasSize(1)

        // And the failure is visible in the ledger row's result payload.
        val row =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT status, CAST(result AS text) AS result
                      FROM hog_maintenance_run
                     WHERE catalog_id = (SELECT catalog_id FROM hog_catalog WHERE name = :cat)
                       AND task = 'compaction'
                     ORDER BY run_id DESC
                     LIMIT 1
                    """,
                )
                    .bind("cat", cat)
                    .map { rs, _ -> rs.getString("status") to rs.getString("result") }
                    .one()
            }
        assertThat(row.first).isEqualTo("ok")
        val resultJson = com.fasterxml.jackson.databind.ObjectMapper().readTree(row.second)
        assertThat(resultJson["failed_groups"].asLong()).isEqualTo(1)
        assertThat(resultJson["groups_compacted"].asLong()).isEqualTo(1)
    }

    @Test
    fun `a no-sort-order group compacts by streaming - row-id order, ids preserved, verify green`() {
        // The streaming rewrite path (no sort spec -> never materialize
        // the group on the heap; the 400MB-seed OOM lesson). Content
        // contract identical to the sorted path.
        val cat = "compact-stream-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(
            cat,
            "ns",
            "t",
            listOf(
                ColumnDef("id", ColType.LONG, nullable = false),
                ColumnDef("name", ColType.STRING),
                ColumnDef("score", ColType.DOUBLE),
            ),
        )
        // Deliberately NO SetSortOrder.
        val regs =
            listOf(
                listOf(TestRow(1, "a", 1.0), TestRow(2, null, null)),
                listOf(TestRow(3, "c", 3.0), TestRow(4, "d", 4.0)),
            ).mapIndexed { i, rows ->
                val bytes = parquetBytes(rows)
                val path = "s3://$BUCKET/$cat/data/ns/t/s$i.parquet"
                store.put(path, bytes)
                FileRegistration(path, rows.size.toLong(), bytes.size.toLong())
            }
        commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", regs))))

        val result = svc.runOnce(cat, cfg.copy(targetBytes = 2048, tierTarget = 2))
        assertThat(result.groupsCompacted).isEqualTo(1)
        assertThat(result.failedGroups).isEqualTo(0)

        val output = catalogs.listFiles(cat, "ns", "t").single()
        assertThat(output.explicitRowIds).isTrue()
        assertThat(output.recordCount).isEqualTo(4)
        // Row-id order, ids 0..3 positional by append order.
        assertThat(readRowIds(store.get(output.path))).containsExactly(0L, 1L, 2L, 3L)
        assertVerifyPasses(cat)
    }

    @Test
    fun `recompacting an explicit-row-id output with a DV preserves surviving ids`() {
        val cat = "compact-recompact-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(
            cat,
            "ns",
            "t",
            listOf(
                ColumnDef("id", ColType.LONG, nullable = false),
                ColumnDef("name", ColType.STRING),
                ColumnDef("score", ColType.DOUBLE),
            ),
        )

        fun appendRows(
            name: String,
            rows: List<TestRow>,
        ) {
            val bytes = parquetBytes(rows)
            val path = "s3://$BUCKET/$cat/data/ns/t/$name.parquet"
            store.put(path, bytes)
            commits.commit(
                cat,
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "t",
                                listOf(FileRegistration(path, rows.size.toLong(), bytes.size.toLong())),
                            ),
                        ),
                ),
            )
        }

        // Row sizes are DELIBERATE (incompressible names -> file bytes track
        // row content): a+b clear the tier-0 floor (16384) of the
        // target=65536 ladder, their uncompressed output lands in tier 1
        // (< 32768), and output+c clears the tier-1 floor (32768).
        fun paddedRows(
            start: Long,
            n: Int,
            pad: Int,
        ): List<TestRow> =
            (0 until n).map { i ->
                val id = start + i
                // Pseudo-random, deterministic, incompressible.
                val chars = (0 until pad).map { 'a' + ((id * 31 + it * 17) % 26).toInt() }.joinToString("")
                TestRow(id, chars, id.toDouble())
            }
        appendRows("a", paddedRows(100, 3, 5000)) // row ids 0..2
        appendRows("b", paddedRows(200, 2, 5000)) // row ids 3..4
        val ladder = cfg.copy(targetBytes = 65536, tierTarget = 2)

        // First compaction: unsorted table -> physical order = row-id order.
        assertThat(svc.runOnce(cat, ladder).groupsCompacted).isEqualTo(1)
        val first = catalogs.listFiles(cat, "ns", "t").single()
        assertThat(first.explicitRowIds).isTrue()
        assertThat(readRowIds(store.get(first.path))).containsExactly(0L, 1L, 2L, 3L, 4L)

        // A DV lands on the compacted output: physical positions 0 and 4,
        // i.e. row ids 0 and 4 die. Then more data arrives.
        registerDv(cat, first.dataFileId, "s3://$BUCKET/$cat/dv/first.puffin", listOf(0L, 4L))
        // c is a peer of the first output, not a fresh lower-tier file.
        appendRows("c", paddedRows(300, 2, 10000)) // row ids 5..6
        val peers = catalogs.listFiles(cat, "ns", "t")
        assertThat(peers.map { CompactionTiers.of(ladder.targetBytes, ladder.tierTarget).tierOf(it.fileSizeBytes) })
            .containsOnly(CompactionTiers.of(ladder.targetBytes, ladder.tierTarget).tierOf(first.fileSizeBytes))

        // Second compaction: the explicit-id input's DV drops by POSITION,
        // survivors keep the ids their _hog_row_id column carries.
        val second = svc.runOnce(cat, ladder)
        assertThat(second.groupsCompacted).isEqualTo(1)
        val output = catalogs.listFiles(cat, "ns", "t").single()
        assertThat(output.recordCount).isEqualTo(5)
        assertThat(output.rowIdStart).isEqualTo(1) // 0 died; min survivor is 1
        assertThat(readRowIds(store.get(output.path))).containsExactly(1L, 2L, 3L, 5L, 6L)
        assertThat(scans.planScan(cat, "ns", "t").single().deleteFile).isNull()
        assertVerifyPasses(cat)
    }

    // ---- heterogeneous schemas ---------------------------------------------

    @Test
    fun `default T eight rewrites repeated eight-file groups and leaves the seventeenth input`() {
        val cat = "compact-eight-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val bytes = parquetBytes(listOf(TestRow(1, null, null), TestRow(2, null, null), TestRow(3, null, null)))
        val regs =
            (0..16).map { i ->
                val path = "s3://$BUCKET/$cat/f$i.parquet"
                store.put(path, bytes)
                FileRegistration(path, 3, bytes.size.toLong())
            }
        commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", regs))))
        // Exact lower-bound inputs need all eight; bytes come from real
        // parquet, not synthetic size metadata. Both groups must execute.
        val policy = cfg.copy(targetBytes = bytes.size.toLong() * 8, tierTarget = 8, maxGroupsPerRun = 20)
        val result = svc.runOnce(cat, policy)
        assertThat(result.groupsCompacted).isEqualTo(2)
        assertThat(result.filesIn).isEqualTo(16)
        assertThat(result.failedGroups).isZero()
        val live = catalogs.listFiles(cat, "ns", "t")
        assertThat(live).hasSize(3)
        assertThat(live.single { !it.explicitRowIds }.path).isEqualTo(regs[16].path)
        assertThat(live.filter { it.explicitRowIds }.map { it.recordCount }).containsExactly(24L, 24L)
        val ids =
            live.flatMap { f ->
                if (f.explicitRowIds) {
                    readRowIds(
                        store.get(f.path),
                    )
                } else {
                    (f.rowIdStart until f.rowIdStart + f.recordCount).toList()
                }
            }
        assertThat(ids.sorted()).containsExactlyElementsOf((0L until 51).toList())
        assertVerifyPasses(cat)
    }

    @Test
    fun `newly promoted files wait for the next run even when they could fill the next tier`() {
        val cat = "compact-no-cascade-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(
            cat,
            "ns",
            "t",
            listOf(
                ColumnDef("id", ColType.LONG, nullable = false),
                ColumnDef("name", ColType.STRING),
                ColumnDef("score", ColType.DOUBLE),
            ),
        )
        val random = kotlin.random.Random(84311)
        var nextId = 0L

        fun input(
            name: String,
            count: Int,
        ): FileRegistration {
            val rows =
                List(count) {
                    val id = nextId++
                    TestRow(id, String(CharArray(64) { ('a'.code + random.nextInt(26)).toChar() }), random.nextDouble())
                }
            val bytes = parquetBytes(rows)
            val path = "s3://$BUCKET/$cat/$name.parquet"
            store.put(path, bytes)
            return FileRegistration(path, count.toLong(), bytes.size.toLong())
        }
        val inputs = listOf(input("a", 1000), input("b", 1000), input("peer", 2200))
        val policy = cfg.copy(targetBytes = 1024 * 1024, tierTarget = 2, maxGroupsPerRun = 20)
        val tiers = CompactionTiers.of(policy.targetBytes, policy.tierTarget)
        val low = tiers.tierOf(inputs[0].fileSizeBytes)!!
        // Pin actual parquet sizes to the intended tiers; no fabricated metadata.
        assertThat(tiers.tierOf(inputs[1].fileSizeBytes)).isEqualTo(low)
        assertThat(tiers.tierOf(inputs[2].fileSizeBytes)).isEqualTo(low + 1)
        commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", inputs))))
        val before = catalogs.getCatalog(cat).headSnapshotId
        val first = svc.runOnce(cat, policy)
        assertThat(first.groupsCompacted).isEqualTo(1)
        assertThat(first.filesIn).isEqualTo(2)
        assertThat(first.failedGroups).isZero()
        val live = catalogs.listFiles(cat, "ns", "t")
        assertThat(live).hasSize(2)
        val promoted = live.single { it.explicitRowIds }
        assertThat(tiers.tierOf(promoted.fileSizeBytes)).isEqualTo(low + 1)
        // There IS enough to promote again, proving this wasn't just a
        // sub-quota no-op. It must nevertheless wait for run number two.
        val next = svc.planTable(cat, "ns", "t", policy)
        assertThat(next.groups).hasSize(1)
        assertThat(next.groups.single().files.map { it.dataFileId }).contains(promoted.dataFileId)
        val second = svc.runOnce(cat, policy)
        assertThat(second.groupsCompacted).isEqualTo(1)
        assertThat(second.filesIn).isEqualTo(2)
        val output = catalogs.listFiles(cat, "ns", "t").single()
        assertThat(output.recordCount).isEqualTo(4200)
        assertThat(readRowIds(store.get(output.path))).containsExactlyElementsOf((0L until 4200).toList())
        assertThat(catalogs.listFiles(cat, "ns", "t", before).map { it.path })
            .containsExactlyInAnyOrderElementsOf(inputs.map { it.path })
        assertVerifyPasses(cat)
    }

    @Test
    fun `multiple groups in one tier execute up to budget including failed attempts`() {
        val cat = "compact-budget-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val bytes = parquetBytes(listOf(TestRow(1, "x", 1.0)))
        val regs =
            (0..5).map { i ->
                val path = "s3://$BUCKET/$cat/f$i.parquet"
                if (i != 0) store.put(path, bytes) // first group fails on missing input
                FileRegistration(path, 1, bytes.size.toLong())
            }
        commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", regs))))
        // Choose a quota between one and two actual file sizes, T=2.
        val policy = cfg.copy(targetBytes = bytes.size.toLong() * 2, tierTarget = 2, maxGroupsPerRun = 2)
        assertThat(svc.planTable(cat, "ns", "t", policy).groups).hasSize(3)
        val result = svc.runOnce(cat, policy)
        assertThat(result.failedGroups).isEqualTo(1)
        assertThat(result.groupsCompacted).isEqualTo(1)
        assertThat(result.filesIn).isEqualTo(2)
        // Last pair untouched: the failed first group consumed budget.
        assertThat(catalogs.listFiles(cat, "ns", "t").map { it.path })
            .contains(regs[0].path, regs[1].path, regs[4].path, regs[5].path)
        assertVerifyPasses(cat)
    }

    @Test
    fun `a heterogeneous group compacts under the live schema - add null-fills, promote up-casts, drop drops`() {
        val cat = "compact-hetero-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(
            cat,
            "ns",
            "t",
            listOf(ColumnDef("id", ColType.INT, nullable = false), ColumnDef("old", ColType.STRING)),
        )

        // Vintage 1: written under (id int [1], old string [2]).
        val v1Schema =
            Types.buildMessage()
                .addField(Types.required(PrimitiveTypeName.INT32).id(1).named("id"))
                .addField(
                    Types.optional(PrimitiveTypeName.BINARY)
                        .`as`(LogicalTypeAnnotation.stringType()).id(2).named("old"),
                )
                .named("t")
        val v1Bytes =
            customParquetBytes(
                v1Schema,
                listOf(
                    { g ->
                        g.add("id", 10)
                        g.add("old", "dead-data")
                    },
                    { g -> g.add("id", 20) },
                ),
            )
        val v1Path = "s3://$BUCKET/$cat/data/ns/t/v1.parquet"
        store.put(v1Path, v1Bytes)
        commits.commit(
            cat,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend("ns", "t", listOf(FileRegistration(v1Path, 2, v1Bytes.size.toLong()))),
                    ),
            ),
        )

        // The ALTER: id promotes to long, old drops, score arrives.
        alter.alterTable(
            cat,
            "ns",
            "t",
            listOf(
                AlterOp.PromoteColumn("id", ColType.LONG),
                AlterOp.DropColumn("old"),
                AlterOp.AddColumn(ColumnDef("score", ColType.DOUBLE)),
            ),
        )
        val scoreFieldId =
            catalogs.getTable(cat, "ns", "t").columns.single { it.def.name == "score" }.fieldId

        // Vintage 2: written under the live schema.
        val v2Schema =
            Types.buildMessage()
                .addField(Types.required(PrimitiveTypeName.INT64).id(1).named("id"))
                .addField(
                    Types.optional(PrimitiveTypeName.DOUBLE).id(Math.toIntExact(scoreFieldId)).named("score"),
                )
                .named("t")
        val v2Bytes =
            customParquetBytes(
                v2Schema,
                listOf({ g ->
                    g.add("id", 30L)
                    g.add("score", 1.5)
                }),
            )
        val v2Path = "s3://$BUCKET/$cat/data/ns/t/v2.parquet"
        store.put(v2Path, v2Bytes)
        commits.commit(
            cat,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend("ns", "t", listOf(FileRegistration(v2Path, 1, v2Bytes.size.toLong()))),
                    ),
            ),
        )

        val result = svc.runOnce(cat, cfg.copy(targetBytes = 2048, tierTarget = 2))
        assertThat(result.groupsCompacted).isEqualTo(1)
        assertThat(result.unconvertibleSchema).isZero()

        val output = catalogs.listFiles(cat, "ns", "t").single()
        assertThat(output.recordCount).isEqualTo(3)
        assertThat(output.explicitRowIds).isTrue()

        // Read back: the LIVE schema (dropped 'old' is gone; its data
        // dropped with it), int32 values up-cast to int64, the added
        // column null-filled for vintage-1 rows, field ids intact.
        data class HetRow(val id: Long, val score: Double?, val rowId: Long)

        var outSchema: MessageType? = null
        val rows = mutableListOf<HetRow>()
        readGroups(store.get(output.path)) { s, g ->
            outSchema = s
            val scoreIdx = s.getFieldIndex("score")
            rows +=
                HetRow(
                    id = g.getLong(s.getFieldIndex("id"), 0),
                    score = if (g.getFieldRepetitionCount(scoreIdx) > 0) g.getDouble(scoreIdx, 0) else null,
                    rowId = g.getLong(s.getFieldIndex(ParquetRewriter.ROW_ID_COLUMN), 0),
                )
        }
        assertThat(outSchema!!.fields.map { it.name })
            .containsExactly("id", "score", ParquetRewriter.ROW_ID_COLUMN)
        assertThat(outSchema!!.getType("id").asPrimitiveType().primitiveTypeName)
            .isEqualTo(PrimitiveTypeName.INT64)
        assertThat(outSchema!!.getType("id").id.intValue()).isEqualTo(1)
        assertThat(outSchema!!.getType("score").id.intValue().toLong()).isEqualTo(scoreFieldId)
        assertThat(rows).containsExactly(
            HetRow(10, null, 0),
            HetRow(20, null, 1),
            HetRow(30, 1.5, 2),
        )
        assertVerifyPasses(cat)
    }

    @Test
    fun `an unproducible live column skips the group as unconvertible_schema and touches nothing`() {
        val cat = "compact-unconv-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        // Live 'id' is INT; the files (a hostile/buggy writer) store
        // field 1 as INT64 — narrowing, never converted.
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.INT, nullable = false)))
        val badSchema =
            Types.buildMessage()
                .addField(Types.required(PrimitiveTypeName.INT64).id(1).named("id"))
                .named("t")
        val regs =
            (0..1).map { i ->
                val bytes = customParquetBytes(badSchema, listOf({ g -> g.add("id", i.toLong()) }))
                val path = "s3://$BUCKET/$cat/data/ns/t/bad$i.parquet"
                store.put(path, bytes)
                FileRegistration(path, 1, bytes.size.toLong())
            }
        commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", regs))))
        val headBefore = catalogs.getCatalog(cat).headSnapshotId

        val result = svc.runOnce(cat, cfg.copy(targetBytes = 2048, tierTarget = 2))
        assertThat(result.groupsCompacted).isZero()
        assertThat(result.unconvertibleSchema).isEqualTo(1)
        assertThat(result.skippedConflicts).isZero()
        assertThat(result.dvSuperseded).isZero()

        // Nothing moved: no snapshot, both files live, no staging ticket
        // (unconvertibility is detected before anything is staged or
        // uploaded).
        assertThat(catalogs.getCatalog(cat).headSnapshotId).isEqualTo(headBefore)
        assertThat(catalogs.listFiles(cat, "ns", "t")).hasSize(2)
        assertThat(removalRows(cat)).isEmpty()
    }

    // ---- lifecycle ---------------------------------------------------------

    @Test
    fun `compacted-away inputs and their dead DV become expiry-queueable once unreachable`() {
        val fx = fixture()
        svc.runOnce(fx.cat, cfg)

        // Age every snapshot far past a 60s retention and sweep: the floor
        // advances to head, the inputs' (and the applied DV's) end_snapshot
        // sinks below it, and their paths enter the removal queue. Only the
        // live output stays.
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                UPDATE hog_snapshot SET snapshot_time = now() - interval '1 hour'
                WHERE catalog_id = (SELECT catalog_id FROM hog_catalog WHERE name = :cat)
                """,
            ).bind("cat", fx.cat).execute()
            h.createUpdate(
                "UPDATE hog_catalog SET snapshot_retention_seconds = 60 WHERE name = :cat",
            ).bind("cat", fx.cat).execute()
        }
        val expiry = ExpiryService(db.jdbi).runOnce(fx.cat, batchSize = 1000)
        assertThat(expiry.dataFilesQueued).isEqualTo(3)
        assertThat(expiry.deleteFilesQueued).isEqualTo(1)

        val queued =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT r.path, r.file_kind FROM hog_file_removal r
                    JOIN hog_catalog c ON c.catalog_id = r.catalog_id
                    WHERE c.name = :cat AND r.reason = 'snapshot_expiry'
                    """,
                ).bind("cat", fx.cat)
                    .map { rs, _ -> rs.getString("path") to rs.getString("file_kind") }
                    .list()
            }
        assertThat(queued.filter { it.second == "data" }.map { it.first })
            .containsExactlyInAnyOrderElementsOf(fx.paths)
        assertThat(queued.filter { it.second == "delete" }.map { it.first })
            .containsExactly(fx.dvPath)
        // Only the compacted output survives.
        assertThat(catalogs.listFiles(fx.cat, "ns", "t").map { it.path })
            .singleElement()
            .matches { it.contains("compacted-") }
    }

    @Test
    fun `inputs with pending stats produce a pending output for the hydrator`() {
        val cat = "compact-pending-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG, nullable = false)))
        val rows1 = listOf(TestRow(1, null, null), TestRow(2, null, null))
        val rows2 = listOf(TestRow(3, null, null))
        // The parquet shape carries name/score under field ids the live
        // schema does not know — the rewrite DROPS those columns (dropped
        // -column semantics), keeping only the live `id`.
        val regs =
            listOf(rows1, rows2).mapIndexed { i, rows ->
                val bytes = parquetBytes(rows)
                val path = "s3://$BUCKET/$cat/data/ns/t/p$i.parquet"
                store.put(path, bytes)
                // No columnStats: registers as stats_state='pending'.
                FileRegistration(path, rows.size.toLong(), bytes.size.toLong())
            }
        commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", regs))))
        val result = svc.runOnce(cat, cfg.copy(targetBytes = 2048, tierTarget = 2))
        assertThat(result.groupsCompacted).isEqualTo(1)
        val output = catalogs.listFiles(cat, "ns", "t").single()
        assertThat(output.explicitRowIds).isTrue()
        assertThat(output.statsState.wire).isEqualTo("pending")
        assertThat(output.recordCount).isEqualTo(3)
        assertThat(readSchemaOnly(store.get(output.path)).fields.map { it.name })
            .containsExactly("id", ParquetRewriter.ROW_ID_COLUMN)
    }

    private fun readSchemaOnly(bytes: ByteArray): MessageType {
        val tmp = Files.createTempFile("compact-schema", ".parquet")
        try {
            Files.write(tmp, bytes)
            return ParquetFileReader.open(LocalInputFile(tmp)).use { it.footer.fileMetaData.schema }
        } finally {
            Files.deleteIfExists(tmp)
        }
    }
}
