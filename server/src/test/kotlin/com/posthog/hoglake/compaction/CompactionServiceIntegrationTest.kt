package com.posthog.hoglake.compaction

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.hydrator.CatalogColumn
import com.posthog.hoglake.hydrator.FooterStats
import com.posthog.hoglake.hydrator.Hydrator
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ChangeKind
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.persistence.getBigintListOrNull
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.CleanupService
import com.posthog.hoglake.service.ExpiryService
import com.posthog.hoglake.service.RemovalStore
import com.posthog.hoglake.service.ReplacementTarget
import com.posthog.hoglake.service.ScanService
import com.posthog.hoglake.service.TableCreationDefinition
import com.posthog.hoglake.service.TableCreationService
import com.posthog.hoglake.stats.IcebergSingleValue
import com.posthog.hoglake.testing.CatalogInvariants
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.testing.TestImages
import com.posthog.hoglake.testing.ThriftRowGroupStarts
import com.posthog.hoglake.testing.tableWithExactTotals
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.MinIOContainer
import java.nio.file.Files
import java.util.UUID
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
 * reclamation via the cleanup drain, and the
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
    private val creations = TableCreationService(db.jdbi, catalogs, commits)
    private val counter = AtomicInteger(0)

    // KB-scale target for the test fixtures' parquet files (~0.7-1 KiB
    // each): T=4 includes 512/2048/8192. The 2048 quota needs THREE files
    // (2 x ~800B < 2048 < 3 x ~800B), so groups still contain all three.
    private val cfg = CompactionConfig(targetBytes = 8192, minInputFiles = 2, maxGroupsPerRun = 10)
    private val svc by lazy { CompactionService(db.jdbi, store, cfg) }

    /**
     * No staging grace: these tests reclaim a `compaction_staging`
     * ticket seconds after the group that staged it, which is exactly
     * what HOGLAKE_CLEANUP_STAGING_GRACE_SECONDS (1 h in production)
     * makes the drain wait for. The grace protects a ticket whose group
     * may still be uploading; here the group's fate is already decided
     * by the time the drain runs, so the fixture says so rather than
     * moving clocks.
     */
    private val cleanup by lazy { CleanupService(db.jdbi, removalStore, stagingGraceSeconds = 0) }

    private companion object {
        /**
         * An object name carrying nothing but identity: the pyhoglake
         * writer's shape, and now compaction's too. A compaction output
         * that announces itself in its name tells a reader something the
         * catalog already owns (explicit_row_ids) and gives every output
         * in a table the same lead-in — see #23.
         */
        private const val BARE_UUID_PARQUET =
            "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\.parquet"

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

    /**
     * [codec] is UNCOMPRESSED for the fixtures whose sizes are arbitrary,
     * and the rewriter's own default for the two tests that compare an
     * INPUT's bytes against an OUTPUT's. Those two are the only ones for
     * which it matters, and for them it matters absolutely: group
     * packing is arithmetic on bytes, so an uncompressed input measured
     * against a compressed output of the same rows would be asserting
     * the writer's codec rather than the packing rule.
     */
    private fun parquetBytes(
        rows: List<TestRow>,
        codec: CompressionCodecName = CompressionCodecName.UNCOMPRESSED,
    ): ByteArray =
        customParquetBytes(
            schema,
            rows.map { r ->
                { g: Group ->
                    g.add("id", r.id)
                    r.name?.let { g.add("name", it) }
                    r.score?.let { g.add("score", it) }
                }
            },
            codec,
        )

    private fun customParquetBytes(
        fileSchema: MessageType,
        rows: List<(Group) -> Unit>,
        codec: CompressionCodecName = CompressionCodecName.UNCOMPRESSED,
    ): ByteArray {
        val tmp = Files.createTempFile("compact-e2e", ".parquet")
        try {
            Files.delete(tmp)
            val factory = SimpleGroupFactory(fileSchema)
            ExampleParquetWriter.builder(LocalOutputFile(tmp))
                .withType(fileSchema)
                .withCompressionCodec(codec)
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

    /**
     * #83's fuzz-found puffin DV: every field this reader owns is sound,
     * and the roaring payload's container count is not. Read from the
     * corpus entry so the bytes here and the ones the fuzz target
     * replays cannot drift apart.
     */
    private fun corruptDvBytes(): ByteArray =
        checkNotNull(
            javaClass.classLoader.getResourceAsStream(
                "com/posthog/hoglake/fuzz/PuffinDeletionVectorFuzzTestInputs/" +
                    "readRefusesLoudlyOrDecodesDeterministically/crash-1c1d87ae",
            ),
        ) { "the #83 fuzz corpus entry is missing" }.use { it.readBytes() }

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
    fun `the inputs' stats rows are not read at all - deleting every one changes nothing`() {
        // THE ABSENCE OF THE READ, asserted the only way that cannot
        // pass for the wrong reason. This path used to fetch the inputs'
        // hog_file_column_stats and re-merge them, which made every
        // property of those rows a property of the output: a row stored
        // before StatsSanity existed could be carried into a brand new
        // file's metadata (the defence was a repair-on-read), a
        // wrong-width bound from a pre-fix promote first threw out of
        // the merge every sweep and then nulled the column's bound, and
        // a group whose inputs had no rows at all could not be done.
        //
        // Corrupting a row proves none of that once the output is the
        // footer's: an exact-footer assertion passes whether the
        // corruption was ignored or merely survived a repair. DELETING
        // every input row does prove it — a path that still read them
        // would have nothing to read, so it could only produce 'pending'
        // or a short row, and this test would fail.
        val fx = fixture(dvOnMiddle = false)
        val deleted =
            db.jdbi.withHandleUnchecked { h ->
                h.createUpdate(
                    """
                    DELETE FROM hog_file_column_stats s
                     USING hog_catalog c
                     WHERE c.catalog_id = s.catalog_id AND c.name = :cat
                    """,
                ).bind("cat", fx.cat).execute()
            }
        assertThat(deleted).describedAs("3 inputs x 3 columns").isEqualTo(9)

        val result = svc.runOnce(fx.cat, cfg)
        assertThat(result.groupsCompacted).describedAs("the sweep is not wedged").isEqualTo(1)

        val output = catalogs.listFiles(fx.cat, "ns", "t").single()
        assertThat(output.statsState.wire).isEqualTo("provided")
        val stats = storedStats(fx.cat, output.dataFileId)
        assertThat(stats).containsOnlyKeys(1L, 2L, 3L)
        // EXACT, from the footer, for all fifteen rows — the bounds span
        // the negatives, which a raw binary min/max of the little-endian
        // encodings would get wrong.
        assertThat(stats.getValue(1L).valueCount).isEqualTo(15)
        assertThat(stats.getValue(1L).nullCount)
            .describedAs("id is required; no input row said so and the footer does")
            .isZero()
        assertThat(stats.getValue(1L).lower).isEqualTo(IcebergSingleValue.encodeLong(-10))
        assertThat(stats.getValue(1L).upper).isEqualTo(IcebergSingleValue.encodeLong(5))
        assertThat(stats.getValue(2L).lower).isEqualTo(IcebergSingleValue.encodeString("a"))
        assertThat(stats.getValue(2L).upper).isEqualTo(IcebergSingleValue.encodeString("mid-4"))
        assertThat(stats.getValue(3L).upper).isEqualTo(IcebergSingleValue.encodeDouble(9.0))
    }

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
        // A FRESH object in the table's data directory, named exactly as
        // an ingested file is: a bare UUID (#23). Pinned positively
        // rather than as "does not start with compacted-", so the
        // convention itself is enforced and not just one former
        // violation of it. explicit_row_ids above is what marks a
        // compaction output; nothing may infer that from the path.
        assertThat(output.path).contains("/data/ns/t/")
        assertThat(output.path).isNotIn(fx.paths)
        assertThat(output.path.substringAfterLast('/')).matches(BARE_UUID_PARQUET)
        assertThat(output.recordCount).isEqualTo(13) // 15 gross - 2 deleted
        assertThat(output.rowIdStart).isEqualTo(0) // min surviving id; positional meaning void
        assertThat(output.beginSnapshot).isEqualTo(compactionSnap)
        // 'provided' even though an input carried a DV: the stats come
        // off the footer the rewrite wrote, which describes the
        // SURVIVORS. (The dedicated assertions are in `a group with a
        // DV'd input registers provided stats over the survivors only`.)
        assertThat(output.statsState.wire).isEqualTo("provided")
        val before = catalogs.listFiles(fx.cat, "ns", "t", snapshot = compactionSnap - 1)
        assertThat(before.map { it.path }).containsExactlyElementsOf(fx.paths)
        assertThat(before.none { it.explicitRowIds }).isTrue()

        // -- aggregates: gross 15 before, 13 after (the deleted rows are
        //    physically gone from the visible file set).
        assertThat(catalogs.getTable(fx.cat, "ns", "t", snapshot = compactionSnap - 1).recordCount).isEqualTo(15)
        val aggAfter = catalogs.tableWithExactTotals(fx.cat, "ns", "t")
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

        // -- the at-rest invariants a rewrite can break, over a table
        //    whose row ids now have HOLES in them (the DV'd rows are
        //    gone forever) and whose output is explicit-row-id: the one
        //    shape most likely to put a bound out of range. The verify
        //    oracle used to stand here; CatalogInvariants says which of
        //    its arms were real and which were unfalsifiable.
        CatalogInvariants.assertVisibilityBounds(db.jdbi, fx.cat)
        CatalogInvariants.assertNoAbsentTicketOverLivePath(db.jdbi, fx.cat)
        CatalogInvariants.assertSnapshotsDense(db.jdbi, fx.cat)
        // NOT assertRemovalQueueUnreferenced here: the group commit sets
        // `drained_at` and `drained_outcome = 'registered'` in the SAME
        // statement (CompactionService:3777), so this catalog has no
        // undrained row left and the assertion would be vacuous. Its
        // call site is the plan-to-commit race below, where the staged
        // ticket is still undrained — mutation-tested there.

        // -- a second run finds nothing left to do.
        val again = svc.runOnce(fx.cat, cfg)
        assertThat(again.groupsCompacted).isZero()
        assertThat(again.skippedConflicts).isZero()
    }

    @Test
    fun `a real staging ticket settled 'absent' over a live path is caught, and 'registered' is clean`() {
        // The ticket here is the one CompactionService minted and settled
        // itself, over an object it really uploaded and registered: the
        // #174 lifecycle end to end, not a row this test invented.
        val fx = fixture(dvOnMiddle = false)
        assertThat(svc.runOnce(fx.cat, cfg).groupsCompacted).isEqualTo(1)
        val ticket = removalRows(fx.cat).single()
        assertThat(ticket.reason).isEqualTo("compaction_staging")
        assertThat(ticket.drainedOutcome).isEqualTo("registered")
        CatalogInvariants.assertNoAbsentTicketOverLivePath(db.jdbi, fx.cat)

        // Now the race resolved the wrong way: cleanup settled the ticket
        // 'absent' — "this object never existed" — while the catalog is
        // serving reads from the very path it names. The removal ledger is
        // the only thing that knows that path, so nothing else can see it,
        // and this assertion is the whole reason the 'absent' outcome and
        // the HEAD-before-DELETE carve-out that produces it still have a
        // reader after #261 (see CleanupService.STAGING_REASON).
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                UPDATE hog_file_removal SET drained_outcome = 'absent'
                WHERE catalog_id = (SELECT catalog_id FROM hog_catalog WHERE name = :cat)
                """,
            ).bind("cat", fx.cat).execute()
        }
        assertThatThrownBy { CatalogInvariants.assertNoAbsentTicketOverLivePath(db.jdbi, fx.cat) }
            .isInstanceOf(AssertionError::class.java)
            .hasMessageContaining(ticket.path)
            .hasMessageContaining("was drained 'absent' but the catalog holds a file row")
    }

    @Test
    fun `the committed object carries the configured codec, in every column chunk`() {
        // CompactionCodecTest pins the rewriter; CompactionConfigTest pins
        // the env wiring into CompactionConfig. Between them sits the
        // sweep, which has to hand its config's codec to the rewriter and
        // upload what came back — and nothing else in this suite would
        // notice if it passed the default instead, because every other
        // assertion here is about values, ids and metadata, all identical
        // under any codec. So: read the codec out of the COMMITTED
        // object's footer, per column chunk, at the default and under an
        // override that cannot be confused with it.
        val fx = fixture(dvOnMiddle = false)
        assertThat(svc.runOnce(fx.cat, cfg).groupsCompacted).isEqualTo(1)
        val defaulted = catalogs.listFiles(fx.cat, "ns", "t").single { it.explicitRowIds }
        assertThat(codecsOf(store.get(defaulted.path)))
            .describedAs("default sweep: every column chunk of %s", defaulted.path)
            .containsOnly(ParquetRewriter.DEFAULT_CODEC)

        val other = fixture(dvOnMiddle = false)
        val overridden = cfg.copy(codec = ParquetRewriter.OutputCodec(CompressionCodecName.GZIP))
        assertThat(svc.runOnce(other.cat, overridden).groupsCompacted).isEqualTo(1)
        val out = catalogs.listFiles(other.cat, "ns", "t").single { it.explicitRowIds }
        assertThat(codecsOf(store.get(out.path)))
            .describedAs("overridden sweep: every column chunk of %s", out.path)
            .containsOnly(CompressionCodecName.GZIP)
    }

    @Test
    fun `a compacted output registers its row-group offsets from the footer it wrote`() {
        val fx = fixture(dvOnMiddle = false)
        // The inputs registered without offsets, so any list the output
        // carries was produced by the compaction commit itself.
        assertThat(fx.fileIds.map { storedSplitOffsets(fx.cat, it) }).containsOnlyNulls()
        assertThat(svc.runOnce(fx.cat, cfg).groupsCompacted).isEqualTo(1)
        val output = catalogs.listFiles(fx.cat, "ns", "t").single { it.explicitRowIds }
        // Independent reading of the committed OBJECT's thrift footer,
        // not of the writer's in-memory metadata the commit used.
        val expected = ThriftRowGroupStarts.of(store.get(output.path)).offsets
        assertThat(expected).isNotEmpty()
        assertThat(storedSplitOffsets(fx.cat, output.dataFileId)).containsExactlyElementsOf(expected)
        // And served: the scan plan hands the same list to an engine.
        val scanned =
            scans.planScan(fx.cat, "ns", "t", splitOffsets = true).single { it.dataFile.explicitRowIds }
        assertThat(scanned.dataFile.splitOffsets).containsExactlyElementsOf(expected)
    }

    private fun storedSplitOffsets(
        cat: String,
        dataFileId: Long,
    ): List<Long>? =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT f.split_offsets FROM hog_data_file f
                JOIN hog_catalog c ON c.catalog_id = f.catalog_id
                WHERE c.name = :cat AND f.data_file_id = :fileId
                """,
            )
                .bind("cat", cat)
                .bind("fileId", dataFileId)
                .map { rs, _ -> rs.getBigintListOrNull("split_offsets") }
                .one()
        }

    /** Every column chunk's recorded codec, across every row group. */
    private fun codecsOf(bytes: ByteArray): List<CompressionCodecName> {
        val tmp = Files.createTempFile("codec-read", ".parquet")
        return try {
            Files.write(tmp, bytes)
            ParquetFileReader.open(LocalInputFile(tmp)).use { reader ->
                reader.footer.blocks.flatMap { block -> block.columns.map { it.codec } }
            }
        } finally {
            Files.deleteIfExists(tmp)
        }
    }

    @Test
    fun `a DV-free group's output stats come from the footer the rewrite just wrote`() {
        // NOT from the inputs' hog_file_column_stats, which this path
        // used to read back and re-merge. The expected numbers below are
        // what the fixture's fifteen rows actually contain, computed
        // from the rows rather than from any stored row.
        val fx = fixture(dvOnMiddle = false)
        val result = svc.runOnce(fx.cat, cfg)
        assertThat(result.groupsCompacted).isEqualTo(1)
        assertThat(result.filesIn).isEqualTo(3)

        val output = catalogs.listFiles(fx.cat, "ns", "t").single()
        assertThat(output.explicitRowIds).isTrue()
        assertThat(output.recordCount).isEqualTo(15)
        assertThat(output.statsState.wire).isEqualTo("provided")

        val stats = storedStats(fx.cat, output.dataFileId)
        // The reserved _hog_row_id leaf is in the file but not in the
        // catalog's column list, so it gets no row.
        assertThat(stats).containsOnlyKeys(1L, 2L, 3L)

        // id (field 1, required): every row, no nulls, and the bounds
        // span the negatives — a raw binary min/max of the little-endian
        // encodings would get these wrong.
        assertThat(stats[1L]!!.valueCount to stats[1L]!!.nullCount).isEqualTo(15L to 0L)
        assertThat(stats[1L]!!.lower).isEqualTo(IcebergSingleValue.encodeLong(-10))
        assertThat(stats[1L]!!.upper).isEqualTo(IcebergSingleValue.encodeLong(5))

        // name (field 2): two of the fifteen rows have none; the widest
        // strings present are "a" and "mid-4".
        assertThat(stats[2L]!!.valueCount to stats[2L]!!.nullCount).isEqualTo(15L to 2L)
        assertThat(stats[2L]!!.lower).isEqualTo(IcebergSingleValue.encodeString("a"))
        assertThat(stats[2L]!!.upper).isEqualTo(IcebergSingleValue.encodeString("mid-4"))

        // score (field 3): two nulls; the smallest value written is 0.0
        // and the largest 9.0.
        assertThat(stats[3L]!!.valueCount to stats[3L]!!.nullCount).isEqualTo(15L to 2L)
        // -0.0, not +0.0: a stored lower bound takes the sign Iceberg
        // fixes for the role, so a total-order evaluator cannot read the
        // pair as an empty range. Same number, canonical bytes — and it
        // proves StatsSanity still runs on this door, since the footer's
        // own minimum for that column is +0.0.
        assertThat(stats[3L]!!.lower).isEqualTo(IcebergSingleValue.encodeDouble(-0.0))
        assertThat(stats[3L]!!.upper).isEqualTo(IcebergSingleValue.encodeDouble(9.0))

        // nan_count comes from the footer too, and only float/double
        // statistics carry one: the double column answers 0 because
        // these fifteen rows hold no NaN, and the long and the string
        // answer null because parquet never records a NaN count for a
        // type that has no NaN.
        assertThat(stats[3L]!!.nanCount).isZero()
        assertThat(listOf(stats[1L]!!.nanCount, stats[2L]!!.nanCount)).containsOnly(null)
        // size_bytes is the footer's own per-column chunk total.
        assertThat(stats.values.map { it.sizeBytes }).doesNotContainNull()
    }

    private data class StoredStats(
        val valueCount: Long,
        val nullCount: Long,
        val nanCount: Long?,
        val sizeBytes: Long?,
        val lower: ByteArray?,
        val upper: ByteArray?,
    )

    /** Every stored hog_file_column_stats row for one file, by field id. */
    private fun storedStats(
        cat: String,
        dataFileId: Long,
    ): Map<Long, StoredStats> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT field_id, value_count, null_count, nan_count, size_bytes,
                       lower_bound, upper_bound
                  FROM hog_file_column_stats s
                  JOIN hog_catalog c ON c.catalog_id = s.catalog_id
                 WHERE c.name = :cat AND s.data_file_id = :fileId
                """,
            )
                .bind("cat", cat)
                .bind("fileId", dataFileId)
                .map { rs, _ ->
                    rs.getLong("field_id") to
                        StoredStats(
                            valueCount = rs.getLong("value_count"),
                            nullCount = rs.getLong("null_count"),
                            nanCount = rs.getObject("nan_count")?.let { (it as Number).toLong() },
                            sizeBytes = rs.getObject("size_bytes")?.let { (it as Number).toLong() },
                            lower = rs.getBytes("lower_bound"),
                            upper = rs.getBytes("upper_bound"),
                        )
                }
                .list()
                .toMap()
        }

    @Test
    fun `a column added after the inputs is null-filled in the output - counted, with no bounds`() {
        // This was `a mixed-width bound pair from a promotion race merges
        // to null, not to garbage`: a promote rewrites existing 4-byte
        // bounds to 8, a hydrator already in flight under the OLD type
        // can land another 4-byte row after it, and the merge then saw
        // one bound of each width for one column. No merge, no race — the
        // output's stats never touch those rows.
        //
        // What the output's own footer says about such a column is the
        // thing worth pinning, and it is not "nothing": the rewriter
        // writes the LIVE schema, so a column added after every input was
        // written exists in the output as an all-null chunk. The footer
        // therefore reports a full value_count, a full null_count, and NO
        // bounds — an all-null chunk has no non-null minimum to bound —
        // which is the honest row for a column that holds no values.
        val fx = fixture(dvOnMiddle = false)
        val added =
            alter.alterTable(
                fx.cat,
                "ns",
                "t",
                listOf(AlterOp.AddColumn(ColumnDef("small", ColType.UINT8))),
            )
        val field = added.columns.single { it.def.name == "small" }.fieldId

        // The stale-typed input rows the old merge choked on, still
        // written, still promoted underneath, and now read by nothing.
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
        // uint8 -> uint32 is int -> long in Iceberg terms: a
        // width-changing promotion exactly like int -> long, with
        // neither name saying "long".
        alter.alterTable(
            fx.cat,
            "ns",
            "t",
            listOf(AlterOp.PromoteColumn("small", ColType.UINT32)),
        )
        // The promote widened all three; put ONE back to the
        // pre-promotion width, which is the racing hydrator's landing.
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
        val stats = storedStats(fx.cat, output.dataFileId)
        val small = stats.getValue(field)
        assertThat(small.valueCount).describedAs("the column is in the output, null-filled").isEqualTo(15)
        assertThat(small.nullCount).isEqualTo(15)
        assertThat(small.lower).describedAs("an all-null chunk bounds nothing").isNull()
        assertThat(small.upper).describedAs("an all-null chunk bounds nothing").isNull()
        // One value-less column must not cost the file's other bounds.
        assertThat(stats[1L]!!.lower).isEqualTo(IcebergSingleValue.encodeLong(-10))
        assertThat(stats[2L]!!.upper).isEqualTo(IcebergSingleValue.encodeString("mid-4"))
        assertThat(stats[3L]!!.upper).isEqualTo(IcebergSingleValue.encodeDouble(9.0))
    }

    /**
     * A catalog + `ns.t` with [cols], two single-row-group input files
     * written from [fileSchema], and nothing else. For the stats shapes
     * the generic fixture's id/name/score cannot express.
     */
    private fun twoFileTable(
        name: String,
        cols: List<ColumnDef>,
        fileSchema: MessageType,
        file1: List<(Group) -> Unit>,
        file2: List<(Group) -> Unit>,
    ): String {
        val cat = "compact-$name-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", cols)
        val regs =
            listOf(file1, file2).mapIndexed { i, rows ->
                val bytes = customParquetBytes(fileSchema, rows)
                val path = "s3://$BUCKET/$cat/data/ns/t/$name$i.parquet"
                store.put(path, bytes)
                // No columnStats: nothing downstream reads them, and
                // registering none proves it again for free.
                FileRegistration(path, rows.size.toLong(), bytes.size.toLong())
            }
        commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", regs))))
        return cat
    }

    @Test
    fun `a decimal column with no declared scale gets bounds, and a scaled one keeps its scale`() {
        // `type_params` WITHOUT a `scale` is ordinary — pyhoglake's
        // convention, BoundWire.scaleOf's, and ParquetRewriter's, all of
        // which read an absent scale as 0; the rewriter even STAMPS
        // `decimalType(0, precision)` on the output leaf. The footer
        // decode used to be the one reader that did not, treating a
        // null catalog scale as "unknown" and dropping the bounds of
        // every such column on both this path and the hydrator's.
        val bare =
            Types.optional(PrimitiveTypeName.BINARY)
                .`as`(LogicalTypeAnnotation.decimalType(0, 10)).id(1).named("bare")
        val cents =
            Types.optional(PrimitiveTypeName.BINARY)
                .`as`(LogicalTypeAnnotation.decimalType(2, 10)).id(2).named("cents")

        fun row(
            b: Long,
            c: Long,
        ): (Group) -> Unit =
            { g ->
                g.add("bare", Binary.fromConstantByteArray(java.math.BigInteger.valueOf(b).toByteArray()))
                g.add("cents", Binary.fromConstantByteArray(java.math.BigInteger.valueOf(c).toByteArray()))
            }

        val cat =
            twoFileTable(
                "decimal",
                listOf(
                    ColumnDef("bare", ColType.DECIMAL, typeParams = mapOf("precision" to 10)),
                    ColumnDef("cents", ColType.DECIMAL, typeParams = mapOf("precision" to 10, "scale" to 2)),
                ),
                Types.buildMessage().addField(bare).addField(cents).named("t"),
                // bare: -3 .. 7 here, cents: -0.01 .. 14.20
                file1 = listOf(row(-3, -1), row(7, 1420)),
                // bare: 11 (the file's and the group's maximum)
                file2 = listOf(row(11, 500)),
            )

        assertThat(svc.runOnce(cat, cfg).groupsCompacted).isEqualTo(1)
        val output = catalogs.listFiles(cat, "ns", "t").single()
        assertThat(output.statsState.wire).isEqualTo("provided")
        val stats = storedStats(cat, output.dataFileId)
        // The scale-less column, which used to get NULL bounds here.
        assertThat(stats.getValue(1L).lower)
            .describedAs("absent type_params.scale means 0, the same as the leaf the rewriter wrote")
            .isEqualTo(IcebergSingleValue.encodeDecimalUnscaled(java.math.BigInteger.valueOf(-3)))
        assertThat(stats.getValue(1L).upper)
            .isEqualTo(IcebergSingleValue.encodeDecimalUnscaled(java.math.BigInteger.valueOf(11)))
        // And the declared-scale column bounds the UNSCALED integer, so
        // -0.01 and 14.20 are -1 and 1420 — the Iceberg encoding, which
        // carries the scale in the catalog and never in the bytes.
        assertThat(stats.getValue(2L).lower)
            .isEqualTo(IcebergSingleValue.encodeDecimalUnscaled(java.math.BigInteger.valueOf(-1)))
        assertThat(stats.getValue(2L).upper)
            .isEqualTo(IcebergSingleValue.encodeDecimalUnscaled(java.math.BigInteger.valueOf(1420)))
    }

    @Test
    fun `nan_count is the footer's own count, not a null placeholder`() {
        // Parquet DOES carry a NaN count — `Statistics.nan_count`,
        // reachable through isNanCountSet/getNanCount — and the writer
        // accumulated this output's over exactly the rows it wrote. It
        // used to be hard-coded null here with a comment saying footers
        // carry no such thing, which cost the one count Iceberg defines
        // for floating-point columns (nan_value_counts) on every file
        // either door produced.
        val v = Types.optional(PrimitiveTypeName.DOUBLE).id(1).named("v")
        val cat =
            twoFileTable(
                "nan",
                listOf(ColumnDef("v", ColType.DOUBLE)),
                Types.buildMessage().addField(v).named("t"),
                file1 = listOf({ g: Group -> g.add("v", 1.0) }, { g: Group -> g.add("v", Double.NaN) }),
                // A NULL as well as a NaN: the two are counted
                // separately and neither may be mistaken for the other.
                file2 = listOf({ g: Group -> g.add("v", Double.NaN) }, { _: Group -> }),
            )

        assertThat(svc.runOnce(cat, cfg).groupsCompacted).isEqualTo(1)
        val output = catalogs.listFiles(cat, "ns", "t").single()
        val row = storedStats(cat, output.dataFileId).getValue(1L)
        assertThat(row.valueCount).isEqualTo(4)
        assertThat(row.nullCount).describedAs("one row has no value at all").isEqualTo(1)
        assertThat(row.nanCount).describedAs("two of the three present values are NaN").isEqualTo(2)
        // A NaN is never a bound (StatsSanity refuses one outright, and
        // parquet's own statistics leave it out of min/max), so the only
        // non-NaN value is both bounds.
        assertThat(row.lower).isEqualTo(IcebergSingleValue.encodeDouble(1.0))
        assertThat(row.upper).isEqualTo(IcebergSingleValue.encodeDouble(1.0))
    }

    @Test
    fun `bounds parquet refuses to write are not stored either - both doors agree`() {
        // parquet-java keeps a chunk's full min/max in the in-memory
        // ParquetMetadata but will not SERIALIZE its statistics once
        // minBytes.length + maxBytes.length reaches
        // ParquetMetadataConverter.MAX_STATS_SIZE (4096). Compaction
        // reads the writer's in-memory footer, so without the size gate
        // it would store multi-KB bounds that re-reading the file can
        // never reproduce — and a later rehydrate would replace them
        // with NULL, silently changing a live file's pruning metadata.
        val big =
            Types.optional(PrimitiveTypeName.BINARY)
                .`as`(LogicalTypeAnnotation.stringType()).id(1).named("big")

        fun row(c: Char): (Group) -> Unit = { g -> g.add("big", c.toString().repeat(2_200)) }
        val cat =
            twoFileTable(
                "bigstring",
                listOf(ColumnDef("big", ColType.STRING)),
                Types.buildMessage().addField(big).named("t"),
                file1 = listOf(row('a'), row('b')),
                file2 = listOf(row('c')),
            )

        assertThat(svc.runOnce(cat, cfg).groupsCompacted).isEqualTo(1)
        val output = catalogs.listFiles(cat, "ns", "t").single()
        assertThat(output.statsState.wire).isEqualTo("provided")
        val row = storedStats(cat, output.dataFileId).getValue(1L)
        assertThat(row.valueCount).describedAs("counts are unaffected").isEqualTo(3)
        assertThat(row.nullCount).isZero()
        assertThat(row.lower).describedAs("4,400 bytes of bounds the file does not carry").isNull()
        assertThat(row.upper).isNull()

        // And the file itself proves it: re-reading the output's footer
        // and running the SAME aggregation finds no statistics at all
        // for that chunk — parquet dropped the whole object, null_count
        // included — so the hydrator's answer is "no row", never a
        // bound. Neither door can produce one, which is the agreement
        // that matters.
        val reread =
            FooterStats.aggregate(
                footerOf(store.get(output.path)),
                listOf(CatalogColumn(1, "big", ColType.STRING, null)),
                output.path,
            )
        assertThat(reread)
            .describedAs("the written footer carries no statistics for an oversized chunk")
            .isEmpty()
    }

    private fun footerOf(bytes: ByteArray): ParquetMetadata {
        val tmp = Files.createTempFile("compact-footer", ".parquet")
        try {
            Files.write(tmp, bytes)
            return ParquetFileReader.open(LocalInputFile(tmp)).use { it.footer }
        } finally {
            Files.deleteIfExists(tmp)
        }
    }

    @Test
    fun `a group with a DV'd input registers provided stats over the survivors only`() {
        // THE CASE THE OLD CODE COULD NOT DO AT ALL. A DV'd input's
        // registered counts describe pre-delete rows, so summing them
        // would have claimed rows the output does not contain; the merge
        // refused the group's stats outright, registered 'pending', and
        // left a hydrator sweep to fetch the same footer back out of S3.
        // The footer the writer just built counts the SURVIVORS, because
        // the survivors are what it wrote.
        val fx = fixture(dvOnMiddle = true)
        // TWO MORE vectors, so that every assertion below can tell a
        // survivor aggregate from a gross one. With only f2's interior
        // rows deleted, each column's extremes and every null still
        // survived, so lower_bound, upper_bound and null_count were all
        // identical for the 15 rows and for the 11 — the counts alone
        // moved, and only on value_count.
        //
        // f3: position 1 is id 2 / score 9.0 (the group's largest score)
        // and position 4 is id 5 / score 0.5 (the group's largest id),
        // so both of those columns' UPPER bounds move.
        registerDv(fx.cat, fx.fileIds[2], "s3://$BUCKET/${fx.cat}/dv/f3.puffin", listOf(1L, 4L))
        // f1: position 0 is id -10 (the group's smallest id, so id's
        // LOWER bound moves to -8) and position 2 is the row with a NULL
        // name AND a NULL score, so both of those columns' null_counts
        // move from 2 to 1.
        registerDv(fx.cat, fx.fileIds[0], "s3://$BUCKET/${fx.cat}/dv/f1.puffin", listOf(0L, 2L))

        val result = svc.runOnce(fx.cat, cfg)
        assertThat(result.groupsCompacted).isEqualTo(1)
        assertThat(result.filesIn).isEqualTo(3)

        val output = catalogs.listFiles(fx.cat, "ns", "t").single()
        assertThat(output.recordCount).describedAs("15 gross - 6 deleted").isEqualTo(9)
        assertThat(output.statsState.wire).isEqualTo("provided")

        val stats = storedStats(fx.cat, output.dataFileId)
        assertThat(stats).containsOnlyKeys(1L, 2L, 3L)
        // Every column counts 9, not the 15 the inputs are registered
        // with — nothing here is a sum of pre-delete counts.
        assertThat(stats.values.map { it.valueCount })
            .describedAs("counts are the survivors', matching record_count")
            .containsOnly(output.recordCount)
        // id: -10 and 5 both died with their vectors, leaving -8 and 4.
        assertThat(stats[1L]!!.nullCount).isZero()
        assertThat(stats[1L]!!.lower)
            .describedAs("id -10 died with f1's vector; the bound is the surviving minimum")
            .isEqualTo(IcebergSingleValue.encodeLong(-8))
        assertThat(stats[1L]!!.upper)
            .describedAs("id 5 died with f3's vector; the bound is the surviving maximum")
            .isEqualTo(IcebergSingleValue.encodeLong(4))
        // name: f1's null died, so only f3's is left; "mid-4" and "a"
        // both survive.
        assertThat(stats[2L]!!.nullCount)
            .describedAs("one of the two nulls died with f1's vector")
            .isEqualTo(1)
        assertThat(stats[2L]!!.lower).isEqualTo(IcebergSingleValue.encodeString("a"))
        assertThat(stats[2L]!!.upper).isEqualTo(IcebergSingleValue.encodeString("mid-4"))
        // score: 9.0 died with f3's vector, so 8.0 is the surviving
        // maximum; 0.0 survives in f2 and stores as the role-signed
        // -0.0; the null that died was f1's.
        assertThat(stats[3L]!!.nullCount)
            .describedAs("one of the two nulls died with f1's vector")
            .isEqualTo(1)
        assertThat(stats[3L]!!.lower).isEqualTo(IcebergSingleValue.encodeDouble(-0.0))
        assertThat(stats[3L]!!.upper)
            .describedAs("score 9.0 died with f3's vector")
            .isEqualTo(IcebergSingleValue.encodeDouble(8.0))
    }

    /**
     * The SEQUENTIAL arm: `parallelGroups = 1`, no pool, [executeWave]
     * calling [CompactionService.executeGroup] on the sweep's own
     * thread. The rethrow escaped straight out of `runOnce` here.
     */
    @Test
    fun `a commit statement that dies for an unruled reason fails ONE group, not the sweep`() {
        assertOneGroupFails(parallelGroups = 1)
    }

    /**
     * The POOL arm, and the one production took: at
     * `parallelGroups` > 1 every group runs on a worker, so the escaping
     * exception came back out of `f.get()` in [executeWave] wrapped in
     * an `ExecutionException` — a different catch shape, outside
     * `executeGroup`'s own arms, and the one that killed the whole
     * sweep's ledger. The sequential arm alone would have left it
     * untested.
     */
    @Test
    fun `the same unruled commit failure on a POOL worker also fails ONE group`() {
        assertOneGroupFails(parallelGroups = 2)
    }

    private fun assertOneGroupFails(parallelGroups: Int) {
        // gigahog-prod-us, 2026-09-30: the commit's stats read crossed
        // the session statement_timeout, and the arm that catches
        // UnableToExecuteStatementException RETHREW it because it was not
        // a lock timeout. A `throw` inside a catch block leaves the try
        // statement, so the sibling `catch (e: Throwable)` — the
        // per-group failure isolation — never saw it; `f.get()` in
        // executeWave surfaced an ExecutionException and the whole sweep
        // died with an empty ledger result, losing every group that had
        // not started.
        //
        // The seam makes a commit statement fail FOR REAL (SQLSTATE
        // 57014 out of the driver, not a hand-thrown Kotlin exception),
        // because the arm under test is keyed on the driver's SQLSTATE.
        val cat = "compact-commit-fail-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        val cols =
            listOf(
                ColumnDef("id", ColType.LONG, nullable = false),
                ColumnDef("name", ColType.STRING),
                ColumnDef("score", ColType.DOUBLE),
            )
        catalogs.createTable(cat, "ns", "a", cols)
        catalogs.createTable(cat, "ns", "b", cols)

        fun file(
            name: String,
            rows: List<TestRow>,
        ): FileRegistration {
            val bytes = parquetBytes(rows)
            val path = "s3://$BUCKET/$cat/data/$name.parquet"
            store.put(path, bytes)
            return FileRegistration(path, rows.size.toLong(), bytes.size.toLong())
        }

        commits.commit(
            cat,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "a",
                            listOf(
                                file("a1", listOf(TestRow(1, "a", 1.0))),
                                file("a2", listOf(TestRow(2, "b", 2.0))),
                            ),
                        ),
                        TableAppend(
                            "ns",
                            "b",
                            listOf(
                                file("b1", listOf(TestRow(3, "c", 3.0))),
                                file("b2", listOf(TestRow(4, "d", 4.0))),
                            ),
                        ),
                    ),
            ),
        )

        // Its OWN service instance: the hook is per-instance state and
        // the shared `svc` is used by every other test in this class.
        // Fires exactly ONCE, on whichever group commits first — the
        // assertion below is about the counts, not about which table
        // lost, so the sweep's group order does not matter.
        val failing = CompactionService(db.jdbi, store, cfg)
        val fired = java.util.concurrent.atomic.AtomicBoolean(false)
        failing.beforeCommitTail = { h ->
            if (fired.compareAndSet(false, true)) {
                h.execute("SET LOCAL statement_timeout = '1ms'")
                h.createQuery("SELECT 1 FROM pg_sleep(0.05)").mapTo(Int::class.javaObjectType).one()
            }
        }

        val policy = cfg.copy(targetBytes = 2048, parallelGroups = parallelGroups)
        val result = failing.runOnce(cat, policy)
        assertThat(fired.get()).describedAs("the seam ran").isTrue()
        // BOTH groups accounted for: one failed, one compacted. Before
        // the fix this call threw and there was no result at all.
        assertThat(result.failedGroups).isEqualTo(1)
        assertThat(result.groupsCompacted).isEqualTo(1)
        // One table is down to its compacted output; the other still
        // holds both inputs, so its group re-plans next sweep.
        val survivor = listOf("a", "b").single { catalogs.listFiles(cat, "ns", it).size == 2 }
        val compacted = listOf("a", "b").single { it != survivor }
        assertThat(catalogs.listFiles(cat, "ns", compacted)).hasSize(1)

        // The RUN is ok, not failed: a failed group is a counter, not a
        // broken sweep.
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

        // The failed group's staged output is left for the cleanup drain:
        // two tickets, one settled 'registered' by the group that
        // committed and one still undrained.
        val tickets = removalRows(cat).filter { it.reason == "compaction_staging" }
        assertThat(tickets).hasSize(2)
        assertThat(tickets.map { it.drainedOutcome }).containsExactlyInAnyOrder("registered", null)

        // THE CLAIM IS RELEASED, which is what makes a failed group a
        // retry rather than a 15-minute outage for that table. The
        // committed group keeps a SHORT claim on purpose (so a sibling's
        // pre-commit plan does not spend a rewrite on dead inputs); the
        // failed one must hold nothing.
        assertThat(claimedTables(cat))
            .describedAs("only the committed group's short-lease claim is left")
            .containsExactly(catalogs.getTable(cat, "ns", compacted).tableId)

        // And the retry really happens: with the seam disarmed the next
        // sweep compacts the group that failed, with nothing in the way.
        failing.beforeCommitTail = {}
        val retry = failing.runOnce(cat, policy)
        assertThat(retry.failedGroups).isZero()
        assertThat(retry.groupsCompacted).describedAs("the failed group re-planned and ran").isEqualTo(1)
        assertThat(catalogs.listFiles(cat, "ns", survivor)).hasSize(1)
    }

    /** Table ids holding a live (unexpired) compaction claim. */
    private fun claimedTables(cat: String): List<Long> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT cl.table_id FROM hog_compaction_claim cl
                  JOIN hog_catalog c ON c.catalog_id = cl.catalog_id
                 WHERE c.name = :cat AND cl.expires_at > now()
                 ORDER BY cl.table_id
                """,
            )
                .bind("cat", cat)
                .mapTo(Long::class.java)
                .list()
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

    @Test
    fun `a table holding a variant at ANY depth produces no candidates`() {
        // THE GATE, which had no test: the rewriter's refusal is only
        // the backstop, and reverting the planner to #77's top-level
        // check left the whole suite green. A nested variant would then
        // be enqueued, reach the rewriter, and come back a skip on every
        // sweep forever — work the planner is supposed to never
        // schedule.
        //
        // UNSORTED, and self-validating. Two earlier attempts passed
        // against the bug: the first used files too small to group at
        // all, the second used the sorted fixture, where ANY nested
        // column triggers the heap derate and empties the plan on its
        // own. Here the same catalog is planned before and after the
        // variant arrives, with the struct already present, so the only
        // thing that changes is the variant.
        for (nested in listOf(false, true)) {
            val label = if (nested) "nested" else "top-level"
            val cat = "compact-variant-$label-${counter.incrementAndGet()}"
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
            if (nested) {
                alter.alterTable(
                    cat,
                    "ns",
                    "t",
                    listOf(
                        AlterOp.AddColumn(
                            ColumnDef("s", ColType.STRUCT, children = listOf(ColumnDef("n", ColType.LONG))),
                        ),
                    ),
                )
            }
            repeat(3) { i ->
                val rows = (0 until 3).map { r -> TestRow((i * 3 + r).toLong(), "name-$i-$r", r.toDouble()) }
                val bytes = parquetBytes(rows)
                val path = "s3://$BUCKET/$cat/data/ns/t/f$i.parquet"
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
            Hydrator(db.jdbi, store).runOnce()
            assertThat(svc.planTable(cat, "ns", "t", cfg).groups)
                .describedAs("%s: the file set IS groupable before the variant exists", label)
                .isNotEmpty()

            alter.alterTable(
                cat,
                "ns",
                "t",
                listOf(
                    if (nested) {
                        AlterOp.AddColumn(ColumnDef("v", ColType.VARIANT), parent = "s")
                    } else {
                        AlterOp.AddColumn(ColumnDef("v", ColType.VARIANT))
                    },
                ),
            )
            assertThat(svc.planTable(cat, "ns", "t", cfg).groups)
                .describedAs("%s variant: nothing is enqueued", label)
                .isEmpty()
            assertThat(svc.runOnce(cat, cfg).groupsCompacted)
                .describedAs("%s variant: and nothing is compacted", label)
                .isZero()
        }
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
        // Invariant 4 while the ticket is UNDRAINED, which is the only
        // window in which it can be violated and the #174 bug class: the
        // group lost the race, so its staged path is queued for deletion
        // and no file row may name it. A commitGroup that registered the
        // output without settling the ticket lands exactly here.
        CatalogInvariants.assertRemovalQueueUnreferenced(db.jdbi, fx.cat)
        val drained = cleanup.runOnce(fx.cat, batchSize = 100)
        assertThat(drained.removed).isEqualTo(1)
        assertThat(drained.stillReferenced).isZero()
        assertThat(removalStore.exists(staged.path)).isFalse()
        assertThat(removalRows(fx.cat).single { it.reason == "compaction_staging" }.drainedOutcome)
            .isEqualTo("deleted")
        CatalogInvariants.assertVisibilityBounds(db.jdbi, fx.cat)
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
    }

    // ---- the cleanup claim vs. the group commit (V21) -----------------------

    /**
     * A store that marks the staging ticket the way a cleanup worker
     * would, the moment the output object becomes real —
     * `completeMultipartUpload` is the last thing that happens before
     * `commitGroup` runs, so this is exactly the window a drain occupies:
     * ticket undrained, object uploaded, commit not yet attempted.
     *
     * [claimAgeSeconds] ages the claim; [attempts] stands for a worker
     * that tried and did not settle (a lost `DeleteObjects` response,
     * which fails the chunk, bumps `attempts` and RELEASES the claim). The
     * three combinations are the three routes by which a ticket can be
     * "touched but not settled" — and a group must refuse ALL of them,
     * because in every one the object may already be gone and nothing
     * anywhere would notice a live file row pointing at it.
     *
     * The mark is written by SQL rather than by running a drain because a
     * drain would also DELETE the object and settle the row, which is the
     * already-covered case; what these cases are about is a ticket the
     * group can still see.
     */
    private fun touchingStore(
        claimAgeSeconds: Long? = 0,
        attempts: Int = 0,
        /**
         * Settle the ticket outright — the #174 case, and the only route of
         * the four that happens every day: a drain that gets its
         * `DeleteObjects` response deletes the object and settles the row.
         * It is also the only route that exercises `drained_at IS NULL`.
         */
        settledOutcome: String? = null,
    ): ObjectStore =
        object : ObjectStore(minio.s3URL, "us-east-1", minio.userName, minio.password, true) {
            override fun completeMultipartUpload(
                pathUri: String,
                uploadId: String,
                etags: List<String>,
            ) {
                super.completeMultipartUpload(pathUri, uploadId, etags)
                val touched =
                    db.jdbi.withHandleUnchecked { h ->
                        h.createUpdate(
                            """
                            UPDATE hog_file_removal
                               SET claimed_at = CASE WHEN :claimed
                                                     THEN now() - make_interval(secs => :age)
                                                     END,
                                   claimed_by = CASE WHEN :claimed THEN 'cleanup-worker' END,
                                   attempts = :attempts,
                                   drained_at = CASE WHEN :outcome IS NOT NULL THEN now() END,
                                   drained_outcome = :outcome
                             WHERE path = :path AND reason = 'compaction_staging'
                               AND drained_at IS NULL
                            """,
                        )
                            .bind("claimed", claimAgeSeconds != null)
                            .bind("age", (claimAgeSeconds ?: 0L).toDouble())
                            .bind("attempts", attempts)
                            .bind("outcome", settledOutcome)
                            .bind("path", pathUri)
                            .execute()
                    }
                check(touched == 1) {
                    "the fixture must mark exactly one undrained staging ticket for $pathUri, not $touched"
                }
            }
        }

    /** Every claim column and the outcome of this catalog's removal rows, in queue order. */
    private fun ticketState(cat: String): List<Triple<String?, String?, Int>> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT drained_outcome, claimed_by, attempts FROM hog_file_removal r " +
                    "JOIN hog_catalog c USING (catalog_id) WHERE c.name = :cat ORDER BY r.removal_id",
            )
                .bind("cat", cat)
                .map { rs, _ ->
                    Triple(rs.getString("drained_outcome"), rs.getString("claimed_by"), rs.getInt("attempts"))
                }
                .list()
        }

    /**
     * Every way a cleanup worker can have TOUCHED a staging ticket, and
     * the one property that has to hold for all of them: **no live file
     * row may ever name a path a drain may already have deleted.**
     *
     * FOUR FIXTURES OVER THREE PREDICATE TERMS, and the mapping is worth
     * stating so a reader auditing coverage counts terms rather than
     * cases:
     *
     * 1. a live claim — `claimed_at IS NULL`;
     * 2. a LAPSED claim (a worker past its lease, or one killed after its
     *    DELETE) — the SAME term, deliberately. The predicate carries no
     *    lease, so 1 and 2 reach one branch; case 2 is the regression
     *    guard against re-introducing a lease into it, which is what
     *    round 1 of this change got wrong;
     * 3. a released claim with `attempts > 0` — a LOST `DeleteObjects`
     *    response: the chunk failed, `attempts` was bumped and the claim
     *    RELEASED, so `claimed_at` is null again while the object may be
     *    gone server-side. `attempts = 0` is the term that covers it;
     * 4. a ticket the drain fully SETTLED — `drained_at IS NULL`, the
     *    #174 case, the original reason the re-read exists and the only
     *    one of the four that happens every day, since a drain that gets
     *    its response settles the row.
     *
     * An earlier version of this change treated 1 and 2 as reclaimable —
     * "the claim expired, so the worker is gone, so the ticket is mine" —
     * which is exactly wrong: an expired claim does not mean the object
     * survived, it means nobody knows. `commitGroup`'s re-read therefore
     * refuses any ticket that is not UNTOUCHED, and these cases pin that
     * in the only terms that matter: outcome `SkippedConflict`, no file
     * row for the output, and a fresh ticket so the object cannot outlive
     * a ticket naming it.
     */
    @Test
    fun `a group whose staging ticket cleanup has TOUCHED loses the group, however it was touched`() {
        data class Route(
            val name: String,
            val term: String,
            val store: ObjectStore,
            val claimant: String?,
            val attempts: Int,
            val outcome: String? = null,
        )

        val routes =
            listOf(
                Route("a live claim", "claimed_at IS NULL", touchingStore(claimAgeSeconds = 0), "cleanup-worker", 0),
                Route(
                    "a LAPSED claim (a worker past its lease, or one killed after its delete)",
                    "claimed_at IS NULL (the same term as route 1: the predicate has no lease)",
                    touchingStore(claimAgeSeconds = 10 * CleanupService.CLAIM_LEASE_SECONDS),
                    "cleanup-worker",
                    0,
                ),
                Route(
                    "a released claim with attempts > 0 (a lost DeleteObjects response)",
                    "attempts = 0",
                    touchingStore(claimAgeSeconds = null, attempts = 1),
                    null,
                    1,
                ),
                Route(
                    "a ticket the drain already SETTLED (#174, and the only route that is routine)",
                    "drained_at IS NULL",
                    touchingStore(claimAgeSeconds = null, settledOutcome = "deleted"),
                    null,
                    0,
                    outcome = "deleted",
                ),
            )
        for (route in routes) {
            val fx = fixture(dvOnMiddle = false)
            val plan = svc.planTable(fx.cat, "ns", "t", cfg)
            val group = plan.groups.single()
            val headBefore = catalogs.getCatalog(fx.cat).headSnapshotId

            val outcome =
                CompactionService(db.jdbi, route.store, cfg)
                    .compactPlannedGroup(fx.cat, "ns", "t", group)

            assertThat(outcome)
                .describedAs(
                    "%s: a touched ticket is not this group's to register (the term that refuses " +
                        "it is `%s`)",
                    route.name,
                    route.term,
                )
                .isEqualTo(CompactionService.GroupOutcome.SkippedConflict)
            assertThat(catalogs.getCatalog(fx.cat).headSnapshotId)
                .describedAs("%s: no snapshot, so nothing registered the output", route.name)
                .isEqualTo(headBefore)
            val files = catalogs.listFiles(fx.cat, "ns", "t")
            assertThat(files.map { it.path })
                .describedAs(
                    "%s: THE PROPERTY — no live file row may name a path a drain may have deleted",
                    route.name,
                )
                .containsExactlyElementsOf(fx.paths)
            assertThat(files.none { it.explicitRowIds })
                .describedAs("%s: and no compaction output was registered at all", route.name)
                .isTrue()

            // TWO tickets for the one path: the touched original, still
            // the drain's to settle, and the re-stage that guarantees the
            // object is reclaimed even if that worker never comes back.
            val tickets = removalRows(fx.cat).filter { it.reason == "compaction_staging" }
            assertThat(tickets).describedAs("%s: the path is re-staged", route.name).hasSize(2)
            assertThat(tickets.map { it.path }.distinct())
                .describedAs("%s: both tickets name the same staged object", route.name)
                .hasSize(1)
            assertThat(tickets.map { it.drainedOutcome })
                .describedAs("%s: the original keeps its outcome and the re-stage has none", route.name)
                .containsExactly(route.outcome, null)
            assertThat(ticketState(fx.cat))
                .describedAs(
                    "%s: the original keeps the mark cleanup left; the re-stage is untouched " +
                        "and drainable",
                    route.name,
                )
                .containsExactly(
                    Triple(route.outcome, route.claimant, route.attempts),
                    Triple(null, null, 0),
                )
            assertThat(removalStore.exists(tickets.first().path))
                .describedAs("%s: this fixture never deleted the object", route.name)
                .isTrue()
        }
    }

    @Test
    fun `the re-staged ticket restarts the staging grace instead of inheriting it`() {
        // The re-stage is a FRESH row, so its `scheduled_at` is now: the
        // drain leaves it alone for HOGLAKE_CLEANUP_STAGING_GRACE_SECONDS,
        // which is what stops it settling the object out from under the
        // NEXT group while that group is uploading. A re-stage that copied
        // the original's timestamp would be eligible immediately.
        val fx = fixture(dvOnMiddle = false)
        val plan = svc.planTable(fx.cat, "ns", "t", cfg)
        CompactionService(db.jdbi, touchingStore(claimAgeSeconds = 0), cfg)
            .compactPlannedGroup(fx.cat, "ns", "t", plan.groups.single())

        val scheduled =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT scheduled_at FROM hog_file_removal r JOIN hog_catalog c " +
                        "USING (catalog_id) WHERE c.name = :cat ORDER BY r.removal_id",
                ).bind("cat", fx.cat).mapTo(java.time.OffsetDateTime::class.java).list()
            }
        assertThat(scheduled).hasSize(2)
        // BOTH TIMESTAMPS COME FROM THE SAME CLOCK — Postgres's, via
        // `now()` in two different transactions — so the assertion is the
        // literal statement of "restarts instead of inheriting" and no JVM
        // clock enters it. Comparing against `Instant.now()` would be the
        // shape that flakes elsewhere in this suite, and it would also be
        // WEAKER: a re-stage that copied the original's `scheduled_at`
        // would pass it whenever the original was itself recent.
        assertThat(scheduled.last().toInstant())
            .describedAs("the re-stage starts its OWN grace rather than inheriting the original's")
            .isAfter(scheduled.first().toInstant())
    }

    @Test
    fun `the ticket re-read takes the row lock it holds until the settle`() {
        // AGENT.md names "FOR UPDATE dropped" as a mutation that must red
        // a test. The behavioural version needs a latch inside
        // `commitGroup` that has no seam today, so this is the statement
        // assertion: without the row lock the re-read and the
        // 'registered' settle are two separate snapshots of the row, and
        // the `check(settled == 1)` the KDoc calls unreachable becomes a
        // thrown IllegalStateException out of a sweep.
        val fx = fixture(dvOnMiddle = false)
        val issued = java.util.concurrent.CopyOnWriteArrayList<String>()
        val recording =
            com.posthog.hoglake.Database.jdbi(db.dataSource).also { j ->
                j.setSqlLogger(
                    object : org.jdbi.v3.core.statement.SqlLogger {
                        override fun logAfterExecution(context: org.jdbi.v3.core.statement.StatementContext) {
                            issued += context.renderedSql
                        }
                    },
                )
            }
        assertThat(CompactionService(recording, store, cfg).runOnce(fx.cat, cfg).groupsCompacted)
            .describedAs("the group must commit, or the settle never runs")
            .isEqualTo(1)

        val reread = issued.filter { it.contains("FROM hog_file_removal") && it.contains("claimed_at") }
        assertThat(reread)
            .describedAs("the ticket re-read must be issued:%n%s", issued.joinToString("\n---\n"))
            .isNotEmpty()
            .allSatisfy {
                assertThat(it)
                    .describedAs("and it must hold the row from the re-read to the settle")
                    .contains("FOR UPDATE")
            }
        // And the settle carries the SAME three terms, so it cannot
        // refuse a row the re-read just authorised.
        assertThat(issued.filter { it.contains("drained_outcome = 'registered'") })
            .isNotEmpty()
            .allSatisfy {
                assertThat(it).contains("drained_at IS NULL AND claimed_at IS NULL AND attempts = 0")
            }
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

    // ---- compaction vs. guarded DML ----------------------------------------
    //
    // Compaction publishes a 'table_compacted' change per group, and on a
    // busy production table it publishes one every couple of minutes. #161
    // counted that as a read-set conflict for guarded mutations, so any
    // UPDATE or MERGE whose read-to-publish window outlived one compaction
    // interval retried into the next compaction forever. These drive a REAL
    // compaction between a writer's read and its publish.

    /** A guarded append registering one real parquet file. */
    private fun guardedAppend(
        cat: String,
        name: String,
    ): Pair<TableAppend, Long> {
        val rows = (100L until 105L).map { TestRow(it, "late-$it", it.toDouble()) }
        val bytes = parquetBytes(rows)
        val path = "s3://$BUCKET/$cat/data/ns/t/$name.parquet"
        store.put(path, bytes)
        val append =
            TableAppend(
                "ns",
                "t",
                listOf(FileRegistration(path, rows.size.toLong(), bytes.size.toLong())),
                catalogs.getTable(cat, "ns", "t").tableUuid,
            )
        val snapshot = commits.commit(cat, CommitRequest(appends = listOf(append))).snapshotId
        val fileId =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT f.data_file_id FROM hog_data_file f
                    JOIN hog_catalog c ON c.catalog_id = f.catalog_id
                    WHERE c.name = :cat AND f.path = :path
                    """,
                ).bind("cat", cat).bind("path", path).mapTo(Long::class.java).one()
            }
        return append to fileId
    }

    /** The /commit/mutations/prepared shape: guarded, idempotent, read-set protected. */
    private fun preparedMutation(
        cat: String,
        readSnapshot: Long,
        dataFileId: Long,
        dvName: String,
        positions: List<Long>,
    ): CommitRequest {
        val dvBytes = PuffinTestFiles.deletionVector(positions)
        val dvPath = "s3://$BUCKET/$cat/dv/$dvName.puffin"
        store.put(dvPath, dvBytes)
        return CommitRequest(
            readSnapshot = readSnapshot,
            deletes =
                listOf(
                    TableDeletes(
                        "ns",
                        "t",
                        listOf(
                            DeleteFileRegistration(
                                dataFileId,
                                dvPath,
                                positions.size.toLong(),
                                dvBytes.size.toLong(),
                            ),
                        ),
                        catalogs.getTable(cat, "ns", "t").tableUuid,
                    ),
                ),
            idempotencyKey = UUID.randomUUID(),
            requireUnchangedTables = true,
        )
    }

    @Test
    fun `a prepared mutation on a file compaction did not touch survives the compaction`() {
        val fx = fixture(dvOnMiddle = false)
        val group = svc.planTable(fx.cat, "ns", "t", cfg).groups.single()
        // f4 lands after planning, so compaction rewrites f1..f3 and leaves it alone.
        val (_, lateFileId) = guardedAppend(fx.cat, "f4")
        val readSnapshot = catalogs.getCatalog(fx.cat).headSnapshotId

        assertThat(svc.compactPlannedGroup(fx.cat, "ns", "t", group))
            .isInstanceOf(CompactionService.GroupOutcome.Committed::class.java)
        val compactionSnap = catalogs.getCatalog(fx.cat).headSnapshotId
        assertThat(compactionSnap).isGreaterThan(readSnapshot)

        val request = preparedMutation(fx.cat, readSnapshot, lateFileId, "f4", listOf(0L, 2L))
        val published = commits.commit(fx.cat, request)
        assertThat(published.snapshotId).isGreaterThan(compactionSnap)
        // Replay is unaffected, and the vector is live on the untouched file.
        assertThat(commits.commit(fx.cat, request)).isEqualTo(published)
        val scan = scans.planScan(fx.cat, "ns", "t").single { it.dataFile.dataFileId == lateFileId }
        assertThat(scan.deleteFile?.deleteCount).isEqualTo(2)
    }

    @Test
    fun `a prepared mutation on a file compaction retired is a conflict not a validation failure`() {
        val fx = fixture(dvOnMiddle = false)
        val group = svc.planTable(fx.cat, "ns", "t", cfg).groups.single()
        val readSnapshot = catalogs.getCatalog(fx.cat).headSnapshotId
        assertThat(svc.compactPlannedGroup(fx.cat, "ns", "t", group))
            .isInstanceOf(CompactionService.GroupOutcome.Committed::class.java)
        val compactionSnap = catalogs.getCatalog(fx.cat).headSnapshotId

        // 409, not 422: the file WAS live at read_snapshot, so re-reading and
        // re-publishing against the compaction output is the writer's fix.
        // 422 told the connector its DELETE was malformed and to give up.
        assertThatThrownBy {
            commits.commit(fx.cat, preparedMutation(fx.cat, readSnapshot, fx.fileIds[0], "retired", listOf(0L)))
        }
            .isInstanceOf(HoglakeException.CommitConflict::class.java)
            .hasMessageContaining("retired at snapshot $compactionSnap")
        assertThat(catalogs.getCatalog(fx.cat).headSnapshotId).isEqualTo(compactionSnap)
    }

    @Test
    fun `an append-only transaction survives a concurrent insert and a compaction group`() {
        val fx = fixture(dvOnMiddle = false)
        val group = svc.planTable(fx.cat, "ns", "t", cfg).groups.single()
        val readSnapshot = catalogs.getCatalog(fx.cat).headSnapshotId

        // Both things an append-only transaction must tolerate: another
        // writer's INSERT into its own target table, and a compaction of it.
        guardedAppend(fx.cat, "concurrent")
        assertThat(svc.compactPlannedGroup(fx.cat, "ns", "t", group))
            .isInstanceOf(CompactionService.GroupOutcome.Committed::class.java)
        val head = catalogs.getCatalog(fx.cat).headSnapshotId

        val rows = (200L until 203L).map { TestRow(it, "txn-$it", it.toDouble()) }
        val bytes = parquetBytes(rows)
        val path = "s3://$BUCKET/${fx.cat}/data/ns/t/txn.parquet"
        store.put(path, bytes)
        // The /commit/transaction shape: requireUnchangedTables is set
        // unconditionally by the route, but with no deletes it binds nothing.
        val request =
            CommitRequest(
                readSnapshot = readSnapshot,
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "t",
                            listOf(FileRegistration(path, rows.size.toLong(), bytes.size.toLong())),
                            catalogs.getTable(fx.cat, "ns", "t").tableUuid,
                        ),
                    ),
                idempotencyKey = UUID.randomUUID(),
                requireUnchangedTables = true,
                allowPendingDeletes = true,
            )
        val published = commits.commit(fx.cat, request)
        assertThat(published.snapshotId).isGreaterThan(head)
        assertThat(commits.commit(fx.cat, request)).isEqualTo(published)
        // The transaction's file is LIVE and scannable beside the
        // compaction output and the concurrent insert — not merely admitted.
        val scanned = scans.planScan(fx.cat, "ns", "t")
        assertThat(scanned.map { it.dataFile.path }).contains(path)
        val txnFile = scanned.single { it.dataFile.path == path }
        assertThat(txnFile.dataFile.recordCount).isEqualTo(rows.size.toLong())
        assertThat(txnFile.deleteFile).isNull()
        assertThat(catalogs.tableWithExactTotals(fx.cat, "ns", "t").recordCount).isEqualTo(23)
    }

    @Test
    fun `a guarded table replacement survives a compaction of the incarnation it retires`() {
        val fx = fixture(dvOnMiddle = false)
        val group = svc.planTable(fx.cat, "ns", "t", cfg).groups.single()
        val target = catalogs.getTable(fx.cat, "ns", "t")
        val prepared =
            creations.prepare(
                fx.cat,
                UUID.randomUUID(),
                TableCreationDefinition(
                    "ns",
                    "t",
                    listOf(ColumnDef("id", ColType.LONG, nullable = false)),
                    ReplacementTarget(target.tableUuid, catalogs.getCatalog(fx.cat).headSnapshotId),
                ),
            )

        // The window a real upload spends writing parquet is longer than
        // one compaction interval on a busy table. The guard used to count
        // 'table_compacted' as a target change and burn the receipt
        // DURABLY — rejected/target_changed, which a retry cannot undo.
        assertThat(svc.compactPlannedGroup(fx.cat, "ns", "t", group))
            .isInstanceOf(CompactionService.GroupOutcome.Committed::class.java)

        val receipt = creations.publish(fx.cat, prepared.operationId, emptyList())
        assertThat(receipt.state).isEqualTo("committed")
        assertThat(receipt.reason).isNull()
        val replaced = catalogs.tableWithExactTotals(fx.cat, "ns", "t")
        assertThat(replaced.tableUuid).isEqualTo(prepared.tableUuid).isNotEqualTo(target.tableUuid)
        assertThat(replaced.recordCount).isZero()
        assertThat(catalogs.listFiles(fx.cat, "ns", "t")).isEmpty()
    }

    /**
     * The whole retry loop the 409 exists to make possible: a DELETE
     * targeting a file compaction retires must be re-readable against the
     * compaction OUTPUT and land there, with the rows actually gone. A 422
     * ended this story at step two.
     */
    @Test
    fun `a delete refused for a retired target re-reads against the output and still deletes the rows`() {
        val fx = fixture(dvOnMiddle = false)
        val group = svc.planTable(fx.cat, "ns", "t", cfg).groups.single()
        val readSnapshot = catalogs.getCatalog(fx.cat).headSnapshotId
        assertThat(svc.compactPlannedGroup(fx.cat, "ns", "t", group))
            .isInstanceOf(CompactionService.GroupOutcome.Committed::class.java)

        assertThatThrownBy {
            commits.commit(fx.cat, preparedMutation(fx.cat, readSnapshot, fx.fileIds[1], "stale", listOf(0L)))
        }.isInstanceOf(HoglakeException.CommitConflict::class.java)

        // Re-read at the compaction snapshot: one output file carrying the
        // same rows, with row ids preserved (invariant 2). Row id 5 — f2's
        // first row, the one the refused vector aimed at — is at whatever
        // position the sorted output put it.
        val output = scans.planScan(fx.cat, "ns", "t").single()
        val outputRowIds = readRowIds(store.get(output.dataFile.path))
        val position = outputRowIds.indexOf(5L).toLong()
        assertThat(position).isNotNegative()

        val retry =
            preparedMutation(
                fx.cat,
                catalogs.getCatalog(fx.cat).headSnapshotId,
                output.dataFile.dataFileId,
                "retry",
                listOf(position),
            )
        commits.commit(fx.cat, retry)

        val rescan = scans.planScan(fx.cat, "ns", "t").single()
        assertThat(rescan.deleteFile?.deleteCount).isEqualTo(1)
        // And a later compaction MATERIALIZES that deletion: pair the
        // output with one more file (a group needs two) and rewrite. Row
        // id 5 is gone from the bytes for good; every other id survives.
        val (_, lateFileId) = guardedAppend(fx.cat, "after-retry")
        val lateRowIds =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT f.row_id_start, f.record_count FROM hog_data_file f
                    JOIN hog_catalog c ON c.catalog_id = f.catalog_id
                    WHERE c.name = :cat AND f.data_file_id = :id
                    """,
                ).bind("cat", fx.cat).bind("id", lateFileId)
                    .map { rs, _ -> rs.getLong(1) until (rs.getLong(1) + rs.getLong(2)) }
                    .one()
            }
        assertThat(svc.runOnce(fx.cat, cfg).groupsCompacted).isEqualTo(1)
        val compacted = catalogs.listFiles(fx.cat, "ns", "t").single()
        assertThat(readRowIds(store.get(compacted.path)))
            .containsExactlyInAnyOrderElementsOf(outputRowIds.filter { it != 5L } + lateRowIds)
        assertThat(scans.planScan(fx.cat, "ns", "t").single().deleteFile).isNull()
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

    /**
     * Catalog with an unsorted table ns.t and two small files: e0 holds
     * ids 1 and 2 (row ids 0..1), e1 holds id 3 (row id 2). A real puffin
     * DV kills every row of e0; e1 is killed too unless [liveSecond]. The
     * two files pack into one group at a 2 KiB target.
     */
    private class DeadFixture(
        val cat: String,
        val fileIds: List<Long>,
        val paths: List<String>,
        val dvPaths: List<String>,
    )

    private fun deadFixture(liveSecond: Boolean = false): DeadFixture {
        val cat = "compact-dead-${counter.incrementAndGet()}"
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
        val fileIds = catalogs.listFiles(cat, "ns", "t").sortedBy { it.rowIdStart }.map { it.dataFileId }
        val dvPaths = mutableListOf("s3://$BUCKET/$cat/dv/e0.puffin")
        registerDv(cat, fileIds[0], dvPaths[0], listOf(0L, 1L))
        if (!liveSecond) {
            dvPaths += "s3://$BUCKET/$cat/dv/e1.puffin"
            registerDv(cat, fileIds[1], dvPaths[1], listOf(0L))
        }
        return DeadFixture(cat, fileIds, regs.map { it.path }, dvPaths)
    }

    private val deadCfg = cfg.copy(targetBytes = 2048)

    /** (end_snapshot) of every data file row of [cat], live or not, by data_file_id. */
    private fun fileEnds(cat: String): Map<Long, Long?> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT f.data_file_id, f.end_snapshot FROM hog_data_file f
                JOIN hog_catalog c ON c.catalog_id = f.catalog_id
                WHERE c.name = :cat
                """,
            )
                .bind("cat", cat)
                .map { rs, _ -> rs.getLong(1) to rs.getObject(2)?.let { (it as Number).toLong() } }
                .list()
                .toMap()
        }

    /** (path -> end_snapshot) of every deletion vector row of [cat]. */
    private fun dvEnds(cat: String): Map<String, Long?> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT d.path, d.end_snapshot FROM hog_delete_file d
                JOIN hog_catalog c ON c.catalog_id = d.catalog_id
                WHERE c.name = :cat
                """,
            )
                .bind("cat", cat)
                .map { rs, _ -> rs.getString(1) to rs.getObject(2)?.let { (it as Number).toLong() } }
                .list()
                .toMap()
        }

    private fun nextFileId(cat: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT next_file_id FROM hog_catalog WHERE name = :cat")
                .bind("cat", cat)
                .mapTo(Long::class.java)
                .one()
        }

    private fun lastLedgerResult(cat: String): com.fasterxml.jackson.databind.JsonNode =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT CAST(result AS text) FROM hog_maintenance_run
                 WHERE catalog_id = (SELECT catalog_id FROM hog_catalog WHERE name = :cat)
                   AND task = 'compaction'
                 ORDER BY run_id DESC LIMIT 1
                """,
            ).bind("cat", cat).mapTo(String::class.java).one()
        }.let { com.fasterxml.jackson.databind.ObjectMapper().readTree(it) }

    @Test
    fun `a group whose every input is fully deleted retires its inputs and registers no output`() {
        // This group used to COMMIT ITS EMPTY OUTPUT: a live hog_data_file
        // row with record_count 0 and a row_id_start (the inputs' minimum)
        // owned by no row in it. Now the rewrite still runs — reading every
        // input through its DV is the proof the group is dead — and the
        // commit retires the inputs with nothing in their place.
        val registry =
            io.micrometer.prometheusmetrics.PrometheusMeterRegistry(
                io.micrometer.prometheusmetrics.PrometheusConfig.DEFAULT,
            )
        com.posthog.hoglake.observability.Metrics.bind(registry)
        val fx = deadFixture()
        val headBefore = catalogs.getCatalog(fx.cat).headSnapshotId
        val fileIdBefore = nextFileId(fx.cat)

        val result = svc.runOnce(fx.cat, deadCfg)
        assertThat(result.groupsRetired).isEqualTo(1)
        assertThat(result.groupsCompacted).describedAs("a retirement is not a compaction").isZero()
        assertThat(listOf(result.filesIn, result.filesOut, result.bytesIn, result.bytesOut))
            .containsOnly(0L)
        assertThat(result.failedGroups).isZero()

        // ONE new snapshot, a table_compacted change, its own message.
        val head = catalogs.getCatalog(fx.cat).headSnapshotId
        assertThat(head).isEqualTo(headBefore + 1)
        val (kind, message) =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT ch.kind, s.commit_message FROM hog_snapshot s
                    JOIN hog_catalog c ON c.catalog_id = s.catalog_id
                    JOIN hog_snapshot_change ch
                      ON ch.catalog_id = s.catalog_id AND ch.snapshot_id = s.snapshot_id
                    WHERE c.name = :cat AND s.snapshot_id = :snap
                    """,
                )
                    .bind("cat", fx.cat)
                    .bind("snap", head)
                    .map { rs, _ -> rs.getString(1) to rs.getString(2) }
                    .one()
            }
        assertThat(kind).isEqualTo("table_compacted")
        assertThat(message).isEqualTo("retired 2 fully-deleted files of ns.t")

        // The inputs AND their DVs end at that snapshot; NO file row was
        // added and no file id was spent.
        assertThat(fileEnds(fx.cat)).isEqualTo(fx.fileIds.associateWith { head })
        assertThat(dvEnds(fx.cat)).isEqualTo(fx.dvPaths.associateWith { head })
        assertThat(catalogs.listFiles(fx.cat, "ns", "t")).isEmpty()
        assertThat(nextFileId(fx.cat)).describedAs("nothing registered, so no id allocated").isEqualTo(fileIdBefore)
        // Time travel below the retirement still sees the inputs.
        assertThat(catalogs.listFiles(fx.cat, "ns", "t", snapshot = headBefore).map { it.path })
            .containsExactlyInAnyOrderElementsOf(fx.paths)

        // The empty object the rewrite uploaded is NOT registered and NOT
        // settled: its staging ticket is undrained, exactly as a lost race
        // leaves it, so the drain is what disposes of it.
        val staged = removalRows(fx.cat).single { it.reason == "compaction_staging" }
        assertThat(staged.drainedOutcome).isNull()
        assertThat(removalStore.exists(staged.path)).isTrue()
        CatalogInvariants.assertRemovalQueueUnreferenced(db.jdbi, fx.cat)
        CatalogInvariants.assertVisibilityBounds(db.jdbi, fx.cat)

        val ledger = lastLedgerResult(fx.cat)
        assertThat(ledger["groups_retired"].asLong()).isEqualTo(1)
        assertThat(ledger["groups_compacted"].asLong()).isZero()
        assertThat(registry.scrape())
            .contains("hoglake_compaction_groups_retired_total{catalog=\"${fx.cat}\"} 1.0")
            .doesNotContain("hoglake_compaction_groups_total{catalog=\"${fx.cat}\"}")

        // Nothing left to plan: the retired files are not candidates.
        assertThat(svc.planTable(fx.cat, "ns", "t", deadCfg).groups).isEmpty()
        val again = svc.runOnce(fx.cat, deadCfg)
        assertThat(again.groupsRetired + again.groupsCompacted).isZero()
        assertThat(catalogs.getCatalog(fx.cat).headSnapshotId).isEqualTo(head)

        // The drain reclaims the empty object.
        val drained = cleanup.runOnce(fx.cat, batchSize = 100)
        assertThat(drained.removed).isEqualTo(1)
        assertThat(drained.stillReferenced).isZero()
        assertThat(removalStore.exists(staged.path)).isFalse()
        assertThat(removalRows(fx.cat).single { it.reason == "compaction_staging" }.drainedOutcome)
            .isEqualTo("deleted")
    }

    /**
     * The SORTED twin of [deadFixture], built so the dead group holds a
     * METADATA-TRUSTED run: ns.t sorted by score, two client files
     * compacted into an output O (explicit_row_ids, registered after the
     * spec began), then a client file e2 appended and every row of O and
     * e2 deleted by real DVs. [DeadFixture.fileIds] is (O, e2).
     */
    private fun sortedDeadFixture(): DeadFixture {
        val cat = "compact-dead-sorted-${counter.incrementAndGet()}"
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

        fun append(
            name: String,
            rows: List<TestRow>,
        ): String {
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
            return path
        }
        append("e0", listOf(TestRow(1, "a", 2.0), TestRow(2, "b", 1.0)))
        append("e1", listOf(TestRow(3, "c", 3.0)))
        assertThat(svc.runOnce(cat, cfg).groupsCompacted).describedAs("the prior output").isEqualTo(1)
        val output = catalogs.listFiles(cat, "ns", "t").single()
        assertThat(output.explicitRowIds).isTrue()
        val e2 = append("e2", listOf(TestRow(4, "d", 5.0), TestRow(5, "e", 4.0)))
        val e2Id = catalogs.listFiles(cat, "ns", "t").single { it.path == e2 }.dataFileId
        val dvPaths = listOf("s3://$BUCKET/$cat/dv/output.puffin", "s3://$BUCKET/$cat/dv/e2.puffin")
        registerDv(cat, output.dataFileId, dvPaths[0], listOf(0L, 1L, 2L))
        registerDv(cat, e2Id, dvPaths[1], listOf(0L, 1L))
        return DeadFixture(cat, listOf(output.dataFileId, e2Id), listOf(output.path, e2), dvPaths)
    }

    @Test
    fun `a fully deleted group on a sorted table retires through the sorted rewrite, trusted run included`() {
        // The sorted path's own empty shape: the trusted run passes
        // confirmFooters, cannot append (it has a DV), and the merge drains
        // an empty queue — e2's rows all die in the chunk phase — so the
        // rewrite writes 0 rows and the commit retires the group.
        val fx = sortedDeadFixture()
        val local = CompactionService(db.jdbi, store, cfg)
        var built: List<ParquetRewriter.Input> = emptyList()
        local.beforeRewrite = { built = it }
        val group = local.planTable(fx.cat, "ns", "t", cfg).groups.single()
        assertThat(group.files.map { it.dataFileId }).containsExactlyInAnyOrderElementsOf(fx.fileIds)
        assertThat(group.survivingRecords).isZero()
        val headBefore = catalogs.getCatalog(fx.cat).headSnapshotId
        val fileIdBefore = nextFileId(fx.cat)
        val endedBefore = fileEnds(fx.cat).filterKeys { it !in fx.fileIds }

        val outcome = local.compactPlannedGroup(fx.cat, "ns", "t", group)
        assertThat(outcome).isInstanceOf(CompactionService.GroupOutcome.Retired::class.java)
        assertThat(built.single { it.label == fx.paths[0] }.trustedSorted)
            .describedAs("the prior output is a metadata-trusted run")
            .isTrue()

        val head = catalogs.getCatalog(fx.cat).headSnapshotId
        assertThat(head).isEqualTo(headBefore + 1)
        assertThat((outcome as CompactionService.GroupOutcome.Retired).snapshotId).isEqualTo(head)
        assertThat(fileEnds(fx.cat))
            .isEqualTo(endedBefore + fx.fileIds.associateWith { head })
        assertThat(dvEnds(fx.cat).filterKeys { it in fx.dvPaths }).isEqualTo(fx.dvPaths.associateWith { head })
        assertThat(catalogs.listFiles(fx.cat, "ns", "t")).isEmpty()
        assertThat(nextFileId(fx.cat)).isEqualTo(fileIdBefore)
        val staged = removalRows(fx.cat).single { it.reason == "compaction_staging" && it.drainedOutcome == null }
        assertThat(removalStore.exists(staged.path)).isTrue()
        CatalogInvariants.assertRemovalQueueUnreferenced(db.jdbi, fx.cat)
        CatalogInvariants.assertVisibilityBounds(db.jdbi, fx.cat)
        assertThat(cleanup.runOnce(fx.cat, batchSize = 100).removed).isEqualTo(1)
        assertThat(removalStore.exists(staged.path)).isFalse()

        // The same shape through a sweep: the ledger and the run counters.
        val swept = sortedDeadFixture()
        val result = svc.runOnce(swept.cat, cfg)
        assertThat(result.groupsRetired).isEqualTo(1)
        assertThat(result.groupsCompacted).isZero()
        assertThat(result.runsTrusted).describedAs("the prior output, read in place").isEqualTo(1)
        assertThat(result.runsSpilled).describedAs("e2's rows all died: no chunk to spill").isZero()
        assertThat(result.rowGroupsAppended).describedAs("a run with a DV never appends").isZero()
        assertThat(catalogs.listFiles(swept.cat, "ns", "t")).isEmpty()
        val ledger = lastLedgerResult(swept.cat)
        assertThat(ledger["groups_retired"].asLong()).isEqualTo(1)
        assertThat(ledger["runs_trusted"].asLong()).isEqualTo(1)
        assertThat(ledger.has("runs_spilled")).describedAs("zero is omitted from the stored row").isFalse()
    }

    @Test
    fun `a group with one dead input and one live input still compacts into one output`() {
        val fx = deadFixture(liveSecond = true)
        val result = svc.runOnce(fx.cat, deadCfg)
        assertThat(result.groupsCompacted).isEqualTo(1)
        assertThat(result.groupsRetired).isZero()
        assertThat(result.filesIn).isEqualTo(2)
        val output = catalogs.listFiles(fx.cat, "ns", "t").single()
        assertThat(output.recordCount).isEqualTo(1)
        assertThat(output.rowIdStart).describedAs("the one survivor's id").isEqualTo(2)
        val (_, rows) = readParquet(store.get(output.path))
        assertThat(rows).containsExactly(OutRow(3, "c", 3.0, 2))
        assertThat(removalRows(fx.cat).single { it.reason == "compaction_staging" }.drainedOutcome)
            .isEqualTo("registered")
        val head = catalogs.getCatalog(fx.cat).headSnapshotId
        assertThat(fileEnds(fx.cat).filterKeys { it in fx.fileIds }).isEqualTo(fx.fileIds.associateWith { head })
        assertThat(dvEnds(fx.cat)).isEqualTo(fx.dvPaths.associateWith { head })
    }

    @Test
    fun `a DV superseded after planning a zero-survivor group is dv_superseded, not a retirement`() {
        val fx = deadFixture()
        val group = svc.planTable(fx.cat, "ns", "t", deadCfg).groups.single()
        assertThat(group.survivingRecords).isZero()

        // The race: e0's vector is superseded (same positions — vectors
        // only grow, and equal is allowed) after the plan. The rewrite
        // applies the PLANNED vector, which is no longer live.
        val superseding = "s3://$BUCKET/${fx.cat}/dv/e0-again.puffin"
        registerDv(fx.cat, fx.fileIds[0], superseding, listOf(0L, 1L))
        val headBefore = catalogs.getCatalog(fx.cat).headSnapshotId

        assertThat(svc.compactPlannedGroup(fx.cat, "ns", "t", group))
            .isEqualTo(CompactionService.GroupOutcome.SkippedDvSuperseded)
        assertThat(catalogs.getCatalog(fx.cat).headSnapshotId).isEqualTo(headBefore)
        assertThat(fileEnds(fx.cat).values).containsOnlyNulls()
        assertThat(dvEnds(fx.cat)[superseding]).isNull()
        val staged = removalRows(fx.cat).single { it.reason == "compaction_staging" }
        assertThat(staged.drainedOutcome).isNull()

        // The next sweep plans against the live vector and retires.
        val rerun = svc.runOnce(fx.cat, deadCfg)
        assertThat(rerun.groupsRetired).isEqualTo(1)
        assertThat(catalogs.listFiles(fx.cat, "ns", "t")).isEmpty()
    }

    @Test
    fun `a corrupt DV object is invalid_data, not a group retried every sweep`() {
        // #83's crafted bitmap, now a typed refusal at the reader (#84).
        // The refusal has to reach the loop's DURABLE channel: the bytes
        // of a registered .dv object never change, so re-planning the
        // group every sweep is the permanent loop `invalid_data` exists
        // to name. Before this it landed in the catch-all as a failed
        // group — the same misfiling the reserved-id and decimal cases
        // had, one layer out.
        val cat = "compact-corrupt-dv-${counter.incrementAndGet()}"
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
                val path = "s3://$BUCKET/$cat/data/ns/t/c$i.parquet"
                store.put(path, bytes)
                FileRegistration(path, rows.size.toLong(), bytes.size.toLong())
            }
        commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", regs))))
        val fileIds =
            catalogs.listFiles(cat, "ns", "t").sortedBy { it.rowIdStart }.map { it.dataFileId }
        val dvPath = "s3://$BUCKET/$cat/dv/c0.puffin"
        registerDv(cat, fileIds[0], dvPath, listOf(0L))
        // Registered sound, then the OBJECT rots — a hostile or corrupt
        // .dv under valid catalog metadata, which is the only shape that
        // reaches the decoder at all.
        store.put(dvPath, corruptDvBytes())

        val result = svc.runOnce(cat, cfg.copy(targetBytes = 2048))
        assertThat(result.invalidData).describedAs("durable, counted once").isEqualTo(1)
        assertThat(result.failedGroups).describedAs("not a retryable failure").isZero()
        assertThat(result.groupsCompacted).isZero()
        // Nothing was rewritten, and the sweep stays green enough to run
        // again: the group is simply never worth re-attempting.
        assertThat(catalogs.listFiles(cat, "ns", "t").map { it.path })
            .containsExactlyInAnyOrderElementsOf(regs.map { it.path })
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

        val result = svc.runOnce(cat, cfg.copy(targetBytes = 2048))
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
    fun `a no-sort-order group compacts by streaming - row-id order, ids preserved`() {
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

        val result = svc.runOnce(cat, cfg.copy(targetBytes = 2048))
        assertThat(result.groupsCompacted).isEqualTo(1)
        assertThat(result.failedGroups).isEqualTo(0)

        val output = catalogs.listFiles(cat, "ns", "t").single()
        assertThat(output.explicitRowIds).isTrue()
        assertThat(output.recordCount).isEqualTo(4)
        // Row-id order, ids 0..3 positional by append order.
        assertThat(readRowIds(store.get(output.path))).containsExactly(0L, 1L, 2L, 3L)
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
            // Written with the codec compaction writes, because this
            // test measures an OUTPUT's bytes against an INPUT's (see
            // parquetBytes): uncompressed inputs and a compressed output
            // of the same rows are not on the same scale.
            val bytes = parquetBytes(rows, ParquetRewriter.DEFAULT_CODEC)
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

        // Row sizes are DELIBERATE: a+b reach the target=65536 group
        // quota, and their output plus c reach it again so the second
        // run has a group. Inputs and outputs are written with the same
        // codec (above), so the arithmetic holds whatever the codec is.
        //
        // The padding is genuinely high-entropy. An earlier version
        // called a `(id * 31 + it * 17) % 26` cycle "incompressible"; 17
        // and 26 are coprime, so it is a repeating 26-character string
        // that any LZ77 codec collapses ~100x, and the size arithmetic
        // above was only ever true because the writer happened not to
        // compress at all.
        fun paddedRows(
            start: Long,
            n: Int,
            pad: Int,
        ): List<TestRow> =
            (0 until n).map { i ->
                val id = start + i
                val rng = kotlin.random.Random(id)
                // Printable ASCII, uniformly drawn: ~6.6 bits of entropy
                // per byte, which no general-purpose codec improves on
                // by much.
                val chars = String(CharArray(pad) { (0x20 + rng.nextInt(0x5F)).toChar() })
                TestRow(id, chars, id.toDouble())
            }
        appendRows("a", paddedRows(100, 3, 5000)) // row ids 0..2
        appendRows("b", paddedRows(200, 2, 5000)) // row ids 3..4
        val policy = cfg.copy(targetBytes = 65536)

        // A DV on `a` BEFORE the first compaction, so the first output's
        // ids come out NON-CONTIGUOUS. That is what makes the second
        // compaction's assertion discriminating: with ids 0..4 and
        // row_id_start 0, positional numbering produces exactly the same
        // answer as reading the carrier, so the test passed with the
        // explicit_row_ids plumbing neutered — it asserted a property it
        // could not distinguish.
        val fileA = catalogs.listFiles(cat, "ns", "t").single { it.rowIdStart == 0L }
        registerDv(cat, fileA.dataFileId, "s3://$BUCKET/$cat/dv/a.puffin", listOf(1L))

        // First compaction: unsorted table -> physical order = row-id order.
        assertThat(svc.runOnce(cat, policy).groupsCompacted).isEqualTo(1)
        val first = catalogs.listFiles(cat, "ns", "t").single()
        assertThat(first.explicitRowIds).isTrue()
        assertThat(first.rowIdStart).isEqualTo(0)
        // Id 1 is GONE, so the carrier no longer equals row_id_start +
        // ordinal for any row past the first.
        assertThat(readRowIds(store.get(first.path))).containsExactly(0L, 2L, 3L, 4L)

        // A DV lands on the compacted output: physical positions 0 and
        // 3, i.e. row ids 0 and 4 die. Then more data arrives.
        registerDv(cat, first.dataFileId, "s3://$BUCKET/$cat/dv/first.puffin", listOf(0L, 3L))
        // c is a peer of the first output: both are still under the
        // target, so both are candidates for the next group. (This used
        // to assert they shared a size tier; there are no tiers now, and
        // "is it a candidate" is what the planner actually asks.)
        appendRows("c", paddedRows(300, 2, 10000)) // row ids 5..6
        val peers = catalogs.listFiles(cat, "ns", "t")
        assertThat(peers.map { it.fileSizeBytes < policy.targetBytes }).containsOnly(true)

        // Second compaction: the explicit-id input's DV drops by POSITION,
        // survivors keep the ids their _hog_row_id column carries.
        val second = svc.runOnce(cat, policy)
        assertThat(second.groupsCompacted).isEqualTo(1)
        val output = catalogs.listFiles(cat, "ns", "t").single()
        assertThat(output.recordCount).isEqualTo(4)
        assertThat(output.rowIdStart).isEqualTo(2) // 0 and 4 died; min survivor is 2
        // [2, 3] from the carrier, NOT [1, 2] — which is what positional
        // numbering of the surviving ordinals {1, 2} would have produced.
        assertThat(readRowIds(store.get(output.path))).containsExactly(2L, 3L, 5L, 6L)
        assertThat(scans.planScan(cat, "ns", "t").single().deleteFile).isNull()
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
        val policy = cfg.copy(targetBytes = bytes.size.toLong() * 8, maxGroupsPerRun = 20)
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
    }

    @Test
    fun `a freshly written output waits for the next run rather than being re-consumed`() {
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
            // Same codec for inputs and output: this test compares the
            // output's size against the target the inputs were sized
            // to, so the two must be measured on the same scale.
            val bytes = parquetBytes(rows, ParquetRewriter.DEFAULT_CODEC)
            val path = "s3://$BUCKET/$cat/$name.parquet"
            store.put(path, bytes)
            return FileRegistration(path, count.toLong(), bytes.size.toLong())
        }
        // `peer` is twice the size of `a` and `b` DELIBERATELY. With all
        // three near-identical, the run-2 group is decided by a coin-flip
        // between near-tied parquet byte sizes, and the dominance split
        // (a file more than twice the bytes held starts its own group)
        // sits right on that boundary. At 2x, run 1's output and `peer`
        // are comparable, so neither can dominate the other and the
        // grouping is deterministic.
        val inputs = listOf(input("a", 1000), input("b", 1000), input("peer", 2000))
        // Close groups on the fan-in cap, not on bytes: a target every
        // file stays under keeps all of them candidates, so the only
        // thing that can hold `peer` back is the rule under test. Sizing
        // the target to `a + b` instead would leave the OUTPUT'S size
        // deciding whether run two has anything to do, which is a
        // compression measurement, not this property.
        val policy =
            cfg.copy(
                targetBytes = inputs.sumOf { it.fileSizeBytes } * 4,
                maxInputFiles = 2,
                // Pinned: `maxInputFiles` is the scaling fan-in's floor
                // now, and this test closes groups on the fan-in cap.
                maxFanIn = 2,
                maxGroupsPerRun = 20,
            )
        commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", inputs))))
        val before = catalogs.getCatalog(cat).headSnapshotId
        val first = svc.runOnce(cat, policy)
        assertThat(first.groupsCompacted).isEqualTo(1)
        assertThat(first.filesIn).isEqualTo(2)
        assertThat(first.failedGroups).isZero()
        val live = catalogs.listFiles(cat, "ns", "t")
        assertThat(live).hasSize(2)
        val promoted = live.single { it.explicitRowIds }
        // There IS enough to group again, proving this wasn't just an
        // under-target no-op. It must nevertheless wait for run two: an
        // output cannot be an input to the run that produced it.
        val next = svc.planTable(cat, "ns", "t", policy)
        assertThat(next.groups).hasSize(1)
        assertThat(next.groups.single().files.map { it.dataFileId }).contains(promoted.dataFileId)
        val second = svc.runOnce(cat, policy)
        assertThat(second.groupsCompacted).isEqualTo(1)
        assertThat(second.filesIn).isEqualTo(2)
        val output = catalogs.listFiles(cat, "ns", "t").single()
        assertThat(output.recordCount).isEqualTo(4000)
        assertThat(readRowIds(store.get(output.path))).containsExactlyElementsOf((0L until 4000).toList())
        assertThat(catalogs.listFiles(cat, "ns", "t", before).map { it.path })
            .containsExactlyInAnyOrderElementsOf(inputs.map { it.path })
    }

    @Test
    fun `multiple groups in one bucket execute up to budget including failed attempts`() {
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
        val policy = cfg.copy(targetBytes = bytes.size.toLong() * 2, maxGroupsPerRun = 2)
        assertThat(svc.planTable(cat, "ns", "t", policy).groups).hasSize(3)
        val result = svc.runOnce(cat, policy)
        assertThat(result.failedGroups).isEqualTo(1)
        assertThat(result.groupsCompacted).isEqualTo(1)
        assertThat(result.filesIn).isEqualTo(2)
        // Last pair untouched: the failed first group consumed budget.
        assertThat(catalogs.listFiles(cat, "ns", "t").map { it.path })
            .contains(regs[0].path, regs[1].path, regs[4].path, regs[5].path)
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

        Hydrator(db.jdbi, store).runOnce()
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

        val result = svc.runOnce(cat, cfg.copy(targetBytes = 2048))
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

        val result = svc.runOnce(cat, cfg.copy(targetBytes = 2048))
        assertThat(result.groupsCompacted).isZero()
        assertThat(result.unconvertibleSchema).isEqualTo(1)
        assertThat(result.skippedConflicts).isZero()
        assertThat(result.dvSuperseded).isZero()

        // Nothing moved: no snapshot, both files live. The staging
        // ticket IS claimed, though, and that is not an oversight —
        // unconvertibility is detected while reading the inputs, which
        // now happens during the rewrite, after the claim. See the
        // ordering note in CompactionService.compactGroup. The claimed
        // path holds no object (the multipart upload aborted) and the
        // cleanup drain reclaims the row.
        assertThat(catalogs.getCatalog(cat).headSnapshotId).isEqualTo(headBefore)
        assertThat(catalogs.listFiles(cat, "ns", "t")).hasSize(2)
        assertThat(removalRows(cat)).describedAs("claimed, reclaimable, holds nothing").hasSize(1)
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
        // Only the compaction output survives — identified as "none of
        // the inputs" rather than by a name shape (#23).
        assertThat(catalogs.listFiles(fx.cat, "ns", "t").map { it.path })
            .singleElement()
            .matches({ it !in fx.paths && it != fx.dvPath }, "a fresh path, not an input")
    }

    @Test
    fun `inputs with pending stats still produce a provided output - the footer needs no input rows`() {
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
        val result = svc.runOnce(cat, cfg.copy(targetBytes = 2048))
        assertThat(result.groupsCompacted).isEqualTo(1)
        val output = catalogs.listFiles(cat, "ns", "t").single()
        assertThat(output.explicitRowIds).isTrue()
        // The inputs had NO stats rows at all — which is exactly why the
        // old merge could not produce any, and registered the output
        // 'pending' for a hydrator sweep to come back and read the
        // footer out of S3. The rewrite already held that footer.
        assertThat(output.statsState.wire).isEqualTo("provided")
        assertThat(output.recordCount).isEqualTo(3)
        assertThat(readSchemaOnly(store.get(output.path)).fields.map { it.name })
            .containsExactly("id", ParquetRewriter.ROW_ID_COLUMN)
        // Only `id` is live, so only `id` gets a row: the dropped
        // name/score leaves are not in the output file and _hog_row_id is
        // not a catalog column.
        val stats = storedStats(cat, output.dataFileId)
        assertThat(stats).containsOnlyKeys(1L)
        assertThat(stats.getValue(1L).valueCount).isEqualTo(3)
        assertThat(stats.getValue(1L).nullCount).isZero()
        assertThat(stats.getValue(1L).lower).isEqualTo(IcebergSingleValue.encodeLong(1))
        assertThat(stats.getValue(1L).upper).isEqualTo(IcebergSingleValue.encodeLong(3))
        // Offsets do not wait for the hydrator either: same footer, same
        // commit.
        assertThat(storedSplitOffsets(cat, output.dataFileId))
            .containsExactlyElementsOf(ThriftRowGroupStarts.of(store.get(output.path)).offsets)
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
