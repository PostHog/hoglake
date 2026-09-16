package com.posthog.hoglake.hydrator

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.compaction.CompactionConfig
import com.posthog.hoglake.compaction.CompactionService
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.model.TableAppend
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
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.MinIOContainer
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

/**
 * Nested columns end to end through the real services: DDL, a real
 * nested parquet file in MinIO, a DEFERRED-stats commit, the hydrator's
 * footer read, and then compaction and a second hydration of the
 * rewritten file.
 *
 * Deferred stats are the point. With inline stats the client's numbers
 * go straight into the catalog and the server never looks at the file;
 * it is the hydrator that has to walk the nested parquet shape, find
 * each leaf by field id, and produce a bound in the right encoding. The
 * per-leaf bounds are asserted BYTE for byte against the same encoder
 * the codec tests pin, before and after compaction — compaction
 * rewrites the file and the hydrator re-reads it, so a drift anywhere
 * in that loop replaces a correct bound with a different one on a file
 * nobody will look at again.
 *
 * The containers themselves must have NO stats row at any point: a
 * bound on "the map" is not a wrong number, it is a category error, and
 * the commit path refuses one from a client for the same reason.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NestedHydrationIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val hydrator by lazy { Hydrator(db.jdbi, store) }
    private val counter = AtomicInteger(0)

    /**
     * Placeholder: the planning config is derived from the fixture's
     * ACTUAL file sizes inside the test (see [configFor]), because a
     * hardcoded target either stops grouping or starts grouping one file
     * the moment the nested fixture's bytes move.
     */
    private val compactionConfig = CompactionConfig(targetBytes = 65536, tierTarget = 2, maxGroupsPerRun = 4)
    private val compaction by lazy { CompactionService(db.jdbi, store, compactionConfig) }

    /**
     * A config under which the fixture's two files form exactly one
     * group, derived from their real sizes.
     *
     * Planning partitions candidates BY TIER and only groups within one,
     * so the two files must land in the same tier and their combined
     * bytes must reach its quota. Tiers are geometric downward from the
     * target with spacing [CompactionConfig.tierTarget]: setting the
     * target to the SUM puts both files in the top tier (each is below
     * the sum, and with T=4 the next floor down is sum/4, which neither
     * is below as long as they are within 3x of each other) and makes
     * the quota exactly the sum, which the pair reaches precisely.
     */
    private fun configFor(sizes: List<Long>) =
        CompactionConfig(targetBytes = sizes.sum(), tierTarget = 4, maxGroupsPerRun = 4)

    @AfterAll
    fun tearDown() = db.close()

    private companion object {
        const val BUCKET = "hoglake-nested-test"

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

        fun footerSizeOf(bytes: ByteArray): Long =
            ByteBuffer.wrap(bytes, bytes.size - 8, 4).order(ByteOrder.LITTLE_ENDIAN).int.toLong()
    }

    /**
     * The table under test, with the field ids the server assigns
     * depth-first:
     *
     *   1 id           long
     *   2 tags         list
     *   3   element    string
     *   4 addr         struct
     *   5   city       string
     *   6   zip        int
     *   7 props        map
     *   8   key        string
     *   9   value      long
     */
    private fun nestedColumns(): List<ColumnDef> =
        listOf(
            ColumnDef("id", ColType.LONG, nullable = false),
            ColumnDef(
                "tags",
                ColType.LIST,
                children = listOf(ColumnDef("element", ColType.STRING)),
            ),
            ColumnDef(
                "addr",
                ColType.STRUCT,
                children = listOf(ColumnDef("city", ColType.STRING), ColumnDef("zip", ColType.INT)),
            ),
            ColumnDef(
                "props",
                ColType.MAP,
                children =
                    listOf(
                        ColumnDef("key", ColType.STRING, nullable = false),
                        ColumnDef("value", ColType.LONG),
                    ),
            ),
        )

    /** The bounds every leaf must carry for [rows]. */
    private fun expectedBounds(): Map<Long, Pair<ByteArray, ByteArray>> =
        mapOf(
            1L to (IcebergSingleValue.encodeLong(1) to IcebergSingleValue.encodeLong(4)),
            3L to ("alpha".toByteArray() to "zulu".toByteArray()),
            5L to ("amsterdam".toByteArray() to "zagreb".toByteArray()),
            6L to (IcebergSingleValue.encodeInt(-1) to IcebergSingleValue.encodeInt(99999)),
            8L to ("a".toByteArray() to "z".toByteArray()),
            9L to (IcebergSingleValue.encodeLong(-7) to IcebergSingleValue.encodeLong(1_000_000)),
        )

    private val leafIds = setOf(1L, 3L, 5L, 6L, 8L, 9L)
    private val containerIds = setOf(2L, 4L, 7L)

    @Test
    fun `a nested file hydrates per-leaf bounds, and compaction preserves them byte for byte`() {
        val cat = "nested-e2e-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        val table = catalogs.createTable(cat, "ns", "t", nestedColumns())

        // The DDL landed the shape the fixture assumes. Asserted rather
        // than trusted, because every bound below is keyed on these ids.
        val ids = table.columns.flatMap { it.selfAndDescendants() }.associate { it.def.name to it.fieldId }
        assertThat(ids).containsAllEntriesOf(
            mapOf(
                "id" to 1L, "tags" to 2L, "element" to 3L, "addr" to 4L,
                "city" to 5L, "zip" to 6L, "props" to 7L, "key" to 8L, "value" to 9L,
            ),
        )

        // Two files, so compaction has a group to plan.
        val paths =
            listOf(0, 1).map { half ->
                val bytes = nestedParquet(half)
                val path = "s3://$BUCKET/$cat/data/ns/t/f$half.parquet"
                store.put(path, bytes)
                FileRegistration(
                    path = path,
                    recordCount = 2,
                    fileSizeBytes = bytes.size.toLong(),
                    footerSize = footerSizeOf(bytes),
                    // DEFERRED: the hydrator produces every bound below.
                    columnStats = null,
                )
            }
        commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", paths))))

        assertThat(hydrator.runOnce()).describedAs("both files hydrate").isEqualTo(2)
        assertBounds(cat, "after hydration")

        // Compaction rewrites the nested file; the commit re-reads the
        // output's footer, so the bounds are produced a SECOND time from
        // a schema this code generated rather than the fixture's.
        val cfg = configFor(paths.map { it.fileSizeBytes })
        val result = compaction.runOnce(cat, cfg)
        assertThat(result.groupsCompacted).describedAs("nested tables are compactable").isEqualTo(1)
        assertThat(result.unconvertibleSchema)
            .describedAs("a nested schema is not an unconvertible one")
            .isZero()
        assertThat(result.filesOut).isEqualTo(1)

        assertBounds(cat, "after compaction")

        // And a re-compaction changes nothing: a tier-1 output is a
        // tier-2 input in production, and per-pass drift would be
        // invisible in a single run.
        compaction.runOnce(cat, cfg)
        assertBounds(cat, "after re-compaction")
    }

    @Test
    fun `a nested SORTED table is planned under the derated group budget`() {
        // F2's boundary, end to end through the planner. The sorted path
        // materializes a whole group to sort it, and a nested group's
        // object graph measured 30-70x its compressed bytes — so
        // targetBytes is not a heap bound for such a table and the
        // planner derates it by nestedSortExpansion.
        //
        // The test pins the BOUNDARY, not the arithmetic: with a budget
        // the pair exactly reaches, the group forms; with the same raw
        // budget and a derate applied, it does not — and an UNSORTED
        // table with the same derate still groups, because only the
        // sorted path materializes.
        val cat = "nested-derate-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "sorted", nestedColumns())
        catalogs.createTable(cat, "ns", "unsorted", nestedColumns())
        // id is field 1 on both tables; sort only the first.
        AlterService(db.jdbi).alterTable(
            cat,
            "ns",
            "sorted",
            listOf(AlterOp.SetSortOrder(listOf(SortFieldDef(1, SortDirection.ASC, NullOrder.NULLS_LAST)))),
        )

        val sizes = mutableListOf<Long>()
        for (table in listOf("sorted", "unsorted")) {
            val regs =
                listOf(0, 1).map { half ->
                    val bytes = nestedParquet(half)
                    val path = "s3://$BUCKET/$cat/data/ns/$table/f$half.parquet"
                    store.put(path, bytes)
                    sizes += bytes.size.toLong()
                    FileRegistration(
                        path = path,
                        recordCount = 2,
                        fileSizeBytes = bytes.size.toLong(),
                        footerSize = footerSizeOf(bytes),
                        columnStats = null,
                    )
                }
            commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", table, regs))))
        }
        hydrator.runOnce()

        // The budget at which the pair exactly reaches a tier quota.
        val raw = configFor(sizes.take(2))

        // Derate OFF: both tables group, which is the premise — without
        // it "does not group" below would prove nothing.
        val noDerate = raw.copy(nestedSortExpansion = 1)
        assertThat(noDerate.effectiveTargetBytes(columnsOf(cat, "sorted"), sorted = true))
            .isEqualTo(raw.targetBytes)
        assertThat(compaction.runOnce(cat, noDerate).groupsCompacted)
            .describedAs("premise: at the raw budget both tables have a group")
            .isEqualTo(2)

        // Now the same files again, with the derate ON.
        for (table in listOf("sorted", "unsorted")) {
            val regs =
                listOf(0, 1).map { half ->
                    val bytes = nestedParquet(half)
                    val path = "s3://$BUCKET/$cat/data/ns/$table/g$half.parquet"
                    store.put(path, bytes)
                    FileRegistration(
                        path = path,
                        recordCount = 2,
                        fileSizeBytes = bytes.size.toLong(),
                        footerSize = footerSizeOf(bytes),
                        columnStats = null,
                    )
                }
            commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", table, regs))))
        }
        hydrator.runOnce()

        val derated = raw.copy(nestedSortExpansion = 64)
        assertThat(derated.effectiveTargetBytes(columnsOf(cat, "sorted"), sorted = true))
            .describedAs("a nested SORTED table is planned smaller")
            .isEqualTo(maxOf(2L, raw.targetBytes / 64))
        assertThat(derated.effectiveTargetBytes(columnsOf(cat, "unsorted"), sorted = false))
            .describedAs("an UNSORTED table streams, so it keeps the raw budget")
            .isEqualTo(raw.targetBytes)

        // Only the unsorted table still forms a group: the sorted one's
        // files are now each above its derated budget.
        val result = compaction.runOnce(cat, derated)
        assertThat(result.groupsCompacted)
            .describedAs("the derate splits the nested sorted table out of the plan")
            .isEqualTo(1)
        assertThat(liveFileCount(cat, "sorted"))
            .describedAs("the sorted table's new files stayed uncompacted")
            .isEqualTo(3) // the earlier run's output + the two new ones
    }

    @Test
    fun `a FLAT sorted table keeps the raw budget however large the derate`() {
        // The derate is about nested object graphs, not about sorting.
        // Applying it to every sorted table would shrink flat tables'
        // groups 64-fold for nothing.
        val flat =
            listOf(ColumnDef("id", ColType.LONG, nullable = false), ColumnDef("name", ColType.STRING))
        val cfg = CompactionConfig(targetBytes = 1_000_000, tierTarget = 4, maxGroupsPerRun = 4)
        val cols = flat.mapIndexed { i, d -> com.posthog.hoglake.model.Column(i + 1L, i, d) }
        assertThat(cfg.effectiveTargetBytes(cols, sorted = true)).isEqualTo(1_000_000)
    }

    /** The live column forest of one table, for the derate decision. */
    private fun columnsOf(
        cat: String,
        table: String,
    ) = catalogs.getTable(cat, "ns", table).columns

    private fun liveFileCount(
        cat: String,
        table: String,
    ): Int = catalogs.listFiles(cat, "ns", table).size

    @Test
    fun `a foreign file with no id on its struct group blocks renames after hydration`() {
        // The end of the data-loss chain, walked from the top. A writer
        // that puts ids on every LEAF but none on the struct group has
        // produced a file whose struct binds by NAME. Until the flag
        // covered containers, nothing recorded that: the rename guard
        // stayed quiet, and `rename_column addr -> location` left the
        // compaction rewriter unable to match the subtree by id (no id)
        // or by name (changed), so it null-filled the lot and
        // end-snapshotted the input for expiry to delete.
        val cat = "nested-idless-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(
            cat,
            "ns",
            "t",
            listOf(
                ColumnDef("k", ColType.INT),
                ColumnDef(
                    "addr",
                    ColType.STRUCT,
                    children = listOf(ColumnDef("a", ColType.INT), ColumnDef("b", ColType.STRING)),
                ),
            ),
        )

        val bytes = leafIdsOnlyParquet()
        val path = "s3://$BUCKET/$cat/data/ns/t/foreign.parquet"
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
                                    recordCount = 3,
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

        assertThat(missingFieldIdsFlag(cat))
            .describedAs("the id-less struct GROUP must flag the file")
            .isTrue()

        // ...and the flag is what the rename guard reads.
        assertThatThrownBy {
            AlterService(db.jdbi).alterTable(
                cat,
                "ns",
                "t",
                listOf(AlterOp.RenameColumn("addr", "location")),
            )
        }
            .isInstanceOf(HoglakeException.IdlessFilesPresent::class.java)
            .hasMessageContaining("id-less")

        // The leaves still bind by id, so stats are unaffected: the two
        // questions ("can I rename?" and "can I read stats?") are
        // different, and only one of them is about names.
        assertThat(statsFieldIds(cat)).containsExactlyInAnyOrder(1L, 3L, 4L)
    }

    /** `hog_data_file.missing_field_ids` for the catalog's one live file. */
    private fun missingFieldIdsFlag(cat: String): Boolean =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT f.missing_field_ids FROM hog_data_file f
                JOIN hog_catalog c ON c.catalog_id = f.catalog_id
                WHERE c.name = :cat AND f.end_snapshot IS NULL
                """,
            ).bind("cat", cat).mapTo(Boolean::class.javaObjectType).one()
        }

    private fun statsFieldIds(cat: String): List<Long> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT s.field_id FROM hog_file_column_stats s
                JOIN hog_catalog c ON c.catalog_id = s.catalog_id
                WHERE c.name = :cat
                """,
            ).bind("cat", cat).mapTo(Long::class.java).list()
        }

    /**
     * The probe file: ids on `k`, `addr.a` and `addr.b`, none on the
     * `addr` group itself.
     */
    private fun leafIdsOnlyParquet(): ByteArray {
        val schema =
            MessageType(
                "foreign",
                listOf(
                    Types.optional(PrimitiveTypeName.INT32).id(1).named("k"),
                    Types.optionalGroup()
                        .addFields(
                            Types.optional(PrimitiveTypeName.INT32).id(3).named("a"),
                            Types.optional(PrimitiveTypeName.BINARY)
                                .`as`(LogicalTypeAnnotation.stringType()).id(4).named("b"),
                        )
                        // NO .id(...): that is the whole point of the fixture.
                        .named("addr"),
                ),
            )
        val tmp = Files.createTempFile("hoglake-foreign", ".parquet")
        Files.deleteIfExists(tmp)
        val factory = SimpleGroupFactory(schema)
        ExampleParquetWriter.builder(LocalOutputFile(tmp))
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build()
            .use { w ->
                for (i in 0 until 3) {
                    val g = factory.newGroup()
                    g.add(0, i)
                    val inner = g.addGroup(1)
                    inner.add(0, 100 + i)
                    inner.add(1, "v$i")
                    w.write(g)
                }
            }
        return try {
            Files.readAllBytes(tmp)
        } finally {
            Files.deleteIfExists(tmp)
        }
    }

    /**
     * Every LEAF carries its expected bound and every CONTAINER carries
     * no stats row at all, over the live files only.
     */
    private fun assertBounds(
        cat: String,
        what: String,
    ) {
        val rows =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT s.field_id, s.lower_bound, s.upper_bound
                    FROM hog_file_column_stats s
                    JOIN hog_catalog c ON c.catalog_id = s.catalog_id
                    JOIN hog_data_file f
                      ON f.catalog_id = s.catalog_id AND f.data_file_id = s.data_file_id
                    WHERE c.name = :cat AND f.end_snapshot IS NULL
                    """,
                )
                    .bind("cat", cat)
                    .map { rs, _ ->
                        Triple(rs.getLong("field_id"), rs.getBytes("lower_bound"), rs.getBytes("upper_bound"))
                    }
                    .list()
            }
        assertThat(rows.map { it.first }.toSet())
            .describedAs("stats rows exist for exactly the leaves, %s", what)
            .isEqualTo(leafIds)
        assertThat(rows.map { it.first }.toSet().intersect(containerIds))
            .describedAs("no container has a stats row, %s", what)
            .isEmpty()

        // Merge across files (there are two before compaction, one after)
        // by the same rule the catalog's readers use, then compare bytes.
        val expected = expectedBounds()
        for (fieldId in leafIds) {
            val mine = rows.filter { it.first == fieldId }
            assertThat(mine).describedAs("stats for field %d, %s", fieldId, what).isNotEmpty()
            val type = typeOfLeaf(fieldId)
            val lowers = mine.mapNotNull { it.second }
            val uppers = mine.mapNotNull { it.third }
            assertThat(lowers).describedAs("lower bounds present for field %d, %s", fieldId, what).hasSize(mine.size)
            val lo = lowers.minWith { a, b -> compare(type, a, b) }
            val hi = uppers.maxWith { a, b -> compare(type, a, b) }
            assertThat(lo)
                .describedAs("lower bound of field %d, %s", fieldId, what)
                .isEqualTo(expected.getValue(fieldId).first)
            assertThat(hi)
                .describedAs("upper bound of field %d, %s", fieldId, what)
                .isEqualTo(expected.getValue(fieldId).second)
        }
    }

    private fun typeOfLeaf(fieldId: Long): ColType =
        when (fieldId) {
            1L, 9L -> ColType.LONG
            6L -> ColType.INT
            else -> ColType.STRING
        }

    private fun compare(
        type: ColType,
        a: ByteArray,
        b: ByteArray,
    ): Int =
        IcebergSingleValue.compareValues(
            type,
            IcebergSingleValue.decode(type, a),
            IcebergSingleValue.decode(type, b),
        )

    // ---- the fixture file ---------------------------------------------------

    /**
     * Two rows of a real nested parquet file, shaped exactly as pyarrow
     * writes one (3-level LIST, `key_value` MAP, field ids on every
     * level), with [half] selecting the first or second pair of rows.
     * Between them the two halves span every bound asserted above.
     */
    private fun nestedParquet(half: Int): ByteArray {
        val schema = fixtureSchema()
        val tmp = Files.createTempFile("hoglake-nested", ".parquet")
        Files.deleteIfExists(tmp)
        val factory = SimpleGroupFactory(schema)
        val rows =
            if (half == 0) {
                listOf(
                    Row(1, listOf("alpha", "mike"), "amsterdam", 99999, listOf("a" to 1_000_000L)),
                    // A null list, a null struct and an EMPTY map in one
                    // row: the shapes a naive walk conflates.
                    Row(2, null, null, null, emptyList()),
                )
            } else {
                listOf(
                    Row(3, listOf("zulu"), "zagreb", -1, listOf("z" to -7L)),
                    Row(4, emptyList(), "berlin", 10115, listOf("m" to 0L, "n" to 42L)),
                )
            }
        ExampleParquetWriter.builder(LocalOutputFile(tmp))
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build()
            .use { w -> for (row in rows) w.write(fill(factory.newGroup(), row)) }
        return try {
            Files.readAllBytes(tmp)
        } finally {
            Files.deleteIfExists(tmp)
        }
    }

    private class Row(
        val id: Long,
        val tags: List<String>?,
        val city: String?,
        val zip: Int?,
        val props: List<Pair<String, Long>>,
    )

    private fun fill(
        g: Group,
        row: Row,
    ): Group {
        g.add(0, row.id)
        if (row.tags != null) {
            val list = g.addGroup(1)
            for (t in row.tags) list.addGroup(0).add(0, t)
        }
        if (row.city != null || row.zip != null) {
            val addr = g.addGroup(2)
            if (row.city != null) addr.add(0, row.city)
            if (row.zip != null) addr.add(1, row.zip)
        }
        val map = g.addGroup(3)
        for ((k, v) in row.props) {
            val entry = map.addGroup(0)
            entry.add(0, k)
            entry.add(1, v)
        }
        return g
    }

    private fun fixtureSchema(): MessageType =
        Types.buildMessage()
            .addField(Types.required(PrimitiveTypeName.INT64).id(1).named("id"))
            .addField(
                Types.optionalGroup()
                    .addField(
                        Types.repeatedGroup()
                            .addField(
                                Types.optional(PrimitiveTypeName.BINARY)
                                    .`as`(LogicalTypeAnnotation.stringType()).id(3).named("element"),
                            )
                            .named("list"),
                    )
                    .`as`(LogicalTypeAnnotation.listType())
                    .id(2).named("tags"),
            )
            .addField(
                Types.optionalGroup()
                    .addFields(
                        Types.optional(PrimitiveTypeName.BINARY)
                            .`as`(LogicalTypeAnnotation.stringType()).id(5).named("city"),
                        Types.optional(PrimitiveTypeName.INT32).id(6).named("zip"),
                    )
                    .id(4).named("addr"),
            )
            .addField(
                Types.optionalGroup()
                    .addField(
                        Types.repeatedGroup()
                            .addFields(
                                Types.required(PrimitiveTypeName.BINARY)
                                    .`as`(LogicalTypeAnnotation.stringType()).id(8).named("key"),
                                Types.optional(PrimitiveTypeName.INT64).id(9).named("value"),
                            )
                            .named("key_value"),
                    )
                    .`as`(LogicalTypeAnnotation.mapType())
                    .id(7).named("props"),
            )
            .named("hoglake_nested_fixture")
}
