package com.posthog.hoglake.hydrator

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.observability.CatalogMetrics
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.stats.IcebergSingleValue
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.testing.TestImages
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

/**
 * The hydrator's object-prefix check (PR 2.1c) on real parquet in MinIO,
 * committed through the real commit path with DEFERRED stats: on
 * `ingest.events` (the option on `team_id`, identity spec) every pending
 * file's footer must prove that every row holds the value its prefix
 * names, or the file flips to `provided` QUARANTINED. `ingest.plain` has
 * the same columns and spec without the option and is never checked.
 *
 * The files carry field ids (team_id 1, v 2), so stats bind by id against
 * the LIVE columns; the hydrator's default floor of 0 writes stats rows
 * for every file unless a test raises it.
 */
@Tag("integration")
class HydratorObjectPrefixIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val alter = AlterService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val cat = "prefix-hydrate"

    @AfterEach
    fun tearDown() = db.close()

    private companion object {
        const val BUCKET = "hoglake-test"

        val minio by lazy { TestImages.minio().also { it.start() } }

        val store: ObjectStore by lazy {
            ObjectStore(minio.s3URL, "us-east-1", minio.userName, minio.password, true).also {
                it.createBucket(BUCKET)
            }
        }

        val FOURS = List(10) { 4L }
        val FORTY_TWOS = List(10) { 42L }
    }

    /** `ingest.events` with the option on team_id, `ingest.plain` without it, plus [extra] option-on tables. */
    private fun fixture(vararg extra: String) {
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ingest")
        val columns = listOf(ColumnDef("team_id", ColType.LONG), ColumnDef("v", ColType.LONG))
        for (table in listOf("events", "plain", *extra)) {
            catalogs.createTable(cat, "ingest", table, columns)
            alter.alterTable(
                cat,
                "ingest",
                table,
                listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(1, Transform.IDENTITY)))),
            )
            if (table != "plain") {
                alter.alterTable(
                    cat,
                    "ingest",
                    table,
                    listOf(AlterOp.SetObjectPrefixField("team_id")),
                )
            }
        }
    }

    private fun under(
        table: String,
        value: String,
    ) = "s3://$BUCKET/$cat/data/ingest/$table/team_id=$value/${UUID.randomUUID()}.parquet"

    /**
     * A parquet file of `team_id` (optional int64, id [teamId]; left out
     * when null) and `v` (required int64, id [vId]), one row per entry
     * of [teams], a null entry being a SQL NULL; a row group closes every
     * [rowGroupRows] rows when given.
     */
    private fun parquet(
        teams: List<Long?>,
        rowGroupRows: Int? = null,
        teamId: Int? = 1,
        vId: Int = 2,
        teamUint16: Boolean = false,
    ): ByteArray {
        val fields = mutableListOf<Type>()
        if (teamId != null) {
            fields +=
                if (teamUint16) {
                    Types.optional(INT32).`as`(LogicalTypeAnnotation.intType(16, false)).id(teamId).named("team_id")
                } else {
                    Types.optional(INT64).id(teamId).named("team_id")
                }
        }
        fields += Types.required(INT64).id(vId).named("v")
        val schema = MessageType("t", fields)
        val tmp = Files.createTempFile("hoglake-prefix", ".parquet")
        try {
            Files.deleteIfExists(tmp)
            val factory = SimpleGroupFactory(schema)
            ExampleParquetWriter.builder(LocalOutputFile(tmp))
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .apply {
                    if (rowGroupRows != null) {
                        withRowGroupRowCountLimit(rowGroupRows)
                        withMinRowCountForPageSizeCheck(1)
                    }
                }
                .build()
                .use { w ->
                    teams.forEachIndexed { i, team ->
                        val g = factory.newGroup()
                        if (teamId != null && team != null) {
                            if (teamUint16) g.add("team_id", team.toInt()) else g.add("team_id", team)
                        }
                        g.add("v", i.toLong())
                        w.write(g)
                    }
                }
            return Files.readAllBytes(tmp)
        } finally {
            Files.deleteIfExists(tmp)
        }
    }

    private fun footerSizeOf(bytes: ByteArray): Long =
        ByteBuffer.wrap(bytes, bytes.size - 8, 4).order(ByteOrder.LITTLE_ENDIAN).int.toLong()

    /**
     * Uploads [bytes] to [path] (unless [upload] is false) and commits it
     * to [table] with deferred stats and partition value [value]; with
     * [tail] false it registers no footer_size, so the hydrator reads the
     * whole object through [ObjectStore.get].
     */
    private fun register(
        table: String,
        path: String,
        value: String,
        bytes: ByteArray,
        rows: Long,
        upload: Boolean = true,
        tail: Boolean = true,
        values: List<String?> = listOf(value),
        catalog: String = cat,
    ): String {
        if (upload) store.put(path, bytes)
        val file =
            FileRegistration(
                path,
                rows,
                bytes.size.toLong(),
                footerSize = if (tail) footerSizeOf(bytes) else null,
                columnStats = null,
                partitionValues = values,
            )
        commits.commit(
            catalog,
            CommitRequest(
                readSnapshot = db.head(catalog),
                appends = listOf(TableAppend("ingest", table, listOf(file))),
            ),
        )
        return path
    }

    private fun register(
        table: String,
        value: String,
        teams: List<Long?>,
        rowGroupRows: Int? = null,
    ): String = register(table, under(table, value), value, parquet(teams, rowGroupRows), teams.size.toLong())

    private data class FileState(val state: String, val quarantined: Boolean, val statsRows: Long)

    private fun state(path: String): FileState =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT f.stats_state, f.prefix_quarantined,
                       (SELECT count(*) FROM hog_file_column_stats s
                         WHERE s.catalog_id = f.catalog_id AND s.data_file_id = f.data_file_id) AS stats_rows
                FROM hog_data_file f WHERE f.path = :path
                """,
            ).bind("path", path)
                .map { rs, _ -> FileState(rs.getString(1), rs.getBoolean(2), rs.getLong(3)) }
                .one()
        }

    private fun teamBounds(path: String): Pair<ByteArray?, ByteArray?> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT s.lower_bound, s.upper_bound FROM hog_file_column_stats s
                JOIN hog_data_file f USING (catalog_id, data_file_id) WHERE f.path = :path AND s.field_id = 1
                """,
            ).bind("path", path).map { rs, _ -> rs.getBytes(1) to rs.getBytes(2) }.one()
        }

    private fun lastSweep(): JsonNode =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT r.result::text FROM hog_maintenance_run r JOIN hog_catalog c USING (catalog_id)
                WHERE c.name = :cat AND r.run_trigger = 'loop' ORDER BY r.run_id DESC LIMIT 1
                """,
            ).bind("cat", cat).mapTo(String::class.java).one()
        }.let { ObjectMapper().readTree(it) }

    private fun quarantinedGauge(): Double? {
        val registry = SimpleMeterRegistry()
        CatalogMetrics(db.jdbi, registry).sampleOnce()
        return registry.find("hoglake_object_prefix_quarantined_files").tag("catalog", cat).gauge()?.value()
    }

    @Test
    fun `a pending file whose footer proves its value flips to provided, unquarantined`() {
        fixture()
        val path = register("events", "4", FOURS)
        assertThat(state(path).state).isEqualTo("pending")
        Hydrator(db.jdbi, store).runOnce()
        assertThat(state(path)).isEqualTo(FileState("provided", quarantined = false, statsRows = 2))
        val (lower, upper) = teamBounds(path)
        assertThat(lower).isEqualTo(IcebergSingleValue.encodeLong(4))
        assertThat(upper).isEqualTo(IcebergSingleValue.encodeLong(4))
        assertThat(lastSweep()["hydrated"].asLong()).isEqualTo(1)
        assertThat(lastSweep().has("quarantined")).describedAs("a zero is omitted").isFalse()
        assertThat(quarantinedGauge()).isEqualTo(0.0)
    }

    /**
     * MUTATION: make the flip's `prefix_quarantined` constant false (or
     * [Hydrator]'s mismatch always null) and this file reads clean.
     */
    @Test
    fun `team 42's rows under team_id=4 are quarantined with their truthful stats, counted and gauged`() {
        fixture()
        val path = register("events", "4", FORTY_TWOS)
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        Metrics.bind(registry)
        try {
            Hydrator(db.jdbi, store).runOnce()
            assertThat(registry.get("hoglake_stats_hydrated_total").tags("result", "quarantined").counter().count())
                .isEqualTo(1.0)
            assertThat(registry.find("hoglake_stats_hydrated_total").tags("result", "provided").counter())
                .describedAs("a quarantined flip is not counted as provided")
                .isNull()
        } finally {
            Metrics.clear()
        }
        assertThat(state(path)).isEqualTo(FileState("provided", quarantined = true, statsRows = 2))
        // The stats are the footer's, not the claimed value's.
        assertThat(teamBounds(path).first).isEqualTo(IcebergSingleValue.encodeLong(42))
        val sweep = lastSweep()
        assertThat(sweep["claimed"].asLong()).isEqualTo(1)
        assertThat(sweep["hydrated"].asLong()).isEqualTo(1)
        assertThat(sweep["failed"].asLong()).isZero()
        assertThat(sweep["quarantined"].asLong()).isEqualTo(1)
        assertThat(quarantinedGauge()).isEqualTo(1.0)
    }

    /** MUTATION: run the check only above the floor and this file flips clean. */
    @Test
    fun `the same mismatch under the size floor is quarantined too, with no stats rows`() {
        fixture()
        val path = register("events", "4", FORTY_TWOS)
        Hydrator(db.jdbi, store, minFileBytes = Hydrator.DEFAULT_MIN_FILE_BYTES).runOnce()
        assertThat(state(path)).isEqualTo(FileState("provided", quarantined = true, statsRows = 0))
        val sweep = lastSweep()
        assertThat(sweep["stats_skipped_below_floor"].asLong()).isEqualTo(1)
        assertThat(sweep["quarantined"].asLong()).isEqualTo(1)
    }

    /**
     * THE FAIL-CLOSED AGGREGATE. Row group 1 is all team 4, row group 2
     * all team 42: each chunk's own bounds are a single value, and only
     * the file's (4..42) gives it away.
     */
    @Test
    fun `one row group of another team's rows quarantines the file`() {
        fixture()
        val path = register("events", "4", List(5) { 4L } + List(5) { 42L }, rowGroupRows = 5)
        Hydrator(db.jdbi, store).runOnce()
        val rowGroups =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT cardinality(split_offsets) FROM hog_data_file WHERE path = :p")
                    .bind("p", path).mapTo(Int::class.java).one()
            }
        assertThat(rowGroups).describedAs("the fixture must hold two row groups").isEqualTo(2)
        assertThat(state(path).quarantined).isTrue()
        assertThat(state(path).state).isEqualTo("provided")
    }

    /**
     * Every other shape the footer cannot prove, each quarantined rather
     * than failed: nulls in the column; the column absent from the file;
     * a field id the file declares twice (FooterStats refuses the whole
     * file and returns no aggregate); and a value row that is gone (only
     * reachable by hand: the commit refuses a null or missing value).
     */
    @Test
    fun `nulls, an absent column, a duplicate field id and a missing value are quarantined`() {
        fixture()
        val nulls = register("events", "4", List(9) { 4L } + listOf(null))
        val absent = register("events", under("events", "4"), "4", parquet(FOURS, teamId = null), 10L)
        val duplicate = register("events", under("events", "4"), "4", parquet(FOURS, vId = 1), 10L)
        val noValue = register("events", "4", FOURS)
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "DELETE FROM hog_file_partition_value v USING hog_data_file f " +
                    "WHERE f.catalog_id = v.catalog_id AND f.data_file_id = v.data_file_id AND f.path = ?",
                noValue,
            )
        }
        Hydrator(db.jdbi, store).runOnce()
        for (path in listOf(nulls, absent, duplicate, noValue)) {
            assertThat(state(path).state).describedAs(path).isEqualTo("provided")
            assertThat(state(path).quarantined).describedAs(path).isTrue()
        }
        assertThat(lastSweep()["quarantined"].asLong()).isEqualTo(4)
        assertThat(quarantinedGauge()).isEqualTo(4.0)
    }

    /** MUTATION: check every table (a layout for tables without one) and plain's file is quarantined. */
    @Test
    fun `a table without the option is never checked`() {
        fixture()
        val path = register("plain", "4", FORTY_TWOS)
        Hydrator(db.jdbi, store).runOnce()
        assertThat(state(path)).isEqualTo(FileState("provided", quarantined = false, statsRows = 2))
        assertThat(lastSweep().has("quarantined")).isFalse()
        assertThat(quarantinedGauge()).isEqualTo(0.0)
    }

    /**
     * Option-off files are not checked even when an option-on file is in
     * the same sweep — beside it in the catalog, or in another catalog
     * under the same table_id (ids are per catalog). MUTATIONS: key the
     * layout map by catalog alone and plain's file is quarantined; key it
     * by table_id alone and the other catalog's `twin` is.
     */
    @Test
    fun `an option-off file swept beside an option-on one is never checked`() {
        fixture()
        val plain = register("plain", "4", FORTY_TWOS)
        val events = register("events", "4", FORTY_TWOS)
        val other = "$cat-off"
        catalogs.createCatalog(other, "s3://$BUCKET/$other")
        catalogs.createNamespace(other, "ingest")
        catalogs.createTable(
            other,
            "ingest",
            "twin",
            listOf(ColumnDef("team_id", ColType.LONG), ColumnDef("v", ColType.LONG)),
        )
        alter.alterTable(
            other,
            "ingest",
            "twin",
            listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(1, Transform.IDENTITY)))),
        )
        val twinPath = "s3://$BUCKET/$other/data/ingest/twin/team_id=4/${UUID.randomUUID()}.parquet"
        val twin = register("twin", twinPath, "4", parquet(FORTY_TWOS), 10L, catalog = other)
        val tableIds =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT path, table_id FROM hog_data_file WHERE path IN (<paths>)")
                    .bindList("paths", listOf(events, twin))
                    .map { rs, _ -> rs.getString(1) to rs.getLong(2) }.list().toMap()
            }
        assertThat(tableIds.getValue(twin))
            .describedAs("the twin must share events' table_id, or the cross-catalog half proves nothing")
            .isEqualTo(tableIds.getValue(events))
        Hydrator(db.jdbi, store).runOnce()
        assertThat(state(plain)).isEqualTo(FileState("provided", quarantined = false, statsRows = 2))
        assertThat(state(twin)).isEqualTo(FileState("provided", quarantined = false, statsRows = 2))
        assertThat(state(events)).isEqualTo(FileState("provided", quarantined = true, statsRows = 2))
        assertThat(lastSweep()["quarantined"].asLong()).isEqualTo(1)
        assertThat(quarantinedGauge()).isEqualTo(1.0)
    }

    /** `rehydrate` flips failed files back to pending, and the sweep that takes them checks them. */
    @Test
    fun `a failed file requeued by rehydrate goes through the same check`() {
        fixture()
        val bytes = parquet(FORTY_TWOS)
        val path = register("events", under("events", "4"), "4", bytes, 10L, upload = false)
        val hydrator = Hydrator(db.jdbi, store)
        hydrator.runOnce()
        assertThat(state(path).state).describedAs("the object is missing").isEqualTo("failed")
        assertThat(state(path).quarantined).isFalse()
        store.put(path, bytes)
        assertThat(hydrator.rehydrateFailed(cat).requeued).isEqualTo(1)
        hydrator.runOnce()
        assertThat(state(path)).isEqualTo(FileState("provided", quarantined = true, statsRows = 2))
    }

    /**
     * `TableRepo.markDropped` ends a table's columns but not its
     * `hog_table_layout` row. A table dropped after the claim therefore
     * still has a live layout and no live column: its file must be
     * quarantined (fail-closed: a time-travel read of the dropped table is
     * still legal), never failed and never waved through, and the other
     * table's file in the same sweep must hydrate as if nothing happened.
     * The drop lands while the first file's object is fetched.
     *
     * MUTATIONS: treat the missing column as proof and `doomed`'s file
     * reads clean; throw on it and that file is `failed`.
     */
    @Test
    fun `a table dropped mid-sweep keeps its layout row, quarantines its own file and nothing else`() {
        fixture("doomed")
        val first = register("events", under("events", "4"), "4", parquet(FOURS), 10L, tail = false)
        val second = register("doomed", under("doomed", "4"), "4", parquet(FOURS), 10L, tail = false)
        val dropped = AtomicBoolean(false)
        val racing =
            object : ObjectStore(minio.s3URL, "us-east-1", minio.userName, minio.password, true) {
                override fun get(pathUri: String): ByteArray {
                    if (pathUri == first && dropped.compareAndSet(false, true)) {
                        catalogs.dropTable(cat, "ingest", "doomed", author = "test", message = "mid-sweep")
                    }
                    return super.get(pathUri)
                }
            }
        Hydrator(db.jdbi, racing).runOnce()
        assertThat(dropped).isTrue()
        val liveLayouts =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT count(*) FROM hog_table_layout l JOIN hog_table t USING (catalog_id, table_id) " +
                        "WHERE t.dropped_snapshot IS NOT NULL AND l.end_snapshot IS NULL",
                ).mapTo(Long::class.java).one()
            }
        assertThat(liveLayouts).describedAs("the dropped table's layout row is still live").isEqualTo(1)
        assertThat(state(first)).isEqualTo(FileState("provided", quarantined = false, statsRows = 2))
        assertThat(state(second)).isEqualTo(FileState("provided", quarantined = true, statsRows = 0))
        val sweep = lastSweep()
        assertThat(sweep["hydrated"].asLong()).isEqualTo(2)
        assertThat(sweep["failed"].asLong()).isZero()
        assertThat(sweep["quarantined"].asLong()).isEqualTo(1)
        // The gauge counts live tables only, so the dropped one's file is not in it.
        assertThat(quarantinedGauge()).isEqualTo(0.0)
    }

    /**
     * THE LOWER BOUND. Row group 1 is team 3, row group 2 team 4: the
     * file's upper bound is the value and only its lower bound gives it
     * away. MUTATION: build the check's ColumnStats with upperBound as
     * its lower bound (or compare only the upper bound) and this file
     * reads clean.
     */
    @Test
    fun `a lower row group of another team's rows quarantines the file`() {
        fixture()
        val path = register("events", "4", List(5) { 3L } + List(5) { 4L }, rowGroupRows = 5)
        Hydrator(db.jdbi, store).runOnce()
        assertThat(state(path)).isEqualTo(FileState("provided", quarantined = true, statsRows = 2))
        val (lower, upper) = teamBounds(path)
        assertThat(lower).isEqualTo(IcebergSingleValue.encodeLong(3))
        assertThat(upper).isEqualTo(IcebergSingleValue.encodeLong(4))
    }

    /**
     * The value is read at the identity key_index of the FILE'S OWN spec.
     *
     *  - F1 was written under spec 1 (team_id at key 0) and is still
     *    pending when spec 2 moves team_id to key 1 behind `v`.
     *  - F2 and F3 are written under spec 2 with values ["9", "4"] and
     *    ["42", "4"]; F3 holds team 42's rows.
     *  - B is on `mixed`, spec [bucket(team_id, 8), identity(team_id)]:
     *    values ["2", "4"] and team 2's rows under team_id=4/.
     *
     * MUTATIONS: resolve from the live spec instead of the file's and F1
     * has no value at key 1; key the cache without the spec and F1 or F2
     * (whichever comes second) reads the other's key; read key 0 always
     * and F3 proves "42"; take the first field on the column whatever
     * its transform and B proves its bucket "2".
     */
    @Test
    fun `each file's value is read at its own spec's identity key_index`() {
        fixture()
        val f1 = register("events", "4", FOURS)
        alter.alterTable(
            cat,
            "ingest",
            "events",
            listOf(
                AlterOp.SetPartitionSpec(
                    listOf(PartitionFieldDef(2, Transform.IDENTITY), PartitionFieldDef(1, Transform.IDENTITY)),
                ),
            ),
        )
        val f2 = register("events", under("events", "4"), "4", parquet(FOURS), 10L, values = listOf("9", "4"))
        val f3 = register("events", under("events", "4"), "4", parquet(FORTY_TWOS), 10L, values = listOf("42", "4"))
        catalogs.createTable(
            cat,
            "ingest",
            "mixed",
            listOf(ColumnDef("team_id", ColType.LONG), ColumnDef("v", ColType.LONG)),
        )
        alter.alterTable(
            cat,
            "ingest",
            "mixed",
            listOf(
                AlterOp.SetPartitionSpec(
                    listOf(PartitionFieldDef(1, Transform.BUCKET, 8), PartitionFieldDef(1, Transform.IDENTITY)),
                ),
                AlterOp.SetObjectPrefixField("team_id"),
            ),
        )
        val b = register("mixed", under("mixed", "4"), "4", parquet(List(10) { 2L }), 10L, values = listOf("2", "4"))
        Hydrator(db.jdbi, store).runOnce()
        assertThat(mapOf("F1" to f1, "F2" to f2, "F3" to f3, "B" to b).mapValues { state(it.value).quarantined })
            .containsExactlyInAnyOrderEntriesOf(mapOf("F1" to false, "F2" to false, "F3" to true, "B" to true))
        for (path in listOf(f1, f2, f3, b)) assertThat(state(path).state).isEqualTo("provided")
        assertThat(lastSweep()["quarantined"].asLong()).isEqualTo(2)
    }

    /**
     * hoglake_stats_pending_head_file_id against a REAL sweep: the head is
     * the oldest pending file, a sweep that claims it moves the head to
     * the next, and an empty queue reads 0. MUTATION: order the head read
     * by anything but `data_file_id` and the head is not the next claim.
     */
    @Test
    fun `the pending head gauge is the next file a sweep claims, and advances when it does`() {
        fixture()
        val first = register("events", "4", FOURS)
        val second = register("events", "4", FOURS)
        val ids =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT path, data_file_id FROM hog_data_file WHERE path IN (<paths>)")
                    .bindList("paths", listOf(first, second))
                    .map { rs, _ -> rs.getString(1) to rs.getLong(2).toDouble() }.list().toMap()
            }

        fun head(): Double? {
            val registry = SimpleMeterRegistry()
            CatalogMetrics(db.jdbi, registry).sampleOnce()
            return registry.find("hoglake_stats_pending_head_file_id").tag("catalog", cat).gauge()?.value()
        }
        assertThat(head()).isEqualTo(ids.getValue(first))
        Hydrator(db.jdbi, store).runOnce(limit = 1)
        assertThat(state(first).state).isEqualTo("provided")
        assertThat(head()).isEqualTo(ids.getValue(second))
        Hydrator(db.jdbi, store).runOnce()
        assertThat(head()).isEqualTo(0.0)
    }

    /**
     * A uint16 prefix field promoted to uint32 while a file written under
     * uint16 (INT32 + INT(16, unsigned)) is pending: the check reads the
     * footer at the LIVE type, and the INT32 leaf must zero-extend into
     * uint32's 8-byte bound rather than lose its bounds and quarantine a
     * file whose every row holds the value. MUTATION: accept only
     * INT(32, unsigned) in FooterStats' uint32 arm and this file is
     * quarantined (NULL_BOUNDS).
     */
    @Test
    fun `a uint16 prefix field promoted to uint32 still proves its pending files`() {
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ingest")
        catalogs.createTable(
            cat,
            "ingest",
            "u16",
            listOf(ColumnDef("team_id", ColType.UINT16), ColumnDef("v", ColType.LONG)),
        )
        alter.alterTable(
            cat,
            "ingest",
            "u16",
            listOf(
                AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(1, Transform.IDENTITY))),
                AlterOp.SetObjectPrefixField("team_id"),
            ),
        )
        val path = register("u16", under("u16", "4"), "4", parquet(FOURS, teamUint16 = true), 10L)
        alter.alterTable(cat, "ingest", "u16", listOf(AlterOp.PromoteColumn("team_id", ColType.UINT32)))
        Hydrator(db.jdbi, store).runOnce()
        assertThat(state(path)).isEqualTo(FileState("provided", quarantined = false, statsRows = 2))
        val (lower, upper) = teamBounds(path)
        assertThat(lower).isEqualTo(IcebergSingleValue.encodeLong(4))
        assertThat(upper).isEqualTo(IcebergSingleValue.encodeLong(4))
    }
}
