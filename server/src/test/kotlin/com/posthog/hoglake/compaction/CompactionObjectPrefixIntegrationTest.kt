package com.posthog.hoglake.compaction

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.hydrator.Hydrator
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.MaintenanceSummarySampler
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.testing.TestImages
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

/**
 * Compaction of a table with the object-prefix layout (2.1d), on real
 * parquet in MinIO committed through the real commit path: the output
 * lands under its group's prefix, only verified inputs are taken, an
 * output that does not prove its value is never registered, the live
 * layout is rechecked under the commit lock, a group with no nameable
 * prefix is refused before any IO, and a DV on an output is held to the
 * output's prefix. The same table without the option compacts as before.
 *
 * `ns.t` has `team_id` (field 1) and `v` (field 2) and a TWO-key spec,
 * `[bucket(v, 4), identity(team_id)]`, so the prefix field is key_index 1
 * and an output path built from the wrong key names `team_id=1/` (every
 * file here is in bucket "1").
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CompactionObjectPrefixIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val alter = AlterService(db.jdbi)
    private val counter = AtomicInteger(0)

    private val cfg = CompactionConfig(targetBytes = 1L shl 20, minInputFiles = 2, maxGroupsPerRun = 10)
    private val svc by lazy { CompactionService(db.jdbi, store, cfg) }

    private companion object {
        const val BUCKET = "hoglake-prefix-compaction"
        const val UUID_PARQUET = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\.parquet"

        val minio by lazy { TestImages.minio().also { it.start() } }

        val store: ObjectStore by lazy {
            ObjectStore(minio.s3URL, "us-east-1", minio.userName, minio.password, true).also {
                it.createBucket(BUCKET)
            }
        }
    }

    @AfterAll
    fun tearDown() = db.close()

    private fun le8(v: Long): ByteArray = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(v).array()

    /** A catalog with `ns.t` and its two-key spec; the object-prefix option on team_id when [option]. */
    private fun fixture(option: Boolean): String {
        val cat = "prefix-compact-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        createTable(cat, option)
        return cat
    }

    private fun createTable(
        cat: String,
        option: Boolean,
    ) {
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("team_id", ColType.LONG), ColumnDef("v", ColType.LONG)))
        val spec =
            AlterOp.SetPartitionSpec(
                listOf(PartitionFieldDef(2, Transform.BUCKET, 4), PartitionFieldDef(1, Transform.IDENTITY)),
            )
        val ops = listOf(spec) + if (option) listOf(AlterOp.SetObjectPrefixField("team_id")) else emptyList()
        alter.alterTable(cat, "ns", "t", ops)
    }

    private fun prefix(
        cat: String,
        team: Long,
    ) = "s3://$BUCKET/$cat/data/ns/t/team_id=$team/"

    /** Parquet of `team_id` (optional int64, id 1) and `v` (required int64, id 2), one row per entry of [teams]. */
    private fun parquet(teams: List<Long>): ByteArray {
        val schema =
            MessageType(
                "t",
                Types.optional(INT64).id(1).named("team_id"),
                Types.required(INT64).id(2).named("v"),
            )
        val tmp = Files.createTempFile("prefix-compact", ".parquet")
        try {
            Files.delete(tmp)
            val factory = SimpleGroupFactory(schema)
            ExampleParquetWriter.builder(LocalOutputFile(tmp))
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
                .use { w ->
                    teams.forEachIndexed { i, team ->
                        w.write(factory.newGroup().append("team_id", team).append("v", i.toLong()))
                    }
                }
            return Files.readAllBytes(tmp)
        } finally {
            Files.deleteIfExists(tmp)
        }
    }

    /**
     * Uploads a file under `team_id=[team]/` whose rows hold [rows] and
     * commits it with partition values `["1", "[team]"]`: inline stats
     * that prove [team] (whatever the rows hold; the commit cannot open
     * the file) or, with [deferred], none. Returns its data_file_id.
     */
    private fun register(
        cat: String,
        team: Long,
        rows: List<Long> = List(5) { team },
        deferred: Boolean = false,
    ): Long {
        val path = prefix(cat, team) + "${UUID.randomUUID()}.parquet"
        val bytes = parquet(rows)
        store.put(path, bytes)
        val stats = listOf(ColumnStats(1, rows.size.toLong(), 0, null, null, le8(team), le8(team)))
        val file =
            FileRegistration(
                path,
                rows.size.toLong(),
                bytes.size.toLong(),
                footerSize = ByteBuffer.wrap(bytes, bytes.size - 8, 4).order(ByteOrder.LITTLE_ENDIAN).int.toLong(),
                columnStats = if (deferred) null else stats,
                partitionValues = listOf("1", team.toString()),
            )
        commits.commit(
            cat,
            CommitRequest(readSnapshot = db.head(cat), appends = listOf(TableAppend("ns", "t", listOf(file)))),
        )
        return db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT f.data_file_id FROM hog_data_file f JOIN hog_catalog c USING (catalog_id) " +
                    "WHERE c.name = :c AND f.path = :p",
            ).bind("c", cat).bind("p", path).mapTo(Long::class.java).one()
        }
    }

    private class LiveFile(
        val id: Long,
        val path: String,
        val records: Long,
        val statsState: String,
        val quarantined: Boolean,
        val explicitRowIds: Boolean,
        val values: List<String?>,
        val teamLower: ByteArray?,
        val teamUpper: ByteArray?,
        val teamNulls: Long?,
    )

    private fun liveFiles(cat: String): List<LiveFile> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT f.data_file_id, f.path, f.record_count, f.stats_state, f.prefix_quarantined,
                       f.explicit_row_ids,
                       (SELECT array_agg(pv.value ORDER BY pv.key_index) FROM hog_file_partition_value pv
                         WHERE pv.catalog_id = f.catalog_id AND pv.data_file_id = f.data_file_id) AS vals,
                       s.lower_bound, s.upper_bound, s.null_count
                  FROM hog_data_file f
                  JOIN hog_catalog c USING (catalog_id)
                  LEFT JOIN hog_file_column_stats s
                    ON s.catalog_id = f.catalog_id AND s.data_file_id = f.data_file_id AND s.field_id = 1
                 WHERE c.name = :c AND f.end_snapshot IS NULL
                 ORDER BY f.data_file_id
                """,
            ).bind("c", cat)
                .map { rs, _ ->
                    LiveFile(
                        rs.getLong("data_file_id"),
                        rs.getString("path"),
                        rs.getLong("record_count"),
                        rs.getString("stats_state"),
                        rs.getBoolean("prefix_quarantined"),
                        rs.getBoolean("explicit_row_ids"),
                        (rs.getArray("vals").array as Array<*>).map { it as String? },
                        rs.getBytes("lower_bound"),
                        rs.getBytes("upper_bound"),
                        rs.getObject("null_count")?.let { (it as Number).toLong() },
                    )
                }.list()
        }

    /** The catalog's compaction staging tickets: path and drained outcome (null = undrained). */
    private fun stagingRows(cat: String): List<Pair<String, String?>> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT r.path, r.drained_outcome FROM hog_file_removal r JOIN hog_catalog c USING (catalog_id) " +
                    "WHERE c.name = :c AND r.reason = 'compaction_staging' ORDER BY r.removal_id",
            ).bind("c", cat).map { rs, _ -> rs.getString("path") to rs.getString("drained_outcome") }.list()
        }

    private fun <T> withRegistry(body: (PrometheusMeterRegistry) -> T): T {
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        Metrics.bind(registry)
        try {
            return body(registry)
        } finally {
            Metrics.clear()
        }
    }

    @Test
    fun `the output lands under its group's prefix, and without the option at the table root as before`() {
        val on = fixture(option = true)
        val inputs = (1..3).map { register(on, 4) }
        withRegistry { registry ->
            val result = svc.runOnce(on, cfg, MaintenanceTrigger.LOOP)
            assertThat(result.groupsCompacted).describedAs("%s", result).isEqualTo(1)
            assertThat(registry.scrape())
                .describedAs("the 2.1b/2.1c skip is gone")
                .doesNotContain("object_prefix_layout")
        }
        val output = liveFiles(on).single()
        assertThat(output.id).isNotIn(inputs)
        // MUTATION: build the path from key_index 0 instead of the spec's
        // identity key on team_id, and the output is written under
        // team_id=1/, its footer proves 4, and the group is invalid_data.
        assertThat(output.path).matches(Regex.escape(prefix(on, 4)) + UUID_PARQUET)
        assertThat(store.get(output.path)).describedAs("the object is where the row says").isNotEmpty()
        assertThat(output.values).isEqualTo(listOf("1", "4"))
        assertThat(output.records).isEqualTo(15)
        assertThat(output.explicitRowIds).isTrue()
        assertThat(output.statsState).describedAs("proven at commit, never pending").isEqualTo("provided")
        assertThat(output.quarantined).isFalse()
        assertThat(output.teamLower).isEqualTo(le8(4))
        assertThat(output.teamUpper).isEqualTo(le8(4))
        assertThat(output.teamNulls).isZero()

        val off = fixture(option = false)
        repeat(3) { register(off, 4) }
        assertThat(svc.runOnce(off, cfg, MaintenanceTrigger.LOOP).groupsCompacted).isEqualTo(1)
        assertThat(liveFiles(off).single().path)
            .describedAs("today's shape: a bare uuid at the table root, whatever the inputs' directories")
            .matches(Regex.escape("s3://$BUCKET/$off/data/ns/t/") + UUID_PARQUET)
    }

    @Test
    fun `a group with no nameable prefix is refused before any IO, uncharged, and counted`() {
        val cat = fixture(option = true)
        val good = (1..3).map { register(cat, 4) }
        // Three buckets of files the commit accepted, each then broken
        // behind the API's back — the one way such rows can exist, since
        // the commit refuses all three shapes: team 5's value made
        // non-canonical ("05"), team 6's made NULL, and team 7's files
        // left with no spec. Four files each, so every bad group sorts
        // FIRST and would take the run's only slot if the refusal were
        // charged.
        val nonCanonical = (1..4).map { register(cat, 5) }
        val nullValue = (1..4).map { register(cat, 6) }
        val noSpec = (1..4).map { register(cat, 7) }
        db.jdbi.useHandleUnchecked { h ->
            val scope = "AND catalog_id = (SELECT catalog_id FROM hog_catalog WHERE name = :c)"
            h.createUpdate(
                "UPDATE hog_file_partition_value SET value = '05' WHERE key_index = 1 " +
                    "AND data_file_id IN (<ids>) $scope",
            )
                .bindList("ids", nonCanonical).bind("c", cat).execute()
            h.createUpdate(
                "UPDATE hog_file_partition_value SET value = NULL WHERE key_index = 1 " +
                    "AND data_file_id IN (<ids>) $scope",
            )
                .bindList("ids", nullValue).bind("c", cat).execute()
            h.createUpdate("UPDATE hog_data_file SET spec_id = NULL WHERE data_file_id IN (<ids>) $scope")
                .bindList("ids", noSpec).bind("c", cat).execute()
        }
        val bad = nonCanonical + nullValue + noSpec
        val plan = svc.planTable(cat, "ns", "t")
        assertThat(plan.objectPrefixUnresolvableGroups).isEqualTo(3)
        assertThat(plan.groups.single().files.map { it.dataFileId }).containsExactlyInAnyOrderElementsOf(good)

        // MUTATION: drop the refusal from `pack`, and a bad group takes
        // the run's one slot and fails (no path can be built for it), and
        // the good group never runs.
        withRegistry { registry ->
            val result = svc.runOnce(cat, cfg.copy(maxGroupsPerRun = 1), MaintenanceTrigger.LOOP)
            assertThat(result.objectPrefixUnresolvable).isEqualTo(3)
            assertThat(result.failedGroups).isZero()
            assertThat(result.groupsCompacted).describedAs("the refusal is not charged").isEqualTo(1)
            assertThat(registry.scrape())
                .contains(
                    "hoglake_compaction_skipped_total{catalog=\"$cat\",reason=\"object_prefix_unresolvable\"} 3.0",
                )
        }
        assertThat(liveFiles(cat).map { it.id }).containsAll(bad)
        assertThat(stagingRows(cat))
            .describedAs("before any IO: the refused group never staged an output path")
            .singleElement()
            .satisfies({ assertThat(it.first).startsWith(prefix(cat, 4)) })
    }

    @Test
    fun `a layout whose column is not live, or not of an allowed type, refuses every group`() {
        // Both shapes are a direct write: the DDL refuses dropping the
        // field and promoting it out of the allowed types. Either way the
        // option is still ON, and the table must not compact as if it were
        // off (its output would land at the table root).
        for (breakage in listOf(
            "end_snapshot = (SELECT last_snapshot_id FROM hog_catalog WHERE name = :c)",
            "col_type = 'uint64'",
        )) {
            val cat = fixture(option = true)
            repeat(3) { register(cat, 4) }
            repeat(2) { register(cat, 5) }
            db.jdbi.useHandleUnchecked { h ->
                h.createUpdate(
                    "UPDATE hog_column SET $breakage WHERE field_id = 1 AND end_snapshot IS NULL " +
                        "AND catalog_id = (SELECT catalog_id FROM hog_catalog WHERE name = :c)",
                ).bind("c", cat).execute()
            }
            // MUTATION: decide "option on" from the resolved layout rather
            // than the field id, and both groups are planned.
            val plan = svc.planTable(cat, "ns", "t")
            assertThat(plan.groups).describedAs(breakage).isEmpty()
            assertThat(plan.objectPrefixUnresolvableGroups).describedAs(breakage).isEqualTo(2)
            val result = svc.runOnce(cat, cfg, MaintenanceTrigger.LOOP)
            assertThat(result.objectPrefixUnresolvable).isEqualTo(2)
            assertThat(result.groupsCompacted).isZero()
            assertThat(stagingRows(cat)).describedAs("nothing staged, nothing read").isEmpty()
        }
    }

    @Test
    fun `pending and quarantined inputs are never compacted with the option, and pending ones are without it`() {
        val on = fixture(option = true)
        val good = (1..3).map { register(on, 4) }
        // Deferred stats with team 42 in the footer under team_id=4/: the
        // hydrator flips it to provided AND quarantined.
        val quarantined = register(on, 4, rows = List(5) { 42L }, deferred = true)
        Hydrator(db.jdbi, store).runOnce()
        assertThat(liveFiles(on).single { it.id == quarantined }.quarantined).isTrue()
        // Registered after the sweep, so still pending.
        val pending = (1..2).map { register(on, 4, deferred = true) }

        // MUTATION: drop VERIFIED_ONLY_SQL from the candidate read, or send
        // it with verifiedOnly = false from either fetch, and all six files
        // are one group. Planned twice: the no-sample fallback, then the
        // bucket probe once the sampler has published a generation.
        assertThat(svc.planTable(on, "ns", "t").groups.single().files.map { it.dataFileId })
            .containsExactlyInAnyOrderElementsOf(good)
        val sampler = MaintenanceSummarySampler(db.jdbi, cfg.targetBytes, cfg.minInputFiles, cfg.maxInputFiles)
        var steps = 0
        while (sampler.runOnce(10_000)) check(++steps < 100)
        val sampled = svc.planTable(on, "ns", "t")
        assertThat(sampled.bucketsConsidered).describedAs("the bucket probe ran").isEqualTo(1)
        assertThat(sampled.groups.single().files.map { it.dataFileId }).containsExactlyInAnyOrderElementsOf(good)
        assertThat(svc.runOnce(on, cfg, MaintenanceTrigger.LOOP).groupsCompacted).isEqualTo(1)
        assertThat(liveFiles(on).map { it.id })
            .contains(quarantined)
            .containsAll(pending)
            .doesNotContainAnyElementsOf(good)

        val off = fixture(option = false)
        val offFiles = (1..3).map { register(off, 4) } + (1..2).map { register(off, 4, deferred = true) }
        assertThat(svc.planTable(off, "ns", "t").groups.single().files.map { it.dataFileId })
            .describedAs("without the option a pending file is a candidate, as before")
            .containsExactlyInAnyOrderElementsOf(offFiles)
    }

    @Test
    fun `an output that does not prove its value is invalid_data naming the inputs, and is not registered`() {
        val cat = fixture(option = true)
        repeat(2) { register(cat, 4) }
        // A lying writer: inline stats prove 4, the rows hold 42. The
        // commit cannot open the file; the output's footer is the proof.
        val liar = register(cat, 4, rows = List(5) { 42L })
        val liarPath = liveFiles(cat).single { it.id == liar }.path
        val group = svc.planTable(cat, "ns", "t").groups.single()

        // MUTATION: drop the output proof, and both of these commit the
        // group with 42s under team_id=4/.
        assertThatThrownBy { svc.compactPlannedGroup(cat, "ns", "t", group) }
            .isInstanceOf(InvalidDataException::class.java)
            .hasMessageContaining("BOUND_MISMATCH")
            .hasMessageContaining("$liar $liarPath")
        val result = svc.runOnce(cat, cfg, MaintenanceTrigger.LOOP)
        assertThat(result.invalidData).isEqualTo(1)
        assertThat(result.groupsCompacted).isZero()
        assertThat(liveFiles(cat)).describedAs("the inputs stay live, nothing registered").hasSize(3)
        assertThat(stagingRows(cat))
            .describedAs("both staged outputs stay queued for the cleanup drain")
            .hasSize(2)
            .allSatisfy { assertThat(it.second).isNull() }
    }

    @Test
    fun `the live layout is rechecked under the commit lock`() {
        val cat = fixture(option = true)
        repeat(3) { register(cat, 4) }
        val group = svc.planTable(cat, "ns", "t").groups.single()
        // The field renamed behind the DDL's back between the rewrite and
        // the commit (the DDL refuses it; a later relayout route would
        // not): the output is under team_id=4/ and the live prefix is
        // tenant=4/.
        val renaming = CompactionService(db.jdbi, store, cfg)
        renaming.beforeRewrite = {
            db.jdbi.useHandleUnchecked { h ->
                h.createUpdate(
                    "UPDATE hog_column SET name = 'tenant' WHERE field_id = 1 AND end_snapshot IS NULL " +
                        "AND catalog_id = (SELECT catalog_id FROM hog_catalog WHERE name = :c)",
                ).bind("c", cat).execute()
            }
        }
        // MUTATION: drop the recheck from `commitGroup`, and this commits.
        assertThat(renaming.compactPlannedGroup(cat, "ns", "t", group))
            .isEqualTo(CompactionService.GroupOutcome.SkippedConflict)
        assertThat(liveFiles(cat)).describedAs("nothing registered; the inputs stay live").hasSize(3)

        // Dropped and recreated with the same name, spec and option between
        // plan and commit: the drop check answers first, and the new
        // incarnation gains nothing.
        val other = fixture(option = true)
        repeat(3) { register(other, 4) }
        val otherGroup = svc.planTable(other, "ns", "t").groups.single()
        val recreating = CompactionService(db.jdbi, store, cfg)
        recreating.beforeRewrite = {
            catalogs.dropTable(other, "ns", "t", author = "integration-test", message = "recreate")
            createTable(other, option = true)
        }
        assertThat(recreating.compactPlannedGroup(other, "ns", "t", otherGroup))
            .isEqualTo(CompactionService.GroupOutcome.SkippedConflict)
        assertThat(liveFiles(other).filter { it.explicitRowIds }).isEmpty()

        // And the probe runs for a table WITHOUT the option: a layout that
        // appears between plan and commit (the DDL refuses it on a table
        // with live files) leaves a root-path output outside every prefix.
        val off = fixture(option = false)
        repeat(3) { register(off, 4) }
        val offGroup = svc.planTable(off, "ns", "t").groups.single()
        val layering = CompactionService(db.jdbi, store, cfg)
        layering.beforeRewrite = {
            db.jdbi.useHandleUnchecked { h ->
                h.createUpdate(
                    "INSERT INTO hog_table_layout (catalog_id, table_id, begin_snapshot, object_prefix_field_id) " +
                        "SELECT catalog_id, :t, last_snapshot_id, 1 FROM hog_catalog WHERE name = :c",
                ).bind("c", off).bind("t", catalogs.getTable(off, "ns", "t").tableId).execute()
            }
        }
        // MUTATION: run the recheck only when the plan had the option.
        assertThat(layering.compactPlannedGroup(off, "ns", "t", offGroup))
            .isEqualTo(CompactionService.GroupOutcome.SkippedConflict)
    }

    @Test
    fun `a group with no survivors retires without an output to prove`() {
        val cat = fixture(option = true)
        val ids = (1..2).map { register(cat, 4) }
        for (id in ids) {
            val bytes = PuffinTestFiles.deletionVector((0L until 5L).toList())
            val path = prefix(cat, 4) + "dv-${UUID.randomUUID()}.puffin"
            store.put(path, bytes)
            commits.commit(
                cat,
                CommitRequest(
                    readSnapshot = db.head(cat),
                    deletes =
                        listOf(
                            TableDeletes("ns", "t", listOf(DeleteFileRegistration(id, path, 5, bytes.size.toLong()))),
                        ),
                ),
            )
        }
        // MUTATION: prove the output whatever its row count, and the empty
        // output's null bounds make this invalid_data instead.
        val result = svc.runOnce(cat, cfg, MaintenanceTrigger.LOOP)
        assertThat(result.groupsRetired).describedAs("%s", result).isEqualTo(1)
        assertThat(liveFiles(cat)).isEmpty()
    }

    @Test
    fun `each group takes the key_index of its own spec, retired or live`() {
        val cat = fixture(option = true)
        val old = (1..2).map { register(cat, 4) }
        // The prefix field moves from key 1 to key 0; the old spec is
        // retired, and its files still name it.
        val keyZero = AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(1, Transform.IDENTITY)))
        alter.alterTable(cat, "ns", "t", listOf(keyZero))
        val path = { prefix(cat, 4) + "${UUID.randomUUID()}.parquet" }
        val fresh =
            (1..2).map {
                val p = path()
                val bytes = parquet(List(5) { 4L })
                store.put(p, bytes)
                val stats = listOf(ColumnStats(1, 5, 0, null, null, le8(4), le8(4)))
                val file = FileRegistration(p, 5, bytes.size.toLong(), null, stats, listOf("4"))
                commits.commit(
                    cat,
                    CommitRequest(
                        readSnapshot = db.head(cat),
                        appends = listOf(TableAppend("ns", "t", listOf(file))),
                    ),
                )
                p
            }
        // MUTATION: take the LIVE spec's key_index for every group, and the
        // old group's value is its bucket, "1".
        val result = svc.runOnce(cat, cfg, MaintenanceTrigger.LOOP)
        assertThat(result.groupsCompacted).describedAs("%s", result).isEqualTo(2)
        val outputs = liveFiles(cat)
        assertThat(outputs.map { it.values }).containsExactlyInAnyOrder(listOf("1", "4"), listOf("4"))
        assertThat(outputs).allSatisfy { assertThat(it.path).startsWith(prefix(cat, 4)) }
        assertThat(outputs.map { it.id }).doesNotContainAnyElementsOf(old)
        assertThat(outputs.map { it.path }).doesNotContainAnyElementsOf(fresh)
    }

    @Test
    fun `a deletion vector on a compaction output must sit under the output's prefix`() {
        val cat = fixture(option = true)
        repeat(3) { register(cat, 4) }
        assertThat(svc.runOnce(cat, cfg, MaintenanceTrigger.LOOP).groupsCompacted).isEqualTo(1)
        val output = liveFiles(cat).single()

        fun dv(path: String) =
            commits.commit(
                cat,
                CommitRequest(
                    readSnapshot = db.head(cat),
                    deletes = listOf(TableDeletes("ns", "t", listOf(DeleteFileRegistration(output.id, path, 1, 100)))),
                ),
            )
        for (outside in listOf("s3://$BUCKET/$cat/data/ns/t/hoglake-dv-x.puffin", prefix(cat, 5) + "dv.puffin")) {
            assertThatThrownBy { dv(outside) }
                .isInstanceOf(HoglakeException.ObjectPrefixViolation::class.java)
                .hasMessageContaining(prefix(cat, 4))
        }
        dv(prefix(cat, 4) + "dv-${UUID.randomUUID()}.puffin")
        assertThat(liveFiles(cat).single().id).isEqualTo(output.id)
    }
}
