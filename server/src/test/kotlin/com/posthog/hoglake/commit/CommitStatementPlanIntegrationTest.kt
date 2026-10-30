package com.posthog.hoglake.commit

import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.testing.ExplainPlan
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

/**
 * The plans of the commit tail's two statements ([CommitTail.READ_SQL],
 * [CommitTail.WRITE_SQL]) on a production-shaped catalog, CUSTOM and
 * GENERIC.
 *
 * Generic because that is what production runs: pgjdbc prepares a
 * statement server-side on its fifth execution on a connection, and the
 * server then plans it once without the parameter values if that plan
 * costs no more than the custom ones did. A statement this size has
 * estimates a generic plan can get wrong (an `unnest` of a parameter is
 * 100 rows to the planner whatever it holds), so a plan proven only with
 * the values in hand proves half of it. The generic plan is taken by
 * PREPAREing the production text (named parameters rewritten to `$n`)
 * under `plan_cache_mode = force_generic_plan` and EXPLAINing its
 * EXECUTE with the production binder.
 *
 * The fixture is the shape of gigahog-prod-us's events catalog at a
 * fraction of its size: one hot partitioned table taking half the
 * commits and most of the files among [TABLES] tables of [COLUMNS]
 * columns, [SNAPSHOTS] retained snapshots each with its change row and
 * a DDL every [DDL_EVERY], [FILES] file rows with scattered paths, a
 * long-lived upload ledger, a removal queue mostly drained, a week of
 * receipts. What each statement may cost is stated beside its
 * assertion, from the probes it makes.
 *
 * The object-prefix layout (V29): every even table has a live layout row,
 * so `hog_table_layout_live` holds [TABLES] / 2 rows and the hot table
 * (odd) is probed and found WITHOUT the option, the shape almost every
 * commit has. [PREFIX] is the option-on table, with [PREFIX_FILES] files
 * of its own and the partition values every partitioned file carries, so
 * the DV-target probe of `hog_file_partition_value` runs against
 * production's shape too. [HISTORY] has the option with its prefix
 * column [VERSIONS] versions deep, among [WIDE] more live columns.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CommitStatementPlanIntegrationTest {
    private companion object {
        const val TABLES = 2_000
        const val COLUMNS = 25
        const val SNAPSHOTS = 100_000
        const val DDL_EVERY = 5_000
        const val FILES = 200_000
        const val UPLOADS = 100_000
        const val QUEUE = 100_000
        const val RECEIPTS = 100_000
        const val DVS = 5_000

        /** Registrations per commit (#240's average) and the operation's active claims. */
        const val PATHS = 270

        /** A prepared append's basis is a few seconds old. */
        const val READ_BEHIND = 50

        const val HOT = 1L

        /** The table with the object-prefix layout, its spec identity on field 1. */
        const val PREFIX = 2L

        /** [PREFIX]'s own files, after the [FILES] and [DVS] ids; one per (team, i). */
        const val PREFIX_FILES = 1_000

        /** Distinct prefix values among them. */
        const val TEAMS = 50

        /**
         * An option-on table (even, so it has a layout on field 1) whose
         * prefix column has [VERSIONS] ended versions behind its live one,
         * what renames from before the option, promotes and comments leave
         * until expiry, and [WIDE] more
         * live columns than the rest.
         */
        const val HISTORY = 4L
        const val VERSIONS = 500
        const val WIDE = 200

        /** Every big table a commit statement touches; none may be scanned whole. */
        val BIG_TABLES =
            listOf(
                "hog_snapshot_change", "hog_data_file", "hog_delete_file", "hog_upload", "hog_file_removal",
                "hog_column", "hog_table_version", "hog_table_stats", "hog_table", "hog_commit_receipt",
                "hog_file_partition_value", "hog_table_layout",
            )

        /**
         * The big tables a prepared append's read probes that are large in
         * THIS fixture (the per-table ones hold 2,000 rows here against
         * production's tens of thousands): reading any of them whole costs
         * at least its pages.
         */
        val PROBED =
            listOf(
                "hog_snapshot_change",
                "hog_data_file",
                "hog_upload",
                "hog_file_removal",
                "hog_column",
                "hog_commit_receipt",
            )
    }

    private val db = PgTestSupport.freshDatabase()
    private var catalogId = 0L
    private val owner = UUID.randomUUID()
    private lateinit var activePaths: List<String>
    private lateinit var activeIds: List<UUID>
    private lateinit var dvTargets: List<Long>
    private lateinit var prefixTargets: List<Long>

    @AfterAll
    fun tearDown() = db.close()

    @BeforeAll
    fun seed() {
        db.jdbi.useHandleUnchecked { h ->
            catalogId =
                h.createQuery(
                    "INSERT INTO hog_catalog (name, data_path) VALUES ('plan', 's3://bucket/plan') " +
                        "RETURNING catalog_id",
                ).mapTo(Long::class.java).one()
            h.execute("INSERT INTO hog_namespace (catalog_id, namespace_id, name) VALUES (?, 1, 'ingest')", catalogId)
            h.createUpdate(
                """
                INSERT INTO hog_table (catalog_id, table_id, created_snapshot, next_field_id)
                SELECT :c, g, 0, :cols + 1 FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("n", TABLES).bind("cols", COLUMNS).execute()
            h.createUpdate(
                """
                INSERT INTO hog_table_version (catalog_id, table_id, begin_snapshot, namespace_id, name)
                SELECT :c, g, 0, 1, 't' || g FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("n", TABLES).execute()
            h.createUpdate(
                """
                INSERT INTO hog_table_stats (catalog_id, table_id, record_count, file_size_bytes, next_row_id)
                SELECT :c, g, 1000000, 1000000, 1000000 FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("n", TABLES).execute()
            h.createUpdate(
                """
                INSERT INTO hog_column (catalog_id, table_id, field_id, begin_snapshot, name, col_type, ordinal)
                SELECT :c, t, f, 0, 'c' || f, 'long', f - 1
                FROM generate_series(1, :n) t, generate_series(1, :cols) f
                """,
            ).bind("c", catalogId).bind("n", TABLES).bind("cols", COLUMNS).execute()
            h.execute(
                "INSERT INTO hog_partition_spec (catalog_id, table_id, spec_id, begin_snapshot) VALUES (?, ?, 1, 0)",
                catalogId,
                HOT,
            )
            h.execute(
                """
                INSERT INTO hog_partition_field (catalog_id, table_id, spec_id, key_index, source_field_id, transform)
                VALUES (?, ?, 1, 0, 1, 'identity')
                """,
                catalogId,
                HOT,
            )
            h.createUpdate(
                "INSERT INTO hog_snapshot (catalog_id, snapshot_id, schema_version) " +
                    "SELECT :c, g, 0 FROM generate_series(1, :n) g",
            ).bind("c", catalogId).bind("n", SNAPSHOTS).execute()
            // Half the commits append to the hot table, the rest spread
            // over the others; a DDL on the hot table every DDL_EVERY
            // snapshots and a compaction every 150.
            h.createUpdate(
                """
                INSERT INTO hog_snapshot_change (catalog_id, snapshot_id, kind, object_id)
                SELECT :c, g, 'table_inserted_into', CASE WHEN g % 2 = 0 THEN 1 ELSE 2 + g % (:n - 1) END
                FROM generate_series(1, :s) g
                UNION ALL
                SELECT :c, g, 'table_altered', 1 FROM generate_series(:ddl, :s, :ddl) g
                UNION ALL
                SELECT :c, g, 'table_compacted', 1 FROM generate_series(150, :s, 150) g
                """,
            ).bind("c", catalogId).bind("n", TABLES).bind("s", SNAPSHOTS).bind("ddl", DDL_EVERY).execute()
            // Mostly the hot table, mostly live, paths scattered as a
            // writer's UUIDs are.
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot, end_snapshot,
                                           path, record_count, file_size_bytes, row_id_start, spec_id)
                SELECT :c, g, CASE WHEN g % 10 = 0 THEN 2 + g % (:n - 1) ELSE 1 END,
                       1 + g % (:s - 1), CASE WHEN g % 100 = 0 THEN :s END,
                       's3://bucket/plan/data/' || md5(g::text) || '.parquet', 10, 1000, g * 10,
                       CASE WHEN g % 10 = 0 THEN NULL ELSE 1 END
                FROM generate_series(1, :f) g
                """,
            ).bind("c", catalogId).bind("n", TABLES).bind("s", SNAPSHOTS).bind("f", FILES).execute()
            // One live DV on each of DVS hot-table files.
            h.createUpdate(
                """
                INSERT INTO hog_delete_file (catalog_id, delete_file_id, table_id, data_file_id, begin_snapshot,
                                             path, delete_count, file_size_bytes)
                SELECT :c, :f + g, 1, g * 40 - 39, 1, 's3://bucket/plan/dv/' || md5('dv' || g) || '.puffin', 1, 16
                FROM generate_series(1, :d) g
                """,
            ).bind("c", catalogId).bind("f", FILES).bind("d", DVS).execute()
            // The object-prefix table: its spec, a live layout on every even
            // table, its own files under their team prefixes, and the
            // partition value of every partitioned file in the fixture.
            h.execute(
                "INSERT INTO hog_partition_spec (catalog_id, table_id, spec_id, begin_snapshot) VALUES (?, ?, 1, 0)",
                catalogId,
                PREFIX,
            )
            h.execute(
                """
                INSERT INTO hog_partition_field (catalog_id, table_id, spec_id, key_index, source_field_id, transform)
                VALUES (?, ?, 1, 0, 1, 'identity')
                """,
                catalogId,
                PREFIX,
            )
            h.createUpdate(
                """
                INSERT INTO hog_table_layout (catalog_id, table_id, begin_snapshot, object_prefix_field_id)
                SELECT :c, g, 1, 1 FROM generate_series(2, :n, 2) g
                """,
            ).bind("c", catalogId).bind("n", TABLES).execute()
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           path, record_count, file_size_bytes, row_id_start, spec_id)
                SELECT :c, :first + g, :t, 1 + g % (:s - 1),
                       's3://bucket/plan/data/ingest/t' || :t || '/team_id=' || g % :teams || '/' || md5(g::text)
                           || '.parquet',
                       10, 1000, g * 10, 1
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("first", FILES + DVS).bind("t", PREFIX).bind("s", SNAPSHOTS)
                .bind("teams", TEAMS).bind("n", PREFIX_FILES).execute()
            h.createUpdate(
                """
                INSERT INTO hog_file_partition_value (catalog_id, data_file_id, key_index, value)
                SELECT f.catalog_id, f.data_file_id, 0, (f.data_file_id % :teams)::text
                FROM hog_data_file f WHERE f.catalog_id = :c AND f.spec_id IS NOT NULL
                """,
            ).bind("c", catalogId).bind("teams", TEAMS).execute()
            prefixTargets = listOf(7L, 77L, 777L).map { FILES + DVS + it }
            h.execute(
                "UPDATE hog_catalog SET last_snapshot_id = ?, next_file_id = ?, next_table_id = ?, " +
                    "next_namespace_id = 2 " +
                    "WHERE catalog_id = ?",
                SNAPSHOTS.toLong(),
                (FILES + DVS + PREFIX_FILES + 1).toLong(),
                (TABLES + 1).toLong(),
                catalogId,
            )
            // A long-lived ledger: finished operations' registered claims,
            // 100 per owner, then the active claims of the one under test.
            h.createUpdate(
                """
                INSERT INTO hog_upload (catalog_id, upload_id, owner, prefix, path, file_kind, state)
                SELECT :c, md5('u' || g)::uuid, md5('o' || (g / 100))::uuid, 's3://bucket/plan',
                       's3://bucket/plan/trino-upload/' || md5('u' || g) || '.parquet', 'data', 'registered'
                FROM generate_series(1, :u) g
                """,
            ).bind("c", catalogId).bind("u", UPLOADS).execute()
            activePaths = (1..PATHS).map { "s3://bucket/plan/trino-upload/${UUID.randomUUID()}.parquet" }
            activeIds = activePaths.map { UUID.randomUUID() }
            h.createUpdate(
                """
                INSERT INTO hog_upload (catalog_id, upload_id, owner, prefix, path, file_kind)
                SELECT :c, id, :owner, 's3://bucket/plan', path, 'data'
                FROM unnest(CAST(:ids AS uuid[]), CAST(:paths AS text[])) AS a(id, path)
                """,
            ).bind("c", catalogId).bind("owner", owner)
                .bindArray("ids", UUID::class.java, activeIds).bindArray("paths", String::class.java, activePaths)
                .execute()
            h.createUpdate(
                """
                INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason, drained_at, drained_outcome)
                SELECT :c, 's3://bucket/plan/old/' || md5('r' || g) || '.parquet', 'data', 'snapshot_expiry',
                       CASE WHEN g % 10 = 0 THEN NULL ELSE now() END,
                       CASE WHEN g % 10 = 0 THEN NULL ELSE 'deleted' END
                FROM generate_series(1, :q) g
                """,
            ).bind("c", catalogId).bind("q", QUEUE).execute()
            h.createUpdate(
                """
                INSERT INTO hog_commit_receipt (catalog_id, idempotency_key, fingerprint, snapshot_id, schema_version)
                SELECT :c, md5('k' || g)::uuid, '\x01'::bytea, 1 + g % (:s - 1), 0 FROM generate_series(1, :r) g
                """,
            ).bind("c", catalogId).bind("s", SNAPSHOTS).bind("r", RECEIPTS).execute()
            dvTargets = listOf(41L, 81L, 121L)
            // HISTORY: every ended version of field 1 OLDER than the live
            // one, as the DDL writes them, and the live one moved past them.
            h.execute(
                "UPDATE hog_column SET begin_snapshot = ? WHERE catalog_id = ? AND table_id = ? AND field_id = 1",
                VERSIONS + 1,
                catalogId,
                HISTORY,
            )
            h.createUpdate(
                """
                INSERT INTO hog_column (catalog_id, table_id, field_id, begin_snapshot, end_snapshot, name, col_type,
                                        ordinal)
                SELECT :c, :t, 1, s, s + 1, 'team_' || s, 'long', 0 FROM generate_series(1, :v) s
                UNION ALL
                SELECT :c, :t, f, 0, NULL, 'c' || f, 'long', f - 1 FROM generate_series(:cols + 1, :cols + :w) f
                """,
            ).bind("c", catalogId).bind("t", HISTORY).bind("v", VERSIONS).bind("cols", COLUMNS).bind("w", WIDE)
                .execute()
            h.execute(
                "UPDATE hog_table SET next_field_id = ? WHERE catalog_id = ? AND table_id = ?",
                COLUMNS + WIDE + 1,
                catalogId,
                HISTORY,
            )
            val small = listOf("hog_namespace", "hog_partition_spec", "hog_partition_field", "hog_catalog")
            for (relation in BIG_TABLES + small) {
                h.execute("VACUUM (ANALYZE) $relation")
            }
        }
    }

    private fun head(): Long = db.headOf(catalogId)

    /** The pyhoglake / millrace shape: a prepared append of fresh, unclaimed paths to the hot table. */
    private fun preparedAppend() =
        CommitTail.ReadInputs(
            catalogId = catalogId,
            idempotencyKey = UUID.randomUUID(),
            tables = listOf(CommitTail.TableKey("ingest", "t$HOT", appending = true, unchanged = false)),
            readSnapshot = head() - READ_BEHIND,
            paths = (1..PATHS).map { "s3://bucket/plan/data/${UUID.randomUUID()}.parquet" },
            targetIds = emptyList(),
        )

    /**
     * A guarded transaction on the object-prefix table: appending under
     * its prefixes and registering DVs on three of its files, so the read
     * takes the layout, the identity key_index and each target's value.
     */
    private fun prefixTransaction() =
        CommitTail.ReadInputs(
            catalogId = catalogId,
            idempotencyKey = UUID.randomUUID(),
            tables = listOf(CommitTail.TableKey("ingest", "t$PREFIX", appending = true, unchanged = true)),
            readSnapshot = head() - READ_BEHIND,
            paths =
                (1..PATHS).map { "s3://bucket/plan/data/ingest/t$PREFIX/team_id=${it % TEAMS}/${UUID.randomUUID()}" },
            targetIds = prefixTargets,
        )

    /** A prepared append to [HISTORY], under its prefixes. */
    private fun historyAppend() =
        CommitTail.ReadInputs(
            catalogId = catalogId,
            idempotencyKey = UUID.randomUUID(),
            tables = listOf(CommitTail.TableKey("ingest", "t$HISTORY", appending = true, unchanged = false)),
            readSnapshot = head() - READ_BEHIND,
            paths = (1..PATHS).map { "s3://bucket/plan/data/ingest/t$HISTORY/c1=${it % TEAMS}/${UUID.randomUUID()}" },
            targetIds = emptyList(),
        )

    /** The Trino shape: a guarded transaction over claimed uploads, appending and superseding live DVs. */
    private fun claimedTransaction() =
        CommitTail.ReadInputs(
            catalogId = catalogId,
            idempotencyKey = UUID.randomUUID(),
            tables = listOf(CommitTail.TableKey("ingest", "t$HOT", appending = true, unchanged = true)),
            readSnapshot = head() - READ_BEHIND,
            paths = activePaths,
            targetIds = dvTargets,
        )

    /**
     * The write of a [PATHS]-file x [COLUMNS]-column partitioned append to
     * the hot table at the current head — with [withDvs], also
     * superseding the live DVs of [dvTargets] and settling the active
     * claims, the claimed transaction's write.
     */
    private fun writeSet(withDvs: Boolean): CommitTail.WriteSet {
        val (head, nextFileId, nextRowId) =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT c.last_snapshot_id, c.next_file_id, ts.next_row_id
                    FROM hog_catalog c JOIN hog_table_stats ts ON ts.catalog_id = c.catalog_id AND ts.table_id = :hot
                    WHERE c.catalog_id = :c
                    """,
                ).bind("c", catalogId).bind("hot", HOT)
                    .map { rs, _ -> Triple(rs.getLong(1), rs.getLong(2), rs.getLong(3)) }.one()
            }
        val bound = ByteArray(8) { it.toByte() }
        val files =
            (0 until PATHS).map { i ->
                CommitTail.DataFileRow(
                    dataFileId = nextFileId + i,
                    tableId = HOT,
                    path = "s3://bucket/plan/data/${UUID.randomUUID()}.parquet",
                    recordCount = 10,
                    fileSizeBytes = 1000,
                    footerSize = 20,
                    rowIdStart = nextRowId + i * 10L,
                    statsState = "provided",
                    specId = 1,
                    splitOffsets = listOf(4L),
                )
            }
        val appends =
            CommitTail.AppendRows(
                stats =
                    listOf(
                        CommitTail.StatsAdvance(HOT, nextRowId, nextRowId + PATHS * 10L, PATHS * 10L, PATHS * 1000L),
                    ),
                files = files,
                partitionValues = files.map { CommitTail.PartitionValueRow(it.dataFileId, 0, "7") },
                columnStats =
                    files.flatMap { f ->
                        (1..COLUMNS).map { c ->
                            CommitTail.ColumnStatsRow(
                                f.dataFileId,
                                ColumnStats(c.toLong(), 10, 0, null, 100, bound, bound),
                            )
                        }
                    },
            )
        val liveDvs =
            if (!withDvs) {
                emptyList()
            } else {
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery(
                        "SELECT delete_file_id FROM hog_delete_file " +
                            "WHERE catalog_id = :c AND data_file_id = ANY(:t) AND end_snapshot IS NULL",
                    ).bind("c", catalogId).bindArray("t", Long::class.javaObjectType, dvTargets)
                        .mapTo(Long::class.java).list()
                }
            }
        val deletes =
            CommitTail.DeleteRows(
                ended = liveDvs,
                files =
                    dvTargets.takeIf { withDvs }.orEmpty().mapIndexed { i, target ->
                        CommitTail.DeleteFileRow(
                            nextFileId + PATHS + i,
                            HOT,
                            target,
                            "s3://bucket/plan/dv/${UUID.randomUUID()}.puffin",
                            2,
                            16,
                        )
                    },
            )
        return CommitTail.WriteSet(
            catalogId = catalogId,
            head = head,
            firstFileId = nextFileId,
            fileCount = files.size + deletes.files.size,
            schemaVersion = 0,
            author = null,
            message = null,
            changes =
                listOf("table_inserted_into" to HOT) +
                    (if (withDvs) listOf("table_deleted_from" to HOT) else emptyList()),
            appends = appends,
            deletes = deletes,
            settleUploads = if (withDvs) activeIds else emptyList(),
            idempotencyKey = UUID.randomUUID(),
            fingerprint = ByteArray(33) { 1 },
        )
    }

    /**
     * EXPLAIN ANALYZE of [sql] bound by [bind] — the custom plan, with
     * what it actually read — rolled back, since both statements write
     * or lock.
     */
    private fun explainAnalyze(
        sql: String,
        bind: (org.jdbi.v3.core.statement.Query) -> org.jdbi.v3.core.statement.Query,
    ): String =
        db.jdbi.withHandleUnchecked { h ->
            h.begin()
            try {
                // A Gather reports its workers' scans with loops= equal to
                // the worker count; serial plans keep the numbers the machine's.
                h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                bind(h.createQuery("EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) $sql"))
                    .mapTo(String::class.java).list().joinToString("\n")
            } finally {
                h.rollback()
            }
        }

    /**
     * The GENERIC plan of [sql]: `EXPLAIN (GENERIC_PLAN)` (PG16+) over the
     * production text with its named parameters rewritten to `$n`, which
     * is the plan the server caches once pgjdbc prepares the statement.
     * No ANALYZE — a generic plan has no values to run with — so this
     * pins the SHAPE and [explainAnalyze] the cost. On a connection of its
     * own in pgjdbc's SIMPLE query mode: over the extended protocol the
     * `$n` are the Parse message's parameters and Bind must supply them.
     */
    private fun explainGeneric(sql: String): String {
        val url = db.jdbcUrl + (if ('?' in db.jdbcUrl) "&" else "?") + "preferQueryMode=simple"
        java.sql.DriverManager.getConnection(url, PgTestSupport.USER, PgTestSupport.PASSWORD).use { conn ->
            conn.createStatement().use { st ->
                st.execute("SET max_parallel_workers_per_gather = 0")
                st.executeQuery("EXPLAIN (GENERIC_PLAN, COSTS false) ${positional(sql)}").use { rs ->
                    return generateSequence { if (rs.next()) rs.getString(1) else null }.joinToString("\n")
                }
            }
        }
    }

    /** [sql] with each JDBI `:name` rewritten to `$n`, one n per distinct name. */
    private fun positional(sql: String): String {
        val names = LinkedHashMap<String, Int>()
        return Regex("""(?<![:\w]):([A-Za-z_][A-Za-z0-9_]*)""").replace(sql) { m ->
            "$" + names.getOrPut(m.groupValues[1]) { names.size + 1 }
        }
    }

    private fun assertNoScanOf(
        tables: List<String>,
        plan: String,
        what: String,
    ) {
        for (table in tables) {
            assertThat(
                plan,
            ).describedAs("%s scans %s whole:%n%s", what, table, plan).doesNotContain("Seq Scan on $table ")
        }
    }

    /** The statement's own total: the root node's cumulative Buffers line. */
    private fun totalBuffers(plan: String): Long = ExplainPlan.nodes(plan).first().buffers

    /** The lines of [plan] under `CTE [name]`: that CTE's subtree alone, its root node first. */
    private fun cte(
        plan: String,
        name: String,
    ): String {
        val lines = plan.lines()
        val start = lines.indexOfFirst { it.trim() == "CTE $name" }
        assertThat(start).describedAs("CTE %s in:%n%s", name, plan).isNotNegative()

        fun depth(l: String) = l.length - l.trimStart().length
        return lines.drop(start + 1).takeWhile { depth(it) > depth(lines[start]) }.joinToString("\n")
    }

    /** pg_class.relpages of [table], read rather than written down. */
    private fun pages(table: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT relpages FROM pg_class WHERE relname = :t",
            ).bind("t", table).mapTo(Long::class.java).one()
        }

    @Test
    fun `the read probes every big table by key, custom and generic`() {
        val generic = explainGeneric(CommitTail.READ_SQL)
        assertNoScanOf(BIG_TABLES, generic, "the generic read")
        // The keys the generic plan has to reach them by. The conflict
        // scan is the one to watch: on the event table it is the CHANGE
        // LOG of the hottest table in the catalog, and a plan that read it
        // from its catalog_id prefix would read every commit since the
        // floor.
        for (index in listOf(
            "hog_commit_receipt_pkey", "hog_table_version_live_name", "hog_table_stats_pkey", "hog_column_live",
            "hog_snapshot_change_conflict", "hog_upload_catalog_id_path_key", "hog_file_removal_undrained_path",
            "hog_data_file_path", "hog_data_file_pkey",
            // A DV target's value on a table with the object-prefix
            // option, by key. (Its identity field is read from
            // hog_partition_field, two rows here and scanned, as the
            // spec's field count already is.)
            "hog_file_partition_value_pkey",
        )) {
            assertThat(generic).describedAs("the generic read uses %s:%n%s", index, generic).contains("using $index on")
        }
        // The layout probe, one per resolved table: the generic plan
        // takes it as a bitmap scan of the live index, which reads the
        // same index pages.
        assertThat(generic)
            .describedAs("the generic read probes hog_table_layout_live:%n%s", generic)
            .containsPattern("(Bitmap Index Scan on|Index Scan using|Index Only Scan using) hog_table_layout_live")
        for ((name, inputs) in listOf(
            "prepared append" to preparedAppend(),
            "claimed transaction" to claimedTransaction(),
            "object-prefix transaction" to prefixTransaction(),
        )) {
            assertNoScanOf(
                BIG_TABLES,
                explainAnalyze(CommitTail.READ_SQL) { q -> CommitTail.bindRead(q, inputs) },
                "the $name read",
            )
        }
    }

    @Test
    fun `the read of a prepared append costs its probes, not the catalog`() {
        // Measured on this fixture: 58 buffers — the receipt, head and
        // resolution descents, 25 live column rows, the 50 change rows
        // since the read snapshot, one descent each of the claim key
        // and the queue key for 270 paths no claim or queue row shares a
        // prefix with, and (V29) the two of the layout probe that finds
        // the hot table has no object-prefix layout; 56 before it. The
        // bound is the smallest relation any of those probes would have
        // to read whole instead (hog_column, 516 pages here), quartered:
        // a plan that scans ANY big table cannot fit.
        val plan = explainAnalyze(CommitTail.READ_SQL) { q -> CommitTail.bindRead(q, preparedAppend()) }
        val budget = PROBED.minOf { pages(it) } / 4
        println("prepared-append read: ${totalBuffers(plan)} buffers (budget $budget)")
        assertThat(totalBuffers(plan)).describedAs("prepared-append read buffers:%n%s", plan).isLessThan(budget)
    }

    @Test
    fun `the read of an object-prefix transaction pays its layout probes by key, not the catalog`() {
        // On top of the prepared append's probes: one descent of the live
        // layout index, one of hog_partition_field for the identity
        // key_index, and for each of the three DV targets its file row,
        // its live DV and its value (hog_partition_field and
        // hog_file_partition_value, both by primary key). Measured 90
        // buffers here; the bound is the prepared append's, the smallest
        // probed relation quartered, which no scan fits.
        val plan = explainAnalyze(CommitTail.READ_SQL) { q -> CommitTail.bindRead(q, prefixTransaction()) }
        val budget = PROBED.minOf { pages(it) } / 4
        println("object-prefix transaction read: ${totalBuffers(plan)} buffers (budget $budget)")
        assertThat(plan).describedAs("the layout probe:%n%s", plan).contains("hog_table_layout_live")
        assertThat(totalBuffers(plan)).describedAs("object-prefix read buffers:%n%s", plan).isLessThan(budget)
        // And the values it read are the targets' own.
        val values =
            db.jdbi.withHandleUnchecked { h ->
                h.begin()
                try {
                    CommitTail.read(h, prefixTransaction()).targets.mapValues { it.value.prefixValues }
                } finally {
                    h.rollback()
                }
            }
        assertThat(values).isEqualTo(prefixTargets.associateWith { listOf("${it % TEAMS}") })
    }

    @Test
    fun `the layout read takes the prefix column's newest live version, not its history`() {
        // `layouts` on HISTORY: one descent of hog_table_layout_live and
        // its heap page (3 buffers), then one backward descent of
        // hog_column_pkey that stops at the live row (4). Measured 7; the
        // bound leaves a descent's slack. MUTATION (#490): the plain LEFT
        // JOIN filters the field's 500 ended versions on the forward
        // pkey scan, Rows Removed by Filter: 500, 21 buffers.
        val plan = explainAnalyze(CommitTail.READ_SQL) { q -> CommitTail.bindRead(q, historyAppend()) }
        val layouts = cte(plan, "layouts")
        val buffers = ExplainPlan.nodes(layouts).first().buffers
        println("history layout read: $buffers buffers, the read ${totalBuffers(plan)}")
        val generic = cte(explainGeneric(CommitTail.READ_SQL), "layouts")
        for ((shape, text) in listOf("custom" to layouts, "generic" to generic)) {
            assertThat(text).describedAs("the %s layout read:%n%s", shape, text)
                .contains("Index Scan Backward using hog_column_pkey on hog_column")
        }
        assertThat(layouts).describedAs("the layout read walks:%n%s", layouts).doesNotContain("Rows Removed by Filter")
        assertThat(buffers).describedAs("layout read buffers:%n%s", layouts).isLessThanOrEqualTo(10)
        // And it answers the live version, not one of the 500 behind it.
        val row =
            db.jdbi.withHandleUnchecked { h ->
                h.begin()
                try {
                    CommitTail.read(h, historyAppend()).tables.values.single().single()
                } finally {
                    h.rollback()
                }
            }
        assertThat(row.prefixFieldName to row.prefixColType).isEqualTo("c1" to "long")
    }

    @Test
    fun `the read of a claimed transaction pays per claim, through the path key`() {
        // 270 claims locked FOR UPDATE in upload_id order: one descent of
        // the (catalog_id, path) key and one heap tuple lock each —
        // measured 1,257-1,540 buffers on LockRows across runs, 4.7-5.7 per
        // claim. 8 per claim is the bound; the whole statement is that
        // plus the prepared append's probes.
        val plan = explainAnalyze(CommitTail.READ_SQL) { q -> CommitTail.bindRead(q, claimedTransaction()) }
        val lock = ExplainPlan.nodes(plan).single { it.line.contains("LockRows") }
        println("claimed-transaction read: ${totalBuffers(plan)} buffers, ${lock.buffers} locking $PATHS claims")
        assertThat(lock.buffers).describedAs("claim lock buffers:%n%s", plan).isLessThan(8L * PATHS)
        assertThat(totalBuffers(plan)).describedAs("claimed-transaction read buffers:%n%s", plan)
            .isLessThan(8L * PATHS + pages("hog_column") / 4)
    }

    @Test
    fun `the write is unnest inserts and keyed updates, custom and generic`() {
        val updated = listOf("hog_table_stats", "hog_delete_file", "hog_upload")
        val generic = explainGeneric(CommitTail.WRITE_SQL)
        assertNoScanOf(updated, generic, "the generic write")
        for (index in listOf("hog_table_stats_pkey", "hog_delete_file_pkey", "hog_upload_pkey")) {
            assertThat(generic).describedAs("the generic write uses %s:%n%s", index, generic).contains(index)
        }
        for (withDvs in listOf(false, true)) {
            val set = writeSet(withDvs)
            val plan = explainAnalyze(CommitTail.WRITE_SQL) { q -> CommitTail.bindWrite(q, set) }
            assertNoScanOf(updated, plan, "the write (DVs: $withDvs)")
            // Per row written: its heap page and a descent per index of
            // its table. hog_file_column_stats has one index and is 25 of
            // every 27 rows here, hog_data_file has ten; measured 3.5 per
            // row without DVs and 4.6 with them (the claims' UPDATE moves
            // every row out of two partial indexes).
            val rows =
                set.appends.files.size + set.appends.partitionValues.size + set.appends.columnStats.size +
                    set.appends.stats.size + set.changes.size + set.deletes.ended.size + set.deletes.files.size +
                    set.settleUploads.size + 2
            println("write (DVs: $withDvs): ${totalBuffers(plan)} buffers for $rows rows")
            assertThat(
                totalBuffers(plan),
            ).describedAs("write buffers for %d rows:%n%s", rows, plan).isLessThan(8L * rows)
        }
    }
}
