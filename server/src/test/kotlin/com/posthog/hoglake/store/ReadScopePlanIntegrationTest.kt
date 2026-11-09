package com.posthog.hoglake.store

import com.posthog.hoglake.Database
import com.posthog.hoglake.auth.PredicateGrant
import com.posthog.hoglake.auth.Scope
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.persistence.FileRepo
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.sql.DriverManager

/**
 * How a scoped scan plans on a production-shaped catalog, by default at
 * [DEFAULT_FILES] files, and once at 41M (plan row 1.2):
 *
 *     HOGLAKE_SCOPE_PLAN_FILES=41000000 \
 *     HOGLAKE_SCOPE_PLAN_JDBC_URL='jdbc:postgresql://127.0.0.1:41432/postgres?user=…&password=…' \
 *     ./gradlew :test --tests '*ReadScopePlanIntegrationTest' --rerun
 *
 * The external database is created as `scope_plan` and kept, so a rerun
 * at the same size reuses it; the size is recorded only after the seed's
 * last step, so an interrupted seed is never reused. Real rows, not planner statistics: Postgres
 * estimates a table's rows as its density times its ACTUAL pages, so a
 * faked `reltuples` does not make a small table plan like a big one.
 *
 * # The catalog
 * One catalog, the shape of `millrace-<env>`: `ingest.events` (90% of the
 * files; spec 1 identity(team_id), month(ts) for its oldest tenth, spec 2
 * identity(team_id), day(ts) after), `sidecar` (8%, the same values, so a
 * value probe is catalog-wide), and `reordered` (2%, half of it under
 * spec 2 day(ts), identity(team_id), the two-arm shape). Team 2 holds 5%
 * of the files; the rest spread over 20,000 teams whose ids overlap the
 * day ordinals. 1% pending, 0.2% quarantined, 3% ended, one live DV per
 * 50 files.
 *
 * # The property
 * In the custom and the generic plan: each arm's ids come from
 * `hog_file_partition_value_lookup` as an InitPlan (index-only, or a
 * bitmap scan for a whale's share of the catalog), the manifest is read
 * on `hog_data_file_pkey` with `data_file_id = ANY(...)` and on no other
 * index, and nothing scans `hog_data_file` or `hog_file_partition_value`
 * sequentially. With ANALYZE, a scan reads about 20 buffers per file it
 * returns plus 2 per id its arms collect, not a number set by the table.
 *
 * # Measured (postgres:18.6, 2026-11-09)
 * At 41M files (`hog_data_file` 744,515 heap pages, 13 GB; 82M partition
 * values, 11 GB), every plan below, custom and generic, had the shape:
 *  - small team: 2,050 ids collected, 1,988 returned, 26,189 buffers;
 *  - reordered, two arms: 4,100 collected, 1,987 returned, 36,475 buffers;
 *  - reordered, team 19500 = a day ordinal: the key-1 arm collected the
 *    whole day, 43,050 ids for 1,988 returned, 27,542 buffers;
 *  - whale team (5%) and a 2,001-value org: the same shape (no ANALYZE).
 * At 200k files the small team collects 10 ids and reads about 100.
 *
 * The statement is ScanService.planScan's with the scope ANDed in, as 1.3
 * will serve it; 1.3 swaps the copy below for the real statement.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReadScopePlanIntegrationTest {
    private companion object {
        const val DEFAULT_FILES = 200_000L
        val FILES: Long = System.getenv("HOGLAKE_SCOPE_PLAN_FILES")?.toLong() ?: DEFAULT_FILES
        val EXTERNAL: String? = System.getenv("HOGLAKE_SCOPE_PLAN_JDBC_URL")
        const val CHUNK = 1_000_000L
        const val WHALE = 2L
        const val CATALOG = "scopeplan"

        /** The data path names the size, so an external rerun knows whether it can reuse the fixture. */
        val DATA_PATH = "s3://$CATALOG/$FILES"

        /** team_id of file g; the same expression in every statement below. */
        const val TEAM = "(CASE WHEN g % 20 = 1 THEN 2 ELSE 1000 + (g * 7919) % 20000 END)"
    }

    private val external: HikariDataSource? = EXTERNAL?.let { externalDataSource(it) }
    private val db = if (external == null) PgTestSupport.freshDatabase() else null
    private val jdbi: Jdbi = external?.let { Database.jdbi(it) } ?: db!!.jdbi
    private var catalogId = 0L
    private val tables = mutableMapOf<String, Long>()
    private var head = 0L
    private var teamField = 0L

    init {
        val reuse =
            jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT count(*) FROM hog_catalog WHERE name = :n AND data_path = :p")
                    .bind("n", CATALOG).bind("p", DATA_PATH).mapTo(Long::class.java).one() == 1L
            }
        if (!reuse) seed()
        jdbi.useHandleUnchecked { h ->
            catalogId =
                h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = :n")
                    .bind("n", CATALOG).mapTo(Long::class.java).one()
            for (name in listOf("events", "sidecar", "reordered")) {
                tables[name] =
                    h.createQuery("SELECT table_id FROM hog_table_version WHERE catalog_id = :c AND name = :n")
                        .bind("c", catalogId).bind("n", name).mapTo(Long::class.java).one()
            }
            teamField =
                h.createQuery("SELECT object_prefix_field_id FROM hog_table_layout WHERE catalog_id = :c LIMIT 1")
                    .bind("c", catalogId).mapTo(Long::class.java).one()
            head = FILES / 100 + 10
        }
    }

    @AfterAll
    fun tearDown() {
        external?.close()
        db?.close()
    }

    private fun externalDataSource(url: String): HikariDataSource {
        val name = "scope_plan"
        DriverManager.getConnection(url).use { conn ->
            val exists =
                conn.prepareStatement("SELECT 1 FROM pg_database WHERE datname = ?").use {
                    it.setString(1, name)
                    it.executeQuery().next()
                }
            if (!exists) conn.createStatement().use { it.execute("CREATE DATABASE $name") }
        }
        val ds = HikariDataSource(HikariConfig().apply { jdbcUrl = url.replace("/postgres?", "/$name?") })
        Database.migrate(ds)
        return ds
    }

    private fun seed() {
        val catalogs = CatalogService(jdbi)
        catalogs.createCatalog(CATALOG, "s3://$CATALOG/seeding")
        catalogs.createNamespace(CATALOG, "ingest")
        val columns = listOf(ColumnDef("team_id", ColType.LONG), ColumnDef("ts", ColType.LONG))
        var ts = 0L
        for (name in listOf("events", "sidecar", "reordered")) {
            val info = catalogs.createTable(CATALOG, "ingest", name, columns)
            tables[name] = info.tableId
            teamField = info.columns.single { it.def.name == "team_id" }.fieldId
            ts = info.columns.single { it.def.name == "ts" }.fieldId
        }
        jdbi.useHandleUnchecked { h ->
            catalogId =
                h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = :n")
                    .bind("n", CATALOG).mapTo(Long::class.java).one()
            spec(h, "events", 1, 0, 1, teamField to "identity", ts to "month")
            spec(h, "events", 2, 1, null, teamField to "identity", ts to "day")
            spec(h, "sidecar", 1, 0, null, teamField to "identity", ts to "day")
            spec(h, "reordered", 1, 0, null, teamField to "identity", ts to "day")
            spec(h, "reordered", 2, 0, null, ts to "day", teamField to "identity")
            for (table in tables.values) {
                h.execute(
                    "INSERT INTO hog_table_layout (catalog_id, table_id, begin_snapshot, object_prefix_field_id) " +
                        "VALUES (?, ?, 0, ?)",
                    catalogId,
                    table,
                    teamField,
                )
            }
            val manifest = listOf("hog_data_file", "hog_file_partition_value", "hog_delete_file")
            // Bulk load without the secondary indexes, then build them from
            // their own definitions: the same indexes, built once.
            val indexes =
                h.createQuery(
                    """
                    SELECT indexname, indexdef FROM pg_indexes
                     WHERE schemaname = 'public' AND tablename = ANY(:t)
                       AND indexname NOT IN (SELECT conname FROM pg_constraint)
                    """,
                ).bindArray("t", String::class.java, manifest)
                    .map { rs, _ -> rs.getString("indexname") to rs.getString("indexdef") }.list()
            indexes.forEach { (name, _) -> h.execute("DROP INDEX $name") }
            h.execute("SET session_replication_role = replica")
            var lo = 1L
            while (lo <= FILES) {
                val hi = minOf(lo + CHUNK - 1, FILES)
                loadChunk(h, lo, hi)
                lo = hi + 1
            }
            h.execute("RESET session_replication_role")
            indexes.forEach { (_, def) -> h.execute(def) }
            manifest.forEach { h.execute("VACUUM (ANALYZE) $it") }
            // Last: what a rerun checks before reusing the fixture.
            h.createUpdate("UPDATE hog_catalog SET data_path = :p WHERE catalog_id = :c")
                .bind("p", DATA_PATH).bind("c", catalogId).execute()
        }
    }

    private fun spec(
        h: Handle,
        table: String,
        spec: Long,
        begin: Long,
        end: Long?,
        vararg fields: Pair<Long, String>,
    ) {
        h.createUpdate(
            "INSERT INTO hog_partition_spec (catalog_id, table_id, spec_id, begin_snapshot, end_snapshot) " +
                "VALUES (:c, :t, :s, :b, :e)",
        ).bind("c", catalogId).bind("t", tables.getValue(table)).bind("s", spec).bind("b", begin)
            .bind("e", end).execute()
        fields.forEachIndexed { key, (source, transform) ->
            h.execute(
                "INSERT INTO hog_partition_field (catalog_id, table_id, spec_id, key_index, source_field_id, " +
                    "transform) VALUES (?, ?, ?, ?, ?, ?)",
                catalogId,
                tables.getValue(table),
                spec,
                key,
                source,
                transform,
            )
        }
    }

    /** Files [lo]..[hi], their two partition values and their DVs, in id order. */
    private fun loadChunk(
        h: Handle,
        lo: Long,
        hi: Long,
    ) {
        h.createUpdate(
            """
            INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot, end_snapshot, path,
                                       record_count, file_size_bytes, row_id_start, spec_id, stats_state,
                                       prefix_quarantined)
            SELECT :c, g,
                   CASE WHEN g % 50 = 0 THEN :reordered WHEN g % 10 = 9 THEN :sidecar ELSE :events END,
                   1 + g / 100,
                   CASE WHEN g % 33 = 5 THEN 2 + g / 100 END,
                   'p' || g,
                   1000,
                   CASE WHEN g % 7 < 2 THEN 209715200 ELSE 65536 END,
                   g * 1000,
                   CASE WHEN g % 100 = 0 THEN 2 WHEN g % 50 = 0 THEN 1
                        WHEN g % 10 = 9 THEN 1 WHEN g <= :n / 10 THEN 1 ELSE 2 END,
                   CASE WHEN g % 100 = 3 THEN 'pending' ELSE 'provided' END,
                   g % 500 = 7
              FROM generate_series(:lo::bigint, :hi::bigint) g
            """,
        ).bind("c", catalogId).bind("n", FILES).bind("lo", lo).bind("hi", hi)
            .bind("events", tables.getValue("events")).bind("sidecar", tables.getValue("sidecar"))
            .bind("reordered", tables.getValue("reordered")).execute()
        // day ordinals 19000..19999 in commit order; the month ordinal is the day's / 30.
        h.createUpdate(
            """
            INSERT INTO hog_file_partition_value (catalog_id, data_file_id, key_index, value)
            SELECT :c, g, k,
                   CASE
                     WHEN (g % 100 = 0) = (k = 1) THEN $TEAM::text
                     WHEN g % 50 <> 0 AND g % 10 <> 9 AND g <= :n / 10 THEN ((19000 + g * 1000 / :n) / 30)::text
                     ELSE (19000 + g * 1000 / :n)::text
                   END
              FROM generate_series(:lo::bigint, :hi::bigint) g CROSS JOIN (VALUES (0), (1)) AS keys (k)
             ORDER BY g, k
            """,
        ).bind("c", catalogId).bind("n", FILES).bind("lo", lo).bind("hi", hi).execute()
        h.createUpdate(
            """
            INSERT INTO hog_delete_file (catalog_id, delete_file_id, table_id, data_file_id, begin_snapshot,
                                         path, delete_count, file_size_bytes)
            SELECT f.catalog_id, f.data_file_id, f.table_id, f.data_file_id, f.begin_snapshot,
                   'dv' || f.data_file_id, 3, 100
              FROM hog_data_file f
             WHERE f.catalog_id = :c AND f.data_file_id BETWEEN :lo AND :hi AND f.data_file_id % 50 = 11
            """,
        ).bind("c", catalogId).bind("lo", lo).bind("hi", hi).execute()
    }

    /** ScanService.planScan's statement, with the scope ANDed into its WHERE. */
    private fun scanSql(scope: ReadScope): String =
        """
        SELECT df.data_file_id, df.table_id, df.path, df.file_format,
               df.record_count, df.file_size_bytes, df.footer_size,
               df.row_id_start, df.stats_state, df.begin_snapshot, df.spec_id,
               df.explicit_row_ids,
               CASE WHEN :splitOffsets THEN df.split_offsets END AS split_offsets,
               pv.partition_values,
               dv.delete_file_id AS dv_id, dv.path AS dv_path,
               dv.file_format AS dv_format, dv.delete_count AS dv_delete_count,
               dv.file_size_bytes AS dv_file_size_bytes,
               dv.begin_snapshot AS dv_begin_snapshot
          FROM hog_data_file df
          LEFT JOIN hog_delete_file dv
            ON dv.catalog_id = df.catalog_id
           AND dv.data_file_id = df.data_file_id
           AND ${FileRepo.visibleAt("dv")}
          LEFT JOIN LATERAL (
                SELECT array_agg(v.value ORDER BY v.key_index) AS partition_values
                  FROM hog_file_partition_value v
                 WHERE v.catalog_id = df.catalog_id
                   AND v.data_file_id = df.data_file_id
               ) pv ON true
         WHERE df.catalog_id = :catalogId AND df.table_id = :tableId
           AND ${FileRepo.visibleAt("df")}
           AND ${scope.sql("df")}
         ORDER BY df.row_id_start, df.data_file_id
        """

    private fun scope(
        table: String,
        mount: Collection<Long>,
        grant: PredicateGrant,
    ): ReadScope =
        jdbi.withHandleUnchecked { h ->
            ReadScope.load(h, catalogId, tables.getValue(table), head, teamField, mount, grant)
        }

    /** EXPLAIN with the values bound: the custom plan. */
    private fun explain(
        table: String,
        scope: ReadScope,
        analyze: Boolean,
    ): String =
        jdbi.withHandleUnchecked { h ->
            h.begin()
            try {
                // Serial plans only: a Gather splits the buffer counts across workers.
                h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                val options = if (analyze) "ANALYZE, BUFFERS, TIMING false, " else ""
                scope.bind(h.createQuery("EXPLAIN ($options COSTS false, SUMMARY false) ${scanSql(scope)}"))
                    .bind("catalogId", catalogId).bind("tableId", tables.getValue(table))
                    .bind("snapshot", head).bind("splitOffsets", false)
                    .mapTo(String::class.java).list().joinToString("\n")
            } finally {
                h.rollback()
            }
        }

    /** EXPLAIN (GENERIC_PLAN): the plan a prepared statement can switch to, with no values known. */
    private fun explainGeneric(scope: ReadScope): String {
        val names = mutableListOf<String>()
        val positional =
            Regex("""(?<!:):([a-zA-Z_][a-zA-Z0-9_]*)""").replace(scanSql(scope)) { m ->
                val name = m.groupValues[1]
                if (name !in names) names += name
                "\$${names.indexOf(name) + 1}"
            }
        // The simple query protocol: over the extended one the server counts
        // the $n placeholders as the statement's parameters and refuses a
        // Bind with none.
        val url = external?.jdbcUrl ?: db!!.jdbcUrl
        val props = java.util.Properties()
        if (external == null) {
            props["user"] = PgTestSupport.USER
            props["password"] = PgTestSupport.PASSWORD
        }
        val separator = if ('?' in url) "&" else "?"
        return DriverManager.getConnection("$url${separator}preferQueryMode=simple", props).use { conn ->
            conn.createStatement().use { st ->
                st.execute("SET max_parallel_workers_per_gather = 0")
                st.executeQuery("EXPLAIN (GENERIC_PLAN, COSTS false) $positional").use { rs ->
                    generateSequence { if (rs.next()) rs.getString(1) else null }.joinToString("\n")
                }
            }
        }
    }

    private fun assertScoped(
        plan: String,
        scope: ReadScope,
    ) {
        assertThat(
            plan,
        ).doesNotContain("Seq Scan on hog_data_file").doesNotContain("Seq Scan on hog_file_partition_value")
        val manifestIndexes = Regex("""hog_data_file_\w+""").findAll(plan).map { it.value }.toSet()
        assertThat(manifestIndexes).describedAs(plan).containsExactly("hog_data_file_pkey")
        assertThat(Regex("""Index Cond: .*data_file_id = ANY \(""").containsMatchIn(plan))
            .describedAs(plan).isTrue()
        // An index-only, index or bitmap scan of the lookup index: each names it once.
        assertThat(Regex("""(?:Scan using|Index Scan on) hog_file_partition_value_lookup\b""").findAll(plan).count())
            .describedAs(plan).isEqualTo(scope.arms.size)
        assertThat(Regex("""(?m)^\s*InitPlan \d+\s*$""").findAll(plan).count())
            .describedAs(plan).isEqualTo(scope.arms.size)
    }

    /** Rows the plan's root returned. */
    private fun returned(plan: String): Long =
        Regex("""actual rows=([\d.]+)""").find(plan.lines().first())!!.groupValues[1].toDouble().toLong()

    /** The work bound: per returned file its probes, per collected id its PK descent. */
    private fun bound(
        plan: String,
        collected: Long,
    ): Long = 20 * returned(plan) + 2 * collected + 200

    /** Buffers the plan's root reports, hit and read. */
    private fun rootBuffers(plan: String): Long {
        val line = plan.lines().first { it.trim().startsWith("Buffers:") }
        return Regex("""\b(?:hit|read)=(\d+)""").findAll(line).sumOf { it.groupValues[1].toLong() }
    }

    /** Files in the catalog with one of [teams] at [key]: what the arm's InitPlan collects. */
    private fun collected(
        teams: List<Long>,
        key: Int,
    ): Long =
        jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT count(*) FROM hog_file_partition_value WHERE catalog_id = :c AND key_index = :k " +
                    "AND value = ANY(:v)",
            ).bind("c", catalogId).bind("k", key).bindArray("v", String::class.java, teams.map { it.toString() })
                .mapTo(Long::class.java).one()
        }

    private val smallTeam = 1000L + (2L * 7919) % 20000

    /** Rows the scoped scan returns for [teams] on [table]. */
    private fun admittedCount(
        table: String,
        teams: List<Long>,
    ): Int {
        val scope = scope(table, teams, PredicateGrant.All(Scope.READ))
        return jdbi.withHandleUnchecked { h ->
            scope.bind(h.createQuery(scanSql(scope)))
                .bind("catalogId", catalogId).bind("tableId", tables.getValue(table))
                .bind("snapshot", head).bind("splitOffsets", false)
                .mapToMap().list().size
        }
    }

    @Test
    fun `a small team's scan is driven by its files, in the custom and the generic plan`() {
        val scope = scope("events", listOf(smallTeam, WHALE), PredicateGrant.Values(mapOf(smallTeam to Scope.READ)))
        assertThat(scope.arms).hasSize(1)
        val plan = explain("events", scope, analyze = true)
        assertScoped(plan, scope)
        assertScoped(explainGeneric(scope), scope)
        val collected = collected(listOf(smallTeam), 0)
        assertThat(collected).isGreaterThan(0)
        // Per collected file: a primary-key probe, its heap page, its partition values and its DV probe.
        assertThat(rootBuffers(plan)).describedAs(plan).isLessThanOrEqualTo(bound(plan, collected))
        println("ReadScopePlan files=$FILES small team: collected=$collected buffers=${rootBuffers(plan)}\n$plan")
        println("ReadScopePlan files=$FILES small team, generic:\n${explainGeneric(scope)}")
    }

    /**
     * Team 12900's files are g = 100 (mod 20000): all under `reordered`'s
     * spec 2, its value at key 1. Team 19500 is also a day ordinal, so the
     * key-1 arm collects that whole day across the catalog and the filter
     * drops it: the cost of a reordered spec, bounded by what it collects.
     */
    @Test
    fun `the two-arm shape of a reordered spec plans the same way`() {
        for (teams in listOf(listOf(smallTeam, 12_900L), listOf(19_500L))) {
            val scope = scope("reordered", teams, PredicateGrant.All(Scope.READ))
            assertThat(scope.arms).hasSize(2)
            val plan = explain("reordered", scope, analyze = true)
            assertScoped(plan, scope)
            assertScoped(explainGeneric(scope), scope)
            val collected = collected(teams, 0) + collected(teams, 1)
            assertThat(rootBuffers(plan)).describedAs(plan).isLessThanOrEqualTo(bound(plan, collected))
            println(
                "ReadScopePlan files=$FILES reordered $teams: collected=$collected buffers=${rootBuffers(plan)}\n$plan",
            )
        }
        assertThat(admittedCount("reordered", listOf(12_900L))).isGreaterThan(0)
    }

    @Test
    fun `a whale team and a whale org keep the shape`() {
        val whale = scope("events", listOf(WHALE), PredicateGrant.All(Scope.READ))
        assertScoped(explain("events", whale, analyze = false), whale)
        assertScoped(explainGeneric(whale), whale)
        val org = (0L until 2_000L).map { 1000L + it } + WHALE
        val all = scope("events", org, PredicateGrant.All(Scope.READ))
        assertThat(all.values).hasSize(org.size)
        val plan = explain("events", all, analyze = false)
        assertScoped(plan, all)
        assertScoped(explainGeneric(all), all)
        println("ReadScopePlan files=$FILES whale org:\n$plan")
    }
}
