package com.posthog.hoglake.persistence

import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * The plan behind `NamespaceRepo.listAt` (#28), at a few thousand
 * namespaces in one catalog.
 *
 * The visibility rule reads the change log once per namespace. The
 * property is that each read is an index PROBE on
 * `hog_snapshot_change_conflict` bounded to that namespace, never a scan
 * of change rows by snapshot range: the EXISTS form this statement
 * replaced planned its drop check as a hashed SubPlan over
 * `snapshot_id > :snapshot` with catalog_id only a hash key, which read
 * the change rows above the pin in every catalog.
 *
 * Two plans, because pgjdbc switches a statement to a server-side
 * prepared one after five executions and Postgres may then pick a
 * GENERIC plan, built without the bound values. EXPLAIN of a bound
 * query always plans custom, so the generic one is forced through
 * PREPARE + `plan_cache_mode = force_generic_plan`.
 *
 * The fixture carries what would make a bad plan visible: 200,000
 * table-scoped change rows on the SAME object ids (the kind filter
 * trap), and a second catalog of the same size whose snapshot ids
 * overlap this one's, so a scan that is not bounded to the catalog
 * reads twice what it should.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NamespaceListingQueryPlanIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private var catalogId = 0L
    private var pin = 0L

    @AfterAll
    fun tearDown() = db.close()

    private companion object {
        const val NAMESPACES = 3000

        /** Every third namespace is dropped; its drop is at base + NAMESPACES + id. */
        const val DROPPED = NAMESPACES / 3
        const val NAMESPACE_CHANGE_ROWS = NAMESPACES + DROPPED
        const val TABLE_CHANGE_ROWS = 200_000

        /**
         * Visible at the pin (base + 1.5 x NAMESPACES): every namespace was
         * created before it; the 2,000 live ones plus the 500 dropped
         * after it.
         */
        const val VISIBLE_AT_PIN = (NAMESPACES - DROPPED) + DROPPED / 2

        /** Measured 3.0 per namespace (one B-tree descent, index-only); headroom for a deeper tree. */
        const val BUFFERS_PER_NAMESPACE = 5
    }

    private enum class Shape { CUSTOM, GENERIC }

    @BeforeAll
    fun seed() {
        catalogs.createCatalog("nsplan", "s3://nsplan")
        catalogs.createCatalog("nsplan-other", "s3://nsplan-other")
        db.jdbi.useHandleUnchecked { h ->
            val ids = listOf("nsplan", "nsplan-other").map { seedCatalog(h, it) }
            catalogId = ids[0].first
            pin = ids[0].second + NAMESPACES + NAMESPACES / 2
            // VACUUM, not just ANALYZE: a bulk insert leaves the visibility
            // map unset, so an Index Only Scan would pay a heap fetch per
            // row and the buffer budget would measure that instead.
            for (r in listOf("hog_namespace", "hog_snapshot_change")) h.execute("VACUUM (ANALYZE) $r")
        }
    }

    /** One catalog's namespaces and change log; returns (catalog_id, base snapshot). */
    private fun seedCatalog(
        h: Handle,
        name: String,
    ): Pair<Long, Long> {
        val c =
            h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = :n")
                .bind("n", name).mapTo(Long::class.java).one()
        val base =
            h.createQuery("SELECT last_snapshot_id FROM hog_catalog WHERE catalog_id = :c")
                .bind("c", c).mapTo(Long::class.java).one()
        h.createUpdate(
            """
            INSERT INTO hog_snapshot (catalog_id, snapshot_id, schema_version)
            SELECT :c, :b + g, 1 FROM generate_series(1, 2 * :n) g
            """,
        ).bind("c", c).bind("b", base).bind("n", NAMESPACES).execute()
        h.createUpdate("UPDATE hog_catalog SET last_snapshot_id = :t WHERE catalog_id = :c")
            .bind("c", c).bind("t", base + 2 * NAMESPACES).execute()
        h.createUpdate(
            """
            INSERT INTO hog_namespace (catalog_id, namespace_id, name, dropped)
            SELECT :c, g, 'ns' || g, g % 3 = 0 FROM generate_series(1, :n) g
            """,
        ).bind("c", c).bind("n", NAMESPACES).execute()
        h.createUpdate(
            """
            INSERT INTO hog_snapshot_change (catalog_id, snapshot_id, kind, object_id)
            SELECT :c, :b + g, 'namespace_created', g FROM generate_series(1, :n) g
            """,
        ).bind("c", c).bind("b", base).bind("n", NAMESPACES).execute()
        h.createUpdate(
            """
            INSERT INTO hog_snapshot_change (catalog_id, snapshot_id, kind, object_id)
            SELECT :c, :b + :n + g, 'namespace_dropped', g FROM generate_series(1, :n) g
            WHERE g % 3 = 0
            """,
        ).bind("c", c).bind("b", base).bind("n", NAMESPACES).execute()
        h.createUpdate(
            """
            INSERT INTO hog_snapshot_change (catalog_id, snapshot_id, kind, object_id)
            SELECT :c, :b + 1 + (g % (2 * :n)), 'table_inserted_into', 1 + (g % :n)
            FROM generate_series(1, :rows) g
            """,
        ).bind("c", c).bind("b", base).bind("n", NAMESPACES).bind("rows", TABLE_CHANGE_ROWS).execute()
        return c to base
    }

    private val explain = "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) "

    private fun plan(shape: Shape): String =
        db.jdbi.inTransactionUnchecked { h ->
            h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
            when (shape) {
                Shape.CUSTOM ->
                    h.createQuery(explain + NamespaceRepo.LIST_AT_SQL)
                        .bind("catalogId", catalogId).bind("snapshot", pin)
                        .mapTo(String::class.java).list().joinToString("\n")
                Shape.GENERIC -> {
                    h.execute("SET LOCAL plan_cache_mode = force_generic_plan")
                    val sql = NamespaceRepo.LIST_AT_SQL.replace(":catalogId", "$1").replace(":snapshot", "$2")
                    h.execute("PREPARE nsplan_generic(bigint, bigint) AS $sql")
                    try {
                        // Literals, not binds: EXPLAIN EXECUTE is a utility
                        // statement and takes no parameters.
                        h.createQuery("${explain}EXECUTE nsplan_generic($catalogId, $pin)")
                            .mapTo(String::class.java).list().joinToString("\n")
                    } finally {
                        h.execute("DEALLOCATE nsplan_generic")
                    }
                }
            }
        }

    /** One scan node; [rows] is post-filter, so work is (rows + removed) x loops. */
    private data class Scan(
        val line: String,
        val index: String?,
        val rows: Double,
        val loops: Long,
        val removed: Long,
        val buffers: Long,
    ) {
        val rowsTouched: Long get() = ((rows + removed) * loops).toLong()
    }

    private fun scans(
        text: String,
        relation: String,
    ): List<Scan> {
        val lines = text.lines()

        fun indent(l: String) = l.length - l.trimStart().length
        return lines.withIndex().filter {
                (_, l) ->
            Regex("""\bon $relation\b""").containsMatchIn(l)
        }.map { (i, line) ->
            val detail = lines.drop(i + 1).takeWhile { it.isNotBlank() && indent(it) > indent(line) }

            fun detailLong(prefix: String) =
                detail.firstOrNull { it.trim().startsWith(prefix) }
                    ?.let { Regex("""(\d+)""").find(it.substringAfter(prefix))?.value?.toLong() } ?: 0L
            val buffers = detail.firstOrNull { it.trim().startsWith("Buffers:") }
            val rowsLoops = Regex("""rows=([\d.]+)\s+loops=(\d+)""").find(line)
            Scan(
                line = line.trim(),
                index = Regex("""Scan using (\S+) on""").find(line)?.groupValues?.get(1),
                rows = rowsLoops?.groupValues?.get(1)?.toDouble() ?: 0.0,
                loops = rowsLoops?.groupValues?.get(2)?.toLong() ?: 0L,
                removed = detailLong("Rows Removed by Filter:"),
                buffers =
                    listOf("""\bhit=(\d+)""", """\bread=(\d+)""").sumOf { p ->
                        buffers?.let { Regex(p).find(it)?.groupValues?.get(1)?.toLong() } ?: 0L
                    },
            )
        }
    }

    @Test
    fun `the change log is probed once per namespace of this catalog, through the index`() {
        for (shape in Shape.entries) {
            val text = plan(shape)
            assertThat(text)
                .describedAs("%s plan must not sequentially scan hog_snapshot_change:\n%s", shape, text)
                .doesNotContain("Seq Scan on hog_snapshot_change")
            val change = scans(text, "hog_snapshot_change")
            assertThat(change).describedAs("%s plan should read the change log:\n%s", shape, text).isNotEmpty()
            assertThat(change).allSatisfy {
                assertThat(
                    it.index,
                ).describedAs("%s: %s\n%s", shape, it.line, text).isEqualTo("hog_snapshot_change_conflict")
                // Once per namespace of THIS catalog: a hashed or
                // range-driven form runs once (loops=1) and reads by
                // snapshot instead.
                assertThat(it.loops).describedAs("%s: %s\n%s", shape, it.line, text).isEqualTo(NAMESPACES.toLong())
            }
        }
    }

    @Test
    fun `the probes look at no more than this catalog's namespace change rows`() {
        for (shape in Shape.entries) {
            val text = plan(shape)
            val touched = scans(text, "hog_snapshot_change").sumOf { it.rowsTouched }
            assertThat(touched)
                .describedAs(
                    "%s plan looked at %d change rows; this catalog holds %d namespace change rows " +
                        "(and %d table ones on the same ids, plus a second catalog's)\n%s",
                    shape,
                    touched,
                    NAMESPACE_CHANGE_ROWS,
                    TABLE_CHANGE_ROWS,
                    text,
                )
                .isLessThanOrEqualTo(NAMESPACE_CHANGE_ROWS.toLong())
        }
    }

    @Test
    fun `the probes cost a few buffers per namespace`() {
        for (shape in Shape.entries) {
            val text = plan(shape)
            val buffers = scans(text, "hog_snapshot_change").sumOf { it.buffers }
            assertThat(buffers)
                .describedAs(
                    "%s plan read %d shared buffers of the change log for %d namespaces " +
                        "(budget %d per namespace)\n%s",
                    shape,
                    buffers,
                    NAMESPACES,
                    BUFFERS_PER_NAMESPACE,
                    text,
                )
                .isLessThanOrEqualTo(NAMESPACES.toLong() * BUFFERS_PER_NAMESPACE)
                .isGreaterThan(0)
        }
    }

    @Test
    fun `the driving scan reads each namespace row at most once`() {
        // MEASURED: with one catalog the planner seq-scans hog_namespace
        // (the head listing's driving scan has the same predicate). With
        // two it may read both catalogs' rows; that is a cheap term, and
        // it is bounded here rather than wished away.
        for (shape in Shape.entries) {
            val text = plan(shape)
            val driving = scans(text, "hog_namespace").single()
            assertThat(driving.rowsTouched)
                .describedAs("%s driving scan looked at %d rows:\n%s", shape, driving.rowsTouched, text)
                .isLessThanOrEqualTo(2L * NAMESPACES)
        }
    }

    @Test
    fun `the listing answers for the pin`() {
        val names =
            db.jdbi.withHandleUnchecked { h -> NamespaceRepo.listAt(h, catalogId, pin) }.map { it.name }
        assertThat(names).hasSize(VISIBLE_AT_PIN)
        // ns3 was dropped at base + NAMESPACES + 3, before the pin; ns2997
        // at base + NAMESPACES + 2997, after it.
        assertThat(names).doesNotContain("ns3").contains("ns1", "ns2997")
    }
}
