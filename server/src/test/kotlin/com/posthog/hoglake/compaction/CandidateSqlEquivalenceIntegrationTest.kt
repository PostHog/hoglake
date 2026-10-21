package com.posthog.hoglake.compaction

import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.persistence.Pg
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * `CompactionService.candidateSql` returns the RIGHT ROWS: for 0, 1 and
 * 2 arms, the new statement, the statement it replaced
 * ([ReplacedCandidateSql]) and a ground truth computed independently of
 * both agree on every bucket the cap can hold.
 *
 * The plan tests (`CompactionBucketProbePlanIntegrationTest`,
 * `CompactionCandidateFetchPlanIntegrationTest`) prove what a probe
 * COSTS. None of them proves it returns the bucket: a probe that drops
 * a key's arm, matches a NULL value as a non-match, joins an ended DV or
 * lets another spec's files in can be cheap and wrong. This is that
 * proof, on a fixture small enough to state every bucket by hand:
 *
 *  - buckets with live DVs and with an ENDED DV, which must not show;
 *  - a NULL value in either key position, including the first, which is
 *    where the SQL starts its id set;
 *  - a bucket over the scan cap, where neither statement promises WHICH
 *    members come back, only that each is one;
 *  - the same tuple under a SECOND spec_id, which is where the replaced
 *    statement under-fetched (its spec filter sat outside the cap; see
 *    `CompactionService.bucketCandidates`) and the new one must not;
 *  - tuple-less files, ended files and over-target files in the probed
 *    bucket, all excluded;
 *  - a neighbour table sharing the values, because the arms are
 *    catalog-scoped and only `table_id` keeps its files out.
 *
 * Each probe runs once custom (`Pg.CUSTOM_PLANS_ONLY`, as production
 * sends it) and seven times uncustomized on one handle, past pgjdbc's
 * fifth-execution prepare threshold, so a GENERIC plan's answer is
 * checked too.
 *
 * Contributed by the QE review of #397; its mutations (a) dropping the
 * `table_id` filter, (b) dropping the second key's `EXISTS`, (c) `IN`
 * for `ARRAY`, (d) the customizer, (e2) the NULL arm were each caught.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CandidateSqlEquivalenceIntegrationTest {
    private val db = PgTestSupport.freshDatabase()

    /** Never contacted: the statements are metadata-only. */
    private val store =
        ObjectStore(
            endpoint = "http://127.0.0.1:9",
            region = "us-east-1",
            accessKey = "unused",
            secretKey = "unused",
            pathStyle = true,
        )

    private val cfg =
        CompactionConfig(
            targetBytes = 1024L * 1024,
            minInputFiles = 5,
            maxInputFiles = 64,
            maxGroupsPerRun = 64,
            maxCandidates = 8_192,
        )
    private val svc by lazy { CompactionService(db.jdbi, store, cfg) }

    private var catalogId = 0L
    private var tableId = 0L
    private var neighbourId = 0L
    private var nextId = 1L

    /** The RESULT cap here, and the default scan cap: small, so bucket D overflows it. */
    private val limit = 40

    data class Row(
        val dataFileId: Long,
        val dvId: Long?,
        val size: Long,
    )

    @AfterAll
    fun tearDown() {
        store.close()
        db.close()
    }

    @BeforeAll
    fun seed() {
        catalogId =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "INSERT INTO hog_catalog (name, data_path, last_snapshot_id) " +
                        "VALUES ('eq', 's3://eq', 0) RETURNING catalog_id",
                ).mapTo(Long::class.java).one()
            }
        db.jdbi.useHandleUnchecked { h ->
            h.execute("INSERT INTO hog_snapshot (catalog_id, snapshot_id, schema_version) VALUES (?, 0, 0)", catalogId)
        }
        val catalogs = CatalogService(db.jdbi)
        catalogs.createNamespace("eq", "ns")
        for (name in listOf("t1", "t2")) {
            catalogs.createTable(
                "eq",
                "ns",
                name,
                listOf(ColumnDef("team_id", ColType.LONG), ColumnDef("month", ColType.INT)),
            )
        }
        tableId = tableIdOf("t1")
        neighbourId = tableIdOf("t2")
        db.jdbi.useHandleUnchecked { h ->
            for ((t, specs) in listOf(tableId to listOf(1L, 2L), neighbourId to listOf(1L))) {
                for (s in specs) {
                    h.execute(
                        "INSERT INTO hog_partition_spec (catalog_id, table_id, spec_id, begin_snapshot) " +
                            "VALUES (?, ?, ?, 0)",
                        catalogId,
                        t,
                        s,
                    )
                    for ((key, source) in listOf(0 to 1, 1 to 2)) {
                        h.execute(
                            "INSERT INTO hog_partition_field " +
                                "(catalog_id, table_id, spec_id, key_index, source_field_id, transform) " +
                                "VALUES (?, ?, ?, ?, ?, 'identity')",
                            catalogId,
                            t,
                            s,
                            key,
                            source,
                        )
                    }
                }
            }
        }
        // Buckets on t1 (spec 1 unless stated):
        //  A  team=1    month=a     30 files, DVs on a third, plus ENDED DVs that must not show
        //  B  team=1    month=b     30 files
        //  C  team=2    month=NULL  30 files (NULL second key)
        //  N  team=NULL month=b     30 files (NULL FIRST key: where the SQL starts)
        //  D  team=3    month=a    200 files (over the scan cap)
        //  E  team=1    month=a     20 files on SPEC 2 (same tuple, other spec)
        //  T  tuple-less             25 files, spec NULL, no partition values
        //  plus 10 ended and 10 over-target files in bucket A (excluded)
        // Neighbour t2: team=1 month=a 40 files, team=3 month=a 40 files.
        seedBucket(tableId, 1L, "1", "a", 30, dvEvery = 3, endedDvEvery = 7)
        seedBucket(tableId, 1L, "1", "b", 30)
        seedBucket(tableId, 1L, "2", null, 30, dvEvery = 5)
        seedBucket(tableId, 1L, null, "b", 30)
        seedBucket(tableId, 1L, "3", "a", 200, dvEvery = 4)
        seedBucket(tableId, 2L, "1", "a", 20)
        seedBucket(tableId, null, null, null, 25, tupleLess = true, dvEvery = 2)
        seedBucket(tableId, 1L, "1", "a", 10, ended = true)
        seedBucket(tableId, 1L, "1", "a", 10, overTarget = true)
        seedBucket(neighbourId, 1L, "1", "a", 40, dvEvery = 2)
        seedBucket(neighbourId, 1L, "3", "a", 40)
        db.jdbi.useHandleUnchecked { h ->
            for (r in listOf("hog_data_file", "hog_file_partition_value", "hog_delete_file")) {
                h.execute("VACUUM (ANALYZE) $r")
            }
        }
    }

    private fun seedBucket(
        table: Long,
        spec: Long?,
        team: String?,
        month: String?,
        n: Int,
        dvEvery: Int = 0,
        endedDvEvery: Int = 0,
        tupleLess: Boolean = false,
        ended: Boolean = false,
        overTarget: Boolean = false,
    ) {
        db.jdbi.useHandleUnchecked { h ->
            repeat(n) { i ->
                val id = nextId++
                val size = if (overTarget) cfg.targetBytes + i else 1000L + (id * 7919) % 4096
                h.createUpdate(
                    """
                    INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot, end_snapshot,
                                               path, record_count, file_size_bytes, row_id_start, spec_id)
                    VALUES (:c, :id, :t, 0, :end, 's3://eq/' || :id || '.parquet', 100, :size, :id * 1000, :spec)
                    """,
                ).bind("c", catalogId).bind("id", id).bind("t", table)
                    .bindByType("end", if (ended) 5L else null, Long::class.javaObjectType)
                    .bind("size", size).bindByType("spec", spec, Long::class.javaObjectType).execute()
                if (!tupleLess) {
                    for ((k, v) in listOf(0 to team, 1 to month)) {
                        h.createUpdate(
                            "INSERT INTO hog_file_partition_value (catalog_id, data_file_id, key_index, value) " +
                                "VALUES (:c, :id, :k, :v)",
                        ).bind("c", catalogId).bind("id", id).bind("k", k)
                            .bindByType("v", v, String::class.java).execute()
                    }
                }
                if (dvEvery > 0 && i % dvEvery == 0) insertDv(h, table, id, endedDv = false)
                if (endedDvEvery > 0 && i % endedDvEvery == 0) insertDv(h, table, id, endedDv = true)
            }
        }
    }

    private fun insertDv(
        h: Handle,
        table: Long,
        dataFileId: Long,
        endedDv: Boolean,
    ) {
        val dvId = 1_000_000L + nextId++
        h.createUpdate(
            """
            INSERT INTO hog_delete_file (catalog_id, delete_file_id, table_id, data_file_id, begin_snapshot,
                                         end_snapshot, path, delete_count, file_size_bytes)
            VALUES (:c, :dv, :t, :f, 1, :end, 's3://eq/dv-' || :dv || '.puffin', 3, 100)
            """,
        ).bind("c", catalogId).bind("dv", dvId).bind("t", table).bind("f", dataFileId)
            .bindByType("end", if (endedDv) 2L else null, Long::class.javaObjectType).execute()
    }

    private fun tableIdOf(name: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT tv.table_id FROM hog_table_version tv WHERE tv.catalog_id = :c AND tv.name = :n")
                .bind("c", catalogId).bind("n", name).mapTo(Long::class.java).one()
        }

    private fun row(
        id: Long,
        dv: Any?,
        size: Long,
    ) = Row(id, dv?.let { (it as Number).toLong() }, size)

    private fun run(
        sql: String,
        values: List<String?>,
        specId: Long?,
        scanLimit: Int,
        customOnly: Boolean,
    ): List<Row> =
        db.jdbi.withHandleUnchecked { h ->
            val q =
                h.createQuery(sql)
                    .bind("catalogId", catalogId).bind("tableId", tableId)
                    .bind("targetBytes", cfg.targetBytes)
                    .bindByType("specId", specId, Long::class.javaObjectType)
                    .bind("scanLimit", scanLimit).bind("limit", limit.coerceAtMost(scanLimit))
            values.forEachIndexed { k, v ->
                q.bind("k$k", k)
                if (v != null) q.bind("v$k", v)
            }
            if (customOnly) q.addCustomizer(Pg.CUSTOM_PLANS_ONLY)
            q.map { rs, _ -> row(rs.getLong("data_file_id"), rs.getObject("dv_id"), rs.getLong("file_size_bytes")) }
                .list()
        }

    /** The bucket's arms as plain correlated filters, for the two independent counts below. */
    private fun truthArms(values: List<String?>): String =
        values.mapIndexed { k, v ->
            val vp = if (v == null) "pv.value IS NULL" else "pv.value = '$v'"
            "AND EXISTS (SELECT 1 FROM hog_file_partition_value pv WHERE pv.catalog_id = f.catalog_id " +
                "AND pv.data_file_id = f.data_file_id AND pv.key_index = $k AND $vp)"
        }.joinToString(" ")

    /** Ground truth, computed independently of either statement: no cap, no index, no array. */
    private fun truth(
        values: List<String?>,
        specId: Long?,
    ): Set<Row> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT f.data_file_id, dv.delete_file_id AS dv_id, f.file_size_bytes
                  FROM hog_data_file f
                  LEFT JOIN hog_delete_file dv
                    ON dv.catalog_id = f.catalog_id AND dv.data_file_id = f.data_file_id
                   AND dv.end_snapshot IS NULL
                 WHERE f.catalog_id = :c AND f.table_id = :t AND f.end_snapshot IS NULL
                   AND f.file_size_bytes < :b
                   AND f.spec_id IS NOT DISTINCT FROM :s ${truthArms(values)}
                """,
            ).bind("c", catalogId).bind("t", tableId).bind("b", cfg.targetBytes)
                .bindByType("s", specId, Long::class.javaObjectType)
                .map { rs, _ -> row(rs.getLong("data_file_id"), rs.getObject("dv_id"), rs.getLong("file_size_bytes")) }
                .toSet()
        }

    /**
     * Rows the REPLACED statement's inner cap counted: the tuple under
     * EVERY spec, because its spec filter sat outside the cap.
     */
    private fun crossSpecCount(values: List<String?>): Int =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT count(*) FROM hog_data_file f WHERE f.catalog_id = :c AND f.table_id = :t " +
                    "AND f.end_snapshot IS NULL AND f.file_size_bytes < :b ${truthArms(values)}",
            ).bind("c", catalogId).bind("t", tableId).bind("b", cfg.targetBytes).mapTo(Int::class.java).one()
        }

    private fun check(
        label: String,
        values: List<String?>,
        specId: Long?,
        scanLimit: Int = limit,
    ) {
        val arms = values.indices.toList()
        val nullArms = values.indices.filter { values[it] == null }.toSet()
        val newSql = svc.candidateSql(arms = arms, nullArms = nullArms, scopeToSpec = true, withPartitionValues = false)
        val oldSql = ReplacedCandidateSql.sql(arms, nullArms)
        val expected = truth(values, specId)
        val cap = limit.coerceAtMost(scanLimit)
        // The replaced statement was exact only when every spec's files
        // for the tuple fit its cap. The no-arm probe still filters spec
        // outside the cap — by design, see `bucketCandidates` — so it is
        // held to the same condition as the old one.
        val oldExact = crossSpecCount(values) <= scanLimit
        val newExact = if (arms.isEmpty()) oldExact else true
        for (customOnly in listOf(true, false)) {
            // Seven uncustomized runs on one handle: past pgjdbc's fifth
            // execution the statement is server-prepared and Postgres may
            // plan it generic, so the generic plan's answer is checked too.
            repeat(if (customOnly) 1 else 7) {
                val n = run(newSql, values, specId, scanLimit, customOnly)
                val o = run(oldSql, values, specId, scanLimit, customOnly)
                val tag = "$label custom=$customOnly values=$values spec=$specId scanLimit=$scanLimit"
                if (expected.size <= cap) {
                    if (newExact) {
                        assertThat(n.toSet()).describedAs("NEW vs truth: $tag").isEqualTo(expected)
                    } else {
                        assertThat(expected).describedAs("NEW subset: $tag").containsAll(n)
                    }
                    assertThat(n.map { it.size }).describedAs("sorted by size: $tag").isSorted()
                    if (oldExact) {
                        assertThat(o.toSet()).describedAs("OLD vs truth: $tag").isEqualTo(expected)
                        assertThat(n).describedAs("NEW vs OLD (ordered): $tag").isEqualTo(o)
                    } else {
                        // RECORDED: the replaced statement's cross-spec
                        // under-fetch. Its rows are still bucket members.
                        assertThat(expected).describedAs("OLD subset: $tag").containsAll(o)
                        println("RECORDED spec under-fetch $tag: truth=${expected.size} new=${n.size} old=${o.size}")
                    }
                } else {
                    // Over the cap: exactly the cap, every row a member of
                    // the bucket. WHICH members is not a property either
                    // statement promises.
                    assertThat(n).describedAs("NEW over-cap size: $tag").hasSize(cap)
                    assertThat(o).describedAs("OLD over-cap size: $tag").hasSize(cap)
                    assertThat(expected).describedAs("NEW over-cap subset: $tag").containsAll(n)
                    assertThat(expected).describedAs("OLD over-cap subset: $tag").containsAll(o)
                }
            }
        }
    }

    @Test
    fun `two arms`() {
        check("A", listOf("1", "a"), 1L)
        check("B", listOf("1", "b"), 1L)
        check("C null month", listOf("2", null), 1L)
        check("N null team (first key)", listOf(null, "b"), 1L)
        check("D over cap", listOf("3", "a"), 1L)
        check("D under cap", listOf("3", "a"), 1L, scanLimit = 1000)
        // MUTATION: move the spec predicate back outside the scan cap and
        // this fails, as does `team 1 spec 2` below: spec 2's 20 files
        // compete with spec 1's for a cap of 40, the under-fetch the
        // replaced statement records here (truth 20, old 10).
        check("E spec 2", listOf("1", "a"), 2L)
        check("E spec 2 under cap", listOf("1", "a"), 2L, scanLimit = 1000)
        check("A under cap", listOf("1", "a"), 1L, scanLimit = 1000)
        check("A wrong spec", listOf("1", "a"), 3L)
        check("absent", listOf("99", "a"), 1L)
        check("null null", listOf(null, null), 1L)
    }

    @Test
    fun `one arm`() {
        check("team 1 (two months, spec 2 excluded)", listOf("1"), 1L)
        check("team 1 spec 2", listOf("1"), 2L)
        check("team 1 spec 2 under cap", listOf("1"), 2L, scanLimit = 1000)
        check("team 1 under cap", listOf("1"), 1L, scanLimit = 1000)
        check("team 2", listOf("2"), 1L)
        check("team NULL", listOf(null), 1L)
        check("team 3 over cap", listOf("3"), 1L)
        check("team 3 under cap", listOf("3"), 1L, scanLimit = 1000)
    }

    @Test
    fun `zero arms`() {
        // Tuple-less bucket: spec NULL, scan cap maxCandidates, as
        // `bucketCandidates` binds it.
        check("tuple-less", emptyList(), null, scanLimit = cfg.maxCandidates)
        check("spec 1 no arms", emptyList(), 1L, scanLimit = cfg.maxCandidates)
        check("spec 2 no arms", emptyList(), 2L, scanLimit = cfg.maxCandidates)
    }

    @Test
    fun `the second key's arm is load-bearing`() {
        // POLARITY for mutation (b): with only the first key, (1, a)
        // would also return (1, b)'s files — so the equality above for A
        // can fail, and is not true of any statement that drops a key.
        val a = truth(listOf("1", "a"), 1L)
        val team1 = truth(listOf("1"), 1L)
        assertThat(team1.size).isGreaterThan(a.size)
    }
}
