package com.posthog.hoglake.service

import com.posthog.hoglake.model.DatabaseActivity
import com.posthog.hoglake.model.DatabaseIndex
import com.posthog.hoglake.model.DatabaseServer
import com.posthog.hoglake.model.DatabaseTable
import com.posthog.hoglake.model.FindingSeverity
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant

/**
 * The findings engine, against synthetic statistics.
 *
 * These are the assertions that matter for this page: the raw numbers
 * are Postgres' own and need no test, while a finding that fires when
 * nothing is wrong trains an operator to ignore the page, and one that
 * stays silent when something is wrong is worse than no page at all.
 *
 * A healthy baseline is the control for every case — each test perturbs
 * exactly one input, so a finding appearing for the wrong reason shows
 * up as the baseline test failing rather than as a passing assertion
 * somewhere else.
 */
class DatabaseHealthFindingsTest {
    private val service = DatabaseHealthService(jdbi = org.jdbi.v3.core.Jdbi.create { error("unused") })

    private fun server(
        xidAge: Long = 1_000_000,
        autovacuum: Boolean = true,
        cacheHit: Double? = 0.999,
        connectionsUsed: Int = 10,
        deadlocks: Long = 0,
    ) = DatabaseServer(
        version = "16.13",
        database = "hoglake",
        sizeBytes = 100L * 1024 * 1024 * 1024,
        startedAt = Instant.parse("2026-09-01T00:00:00Z"),
        connectionsUsed = connectionsUsed,
        connectionsMax = 100,
        cacheHitRatio = cacheHit,
        deadlocks = deadlocks,
        committed = 1_000_000,
        rolledBack = 12,
        xidAge = xidAge,
        xidFreezeMaxAge = 200_000_000,
        autovacuumEnabled = autovacuum,
    )

    private fun activity(
        idleInTransaction: Int = 0,
        longestTransaction: Double? = 0.4,
        longestIdleInTransaction: Double? = null,
        longestWait: Double? = null,
        waiting: Int = 0,
    ) = DatabaseActivity(
        active = 2,
        idle = 8,
        idleInTransaction = idleInTransaction,
        waiting = waiting,
        longestTransactionSeconds = longestTransaction,
        longestIdleInTransactionSeconds = longestIdleInTransaction,
        longestWaitSeconds = longestWait,
    )

    /** A big, healthy manifest table: vacuumed, analyzed, index-scanned. */
    private fun table(
        name: String = "hog_data_file",
        live: Long = 2_000_000,
        dead: Long = 1_000,
        bytes: Long = 4L * 1024 * 1024 * 1024,
        seqScans: Long = 2,
        indexScans: Long = 5_000_000,
        analyzed: Instant? = Instant.parse("2026-09-17T00:00:00Z"),
    ) = DatabaseTable(
        name = name,
        liveTuples = live,
        deadTuples = dead,
        tableBytes = bytes,
        indexBytes = bytes / 4,
        toastBytes = 0,
        seqScans = seqScans,
        indexScans = indexScans,
        lastVacuum = null,
        lastAutovacuum = Instant.parse("2026-09-17T12:00:00Z"),
        lastAnalyze = null,
        lastAutoanalyze = analyzed,
        autovacuumCount = 40,
    )

    private fun index(
        name: String = "hog_data_file_live",
        scans: Long = 5_000_000,
        bytes: Long = 512L * 1024 * 1024,
        constraintBacking: Boolean = false,
    ) = DatabaseIndex("hog_data_file", name, bytes, scans, constraintBacking)

    private fun codes(
        server: DatabaseServer = server(),
        activity: DatabaseActivity = activity(),
        tables: List<DatabaseTable> = listOf(table()),
        indexes: List<DatabaseIndex> = listOf(index()),
    ) = service.findings(server, activity, tables, indexes).map { it.code }

    @Test
    fun `a healthy instance produces no findings`() {
        assertThat(codes()).isEmpty()
    }

    @Test
    fun `autovacuum off is critical`() {
        val findings = service.findings(server(autovacuum = false), activity(), listOf(table()), listOf(index()))
        assertThat(findings.map { it.code }).contains("autovacuum_disabled")
        assertThat(findings.single { it.code == "autovacuum_disabled" }.severity)
            .isEqualTo(FindingSeverity.CRITICAL)
    }

    @Test
    fun `xid age escalates from warn to critical`() {
        // 200M freeze limit: 40% quiet, 60% warn, 90% critical.
        assertThat(codes(server = server(xidAge = 80_000_000))).doesNotContain("xid_wraparound")
        val warn = service.findings(server(xidAge = 120_000_000), activity(), listOf(table()), listOf(index()))
        assertThat(warn.single { it.code == "xid_wraparound" }.severity).isEqualTo(FindingSeverity.WARN)
        val critical = service.findings(server(xidAge = 180_000_000), activity(), listOf(table()), listOf(index()))
        assertThat(critical.single { it.code == "xid_wraparound" }.severity).isEqualTo(FindingSeverity.CRITICAL)
    }

    @Test
    fun `idle in transaction escalates, and a brief one is quiet`() {
        assertThat(codes(activity = activity(idleInTransaction = 1, longestIdleInTransaction = 5.0)))
            .doesNotContain("idle_in_transaction")
        val warn =
            service.findings(
                server(),
                activity(idleInTransaction = 1, longestIdleInTransaction = 90.0),
                listOf(table()),
                listOf(index()),
            )
        assertThat(warn.single { it.code == "idle_in_transaction" }.severity).isEqualTo(FindingSeverity.WARN)
        val critical =
            service.findings(
                server(),
                activity(idleInTransaction = 1, longestIdleInTransaction = 600.0),
                listOf(table()),
                listOf(index()),
            )
        assertThat(critical.single { it.code == "idle_in_transaction" }.severity)
            .isEqualTo(FindingSeverity.CRITICAL)
    }

    @Test
    fun `a maintenance-length transaction is quiet and a stuck one is not`() {
        // Expiry and compaction sweeps legitimately run for minutes.
        assertThat(codes(activity = activity(longestTransaction = 120.0))).doesNotContain("long_transaction")
        assertThat(codes(activity = activity(longestTransaction = 900.0))).contains("long_transaction")
    }

    @Test
    fun `dead tuples escalate, and only on tables big enough to matter`() {
        assertThat(codes(tables = listOf(table(live = 700_000, dead = 300_000)))).contains("dead_tuples")
        assertThat(
            service.findings(
                server(),
                activity(),
                listOf(table(live = 500_000, dead = 500_000)),
                listOf(index()),
            ).single { it.code == "dead_tuples" }.severity,
        ).isEqualTo(FindingSeverity.CRITICAL)
        // The same ratio on a small table is noise, not a finding.
        assertThat(codes(tables = listOf(table(live = 5, dead = 5, bytes = 8 * 1024))))
            .doesNotContain("dead_tuples")
    }

    @Test
    fun `the dead-tuple impact names why THIS table matters`() {
        // The interpretation is the product here; a generic string would
        // make the whole findings layer pointless.
        fun impactFor(name: String) =
            service.findings(
                server(),
                activity(),
                listOf(table(name = name, live = 600_000, dead = 400_000)),
                listOf(index()),
            ).single { it.code == "dead_tuples" }.hoglakeImpact

        assertThat(impactFor("hog_data_file")).contains("end_snapshot IS NULL")
        assertThat(impactFor("hog_file_column_stats")).contains("per file, per column")
        assertThat(impactFor("hog_snapshot")).contains("changefeed")
        assertThat(impactFor("hog_view")).isNotBlank()
    }

    @Test
    fun `an unused index is reported, but never a constraint-backing one`() {
        assertThat(codes(indexes = listOf(index(name = "hog_data_file_changefeed", scans = 0))))
            .contains("unused_indexes")
        // A unique index is doing its job whether or not anything scans
        // it; advising its removal would break the schema.
        assertThat(codes(indexes = listOf(index(name = "hog_catalog_pkey", scans = 0, constraintBacking = true))))
            .doesNotContain("unused_indexes")
        // Neither is a tiny index worth an operator's attention.
        assertThat(codes(indexes = listOf(index(name = "small_idx", scans = 0, bytes = 16 * 1024))))
            .doesNotContain("unused_indexes")
    }

    @Test
    fun `sequential scans are flagged only with enough samples to mean it`() {
        assertThat(codes(tables = listOf(table(seqScans = 900, indexScans = 100)))).contains("sequential_scans")
        // A handful of scans on a fresh table proves nothing either way.
        assertThat(codes(tables = listOf(table(seqScans = 9, indexScans = 1)))).doesNotContain("sequential_scans")
    }

    @Test
    fun `a never-analyzed table with real rows is flagged`() {
        assertThat(codes(tables = listOf(table(analyzed = null)))).contains("never_analyzed")
        // An empty table has nothing to analyze.
        assertThat(codes(tables = listOf(table(live = 3, analyzed = null)))).doesNotContain("never_analyzed")
    }

    @Test
    fun `cache, connections and deadlocks are each reported`() {
        assertThat(codes(server = server(cacheHit = 0.80))).contains("cache_hit_ratio")
        assertThat(codes(server = server(cacheHit = null))).doesNotContain("cache_hit_ratio")
        assertThat(codes(server = server(connectionsUsed = 95))).contains("connection_saturation")
        assertThat(codes(server = server(deadlocks = 3))).contains("deadlocks")
    }

    @Test
    fun `lock waits are quiet at commit-tail durations`() {
        // Commits serialize on the advisory lock by design; a short wait
        // IS the system working, and flagging it would be noise.
        assertThat(codes(activity = activity(waiting = 3, longestWait = 0.05))).doesNotContain("lock_wait")
        assertThat(codes(activity = activity(waiting = 3, longestWait = 45.0))).contains("lock_wait")
    }

    @Test
    fun `findings are ordered worst-first`() {
        val findings =
            service.findings(
                server(autovacuum = false, deadlocks = 1),
                activity(),
                listOf(table()),
                listOf(index(scans = 0)),
            )
        assertThat(findings.map { it.severity })
            .isSortedAccordingTo(compareByDescending { it.ordinal })
        assertThat(findings.first().severity).isEqualTo(FindingSeverity.CRITICAL)
    }

    @Test
    fun `every finding explains itself`() {
        val findings =
            service.findings(
                server(autovacuum = false, xidAge = 190_000_000, cacheHit = 0.5, connectionsUsed = 99, deadlocks = 2),
                activity(
                    idleInTransaction = 2,
                    longestTransaction = 9000.0,
                    longestIdleInTransaction = 8000.0,
                    waiting = 4,
                    longestWait = 60.0,
                ),
                listOf(table(live = 100_000, dead = 900_000, analyzed = null, seqScans = 5000, indexScans = 1)),
                listOf(index(scans = 0)),
            )
        assertThat(findings).isNotEmpty()
        assertThat(findings.map { it.code }).doesNotHaveDuplicates()
        for (finding in findings) {
            assertThat(finding.title).describedAs("title of ${finding.code}").isNotBlank()
            assertThat(finding.detail).describedAs("detail of ${finding.code}").isNotBlank()
            assertThat(finding.hoglakeImpact).describedAs("impact of ${finding.code}").isNotBlank()
        }
    }
}
