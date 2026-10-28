package com.posthog.hoglake.service

import com.posthog.hoglake.Database
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ChangeKind
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceInfo
import com.posthog.hoglake.model.TableInfo
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.statement.SqlLogger
import org.jdbi.v3.core.statement.StatementContext
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Concurrency regression tests for the idempotent creates. A SqlLogger
 * hook on a second Jdbi schedules a COMPETING service call at an exact
 * statement boundary inside the create under test — the hook never
 * modifies production code or fabricates state, it only interleaves
 * real calls deterministically where threads once raced.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class IdempotentCreateRaceIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val svc = CatalogService(db.jdbi)
    private val alter = AlterService(db.jdbi)
    private val cols = listOf(ColumnDef("id", ColType.LONG))

    @AfterAll
    fun tearDown() = db.close()

    private fun fixture(catalog: String) {
        svc.createCatalog(catalog, "s3://bucket/$catalog")
        svc.createNamespace(catalog, "ns")
        svc.createTable(catalog, "ns", "t", cols)
    }

    /**
     * A CatalogService whose Jdbi runs [hook] (once) when a statement
     * whose SQL contains [marker] is about to execute (or just did).
     */
    private fun instrumented(
        marker: String,
        after: Boolean = false,
        hook: () -> Unit,
    ): CatalogService {
        var fired = false
        val jdbi =
            Database.jdbi(db.dataSource).also { j ->
                j.setSqlLogger(
                    object : SqlLogger {
                        override fun logBeforeExecution(context: StatementContext) {
                            if (!after) maybeFire(context, marker, { fired }, { fired = true }, hook)
                        }

                        override fun logAfterExecution(context: StatementContext) {
                            if (after) maybeFire(context, marker, { fired }, { fired = true }, hook)
                        }
                    },
                )
            }
        return CatalogService(jdbi)
    }

    private fun maybeFire(
        context: StatementContext,
        marker: String,
        fired: () -> Boolean,
        mark: () -> Unit,
        hook: () -> Unit,
    ) {
        if (!fired() && context.rawSql.contains(marker)) {
            mark()
            hook()
        }
    }

    // ---- catalog races ---------------------------------------------------

    @Test
    fun `catalog committed between the pre-check and the overlap scan returns the winner`() {
        // The overlap scan (listAll) must skip same-name rows: the
        // pre-check ran on a different READ COMMITTED statement, so this
        // interleaving used to answer 422 "overlaps" — bypassing the
        // unique-constraint arbitration entirely.
        val raced =
            instrumented("FROM hog_catalog ORDER BY name") {
                svc.createCatalog("race-list", "s3://bucket/race-list")
            }
        val outcome = raced.createCatalogOutcome("race-list", "s3://bucket/race-list")
        assertThat(outcome.created).isFalse()
        assertThat(outcome.value).isEqualTo(svc.getCatalog("race-list"))
    }

    @Test
    fun `catalog committed between the overlap scan and the insert returns the winner`() {
        // The unique-constraint window itself: the loser re-reads on a
        // fresh handle (its own transaction is aborted) and answers the
        // identical winner — no second snapshot 0.
        val raced =
            instrumented("INSERT INTO hog_catalog") {
                svc.createCatalog("race-insert", "s3://bucket/race-insert")
            }
        val outcome = raced.createCatalogOutcome("race-insert", "s3://bucket/race-insert")
        assertThat(outcome.created).isFalse()
        assertThat(outcome.value).isEqualTo(svc.getCatalog("race-insert"))
        assertThat(svc.getCatalog("race-insert").headSnapshotId).isEqualTo(0)
    }

    @Test
    fun `catalog committed between the overlap scan and the insert with a different path stays a 409`() {
        val raced =
            instrumented("INSERT INTO hog_catalog") {
                svc.createCatalog("race-diff", "s3://bucket/race-diff-other")
            }
        assertThatThrownBy { raced.createCatalogOutcome("race-diff", "s3://bucket/race-diff") }
            .isInstanceOf(HoglakeException.AlreadyExists::class.java)
    }

    // ---- table races -----------------------------------------------------

    @Test
    fun `an alter between the identity check and the answer still returns the checked definition`() {
        fixture("race-alter")
        // The hook fires INSIDE the one REPEATABLE READ transaction the
        // check and the answer share: the alter commits after that
        // transaction's snapshot, so the answer is the checked [id]
        // definition, never the altered one.
        val raced =
            instrumented("FROM hog_sort_spec", after = true) {
                alter.alterTable(
                    "race-alter",
                    "ns",
                    "t",
                    listOf(AlterOp.AddColumn(ColumnDef("extra", ColType.STRING))),
                )
            }
        val outcome = raced.createTableOutcome("race-alter", "ns", "t", cols)
        assertThat(outcome.created).isFalse()
        assertThat(outcome.value.columns.map { it.def.name }).containsExactly("id")
        // The alter did land — just after the answer's snapshot.
        assertThat(svc.getTable("race-alter", "ns", "t").columns.map { it.def.name })
            .containsExactly("id", "extra")
    }

    @Test
    fun `a drop and recreate between the identity check and the answer returns the checked incarnation`() {
        fixture("race-replace")
        val original = svc.getTable("race-replace", "ns", "t")
        val raced =
            instrumented("FROM hog_sort_spec", after = true) {
                svc.dropTable("race-replace", "ns", "t", author = "race-test", message = "race test")
                svc.createTable("race-replace", "ns", "t", listOf(ColumnDef("unrelated", ColType.STRING)))
            }
        val outcome = raced.createTableOutcome("race-replace", "ns", "t", cols)
        assertThat(outcome.created).isFalse()
        // The ORIGINAL incarnation — the one the identity check approved.
        assertThat(outcome.value.tableUuid).isEqualTo(original.tableUuid)
        assertThat(outcome.value.columns.map { it.def.name }).containsExactly("id")
    }

    @Test
    fun `concurrent identical namespace and table creates allocate exactly once`() {
        svc.createCatalog("race-parallel", "s3://bucket/race-parallel")
        val pool = Executors.newFixedThreadPool(2)
        try {
            val barrier = CyclicBarrier(2)
            val namespaces =
                (1..2)
                    .map {
                        pool.submit<CreateOutcome<NamespaceInfo>> {
                            barrier.await()
                            svc.createNamespaceOutcome("race-parallel", "ns")
                        }
                    }.map { it.get(15, TimeUnit.SECONDS) }
            assertThat(namespaces.map { it.created }).containsExactlyInAnyOrder(true, false)
            assertThat(namespaces.map { it.value.namespaceId }.distinct()).hasSize(1)

            val tables =
                (1..2)
                    .map {
                        pool.submit<CreateOutcome<TableInfo>> {
                            barrier.await()
                            svc.createTableOutcome("race-parallel", "ns", "t", cols)
                        }
                    }.map { it.get(15, TimeUnit.SECONDS) }
            assertThat(tables.map { it.created }).containsExactlyInAnyOrder(true, false)
            assertThat(tables.map { it.value.tableUuid }.distinct()).hasSize(1)

            // Two snapshots (namespace + table), two change rows: the
            // no-op loser allocated nothing.
            assertThat(svc.getCatalog("race-parallel").headSnapshotId).isEqualTo(2)
            val changes =
                svc.listSnapshots("race-parallel", after = -1, limit = 10).first
                    .flatMap { it.changes }
            assertThat(changes.map { it.kind })
                .containsExactlyInAnyOrder(ChangeKind.NAMESPACE_CREATED, ChangeKind.TABLE_CREATED)
        } finally {
            pool.shutdownNow()
        }
    }
}
