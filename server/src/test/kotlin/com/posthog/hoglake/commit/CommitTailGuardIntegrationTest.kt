package com.posthog.hoglake.commit

import com.posthog.hoglake.Database
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.jdbi.v3.core.statement.SqlLogger
import org.jdbi.v3.core.statement.StatementContext
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * The commit tail's write ([CommitTail.WRITE_SQL]) publishes ids the
 * READ computed, so it carries the values it computed them from as
 * guards: the allocator's `last_snapshot_id` / `next_file_id` and each
 * appended table's `next_row_id`. Under the commit lock nothing can move
 * them; these tests move them anyway — from another connection, between
 * the two statements — and require the commit to fail whole rather than
 * publish snapshot and file ids the allocator has already moved past, or
 * a row-id range that overlaps the mover's.
 *
 * MUTATION: drop any guard from WRITE_SQL (`AND last_snapshot_id =
 * :head`, `AND next_file_id = :firstFileId`, `AND ts.next_row_id =
 * u.from_row_id`) and the matching test reds.
 *
 * The rest are the READ's own predicates whose violating state the
 * other suites never build, each pinned by the state it defends against:
 * a dropped table whose version row was left open, a renamed-away name,
 * stats naming a dropped column after the drop, two live specs.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CommitTailGuardIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val alter = AlterService(db.jdbi)
    private val counter = AtomicInteger(0)

    @AfterAll
    fun tearDown() = db.close()

    private fun fixture(): String {
        val cat = "guard-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        return cat
    }

    private fun append(cat: String) =
        CommitRequest(
            appends =
                listOf(
                    TableAppend(
                        "ns",
                        "t",
                        listOf(
                            FileRegistration(
                                "s3://bucket/$cat/data/${java.util.UUID.randomUUID()}.parquet",
                                10,
                                1000,
                                20,
                            ),
                        ),
                    ),
                ),
        )

    /**
     * A CommitService whose read statement is followed, on another
     * connection and before the write, by [between] — a writer that
     * ignored the commit lock.
     */
    private fun commitsWith(between: String): CommitService {
        val fired = AtomicBoolean()
        val instrumented = Database.jdbi(db.dataSource)
        instrumented.setSqlLogger(
            object : SqlLogger {
                override fun logAfterExecution(context: StatementContext) {
                    if (context.rawSql == CommitTail.READ_SQL && fired.compareAndSet(false, true)) {
                        // Another thread, so the statement runs on a connection
                        // of its own and commits before the write starts.
                        Executors.newSingleThreadExecutor().use { executor ->
                            executor.submit { db.jdbi.useHandleUnchecked { h -> h.execute(between) } }
                                .get(10, TimeUnit.SECONDS)
                        }
                    }
                }
            },
        )
        return CommitService(instrumented)
    }

    private fun counts(cat: String): Pair<Long, Long> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT (SELECT count(*) FROM hog_snapshot s WHERE s.catalog_id = c.catalog_id),
                       (SELECT count(*) FROM hog_data_file f WHERE f.catalog_id = c.catalog_id)
                FROM hog_catalog c WHERE c.name = :cat
                """,
            ).bind("cat", cat).map { rs, _ -> rs.getLong(1) to rs.getLong(2) }.one()
        }

    /** The table id of every data file in [cat]. */
    private fun fileTables(cat: String): List<Long> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT f.table_id FROM hog_data_file f JOIN hog_catalog c USING (catalog_id) WHERE c.name = :cat",
            ).bind("cat", cat).mapTo(Long::class.java).list()
        }

    @Test
    fun `a snapshot allocator moved between the read and the write fails the commit whole`() {
        // MUTATION: drop `last_snapshot_id = :head` from `alloc` and the
        // UPDATE allocates head + 2; the count check still refuses that,
        // but as a row-count disagreement rather than the moved allocator
        // it is, and this assertion on the message reds.
        val cat = fixture()
        val before = counts(cat)
        assertThatThrownBy {
            commitsWith("UPDATE hog_catalog SET last_snapshot_id = last_snapshot_id + 1 WHERE name = '$cat'")
                .commit(cat, append(cat))
        }.isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("moved under the commit lock")
        assertThat(counts(cat)).describedAs("snapshots and files after the refused commit").isEqualTo(before)
    }

    @Test
    fun `a file-id allocator moved between the read and the write fails the commit whole`() {
        val cat = fixture()
        val before = counts(cat)
        assertThatThrownBy {
            commitsWith(
                "UPDATE hog_catalog SET next_file_id = next_file_id + 5 WHERE name = '$cat'",
            ).commit(cat, append(cat))
        }.isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("moved under the commit lock")
        assertThat(counts(cat)).describedAs("snapshots and files after the refused commit").isEqualTo(before)
        // And the catalog is not wedged: the next commit reads the moved allocator.
        CommitService(db.jdbi).commit(cat, append(cat))
        assertThat(counts(cat)).isEqualTo(before.first + 1 to before.second + 1)
    }

    @Test
    fun `a row-id allocator moved between the read and the write fails the commit whole`() {
        val cat = fixture()
        val before = counts(cat)
        assertThatThrownBy {
            commitsWith(
                "UPDATE hog_table_stats SET next_row_id = next_row_id + 7 " +
                    "WHERE catalog_id = (SELECT catalog_id FROM hog_catalog WHERE name = '$cat')",
            ).commit(cat, append(cat))
        }.isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("moved under the commit lock")
        assertThat(counts(cat)).describedAs("snapshots and files after the refused commit").isEqualTo(before)
    }

    @Test
    fun `a dropped table whose version row was left open is not committed into`() {
        // Invariant 11: the table row is the authority. `markDropped` ends
        // the version row too, so on well-formed data the read's
        // `tv.end_snapshot IS NULL` already misses a dropped table; this
        // is the state where it cannot (a row a future change leaves open,
        // or one repaired by hand), and `t.dropped_snapshot IS NULL` is all
        // that stands between the commit and a dropped table's files.
        // MUTATION: drop that clause from READ_SQL's `resolved` and the
        // commit publishes into the dropped table.
        val cat = fixture()
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_table SET dropped_snapshot = 1 " +
                    "WHERE catalog_id = (SELECT catalog_id FROM hog_catalog WHERE name = '$cat')",
            )
        }
        val before = counts(cat)
        assertThatThrownBy { CommitService(db.jdbi).commit(cat, append(cat)) }
            .isInstanceOf(com.posthog.hoglake.model.HoglakeException.Validation::class.java)
            .hasMessageContaining("unknown table ns.t")
        assertThat(counts(cat)).isEqualTo(before)
    }

    @Test
    fun `a renamed-away name does not resolve, and a new table under it is the one committed to`() {
        // The read's `tv.end_snapshot IS NULL`: a rename closes t's version
        // row and opens u's. MUTATION: drop that clause and the closed row
        // resolves "t" to the renamed table — the keyless commit below
        // publishes into u — and once a new t exists the name resolves
        // to two tables.
        val cat = fixture()
        alter.alterTable(cat, "ns", "t", listOf(AlterOp.RenameTable("u")))
        val before = counts(cat)
        assertThatThrownBy { CommitService(db.jdbi).commit(cat, append(cat)) }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("unknown table ns.t")
        assertThat(counts(cat)).isEqualTo(before)
        val fresh = catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG))).tableId
        CommitService(db.jdbi).commit(cat, append(cat))
        assertThat(fileTables(cat)).describedAs("the table every file landed in").containsExactly(fresh)
    }

    @Test
    fun `stats naming a column dropped before the read snapshot are refused`() {
        // The read's `columns` filter, `c.end_snapshot IS NULL`. With a
        // read_snapshot from BEFORE the drop the DDL arm refuses first
        // (AppendReadSnapshotGuardIntegrationTest); from after it, the
        // live-column filter is all that refuses stats for a field the
        // table no longer has. MUTATION: drop that clause and they are
        // stored.
        val cat = fixture()
        catalogs.createTable(cat, "ns", "wide", listOf(ColumnDef("id", ColType.LONG), ColumnDef("gone", ColType.LONG)))
        val gone = catalogs.getTable(cat, "ns", "wide", totals = false).columns.single { it.def.name == "gone" }.fieldId
        alter.alterTable(cat, "ns", "wide", listOf(AlterOp.DropColumn("gone")))
        val before = counts(cat)
        val file =
            FileRegistration(
                "s3://bucket/$cat/data/${java.util.UUID.randomUUID()}.parquet",
                10,
                1000,
                20,
                listOf(ColumnStats(gone, 10, 0, null, 80, null, null)),
            )
        assertThatThrownBy {
            CommitService(db.jdbi).commit(
                cat,
                CommitRequest(
                    readSnapshot = catalogs.getCatalog(cat).headSnapshotId,
                    appends = listOf(TableAppend("ns", "wide", listOf(file))),
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("unknown field_id $gone")
        assertThat(counts(cat)).isEqualTo(before)
    }

    @Test
    fun `two live partition specs on one table are refused, not published under either`() {
        // No index makes a table's live spec unique, so this is reachable
        // only through a bug; the per-table lookup refused it through
        // `findOne`, and the read's LIMIT 2 is what keeps it visible.
        val cat = fixture()
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                """
                INSERT INTO hog_partition_spec (catalog_id, table_id, spec_id, begin_snapshot)
                SELECT t.catalog_id, t.table_id, s, 0 FROM hog_table t JOIN hog_catalog c USING (catalog_id),
                       generate_series(1, 2) s
                WHERE c.name = '$cat'
                """,
            )
        }
        val before = counts(cat)
        assertThatThrownBy { CommitService(db.jdbi).commit(cat, append(cat)) }
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("more than one live partition spec")
        assertThat(counts(cat)).isEqualTo(before)
    }
}
