package com.posthog.hoglake.service

import com.posthog.hoglake.Database
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.jdbi.v3.core.statement.SqlLogger
import org.jdbi.v3.core.statement.StatementContext
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.CopyOnWriteArrayList

/**
 * THE PRODUCTION FAILURE, at a size where it reproduces (#193).
 *
 * `DROP TABLE main.events_raw` on gigahog-prod-us could not complete.
 * The drop end-snapshotted every live file row in the same transaction,
 * under the per-catalog commit lock: 3,008,849 rows measured at 44.6 s
 * and 3.9 GB of WAL warm and uncontended. Past the 60 s session
 * `statement_timeout` it rolls back, so the table stayed undroppable,
 * and for the seconds it did hold the lock every commit in the catalog
 * queued behind it.
 *
 * TWO ASSERTIONS, and they fail for different reasons on purpose:
 *
 *  - a STATEMENT BOUND the old drop could not fit inside. This fixture
 *    is [FILES] rows rather than three million, so the bound is set to
 *    match: the class measures how long the old `UPDATE hog_data_file
 *    SET end_snapshot` takes on THESE rows, and the drop then runs
 *    under a bound well below it. That makes the case reproduce the
 *    production shape at a size a test suite can afford, and the
 *    numbers are printed rather than assumed;
 *  - a STATEMENT CAPTURE. The drop is watched with a JDBI `SqlLogger`
 *    and asserted to issue NOTHING that writes `hog_data_file` or
 *    `hog_delete_file`. That half is size-independent and reds
 *    instantly under the mutation, where the timing half would only
 *    red on a fixture big enough.
 *
 * The rest is the SEMANTICS the change must not have moved: head reads
 * 404, a read below the drop still sees every file, and a commit into
 * the dropped table gets a typed refusal rather than "unknown table".
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DropAtScaleIntegrationTest {
    private companion object {
        /**
         * Live file rows on the table being dropped.
         *
         * Production is 3,008,849. This is the SMALLEST size at which
         * the old drop's single UPDATE still takes multiple seconds —
         * comfortably past the bound below — and it was 750,000 until
         * the cost of the class (22.6 s, the heaviest new one in the
         * suite) stopped being worth three times the margin. The point
         * being pinned is a COMPLEXITY CLASS (O(rows) versus
         * O(columns)), and that is visible at any size where the
         * constant is measurable; the assertion below checks that the
         * fixture really is past the bound, so a future shrink that
         * goes too far fails rather than passing vacuously.
         */
        const val FILES = 250_000

        /**
         * The statement bound the drop runs under, in seconds.
         *
         * Production's is 60 s (`Database.SESSION_INIT_SQL`) against a
         * 44.6 s UPDATE — the margin that made the drop a coin flip.
         * Here the bound is deliberately far BELOW the measured cost of
         * the same UPDATE on this fixture, so the case reproduces "the
         * bound fires" rather than "the bound nearly fires".
         */
        const val STATEMENT_BOUND_SECONDS = 1
    }

    private val db = PgTestSupport.freshDatabase()
    private val jdbi get() = db.jdbi
    private val catalogs = CatalogService(jdbi)
    private val commits = CommitService(jdbi)

    private var catalogId = 0L
    private var tableId = 0L
    private var seedSnapshot = 0L

    /** How long the OLD drop's one UPDATE takes on this fixture. */
    private var endLiveFilesMillis = 0L
    private var dropMillis = 0L

    @AfterAll
    fun tearDown() = db.close()

    @BeforeAll
    fun seed() {
        catalogs.createCatalog(CATALOG, "s3://bucket/$CATALOG")
        catalogs.createNamespace(CATALOG, "ns")
        catalogs.createTable(CATALOG, "ns", "events_raw", listOf(ColumnDef("id", ColType.LONG, nullable = false)))
        catalogs.createTable(CATALOG, "ns", "keeper", listOf(ColumnDef("id", ColType.LONG, nullable = false)))
        // One real commit, so the table has a registered file that every
        // read path agrees about, then the bulk by SQL: the point is the
        // ROW COUNT a drop has to touch, and 750,000 commits would be a
        // different test entirely.
        seedSnapshot =
            commits.commit(
                CATALOG,
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "events_raw",
                                listOf(FileRegistration("s3://bucket/$CATALOG/registered.parquet", 10, 100)),
                            ),
                        ),
                ),
            ).snapshotId
        catalogId = catalogs.getCatalog(CATALOG).catalogId
        tableId =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT table_id FROM hog_table_version WHERE catalog_id = :c AND name = 'events_raw' " +
                        "AND end_snapshot IS NULL",
                ).bind("c", catalogId).mapTo(Long::class.java).one()
            }
        jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           path, record_count, file_size_bytes, row_id_start)
                SELECT :c, 1000000 + g, :t, :s,
                       's3://bucket/$CATALOG/events_raw/part-' || g || '.parquet', 120000, 268435456,
                       g::bigint * 120000
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("t", tableId).bind("s", seedSnapshot).bind("n", FILES).execute()
            h.execute("VACUUM (ANALYZE) hog_data_file")
        }

        // THE COST THE OLD DROP PAID, measured on these exact rows and
        // then rolled back. Not a claim about production hardware — a
        // calibration of the bound this case runs the drop under.
        endLiveFilesMillis =
            jdbi.withHandleUnchecked { h ->
                h.begin()
                val start = System.nanoTime()
                h.createUpdate(
                    "UPDATE hog_data_file SET end_snapshot = 999999 " +
                        "WHERE catalog_id = :c AND table_id = :t AND end_snapshot IS NULL",
                ).bind("c", catalogId).bind("t", tableId).execute()
                val elapsed = (System.nanoTime() - start) / 1_000_000
                h.rollback()
                elapsed
            }
        println(
            "[#193] endLiveFiles over $FILES rows: ${endLiveFilesMillis}ms " +
                "(${"%.1f".format(endLiveFilesMillis * 1000.0 / FILES)} us/row); " +
                "the drop below runs under a ${STATEMENT_BOUND_SECONDS}s statement bound",
        )

        // Every NEW connection now carries the bound; the pool's idle
        // ones are evicted so the drop cannot get one that predates it.
        val database = db.jdbcUrl.substringAfterLast('/').substringBefore('?')
        jdbi.useHandleUnchecked { h ->
            h.execute("ALTER DATABASE $database SET statement_timeout = '${STATEMENT_BOUND_SECONDS}s'")
        }
        db.dataSource.hikariPoolMXBean.softEvictConnections()
        jdbi.useHandleUnchecked { h ->
            val effective =
                h.createQuery("SHOW statement_timeout").mapTo(String::class.java).one()
            check(effective == "${STATEMENT_BOUND_SECONDS}s") {
                "the bound this case exists to run under is not in force (got '$effective'); " +
                    "without it the drop would pass on a machine of any speed"
            }
        }
    }

    /** Every statement one block of work issued, in order. */
    private fun captured(block: (org.jdbi.v3.core.Jdbi) -> Unit): List<String> {
        val issued = CopyOnWriteArrayList<String>()
        val instrumented = Database.jdbi(db.dataSource)
        instrumented.setSqlLogger(
            object : SqlLogger {
                override fun logAfterExecution(context: StatementContext) {
                    issued += context.renderedSql
                }
            },
        )
        block(instrumented)
        return issued.toList()
    }

    private var dropSnapshot = 0L

    @Test
    fun `dropping a large table is O(columns) - inside a bound the old drop could not fit`() {
        val statements: List<String>
        val start = System.nanoTime()
        statements =
            captured { instrumented ->
                dropSnapshot = CatalogService(instrumented).dropTable(CATALOG, "ns", "events_raw").snapshotId
            }
        dropMillis = (System.nanoTime() - start) / 1_000_000
        println(
            "[#193] dropTable over $FILES live file rows: ${dropMillis}ms " +
                "(the same rows cost ${endLiveFilesMillis}ms to end-snapshot)",
        )

        // The bound did not fire, and the margin is what the change
        // bought: the same rows under the old code took
        // endLiveFilesMillis, which is far past it.
        assertThat(dropMillis)
            .describedAs(
                "the drop must not size with the table: %d ms against %d ms to end-snapshot the " +
                    "same rows",
                dropMillis,
                endLiveFilesMillis,
            )
            .isLessThan(STATEMENT_BOUND_SECONDS * 1000L)
        assertThat(endLiveFilesMillis)
            .describedAs(
                "this fixture must be big enough that the OLD drop would have hit the bound, or " +
                    "the case above passes for the wrong reason",
            )
            .isGreaterThan(STATEMENT_BOUND_SECONDS * 1000L)

        // THE SIZE-INDEPENDENT HALF. MUTATION: restore
        // `FileRepo.endLiveFiles` / `endLiveDeleteFiles` in
        // CatalogService.dropTable and this reds immediately, on any
        // fixture, naming the statement.
        val fileWrites =
            statements.filter { sql ->
                val normalized = sql.uppercase()
                listOf(
                    "UPDATE HOG_DATA_FILE",
                    "UPDATE HOG_DELETE_FILE",
                    "DELETE FROM HOG_DATA_FILE",
                    "DELETE FROM HOG_DELETE_FILE",
                ).any { normalized.contains(it) }
            }
        assertThat(fileWrites)
            .describedAs("a drop must write no file row at all; it issued:%n%s", statements.joinToString("\n---\n"))
            .isEmpty()

        // The rows are all still there, live, waiting for retirement.
        val live =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT count(*) FROM hog_data_file WHERE catalog_id = :c AND table_id = :t " +
                        "AND end_snapshot IS NULL",
                ).bind("c", catalogId).bind("t", tableId).mapTo(Long::class.java).one()
            }
        assertThat(live).isEqualTo(FILES + 1L)
    }

    @Test
    fun `the dropped table is gone at head and intact below the drop`() {
        val drop = dropSnapshotOrDrop()

        assertThatThrownBy { catalogs.getTable(CATALOG, "ns", "events_raw") }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(catalogs.listTables(CATALOG, "ns").map { it.name }).containsExactly("keeper")
        assertThatThrownBy { catalogs.listFiles(CATALOG, "ns", "events_raw") }
            .isInstanceOf(HoglakeException.NotFound::class.java)

        // TIME TRAVEL IS UNCHANGED, and this is the whole reason
        // retirement waits for the expiry floor: below the drop the
        // table and every one of its files is still a legal read.
        val below = catalogs.getTable(CATALOG, "ns", "events_raw", snapshot = drop - 1)
        assertThat(below.fileCount).isEqualTo(FILES + 1L)
        assertThat(catalogs.listFiles(CATALOG, "ns", "events_raw", snapshot = drop - 1, limit = 3))
            .hasSize(3)
        // AT the drop snapshot the version row is already closed.
        assertThatThrownBy { catalogs.getTable(CATALOG, "ns", "events_raw", snapshot = drop) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
    }

    @Test
    fun `a commit into the dropped table is a typed refusal naming the drop snapshot`() {
        val drop = dropSnapshotOrDrop()
        // MUTATION: delete `droppedTableRefusal`'s dropped-table probe
        // and this reds back to the old `Validation("unknown table")`,
        // which a writer cannot tell from a typo.
        assertThatThrownBy {
            commits.commit(
                CATALOG,
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "events_raw",
                                listOf(FileRegistration("s3://bucket/$CATALOG/after-drop.parquet", 1, 1)),
                            ),
                        ),
                ),
            )
        }
            .isInstanceOf(HoglakeException.TableDropped::class.java)
            .hasMessageContaining("dropped in snapshot $drop")

        // A name that never existed is STILL the old validation
        // failure: the two cases must stay distinguishable in both
        // directions, or the new type says nothing.
        assertThatThrownBy {
            commits.commit(
                CATALOG,
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "never_existed",
                                listOf(FileRegistration("s3://bucket/$CATALOG/typo.parquet", 1, 1)),
                            ),
                        ),
                ),
            )
        }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("unknown table")
    }

    /** The drop runs in the first case; the others tolerate any order. */
    private fun dropSnapshotOrDrop(): Long {
        if (dropSnapshot == 0L) {
            dropSnapshot = catalogs.dropTable(CATALOG, "ns", "events_raw").snapshotId
        }
        return dropSnapshot
    }
}

private const val CATALOG = "drop-scale"
