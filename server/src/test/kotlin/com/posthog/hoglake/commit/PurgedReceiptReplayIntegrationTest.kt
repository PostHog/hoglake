package com.posthog.hoglake.commit

import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.UUID

/**
 * WHAT A RECEIPT'S RETENTION BUYS, asserted rather than asserted about.
 *
 * Every number in #240's retention half exists to prevent one outcome —
 * the per-catalog floor, the seven-day default, the 3,600 s boot floor,
 * V24's `NOT NULL DEFAULT now()` grace window — and before this file
 * that outcome was described in prose four times and pinned nowhere. A
 * later reader who weakens any of them sees only the tests that pin the
 * arithmetic, not what the arithmetic is for.
 *
 * THIS IS A DOCUMENTED-BEHAVIOUR TEST, not a defect report. The
 * behaviour below is correct and unavoidable: a commit with an
 * idempotency key whose receipt no longer exists is, to the server,
 * simply a commit. There is no detector here and none is wanted — the
 * only honest fence is that the receipt outlives every payload a client
 * may still hold, which is what `HOGLAKE_RECEIPT_RETENTION_SECONDS`
 * (`Config.receiptRetentionSeconds`, 7 days, floored per catalog in
 * `CleanupService.purgeCommitReceipts`) is. If this test's assertions
 * ever need changing, the change being made is to that contract.
 *
 * The failure it describes is also the QUIETEST one in the system: the
 * commit succeeds, the snapshot is valid, every counter reports success,
 * and the table simply has the rows twice.
 */
@Tag("integration")
class PurgedReceiptReplayIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val jdbi get() = db.jdbi

    @AfterEach
    fun tearDown() = db.close()

    private fun seed() =
        jdbi.useHandleUnchecked { h ->
            val c =
                h.createQuery(
                    "INSERT INTO hog_catalog (name, data_path) VALUES ('cat', 's3://b') RETURNING catalog_id",
                ).mapTo(Long::class.java).one()
            h.createUpdate("INSERT INTO hog_namespace (catalog_id, namespace_id, name) VALUES (?, 0, 'ns')")
                .bind(0, c).execute()
            h.createUpdate(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot, next_field_id) VALUES (?, 0, 0, 2)",
            ).bind(0, c).execute()
            h.createUpdate(
                "INSERT INTO hog_table_version (catalog_id, table_id, begin_snapshot, namespace_id, name) " +
                    "VALUES (?, 0, 0, 0, 'events')",
            ).bind(0, c).execute()
            h.createUpdate(
                "INSERT INTO hog_column (catalog_id, table_id, field_id, begin_snapshot, name, col_type, ordinal) " +
                    "VALUES (?, 0, 1, 0, 'c1', 'long', 0)",
            ).bind(0, c).execute()
            h.createUpdate("INSERT INTO hog_table_stats (catalog_id, table_id) VALUES (?, 0)")
                .bind(0, c).execute()
        }

    @Test
    fun `a replay whose receipt was purged publishes the same files again, as duplicate live rows`() {
        seed()
        val key = UUID.randomUUID()
        val path = "s3://b/data/dup.parquet"
        val request =
            CommitRequest(
                idempotencyKey = key,
                appends = listOf(TableAppend("ns", "events", listOf(FileRegistration(path, 100, 5000, 20)))),
            )
        val first = CommitService(jdbi).commit("cat", request)

        // The purge, exactly: the row is gone and nothing else changed.
        // (Doing it by hand rather than through CleanupService keeps this
        // test about the CONSEQUENCE; the purge's own cutoff, floor and
        // bounds are `CleanupReceiptPurgeIntegrationTest`'s subject.)
        jdbi.useHandleUnchecked { h -> h.execute("DELETE FROM hog_commit_receipt") }

        // The client retries the same request — pyhoglake from its
        // prepared-payload cache, hedgerow from `PendingStore.recover()`
        // after a restart. Neither is doing anything wrong.
        val second = CommitService(jdbi).commit("cat", request)

        // A SECOND PUBLICATION, not an error and not the first result.
        assertThat(second)
            .describedAs("with no receipt to find, a keyed commit is just a commit")
            .isNotEqualTo(first)
        assertThat(second.snapshotId).isGreaterThan(first.snapshotId)

        val rows =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT data_file_id, begin_snapshot, end_snapshot, record_count, row_id_start " +
                        "FROM hog_data_file WHERE path = :p ORDER BY data_file_id",
                ).bind("p", path)
                    .map { rs, _ ->
                        listOf(
                            rs.getLong("data_file_id"),
                            rs.getLong("begin_snapshot"),
                            rs.getLong("record_count"),
                            rs.getLong("row_id_start"),
                        )
                    }.list()
            }
        assertThat(rows).describedAs("one file row per publication").hasSize(2)

        // BOTH ARE LIVE, which is the part that makes this a data defect
        // rather than a wasted commit: the second registration does not
        // supersede the first, so a scan at the head snapshot reads the
        // same 100 records twice.
        val live =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT count(*) FROM hog_data_file WHERE path = :p AND end_snapshot IS NULL",
                ).bind("p", path).mapTo(Long::class.java).one()
            }
        assertThat(live)
            .describedAs("both registrations of the same object are live at the head snapshot")
            .isEqualTo(2)

        // And the row-id spans do NOT overlap, so nothing downstream can
        // dedupe them by identity either: they are, to every reader, two
        // different files that happen to share a path.
        val spans = rows.map { it[3] }
        assertThat(spans.distinct())
            .describedAs("non-overlapping row-id spans: two distinct files to every reader")
            .hasSize(2)

        // Nothing anywhere reports a problem. This is the assertion that
        // says why the retention floor is the only fence: there is no
        // signal to alert on.
        val snapshots =
            jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT count(*) FROM hog_snapshot").mapTo(Long::class.java).one()
            }
        assertThat(snapshots).describedAs("two valid snapshots, no failure anywhere").isEqualTo(2)
    }

    /**
     * The same replay WITH the receipt still there, as the control.
     *
     * Without it the case above could pass for a reason that has nothing
     * to do with the receipt — a broken idempotency path, say — and the
     * whole point is that the ONLY difference between the two is whether
     * the row was purged.
     */
    @Test
    fun `the same replay with its receipt intact publishes nothing`() {
        seed()
        val key = UUID.randomUUID()
        val path = "s3://b/data/dup.parquet"
        val request =
            CommitRequest(
                idempotencyKey = key,
                appends = listOf(TableAppend("ns", "events", listOf(FileRegistration(path, 100, 5000, 20)))),
            )
        val first = CommitService(jdbi).commit("cat", request)
        assertThat(CommitService(jdbi).commit("cat", request)).isEqualTo(first)
        val live =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT count(*) FROM hog_data_file WHERE path = :p AND end_snapshot IS NULL",
                ).bind("p", path).mapTo(Long::class.java).one()
            }
        assertThat(live).isEqualTo(1)
    }
}
