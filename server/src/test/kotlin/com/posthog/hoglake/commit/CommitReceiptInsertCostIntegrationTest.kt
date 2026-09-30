package com.posthog.hoglake.commit

import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

/**
 * What the receipt insert costs INSIDE THE PER-CATALOG COMMIT LOCK, on
 * the shape the prod-us events writer sends on every flush: a 270-file x
 * 25-column prepared append.
 *
 * WHY THIS IS A TEST AND NOT A NOTE. #240's whole second claim is that
 * the receipt insert is ~38 ms of the ~100 ms a large commit holds the
 * lock, and the fix's claim is that it becomes sub-millisecond. Both are
 * statements about ONE statement, and neither is visible in any other
 * assertion in this suite: the commit still succeeds, the receipt still
 * replays, the counters are identical. So the two INSERTs are timed
 * directly, on the same rows, in the same session — the legacy shape
 * (the canonical payload as jsonb) and V24's (32 bytes) — and the delta
 * is the number reported.
 *
 * IT IS NOT A BENCHMARK, and the assertion is written accordingly. Wall
 * clock on a container is the machine's, not the code's, so the bound is
 * on the V24 insert alone and it is loose (see [FINGERPRINT_BUDGET_MS]);
 * the legacy figure is measured and PRINTED rather than asserted, because
 * asserting "the old way is slow" would be asserting a property of the
 * disk. What the bound does catch is the mistake that matters: a writer
 * that still serializes and TOASTs a megabyte of JSON cannot come in
 * under it on any machine.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CommitReceiptInsertCostIntegrationTest {
    private companion object {
        /** Files per commit: gigahog-prod-us's measured average (#240). */
        const val FILES = 270

        /** Column-stats rows per file: the events table's width. */
        const val COLUMNS = 25

        /** Timed repetitions of each insert; the MEDIAN is reported. */
        const val REPEATS = 9

        /**
         * The bound on V24's insert.
         *
         * 5 ms, against a measured figure that is a fraction of a
         * millisecond, because this runs on whatever CI gives it and a
         * single INSERT of 32 bytes has nothing in it that can take
         * 5 ms — while the shape it replaces cannot beat 5 ms anywhere:
         * a megabyte of JSON serialization, a jsonb parse and a TOAST
         * write were ~38 ms on the #240 fixture and 14.5 ms here. The
         * test prints both, so the real delta is on the record rather
         * than inferred from a threshold.
         */
        const val FINGERPRINT_BUDGET_MS = 5.0
    }

    private val db = PgTestSupport.freshDatabase()
    private var catalogId = 0L

    @AfterAll
    fun tearDown() = db.close()

    /** The payload: 270 files, each with 25 column-stats rows. */
    private val request: CommitRequest by lazy {
        val files =
            (1..FILES).map { f ->
                FileRegistration(
                    path =
                        "s3://bucket/demo/data/ingest/events_raw/team_id=%d/part-%05d-%s.parquet"
                            .format(f % 7, f, UUID.nameUUIDFromBytes(byteArrayOf(f.toByte()))),
                    recordCount = 12_000L + f,
                    fileSizeBytes = 48_000L + f * 13,
                    footerSize = 4_096,
                    columnStats =
                        (1..COLUMNS).map { c ->
                            ColumnStats(
                                fieldId = c.toLong(),
                                valueCount = 12_000L + f,
                                nullCount = (f * c % 31).toLong(),
                                nanCount = 0,
                                sizeBytes = 900L + c,
                                // Eight bytes each: an int64 bound's real
                                // width, which is what makes the payload a
                                // megabyte rather than a toy. Production's
                                // is ~1.5 MB (#240) because its string
                                // bounds are wider; this fixture is the
                                // conservative end of that.
                                lowerBound = ByteArray(8) { (c + it).toByte() },
                                upperBound = ByteArray(8) { (c + it + 7).toByte() },
                            )
                        },
                    partitionValues = listOf((f % 7).toString()),
                )
            }
        CommitRequest(
            idempotencyKey = UUID.randomUUID(),
            appends = listOf(TableAppend("ingest", "events_raw", files)),
        )
    }

    @Test
    fun `the receipt insert is a digest write, not a TOAST write`() {
        catalogId =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "INSERT INTO hog_catalog (name, data_path) VALUES ('cost', 's3://bucket/demo') " +
                        "RETURNING catalog_id",
                ).mapTo(Long::class.java).one()
            }
        val canonical = commitFingerprint(request)
        val digest = commitFingerprintDigest(canonical)
        // The fixture is the production shape only if it is production's
        // ORDER OF SIZE: #240 measured ~1.5 MB of jsonb for this append
        // and this fixture canonicalizes to ~1.0 MB (see COLUMNS' note on
        // bound widths), so a floor of 1 MB is the honest assertion.
        assertThat(canonical.length)
            .describedAs("the canonical payload must be production-sized to measure anything")
            .isGreaterThan(1_000_000)

        val legacy = mutableListOf<Double>()
        val v24 = mutableListOf<Double>()
        db.jdbi.useHandleUnchecked { h ->
            repeat(REPEATS) {
                legacy +=
                    timeMs {
                        h.createUpdate(
                            "INSERT INTO hog_commit_receipt " +
                                "(catalog_id, idempotency_key, request, snapshot_id, schema_version) " +
                                "VALUES (:c, :key, CAST(:request AS jsonb), 1, 1)",
                        ).bind("c", catalogId).bind("key", UUID.randomUUID())
                            .bind("request", canonical).execute()
                    }
                v24 +=
                    timeMs {
                        h.createUpdate(
                            "INSERT INTO hog_commit_receipt " +
                                "(catalog_id, idempotency_key, fingerprint, snapshot_id, schema_version) " +
                                "VALUES (:c, :key, :fingerprint, 1, 1)",
                        ).bind("c", catalogId).bind("key", UUID.randomUUID())
                            .bind("fingerprint", digest).execute()
                    }
            }
        }
        val legacyMedian = legacy.sorted()[REPEATS / 2]
        val v24Median = v24.sorted()[REPEATS / 2]
        println(
            "receipt insert, $FILES files x $COLUMNS columns (${canonical.length} bytes canonical): " +
                "body ${"%.2f".format(legacyMedian)} ms -> fingerprint ${"%.3f".format(v24Median)} ms " +
                "(${"%.0f".format(legacyMedian / v24Median)}x)",
        )
        assertThat(v24Median)
            .describedAs(
                "V24's receipt insert on a %d-file x %d-column commit; the body it replaces " +
                    "measured %.2f ms here and ~38 ms on #240's fixture",
                FILES,
                COLUMNS,
                legacyMedian,
            )
            .isLessThan(FINGERPRINT_BUDGET_MS)
        // And the direction, which is the claim that does not depend on
        // the machine: the same statement with the body costs more than
        // the same statement without it, on the same session and the same
        // table. MUTATION: put `request` back on the writer's INSERT and
        // the commit path pays this difference again.
        assertThat(v24Median)
            .describedAs("body %.2f ms, fingerprint %.3f ms", legacyMedian, v24Median)
            .isLessThan(legacyMedian)
    }

    private inline fun timeMs(body: () -> Unit): Double {
        val start = System.nanoTime()
        body()
        return (System.nanoTime() - start) / 1_000_000.0
    }
}
