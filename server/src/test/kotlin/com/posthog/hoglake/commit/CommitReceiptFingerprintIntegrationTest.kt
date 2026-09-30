package com.posthog.hoglake.commit

import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.wireObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.UUID

/**
 * The receipt after V24: a digest instead of a body (#240), with the
 * replay contract unchanged and the pre-V24 rows still answered.
 *
 * WHAT "UNCHANGED" MEANS HERE, since that is the whole risk of the
 * change. The digest is taken OF the canonical string
 * `commitFingerprint` already produced, so every property that string
 * had — the wire mapper's field set, order-insensitivity over appends,
 * files and column stats, defaulted booleans omitted — is a property of
 * the digest by construction. These cases pin that the comparison the
 * commit path performs is still that string's, now through 33 bytes,
 * that a row written before the column is still compared the old way,
 * and that a digest version this build does not know is answered rather
 * than refused.
 *
 * WHAT PINS THE STRING IS `CommitFingerprintGoldenTest`, not
 * `CommitFingerprintTest`, and the distinction is load-bearing: the
 * latter computes its expectation from the live mapper, so it pins the
 * FUNCTION and stays green through exactly the format drift that
 * invalidates every stored digest. The golden test holds the literal.
 */
@Tag("integration")
class CommitReceiptFingerprintIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val jdbi: Jdbi get() = db.jdbi
    private val service = CommitService(db.jdbi)

    @AfterEach
    fun tearDown() = db.close()

    private fun seed(columns: Int = 2): Long =
        jdbi.withHandleUnchecked { h ->
            val catalogId =
                h.createQuery(
                    "INSERT INTO hog_catalog (name, data_path) VALUES ('cat', 's3://b') RETURNING catalog_id",
                ).mapTo(Long::class.java).one()
            h.createUpdate("INSERT INTO hog_namespace (catalog_id, namespace_id, name) VALUES (?, 0, 'ns')")
                .bind(0, catalogId).execute()
            h.createUpdate(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot, next_field_id) VALUES (?, 0, 0, ?)",
            ).bind(0, catalogId).bind(1, columns + 1L).execute()
            h.createUpdate(
                "INSERT INTO hog_table_version (catalog_id, table_id, begin_snapshot, namespace_id, name) " +
                    "VALUES (?, 0, 0, 0, 'events')",
            ).bind(0, catalogId).execute()
            for (ordinal in 0 until columns) {
                h.createUpdate(
                    """
                    INSERT INTO hog_column (catalog_id, table_id, field_id, begin_snapshot, name, col_type, ordinal)
                    VALUES (?, 0, ?, 0, ?, ?, ?)
                    """,
                ).bind(0, catalogId).bind(1, ordinal + 1L).bind(2, "col${ordinal + 1}")
                    .bind(3, if (ordinal == 0) "long" else "string").bind(4, ordinal).execute()
            }
            h.createUpdate("INSERT INTO hog_table_stats (catalog_id, table_id) VALUES (?, 0)")
                .bind(0, catalogId).execute()
            catalogId
        }

    private fun request(
        key: UUID,
        files: List<FileRegistration>,
    ) = CommitRequest(idempotencyKey = key, appends = listOf(TableAppend("ns", "events", files)))

    private fun storedReceipt(key: UUID): Triple<ByteArray?, String?, Long> =
        jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT fingerprint, request::text AS request, snapshot_id " +
                    "FROM hog_commit_receipt WHERE idempotency_key = :key",
            ).bind("key", key)
                .map { rs, _ ->
                    Triple(rs.getBytes("fingerprint"), rs.getString("request"), rs.getLong("snapshot_id"))
                }.one()
        }

    /** Turn a receipt into the shape a pre-V24 replica wrote: body, no digest. */
    private fun makeLegacy(
        key: UUID,
        body: CommitRequest,
    ) = jdbi.useHandleUnchecked { h ->
        h.createUpdate(
            "UPDATE hog_commit_receipt SET fingerprint = NULL, request = CAST(:request AS jsonb) " +
                "WHERE idempotency_key = :key",
        ).bind("request", wireObjectMapper().writeValueAsString(body)).bind("key", key).execute()
    }

    // ---- what the writer stores ---------------------------------------------

    @Test
    fun `a commit stores the digest and no body`() {
        seed()
        val key = UUID.randomUUID()
        val req = request(key, listOf(FileRegistration("s3://b/data/a.parquet", 3, 300, 20)))
        service.commit("cat", req)

        val (fingerprint, body, _) = storedReceipt(key)
        // MUTATION: put `request` back on the INSERT and the body assertion
        // reds; drop `fingerprint` from it and the first two do.
        assertThat(body).describedAs("the 566 KiB/commit #240 measured must not be written").isNull()
        assertThat(fingerprint).isNotNull()
        assertThat(fingerprint!!.size)
            .describedAs("one version byte plus a SHA-256, as bytes rather than hex")
            .isEqualTo(COMMIT_FINGERPRINT_LENGTH)
        assertThat(fingerprint.first())
            .describedAs("the version byte the replay arm switches on")
            .isEqualTo(COMMIT_FINGERPRINT_VERSION)
        // The stored bytes are the digest OF the canonical string, which
        // is what makes the replay comparison the old comparison. Computed
        // here from the request rather than copied from the row.
        assertThat(fingerprint).isEqualTo(commitFingerprintDigest(commitFingerprint(req)))
    }

    @Test
    fun `a commit with no idempotency key writes no receipt at all`() {
        seed()
        service.commit(
            "cat",
            CommitRequest(
                appends =
                    listOf(
                        TableAppend("ns", "events", listOf(FileRegistration("s3://b/data/n.parquet", 1, 100, 20))),
                    ),
            ),
        )
        val receipts =
            jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT count(*) FROM hog_commit_receipt").mapTo(Long::class.java).one()
            }
        assertThat(receipts).isZero()
    }

    // ---- replay through the digest -------------------------------------------

    @Test
    fun `the same key and payload replay to the same snapshot through the digest`() {
        seed()
        val key = UUID.randomUUID()
        val req = request(key, listOf(FileRegistration("s3://b/data/a.parquet", 3, 300, 20)))
        val first = service.commit("cat", req)
        // A fresh service, so nothing in-process can be answering this.
        assertThat(CommitService(jdbi).commit("cat", req)).isEqualTo(first)
        val files =
            jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT count(*) FROM hog_data_file").mapTo(Long::class.java).one()
            }
        assertThat(files).describedAs("a replay publishes nothing").isEqualTo(1)
    }

    @Test
    fun `permuted appends, files and column stats still replay`() {
        seed(columns = 3)
        val key = UUID.randomUUID()
        val stats =
            listOf(
                ColumnStats(1, 3, 0, 0, 10, byteArrayOf(1), byteArrayOf(3)),
                ColumnStats(2, 3, 1, 0, 12, null, null),
                ColumnStats(3, 3, 0, 0, 14, byteArrayOf(4), byteArrayOf(9)),
            )
        val a = FileRegistration("s3://b/data/a.parquet", 3, 300, 20, stats)
        val z = FileRegistration("s3://b/data/z.parquet", 3, 300, 20, stats.reversed())
        val first = service.commit("cat", request(key, listOf(a, z)))

        // Files reversed AND every file's stats reversed: the
        // order-insensitivity the canonical string has, now decided by 32
        // bytes. MUTATION: drop the stats sort or the file sort from
        // commitFingerprint and this becomes a 422.
        val permuted =
            request(
                key,
                listOf(z.copy(columnStats = stats), a.copy(columnStats = stats.reversed())),
            )
        assertThat(CommitService(jdbi).commit("cat", permuted)).isEqualTo(first)
    }

    @Test
    fun `the same key with a different payload is still refused`() {
        seed()
        val key = UUID.randomUUID()
        service.commit("cat", request(key, listOf(FileRegistration("s3://b/data/a.parquet", 3, 300, 20))))
        // One record count apart — the smallest difference the canonical
        // string carries. MUTATION: compare the arrays with `==` instead
        // of `contentEquals` and every replay is refused, so the positive
        // cases above red; drop the `if (!same) throw` and this one does.
        assertThatThrownBy {
            CommitService(
                jdbi,
            ).commit("cat", request(key, listOf(FileRegistration("s3://b/data/a.parquet", 4, 300, 20))))
        }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("idempotency_key reused with a different request")
    }

    // ---- the pre-V24 rows ----------------------------------------------------

    @Test
    fun `a receipt with no digest is compared through its stored body`() {
        seed()
        val key = UUID.randomUUID()
        val files =
            listOf(
                FileRegistration("s3://b/data/z.parquet", 7, 700, 20),
                FileRegistration("s3://b/data/a.parquet", 11, 1100, 20),
            )
        val req = request(key, files)
        val first = service.commit("cat", req)
        // The row a pre-V24 replica left: the body in its ORIGINAL array
        // order (not the canonical one), and no digest.
        makeLegacy(key, req)

        // Replayed with the files in the other order, which is what makes
        // this the canonicalization path and not a string compare of the
        // request as sent.
        val reordered = request(key, files.reversed())
        // MUTATION: delete the `stored != null` branch's else arm (the
        // body comparison) and this reds — a null digest would compare
        // null against 32 bytes and refuse.
        assertThat(CommitService(jdbi).commit("cat", reordered)).isEqualTo(first)
        assertThat(storedReceipt(key).first).describedAs("a replay does not backfill the digest").isNull()
    }

    @Test
    fun `a receipt with no digest still refuses a different payload`() {
        seed()
        val key = UUID.randomUUID()
        val req = request(key, listOf(FileRegistration("s3://b/data/a.parquet", 3, 300, 20)))
        service.commit("cat", req)
        makeLegacy(key, req)
        assertThatThrownBy {
            CommitService(
                jdbi,
            ).commit("cat", request(key, listOf(FileRegistration("s3://b/data/a.parquet", 9, 300, 20))))
        }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("idempotency_key reused with a different request")
    }

    @Test
    fun `the digest arm wins over a body that disagrees with it`() {
        seed()
        val key = UUID.randomUUID()
        val req = request(key, listOf(FileRegistration("s3://b/data/a.parquet", 3, 300, 20)))
        val first = service.commit("cat", req)
        // A row carrying BOTH: the digest of the real request and a body
        // that is somebody else's. Not a state this service can produce —
        // the writer leaves `request` NULL — but the arm order has to be
        // stated, because the transition's whole safety argument is "a row
        // with a digest is judged by the digest". MUTATION: swap the arms
        // (prefer the body when present) and this becomes a 422.
        jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                "UPDATE hog_commit_receipt SET request = '{\"appends\":[]}'::jsonb WHERE idempotency_key = :key",
            ).bind("key", key).execute()
        }
        assertThat(CommitService(jdbi).commit("cat", req)).isEqualTo(first)
    }

    /**
     * The drift case, which is the reason the version byte exists.
     *
     * A canonical-string change makes every stored digest uncomparable —
     * not wrong, UNCOMPARABLE, since the old string is unrecoverable. The
     * two available answers are "this key was published, here is its
     * snapshot" and "422, your request differs", and the second is a new
     * 4xx on a path every deployed writer retries on, caused by our own
     * format change, for a whole retention window. So an unknown version
     * byte returns the stored result.
     */
    @Test
    fun `a digest version this build does not know returns the stored snapshot, never a refusal`() {
        seed()
        val key = UUID.randomUUID()
        val req = request(key, listOf(FileRegistration("s3://b/data/a.parquet", 3, 300, 20)))
        val first = service.commit("cat", req)
        // A receipt a FUTURE build wrote: version 0x02, 32 bytes of
        // something this build cannot reproduce.
        jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                "UPDATE hog_commit_receipt SET fingerprint = :fp WHERE idempotency_key = :key",
            ).bind("fp", byteArrayOf(0x02) + ByteArray(32) { 0x7f }).bind("key", key).execute()
        }
        // MUTATION: drop the version arm (so the comparison falls through
        // to contentEquals) and this becomes the 422 the arm exists to
        // prevent.
        assertThat(CommitService(jdbi).commit("cat", req))
            .describedAs("a correct client's replay must never be told its request differs")
            .isEqualTo(first)
        // And it published nothing: the answer is the receipt's, not a
        // second commit's.
        val files =
            jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT count(*) FROM hog_data_file").mapTo(Long::class.java).one()
            }
        assertThat(files).isEqualTo(1)
    }

    /**
     * The same arm, from the other side: an unknown version answers with
     * the stored snapshot even when the request genuinely differs.
     *
     * That is the cost, stated rather than hidden. A client that really
     * did reuse a key with a different payload gets the earlier
     * publication's result instead of a refusal — a client bug reported
     * as a success. It is the right trade (the alternative punishes every
     * correct client to catch a broken one) and it is what
     * idempotency-key APIs generally do, but it only holds while
     * unknown-version receipts are rare, which is what
     * `hoglake_commit_receipt_unknown_digest_version_total` is for.
     */
    @Test
    fun `an unknown digest version does not refuse a genuinely different payload either`() {
        seed()
        val key = UUID.randomUUID()
        val first = service.commit("cat", request(key, listOf(FileRegistration("s3://b/data/a.parquet", 3, 300, 20))))
        jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                "UPDATE hog_commit_receipt SET fingerprint = :fp WHERE idempotency_key = :key",
            ).bind("fp", byteArrayOf(0x7e) + ByteArray(32)).bind("key", key).execute()
        }
        val replayed =
            CommitService(
                jdbi,
            ).commit("cat", request(key, listOf(FileRegistration("s3://b/data/other.parquet", 9, 900, 20))))
        assertThat(replayed).isEqualTo(first)
    }

    @Test
    fun `a receipt read by key is unchanged by the column swap`() {
        seed()
        val key = UUID.randomUUID()
        val result = service.commit("cat", request(key, listOf(FileRegistration("s3://b/data/a.parquet", 3, 300, 20))))
        // `receipt()` never read the body; it reads the two id columns.
        assertThat(service.receipt("cat", key)).isEqualTo(result)
        assertThatThrownBy { service.receipt("cat", UUID.randomUUID()) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
    }
}
