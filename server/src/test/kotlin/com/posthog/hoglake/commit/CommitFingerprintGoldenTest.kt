package com.posthog.hoglake.commit

import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.UUID

/**
 * THE TRIPWIRE FOR A PERSISTED FORMAT. `commitFingerprint`'s output
 * stopped being an ephemeral comparison value in V24 (#240) and became
 * the input to the digest stored in `hog_commit_receipt.fingerprint`, so
 * any change to it invalidates every receipt inside the retention window.
 *
 * WHY `CommitFingerprintTest` DOES NOT COVER THIS, which is the whole
 * reason this file exists. That test computes its expectation as
 * `wireObjectMapper().writeValueAsString(canonical)` — it pins the
 * FUNCTION against the live mapper, which is the right assertion for
 * order-insensitivity and the wrong one for format stability: every
 * drift below moves both sides of its comparison together and the suite
 * stays green while every stored digest silently stops matching.
 *
 * So the expectation here is a LITERAL, typed out, and it must never be
 * "fixed" by pasting in whatever the code now produces:
 *
 *  - flipping a `NON_DEFAULT` boolean's default on `CommitRequest`
 *    (`require_unchanged_tables`, `allow_pending_deletes`);
 *  - reordering `CommitRequest`'s or `FileRegistration`'s property
 *    declarations (Jackson serializes Kotlin properties in declaration
 *    order);
 *  - adding any non-null field (`NON_NULL` only elides nullable ones);
 *  - a Jackson upgrade that renders a number or an escape differently.
 *
 * IF THIS TEST REDS, the canonical string has moved and every receipt
 * digest written by a deployed replica has just become uncomparable.
 * That is survivable — but only through
 * [COMMIT_FINGERPRINT_VERSION]: bump it in the same change, so the
 * replay arm answers an unknown version with the stored snapshot rather
 * than a 422 telling a correct client its request differs. Then, and
 * only then, update the literals here. Updating them WITHOUT the bump is
 * the one edit that ships the silent-422 failure.
 */
class CommitFingerprintGoldenTest {
    private companion object {
        /**
         * Every id is fixed, because a random UUID in a golden fixture
         * makes the literal unwritable. The shape covers what the format
         * actually has to be stable about: two tables (so the append
         * sort runs), two files in one of them (so the file sort runs),
         * column stats with and without bounds (so the stats sort and
         * the base64 rendering of `bytea` run), a partition value list
         * with a null in it, a delete registration, and the
         * publication pins (`read_snapshot`, `expected_table_uuid`,
         * author, message).
         */
        val KEY: UUID = UUID.fromString("11111111-2222-3333-4444-555555555555")
        val TABLE_UUID: UUID = UUID.fromString("66666666-7777-8888-9999-aaaaaaaaaaaa")

        /**
         * The canonical string, as a literal. Produced once by the code
         * under test and then TYPED OUT here; see the class KDoc for why
         * it must not be regenerated on a red.
         */
        const val GOLDEN_CANONICAL: String =
            """{"read_snapshot":1234,"appends":[{"namespace":"ns","table":"a","files":[""" +
                """{"path":"s3://bucket/demo/data/a.parquet","record_count":10,"file_size_bytes":700,""" +
                """"footer_size":100,"column_stats":[{"field_id":1,"value_count":7,"null_count":0,""" +
                """"nan_count":0,"size_bytes":30,"lower_bound":"AQ==","upper_bound":"Bw=="},""" +
                """{"field_id":2,"value_count":7,"null_count":1,"nan_count":0,"size_bytes":20}],""" +
                """"partition_values":["b",null,"a"]},""" +
                """{"path":"s3://bucket/demo/data/z.parquet","record_count":11,"file_size_bytes":700,""" +
                """"footer_size":100}],"expected_table_uuid":"66666666-7777-8888-9999-aaaaaaaaaaaa"},""" +
                """{"namespace":"ns","table":"z","files":[]}],"deletes":[{"namespace":"ns","table":"a",""" +
                """"files":[{"data_file_id":1,"path":"s3://bucket/demo/data/a.dv","delete_count":2,""" +
                """"file_size_bytes":100}]}],"author":"golden-author","message":"golden receipt",""" +
                """"idempotency_key":"11111111-2222-3333-4444-555555555555"}"""

        /**
         * `commitFingerprintDigest(GOLDEN_CANONICAL)` as hex, version
         * byte included: `01` then the SHA-256. Also a literal — it is
         * the value a deployed replica has written into
         * `hog_commit_receipt.fingerprint`.
         */
        const val GOLDEN_DIGEST_HEX: String =
            "0115388164572649483f0f54810332c78ed333d9f5e09cdd7991e98aec52c41e2f"
    }

    private fun request(): CommitRequest {
        val stats =
            listOf(
                ColumnStats(1, 7, 0, 0, 30, byteArrayOf(1), byteArrayOf(7)),
                ColumnStats(2, 7, 1, 0, 20, null, null),
            )
        return CommitRequest(
            readSnapshot = 1234,
            appends =
                listOf(
                    TableAppend(
                        "ns",
                        "a",
                        listOf(
                            FileRegistration(
                                "s3://bucket/demo/data/a.parquet",
                                10,
                                700,
                                100,
                                stats,
                                listOf("b", null, "a"),
                            ),
                            FileRegistration("s3://bucket/demo/data/z.parquet", 11, 700, 100),
                        ),
                        TABLE_UUID,
                    ),
                    TableAppend("ns", "z", emptyList()),
                ),
            deletes =
                listOf(
                    TableDeletes(
                        "ns",
                        "a",
                        listOf(DeleteFileRegistration(1, "s3://bucket/demo/data/a.dv", 2, 100)),
                    ),
                ),
            author = "golden-author",
            message = "golden receipt",
            idempotencyKey = KEY,
        )
    }

    @Test
    fun `the canonical string is byte-for-byte what deployed receipts were digested from`() {
        assertThat(commitFingerprint(request()))
            .describedAs(
                "the canonical fingerprint string is a PERSISTED format (V24). A red here means " +
                    "every stored receipt digest has just become uncomparable — bump " +
                    "COMMIT_FINGERPRINT_VERSION in this same change before touching this literal.",
            )
            .isEqualTo(GOLDEN_CANONICAL)
    }

    @Test
    fun `the digest is the version byte plus the SHA-256 of that string`() {
        val digest = commitFingerprintDigest(commitFingerprint(request()))
        assertThat(digest.size)
            .describedAs("one version byte plus a SHA-256")
            .isEqualTo(COMMIT_FINGERPRINT_LENGTH)
        assertThat(digest.first())
            .describedAs("the version byte the replay arm switches on")
            .isEqualTo(COMMIT_FINGERPRINT_VERSION)
        assertThat(digest.toHex())
            .describedAs(
                "the exact bytes a deployed replica has written into " +
                    "hog_commit_receipt.fingerprint; see the class KDoc before changing this",
            )
            .isEqualTo(GOLDEN_DIGEST_HEX)
    }

    /**
     * The order-insensitivity, asserted THROUGH the golden literal
     * rather than through the mapper.
     *
     * `CommitFingerprintTest` already pins that permutations agree with
     * each other; this pins that they agree with the value on disk,
     * which is the property a replay of a receipt written days ago
     * depends on.
     */
    @Test
    fun `a permuted request digests to the stored value, not merely to itself`() {
        val original = request()
        val permuted =
            original.copy(
                appends =
                    original.appends.reversed().map { append ->
                        append.copy(
                            files =
                                append.files.reversed().map {
                                    it.copy(columnStats = it.columnStats?.reversed())
                                },
                        )
                    },
            )
        assertThat(commitFingerprint(permuted)).isEqualTo(GOLDEN_CANONICAL)
        assertThat(commitFingerprintDigest(commitFingerprint(permuted)).toHex()).isEqualTo(GOLDEN_DIGEST_HEX)
    }

    private fun ByteArray.toHex(): String = joinToString("") { "%02x".format(it) }
}
