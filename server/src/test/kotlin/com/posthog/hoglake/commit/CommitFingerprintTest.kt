package com.posthog.hoglake.commit

import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.wireObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.random.Random

class CommitFingerprintTest {
    @Test
    fun `canonical bytes preserve the receipt format across registration permutations`() {
        val stats =
            listOf(ColumnStats(1, 7, 0, 0, 30, byteArrayOf(1), byteArrayOf(7)), ColumnStats(2, 7, 1, 0, 20, null, null))
        val file = FileRegistration("s3://synthetic/a.parquet", 10, 700, 100, stats, listOf("b", null, "a"))
        val append =
            TableAppend(
                "ns",
                "a",
                // Sorting is by the complete serialized registration, including
                // lexicographic numeric order, not just the path or table name.
                listOf(
                    file,
                    file.copy(recordCount = 2),
                    file.copy(path = "s3://synthetic/z.parquet", columnStats = null),
                ),
                UUID.fromString("12345678-1234-5678-90ab-1234567890ab"),
            )
        val canonical =
            CommitRequest(
                readSnapshot = 4,
                appends = listOf(append, append.copy(table = "z", files = emptyList())),
                deletes =
                    listOf(
                        TableDeletes("ns", "a", listOf(DeleteFileRegistration(1, "s3://synthetic/a.dv", 2, 100))),
                    ),
                author = "synthetic-author",
                message = "canonical receipt",
                idempotencyKey = UUID.fromString("12345678-1234-5678-90ab-1234567890ac"),
            )
        val expected = wireObjectMapper().writeValueAsString(canonical)
        repeat(20) { seed ->
            val random = Random(seed)
            val permuted =
                canonical.copy(
                    appends =
                        canonical.appends.shuffled(random).map { table ->
                            table.copy(
                                files =
                                    table.files.shuffled(
                                        random,
                                    ).map { it.copy(columnStats = it.columnStats?.shuffled(random)) },
                            )
                        },
                )
            assertThat(commitFingerprint(permuted)).isEqualTo(expected)
        }
    }

    @Test
    fun `every file registration field and publication pin participates in the fingerprint`() {
        val stats = ColumnStats(1, 7, 0, 0, 30, byteArrayOf(1), byteArrayOf(7))
        val file = FileRegistration("s3://synthetic/a.parquet", 7, 700, 100, listOf(stats), listOf("a", "b"))
        val append = TableAppend("ns", "target", listOf(file), UUID.randomUUID())
        val request = CommitRequest(readSnapshot = 4, appends = listOf(append), idempotencyKey = UUID.randomUUID())
        val original = commitFingerprint(request)
        val changedStats =
            listOf(
                stats.copy(fieldId = 2),
                stats.copy(valueCount = 8),
                stats.copy(nullCount = 1),
                stats.copy(nanCount = 1),
                stats.copy(sizeBytes = 31),
                stats.copy(lowerBound = byteArrayOf(2)),
                stats.copy(upperBound = byteArrayOf(8)),
            )
        val changedFiles =
            listOf(
                file.copy(path = "s3://synthetic/b.parquet"),
                file.copy(recordCount = 8),
                file.copy(fileSizeBytes = 701),
                file.copy(footerSize = 101),
                file.copy(columnStats = null),
                file.copy(partitionValues = listOf("b", "a")),
            ) + changedStats.map { file.copy(columnStats = listOf(it)) }
        val changedRequests =
            listOf(
                request.copy(readSnapshot = 5),
                request.copy(author = "synthetic-author"),
                request.copy(message = "changed"),
                request.copy(appends = listOf(append.copy(expectedTableUuid = UUID.randomUUID()))),
                request.copy(appends = listOf(append.copy(table = "another"))),
            ) + changedFiles.map { request.copy(appends = listOf(append.copy(files = listOf(it)))) }
        changedRequests.forEach { assertThat(commitFingerprint(it)).isNotEqualTo(original) }
    }
}
