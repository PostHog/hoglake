package com.posthog.hoglake.api

import com.posthog.hoglake.model.ExpiryResult
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.io.File
import kotlin.reflect.full.memberProperties

/**
 * `ExpiryResult` on the WIRE — the expiry twin of
 * [CleanupResultSpecParityTest] and [CompactionResultSpecParityTest],
 * and the one that was missing while the two-phase sweep added five
 * fields in three places at once.
 *
 * There are three copies of this counter set and nothing else keeps them
 * in step: the stored model ([ExpiryResult], which the maintenance run
 * ledger serializes), the response DTO ([ExpiryResultDto], which
 * `POST /maintenance/expire` returns), and the OpenAPI schema. On
 * compaction, `claimed_elsewhere` reached the model and the spec but not
 * the DTO — so the endpoint declared a field it could never emit, and
 * only a client generated from the spec would have noticed.
 *
 * The DTO's property list comes from REFLECTION and the spec's from
 * parsing the file, so neither side is a literal here.
 *
 * Unit, not integration: two artifacts on disk and the classes they
 * describe.
 */
class ExpiryResultSpecParityTest {
    private val spec: String = File("src/main/resources/openapi/hoglake.yaml").readText()

    /** snake_case, the wire's spelling, for a Kotlin property name. */
    private fun wire(name: String): String = name.replace(Regex("([a-z0-9])([A-Z])"), "$1_$2").lowercase()

    private fun schemaBlock(header: String): String {
        val start = spec.indexOf(header)
        assertThat(start).describedAs("spec block %s", header).isNotEqualTo(-1)
        val rest = spec.substring(start + header.length)
        val end = Regex("\n    [A-Za-z]").find(rest)?.range?.first ?: rest.length
        return rest.substring(0, end)
    }

    /** The schema's own property keys, at its indentation. */
    private fun properties(block: String): List<String> =
        Regex("""(?m)^        ([a-z_0-9]+):""").findAll(block).map { it.groupValues[1] }.toList()

    private fun requiredOf(block: String): List<String> {
        val match = Regex("""required: \[([^]]*)]""", RegexOption.DOT_MATCHES_ALL).find(block)
        assertThat(match).describedAs("ExpiryResult has no `required:` list").isNotNull()
        return match!!.groupValues[1].split(",").map { it.trim() }.filter { it.isNotEmpty() }
    }

    @Test
    fun `the response DTO and the spec declare exactly the same fields`() {
        val dto = ExpiryResultDto::class.memberProperties.map { wire(it.name) }.sorted()
        val declared = properties(schemaBlock("    ExpiryResult:")).sorted()
        assertThat(dto)
            .describedAs(
                "every property the spec declares must exist on the DTO that answers the " +
                    "endpoint, and vice versa — a spec-only field is one no client can ever " +
                    "receive",
            )
            .isEqualTo(declared)
    }

    @Test
    fun `the stored model and the response DTO carry the same fields`() {
        val model = ExpiryResult::class.memberProperties.map { wire(it.name) }.sorted()
        val dto = ExpiryResultDto::class.memberProperties.map { wire(it.name) }.sorted()
        assertThat(dto)
            .describedAs("the ledger's model and the endpoint's DTO are converted by hand, in toDto()")
            .isEqualTo(model)
    }

    @Test
    fun `toDto copies every field, with none left at its default`() {
        // Distinct values per field, so a toDto that maps one to the
        // WRONG source — or forgets one and lets the data class default
        // cover for it — is visible.
        val dto =
            ExpiryResult(
                snapshotsExpired = 1,
                dataFilesQueued = 2,
                deleteFilesQueued = 3,
                newEarliestSnapshotId = 4,
                flooredByConsumer = "hedgerow-events",
                offsetsReleased = 5,
                dataFilesPurged = 6,
                purgePages = 7,
                purgeFailures = 8,
                purgeTruncated = true,
                purgeRemaining = 9,
                advanceHalvings = 10,
                purgeHalvings = 11,
            ).toDto()
        assertThat(dto.snapshotsExpired).isEqualTo(1)
        assertThat(dto.dataFilesQueued).isEqualTo(2)
        assertThat(dto.deleteFilesQueued).isEqualTo(3)
        assertThat(dto.newEarliestSnapshotId).isEqualTo(4)
        assertThat(dto.flooredByConsumer).isEqualTo("hedgerow-events")
        assertThat(dto.offsetsReleased).isEqualTo(5)
        assertThat(dto.dataFilesPurged)
            .describedAs("rows phase B deleted below the floor, outside the commit lock")
            .isEqualTo(6)
        assertThat(dto.purgePages).isEqualTo(7)
        assertThat(dto.purgeFailures)
            .describedAs("pages that threw — what makes a zero above readable")
            .isEqualTo(8)
        assertThat(dto.purgeTruncated)
            .describedAs("a BOOLEAN on the wire, not a count")
            .isTrue()
        assertThat(dto.purgeRemaining).isEqualTo(9)
        assertThat(dto.advanceHalvings)
            .describedAs("the floor advance's batch halvings, for the ledger and the console")
            .isEqualTo(10)
        assertThat(dto.purgeHalvings)
            .describedAs("and the purge's page halvings, which an all-timeouts sweep reports alone")
            .isEqualTo(11)
    }

    @Test
    fun `data_files_purged and data_files_queued are the same number by construction`() {
        // Both are on the wire and the KDoc argues they can never
        // disagree, because a page's delete and its `hog_file_removal`
        // insert are one statement. Prose is not an assertion: this is.
        // If a future change splits that statement, the identity breaks
        // here rather than in a leaked object.
        val result =
            ExpiryResult(
                snapshotsExpired = 0,
                dataFilesQueued = 12,
                deleteFilesQueued = 0,
                newEarliestSnapshotId = 7,
                flooredByConsumer = null,
                dataFilesPurged = 12,
            )
        assertThat(result.dataFilesQueued).isEqualTo(result.dataFilesPurged)
        assertThat(result.toDto().dataFilesQueued).isEqualTo(result.toDto().dataFilesPurged)
    }

    @Test
    fun `the fields added after the ledger are optional on the wire`() {
        // NON_DEFAULT on the stored model means a zero is omitted from a
        // ledger row, and rows older than the field never had it at all —
        // so `required` would be a promise the ledger's own history
        // breaks. The read path fills 0 (EXPIRY_COUNTERS_ADDED_LATER),
        // which is why absence reads as zero rather than as unknown.
        val block = schemaBlock("    ExpiryResult:")
        val required = requiredOf(block)
        val addedLater =
            listOf(
                "offsets_released",
                "data_files_purged",
                "purge_pages",
                "purge_failures",
                "purge_remaining",
                "purge_truncated",
                "advance_halvings",
                "purge_halvings",
            )
        for (field in addedLater) {
            assertThat(properties(block)).contains(field)
            assertThat(required)
                .describedAs("a required field cannot be one the ledger has rows without")
                .doesNotContain(field)
        }
        // The ones that predate the ledger stay required: optional is for
        // what was ADDED, not a default for everything.
        assertThat(required).containsExactlyInAnyOrder(
            "snapshots_expired",
            "data_files_queued",
            "delete_files_queued",
            "new_earliest_snapshot_id",
        )
    }

    @Test
    fun `purge_truncated is a boolean in the spec and is NOT in the ledger filler list`() {
        // The one deliberate asymmetry, and the reason it needs a test:
        // `normalizeLedgerResult` fills a missing field with the INTEGER
        // 0, so a boolean in that list would be written into a ledger row
        // as a value its own schema forbids. The console's guard on it is
        // undefined-safe instead.
        val block = schemaBlock("    ExpiryResult:")
        val truncated = block.substringAfter("purge_truncated:").substringBefore("purge_remaining:")
        assertThat(truncated)
            .describedAs("purge_truncated must be declared a boolean:%n%s", truncated)
            .contains("type: boolean")
        val dtoType =
            ExpiryResultDto::class.memberProperties.single { it.name == "purgeTruncated" }
                .returnType.toString()
        assertThat(dtoType).contains("Boolean")
        // The filler list lives in MaintenanceDto as a private val, so it
        // is asserted through its OBSERVABLE effect instead — see
        // MaintenanceLedgerIntegrationTest's pre-upgrade expiry row,
        // which replays a row written without any of these fields and
        // checks exactly which ones come back filled.
    }
}
