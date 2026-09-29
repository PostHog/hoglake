package com.posthog.hoglake.api

import com.posthog.hoglake.service.PartitionListingService
import com.posthog.hoglake.wireObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.io.File
import java.time.Instant

/**
 * The partitions-listing DTOs against their OpenAPI schemas, both read
 * off the artifacts rather than restated here — `TableSummarySpecParityTest`'s
 * shape, and its reasoning:
 *
 *  - a DTO property the spec never declares is a field no generated
 *    client can see;
 *  - a spec property the DTO never sends is a field a generated client
 *    declares and always finds missing.
 *
 * The DTO side is a REAL SERIALIZATION through the wire mapper, so the
 * snake_case strategy and the per-property inclusion rules are exercised
 * rather than assumed. That last part carries weight on this endpoint:
 * `record_count`, `last_written_snapshot`, `sampled_at` and
 * `sampled_snapshot_id` are `@JsonInclude(ALWAYS)` BECAUSE null is a
 * distinct answer on each of them ("the sample never measured this",
 * "there is no sample"), and an omitted key would read as zero on a
 * client that defaults it. The spec's `required` lists them for the same
 * reason, and the minimal-serialization test is what keeps the two
 * statements equal.
 *
 * Unit test on purpose: it reads a file and serializes an object, so it
 * runs under `-PunitOnly` where a Docker-less machine can still catch
 * the drift.
 */
class PartitionListingSpecParityTest {
    private val spec: String = File("src/main/resources/openapi/hoglake.yaml").readText()
    private val mapper = wireObjectMapper()

    /** The named schema block, up to the next sibling schema. */
    private fun block(name: String): String {
        val header = "    $name:\n"
        val start = spec.indexOf(header)
        assertThat(start).describedAs("spec block %s", name).isNotEqualTo(-1)
        val rest = spec.substring(start + header.length)
        val end = Regex("\n    [A-Za-z]").find(rest)?.range?.first ?: rest.length
        return rest.substring(0, end)
    }

    private fun specProperties(name: String): List<String> {
        val b = block(name)
        val start = b.indexOf("      properties:\n")
        assertThat(start).describedAs("%s has a properties block", name).isNotEqualTo(-1)
        return b.substring(start)
            .lines()
            .drop(1)
            .takeWhile { it.isBlank() || it.startsWith("        ") }
            .mapNotNull { Regex("^        ([a-z_]+):").find(it)?.groupValues?.get(1) }
    }

    private fun specRequired(name: String): List<String> {
        val body =
            Regex("""required: \[([^]]*)]""", RegexOption.DOT_MATCHES_ALL).find(block(name))!!.groupValues[1]
        return body.split(",").map { it.trim() }.filter { it.isNotEmpty() }
    }

    private fun keysOf(dto: Any): List<String> =
        mapper.readTree(mapper.writeValueAsString(dto)).fieldNames().asSequence().toList()

    private fun value() = PartitionListingValueDto(field = "ts_day", raw = "20713", decoded = "2026-09-17")

    private fun specField() =
        PartitionSpecFieldDto(field = "url_bucket", transform = "bucket", transformParam = 16, sourceFieldId = 4)

    private fun group() =
        PartitionGroupDto(
            specId = 2,
            values = listOf(value()),
            fileCount = 9,
            smallFileCount = 7,
            totalBytes = 900,
            smallFileBytes = 700,
            avgFileBytes = 100,
            dvCount = 1,
            debtScore = 7,
            recordCount = 4200,
            lastWrittenSnapshot = 88,
        )

    private fun full() =
        PartitionListingDto(
            sampledAt = Instant.parse("2026-09-29T12:00:00Z"),
            sampleStarted = Instant.parse("2026-09-29T11:32:00Z"),
            sampledSnapshotId = 91,
            spec = PartitionSpecSummaryDto(specId = 2, fields = listOf(specField())),
            total = 3,
            staleSpecGroups = 1,
            partitions = listOf(group()),
        )

    /** The warm-up state, and an unpartitioned table: every nullable unset. */
    private fun minimal() = full().copy(sampledAt = null, sampleStarted = null, sampledSnapshotId = null, spec = null)

    @Test
    fun `every DTO property is a declared spec property, and vice versa`() {
        assertThat(keysOf(full())).containsExactlyInAnyOrderElementsOf(specProperties("PartitionListing"))
        assertThat(keysOf(group())).containsExactlyInAnyOrderElementsOf(specProperties("PartitionGroup"))
        assertThat(keysOf(value())).containsExactlyInAnyOrderElementsOf(specProperties("PartitionListingValue"))
        assertThat(keysOf(specField()))
            .containsExactlyInAnyOrderElementsOf(specProperties("PartitionSpecField"))
        assertThat(keysOf(full().spec!!))
            .containsExactlyInAnyOrderElementsOf(specProperties("PartitionSpecSummary"))
    }

    @Test
    fun `the required lists are exactly what a minimal response always carries`() {
        assertThat(keysOf(minimal()))
            .describedAs("a listing with no sample and an unpartitioned table")
            .containsExactlyInAnyOrderElementsOf(specRequired("PartitionListing"))
        assertThat(keysOf(group().copy(specId = null)))
            .describedAs("an unpartitioned vintage's group")
            .containsExactlyInAnyOrderElementsOf(specRequired("PartitionGroup"))
        assertThat(keysOf(specField().copy(transformParam = null)))
            .describedAs("a non-bucket partition field")
            .containsExactlyInAnyOrderElementsOf(specRequired("PartitionSpecField"))
    }

    @Test
    fun `the four fields where null is an answer are sent as null rather than omitted`() {
        // Each of these distinguishes "not measured" or "no sample"
        // from zero, so an omitted key would be read as the wrong one.
        val wire = mapper.writeValueAsString(minimal())
        assertThat(wire).contains(
            "\"sampled_at\":null",
            "\"sample_started\":null",
            "\"sampled_snapshot_id\":null",
        )
        // ...and the spec must not be one of them: an unpartitioned
        // table has no spec, which NON_NULL omission says plainly.
        assertThat(keysOf(minimal())).doesNotContain("spec")

        val unsampled =
            mapper.writeValueAsString(group().copy(recordCount = null, lastWrittenSnapshot = null))
        assertThat(unsampled).contains("\"record_count\":null", "\"last_written_snapshot\":null")

        val nullValue = mapper.writeValueAsString(value().copy(raw = null, decoded = null))
        assertThat(nullValue).contains("\"raw\":null", "\"decoded\":null")
    }

    @Test
    fun `the spec's sort enum is exactly the service's sort columns`() {
        // The enum is the contract a client generates a dropdown from,
        // and the service is what answers it. A column added to one and
        // not the other is a 400 the client cannot see coming.
        val path = spec.substringAfter("      operationId: listTablePartitions")
        val enum =
            Regex("""enum: \[([^]]*)]""", RegexOption.DOT_MATCHES_ALL).find(path)!!.groupValues[1]
                .split(",").map { it.trim() }.filter { it.isNotEmpty() }
        assertThat(enum)
            .containsExactlyInAnyOrderElementsOf(
                PartitionListingService.SortColumn.entries.map { it.wire },
            )
    }
}
