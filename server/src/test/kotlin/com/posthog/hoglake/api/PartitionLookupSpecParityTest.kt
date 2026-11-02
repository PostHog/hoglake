package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.JsonNode
import com.posthog.hoglake.model.PartitionLookup
import com.posthog.hoglake.model.PartitionLookupAggregates
import com.posthog.hoglake.model.PartitionLookupStatsState
import com.posthog.hoglake.model.PartitionLookupTotals
import com.posthog.hoglake.model.PartitionLookupUnavailable
import com.posthog.hoglake.wireObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.io.File
import java.util.UUID

class PartitionLookupSpecParityTest {
    private val spec = File("src/main/resources/openapi/hoglake.yaml").readText()
    private val mapper = wireObjectMapper()

    @Test
    fun `lookup publishes POST with a complete tuple body and lossless int64 query controls`() {
        val path = "/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/partitions/lookup"
        val block = spec.substringAfter("  $path:\n").substringBefore("\n  /catalogs/")
        assertThat(block).startsWith("    post:\n")
        assertThat(block).doesNotContain("    get:", "        - name: values")
        assertThat(block).contains(
            "      requestBody:\n        required: true",
            "              type: array",
            "              minItems: 1",
            "              maxItems: 64",
            "              items: { type: string, nullable: true, maxLength: 8192 }",
            "          schema: { type: integer, format: int64, minimum: 1 }",
            "        - { \$ref: \"#/components/parameters/snapshot\" }",
        )
    }

    private fun schema(name: String): String {
        val header = "    $name:\n"
        assertThat(spec).contains(header)
        return spec.substringAfter(header).split(Regex("\n    [A-Za-z]"), limit = 2)[0]
    }

    private fun assertShape(
        name: String,
        body: JsonNode,
    ) {
        val block = schema(name)
        val properties = Regex("(?m)^        ([a-z_]+):").findAll(block).map { it.groupValues[1] }.toList()
        val required =
            Regex("""required: \[([^]]*)]""", RegexOption.DOT_MATCHES_ALL).find(block)!!.groupValues[1]
                .split(',').map { it.trim() }
        assertThat(body.fieldNames().asSequence().toList()).containsExactlyInAnyOrderElementsOf(properties)
        assertThat(properties).containsExactlyInAnyOrderElementsOf(required)
    }

    private fun lookup(aggregates: PartitionLookupAggregates) =
        PartitionLookup(
            snapshotId = Long.MAX_VALUE,
            tableUuid = UUID.fromString("00000000-0000-0000-0000-000000000001"),
            specId = Long.MAX_VALUE,
            values = listOf(null, "", "null"),
            aggregates = aggregates,
            files = emptyList(),
            nextCursor = null,
        )

    @Test
    fun `unavailable totals and end cursor remain explicit nulls on the wire`() {
        val body =
            mapper.valueToTree<JsonNode>(
                lookup(PartitionLookupAggregates("unavailable", "candidate_scan_limit", null)).toDto(),
            )
        assertShape("PartitionLookup", body)
        assertShape("PartitionLookupAggregates", body["aggregates"])
        assertThat(body["next_cursor"].isNull).isTrue()
        assertThat(body["aggregates"]["totals"].isNull).isTrue()
        assertThat(body["values"].map { if (it.isNull) null else it.asText() }).containsExactly(null, "", "null")
        assertThat(body["snapshot_id"].isIntegralNumber).isTrue()
        assertThat(body["snapshot_id"].longValue()).isEqualTo(Long.MAX_VALUE)
        assertThat(body["spec_id"].longValue()).isEqualTo(Long.MAX_VALUE)
    }

    @Test
    fun `exact totals match the schema including absent file write and null reason`() {
        val totals =
            PartitionLookupTotals(0, Long.MAX_VALUE, Long.MAX_VALUE, 0, PartitionLookupStatsState(0, 0, 0), null)
        val body = mapper.valueToTree<JsonNode>(lookup(PartitionLookupAggregates("exact", null, totals)).toDto())
        assertShape("PartitionLookupAggregates", body["aggregates"])
        val wireTotals = body["aggregates"]["totals"]
        assertShape("PartitionLookupTotals", wireTotals)
        assertThat(body["aggregates"]["reason"].isNull).isTrue()
        assertThat(wireTotals["last_file_write_snapshot"].isNull).isTrue()
        assertThat(wireTotals["physical_rows"].isIntegralNumber).isTrue()
        assertThat(wireTotals["physical_rows"].longValue()).isEqualTo(Long.MAX_VALUE)
        assertThat(wireTotals["total_bytes"].longValue()).isEqualTo(Long.MAX_VALUE)
    }

    @Test
    fun `lookup unavailable error has its own schema without extending catalog drop reasons`() {
        val body =
            mapper.valueToTree<JsonNode>(
                errorBody(PartitionLookupUnavailable("busy", "busy")).second,
            )
        assertShape("PartitionLookupUnavailableError", body)
        assertThat(schema("PartitionLookupUnavailableError")).contains("enum: [busy, timeout]")
        assertThat(schema("ApiError").substringAfter("        reason:\n")).doesNotContain("busy", "timeout")
    }
}
