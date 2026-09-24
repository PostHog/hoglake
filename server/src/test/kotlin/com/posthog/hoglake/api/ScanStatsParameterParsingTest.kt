package com.posthog.hoglake.api

import com.posthog.hoglake.model.HoglakeException
import io.ktor.server.plugins.BadRequestException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * `include` and `stats_fields` at the parser, without a database.
 *
 * The HTTP-level contract lives in `FileStatsApiTest` (statuses,
 * repeated occurrences, the route's own joining). What lives here is
 * the arithmetic of the two caps, because it is the part with an edge
 * the wire cannot reach comfortably: the raw-token guard trips at a
 * hundred thousand values, which is a query string no test client
 * should be asked to carry.
 *
 * THE ORDER IS THE POINT. Both caps count DISTINCT values, after the
 * dedupe. Counting raw tokens instead refuses the parameters' own
 * canonical encoding: under `style: form, explode: true` a generated
 * client sends `include=column_stats&include=column_stats` for a
 * two-element list, and the route joins occurrences with commas before
 * this parser sees them, so "one legal value, repeated" is the SHAPE a
 * conforming client produces — not an abuse. A raw-token cap of
 * sixteen turned that into a 400.
 */
class ScanStatsParameterParsingTest {
    @Test
    fun `one legal include value repeated past the distinct cap is still one value`() {
        val repeated = (1..MAX_SCAN_INCLUDES + 1).joinToString(",") { "column_stats" }
        assertThat(parseScanIncludes(repeated)).containsExactly("column_stats")
        // ...and the request it belongs to is accepted, not refused.
        assertThat(parseScanStatsRequest(repeated, null)).isNotNull()
    }

    @Test
    fun `include is capped on DISTINCT values`() {
        val sixteen = (1..MAX_SCAN_INCLUDES).joinToString(",") { "v$it" }
        // Sixteen distinct values is a well-formed list; they are simply
        // not values this server knows, which is the 422 arm.
        assertThatThrownBy { parseScanIncludes(sixteen) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { parseScanIncludes("$sixteen,v17") }
            .isInstanceOf(BadRequestException::class.java)
            .hasMessageContaining("distinct")
    }

    @Test
    fun `one field id repeated past the distinct cap is still one field id`() {
        // 10,001 tokens, one value. Before the dedupe moved first this
        // was a 400 saying the caller had named more than 10,000 field
        // ids, which was not true by a factor of ten thousand.
        val repeated = (1..MAX_STATS_FIELDS + 1).joinToString(",") { "7" }
        assertThat(parseStatsFields(repeated)).containsExactly(7L)
    }

    @Test
    fun `stats_fields is capped on DISTINCT ids`() {
        assertThat(parseStatsFields((1..MAX_STATS_FIELDS).joinToString(","))).hasSize(MAX_STATS_FIELDS)
        assertThatThrownBy { parseStatsFields((1..MAX_STATS_FIELDS + 1).joinToString(",")) }
            .isInstanceOf(BadRequestException::class.java)
            .hasMessageContaining("distinct")
    }

    @Test
    fun `the raw-token guard bounds the work done before the dedupe`() {
        // The distinct caps cannot be evaluated without building the
        // set, so something has to bound the split. Ten times the larger
        // cap: a client sending every legal field id, twice over, is
        // still inside it.
        val atGuard = (1..MAX_RAW_SCAN_TOKENS).joinToString(",") { "7" }
        assertThat(parseStatsFields(atGuard)).containsExactly(7L)
        assertThatThrownBy { parseStatsFields("$atGuard,7") }
            .isInstanceOf(BadRequestException::class.java)
            .hasMessageContaining("more than $MAX_RAW_SCAN_TOKENS")
        assertThatThrownBy { parseScanIncludes((1..MAX_RAW_SCAN_TOKENS + 1).joinToString(",") { "column_stats" }) }
            .isInstanceOf(BadRequestException::class.java)
            .hasMessageContaining("more than $MAX_RAW_SCAN_TOKENS")
    }

    @Test
    fun `the field-id cap is the column-node cap itself, not a copy of its number`() {
        // A request may legitimately name every column node a table is
        // allowed to have. Restating the number would let the parser
        // refuse ids the DDL had just allowed.
        assertThat(MAX_STATS_FIELDS).isEqualTo(com.posthog.hoglake.service.ColumnTrees.MAX_COLUMN_NODES)
    }
}
