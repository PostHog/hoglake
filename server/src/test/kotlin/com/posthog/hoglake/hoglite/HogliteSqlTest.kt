package com.posthog.hoglake.hoglite

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant

/**
 * The snapshot row every DDL and commit transaction writes. Its ts is the
 * engine's clock read inside the transaction, clamped to the previous
 * snapshot's ts, so a pod's clock never reaches the column: a fast replica
 * would pin every later snapshot forward through the clamp, and a commit
 * that queued before landing would record a time before it was visible
 * (#672). Postgres stamps `clock_timestamp()` under the commit lock.
 */
class HogliteSqlTest {
    @Test
    fun `the snapshot row stamps the engine clock inside the transaction, under the monotonic clamp`() {
        val tx = HogliteTx()
        tx.insertSnapshotRow("me", "hello")

        val statement = tx.statements().single()
        assertThat(statement.sql).isEqualTo(
            "INSERT INTO snapshots(id, ts, author, message, schema_version) " +
                "VALUES ((SELECT next_snapshot - 1 FROM meta WHERE id = 1), " +
                "MAX(COALESCE((SELECT ts FROM snapshots ORDER BY id DESC LIMIT 1), ''), " +
                "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')), " +
                "?, ?, (SELECT schema_version FROM meta WHERE id = 1))",
        )
        // Author and message only: no timestamp is bound from this JVM.
        assertThat(statement.args).containsExactly("me", "hello")
        assertThat(statement.expectAffected).isNull()
        assertThat(statement.expectScalarInt).isNull()
    }

    /**
     * The engine writes what the codec writes, byte for byte, so the rows
     * of an existing tenant and the rows stamped from now on sort and parse
     * as one column: `%Y-%m-%d`, `T`, `%H:%M:%f` (SQLite's `%f` is
     * `SS.SSS`), `Z` — the 24 bytes of [HogliteSchema.fmt].
     */
    @Test
    fun `the engine clock expression spells the codec's fixed-width UTC millis`() {
        assertThat(HogliteSchema.NOW_SQL).isEqualTo("strftime('%Y-%m-%dT%H:%M:%fZ', 'now')")
        val sample = HogliteSchema.fmt(Instant.parse("2026-11-04T12:00:00.007Z"))
        assertThat(sample).isEqualTo("2026-11-04T12:00:00.007Z").hasSize(24)
        assertThat(HogliteSchema.parse(sample)).isEqualTo(Instant.parse("2026-11-04T12:00:00.007Z"))
    }

    /**
     * The retention cutoff is the same expression moved back by the bound
     * seconds (SQLite's `'-NNN seconds'` modifier, spliced from the one
     * parameter), so the predicate `ts >= cutoff` compares two readings of
     * the engine's clock in one encoding, as ExpiryService's
     * `snapshot_time >= now() - make_interval(secs => :retention)` does.
     */
    @Test
    fun `the retention cutoff is the engine clock expression minus the bound seconds`() {
        assertThat(HogliteSchema.NOW_MINUS_SECONDS_SQL)
            .isEqualTo("strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-' || ? || ' seconds')")
            .startsWith(HogliteSchema.NOW_SQL.removeSuffix(")"))
        assertThat(HogliteSchema.NOW_MINUS_SECONDS_SQL.count { it == '?' }).isEqualTo(1)
    }
}
