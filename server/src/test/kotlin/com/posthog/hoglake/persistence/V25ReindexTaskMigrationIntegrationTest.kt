package com.posthog.hoglake.persistence

import com.posthog.hoglake.Database
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * V25 — `hog_maintenance_run.task` admits `'reindex'` (#268), and nothing
 * else changes. The database arrives at V24 holding a ledger row of every
 * existing task, so the VALIDATE runs against real rows rather than an
 * empty table.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class V25ReindexTaskMigrationIntegrationTest {
    private val db = PgTestSupport.freshDatabaseAt("24")
    private val catalogId =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "INSERT INTO hog_catalog (name, data_path) VALUES ('v25', 's3://v25') RETURNING catalog_id",
            ).mapTo(Long::class.java).one()
        }

    @AfterAll
    fun tearDown() = db.close()

    private fun insert(task: String) =
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_maintenance_run (catalog_id, task, run_trigger, started_at, " +
                    "finished_at, status) VALUES (?, ?, 'loop', now(), now(), 'ok')",
                catalogId,
                task,
            )
        }

    private fun constraint(): Pair<Long, Boolean> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT count(*) AS n, bool_and(convalidated) AS ok FROM pg_constraint " +
                    "WHERE conrelid = 'hog_maintenance_run'::regclass " +
                    "AND conname = 'hog_maintenance_run_task_check'",
            ).map { rs, _ -> rs.getLong("n") to rs.getBoolean("ok") }.one()
        }

    @Test
    fun `V25 admits reindex, keeps every older task, and leaves one validated CHECK`() {
        for (task in listOf("hydrator", "expiry", "cleanup", "compaction", "verify", "retirement")) insert(task)
        assertThatThrownBy { insert("reindex") }
            .describedAs("before V25 the vocabulary refuses the new task")
            .hasMessageContaining("hog_maintenance_run_task_check")

        Database.migrate(db.dataSource)

        insert("reindex")
        assertThatThrownBy { insert("reindexx") }.hasMessageContaining("hog_maintenance_run_task_check")
        val (count, validated) = constraint()
        assertThat(count).isEqualTo(1)
        assertThat(validated).describedAs("NOT VALID must be followed by VALIDATE").isTrue()

        // Re-appliable: a rolled-back deploy simply runs it again.
        db.jdbi.useHandleUnchecked { h -> h.execute("DELETE FROM flyway_schema_history WHERE version::numeric >= 25") }
        Database.migrate(db.dataSource)
        assertThat(constraint()).isEqualTo(1L to true)
    }
}
