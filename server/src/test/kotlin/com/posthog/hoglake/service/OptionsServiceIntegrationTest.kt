package com.posthog.hoglake.service

import com.posthog.hoglake.model.CatalogOptions
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Options surface semantics — especially the PATCH tri-state on
 * snapshot_retention_seconds: absent = unchanged, explicit null =
 * expiry disabled, value = set (positive only).
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OptionsServiceIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val svc = OptionsService(db.jdbi)

    @AfterAll
    fun tearDown() = db.close()

    @Test
    fun `new catalog defaults - no retention, consumer floor on, earliest 0`() {
        catalogs.createCatalog("opt-defaults", "s3://b/opt-defaults")
        val opts = svc.get("opt-defaults")
        assertThat(opts.snapshotRetentionSeconds).isNull()
        assertThat(opts.consumerFloor).isTrue()
        assertThat(opts.earliestSnapshotId).isEqualTo(0)
    }

    @Test
    fun `patch value sets retention and leaves the absent field untouched`() {
        catalogs.createCatalog("opt-set", "s3://b/opt-set")
        val patched =
            svc.patch(
                "opt-set",
                PatchField.Set(3_600L),
                consumerFloor = null,
                author = "integration-test",
                message = "Configure test retention",
            )
        assertThat(patched.snapshotRetentionSeconds).isEqualTo(3_600L)
        assertThat(patched.consumerFloor).isTrue() // absent -> unchanged

        // Now the reverse: retention absent, floor set.
        val patched2 = svc.patch("opt-set", PatchField.Absent, consumerFloor = false)
        assertThat(patched2.snapshotRetentionSeconds).isEqualTo(3_600L) // absent -> unchanged
        assertThat(patched2.consumerFloor).isFalse()

        // Persisted, not just echoed.
        val readBack = svc.get("opt-set")
        assertThat(readBack.snapshotRetentionSeconds).isEqualTo(3_600L)
        assertThat(readBack.consumerFloor).isFalse()
    }

    @Test
    fun `patch explicit null disables expiry`() {
        catalogs.createCatalog("opt-null", "s3://b/opt-null")
        svc.patch(
            "opt-null",
            PatchField.Set(60L),
            consumerFloor = null,
            author = "integration-test",
            message = "Configure test retention",
        )
        assertThat(svc.get("opt-null").snapshotRetentionSeconds).isEqualTo(60L)

        val disabled =
            svc.patch(
                "opt-null",
                PatchField.Set(null),
                consumerFloor = null,
                author = "integration-test",
                message = "Configure test retention",
            )
        assertThat(disabled.snapshotRetentionSeconds).isNull()
        assertThat(svc.get("opt-null").snapshotRetentionSeconds).isNull()
    }

    @Test
    fun `patch with both fields absent is a no-op read`() {
        catalogs.createCatalog("opt-noop", "s3://b/opt-noop")
        svc.patch(
            "opt-noop",
            PatchField.Set(120L),
            consumerFloor = false,
            author = "integration-test",
            message = "Configure test retention",
        )
        val opts = svc.patch("opt-noop", PatchField.Absent, consumerFloor = null)
        assertThat(opts.snapshotRetentionSeconds).isEqualTo(120L)
        assertThat(opts.consumerFloor).isFalse()
    }

    @Test
    fun `non-positive retention is rejected without touching the row`() {
        catalogs.createCatalog("opt-invalid", "s3://b/opt-invalid")
        svc.patch(
            "opt-invalid",
            PatchField.Set(60L),
            consumerFloor = null,
            author = "integration-test",
            message = "Configure test retention",
        )

        for (bad in listOf(0L, -1L)) {
            assertThatThrownBy {
                svc.patch(
                    "opt-invalid",
                    PatchField.Set(bad),
                    consumerFloor = null,
                    author = "integration-test",
                    message = "Configure test retention",
                )
            }
                .isInstanceOf(HoglakeException.Validation::class.java)
        }
        assertThat(svc.get("opt-invalid").snapshotRetentionSeconds).isEqualTo(60L)
    }

    @Test
    fun `unknown catalog is NotFound for both get and patch`() {
        assertThatThrownBy { svc.get("opt-nope") }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy {
            svc.patch(
                "opt-nope",
                PatchField.Set(60L),
                consumerFloor = null,
                author = "integration-test",
                message = "Configure test retention",
            )
        }
            .isInstanceOf(HoglakeException.NotFound::class.java)
    }

    @Test
    fun `only retention reductions require attribution including unlimited to finite`() {
        catalogs.createCatalog("opt-reduction", "s3://b/reduction")
        for (missing in listOf(null, "", " \t", "\u2003", "\u0085")) {
            assertThatThrownBy { svc.patch("opt-reduction", PatchField.Set(60L), false, missing, "reason") }
                .isInstanceOf(HoglakeException.Validation::class.java).hasMessageContaining("author")
            assertThatThrownBy { svc.patch("opt-reduction", PatchField.Set(60L), false, "qe", missing) }
                .isInstanceOf(HoglakeException.Validation::class.java).hasMessageContaining("message")
            assertThat(svc.get("opt-reduction").snapshotRetentionSeconds).isNull()
            assertThat(svc.get("opt-reduction").consumerFloor).isTrue()
        }
        svc.patch("opt-reduction", PatchField.Set(60L), null, "qe", "enable")
        svc.patch("opt-reduction", PatchField.Set(60L), null)
        svc.patch("opt-reduction", PatchField.Set(120L), null)
        assertThatThrownBy { svc.patch("opt-reduction", PatchField.Set(60L), false) }
            .isInstanceOf(HoglakeException.Validation::class.java).hasMessageContaining("author")
        assertThat(svc.get("opt-reduction").snapshotRetentionSeconds).isEqualTo(120L)
        assertThat(svc.get("opt-reduction").consumerFloor).isTrue()
        svc.patch("opt-reduction", PatchField.Set(60L), null, "qe", "reduce")
        svc.patch("opt-reduction", PatchField.Set(null), null)
        svc.patch("opt-reduction", PatchField.Absent, false)
        assertThat(catalogs.getCatalog("opt-reduction").headSnapshotId).isEqualTo(0)
    }

    @Test
    fun `retention comparison waits for a competing committed update`() {
        catalogs.createCatalog("opt-race", "s3://b/race")
        svc.patch("opt-race", PatchField.Set(60L), null, "qe", "enable")
        val executor = Executors.newSingleThreadExecutor()
        try {
            lateinit var result: java.util.concurrent.Future<CatalogOptions>
            db.jdbi.inTransactionUnchecked { h ->
                h.execute("UPDATE hog_catalog SET snapshot_retention_seconds = 120 WHERE name = ?", "opt-race")
                result = executor.submit<CatalogOptions> { svc.patch("opt-race", PatchField.Set(90L), null) }
                // Wait until the patch is blocked by our catalog row. Without FOR UPDATE,
                // validation passes against 60 and only the later UPDATE waits.
                val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
                while (true) {
                    h.execute("SELECT pg_stat_clear_snapshot()")
                    val waiting =
                        h.createQuery(
                            """
                        SELECT count(*) FROM pg_stat_activity
                        WHERE datname = current_database() AND wait_event_type = 'Lock'
                          AND pid <> pg_backend_pid() AND query LIKE '%hog_catalog%'
                        """,
                        )
                            .mapTo(Int::class.java).one()
                    if (waiting > 0) break
                    check(System.nanoTime() < deadline) { "patch did not reach the catalog row lock" }
                    Thread.sleep(10)
                }
            }
            assertThatThrownBy { result.get(5, TimeUnit.SECONDS) }
                .hasCauseInstanceOf(HoglakeException.Validation::class.java)
            assertThat(svc.get("opt-race").snapshotRetentionSeconds).isEqualTo(120L)
        } finally {
            executor.shutdownNow()
        }
    }
}
