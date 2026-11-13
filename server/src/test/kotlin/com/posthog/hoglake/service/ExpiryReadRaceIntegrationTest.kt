package com.posthog.hoglake.service

import com.posthog.hoglake.Database
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.statement.SqlLogger
import org.jdbi.v3.core.statement.StatementContext
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.time.Instant
import java.time.ZoneOffset
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

/** Expiry interposes between a pinned read's floor check and its last metadata statement. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ExpiryReadRaceIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val alters = AlterService(db.jdbi)
    private val expiry = ExpiryService(db.jdbi)

    @AfterAll
    fun tearDown() = db.close()

    @ParameterizedTest
    @ValueSource(strings = ["table", "table-time", "listing", "listing-time"])
    fun `pinned metadata reads refuse a floor advance before their last statement`(mode: String) {
        val name = "read-race-${UUID.randomUUID().toString().take(8)}"
        val cat = catalogs.createCatalog(name, "s3://bucket/$name")
        catalogs.createNamespace(name, "ns")
        catalogs.createTable(name, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val at = db.head(name)
        val time = Instant.parse("2026-01-01T00:00:00Z")
        db.jdbi.useHandleUnchecked { h ->
            h.execute("UPDATE hog_catalog SET snapshot_retention_seconds = 60 WHERE catalog_id = ?", cat.catalogId)
            h.execute(
                "UPDATE hog_snapshot SET snapshot_time = ? WHERE catalog_id = ?",
                time.atOffset(ZoneOffset.UTC),
                cat.catalogId,
            )
        }
        val watched = Database.jdbi(db.dataSource)
        val fired = AtomicBoolean()
        watched.setSqlLogger(
            object : SqlLogger {
                override fun logBeforeExecution(context: StatementContext) {
                    val matches =
                        if (mode.startsWith("table")) {
                            "FROM hog_column" in context.rawSql
                        } else {
                            "WITH bounds AS" in context.rawSql
                        }
                    if (matches && fired.compareAndSet(false, true)) {
                        alters.alterTable(name, "ns", "t", listOf(AlterOp.RenameColumn("id", "renamed")))
                        commits.commit(
                            name,
                            CommitRequest(
                                appends =
                                    listOf(
                                        TableAppend(
                                            "ns",
                                            "t",
                                            listOf(
                                                FileRegistration("s3://bucket/$name/new.parquet", 10, 100, 50),
                                            ),
                                        ),
                                    ),
                            ),
                        )
                        db.jdbi.useHandleUnchecked { h ->
                            h.execute(
                                "UPDATE hog_snapshot SET snapshot_time = ? WHERE catalog_id = ?",
                                time.atOffset(ZoneOffset.UTC),
                                cat.catalogId,
                            )
                        }
                        expiry.runOnce(name, 100)
                    }
                }
            },
        )
        val reader = CatalogService(watched)
        assertThatThrownBy {
            val snapshot = at.takeUnless { mode.endsWith("-time") }
            val timestamp = time.takeIf { mode.endsWith("-time") }
            if (mode.startsWith("table")) {
                reader.getTable(name, "ns", "t", snapshot = snapshot, atTimestamp = timestamp)
            } else {
                reader.listTables(name, "ns", snapshot = snapshot, atTimestamp = timestamp)
            }
        }.isExactlyInstanceOf(HoglakeException.Expired::class.java)
        assertThat(fired).isTrue()
        assertThat(catalogs.getCatalog(name).earliestSnapshotId).isGreaterThan(at)
    }

    @ParameterizedTest
    @ValueSource(strings = ["table", "listing"])
    fun `head metadata reads add no retention probe`(mode: String) {
        val name = "head-read-${UUID.randomUUID().toString().take(8)}"
        catalogs.createCatalog(name, "s3://bucket/$name")
        catalogs.createNamespace(name, "ns")
        catalogs.createTable(name, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val watched = Database.jdbi(db.dataSource)
        val floorRead = AtomicBoolean()
        watched.setSqlLogger(
            object : SqlLogger {
                override fun logBeforeExecution(context: StatementContext) {
                    if (context.rawSql.trimStart().startsWith("SELECT earliest_snapshot_id")) floorRead.set(true)
                }
            },
        )
        val reader = CatalogService(watched)
        if (mode == "table") {
            assertThat(reader.getTable(name, "ns", "t").columns).hasSize(1)
        } else {
            assertThat(reader.listTables(name, "ns")).hasSize(1)
        }
        assertThat(floorRead).isFalse()
    }
}
