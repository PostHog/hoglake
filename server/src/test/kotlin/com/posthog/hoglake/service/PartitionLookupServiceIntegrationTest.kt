package com.posthog.hoglake.service

import com.posthog.hoglake.Database
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.PartitionLookupInvalidRequest
import com.posthog.hoglake.model.PartitionLookupUnavailable
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.testing.PartitionLookupFixture
import com.posthog.hoglake.testing.PgTestSupport
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.Awaitility.await
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.statement.SqlLogger
import org.jdbi.v3.core.statement.StatementContext
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.postgresql.PGConnection
import org.postgresql.util.PSQLException
import java.sql.SQLException
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PartitionLookupServiceIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val service = PartitionLookupService(db.jdbi)

    @AfterAll
    fun close() = db.close()

    @Test
    fun `exact tuple distinguishes every raw nullable component without a sample`() {
        val f = PartitionLookupFixture(db)
        f.append(listOf("a", null), listOf("a", ""), listOf("a", "null"), listOf("b", null), listOf("01", "a:b"))
        for (tuple in listOf(
            listOf("a", null),
            listOf("a", ""),
            listOf("a", "null"),
            listOf("b", null),
            listOf("01", "a:b"),
        )) {
            val got = service.lookup(f.name, "ns", "t", f.specId, tuple)
            assertThat(got.aggregates.status).isEqualTo("exact")
            assertThat(got.aggregates.totals!!.fileCount).isEqualTo(1)
            assertThat(got.aggregates.totals!!.physicalRows).isEqualTo(10)
            assertThat(got.aggregates.totals!!.totalBytes).isEqualTo(100)
            assertThat(got.aggregates.totals!!.statsState.pending).isEqualTo(1)
            assertThat(got.files.single().partitionValues).isEqualTo(tuple)
            assertThat(got.nextCursor).isNull()
        }
        val empty = service.lookup(f.name, "ns", "t", f.specId, listOf("missing", null))
        assertThat(empty.files).isEmpty()
        assertThat(empty.aggregates.totals!!.fileCount).isZero()
        assertThat(empty.aggregates.totals!!.lastFileWriteSnapshot).isNull()
        assertThat(empty.aggregates.totals!!.statsState.pending).isZero()
    }

    @Test
    fun `old and current same-arity specs never mix`() {
        val f = PartitionLookupFixture(db)
        val tuple = listOf("a", "b")
        f.append(tuple)
        f.alter.alterTable(
            f.name,
            "ns",
            "t",
            listOf(
                AlterOp.SetPartitionSpec(
                    listOf(PartitionFieldDef(2, Transform.IDENTITY), PartitionFieldDef(1, Transform.IDENTITY)),
                ),
            ),
        )
        f.append(tuple)
        val old = service.lookup(f.name, "ns", "t", f.specId, tuple)
        val current = service.lookup(f.name, "ns", "t", f.specId + 1, tuple)
        assertThat(old.files).hasSize(1)
        assertThat(current.files).hasSize(1)
        assertThat(old.files.single().dataFileId).isNotEqualTo(current.files.single().dataFileId)
        assertThatThrownBy { service.lookup(f.name, "ns", "t", f.specId, listOf("a")) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { service.lookup(f.name, "ns", "t", f.specId + 2, tuple) }
            .isInstanceOf(HoglakeException.Validation::class.java).hasMessageContaining("unavailable")
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "DELETE FROM hog_partition_spec WHERE catalog_id = ? AND table_id = ? AND spec_id = ?",
                f.catalogId,
                f.tableId,
                f.specId,
            )
        }
        assertThatThrownBy { service.lookup(f.name, "ns", "t", f.specId, tuple) }
            .isInstanceOf(HoglakeException.Validation::class.java).hasMessageContaining("expired definition")
    }

    @Test
    fun `same catalog sibling tables with same spec and tuple remain isolated`() {
        val f = PartitionLookupFixture(db)
        val tuple = listOf("a", "b")
        f.append(tuple)
        f.catalogs.createTable(
            f.name,
            "ns",
            "sibling",
            listOf(ColumnDef("a", ColType.STRING), ColumnDef("b", ColType.STRING)),
        )
        f.alter.alterTable(
            f.name,
            "ns",
            "sibling",
            listOf(
                AlterOp.SetPartitionSpec(
                    listOf(PartitionFieldDef(1, Transform.IDENTITY), PartitionFieldDef(2, Transform.IDENTITY)),
                ),
            ),
        )
        f.commits.commit(
            f.name,
            CommitRequest(
                readSnapshot = db.head(f.name),
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "sibling",
                            listOf(
                                FileRegistration(
                                    path = "s3://bucket/${f.name}/sibling.parquet",
                                    recordCount = 30,
                                    fileSizeBytes = 300,
                                    partitionValues = tuple,
                                ),
                            ),
                        ),
                    ),
            ),
        )
        val own = service.lookup(f.name, "ns", "t", f.specId, tuple)
        val sibling = service.lookup(f.name, "ns", "sibling", f.specId, tuple)
        assertThat(own.specId).isEqualTo(sibling.specId)
        assertThat(own.aggregates.totals!!.fileCount).isEqualTo(1)
        assertThat(sibling.aggregates.totals!!.fileCount).isEqualTo(1)
        assertThat(own.aggregates.totals!!.physicalRows).isEqualTo(10)
        assertThat(sibling.aggregates.totals!!.physicalRows).isEqualTo(30)
        assertThat(own.aggregates.totals!!.totalBytes).isEqualTo(100)
        assertThat(sibling.aggregates.totals!!.totalBytes).isEqualTo(300)
        assertThat(own.files).hasSize(1)
        assertThat(sibling.files).hasSize(1)
        assertThat(own.files.single().dataFileId).isNotEqualTo(sibling.files.single().dataFileId)
        assertThat(sibling.files.single().path).endsWith("/sibling.parquet")
    }

    @Test
    fun `future spec definition is unavailable at old snapshot and floor equality is readable`() {
        val f = PartitionLookupFixture(db)
        val snapshot = f.append(listOf("a", "b"))
        f.alter.alterTable(
            f.name,
            "ns",
            "t",
            listOf(
                AlterOp.SetPartitionSpec(
                    listOf(PartitionFieldDef(2, Transform.IDENTITY), PartitionFieldDef(1, Transform.IDENTITY)),
                ),
            ),
        )
        assertThatThrownBy { service.lookup(f.name, "ns", "t", f.specId + 1, listOf("a", "b"), snapshot) }
            .isInstanceOf(HoglakeException.Validation::class.java).hasMessageContaining("unavailable")
        db.jdbi.useHandleUnchecked {
                h ->
            h.execute("UPDATE hog_catalog SET earliest_snapshot_id = ? WHERE catalog_id = ?", snapshot, f.catalogId)
        }
        assertThat(service.lookup(f.name, "ns", "t", f.specId, listOf("a", "b"), snapshot).files).hasSize(1)
    }

    @Test
    fun `malformed stored tuples with extra negative or missing indices cannot match`() {
        val f = PartitionLookupFixture(db)
        f.append(*List(4) { listOf("a", "b") }.toTypedArray())
        val files = service.lookup(f.name, "ns", "t", f.specId, listOf("a", "b")).files
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_file_partition_value (catalog_id, data_file_id, key_index, value) " +
                    "VALUES (?, ?, 2, 'extra')",
                f.catalogId,
                files[0].dataFileId,
            )
            h.execute(
                "INSERT INTO hog_file_partition_value (catalog_id, data_file_id, key_index, value) " +
                    "VALUES (?, ?, -1, 'negative')",
                f.catalogId,
                files[1].dataFileId,
            )
            h.execute(
                "DELETE FROM hog_file_partition_value WHERE catalog_id = ? AND data_file_id = ? AND key_index = 1",
                f.catalogId,
                files[2].dataFileId,
            )
        }
        val got = service.lookup(f.name, "ns", "t", f.specId, listOf("a", "b"))
        assertThat(got.files.map { it.dataFileId }).containsExactly(files[3].dataFileId)
        assertThat(got.aggregates.totals!!.fileCount).isEqualTo(1)
    }

    @Test
    fun `candidate budget boundaries distinguish complete prefix from overflow`() {
        for (size in listOf(2, 3, 4)) {
            val f = PartitionLookupFixture(db)
            f.append(*List(size) { listOf(null, "  東京🚀  ") }.toTypedArray())
            val bounded = PartitionLookupService(db.jdbi, candidateLimit = 3)
            val got = bounded.lookup(f.name, "ns", "t", f.specId, listOf(null, "  東京🚀  "))
            assertThat(got.files).hasSize(minOf(size, 3))
            if (size <= 3) {
                assertThat(got.aggregates.totals!!.fileCount).isEqualTo(size.toLong())
                assertThat(got.nextCursor).isNull()
            } else {
                assertThat(got.aggregates.reason).isEqualTo("candidate_scan_limit")
                assertThat(got.nextCursor).isNotNull()
            }
        }
    }

    @Test
    fun `selective later key avoids common leading value budget exhaustion`() {
        val f = PartitionLookupFixture(db)
        f.append(
            *List(7) { listOf("common", "other") }.toTypedArray(),
            listOf("common", "wanted"),
            listOf("common", "wanted"),
        )
        val bounded = PartitionLookupService(db.jdbi, candidateLimit = 3)
        val first = bounded.lookup(f.name, "ns", "t", f.specId, listOf("common", "wanted"), limit = 1)
        assertThat(first.aggregates.totals!!.fileCount).isEqualTo(2)
        assertThat(first.files).hasSize(1)
        val second =
            bounded.lookup(
                f.name,
                "ns",
                "t",
                f.specId,
                listOf("common", "wanted"),
                limit = 1,
                cursor = first.nextCursor,
            )
        assertThat(second.files).hasSize(1)
        assertThat(second.files.single().dataFileId).isGreaterThan(first.files.single().dataFileId)
        assertThat(second.aggregates.reason).isEqualTo("continuation")
        assertThat(second.nextCursor).isNull()
        val missing = bounded.lookup(f.name, "ns", "t", f.specId, listOf("common", "missing"))
        assertThat(missing.aggregates.totals!!.fileCount).isZero()
        assertThat(missing.files).isEmpty()
    }

    @Test
    fun `candidate cap yields sparse advancing pages and never exact suffix totals`() {
        val f = PartitionLookupFixture(db)
        f.append(
            *List(7) { listOf("pivot", "unrelated") }.toTypedArray(),
            *List(7) { listOf("other", "wanted") }.toTypedArray(),
            listOf("pivot", "wanted"),
            listOf("pivot", "wanted"),
        )
        val bounded = PartitionLookupService(db.jdbi, candidateLimit = 3)
        val seen = mutableListOf<Long>()
        val cursors = mutableSetOf<String>()
        var cursor: String? = null
        var emptyPages = 0
        do {
            val got = bounded.lookup(f.name, "ns", "t", f.specId, listOf("pivot", "wanted"), limit = 1, cursor = cursor)
            assertThat(got.aggregates.status).isEqualTo("unavailable")
            assertThat(got.aggregates.reason).isEqualTo(if (cursor == null) "candidate_scan_limit" else "continuation")
            assertThat(got.aggregates.totals).isNull()
            if (got.files.isEmpty()) emptyPages++
            seen += got.files.map { it.dataFileId }
            cursor = got.nextCursor
            if (cursor != null) assertThat(cursors.add(cursor)).isTrue()
            assertThat(cursors.size).isLessThan(10)
        } while (cursor != null)
        assertThat(emptyPages).isEqualTo(1)
        assertThat(seen).hasSize(2).doesNotHaveDuplicates()
    }

    @Test
    fun `filled pages resume at returned file rather than skip remaining window matches`() {
        val f = PartitionLookupFixture(db)
        f.append(*List(8) { listOf("a", "b") }.toTypedArray())
        val bounded = PartitionLookupService(db.jdbi, candidateLimit = 3)
        val ids = mutableListOf<Long>()
        var cursor: String? = null
        do {
            val page = bounded.lookup(f.name, "ns", "t", f.specId, listOf("a", "b"), limit = 2, cursor = cursor)
            ids += page.files.map { it.dataFileId }
            cursor = page.nextCursor
            assertThat(ids.size).isLessThanOrEqualTo(8)
        } while (cursor != null)
        assertThat(ids).hasSize(8).doesNotHaveDuplicates().isSorted()
    }

    @Test
    fun `continuations freeze visibility and reject changed tuple spec limit catalog and table`() {
        val f = PartitionLookupFixture(db)
        f.append(*List(3) { listOf("a", "b") }.toTypedArray())
        val first = service.lookup(f.name, "ns", "t", f.specId, listOf("a", "b"), limit = 1)
        val frozen = first.snapshotId
        val nextHead = f.append(listOf("a", "b"))
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_data_file SET end_snapshot = ? " +
                    "WHERE catalog_id = ? AND data_file_id > ? AND begin_snapshot <= ?",
                nextHead,
                f.catalogId,
                first.files.single().dataFileId,
                frozen,
            )
        }
        val second = service.lookup(f.name, "ns", "t", f.specId, listOf("a", "b"), limit = 1, cursor = first.nextCursor)
        assertThat(second.snapshotId).isEqualTo(frozen)
        assertThat(second.files).hasSize(1)
        assertThat(second.aggregates.status).isEqualTo("unavailable")
        assertThat(second.aggregates.reason).isEqualTo("continuation")
        assertThat(second.aggregates.totals).isNull()
        assertThatThrownBy {
            service.lookup(f.name, "ns", "t", f.specId, listOf("a", null), limit = 1, cursor = first.nextCursor)
        }.isInstanceOf(PartitionLookupInvalidRequest::class.java)
        assertThatThrownBy {
            service.lookup(f.name, "ns", "t", f.specId + 1, listOf("a", "b"), limit = 1, cursor = first.nextCursor)
        }.isInstanceOf(PartitionLookupInvalidRequest::class.java)
        assertThatThrownBy {
            service.lookup(f.name, "ns", "t", f.specId, listOf("a", "b"), limit = 2, cursor = first.nextCursor)
        }.isInstanceOf(PartitionLookupInvalidRequest::class.java)
        val originalCursor = PartitionLookupCursor.parse(first.nextCursor!!)
        assertThatThrownBy {
            service.lookup(
                f.name,
                "ns",
                "t",
                f.specId,
                listOf("a", "b"),
                limit = 2,
                cursor = originalCursor.copy(limit = 2).encode(),
            )
        }.isInstanceOf(PartitionLookupInvalidRequest::class.java)
        assertThatThrownBy {
            service.lookup(
                f.name,
                "ns",
                "t",
                f.specId,
                listOf("a", "b"),
                limit = 1,
                cursor = originalCursor.copy(snapshot = frozen - 1).encode(),
            )
        }.isInstanceOf(PartitionLookupInvalidRequest::class.java)
        assertThatThrownBy {
            service.lookup(
                f.name,
                "ns",
                "t",
                f.specId,
                listOf("a", "b"),
                snapshot = nextHead,
                limit = 1,
                cursor = first.nextCursor,
            )
        }.isInstanceOf(PartitionLookupInvalidRequest::class.java)
        val other = PartitionLookupFixture(db)
        other.append(listOf("a", "b"))
        assertThatThrownBy {
            service.lookup(other.name, "ns", "t", other.specId, listOf("a", "b"), limit = 1, cursor = first.nextCursor)
        }.isInstanceOf(PartitionLookupInvalidRequest::class.java)
    }

    @Test
    fun `expired snapshot refuses continuation and future snapshot refuses lookup`() {
        val f = PartitionLookupFixture(db)
        f.append(listOf("a", "b"), listOf("a", "b"))
        val first = service.lookup(f.name, "ns", "t", f.specId, listOf("a", "b"), limit = 1)
        f.append(listOf("a", "b"))
        db.jdbi.useHandleUnchecked {
                h ->
            h.execute(
                "UPDATE hog_catalog SET earliest_snapshot_id = ? WHERE catalog_id = ?",
                first.snapshotId + 1,
                f.catalogId,
            )
        }
        assertThatThrownBy {
            service.lookup(f.name, "ns", "t", f.specId, listOf("a", "b"), limit = 1, cursor = first.nextCursor)
        }.isInstanceOf(HoglakeException.Expired::class.java)
        assertThatThrownBy {
            service.lookup(f.name, "ns", "t", f.specId, listOf("a", "b"), snapshot = db.head(f.name) + 1)
        }.isInstanceOf(HoglakeException.Validation::class.java)
    }

    @Test
    fun `aggregate bigint overflow is unavailable while files remain browseable`() {
        val f = PartitionLookupFixture(db)
        f.append(listOf("a", "b"), listOf("a", "b"))
        db.jdbi.useHandleUnchecked {
                h ->
            h.execute("UPDATE hog_data_file SET file_size_bytes = ? WHERE catalog_id = ?", Long.MAX_VALUE, f.catalogId)
        }
        val got = service.lookup(f.name, "ns", "t", f.specId, listOf("a", "b"))
        assertThat(got.aggregates.reason).isEqualTo("overflow")
        assertThat(got.aggregates.totals).isNull()
        assertThat(got.files).hasSize(2)
    }

    @Test
    fun `continuous appends cannot extend a cursor beyond its original candidate highwater`() {
        val f = PartitionLookupFixture(db)
        f.append(
            *List(7) { listOf("pivot", "other") }.toTypedArray(),
            *List(7) { listOf("other", "wanted") }.toTypedArray(),
            listOf("pivot", "wanted"),
            listOf("pivot", "wanted"),
        )
        val bounded = PartitionLookupService(db.jdbi, candidateLimit = 3)
        val first = bounded.lookup(f.name, "ns", "t", f.specId, listOf("pivot", "wanted"), limit = 1)
        val originalUpper = PartitionLookupCursor.parse(first.nextCursor!!).upper
        val seen = mutableListOf<Long>()
        var cursor = first.nextCursor
        var pages = 1
        while (cursor != null) {
            f.append(*List(4) { listOf("pivot", "wanted") }.toTypedArray())
            val got = bounded.lookup(f.name, "ns", "t", f.specId, listOf("pivot", "wanted"), limit = 1, cursor = cursor)
            assertThat(got.snapshotId).isEqualTo(first.snapshotId)
            assertThat(got.files.all { it.dataFileId < originalUpper }).isTrue()
            seen += got.files.map { it.dataFileId }
            cursor = got.nextCursor
            if (cursor != null) assertThat(PartitionLookupCursor.parse(cursor).upper).isEqualTo(originalUpper)
            pages++
            assertThat(pages).isLessThan(6)
        }
        assertThat(seen).hasSize(2).doesNotHaveDuplicates()
    }

    @Test
    fun `DV latest-version seek preserves gaps and ended history without changing physical rows`() {
        val f = PartitionLookupFixture(db)
        val first = f.append(listOf("a", "b"))
        val file = service.lookup(f.name, "ns", "t", f.specId, listOf("a", "b")).files.single()
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                """
                INSERT INTO hog_snapshot (catalog_id, snapshot_id, schema_version)
                SELECT catalog_id, g, schema_version FROM hog_catalog CROSS JOIN generate_series(?, ?) g
                WHERE catalog_id = ?
                """,
                first + 1,
                first + 4,
                f.catalogId,
            )
            h.execute("UPDATE hog_catalog SET last_snapshot_id = ? WHERE catalog_id = ?", first + 4, f.catalogId)
            h.execute(
                """
                INSERT INTO hog_delete_file (catalog_id, delete_file_id, table_id, data_file_id,
                    begin_snapshot, end_snapshot, path, delete_count, file_size_bytes)
                VALUES (?, 900001, ?, ?, ?, ?, 's3://fixture/dv1', 1, 1),
                       (?, 900002, ?, ?, ?, ?, 's3://fixture/dv2', 1, 1)
                """,
                f.catalogId, f.tableId, file.dataFileId, first + 1, first + 2,
                f.catalogId, f.tableId, file.dataFileId, first + 3, first + 4,
            )
        }
        for ((offset, count) in listOf(0 to 0L, 1 to 1L, 2 to 0L, 3 to 1L, 4 to 0L)) {
            val totals =
                service.lookup(
                    f.name,
                    "ns",
                    "t",
                    f.specId,
                    listOf("a", "b"),
                    first + offset,
                ).aggregates.totals!!
            assertThat(totals.dvCount).isEqualTo(count)
            assertThat(totals.physicalRows).isEqualTo(10)
            assertThat(totals.lastFileWriteSnapshot).isEqualTo(first)
        }
    }

    @Test
    fun `omitted continuation limit inherits fifty while an explicit hundred is refused`() {
        val f = PartitionLookupFixture(db)
        f.append(*Array(101) { listOf("a", "b") })
        val first = service.lookup(f.name, "ns", "t", f.specId, listOf("a", "b"), limit = 50)
        assertThat(first.files).hasSize(50)
        val second = service.lookup(f.name, "ns", "t", f.specId, listOf("a", "b"), cursor = first.nextCursor)
        assertThat(second.files).hasSize(50)
        assertThat(second.files.map { it.dataFileId }).doesNotContainAnyElementsOf(first.files.map { it.dataFileId })
        assertThat(PartitionLookupCursor.parse(second.nextCursor!!).limit).isEqualTo(50)
        assertThatThrownBy {
            service.lookup(f.name, "ns", "t", f.specId, listOf("a", "b"), limit = 100, cursor = first.nextCursor)
        }.isInstanceOf(PartitionLookupInvalidRequest::class.java)
    }

    @Test
    fun `remaining statement budget cancels with 57014 and reuses the same pooled backend`() {
        val f = PartitionLookupFixture(db)
        f.append(listOf("a", "b"))
        lookupPool().use { pool ->
            val instrumented = Database.jdbi(pool)
            val firstPid = pool.connection.use { it.unwrap(PGConnection::class.java).backendPID }
            val failure = AtomicReference<SQLException>()
            val backstopMargin = AtomicReference<Long>()
            val delayed = AtomicInteger()
            instrumented.setSqlLogger(
                object : SqlLogger {
                    override fun logBeforeExecution(context: StatementContext) {
                        if (context.rawSql.trimStart().startsWith("SELECT") && !context.rawSql.contains("set_config")) {
                            val queryNumber = delayed.incrementAndGet()
                            if (queryNumber == 1) {
                                context.connection.createStatement().use { statement ->
                                    statement.executeQuery(
                                        "SELECT EXTRACT(EPOCH FROM " +
                                            "current_setting('transaction_timeout')::interval) * 1000 - " +
                                            "EXTRACT(EPOCH FROM current_setting('statement_timeout')::interval) * 1000",
                                    ).use { rows ->
                                        check(rows.next())
                                        backstopMargin.set(rows.getLong(1))
                                    }
                                }
                            }
                            if (queryNumber <= 2) Thread.sleep(200)
                        }
                    }

                    override fun logException(
                        context: StatementContext,
                        ex: SQLException,
                    ) {
                        failure.set(ex)
                    }
                },
            )
            val bounded = PartitionLookupService(instrumented, statementTimeoutMs = 700)
            db.jdbi.useHandleUnchecked { lock ->
                lock.begin()
                lock.execute("LOCK TABLE hog_data_file IN ACCESS EXCLUSIVE MODE")
                try {
                    val started = System.nanoTime()
                    assertThatThrownBy { bounded.lookup(f.name, "ns", "t", f.specId, listOf("a", "b")) }
                        .isInstanceOf(PartitionLookupUnavailable::class.java).extracting("reason").isEqualTo("timeout")
                    assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)).isLessThan(950)
                    assertThat(backstopMargin.get()!!).isGreaterThan(3_000)
                    assertThat(failure.get()?.sqlState).isEqualTo("57014")
                    assertThat((failure.get() as PSQLException).serverErrorMessage?.severity).isEqualTo("ERROR")
                } finally {
                    lock.rollback()
                }
            }
            pool.connection.use { connection ->
                assertThat(connection.unwrap(PGConnection::class.java).backendPID).isEqualTo(firstPid)
                connection.createStatement().use { statement ->
                    statement.executeQuery(
                        "SELECT current_setting('transaction_timeout'), current_setting('statement_timeout'), 1",
                    )
                        .use { rows ->
                            assertThat(rows.next()).isTrue()
                            assertThat(rows.getString(1)).isEqualTo("0")
                            assertThat(rows.getString(2)).isEqualTo("1min")
                            assertThat(rows.getInt(3)).isEqualTo(1)
                        }
                }
            }
            instrumented.setSqlLogger(SqlLogger.NOP_SQL_LOGGER)
            assertThat(bounded.lookup(f.name, "ns", "t", f.specId, listOf("a", "b")).files).hasSize(1)
        }
    }

    @Test
    fun `connection acquisition consumes the request budget before opening a transaction`() {
        val f = PartitionLookupFixture(db)
        lookupPool().use { pool ->
            val instrumented = Database.jdbi(pool)
            val began = AtomicBoolean()
            instrumented.setSqlLogger(
                object : SqlLogger {
                    override fun logBeforeExecution(context: StatementContext) {
                        if (context.rawSql.startsWith("SET TRANSACTION")) began.set(true)
                    }
                },
            )
            val bounded = PartitionLookupService(instrumented, statementTimeoutMs = 100)
            val borrowed = pool.connection
            Executors.newSingleThreadExecutor().use { executor ->
                val result =
                    executor.submit<Throwable?> {
                        try {
                            bounded.lookup(f.name, "ns", "t", f.specId, listOf("a", "b"))
                            null
                        } catch (e: Throwable) {
                            e
                        }
                    }
                try {
                    await().pollInterval(Duration.ofMillis(1)).atMost(Duration.ofSeconds(1))
                        .until { pool.hikariPoolMXBean.threadsAwaitingConnection == 1 }
                    Thread.sleep(150)
                } finally {
                    borrowed.close()
                }
                assertThat(result.get(1, TimeUnit.SECONDS)).isInstanceOf(PartitionLookupUnavailable::class.java)
                    .extracting("reason").isEqualTo("timeout")
                assertThat(began.get()).isFalse()
            }
        }
    }

    private fun lookupPool() =
        HikariDataSource(
            HikariConfig().apply {
                jdbcUrl = db.jdbcUrl
                username = PgTestSupport.USER
                password = PgTestSupport.PASSWORD
                maximumPoolSize = 1
                connectionInitSql = Database.SESSION_INIT_SQL
            },
        )

    @Test
    fun `one catalog cannot occupy both lookup permits`() {
        val f = PartitionLookupFixture(db)
        val other = PartitionLookupFixture(db)
        f.append(listOf("a", "b"))
        other.append(listOf("a", "b"))
        val arrived = CountDownLatch(1)
        val proceed = CountDownLatch(1)
        val blocked = AtomicBoolean()
        val instrumented = Database.jdbi(db.dataSource)
        instrumented.setSqlLogger(
            object : SqlLogger {
                override fun logBeforeExecution(context: StatementContext) {
                    if (context.rawSql.contains("WITH probes AS MATERIALIZED") && blocked.compareAndSet(false, true)) {
                        arrived.countDown()
                        check(proceed.await(5, TimeUnit.SECONDS))
                    }
                }
            },
        )
        val bounded = PartitionLookupService(instrumented)
        Executors.newSingleThreadExecutor().use { executor ->
            val first = executor.submit { bounded.lookup(f.name, "ns", "t", f.specId, listOf("a", "b")) }
            try {
                assertThat(arrived.await(5, TimeUnit.SECONDS)).isTrue()
                val before = db.dataSource.hikariPoolMXBean.activeConnections
                assertThatThrownBy { bounded.lookup(f.name, "ns", "t", f.specId, listOf("a", "b")) }
                    .isInstanceOf(PartitionLookupUnavailable::class.java).extracting("reason").isEqualTo("busy")
                assertThat(db.dataSource.hikariPoolMXBean.activeConnections).isEqualTo(before)
                assertThat(bounded.lookup(other.name, "ns", "t", other.specId, listOf("a", "b")).files).hasSize(1)
            } finally {
                proceed.countDown()
            }
            first.get(5, TimeUnit.SECONDS)
        }
        assertThat(bounded.lookup(f.name, "ns", "t", f.specId, listOf("a", "b")).files).hasSize(1)
    }

    @Test
    fun `lookup statements share one transaction time budget and release admission after termination`() {
        val f = PartitionLookupFixture(db)
        f.append(listOf("a", "b"))
        val instrumented = Database.jdbi(db.dataSource)
        instrumented.setSqlLogger(
            object : SqlLogger {
                override fun logBeforeExecution(context: StatementContext) {
                    if (context.rawSql.trimStart().startsWith("SELECT")) Thread.sleep(180)
                }
            },
        )
        val bounded = PartitionLookupService(instrumented, statementTimeoutMs = 300)
        val started = System.nanoTime()
        assertThatThrownBy { bounded.lookup(f.name, "ns", "t", f.specId, listOf("a", "b")) }
            .isInstanceOf(PartitionLookupUnavailable::class.java).extracting("reason").isEqualTo("timeout")
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)).isLessThan(1000)
        instrumented.setSqlLogger(SqlLogger.NOP_SQL_LOGGER)
        assertThat(bounded.lookup(f.name, "ns", "t", f.specId, listOf("a", "b")).files).hasSize(1)
    }

    @Test
    fun `admission rejects before borrowing and timed-out lookup releases connection and permit`() {
        val f = PartitionLookupFixture(db)
        f.append(listOf("a", "b"))
        val arrived = CountDownLatch(1)
        val proceed = CountDownLatch(1)
        val instrumented = Database.jdbi(db.dataSource)
        instrumented.setSqlLogger(
            object : SqlLogger {
                override fun logBeforeExecution(context: StatementContext) {
                    if (context.rawSql.contains("WITH probes AS MATERIALIZED")) {
                        arrived.countDown()
                        check(proceed.await(5, TimeUnit.SECONDS))
                    }
                }
            },
        )
        val bounded = PartitionLookupService(instrumented, statementTimeoutMs = 250, maxConcurrentRequests = 1)
        val executor = Executors.newSingleThreadExecutor()
        try {
            db.jdbi.useHandleUnchecked { lock ->
                lock.begin()
                lock.execute("LOCK TABLE hog_data_file IN ACCESS EXCLUSIVE MODE")
                val future =
                    executor.submit<Throwable?> {
                        try {
                            bounded.lookup(f.name, "ns", "t", f.specId, listOf("a", "b"))
                            null
                        } catch (e: Throwable) {
                            e
                        }
                    }
                assertThat(arrived.await(5, TimeUnit.SECONDS)).isTrue()
                val before = db.dataSource.hikariPoolMXBean.activeConnections
                assertThatThrownBy { bounded.lookup(f.name, "ns", "t", f.specId, listOf("a", "b")) }
                    .isInstanceOf(PartitionLookupUnavailable::class.java).extracting("reason").isEqualTo("busy")
                assertThat(db.dataSource.hikariPoolMXBean.activeConnections).isEqualTo(before)
                proceed.countDown()
                assertThat(
                    future.get(1500, TimeUnit.MILLISECONDS),
                ).isInstanceOf(PartitionLookupUnavailable::class.java).extracting("reason").isEqualTo("timeout")
                lock.rollback()
            }
            assertThat(bounded.lookup(f.name, "ns", "t", f.specId, listOf("a", "b")).files).hasSize(1)
        } finally {
            proceed.countDown()
            executor.shutdownNow()
        }
    }
}
