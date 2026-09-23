package com.posthog.hoglake.service

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.persistence.OffsetRepo
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList

@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TableReplacementIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val creations = TableCreationService(db.jdbi, catalogs, commits)
    private val columns = listOf(ColumnDef("id", ColType.LONG))

    @AfterAll
    fun close() = db.close()

    private fun fixture(): String {
        val cat = "replacement-${UUID.randomUUID()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ns")
        return cat
    }

    private fun prepare(
        cat: String,
        uuid: UUID?,
    ) = creations.prepare(
        cat,
        UUID.randomUUID(),
        TableCreationDefinition("ns", "t", columns, ReplacementTarget(uuid, catalogs.getCatalog(cat).headSnapshotId)),
    )

    private fun append(cat: String) =
        CommitRequest(
            readSnapshot = catalogs.getCatalog(cat).headSnapshotId,
            idempotencyKey = UUID.randomUUID(),
            appends =
                listOf(
                    TableAppend(
                        "ns",
                        "t",
                        listOf(FileRegistration("s3://bucket/$cat/${UUID.randomUUID()}.parquet", 5, 100, 20)),
                        expectedTableUuid = catalogs.getTable(cat, "ns", "t").tableUuid,
                    ),
                ),
        )

    @Test
    fun `replacement publishes one snapshot preserves history and fences stale writers and feeds`() {
        val cat = fixture()
        val old = catalogs.createTable(cat, "ns", "t", columns)
        val request = append(cat)
        val oldReceipt = commits.commit(cat, request)
        val stale = append(cat)
        val before = catalogs.getCatalog(cat).headSnapshotId
        val prepared = prepare(cat, old.tableUuid)
        assertThat(catalogs.getTable(cat, "ns", "t").tableUuid).isEqualTo(old.tableUuid)
        assertThat(catalogs.getTable(cat, "ns", "t").recordCount).isEqualTo(5)
        val files = listOf(FileRegistration(prepared.writePath + "new.parquet", 2, 100, 20))
        val receipt = creations.publish(cat, prepared.operationId, files)
        assertThat(receipt.snapshotId).isEqualTo(before + 1)
        val current = catalogs.getTable(cat, "ns", "t")
        assertThat(current.tableUuid).isEqualTo(prepared.tableUuid).isNotEqualTo(old.tableUuid)
        assertThat(current.recordCount).isEqualTo(2)
        assertThat(catalogs.getTable(cat, "ns", "t", before).tableUuid).isEqualTo(old.tableUuid)
        assertThat(catalogs.listFiles(cat, "ns", "t", before)).hasSize(1)
        assertThat(creations.publish(cat, prepared.operationId, files)).isEqualTo(receipt)
        assertThat(commits.commit(cat, request)).isEqualTo(oldReceipt)
        assertThatThrownBy { commits.commit(cat, stale) }.isInstanceOf(HoglakeException.CommitConflict::class.java)
        for (from in listOf(0L, before)) {
            assertThatThrownBy { catalogs.changes(cat, "ns", "t", from) }
                .isInstanceOf(HoglakeException.ReconciliationRequired::class.java)
        }
        assertThat(catalogs.changes(cat, "ns", "t", before + 1).files).isEmpty()
        assertThat(catalogs.getTable(cat, "ns", "t").recordCount).isEqualTo(2)
    }

    @Test
    fun `replacement feed barrier survives expiry of old version at the floor`() {
        val cat = fixture()
        val old = catalogs.createTable(cat, "ns", "t", columns)
        val prepared = prepare(cat, old.tableUuid)
        val snapshot = creations.publish(cat, prepared.operationId, emptyList()).snapshotId!!
        db.jdbi.useHandle<Exception> { h ->
            h.createUpdate(
                "UPDATE hog_catalog SET snapshot_retention_seconds = 3600, consumer_floor = false WHERE name = :cat",
            )
                .bind("cat", cat).execute()
            h.createUpdate("UPDATE hog_snapshot SET snapshot_time = now() - interval '2 days' WHERE catalog_id = :id")
                .bind("id", catalogs.getCatalog(cat).catalogId).execute()
        }
        ExpiryService(db.jdbi).runOnce(cat, batchSize = 1000)
        assertThat(catalogs.getCatalog(cat).earliestSnapshotId).isEqualTo(snapshot)
        assertThatThrownBy { catalogs.changes(cat, "ns", "t", snapshot - 1, snapshot) }
            .isInstanceOf(HoglakeException.ReconciliationRequired::class.java)
        assertThat(catalogs.changes(cat, "ns", "t", snapshot, snapshot).files).isEmpty()
    }

    // ---- consumer offsets across a replacement -----------------------------
    //
    // Offsets survive a DROP by design, so a consumer can finish reading a
    // dropped table. A REPLACEMENT is different: the consumer reconciles
    // onto the new table_uuid and never looks at the retired one again, so
    // its row there would pin the consumer_floor sweep at a pre-replacement
    // snapshot with nothing left in the system that could advance it.

    /** consumer_floor retention with every existing snapshot outside the window. */
    private fun ageForExpiry(cat: String) =
        db.jdbi.useHandle<Exception> { h ->
            h.createUpdate(
                "UPDATE hog_catalog SET snapshot_retention_seconds = 3600, consumer_floor = true WHERE name = :cat",
            ).bind("cat", cat).execute()
            h.createUpdate("UPDATE hog_snapshot SET snapshot_time = now() - interval '2 days' WHERE catalog_id = :id")
                .bind("id", catalogs.getCatalog(cat).catalogId).execute()
        }

    /** An offset row written WITHOUT going through commitOffset's release step. */
    private fun rawOffset(
        cat: String,
        tableUuid: UUID,
        snapshot: Long,
        consumer: String = "c",
    ) = db.jdbi.useHandle<Exception> { h ->
        h.createUpdate(
            """
            INSERT INTO hog_consumer_offset (catalog_id, consumer_id, table_uuid, committed_snapshot)
            VALUES (:cat, :consumer, :uuid, :snapshot)
            """,
        ).bind("cat", catalogs.getCatalog(cat).catalogId).bind("consumer", consumer)
            .bind("uuid", tableUuid).bind("snapshot", snapshot).execute()
    }

    private fun offsetUuids(
        cat: String,
        consumer: String = "c",
    ) = catalogs.listOffsets(cat, consumer).map { it.tableUuid }

    @Test
    fun `reconciling onto the replacement releases the retired incarnation's offset`() {
        val cat = fixture()
        val old = catalogs.createTable(cat, "ns", "t", columns)
        commits.commit(cat, append(cat))
        val beforeReplacement = catalogs.getCatalog(cat).headSnapshotId
        catalogs.commitOffset(cat, "c", old.tableUuid, beforeReplacement)

        val prepared = prepare(cat, old.tableUuid)
        val replacement = creations.publish(cat, prepared.operationId, emptyList()).snapshotId!!
        ageForExpiry(cat)

        // Not yet reconciled: the retired incarnation is a real pin.
        val pinned = ExpiryService(db.jdbi).runOnce(cat, batchSize = 1000)
        assertThat(pinned.flooredByConsumer).isEqualTo("c")
        assertThat(catalogs.getCatalog(cat).earliestSnapshotId).isEqualTo(beforeReplacement)

        // Committing on the new incarnation at or past the replacement
        // snapshot IS the reconciliation, so the retired row is released.
        catalogs.commitOffset(cat, "c", prepared.tableUuid, replacement)
        assertThat(offsetUuids(cat)).containsExactly(prepared.tableUuid)
        val advanced = ExpiryService(db.jdbi).runOnce(cat, batchSize = 1000)
        assertThat(advanced.flooredByConsumer).isNull()
        assertThat(advanced.newEarliestSnapshotId).isGreaterThanOrEqualTo(replacement)
        assertThat(catalogs.getCatalog(cat).earliestSnapshotId).isGreaterThanOrEqualTo(replacement)
    }

    @Test
    fun `a consumer that has not reconciled still pins expiry at the retired incarnation`() {
        val cat = fixture()
        val old = catalogs.createTable(cat, "ns", "t", columns)
        commits.commit(cat, append(cat))
        val beforeReplacement = catalogs.getCatalog(cat).headSnapshotId
        catalogs.commitOffset(cat, "c", old.tableUuid, beforeReplacement)
        val prepared = prepare(cat, old.tableUuid)
        creations.publish(cat, prepared.operationId, emptyList())
        commits.commit(cat, append(cat))
        ageForExpiry(cat)

        repeat(2) {
            val result = ExpiryService(db.jdbi).runOnce(cat, batchSize = 1000)
            assertThat(result.flooredByConsumer).isEqualTo("c")
            assertThat(catalogs.getCatalog(cat).earliestSnapshotId).isEqualTo(beforeReplacement)
        }
        assertThat(offsetUuids(cat)).containsExactly(old.tableUuid)
    }

    /**
     * A catalog with expiry OFF still releases stranded offsets.
     *
     * The release used to sit BELOW `sweep()`'s retention early-return,
     * so a retention-null catalog — expiry deliberately configured off,
     * which several production catalogs are — could never release
     * anything. The rows were unreachable: the commit path's backward
     * walk only fires when the consumer commits on the successor, and it
     * never will (the whole point is that it reconciled long ago), so
     * nothing left in the system would ever clear them. `/maintenance/
     * verify`'s offset_release check would have reported the violation
     * forever with no action an operator could take. The release is now
     * the FIRST thing the sweep does, above the retention check.
     */
    @Test
    fun `a retention-null catalog still releases offsets stranded by a replacement`() {
        val cat = fixture()
        val old = catalogs.createTable(cat, "ns", "t", columns)
        commits.commit(cat, append(cat))
        val beforeReplacement = catalogs.getCatalog(cat).headSnapshotId
        val prepared = prepare(cat, old.tableUuid)
        val replacement = creations.publish(cat, prepared.operationId, emptyList()).snapshotId!!
        rawOffset(cat, old.tableUuid, beforeReplacement)
        rawOffset(cat, prepared.tableUuid, replacement)
        // NO retention: sweep() returns before it expires anything.
        assertThat(catalogs.getCatalog(cat).snapshotRetentionSeconds).isNull()

        val audit = CopyOnWriteArrayList<ILoggingEvent>()
        val appender =
            object : AppenderBase<ILoggingEvent>() {
                override fun append(event: ILoggingEvent) {
                    audit += event
                }
            }
        appender.context = LoggerFactory.getILoggerFactory() as LoggerContext
        appender.start()
        val auditLogger = LoggerFactory.getLogger("hoglake.audit") as Logger
        auditLogger.addAppender(appender)
        val result =
            try {
                ExpiryService(db.jdbi).runOnce(cat, batchSize = 1000)
            } finally {
                auditLogger.detachAppender(appender)
                appender.stop()
            }
        assertThat(result.snapshotsExpired).describedAs("expiry is off").isZero()
        // A sweep that deleted a consumer position is NOT zero work, so
        // it emits an audit event rather than an app-log debug line —
        // and the event says how many positions it deleted.
        assertThat(audit.map { it.formattedMessage })
            .describedAs("a sweep that released offsets must not be logged as nothing-to-do")
            .isNotEmpty()
        // The detail rides as a logstash StructuredArgument, so the
        // assertion reads the argument array rather than the rendered
        // message (which is just "expiry ok").
        assertThat(audit.flatMap { e -> e.argumentArray.orEmpty().map { it.toString() } })
            .anySatisfy { assertThat(it).contains("offsets_released=1") }
        assertThat(offsetUuids(cat))
            .describedAs("the stranded row is gone even though nothing expired")
            .containsExactly(prepared.tableUuid)
        // COUNTED, not silent. Deleting a consumer's position is the
        // only work this sweep can do, and an uncounted one is reported
        // as "nothing to do" — which is how the stranded rows stayed
        // invisible in the first place.
        assertThat(result.offsetsReleased).isEqualTo(1)

        // A second sweep is idempotent AND says so: nothing left to
        // release, so the counter is back to zero.
        assertThat(ExpiryService(db.jdbi).runOnce(cat, batchSize = 1000).offsetsReleased).isZero()
        // And the invariant scan agrees, which is the surface that would
        // otherwise have alerted on it forever.
        val report = VerifyService(db.jdbi).runOnce(cat)
        assertThat(report.checks.single { it.check == "offset_release" }.violations).isZero()
    }

    /**
     * (t1) The pre-fix population: BOTH rows written straight to SQL, so a
     * broken release fails on the assertion rather than on a primary-key
     * violation from re-inserting a row commitOffset just deleted.
     *
     * Swept TWICE on purpose. The first sweep's own retention step deletes
     * the retired incarnation's hog_table_version row (end_snapshot <=
     * the new floor), so a lineage derivation that read version rows lost
     * its evidence exactly here: sweep one would advance, sweep two would
     * find the row again and pin forever. hog_table rows are never
     * deleted, which is why the derivation uses them alone.
     */
    @Test
    fun `an offset stranded before this fix is released and stays released across sweeps`() {
        val cat = fixture()
        val old = catalogs.createTable(cat, "ns", "t", columns)
        commits.commit(cat, append(cat))
        val beforeReplacement = catalogs.getCatalog(cat).headSnapshotId
        val prepared = prepare(cat, old.tableUuid)
        val replacement = creations.publish(cat, prepared.operationId, emptyList()).snapshotId!!
        commits.commit(cat, append(cat))
        val head = catalogs.getCatalog(cat).headSnapshotId
        rawOffset(cat, old.tableUuid, beforeReplacement)
        rawOffset(cat, prepared.tableUuid, head)
        ageForExpiry(cat)

        repeat(2) { sweep ->
            val result = ExpiryService(db.jdbi).runOnce(cat, batchSize = 1000)
            assertThat(result.flooredByConsumer).describedAs("sweep %s", sweep).isNull()
            assertThat(catalogs.getCatalog(cat).earliestSnapshotId)
                .describedAs("sweep %s floor", sweep)
                .isGreaterThan(replacement)
            assertThat(offsetUuids(cat)).describedAs("sweep %s rows", sweep)
                .containsExactly(prepared.tableUuid)
        }
        // And the version row the old derivation depended on is indeed gone.
        val retiredVersionRows =
            db.jdbi.withHandle<Int, Exception> { h ->
                h.createQuery(
                    "SELECT count(*) FROM hog_table_version WHERE catalog_id = :cat AND table_id = :tid",
                ).bind("cat", catalogs.getCatalog(cat).catalogId).bind("tid", old.tableId)
                    .mapTo(Int::class.java).one()
            }
        assertThat(retiredVersionRows).isZero()
    }

    /**
     * (t2) A -> B -> C with the consumer down across both replacements: it
     * reconciles straight to C, so BOTH ancestors must be released. One
     * hop would leave A pinning the floor with nothing left to advance it.
     */
    @Test
    fun `a consumer reconciling across a replacement chain releases every ancestor`() {
        val cat = fixture()
        val a = catalogs.createTable(cat, "ns", "t", columns)
        commits.commit(cat, append(cat))
        val atA = catalogs.getCatalog(cat).headSnapshotId
        val bPrepared = prepare(cat, a.tableUuid)
        val bSnapshot = creations.publish(cat, bPrepared.operationId, emptyList()).snapshotId!!
        val cPrepared = prepare(cat, bPrepared.tableUuid)
        val cSnapshot = creations.publish(cat, cPrepared.operationId, emptyList()).snapshotId!!
        assertThat(cSnapshot).isGreaterThan(bSnapshot)

        // ONLY A and C. An offset on B as well would let the walk succeed
        // at depth 1 and prove nothing about recursion (QE found exactly
        // that); with B absent, releasing A REQUIRES two hops.
        rawOffset(cat, a.tableUuid, atA)
        catalogs.commitOffset(cat, "c", cPrepared.tableUuid, cSnapshot)

        assertThat(offsetUuids(cat)).containsExactly(cPrepared.tableUuid)
        ageForExpiry(cat)
        val advanced = ExpiryService(db.jdbi).runOnce(cat, batchSize = 1000)
        assertThat(advanced.flooredByConsumer).isNull()
        assertThat(catalogs.getCatalog(cat).earliestSnapshotId).isGreaterThanOrEqualTo(cSnapshot)
    }

    /**
     * The walk releases what the consumer has PASSED, not the whole
     * chain: a consumer that reconciled onto B and stopped there loses
     * its A row and keeps its B row.
     */
    @Test
    fun `a consumer stopped at the middle incarnation releases only what it passed`() {
        val cat = fixture()
        val a = catalogs.createTable(cat, "ns", "t", columns)
        commits.commit(cat, append(cat))
        val atA = catalogs.getCatalog(cat).headSnapshotId
        val bPrepared = prepare(cat, a.tableUuid)
        val bSnapshot = creations.publish(cat, bPrepared.operationId, emptyList()).snapshotId!!
        val cPrepared = prepare(cat, bPrepared.tableUuid)
        val cSnapshot = creations.publish(cat, cPrepared.operationId, emptyList()).snapshotId!!

        rawOffset(cat, a.tableUuid, atA)
        catalogs.commitOffset(cat, "c", bPrepared.tableUuid, bSnapshot)

        assertThat(offsetUuids(cat)).containsExactly(bPrepared.tableUuid)
        ageForExpiry(cat)
        val result = ExpiryService(db.jdbi).runOnce(cat, batchSize = 1000)
        assertThat(result.flooredByConsumer).isEqualTo("c")
        assertThat(catalogs.getCatalog(cat).earliestSnapshotId).isEqualTo(bSnapshot)
        assertThat(cSnapshot).isGreaterThan(bSnapshot)
    }

    /**
     * One consumer's reconciliation says nothing about another's.
     * Dropping `reconciled.consumer_id = l.consumer_id` from the walk
     * would delete the lagging consumer's live position, and QE found
     * that mutation uncaught.
     */
    @Test
    fun `one consumer reconciling does not release another consumer's retired offset`() {
        val cat = fixture()
        val old = catalogs.createTable(cat, "ns", "t", columns)
        commits.commit(cat, append(cat))
        val beforeReplacement = catalogs.getCatalog(cat).headSnapshotId
        catalogs.commitOffset(cat, "reconciled", old.tableUuid, beforeReplacement)
        catalogs.commitOffset(cat, "lagging", old.tableUuid, beforeReplacement)
        val prepared = prepare(cat, old.tableUuid)
        val replacement = creations.publish(cat, prepared.operationId, emptyList()).snapshotId!!

        catalogs.commitOffset(cat, "reconciled", prepared.tableUuid, replacement)

        assertThat(offsetUuids(cat, "reconciled")).containsExactly(prepared.tableUuid)
        assertThat(offsetUuids(cat, "lagging")).containsExactly(old.tableUuid)
        ageForExpiry(cat)
        val result = ExpiryService(db.jdbi).runOnce(cat, batchSize = 1000)
        assertThat(result.flooredByConsumer).isEqualTo("lagging")
        assertThat(catalogs.getCatalog(cat).earliestSnapshotId).isEqualTo(beforeReplacement)
        assertThat(offsetUuids(cat, "lagging")).containsExactly(old.tableUuid)
    }

    /**
     * (T1) The RECORDED edge is what the release reads — not the snapshot
     * convention that happens to agree with it. Blank the edge and the
     * release must stop happening, or `replaced_table_id` is decorative
     * and a regression back to the derivation would pass unnoticed.
     */
    @Test
    fun `a missing replacement edge means no release even when the snapshots line up`() {
        val cat = fixture()
        val old = catalogs.createTable(cat, "ns", "t", columns)
        commits.commit(cat, append(cat))
        val beforeReplacement = catalogs.getCatalog(cat).headSnapshotId
        catalogs.commitOffset(cat, "c", old.tableUuid, beforeReplacement)
        val prepared = prepare(cat, old.tableUuid)
        val replacement = creations.publish(cat, prepared.operationId, emptyList()).snapshotId!!

        // The snapshot convention still holds perfectly here; only the
        // recorded edge is gone, as a 1.2.0 replica would have left it.
        val newTableId = catalogs.getTable(cat, "ns", "t").tableId
        db.jdbi.useHandle<Exception> { h ->
            h.createUpdate(
                "UPDATE hog_table SET replaced_table_id = NULL WHERE catalog_id = :cat AND table_id = :id",
            ).bind("cat", catalogs.getCatalog(cat).catalogId).bind("id", newTableId).execute()
        }

        catalogs.commitOffset(cat, "c", prepared.tableUuid, replacement)
        assertThat(offsetUuids(cat)).containsExactlyInAnyOrder(old.tableUuid, prepared.tableUuid)
        ageForExpiry(cat)
        val result = ExpiryService(db.jdbi).runOnce(cat, batchSize = 1000)
        assertThat(result.flooredByConsumer).isEqualTo("c")
        assertThat(catalogs.getCatalog(cat).earliestSnapshotId).isEqualTo(beforeReplacement)
    }

    /**
     * (T1, inverse) ... and the edge alone is sufficient: break the
     * snapshot convention on the retired row and the release still
     * happens, because nothing at runtime consults it.
     */
    @Test
    fun `a recorded edge releases even when the snapshot convention is broken`() {
        val cat = fixture()
        val old = catalogs.createTable(cat, "ns", "t", columns)
        commits.commit(cat, append(cat))
        val beforeReplacement = catalogs.getCatalog(cat).headSnapshotId
        catalogs.commitOffset(cat, "c", old.tableUuid, beforeReplacement)
        val prepared = prepare(cat, old.tableUuid)
        val replacement = creations.publish(cat, prepared.operationId, emptyList()).snapshotId!!

        // dropped_snapshot no longer equals the successor's
        // created_snapshot. A derivation would find nothing here.
        db.jdbi.useHandle<Exception> { h ->
            h.createUpdate(
                "UPDATE hog_table SET dropped_snapshot = :bogus WHERE catalog_id = :cat AND table_id = :id",
            ).bind("bogus", replacement + 1).bind("cat", catalogs.getCatalog(cat).catalogId)
                .bind("id", old.tableId).execute()
        }

        catalogs.commitOffset(cat, "c", prepared.tableUuid, replacement)
        assertThat(offsetUuids(cat)).containsExactly(prepared.tableUuid)
    }

    /**
     * (T2) Termination, with no coverage before now. A forged 2-cycle is
     * the shape the `created_snapshot` ordering guards exist for (the
     * 1-cycle is ruled out by hog_table_no_self_replacement). Both walks
     * must terminate and delete nothing; a runaway UNION ALL would hang
     * the commit tail, so the statement timeout is the assertion.
     */
    @Test
    fun `a forged replacement cycle terminates both walks`() {
        val cat = fixture()
        val a = catalogs.createTable(cat, "ns", "t", columns)
        commits.commit(cat, append(cat))
        val atA = catalogs.getCatalog(cat).headSnapshotId
        val prepared = prepare(cat, a.tableUuid)
        val replacement = creations.publish(cat, prepared.operationId, emptyList()).snapshotId!!
        val catalogId = catalogs.getCatalog(cat).catalogId
        val bTableId = catalogs.getTable(cat, "ns", "t").tableId
        // Raw, so neither walk has run yet and both have a real seed.
        rawOffset(cat, a.tableUuid, atA)
        rawOffset(cat, prepared.tableUuid, replacement)

        // B -> A is the genuine edge; forge A -> B so the pair is a cycle.
        db.jdbi.useHandle<Exception> { h ->
            h.createUpdate(
                "UPDATE hog_table SET replaced_table_id = :b WHERE catalog_id = :cat AND table_id = :a",
            ).bind("b", bTableId).bind("cat", catalogId).bind("a", a.tableId).execute()
        }

        // The statement timeout IS the assertion: a runaway UNION ALL would
        // hang here, and in production it would hang the commit tail or the
        // expiry sweep, both holding the catalog commit lock. The counts
        // assert the cycle changed no OUTCOME — A is released because B
        // genuinely replaced it, exactly as it would be without the forgery.
        db.jdbi.useHandle<Exception> { h ->
            h.execute("SET LOCAL statement_timeout = '2s'")
            assertThat(OffsetRepo.releaseSupersededOffsets(h, catalogId))
                .describedAs("forward walk terminates and releases only the genuine ancestor").isEqualTo(1)
            assertThat(
                OffsetRepo.releaseAncestorsOf(h, catalogId, "c", prepared.tableUuid, replacement),
            ).describedAs("backward walk terminates with nothing left to release").isZero()
        }
        assertThat(offsetUuids(cat)).containsExactly(prepared.tableUuid)
    }

    /** (S2) The 1-cycle the recursive walks cannot be asked to survive. */
    @Test
    fun `a self-replacement edge is rejected by the database`() {
        val cat = fixture()
        val t = catalogs.createTable(cat, "ns", "t", columns)
        assertThatThrownBy {
            db.jdbi.useHandle<Exception> { h ->
                h.createUpdate(
                    "UPDATE hog_table SET replaced_table_id = table_id WHERE catalog_id = :cat AND table_id = :id",
                ).bind("cat", catalogs.getCatalog(cat).catalogId).bind("id", t.tableId).execute()
            }
        }.hasMessageContaining("hog_table_no_self_replacement")
    }

    /**
     * (t3) An offset committed at exactly S is legal — the consumer read
     * the window ending at S, saw table_dropped, and checkpointed there.
     * It strands just the same, so it releases just the same.
     */
    @Test
    fun `an offset on the retired incarnation at exactly the replacement snapshot releases`() {
        val cat = fixture()
        val old = catalogs.createTable(cat, "ns", "t", columns)
        commits.commit(cat, append(cat))
        val prepared = prepare(cat, old.tableUuid)
        val replacement = creations.publish(cat, prepared.operationId, emptyList()).snapshotId!!
        catalogs.commitOffset(cat, "c", old.tableUuid, replacement)
        assertThat(offsetUuids(cat)).containsExactly(old.tableUuid)

        catalogs.commitOffset(cat, "c", prepared.tableUuid, replacement)
        assertThat(offsetUuids(cat)).containsExactly(prepared.tableUuid)
        ageForExpiry(cat)
        assertThat(ExpiryService(db.jdbi).runOnce(cat, batchSize = 1000).flooredByConsumer).isNull()
    }

    /**
     * (t4) The release is gated on REACHING the replacement, not on merely
     * holding a row there: an offset below the new incarnation's
     * created_snapshot means the consumer has not finished reconciling,
     * and the retired row must keep pinning.
     */
    @Test
    fun `an offset below the replacement snapshot does not release the retired incarnation`() {
        val cat = fixture()
        val old = catalogs.createTable(cat, "ns", "t", columns)
        commits.commit(cat, append(cat))
        val beforeReplacement = catalogs.getCatalog(cat).headSnapshotId
        val prepared = prepare(cat, old.tableUuid)
        val replacement = creations.publish(cat, prepared.operationId, emptyList()).snapshotId!!
        // head moves past the replacement, so the floor the sweep COULD
        // reach is unambiguously above the pin it actually reports.
        commits.commit(cat, append(cat))
        rawOffset(cat, old.tableUuid, beforeReplacement)
        rawOffset(cat, prepared.tableUuid, replacement - 1)
        ageForExpiry(cat)

        val result = ExpiryService(db.jdbi).runOnce(cat, batchSize = 1000)
        assertThat(result.flooredByConsumer).isEqualTo("c")
        assertThat(catalogs.getCatalog(cat).earliestSnapshotId).isEqualTo(beforeReplacement)
        assertThat(offsetUuids(cat)).containsExactlyInAnyOrder(old.tableUuid, prepared.tableUuid)
    }

    /**
     * (t5) A rename before the replacement must not break the derivation.
     * It did under the version-row derivation, which required the retired
     * incarnation's name at S-1 to equal the new one's at S.
     */
    @Test
    fun `a rename before the replacement does not break the lineage`() {
        val cat = fixture()
        val old = catalogs.createTable(cat, "ns", "t", columns)
        AlterService(db.jdbi).alterTable(cat, "ns", "t", listOf(AlterOp.RenameTable("renamed")))
        val beforeReplacement = catalogs.getCatalog(cat).headSnapshotId
        catalogs.commitOffset(cat, "c", old.tableUuid, beforeReplacement)
        // Replace the renamed table, under its current name.
        val prepared =
            creations.prepare(
                cat,
                UUID.randomUUID(),
                TableCreationDefinition(
                    "ns",
                    "renamed",
                    columns,
                    ReplacementTarget(old.tableUuid, catalogs.getCatalog(cat).headSnapshotId),
                ),
            )
        val replacement = creations.publish(cat, prepared.operationId, emptyList()).snapshotId!!

        catalogs.commitOffset(cat, "c", prepared.tableUuid, replacement)
        assertThat(offsetUuids(cat)).containsExactly(prepared.tableUuid)
        ageForExpiry(cat)
        assertThat(ExpiryService(db.jdbi).runOnce(cat, batchSize = 1000).flooredByConsumer).isNull()
    }

    @Test
    fun `absent target cannot overwrite a table created before publication`() {
        val cat = fixture()
        val prepared = prepare(cat, null)
        val winner = catalogs.createTable(cat, "ns", "t", columns)
        assertThat(creations.publish(cat, prepared.operationId, emptyList()).reason).isEqualTo("target_changed")
        // createTable returns the commit's snapshotId; a getTable read
        // carries none, so compare the table shape with that field cleared.
        assertThat(catalogs.getTable(cat, "ns", "t")).isEqualTo(winner.copy(snapshotId = null))
    }

    @Test
    fun `absent target and empty replacement use the same durable receipt`() {
        val cat = fixture()
        val creation = prepare(cat, null)
        assertThat(creations.publish(cat, creation.operationId, emptyList()).state).isEqualTo("committed")
        val replacement = prepare(cat, creation.tableUuid)
        val receipt = creations.publish(cat, replacement.operationId, emptyList())
        assertThat(receipt.state).isEqualTo("committed")
        assertThat(catalogs.getTable(cat, "ns", "t").tableUuid).isEqualTo(replacement.tableUuid)
        assertThat(catalogs.getTable(cat, "ns", "t").recordCount).isZero()
    }

    @Test
    fun `abort and invalid publication leave the original intact`() {
        val cat = fixture()
        val original = catalogs.createTable(cat, "ns", "t", columns)
        val prepared = prepare(cat, original.tableUuid)
        assertThatThrownBy {
            creations.publish(
                cat,
                prepared.operationId,
                listOf(FileRegistration(prepared.writePath + "bad", -1, 100, 20)),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
        // createTable returns the commit's snapshotId; a getTable read
        // carries none, so compare the table shape with that field cleared.
        assertThat(catalogs.getTable(cat, "ns", "t")).isEqualTo(original.copy(snapshotId = null))
        assertThat(creations.abort(cat, prepared.operationId).state).isEqualTo("aborted")
        assertThat(creations.publish(cat, prepared.operationId, emptyList()).state).isEqualTo("aborted")
        assertThat(catalogs.getTable(cat, "ns", "t")).isEqualTo(original.copy(snapshotId = null))
    }

    @Test
    fun `target mutations after preparation reject replacement without overwriting them`() {
        for (mutation in listOf("insert", "rename", "drop", "truncate", "replace", "reuse")) {
            val cat = fixture()
            val original = catalogs.createTable(cat, "ns", "t", columns)
            val prepared = prepare(cat, original.tableUuid)
            when (mutation) {
                "insert" -> commits.commit(cat, append(cat))
                "rename" -> AlterService(db.jdbi).alterTable(cat, "ns", "t", listOf(AlterOp.RenameTable("renamed")))
                "drop" -> catalogs.dropTable(cat, "ns", "t")
                "truncate" -> catalogs.truncateTable(cat, "ns", "t", original.tableUuid)
                "replace" -> creations.publish(cat, prepare(cat, original.tableUuid).operationId, emptyList())
                "reuse" -> {
                    catalogs.dropTable(cat, "ns", "t")
                    catalogs.createTable(cat, "ns", "t", columns)
                }
            }
            val head = catalogs.getCatalog(cat).headSnapshotId
            val rejected = creations.publish(cat, prepared.operationId, emptyList())
            assertThat(rejected.state).describedAs(mutation).isEqualTo("rejected")
            assertThat(rejected.reason).isEqualTo("target_changed")
            assertThat(catalogs.getCatalog(cat).headSnapshotId).isEqualTo(head)
            assertThat(creations.publish(cat, prepared.operationId, emptyList())).isEqualTo(rejected)
        }
    }
}
