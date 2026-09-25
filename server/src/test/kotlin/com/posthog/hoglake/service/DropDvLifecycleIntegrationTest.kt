package com.posthog.hoglake.service

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * Pinned regression for bug hunt #16, CARRIED FORWARD across #193.
 *
 * THE ORIGINAL BUG: `dropTable` end-snapshotted a table's data files
 * and forgot its deletion vectors. A live DV row on a dropped table is
 * invisible to expiry's range predicates — `end_snapshot IS NULL` never
 * sinks below the floor — so the row and its puffin object leaked
 * forever. The fix was to end-snapshot the DVs too, FIRST.
 *
 * THE RULE SURVIVED THE REDESIGN; ITS LOCATION MOVED. Drop is now
 * O(columns) and touches NO file row at all, so there is nothing for it
 * to forget: what deletes a dropped table's rows is
 * [RetirementService], and the same ordering hazard lives inside its
 * batch. `hog_delete_file`'s FK to `hog_data_file` is ON DELETE
 * CASCADE, so a batch that deleted data files first would take every
 * one of their vectors away WITHOUT queueing a path — the identical
 * leak, one layer down and harder to see, because now the row vanishes
 * instead of lingering.
 *
 * So this file asserts the ordering where it now lives, and it asserts
 * it with a SUPERSEDED vector as well as a live one. A DV delete
 * restricted to `end_snapshot IS NULL` would look correct against a
 * fixture of live vectors only, and would leak every superseded one.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DropDvLifecycleIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val jdbi get() = db.jdbi
    private val catalogs = CatalogService(jdbi)
    private val commits = CommitService(jdbi)
    private val retirement = RetirementService(jdbi, batchSize = 10, pauseMs = 0)

    @AfterAll
    fun tearDown() = db.close()

    @Test
    fun `retirement queues a dropped table's data and puffin objects, DVs first`() {
        val cat = "drop-dv"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG, nullable = false)))
        val catalogId = catalogs.getCatalog(cat).catalogId

        val dataPath = "s3://bucket/$cat/f.parquet"
        val dvPath = "s3://bucket/$cat/f.dv"
        val supersededDvPath = "s3://bucket/$cat/f.dv.v2"
        val appendSnap =
            commits.commit(
                cat,
                CommitRequest(appends = listOf(TableAppend("ns", "t", listOf(FileRegistration(dataPath, 10, 100))))),
            ).snapshotId
        val dataFileId =
            jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT data_file_id FROM hog_data_file WHERE catalog_id = ?")
                    .bind(0, catalogId).mapTo(Long::class.java).one()
            }
        val firstDv =
            commits.commit(
                cat,
                CommitRequest(
                    readSnapshot = appendSnap,
                    deletes =
                        listOf(TableDeletes("ns", "t", listOf(DeleteFileRegistration(dataFileId, dvPath, 2, 16)))),
                ),
            ).snapshotId
        // Supersede it: the first vector becomes HISTORICAL, which is
        // the population a live-only DV delete would leak.
        commits.commit(
            cat,
            CommitRequest(
                readSnapshot = firstDv,
                deletes =
                    listOf(
                        TableDeletes(
                            "ns",
                            "t",
                            listOf(DeleteFileRegistration(dataFileId, supersededDvPath, 5, 32)),
                        ),
                    ),
            ),
        )

        val dropSnap = catalogs.dropTable(cat, "ns", "t").snapshotId

        // THE DROP LEFT EVERY FILE ROW EXACTLY AS IT WAS. This is the
        // #193 change, and it is asserted here because the rest of this
        // test is about who cleans up after it.
        jdbi.useHandleUnchecked { h ->
            val open =
                h.createQuery(
                    "SELECT count(*) FROM hog_data_file WHERE catalog_id = ? AND end_snapshot IS NULL",
                ).bind(0, catalogId).mapTo(Long::class.java).one()
            assertThat(open).describedAs("the drop is O(columns); file rows are retirement's work").isEqualTo(1)
            val liveDv =
                h.createQuery(
                    "SELECT count(*) FROM hog_delete_file WHERE catalog_id = ? AND end_snapshot IS NULL",
                ).bind(0, catalogId).mapTo(Long::class.java).one()
            assertThat(liveDv).isEqualTo(1)
        }

        // Nothing is eligible while the drop sits above the floor.
        assertThat(retirement.runOnce(cat).rowsRetired).isZero()

        // Move the floor past the drop, as an expiry sweep does.
        jdbi.useHandleUnchecked { h ->
            h.createUpdate("UPDATE hog_catalog SET earliest_snapshot_id = :s WHERE catalog_id = :c")
                .bind("s", dropSnap).bind("c", catalogId).execute()
        }

        val result = retirement.runOnce(cat)
        assertThat(result.rowsRetired).isEqualTo(1)
        assertThat(result.dvsRetired).describedAs("the live vector AND the superseded one").isEqualTo(2)
        assertThat(result.pathsQueued).isEqualTo(3)

        // ALL THREE OBJECTS ARE QUEUED, each exactly once, under the
        // reason that has been in `hog_file_removal`'s CHECK since V1
        // and had never been written until now.
        //
        // MUTATION: add `AND end_snapshot IS NULL` to
        // RetirementService's DV delete and the superseded puffin is
        // missing here — its row was taken away by the data-file
        // cascade with nothing naming its object. MUTATION: swap the
        // two DELETEs so data files go first, and BOTH puffins are
        // missing.
        val queued =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT path, file_kind, reason FROM hog_file_removal WHERE catalog_id = ? " +
                        "ORDER BY removal_id",
                ).bind(0, catalogId)
                    .map { rs, _ ->
                        Triple(rs.getString("path"), rs.getString("file_kind"), rs.getString("reason"))
                    }
                    .list()
            }
        assertThat(queued).containsExactlyInAnyOrder(
            Triple(dataPath, "data", "table_drop_gc"),
            Triple(dvPath, "delete", "table_drop_gc"),
            Triple(supersededDvPath, "delete", "table_drop_gc"),
        )
        // DVs FIRST is an ordering inside the transaction, and the
        // queue's own identity column is what records it.
        assertThat(queued.map { it.second })
            .describedAs("deletion vectors are queued before their data file")
            .containsExactly("delete", "delete", "data")

        // Nothing of the table's file state survives.
        jdbi.useHandleUnchecked { h ->
            for (table in listOf("hog_data_file", "hog_delete_file")) {
                assertThat(
                    h.createQuery("SELECT count(*) FROM $table WHERE catalog_id = ?")
                        .bind(0, catalogId).mapTo(Long::class.java).one(),
                ).describedAs("%s", table).isZero()
            }
        }
    }
}
