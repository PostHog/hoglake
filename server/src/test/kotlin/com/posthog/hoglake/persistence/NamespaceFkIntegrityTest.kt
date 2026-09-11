package com.posthog.hoglake.persistence

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.ViewService
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
 * Pins the namespace FK semantics decided from sql-suggestions.md #3.

 * hog_table_version and hog_view reference hog_namespace with a
 * DEFERRABLE INITIALLY DEFERRED no-action FK (not CASCADE): a direct
 * namespace-row delete with live children must FAIL — CASCADE would
 * silently hole the versioned history (rows vanish while hog_table
 * survives, breaking time travel). The FK must be DEFERRED because the
 * graph is a diamond: a whole-catalog delete cascades into
 * hog_namespace and (via hog_table) into hog_table_version as separate
 * internal statements with unspecified order, so an immediate check
 * (RESTRICT or plain NO ACTION — both verified empirically) can abort
 * the catalog cascade before the other branch empties the children.
 * The commit-time check sees the settled state.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NamespaceFkIntegrityTest {
    private val db = PgTestSupport.freshDatabase()
    private val jdbi get() = db.jdbi
    private val catalogs = CatalogService(jdbi)
    private val commits = CommitService(jdbi)
    private val views = ViewService(jdbi)

    @AfterAll
    fun tearDown() = db.close()

    private fun fixture(cat: String): Long {
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG, nullable = false)))
        commits.commit(
            cat,
            CommitRequest(
                appends = listOf(TableAppend("ns", "t", listOf(FileRegistration("s3://bucket/$cat/f.parquet", 1, 10)))),
            ),
        )
        views.create(cat, "ns", "v", "SELECT 1")
        return catalogs.getCatalog(cat).catalogId
    }

    @Test
    fun `deleting a namespace row with live children fails on the NO ACTION FK`() {
        val id = fixture("fk-direct")
        assertThatThrownBy {
            jdbi.useHandleUnchecked { h ->
                h.execute("DELETE FROM hog_namespace WHERE catalog_id = ?", id)
            }
        }.hasMessageContaining("violates foreign key constraint")
        // Nothing vanished: the versioned history is intact.
        val counts =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT (SELECT count(*) FROM hog_table_version WHERE catalog_id = :id) + " +
                        "(SELECT count(*) FROM hog_view WHERE catalog_id = :id)",
                ).bind("id", id).mapTo(Long::class.java).one()
            }
        assertThat(counts).isEqualTo(2)
    }

    @Test
    fun `a whole-catalog delete still cascades through the FK diamond`() {
        val id = fixture("fk-diamond")
        jdbi.useHandleUnchecked { h ->
            h.execute("DELETE FROM hog_catalog WHERE catalog_id = ?", id)
        }
        val leftovers =
            jdbi.withHandleUnchecked { h ->
                listOf(
                    "hog_namespace",
                    "hog_table",
                    "hog_table_version",
                    "hog_column",
                    "hog_view",
                    "hog_snapshot",
                    "hog_snapshot_change",
                    "hog_data_file",
                ).sumOf { t ->
                    h.createQuery("SELECT count(*) FROM $t WHERE catalog_id = ?")
                        .bind(0, id).mapTo(Long::class.java).one()
                }
            }
        assertThat(leftovers).isEqualTo(0L)
    }
}
