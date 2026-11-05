package com.posthog.hoglake.commit

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.persistence.CommitLockPath
import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.TableCreationDefinition
import com.posthog.hoglake.service.TableCreationService
import com.posthog.hoglake.service.UploadService
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Namespace containment (V33): a namespace marked contained for
 * `field = value` owns `<data_path>/data/<namespace>/`. Its tables register
 * files only there; no other table registers files there.
 *
 * Every refusal has a twin that is accepted, so the rule refuses exactly
 * what it says and a catalog with no contained namespace behaves as before.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NamespaceContainmentIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val uploads = UploadService(db.jdbi)
    private val creations = TableCreationService(db.jdbi, catalogs, commits)
    private val counter = AtomicInteger(0)
    private val columns = listOf(ColumnDef("v", ColType.LONG))
    private val team42 = NamespaceContainment("team_id", 42)

    @AfterAll
    fun tearDown() = db.close()

    /** `team_42` contained for team_id = 42 with table `t`, and an ordinary `main` with table `t`. */
    private fun fixture(): String {
        val cat = "contain-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "team_42")
        catalogs.createNamespace(cat, "main")
        catalogs.setNamespaceContainment(cat, "team_42", team42)
        catalogs.createTable(cat, "team_42", "t", columns)
        catalogs.createTable(cat, "main", "t", columns)
        return cat
    }

    private fun data(
        cat: String,
        rest: String,
    ) = "s3://bucket/$cat/data/$rest"

    private fun file(path: String) = FileRegistration(path, 10, 1_000, 20, null, null)

    private fun append(
        cat: String,
        namespace: String,
        vararg paths: String,
    ) = commits.commit(cat, CommitRequest(appends = listOf(TableAppend(namespace, "t", paths.map(::file)))))

    private fun refused(block: () -> Unit) =
        assertThatThrownBy(block).isInstanceOf(HoglakeException.ContainmentViolation::class.java)

    @Test
    fun `a contained namespace's table registers only under its own prefix`() {
        val cat = fixture()
        append(cat, "team_42", data(cat, "team_42/t/a.parquet"))
        refused { append(cat, "team_42", data(cat, "main/t/b.parquet")) }
        refused { append(cat, "team_42", data(cat, "c.parquet")) }
        refused { append(cat, "team_42", "s3://bucket/$cat/elsewhere/d.parquet") }
        // One bad file refuses the whole commit.
        refused { append(cat, "team_42", data(cat, "team_42/t/e.parquet"), data(cat, "main/t/f.parquet")) }
        // A percent-escaped spelling of its own name is not its own prefix.
        refused { append(cat, "team_42", data(cat, "team%5F42/t/g.parquet")) }
    }

    @Test
    fun `no other table registers under a contained prefix, however it is spelled`() {
        val cat = fixture()
        append(cat, "main", data(cat, "main/t/a.parquet"))
        append(cat, "main", data(cat, "team_43/t/b.parquet"))
        refused { append(cat, "main", data(cat, "team_42/t/c.parquet")) }
        refused { append(cat, "main", data(cat, "team%5F42/t/d.parquet")) }
        refused { append(cat, "main", data(cat, "team%5f42/t/e.parquet")) }
        // The `data` directory escaped, alone or with the segment.
        refused { append(cat, "main", "s3://bucket/$cat/d%61ta/team_42/t/f.parquet") }
        refused { append(cat, "main", "s3://bucket/$cat/%64%61ta/team%5F42/t/g.parquet") }
        // ...which is not `data/` for the contained table itself either.
        refused { append(cat, "team_42", "s3://bucket/$cat/d%61ta/team_42/t/h.parquet") }
        // An escaped directory that is not `data` reaches nothing.
        append(cat, "main", "s3://bucket/$cat/d%62ta/team_42/t/i.parquet")
    }

    @Test
    fun `deletion vectors are held to the same rule`() {
        val cat = fixture()
        append(cat, "main", data(cat, "main/t/a.parquet"))
        val target =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT data_file_id FROM hog_data_file f JOIN hog_catalog c USING (catalog_id) " +
                        "WHERE c.name = :c AND f.path LIKE '%/main/t/a.parquet'",
                ).bind("c", cat).mapTo(Long::class.java).one()
            }

        fun dv(path: String) =
            commits.commit(
                cat,
                CommitRequest(
                    readSnapshot = db.head(cat),
                    deletes = listOf(TableDeletes("main", "t", listOf(DeleteFileRegistration(target, path, 1, 64)))),
                ),
            )
        refused { dv(data(cat, "team_42/t/dv.puffin")) }
        dv(data(cat, "main/t/dv.puffin"))
    }

    @Test
    fun `a catalog with no contained namespace refuses nothing`() {
        val cat = "contain-none-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "main")
        catalogs.createTable(cat, "main", "t", columns)
        catalogs.createNamespace(cat, "team_42")
        append(cat, "main", data(cat, "team_42/t/a.parquet"), data(cat, "x.parquet"))
    }

    @Test
    fun `a table creation in a contained namespace writes under its prefix`() {
        val cat = fixture()
        val contained =
            creations.prepare(cat, UUID.randomUUID(), TableCreationDefinition("team_42", "created", columns))
        assertThat(contained.writePath).startsWith(data(cat, "team_42/"))
        creations.publish(cat, contained.operationId, listOf(file(contained.writePath + "a.parquet")))

        val plain = creations.prepare(cat, UUID.randomUUID(), TableCreationDefinition("main", "created", columns))
        assertThat(plain.writePath).doesNotStartWith(data(cat, "main/"))
        creations.publish(cat, plain.operationId, listOf(file(plain.writePath + "a.parquet")))

        // Prepared before the namespace was marked: the publish is refused.
        catalogs.createNamespace(cat, "team_7")
        val early = creations.prepare(cat, UUID.randomUUID(), TableCreationDefinition("team_7", "t", columns))
        catalogs.setNamespaceContainment(cat, "team_7", NamespaceContainment("team_id", 7))
        refused { creations.publish(cat, early.operationId, listOf(file(early.writePath + "a.parquet"))) }
    }

    @Test
    fun `marking requires a namespace that never held a table and a prefix nobody else used`() {
        val cat = fixture()
        // Already holds a table.
        assertThatThrownBy { catalogs.setNamespaceContainment(cat, "main", NamespaceContainment("team_id", 1)) }
            .isInstanceOf(HoglakeException.NamespaceNotEmpty::class.java)
        // Held one once: dropped tables still count.
        catalogs.createNamespace(cat, "team_5")
        catalogs.createTable(cat, "team_5", "gone", columns)
        catalogs.dropTable(cat, "team_5", "gone", author = "test", message = "test")
        assertThatThrownBy { catalogs.setNamespaceContainment(cat, "team_5", NamespaceContainment("team_id", 5)) }
            .isInstanceOf(HoglakeException.NamespaceNotEmpty::class.java)
        // A name another namespace once had: a read at an old snapshot
        // resolves the name to that one, so it can never be contained.
        catalogs.createNamespace(cat, "team_6")
        catalogs.dropNamespace(cat, "team_6", author = "test", message = "test")
        catalogs.createNamespace(cat, "team_6")
        assertThatThrownBy { catalogs.setNamespaceContainment(cat, "team_6", NamespaceContainment("team_id", 6)) }
            .isInstanceOf(HoglakeException.CommitConflict::class.java).hasMessageContaining("dropped namespace")
        // Another table's file already under the prefix, plainly or escaped.
        for ((ns, path) in listOf("team_8" to "team_8/x.parquet", "team_9" to "team%5F9/x.parquet")) {
            append(cat, "main", data(cat, path))
            catalogs.createNamespace(cat, ns)
            assertThatThrownBy {
                catalogs.setNamespaceContainment(
                    cat,
                    ns,
                    NamespaceContainment("team_id", ns.removePrefix("team_").toLong()),
                )
            }.isInstanceOf(HoglakeException.CommitConflict::class.java).hasMessageContaining(path)
        }
        // A live upload claim under it.
        catalogs.createNamespace(cat, "team_10")
        uploads.claim(cat, UUID.randomUUID(), UUID.randomUUID(), data(cat, "team_10"), "data")
        assertThatThrownBy { catalogs.setNamespaceContainment(cat, "team_10", NamespaceContainment("team_id", 10)) }
            .isInstanceOf(HoglakeException.CommitConflict::class.java)
        // A path queued for removal and not yet drained: the object is still
        // in the store. Once drained, it no longer blocks.
        catalogs.createNamespace(cat, "team_12")
        val queued = data(cat, "team_12/old.parquet")
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
                SELECT catalog_id, :p, 'data', 'snapshot_expiry' FROM hog_catalog WHERE name = :cat
                """,
            ).bind("p", queued).bind("cat", cat).execute()
        }
        assertThatThrownBy { catalogs.setNamespaceContainment(cat, "team_12", NamespaceContainment("team_id", 12)) }
            .isInstanceOf(HoglakeException.CommitConflict::class.java).hasMessageContaining(queued)
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_file_removal SET drained_at = now(), drained_outcome = 'deleted' WHERE path = ?",
                queued,
            )
        }
        assertThat(catalogs.setNamespaceContainment(cat, "team_12", NamespaceContainment("team_id", 12)).containment)
            .isNotNull()
        // Escaped segments that decode to something else don't block it
        // (this catalog already holds `team%5F9/` from above, too).
        append(cat, "main", data(cat, "team%5F13x/y.parquet"), data(cat, "other%20name/y.parquet"))
        catalogs.createNamespace(cat, "team_13")
        assertThat(catalogs.setNamespaceContainment(cat, "team_13", NamespaceContainment("team_id", 13)).containment)
            .isNotNull()
        // So is an escaped `data` directory.
        append(cat, "main", "s3://bucket/$cat/d%61ta/team_15/y.parquet")
        catalogs.createNamespace(cat, "team_15")
        assertThatThrownBy { catalogs.setNamespaceContainment(cat, "team_15", NamespaceContainment("team_id", 15)) }
            .isInstanceOf(HoglakeException.CommitConflict::class.java).hasMessageContaining("d%61ta/team_15")
        // Lower-case hex is a spelling too.
        append(cat, "main", data(cat, "team%5f14/y.parquet"))
        catalogs.createNamespace(cat, "team_14")
        assertThatThrownBy { catalogs.setNamespaceContainment(cat, "team_14", NamespaceContainment("team_id", 14)) }
            .isInstanceOf(HoglakeException.CommitConflict::class.java)
    }

    @Test
    fun `a file queued for removal between the scan and the lock still blocks the mark`() {
        // The unlocked scan finds nothing; then, while marking waits on the
        // commit lock, a path under the prefix lands in the removal queue (as
        // expiry or retirement would move a file registered after the scan).
        // The locked pass must find it.
        val cat = fixture()
        catalogs.createNamespace(cat, "team_20")
        val late = data(cat, "team_20/late.parquet")
        val executor = Executors.newSingleThreadExecutor()
        try {
            lateinit var marking: Future<*>
            db.jdbi.inTransactionUnchecked { h ->
                val catalogId =
                    h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = :n").bind("n", cat)
                        .mapTo(Long::class.java).one()
                Locks.acquireCatalogCommitLock(h, catalogId, cat, CommitLockPath.DDL)
                marking =
                    executor.submit {
                        catalogs.setNamespaceContainment(cat, "team_20", NamespaceContainment("team_id", 20))
                    }
                val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
                while (true) {
                    h.execute("SELECT pg_stat_clear_snapshot()")
                    val waiting =
                        h.createQuery(
                            """
                            SELECT count(*) FROM pg_stat_activity
                            WHERE datname = current_database() AND wait_event_type = 'Lock'
                              AND wait_event = 'advisory' AND pid <> pg_backend_pid()
                            """,
                        ).mapTo(Int::class.java).one()
                    if (waiting > 0) break
                    check(System.nanoTime() < deadline) { "marking did not reach the commit lock" }
                    Thread.sleep(10)
                }
                db.jdbi.useHandleUnchecked { other ->
                    other.execute(
                        "INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason) " +
                            "VALUES (?, ?, 'data', 'snapshot_expiry')",
                        catalogId,
                        late,
                    )
                }
            }
            assertThatThrownBy { marking.get(10, TimeUnit.SECONDS) }
                .hasCauseInstanceOf(HoglakeException.CommitConflict::class.java)
                .hasMessageContaining(late)
            assertThat(catalogs.getNamespace(cat, "team_20").containment).isNull()
        } finally {
            executor.shutdownNow()
        }
    }

    @Test
    fun `one live namespace per value, marking is idempotent, and changing means clearing first`() {
        val cat = fixture()
        assertThat(catalogs.setNamespaceContainment(cat, "team_42", team42).containment).isEqualTo(team42)
        catalogs.createNamespace(cat, "other")
        assertThatThrownBy { catalogs.setNamespaceContainment(cat, "other", team42) }
            .isInstanceOf(HoglakeException.AlreadyExists::class.java)
        assertThatThrownBy { catalogs.setNamespaceContainment(cat, "team_42", NamespaceContainment("team_id", 43)) }
            .isInstanceOf(HoglakeException.CommitConflict::class.java)
        for (bad in listOf(
            NamespaceContainment("Team", 1),
            NamespaceContainment("team_id", 0),
            NamespaceContainment("team_id\n", 1),
        )) {
            assertThatThrownBy { catalogs.setNamespaceContainment(cat, "other", bad) }
                .isInstanceOf(HoglakeException.Validation::class.java)
        }
        assertThat(catalogs.getNamespace(cat, "team_42").containment).isEqualTo(team42)
        assertThat(catalogs.listNamespaces(cat).single { it.name == "main" }.containment).isNull()
        // The database holds both-or-neither itself: neither half alone.
        for (set in listOf("contained_field = 'team_id', contained_value = NULL", "contained_value = 7")) {
            assertThatThrownBy {
                db.jdbi.useHandleUnchecked { h ->
                    h.execute(
                        "UPDATE hog_namespace SET $set WHERE name = 'other' " +
                            "AND catalog_id = (SELECT catalog_id FROM hog_catalog WHERE name = ?)",
                        cat,
                    )
                }
            }.hasMessageContaining("hog_namespace_containment_shape")
        }
    }

    @Test
    fun `clearing needs an empty namespace, and a tenant may not drop a contained one`() {
        val cat = fixture()
        assertThatThrownBy { catalogs.setNamespaceContainment(cat, "team_42", null) }
            .isInstanceOf(HoglakeException.NamespaceNotEmpty::class.java)
        catalogs.dropTable(cat, "team_42", "t", author = "test", message = "test")
        assertThatThrownBy {
            catalogs.dropNamespace(cat, "team_42", author = "test", message = "test", mayDropContained = false)
        }.isInstanceOf(HoglakeException.Forbidden::class.java)
        // An operator may; or containment can be cleared first.
        catalogs.setNamespaceContainment(cat, "team_42", null)
        assertThat(catalogs.getNamespace(cat, "team_42").containment).isNull()
        // Cleared, its prefix is ordinary again.
        append(cat, "main", data(cat, "team_42/t/after.parquet"))
        catalogs.dropNamespace(cat, "team_42", author = "test", message = "test", mayDropContained = false)
    }
}
