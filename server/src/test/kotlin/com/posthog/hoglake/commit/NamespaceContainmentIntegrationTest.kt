package com.posthog.hoglake.commit

import com.posthog.hoglake.api.toDto
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
import org.jdbi.v3.core.Handle
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
        catalogs.createTable(cat, "team_6", "gone", columns)
        catalogs.dropTable(cat, "team_6", "gone", author = "test", message = "test")
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

    private val org = "01a1e5de-1ac5-0000-e53e-d2f9a77deae4"
    private val team9400 = NamespaceContainment("team_id", 9400, org)

    /** `team_9400` org-marked, with table `t`; `main` and `team_42` (contained, no org mark) with table `t` each. */
    private fun orgFixture(): String {
        val cat = "contain-org-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat/imports")
        for (ns in listOf("team_9400", "team_42", "main")) catalogs.createNamespace(cat, ns)
        catalogs.setNamespaceContainment(cat, "team_9400", team9400)
        catalogs.setNamespaceContainment(cat, "team_42", team42)
        for (ns in listOf("team_9400", "team_42", "main")) catalogs.createTable(cat, ns, "t", columns)
        return cat
    }

    private fun hive(
        cat: String,
        rest: String,
        head: String = "org_id=$org/team_id=9400",
    ) = "s3://bucket/$cat/imports/$head/$rest"

    @Test
    fun `an org mark round-trips and is validated`() {
        val cat = "contain-org-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "team_9400")
        for (bad in listOf(
            "UPPER",
            "-a",
            "a_b",
            "a".repeat(65),
            "",
            "a/b",
            "team-slug",
            org.uppercase(),
            org.replace("-", ""),
            "$org-0",
        )) {
            assertThatThrownBy {
                catalogs.setNamespaceContainment(
                    cat,
                    "team_9400",
                    NamespaceContainment("team_id", 1, bad),
                )
            }
                .describedAs(bad).isInstanceOf(HoglakeException.Validation::class.java)
        }
        assertThat(catalogs.setNamespaceContainment(cat, "team_9400", team9400).containment).isEqualTo(team9400)
        assertThat(catalogs.getNamespace(cat, "team_9400").containment).isEqualTo(team9400)
        assertThat(catalogs.listNamespaces(cat).single().containment).isEqualTo(team9400)
        assertThat(catalogs.liveContainment(cat, "team_9400")).isEqualTo(team9400)
        assertThat(catalogs.listNamespaces(cat, snapshot = db.head(cat)).single().containment).isEqualTo(team9400)
        assertThat(catalogs.liveContainedFor(cat, "team_id", 9400)).isEqualTo("team_9400" to team9400)
        assertThat(catalogs.liveContainedFor(cat, "team_id", 9401)).isNull()
        assertThat(catalogs.getCatalog(cat).toDto().capabilities).contains("org-marked-containment-v1")
        // The database holds the shape too: an org needs the field and value.
        assertThatThrownBy {
            db.jdbi.useHandleUnchecked { h ->
                h.execute("UPDATE hog_namespace SET contained_org = 'x' WHERE contained_field IS NULL")
            }
        }.hasMessageContaining("hog_namespace_containment_org_shape")
        assertThatThrownBy {
            db.jdbi.useHandleUnchecked { h ->
                h.execute("UPDATE hog_namespace SET contained_org = 'team-slug' WHERE contained_field IS NOT NULL")
            }
        }.hasMessageContaining("hog_namespace_containment_org_shape")
    }

    @Test
    fun `an org-marked namespace's table registers only under its org prefix`() {
        val cat = orgFixture()
        append(cat, "team_9400", hive(cat, "t/a.parquet"))
        refused { append(cat, "team_9400", "s3://bucket/$cat/imports/data/team_9400/t/b.parquet") }
        refused { append(cat, "team_9400", hive(cat, "t/c.parquet", "org_id=$org/team_id=9401")) }
        refused { append(cat, "team_9400", hive(cat, "t/d.parquet", "org_id=$otherOrg/team_id=9400")) }
        refused { append(cat, "team_9400", hive(cat, "t/e.parquet", "org_id%3D$org/team_id=9400")) }
        refused { append(cat, "team_9400", "s3://bucket/$cat/imports/x.parquet") }
        // One bad file refuses the whole commit.
        refused { append(cat, "team_9400", hive(cat, "t/f.parquet"), "s3://bucket/$cat/imports/g.parquet") }
    }

    @Test
    fun `no other table registers under an org prefix, however it is spelled`() {
        val cat = orgFixture()
        for (head in listOf(
            "org_id=$org/team_id=9400",
            "org_id%3D$org/team_id=9400",
            "org%5Fid=$org/team_id=9400",
            "org_id=$org/team%5Fid=9400",
            "org_id=$org/team_id%3D9400",
            "org%5fid%3d$org/team%5fid%3d9400",
        )) {
            refused { append(cat, "main", hive(cat, "t/x.parquet", head)) }
            // A contained namespace without an org mark is no owner of it either.
            refused { append(cat, "team_42", hive(cat, "t/x.parquet", head)) }
        }
        // Another org's, another value's, or another field's directory is not this namespace's.
        append(cat, "main", hive(cat, "t/a.parquet", "org_id=$otherOrg/team_id=9400"))
        append(cat, "main", hive(cat, "t/b.parquet", "org_id=$org/team_id=9401"))
        append(cat, "main", hive(cat, "t/c.parquet", "org_id=$org/project_id=9400"))
        append(cat, "main", hive(cat, "t/d.parquet", "org_id=$org/team_id=09400"))
        // Deletion vectors are held to it too.
        append(cat, "main", "s3://bucket/$cat/imports/main/t/e.parquet")
        val target =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT data_file_id FROM hog_data_file f JOIN hog_catalog c USING (catalog_id) " +
                        "WHERE c.name = :c AND f.path LIKE '%/main/t/e.parquet'",
                ).bind("c", cat).mapTo(Long::class.java).one()
            }
        refused {
            commits.commit(
                cat,
                CommitRequest(
                    readSnapshot = db.head(cat),
                    deletes =
                        listOf(
                            TableDeletes(
                                "main",
                                "t",
                                listOf(DeleteFileRegistration(target, hive(cat, "dv.puffin"), 1, 64)),
                            ),
                        ),
                ),
            )
        }
    }

    @Test
    fun `marking refuses a foreign file already under the org prefix`() {
        val cat = "contain-org-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat/imports")
        catalogs.createNamespace(cat, "main")
        catalogs.createTable(cat, "main", "t", columns)
        // Files under other orgs, values and `data/<ns>/` do not block a mark.
        append(
            cat,
            "main",
            hive(cat, "t/a.parquet", "org_id=$otherOrg/team_id=9500"),
            hive(cat, "t/b.parquet", "org_id=$org/team_id=9501"),
            "s3://bucket/$cat/imports/data/team_9500/t/c.parquet",
        )
        for ((value, path) in listOf(
            9500L to hive(cat, "t/x.parquet", "org_id=$org/team_id=9500"),
            9502L to hive(cat, "t/x.parquet", "org_id%3D$org/team%5Fid=9502"),
            9503L to hive(cat, "t/x.parquet", "org%5fid=$org/team_id%3d9503"),
        )) {
            append(cat, "main", path)
            catalogs.createNamespace(cat, "team_$value")
            assertThatThrownBy {
                catalogs.setNamespaceContainment(cat, "team_$value", NamespaceContainment("team_id", value, org))
            }.isInstanceOf(HoglakeException.CommitConflict::class.java).hasMessageContaining(path)
        }
        catalogs.createNamespace(cat, "team_9501")
        assertThat(
            catalogs.setNamespaceContainment(
                cat,
                "team_9501",
                NamespaceContainment("team_id", 9501, otherOrg),
            ).containment,
        ).isNotNull()
        // A live claim under the prefix blocks too.
        catalogs.createNamespace(cat, "team_9600")
        uploads.claim(
            cat,
            UUID.randomUUID(),
            UUID.randomUUID(),
            hive(cat, "", "org_id=$org/team_id=9600").trimEnd('/'),
            "data",
        )
        assertThatThrownBy {
            catalogs.setNamespaceContainment(cat, "team_9600", NamespaceContainment("team_id", 9600, org))
        }.isInstanceOf(HoglakeException.CommitConflict::class.java)
    }

    private val otherOrg = "02b2f6ef-2bd6-0000-f64f-e3a0b88efbf5"

    @Test
    fun `an org mark is permanent once the namespace has held a table`() {
        val cat = orgFixture()
        // Identical: the existing no-op, even with a table.
        assertThat(catalogs.setNamespaceContainment(cat, "team_9400", team9400).containment).isEqualTo(team9400)
        for (other in listOf(
            NamespaceContainment("team_id", 9400, otherOrg),
            NamespaceContainment("team_id", 9400),
            NamespaceContainment("team_id", 9401, org),
        )) {
            assertThatThrownBy { catalogs.setNamespaceContainment(cat, "team_9400", other) }
                .describedAs(other.toString()).isInstanceOf(HoglakeException.CommitConflict::class.java)
                .hasMessageContaining("permanent")
        }
        assertThatThrownBy { catalogs.setNamespaceContainment(cat, "team_9400", null) }
            .isInstanceOf(HoglakeException.CommitConflict::class.java).hasMessageContaining("permanent")
        // Dropping the table does not undo it: the namespace has held one.
        catalogs.dropTable(cat, "team_9400", "t", author = "test", message = "test")
        assertThatThrownBy { catalogs.setNamespaceContainment(cat, "team_9400", null) }
            .isInstanceOf(HoglakeException.CommitConflict::class.java).hasMessageContaining("permanent")
        assertThatThrownBy {
            catalogs.setNamespaceContainment(cat, "team_9400", NamespaceContainment("team_id", 9400, otherOrg))
        }
            .isInstanceOf(HoglakeException.CommitConflict::class.java)
        assertThat(catalogs.getNamespace(cat, "team_9400").containment).isEqualTo(team9400)
        // A non-org mark is as before: cleared once empty.
        catalogs.dropTable(cat, "team_42", "t", author = "test", message = "test")
        catalogs.setNamespaceContainment(cat, "team_42", null)
    }

    @Test
    fun `an org mark on a namespace that never held a table can be changed or cleared`() {
        val cat = "contain-org-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat/imports")
        catalogs.createNamespace(cat, "team_9400")
        catalogs.setNamespaceContainment(cat, "team_9400", team9400)
        val moved = NamespaceContainment("team_id", 9400, otherOrg)
        assertThat(catalogs.setNamespaceContainment(cat, "team_9400", moved).containment).isEqualTo(moved)
        assertThat(
            catalogs.setNamespaceContainment(cat, "team_9400", NamespaceContainment("team_id", 9401, org)).containment,
        )
            .isEqualTo(NamespaceContainment("team_id", 9401, org))
        assertThat(catalogs.setNamespaceContainment(cat, "team_9400", null).containment).isNull()
        assertThat(catalogs.setNamespaceContainment(cat, "team_9400", team9400).containment).isEqualTo(team9400)
        // The new prefix is still scanned for foreign files.
        catalogs.createNamespace(cat, "main")
        catalogs.createTable(cat, "main", "t", columns)
        append(cat, "main", hive(cat, "t/x.parquet", "org_id=$otherOrg/team_id=9400"))
        assertThatThrownBy { catalogs.setNamespaceContainment(cat, "team_9400", moved) }
            .isInstanceOf(HoglakeException.CommitConflict::class.java)
        assertThat(catalogs.getNamespace(cat, "team_9400").containment).isEqualTo(team9400)
    }

    @Test
    fun `a view or a table landing between the scan and the lock keeps an org mark from being replaced`() {
        val cat = "contain-org-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat/imports")
        catalogs.createNamespace(cat, "team_9400")
        catalogs.setNamespaceContainment(cat, "team_9400", team9400)
        val moved = NamespaceContainment("team_id", 9400, otherOrg)
        val marking =
            markWhileLockHeld(cat, { catalogs.setNamespaceContainment(cat, "team_9400", moved) }) { h, catalogId ->
                // Held a view after the unlocked pass found the namespace empty.
                db.jdbi.useHandleUnchecked { other ->
                    other.execute(
                        "INSERT INTO hog_view (catalog_id, view_id, namespace_id, name, sql, begin_snapshot) " +
                            "SELECT catalog_id, 900001, namespace_id, 'v', 'select 1', 1 FROM hog_namespace " +
                            "WHERE catalog_id = ? AND name = 'team_9400'",
                        catalogId,
                    )
                }
            }
        assertThatThrownBy { marking.get(10, TimeUnit.SECONDS) }
            .hasCauseInstanceOf(HoglakeException.CommitConflict::class.java)
            .hasMessageContaining("has held a table or view")
        assertThat(catalogs.getNamespace(cat, "team_9400").containment).isEqualTo(team9400)
    }

    @Test
    fun `a claim or an undrained removal under the current org prefix keeps the mark from being replaced or cleared`() {
        val cat = "contain-org-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat/imports")
        for (kind in listOf("claim", "removal")) {
            val value = if (kind == "claim") 9910L else 9911L
            val ns = "team_$value"
            val mark = NamespaceContainment("team_id", value, org)
            catalogs.createNamespace(cat, ns)
            catalogs.setNamespaceContainment(cat, ns, mark)
            val prefix = hive(cat, "t", "org_id=$org/team_id=$value")
            if (kind == "claim") {
                uploads.claim(cat, UUID.randomUUID(), UUID.randomUUID(), prefix, "data")
            } else {
                db.jdbi.useHandleUnchecked { h ->
                    h.createUpdate(
                        """
                        INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
                        SELECT catalog_id, :p, 'data', 'snapshot_expiry' FROM hog_catalog WHERE name = :cat
                        """,
                    ).bind("p", "$prefix/old.parquet").bind("cat", cat).execute()
                }
            }
            for (change in listOf<NamespaceContainment?>(NamespaceContainment("team_id", value, otherOrg), null)) {
                assertThatThrownBy { catalogs.setNamespaceContainment(cat, ns, change) }
                    .describedAs("$kind, $change")
                    .isInstanceOf(HoglakeException.CommitConflict::class.java)
                    .hasMessageContaining("registered or claimed under its prefix")
            }
            assertThat(catalogs.getNamespace(cat, ns).containment).isEqualTo(mark)
        }
    }

    @Test
    fun `a dropped org-marked namespace that never held a table leaves its name markable`() {
        val cat = "contain-org-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat/imports")
        catalogs.createNamespace(cat, "team_9400")
        catalogs.setNamespaceContainment(cat, "team_9400", team9400)
        catalogs.dropNamespace(cat, "team_9400", author = "test", message = "test", mayDropContained = true)
        catalogs.createNamespace(cat, "team_9400")
        assertThat(catalogs.setNamespaceContainment(cat, "team_9400", team9400).containment).isEqualTo(team9400)
    }

    @Test
    fun `a dropped namespace that held a table keeps its name from being marked`() {
        val cat = "contain-org-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat/imports")
        catalogs.createNamespace(cat, "team_9400")
        catalogs.setNamespaceContainment(cat, "team_9400", team9400)
        catalogs.createTable(cat, "team_9400", "t", columns)
        catalogs.dropTable(cat, "team_9400", "t", author = "test", message = "test")
        catalogs.dropNamespace(cat, "team_9400", author = "test", message = "test", mayDropContained = true)
        catalogs.createNamespace(cat, "team_9400")
        assertThatThrownBy { catalogs.setNamespaceContainment(cat, "team_9400", team9400) }
            .isInstanceOf(HoglakeException.CommitConflict::class.java)
            .hasMessageContaining("belonged to another")
    }

    @Test
    fun `a table creation in an org-marked namespace writes under its org prefix`() {
        val cat = orgFixture()
        val created =
            creations.prepare(cat, UUID.randomUUID(), TableCreationDefinition("team_9400", "created", columns))
        assertThat(created.writePath).startsWith(hive(cat, ""))
        creations.publish(cat, created.operationId, listOf(file(created.writePath + "a.parquet")))
        val plain = creations.prepare(cat, UUID.randomUUID(), TableCreationDefinition("main", "created", columns))
        assertThat(plain.writePath).doesNotStartWith(hive(cat, ""))
        // Prepared before the mark, its write path is not under the org prefix: refused.
        catalogs.createNamespace(cat, "team_7")
        val early = creations.prepare(cat, UUID.randomUUID(), TableCreationDefinition("team_7", "t", columns))
        catalogs.setNamespaceContainment(cat, "team_7", NamespaceContainment("team_id", 7, org))
        refused { creations.publish(cat, early.operationId, listOf(file(early.writePath + "a.parquet"))) }
    }

    /**
     * Starts [mark] on a thread, waits until it queues on the catalog commit
     * lock (so its pre-lock reads are done), then runs [whileHeld] under that
     * lock and releases it.
     */
    private fun markWhileLockHeld(
        cat: String,
        mark: () -> Unit,
        whileHeld: (Handle, Long) -> Unit,
    ): Future<*> {
        val executor = Executors.newSingleThreadExecutor()
        try {
            lateinit var marking: Future<*>
            db.jdbi.inTransactionUnchecked { h ->
                val catalogId =
                    h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = :n").bind("n", cat)
                        .mapTo(Long::class.java).one()
                Locks.acquireCatalogCommitLock(h, catalogId, cat, CommitLockPath.DDL)
                marking = executor.submit(mark)
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
                whileHeld(h, catalogId)
            }
            return marking
        } finally {
            executor.shutdown()
        }
    }

    @Test
    fun `a mark whose namespace was cleared after its pre-lock read is refused, not marked unscanned`() {
        // team_9800 is plain-contained, so the pre-lock read skips the full scan; a foreign file
        // already sits under the org prefix. While the mark waits on the lock the containment is
        // cleared, and the locked pass alone (it reads only newer rows) would miss that file.
        val cat = "contain-org-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat/imports")
        catalogs.createNamespace(cat, "main")
        catalogs.createTable(cat, "main", "t", columns)
        catalogs.createNamespace(cat, "team_9800")
        catalogs.setNamespaceContainment(cat, "team_9800", NamespaceContainment("team_id", 9800))
        val old = hive(cat, "t/old.parquet", "org_id=$org/team_id=9800")
        append(cat, "main", old)
        val marking =
            markWhileLockHeld(
                cat,
                { catalogs.setNamespaceContainment(cat, "team_9800", NamespaceContainment("team_id", 9800, org)) },
            ) { h, catalogId ->
                h.execute(
                    "UPDATE hog_namespace SET contained_field = NULL, contained_value = NULL " +
                        "WHERE catalog_id = ? AND name = 'team_9800'",
                    catalogId,
                )
            }
        assertThatThrownBy { marking.get(10, TimeUnit.SECONDS) }
            .hasCauseInstanceOf(HoglakeException.CommitConflict::class.java)
            .hasMessageContaining("changed concurrently")
        assertThat(catalogs.getNamespace(cat, "team_9800").containment).isNull()
        // Retried with nothing in the way changed, the full scan runs and refuses on the file.
        assertThatThrownBy {
            catalogs.setNamespaceContainment(cat, "team_9800", NamespaceContainment("team_id", 9800, org))
        }.isInstanceOf(HoglakeException.CommitConflict::class.java).hasMessageContaining(old)
    }

    @Test
    fun `a file or removal landing between the scan and the lock under an org prefix still blocks the mark`() {
        val cat = "contain-org-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat/imports")
        catalogs.createNamespace(cat, "main")
        catalogs.createTable(cat, "main", "t", columns)
        for ((value, late) in listOf(9810L to "removal", 9811L to "file")) {
            catalogs.createNamespace(cat, "team_$value")
            val path = hive(cat, "t/late.parquet", "org_id=$org/team_id=$value")
            val marking =
                markWhileLockHeld(
                    cat,
                    {
                        catalogs.setNamespaceContainment(
                            cat,
                            "team_$value",
                            NamespaceContainment("team_id", value, org),
                        )
                    },
                ) { h, catalogId ->
                    db.jdbi.useHandleUnchecked { other ->
                        if (late == "removal") {
                            other.execute(
                                "INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason) " +
                                    "VALUES (?, ?, 'data', 'snapshot_expiry')",
                                catalogId,
                                path,
                            )
                        } else {
                            // A foreign table's file, registered after the scan: the commit lock is
                            // held here, so insert the row as the commit path would have.
                            other.execute(
                                "INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot, " +
                                    "path, file_format, record_count, file_size_bytes, row_id_start) " +
                                    "SELECT t.catalog_id, c.next_file_id, t.table_id, 1, ?, 'parquet', 1, 1, 0 " +
                                    "FROM hog_catalog c JOIN hog_table t ON t.catalog_id = c.catalog_id " +
                                    "WHERE c.catalog_id = ? LIMIT 1",
                                path,
                                catalogId,
                            )
                        }
                    }
                }
            assertThatThrownBy { marking.get(10, TimeUnit.SECONDS) }
                .describedAs(late)
                .hasCauseInstanceOf(HoglakeException.CommitConflict::class.java)
                .hasMessageContaining(path)
            assertThat(catalogs.getNamespace(cat, "team_$value").containment).isNull()
        }
    }

    @Test
    fun `an undrained removal under the org prefix blocks the mark, a drained one does not`() {
        val cat = "contain-org-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat/imports")
        catalogs.createNamespace(cat, "team_9820")
        val queued = hive(cat, "old.parquet", "org_id=$org/team_id=9820")
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
                SELECT catalog_id, :p, 'data', 'snapshot_expiry' FROM hog_catalog WHERE name = :cat
                """,
            ).bind("p", queued).bind("cat", cat).execute()
        }
        val mark = NamespaceContainment("team_id", 9820, org)
        assertThatThrownBy { catalogs.setNamespaceContainment(cat, "team_9820", mark) }
            .isInstanceOf(HoglakeException.CommitConflict::class.java).hasMessageContaining(queued)
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_file_removal SET drained_at = now(), drained_outcome = 'deleted' WHERE path = ?",
                queued,
            )
        }
        assertThat(catalogs.setNamespaceContainment(cat, "team_9820", mark).containment).isEqualTo(mark)
    }

    @Test
    fun `a deletion vector outside the org prefix is refused for its own table, one inside is not`() {
        val cat = orgFixture()
        append(cat, "team_9400", hive(cat, "t/a.parquet"))
        val target =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT data_file_id FROM hog_data_file f JOIN hog_catalog c USING (catalog_id) " +
                        "WHERE c.name = :c AND f.path LIKE '%/t/a.parquet'",
                ).bind("c", cat).mapTo(Long::class.java).one()
            }

        fun dv(path: String) =
            commits.commit(
                cat,
                CommitRequest(
                    readSnapshot = db.head(cat),
                    deletes =
                        listOf(
                            TableDeletes("team_9400", "t", listOf(DeleteFileRegistration(target, path, 1, 64))),
                        ),
                ),
            )
        refused { dv("s3://bucket/$cat/imports/data/team_9400/t/dv.puffin") }
        refused { dv("s3://bucket/$cat/imports/elsewhere/dv.puffin") }
        dv(hive(cat, "t/dv.puffin"))
    }

    @Test
    fun `a commit across tables is refused whole when one registers under another's org prefix`() {
        val cat = orgFixture()
        val before = db.head(cat)
        refused {
            commits.commit(
                cat,
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend("team_9400", "t", listOf(file(hive(cat, "t/own.parquet")))),
                            TableAppend("main", "t", listOf(file(hive(cat, "t/foreign.parquet")))),
                        ),
                ),
            )
        }
        assertThat(db.head(cat)).describedAs("nothing was published").isEqualTo(before)
        // The same commit with main under its own prefix goes through.
        commits.commit(
            cat,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend("team_9400", "t", listOf(file(hive(cat, "t/own.parquet")))),
                        TableAppend("main", "t", listOf(file("s3://bucket/$cat/imports/main/t/ok.parquet"))),
                    ),
            ),
        )
    }
}
