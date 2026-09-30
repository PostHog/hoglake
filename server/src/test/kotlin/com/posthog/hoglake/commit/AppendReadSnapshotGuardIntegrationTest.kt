package com.posthog.hoglake.commit

import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.TableCreationDefinition
import com.posthog.hoglake.service.TableCreationService
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

/**
 * `read_snapshot` as the append's check-and-set on the table's shape
 * (#233), which is two rules:
 *
 *  - a pure-DDL conflict is typed apart from a row-content one, because
 *    the first cannot be fixed by replaying and the second can. Before
 *    this, both arrived as `commit_conflict` — which pyhoglake maps to a
 *    RETRYABLE error, so a writer looped on a payload whose own
 *    `read_snapshot` guaranteed the next attempt would fail too;
 *  - an append carrying `partition_values` requires a `read_snapshot`,
 *    closing the blind path where there is no conflict window at all.
 *
 * Real DDL through CatalogService/AlterService rather than SQL fixtures:
 * both rules are statements about what the DDL path records, and a
 * hand-seeded `hog_snapshot_change` row would let a wrong predicate pass.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AppendReadSnapshotGuardIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val alter = AlterService(db.jdbi)
    private val commits = CommitService(db.jdbi)

    /** The same service with HOGLAKE_REFUSE_BLIND_PARTITIONED_APPENDS on. */
    private val strictCommits = CommitService(db.jdbi, refuseBlindPartitionedAppends = true)
    private val counter = AtomicInteger(0)

    @AfterAll
    fun tearDown() = db.close()

    /** Fresh catalog+ns+table `t`: id(long, required), name(string), ts(timestamp). */
    private fun fixture(): String {
        val cat = "rsg-cat-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(
            cat,
            "ns",
            "t",
            listOf(
                ColumnDef("id", ColType.LONG, nullable = false),
                ColumnDef("name", ColType.STRING),
                ColumnDef("ts", ColType.TIMESTAMP),
            ),
        )
        return cat
    }

    private fun head(cat: String): Long = catalogs.getCatalog(cat).headSnapshotId

    /** Paths have to live under the catalog's own data_path. */
    private fun file(
        cat: String,
        name: String,
        partitionValues: List<String?>? = null,
    ) = FileRegistration(
        path = "s3://bucket/$cat/data/$name.parquet",
        recordCount = 10,
        fileSizeBytes = 1000,
        footerSize = 20,
        columnStats = null,
        partitionValues = partitionValues,
    )

    private fun partitionBy(transform: Transform) = AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(3, transform)))

    // ------------------------------------------------------------------
    // The typed DDL refusal
    // ------------------------------------------------------------------

    @Test
    fun `DDL after the read snapshot is ddl_since_read_snapshot, naming the tables and the snapshot`() {
        val cat = fixture()
        val readSnapshot = head(cat)
        // The DDL the writer does not see. Same arity as a day() spec
        // would be, which is exactly the case validateFiles' arity check
        // cannot tell apart — and the reason read_snapshot has to be the
        // guard rather than the arity.
        alter.alterTable(cat, "ns", "t", listOf(partitionBy(Transform.MONTH)))

        assertThatThrownBy {
            commits.commit(
                cat,
                CommitRequest(
                    readSnapshot = readSnapshot,
                    appends = listOf(TableAppend("ns", "t", listOf(file(cat, "f", listOf("2026-09"))))),
                ),
            )
        }
            .isInstanceOf(HoglakeException.DdlSinceReadSnapshot::class.java)
            // A SUBCLASS of CommitConflict, deliberately: every client
            // that discriminates with an isinstance ladder (millpond's
            // retry budget does) keeps classifying it as it does today,
            // while the wire CODE changes so a client that wants the
            // finer answer can have it. Un-subclassing would turn
            // millpond's working recovery into a hard re-raise.
            .isInstanceOf(HoglakeException.CommitConflict::class.java)
            .hasMessageContaining("re-prepare")
            .satisfies({
                val e = it as HoglakeException.DdlSinceReadSnapshot
                assertThat(e.tables).containsExactly("ns.t")
                assertThat(e.readSnapshot).isEqualTo(readSnapshot)
            })
        // Atomic: the refusal wrote nothing.
        assertThat(catalogs.listFiles(cat, "ns", "t")).isEmpty()
    }

    @Test
    fun `re-preparing against the new spec commits the same files`() {
        val cat = fixture()
        val stale = head(cat)
        val spec = alter.alterTable(cat, "ns", "t", listOf(partitionBy(Transform.MONTH))).partitionSpec!!
        assertThat(spec.specId).isEqualTo(1L)
        assertThatThrownBy {
            commits.commit(
                cat,
                CommitRequest(
                    readSnapshot = stale,
                    appends = listOf(TableAppend("ns", "t", listOf(file(cat, "f", listOf("2026-09"))))),
                ),
            )
        }.isInstanceOf(HoglakeException.DdlSinceReadSnapshot::class.java)

        // The recovery the refusal names: read again, prepare again.
        commits.commit(
            cat,
            CommitRequest(
                readSnapshot = head(cat),
                appends = listOf(TableAppend("ns", "t", listOf(file(cat, "f", listOf("2026-09"))))),
            ),
        )
        assertThat(catalogs.listFiles(cat, "ns", "t")).hasSize(1)
    }

    @Test
    fun `a table drop after the read snapshot is the same typed refusal`() {
        val cat = fixture()
        catalogs.createTable(cat, "ns", "other", listOf(ColumnDef("id", ColType.LONG)))
        val readSnapshot = head(cat)
        catalogs.dropTable(cat, "ns", "other")
        assertThatThrownBy {
            commits.commit(
                cat,
                CommitRequest(
                    readSnapshot = readSnapshot,
                    appends = listOf(TableAppend("ns", "other", listOf(file(cat, "d")))),
                ),
            )
        }
            // The table row is gone as a LIVE table, so resolution misses
            // first and table_dropped is the answer — also non-retryable,
            // and already typed. Pinned so the two refusals are not
            // confused for each other.
            .isInstanceOf(HoglakeException.TableDropped::class.java)
    }

    @Test
    fun `a row-content conflict on a guarded delete target stays commit_conflict`() {
        val cat = fixture()
        val uuid = catalogs.getTable(cat, "ns", "t").tableUuid
        commits.commit(
            cat,
            CommitRequest(readSnapshot = head(cat), appends = listOf(TableAppend("ns", "t", listOf(file(cat, "a"))))),
        )
        val target = catalogs.listFiles(cat, "ns", "t").single()
        val readSnapshot = head(cat)
        // Another writer INSERTS. No DDL at all, so the guarded mutation's
        // read set moved and re-reading + replanning IS the recovery.
        commits.commit(
            cat,
            CommitRequest(
                readSnapshot = readSnapshot,
                appends = listOf(TableAppend("ns", "t", listOf(file(cat, "b")))),
            ),
        )
        assertThatThrownBy {
            commits.commit(
                cat,
                CommitRequest(
                    readSnapshot = readSnapshot,
                    requireUnchangedTables = true,
                    deletes =
                        listOf(
                            TableDeletes(
                                "ns",
                                "t",
                                listOf(
                                    DeleteFileRegistration(
                                        target.dataFileId,
                                        "s3://bucket/$cat/data/dv.puffin",
                                        1,
                                        64,
                                    ),
                                ),
                                expectedTableUuid = uuid,
                            ),
                        ),
                ),
            )
        }
            .isInstanceOf(HoglakeException.CommitConflict::class.java)
            .isNotInstanceOf(HoglakeException.DdlSinceReadSnapshot::class.java)
            .hasMessageContaining("table change")
    }

    @Test
    fun `a refusal mixing DDL and row content stays commit_conflict`() {
        val cat = fixture()
        val uuid = catalogs.getTable(cat, "ns", "t").tableUuid
        commits.commit(
            cat,
            CommitRequest(readSnapshot = head(cat), appends = listOf(TableAppend("ns", "t", listOf(file(cat, "a"))))),
        )
        val target = catalogs.listFiles(cat, "ns", "t").single()
        val readSnapshot = head(cat)
        // BOTH kinds land after the read snapshot. `all { ddl }`, not
        // `any`: telling the caller "replaying cannot work" would send it
        // down the expensive path for a conflict a plain retry clears.
        commits.commit(
            cat,
            CommitRequest(
                readSnapshot = readSnapshot,
                appends = listOf(TableAppend("ns", "t", listOf(file(cat, "b")))),
            ),
        )
        alter.alterTable(cat, "ns", "t", listOf(AlterOp.SetTableComment("moved")))
        assertThatThrownBy {
            commits.commit(
                cat,
                CommitRequest(
                    readSnapshot = readSnapshot,
                    requireUnchangedTables = true,
                    deletes =
                        listOf(
                            TableDeletes(
                                "ns",
                                "t",
                                listOf(
                                    DeleteFileRegistration(
                                        target.dataFileId,
                                        "s3://bucket/$cat/data/dv2.puffin",
                                        1,
                                        64,
                                    ),
                                ),
                                expectedTableUuid = uuid,
                            ),
                        ),
                ),
            )
        }
            .isInstanceOf(HoglakeException.CommitConflict::class.java)
            .isNotInstanceOf(HoglakeException.DdlSinceReadSnapshot::class.java)
    }

    @Test
    fun `an append with no DDL since its read snapshot commits`() {
        val cat = fixture()
        val readSnapshot = head(cat)
        commits.commit(
            cat,
            CommitRequest(
                readSnapshot = readSnapshot,
                appends = listOf(TableAppend("ns", "t", listOf(file(cat, "f")))),
            ),
        )
        assertThat(catalogs.listFiles(cat, "ns", "t")).hasSize(1)
    }

    @Test
    fun `a drop and recreate under the same name is table_recreated, not a retryable conflict`() {
        val cat = fixture()
        val original = catalogs.getTable(cat, "ns", "t").tableUuid
        catalogs.dropTable(cat, "ns", "t")
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val recreated = catalogs.getTable(cat, "ns", "t").tableUuid
        assertThat(recreated).isNotEqualTo(original)

        assertThatThrownBy {
            commits.commit(
                cat,
                CommitRequest(
                    readSnapshot = head(cat),
                    appends =
                        listOf(
                            TableAppend("ns", "t", listOf(file(cat, "f")), expectedTableUuid = original),
                        ),
                ),
            )
        }
            .isInstanceOf(HoglakeException.TableRecreated::class.java)
            // NOT a CommitConflict, unlike DdlSinceReadSnapshot: nothing
            // ever classified a recreation as retryable — pyhoglake
            // already mapped it to IncarnationChangedError, by searching
            // the detail string for a phrase. The code is the contract
            // now, and the class stays its own.
            .isNotInstanceOf(HoglakeException.CommitConflict::class.java)
            .satisfies({
                val e = it as HoglakeException.TableRecreated
                assertThat(e.table).isEqualTo("ns.t")
                assertThat(e.expectedTableUuid).isEqualTo(original)
                assertThat(e.currentTableUuid).isEqualTo(recreated)
            })
        assertThat(catalogs.listFiles(cat, "ns", "t")).isEmpty()
    }

    @Test
    fun `a read snapshot a thousand snapshots behind head is accepted when no DDL touched the table`() {
        val cat = fixture()
        val ancient = head(cat)
        // The cost of an OLD read_snapshot, which is what lets a writer
        // cache one instead of re-reading per flush: the conflict scan
        // is an index range over `hog_snapshot_change_conflict
        // (catalog_id, object_id, kind, snapshot_id)` for ONE table, so
        // reaching back a thousand snapshots costs the same as reaching
        // back one. Only the EXPIRY FLOOR bounds how old it may get.
        catalogs.createTable(cat, "ns", "busy", listOf(ColumnDef("id", ColType.LONG)))
        repeat(1_000) { i ->
            commits.commit(
                cat,
                CommitRequest(
                    readSnapshot = head(cat),
                    appends = listOf(TableAppend("ns", "busy", listOf(file(cat, "busy-$i")))),
                ),
            )
        }
        assertThat(head(cat) - ancient).isGreaterThan(1_000)

        commits.commit(
            cat,
            CommitRequest(
                readSnapshot = ancient,
                appends = listOf(TableAppend("ns", "t", listOf(file(cat, "late")))),
            ),
        )
        assertThat(catalogs.listFiles(cat, "ns", "t")).hasSize(1)
    }

    @Test
    fun `a read snapshot below the expiry floor is 410, not a conflict`() {
        val cat = fixture()
        val stale = head(cat)
        // As expiry would: the snapshot the writer planned against is no
        // longer retained, so its conflict window cannot be evaluated at
        // all. The recovery is the same re-read, which is why pyhoglake
        // flags it re_prepare too.
        db.jdbi.useHandle<Exception> { h ->
            h.createUpdate("UPDATE hog_catalog SET earliest_snapshot_id = :floor WHERE name = :cat")
                .bind("floor", stale + 1)
                .bind("cat", cat)
                .execute()
        }
        assertThatThrownBy {
            commits.commit(
                cat,
                CommitRequest(
                    readSnapshot = stale,
                    appends = listOf(TableAppend("ns", "t", listOf(file(cat, "f")))),
                ),
            )
        }.isInstanceOf(HoglakeException.Expired::class.java)
    }

    // ------------------------------------------------------------------
    // Conflict check BEFORE structural validation
    // ------------------------------------------------------------------
    //
    // A DDL change and the file-level symptom it produces arrive
    // together, and whichever check runs first decides what the client is
    // told. Each of these three used to answer 422 Validation — "your
    // request is malformed" — which tells a caching writer nothing about
    // its stale basis, so it rebuilt the same doomed payload every flush
    // until its cache aged out (measured at half the retention). The
    // conflict is the cause and the malformed file is the symptom, so the
    // cause answers first.

    @Test
    fun `a DROP COLUMN whose stats name the dead field is the typed 409, not a 422`() {
        val cat = fixture()
        val readSnapshot = head(cat)
        alter.alterTable(cat, "ns", "t", listOf(AlterOp.DropColumn("name")))
        assertThatThrownBy {
            commits.commit(
                cat,
                CommitRequest(
                    readSnapshot = readSnapshot,
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "t",
                                // field 2 is `name`, which the alter just retired.
                                listOf(
                                    file(
                                        cat,
                                        "f",
                                    ).copy(columnStats = listOf(ColumnStats(2, 10, 0, null, null, null, null))),
                                ),
                            ),
                        ),
                ),
            )
        }
            .isInstanceOf(HoglakeException.DdlSinceReadSnapshot::class.java)
            .isNotInstanceOf(HoglakeException.Validation::class.java)
    }

    @Test
    fun `an unpartitioned to partitioned transition is the typed 409, not a 422`() {
        val cat = fixture()
        val readSnapshot = head(cat)
        alter.alterTable(cat, "ns", "t", listOf(partitionBy(Transform.DAY)))
        assertThatThrownBy {
            commits.commit(
                cat,
                CommitRequest(
                    readSnapshot = readSnapshot,
                    // Prepared when the table was unpartitioned, so the
                    // files carry no partition values — which validateFiles
                    // would call "is partitioned but has no partition_values".
                    appends = listOf(TableAppend("ns", "t", listOf(file(cat, "f")))),
                ),
            )
        }
            .isInstanceOf(HoglakeException.DdlSinceReadSnapshot::class.java)
            .isNotInstanceOf(HoglakeException.Validation::class.java)
    }

    @Test
    fun `a partitioned to unpartitioned transition is the typed 409, not a 422`() {
        val cat = fixture()
        alter.alterTable(cat, "ns", "t", listOf(partitionBy(Transform.DAY)))
        val readSnapshot = head(cat)
        alter.alterTable(cat, "ns", "t", listOf(AlterOp.SetPartitionSpec(emptyList())))
        assertThatThrownBy {
            commits.commit(
                cat,
                CommitRequest(
                    readSnapshot = readSnapshot,
                    appends = listOf(TableAppend("ns", "t", listOf(file(cat, "f", listOf("2026-09-29"))))),
                ),
            )
        }
            .isInstanceOf(HoglakeException.DdlSinceReadSnapshot::class.java)
            .isNotInstanceOf(HoglakeException.Validation::class.java)
    }

    @Test
    fun `a genuinely malformed file with no DDL is still a 422`() {
        // The order must not swallow real validation failures: with no
        // conflict to report, the structural check answers as it always
        // did.
        val cat = fixture()
        alter.alterTable(cat, "ns", "t", listOf(partitionBy(Transform.DAY)))
        assertThatThrownBy {
            commits.commit(
                cat,
                CommitRequest(
                    readSnapshot = head(cat),
                    appends =
                        listOf(
                            TableAppend("ns", "t", listOf(file(cat, "f", listOf("too", "many")))),
                        ),
                ),
            )
        }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("arity")
    }

    // ------------------------------------------------------------------
    // The blind-append rule
    // ------------------------------------------------------------------

    @Test
    fun `a blind append carrying partition values is refused when the flag is on`() {
        val cat = fixture()
        alter.alterTable(cat, "ns", "t", listOf(partitionBy(Transform.DAY)))
        assertThatThrownBy {
            strictCommits.commit(
                cat,
                CommitRequest(appends = listOf(TableAppend("ns", "t", listOf(file(cat, "f", listOf("2026-09-29")))))),
            )
        }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("read_snapshot is required when an append carries partition_values")
        assertThat(catalogs.listFiles(cat, "ns", "t")).isEmpty()
    }

    @Test
    fun `by default the same append is accepted and WARN-logged once per client and table`() {
        // duckdb-client sends exactly this shape — append-only commits
        // carry no read_snapshot and it sets partition values whenever
        // the table has a live spec — so the refusal ships OFF and the
        // WARN names the client that has to change. Once per (catalog,
        // table, user agent) per pod, not once per flush: a line that
        // fires on every commit is a line nobody reads.
        val cat = fixture()
        alter.alterTable(cat, "ns", "t", listOf(partitionBy(Transform.DAY)))
        val lines = mutableListOf<String>()
        withWarnCapture(lines) {
            repeat(3) { i ->
                commits.commit(
                    cat,
                    CommitRequest(
                        appends = listOf(TableAppend("ns", "t", listOf(file(cat, "blind-$i", listOf("2026-09-29"))))),
                    ),
                    userAgent = "duckdb-client/1.0",
                )
            }
        }
        assertThat(catalogs.listFiles(cat, "ns", "t")).hasSize(3)
        val warnings = lines.filter { it.contains("blind append with partition_values") }
        assertThat(warnings).hasSize(1)
        assertThat(warnings.single())
            .contains("duckdb-client/1.0")
            .contains("ns.t")
            .contains("HOGLAKE_REFUSE_BLIND_PARTITIONED_APPENDS")
            // The message quotes the refusal the client will get, so the
            // warning and the eventual 422 say the same thing.
            .contains("read_snapshot is required when an append carries partition_values")
    }

    @Test
    fun `a second client on the same table does not get its own warning`() {
        // The stated cost of keying on the table alone: the second client
        // is named only in whatever logs it writes elsewhere. Accepted,
        // because the first line already says this table needs attention,
        // and the alternative — the User-Agent in the key — let a
        // UA-minting client fill the cap and silence everyone.
        val cat = fixture()
        alter.alterTable(cat, "ns", "t", listOf(partitionBy(Transform.DAY)))
        val lines = mutableListOf<String>()
        withWarnCapture(lines) {
            for (agent in listOf("duckdb-client/1.0", "some-other-writer/2")) {
                commits.commit(
                    cat,
                    CommitRequest(
                        appends = listOf(TableAppend("ns", "t", listOf(file(cat, "by-$agent", listOf("2026-09-29"))))),
                    ),
                    userAgent = agent,
                )
            }
        }
        val warnings = lines.filter { it.contains("blind append with partition_values") }
        assertThat(warnings).hasSize(1)
        assertThat(warnings.single()).contains("duckdb-client/1.0")
    }

    @Test
    fun `two tables on one client each get a warning`() {
        // The axis that IS in the key.
        val cat = fixture()
        catalogs.createTable(cat, "ns", "other", listOf(ColumnDef("d", ColType.STRING)))
        alter.alterTable(cat, "ns", "t", listOf(partitionBy(Transform.DAY)))
        alter.alterTable(
            cat,
            "ns",
            "other",
            listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(1, Transform.IDENTITY)))),
        )
        val lines = mutableListOf<String>()
        withWarnCapture(lines) {
            commits.commit(
                cat,
                CommitRequest(appends = listOf(TableAppend("ns", "t", listOf(file(cat, "a", listOf("2026-09-29")))))),
                userAgent = "duckdb-client/1.0",
            )
            commits.commit(
                cat,
                CommitRequest(appends = listOf(TableAppend("ns", "other", listOf(file(cat, "b", listOf("v")))))),
                userAgent = "duckdb-client/1.0",
            )
        }
        assertThat(lines.filter { it.contains("blind append with partition_values") }).hasSize(2)
    }

    @Test
    fun `the same append with a read snapshot commits`() {
        val cat = fixture()
        alter.alterTable(cat, "ns", "t", listOf(partitionBy(Transform.DAY)))
        commits.commit(
            cat,
            CommitRequest(
                readSnapshot = head(cat),
                appends = listOf(TableAppend("ns", "t", listOf(file(cat, "f", listOf("2026-09-29"))))),
            ),
        )
        assertThat(catalogs.listFiles(cat, "ns", "t")).hasSize(1)
    }

    @Test
    fun `a blind append to an unpartitioned table is unchanged`() {
        val cat = fixture()
        // The legacy shape, still legal: no partition values means nothing
        // is bound to a spec, so there is nothing a concurrent alter can
        // invalidate that the field-id content checks do not catch.
        commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", listOf(file(cat, "f"))))))
        assertThat(catalogs.listFiles(cat, "ns", "t")).hasSize(1)
    }

    @Test
    fun `the refusal and the warning name only the offending tables`() {
        // A commit can append to several tables, and only the ones
        // carrying partition values are at fault. A request-level boolean
        // made the WARN say it about every table in the request — false
        // for each unpartitioned one, and each of those keys then poisoned
        // the dedupe set so the line could never be corrected — and left
        // the refusal naming no table at all.
        val cat = fixture()
        catalogs.createTable(cat, "ns", "plain", listOf(ColumnDef("id", ColType.LONG)))
        alter.alterTable(cat, "ns", "t", listOf(partitionBy(Transform.DAY)))
        val request =
            CommitRequest(
                appends =
                    listOf(
                        TableAppend("ns", "plain", listOf(file(cat, "unpartitioned"))),
                        TableAppend("ns", "t", listOf(file(cat, "parted", listOf("2026-09-29")))),
                    ),
            )

        val lines = mutableListOf<String>()
        withWarnCapture(lines) { commits.commit(cat, request, userAgent = "duckdb-client/1.0") }
        val warnings = lines.filter { it.contains("blind append with partition_values") }
        assertThat(warnings).hasSize(1)
        assertThat(warnings.single()).contains("ns.t").doesNotContain("ns.plain")

        // ...and the refusal names it too, so a refused multi-table commit
        // says which group to fix.
        assertThatThrownBy { strictCommits.commit(cat, request) }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("ns.t")
            .satisfies({ assertThat(it.message).doesNotContain("ns.plain") })
    }

    @Test
    fun `a user-agent-minting client cannot fill the dedupe set`() {
        // The User-Agent is client-supplied, so it is NOT part of the key
        // — only of the message. When it was, a client minting one per
        // request could fill the cap and silence the line for every other
        // writer for the pod's life, which is the opposite of what the
        // line is for.
        val cat = fixture()
        alter.alterTable(cat, "ns", "t", listOf(partitionBy(Transform.DAY)))
        val service = CommitService(db.jdbi)
        val lines = mutableListOf<String>()
        withWarnCapture(lines) {
            repeat(600) { i ->
                service.commit(
                    cat,
                    CommitRequest(
                        appends = listOf(TableAppend("ns", "t", listOf(file(cat, "cap-$i", listOf("2026-09-29"))))),
                    ),
                    userAgent = "writer/$i",
                )
            }
        }
        // ONE line for the one table, however many agents asked.
        assertThat(lines.filter { it.contains("blind append with partition_values") }).hasSize(1)
        // And the cap was never approached, so it never spoke.
        assertThat(lines.filter { it.contains("warnings suppressed past") }).isEmpty()
        // Every commit still landed — the dedupe touches the log, never
        // the write.
        assertThat(catalogs.listFiles(cat, "ns", "t")).hasSize(600)
    }

    @Test
    fun `a name that does not resolve gets the unknown-table refusal, not a dedupe slot`() {
        // The dedupe key is the server's tableId, so it can only be taken
        // by a table that EXISTS. Keyed on the request's own
        // (namespace, table) strings — which is where this check used to
        // sit, before resolution — a client could have minted 512 names
        // and silenced the line for the pod's life without creating
        // anything.
        val cat = fixture()
        alter.alterTable(cat, "ns", "t", listOf(partitionBy(Transform.DAY)))
        val service = CommitService(db.jdbi)
        val lines = mutableListOf<String>()
        withWarnCapture(lines) {
            repeat(600) { i ->
                assertThatThrownBy {
                    service.commit(
                        cat,
                        CommitRequest(
                            appends =
                                listOf(
                                    TableAppend(
                                        "ns",
                                        "nope$i",
                                        listOf(file(cat, "ghost-$i", listOf("2026-09-29"))),
                                    ),
                                ),
                        ),
                        userAgent = "liar/1.0",
                    )
                }.isInstanceOf(HoglakeException.Validation::class.java)
            }
            // The real table's line still gets through afterwards, which
            // is the property: 600 bogus names consumed nothing.
            service.commit(
                cat,
                CommitRequest(
                    appends = listOf(TableAppend("ns", "t", listOf(file(cat, "real", listOf("2026-09-29"))))),
                ),
                userAgent = "duckdb-client/1.0",
            )
        }
        val warnings = lines.filter { it.contains("blind append with partition_values") }
        assertThat(warnings).hasSize(1)
        assertThat(warnings.single()).contains("ns.t").contains("duckdb-client/1.0")
        assertThat(lines.filter { it.contains("warnings suppressed past") }).isEmpty()
    }

    @Test
    fun `the warning stops at its cap, and the cap means what it says`() {
        // Defence in depth against a pathological table count. The cap's
        // own line used to be a sentinel added to the set AFTER the size
        // check, which let it reach cap + 1 — so the number in the
        // message was not the number enforced. It is a flag now.
        val cat = fixture()
        val service = CommitService(db.jdbi)
        val lines = mutableListOf<String>()
        withWarnCapture(lines) {
            repeat(520) { i ->
                catalogs.createTable(cat, "ns", "cap$i", listOf(ColumnDef("d", ColType.STRING)))
                alter.alterTable(
                    cat,
                    "ns",
                    "cap$i",
                    listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(1, Transform.IDENTITY)))),
                )
                service.commit(
                    cat,
                    CommitRequest(
                        appends =
                            listOf(
                                TableAppend("ns", "cap$i", listOf(file(cat, "t$i", listOf("v")))),
                            ),
                    ),
                    userAgent = "duckdb-client/1.0",
                )
            }
        }
        assertThat(lines.filter { it.contains("blind append with partition_values") }).hasSize(512)
        assertThat(lines.filter { it.contains("warnings suppressed past") }).hasSize(1)
    }

    @Test
    fun `the blind refusal fires even when only one file of one group carries values`() {
        val cat = fixture()
        alter.alterTable(cat, "ns", "t", listOf(partitionBy(Transform.DAY)))
        val commits = strictCommits
        // A mixed request is already a 422 on arity grounds; the point
        // here is that the read_snapshot rule looks at EVERY file of
        // every merged group, so a single partitioned file cannot ride in
        // on an otherwise-unpartitioned batch.
        assertThatThrownBy {
            commits.commit(
                cat,
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend("ns", "t", listOf(file(cat, "plain"))),
                            TableAppend("ns", "t", listOf(file(cat, "parted", listOf("2026-09-29")))),
                        ),
                ),
            )
        }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("read_snapshot is required when an append carries partition_values")
    }

    @Test
    fun `atomic partitioned table creation still publishes its files without a read snapshot`() {
        // registerInitialFiles does NOT go through doCommit, so the new
        // rule must not reach the one legitimate producer of
        // partition-valued files with no read snapshot: the create itself,
        // which installs the spec in the same transaction and therefore
        // cannot race it.
        val cat = "rsg-create-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ns")
        val op = UUID.randomUUID()
        val creations = TableCreationService(db.jdbi, catalogs, commits)
        val prepared =
            creations.prepare(
                cat,
                op,
                TableCreationDefinition(
                    namespace = "ns",
                    name = "created",
                    columns = listOf(ColumnDef("id", ColType.LONG), ColumnDef("ts", ColType.TIMESTAMP)),
                    partitionFields = listOf(PartitionFieldDef(2, Transform.DAY)),
                ),
            )
        creations.publish(
            cat,
            op,
            listOf(
                FileRegistration(
                    path = prepared.writePath.trimEnd('/') + "/init.parquet",
                    recordCount = 1,
                    fileSizeBytes = 100,
                    footerSize = 20,
                    columnStats = null,
                    partitionValues = listOf("2026-09-29"),
                ),
            ),
        )
        assertThat(catalogs.listFiles(cat, "ns", "created")).hasSize(1)
    }

    /** Captures CommitService's WARN lines for the transition assertions. */
    private fun withWarnCapture(
        sink: MutableList<String>,
        block: () -> Unit,
    ) {
        val logger =
            org.slf4j.LoggerFactory.getLogger(CommitService::class.java)
                as ch.qos.logback.classic.Logger
        val ctx = org.slf4j.LoggerFactory.getILoggerFactory() as ch.qos.logback.classic.LoggerContext
        val appender =
            object : ch.qos.logback.core.AppenderBase<ch.qos.logback.classic.spi.ILoggingEvent>() {
                override fun append(event: ch.qos.logback.classic.spi.ILoggingEvent) {
                    sink += event.formattedMessage
                }
            }
        appender.context = ctx
        appender.start()
        val previous = logger.level
        logger.level = ch.qos.logback.classic.Level.WARN
        logger.addAppender(appender)
        try {
            block()
        } finally {
            logger.detachAppender(appender)
            appender.stop()
            logger.level = previous
        }
    }
}
