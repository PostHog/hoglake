package com.posthog.hoglake.commit

import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
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
import com.posthog.hoglake.service.ExpiryService
import com.posthog.hoglake.service.TableCreationDefinition
import com.posthog.hoglake.service.TableCreationService
import com.posthog.hoglake.service.UploadService
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

/**
 * The commit-time rule of the object-prefix layout (model/ObjectPrefix.kt):
 * on a table with the option, every data file sits under
 * `<table root>team_id=<value>/` for its canonical identity value, ships
 * its stats inline, and proves with them that every row holds the value;
 * every deletion vector sits under its target's prefix.
 *
 * EVERY REFUSAL HAS A TWIN. Each file shape refused on `ingest.events`
 * (the option on) is committed to `ingest.plain` (the same columns and
 * spec, the option off) and accepted there, because the whole point of
 * gating the rule on the layout row is that a table without one behaves
 * exactly as it did.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CommitObjectPrefixIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val alter = AlterService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val uploads = UploadService(db.jdbi)
    private val creations = TableCreationService(db.jdbi, catalogs, commits)
    private val app by lazy { App.build(Config(hydratorIntervalMs = 0), db.jdbi) }
    private val counter = AtomicInteger(0)

    @AfterAll
    fun tearDown() = db.close()

    private val columns = listOf(ColumnDef("team_id", ColType.LONG), ColumnDef("v", ColType.LONG))
    private val spec = listOf(PartitionFieldDef(1, Transform.IDENTITY))

    /** `ingest.events` with the option on team_id, and `ingest.plain` without it. */
    private fun fixture(): String {
        val cat = "prefix-commit-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ingest")
        for (table in listOf("events", "plain")) {
            catalogs.createTable(cat, "ingest", table, columns)
            alter.alterTable(cat, "ingest", table, listOf(AlterOp.SetPartitionSpec(spec)))
        }
        alter.alterTable(cat, "ingest", "events", listOf(AlterOp.SetObjectPrefixField("team_id")))
        return cat
    }

    private fun root(
        cat: String,
        table: String,
    ) = "s3://bucket/$cat/data/ingest/$table/"

    private fun under(
        cat: String,
        table: String,
        value: String,
    ) = root(cat, table) + "team_id=$value/"

    private fun le8(v: Long): ByteArray = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(v).array()

    private fun le4(v: Int): ByteArray = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(v).array()

    /** Stats for both columns, team_id's bounds as given. */
    private fun stats(
        lower: ByteArray?,
        upper: ByteArray?,
        nulls: Long = 0,
    ) = listOf(
        ColumnStats(1, 10, nulls, null, null, lower, upper),
        ColumnStats(2, 10, 0, null, null, le8(1), le8(9)),
    )

    private fun file(
        path: String,
        value: String?,
        columnStats: List<ColumnStats>?,
    ) = FileRegistration(path, 10, 1_000, 20, columnStats, listOf(value))

    /** A file the layout admits: under team_id=[value]/, stats proving it. */
    private fun good(
        cat: String,
        table: String = "events",
        value: Long = 4,
        name: String = "${UUID.randomUUID()}.parquet",
    ) = file(under(cat, table, "$value") + name, "$value", stats(le8(value), le8(value)))

    private fun append(
        cat: String,
        table: String,
        vararg files: FileRegistration,
    ) = commits.commit(
        cat,
        CommitRequest(readSnapshot = db.head(cat), appends = listOf(TableAppend("ingest", table, files.toList()))),
    )

    /** A blind append: no read_snapshot, the shape of every production flush today. */
    private fun blindAppend(
        cat: String,
        table: String,
        vararg files: FileRegistration,
    ) = commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ingest", table, files.toList()))))

    private fun ids(
        cat: String,
        paths: List<String>,
    ): List<Long> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT f.data_file_id FROM hog_data_file f JOIN hog_catalog c USING (catalog_id)
                WHERE c.name = :cat AND f.path = ANY(:paths) ORDER BY array_position(:paths, f.path)
                """,
            ).bind("cat", cat).bindArray("paths", String::class.java, paths).mapTo(Long::class.java).list()
        }

    /** The refusal [action] gets, asserting it published nothing. */
    private fun refusal(
        cat: String,
        action: () -> Unit,
    ): HoglakeException.ObjectPrefixViolation {
        val head = db.head(cat)
        val thrown = catchThrowable { action() }
        assertThat(thrown).isInstanceOf(HoglakeException.ObjectPrefixViolation::class.java)
        assertThat(db.head(cat)).describedAs("a refused commit publishes nothing").isEqualTo(head)
        return thrown as HoglakeException.ObjectPrefixViolation
    }

    /**
     * Every data-file shape the layout refuses, built for [table] (so the
     * same shape can be put to the option-off twin), with the phrase its
     * refusal names.
     */
    private fun refusedShapes(
        cat: String,
        table: String,
    ): Map<String, Pair<FileRegistration, String>> {
        val id = { "${UUID.randomUUID()}.parquet" }
        return linkedMapOf(
            "another value's prefix" to
                (file(under(cat, table, "42") + id(), "4", stats(le8(4), le8(4))) to "expected under"),
            "the table root" to (file(root(cat, table) + id(), "4", stats(le8(4), le8(4))) to "expected under"),
            "a leading zero" to
                (file(under(cat, table, "042") + id(), "042", stats(le8(42), le8(42))) to "not a canonical long"),
            "a sign" to (file(under(cat, table, "+4") + id(), "+4", stats(le8(4), le8(4))) to "not a canonical long"),
            "a null value" to (file(under(cat, table, "4") + id(), null, stats(le8(4), le8(4))) to "value null"),
            "a value past long" to
                (
                    file(under(cat, table, "9223372036854775808") + id(), "9223372036854775808", null) to
                        "not a canonical long"
                ),
            "deferred stats" to (file(under(cat, table, "4") + id(), "4", null) to "without column_stats"),
            "no stats row for the field" to
                (file(under(cat, table, "4") + id(), "4", stats(le8(4), le8(4)).drop(1)) to "no stats row"),
            "an empty stats list" to (file(under(cat, table, "4") + id(), "4", emptyList()) to "no stats row"),
            // A 4-byte bound on a long column is undecodable, so
            // StatsSanity drops it: the check runs on what is stored.
            "a bound StatsSanity drops" to
                (file(under(cat, table, "4") + id(), "4", stats(le4(4), le4(4))) to "a bound is missing"),
            "nulls in the field" to
                (file(under(cat, table, "4") + id(), "4", stats(le8(4), le8(4), nulls = 1)) to "null_count"),
            "a range of values" to
                (file(under(cat, table, "4") + id(), "4", stats(le8(4), le8(5))) to "not both the value"),
            // Rows of team 7 registered as team 4: the stats give it away.
            "another value's rows" to
                (file(under(cat, table, "4") + id(), "4", stats(le8(7), le8(7))) to "not both the value"),
        )
    }

    // ---- data files ------------------------------------------------------------

    @Test
    fun `a conforming file registers, under its prefix or deeper`() {
        val cat = fixture()
        append(cat, "events", good(cat), good(cat, value = 0), good(cat, name = "sub/dir/${UUID.randomUUID()}.parquet"))
        // A time-travel read, so the count is exact rather than sampled.
        assertThat(catalogs.getTable(cat, "ingest", "events", snapshot = db.head(cat)).fileCount).isEqualTo(3)
    }

    @Test
    fun `every refused shape is refused on the table with the option, naming the path and why`() {
        val cat = fixture()
        for ((name, shape) in refusedShapes(cat, "events")) {
            val (file, reason) = shape
            val refused = refusal(cat) { append(cat, "events", file) }
            assertThat(refused.table).describedAs(name).isEqualTo("ingest.events")
            assertThat(refused.message).describedAs(name).contains(file.path).contains(reason)
        }
        val wrongTeam = refusal(cat) { append(cat, "events", file(under(cat, "events", "42") + "a", "4", null)) }
        assertThat(wrongTeam.message)
            .describedAs("the expected prefix is the one a writer has to use")
            .contains("expected under ${under(cat, "events", "4")}")
    }

    @Test
    fun `the same shapes on the option-off twin are all accepted, as they always were`() {
        val cat = fixture()
        for ((name, shape) in refusedShapes(cat, "plain")) {
            val before = db.head(cat)
            append(cat, "plain", shape.first)
            assertThat(db.head(cat)).describedAs(name).isEqualTo(before + 1)
        }
    }

    /**
     * A blind append skips the conflict check (it has no read_snapshot to
     * fence on), and the layout must not go with it: the writer this
     * refusal exists for is the one that sends no read_snapshot.
     */
    @Test
    fun `a blind append is held to the layout too, and the option-off twin is unchanged`() {
        val cat = fixture()
        val atRoot = file(root(cat, "events") + "${UUID.randomUUID()}.parquet", "4", stats(le8(4), le8(4)))
        val refused = refusal(cat) { blindAppend(cat, "events", atRoot) }
        assertThat(refused.table).isEqualTo("ingest.events")
        assertThat(refused.message).contains(atRoot.path).contains("expected under ${under(cat, "events", "4")}")
        val deferred = file(under(cat, "events", "4") + "${UUID.randomUUID()}.parquet", "4", null)
        assertThat(refusal(cat) { blindAppend(cat, "events", deferred) }.message).contains("without column_stats")
        val before = db.head(cat)
        blindAppend(cat, "events", good(cat))
        assertThat(db.head(cat)).describedAs("a conforming blind append registers").isEqualTo(before + 1)
        blindAppend(cat, "plain", file(root(cat, "plain") + "${UUID.randomUUID()}.parquet", "4", stats(le8(4), le8(4))))
        assertThat(db.head(cat)).describedAs("the option-off twin takes the root path blind").isEqualTo(before + 2)
    }

    @Test
    fun `one refusal names the first five offending paths and counts the rest`() {
        val cat = fixture()
        val files = (1..7).map { file(root(cat, "events") + "f$it.parquet", "4", stats(le8(4), le8(4))) }
        val refused = refusal(cat) { append(cat, "events", good(cat), *files.toTypedArray()) }
        assertThat(refused.message).startsWith("7 file(s) registered to ingest.events")
        files.take(5).forEach { assertThat(refused.message).contains(it.path) }
        assertThat(refused.message).doesNotContain(files[5].path).contains("(+2 more)")
    }

    @Test
    fun `over the wire the refusal is a 422 object_prefix_violation that says re-prepare`() {
        val cat = fixture()
        val path = under(cat, "events", "4") + "${UUID.randomUUID()}.parquet"
        testApplication {
            application { app.module(this) }
            val response =
                client.post("/v1/catalogs/$cat/commit") {
                    contentType(ContentType.Application.Json)
                    setBody(
                        """{"read_snapshot":${db.head(cat)},"appends":[{"namespace":"ingest","table":"events",
                           "files":[{"path":"$path","record_count":10,"file_size_bytes":1000,"footer_size":20,
                           "partition_values":["4"]}]}]}""",
                    )
                }
            assertThat(response.status).isEqualTo(HttpStatusCode.UnprocessableEntity)
            val body = ObjectMapper().readTree(response.bodyAsText())
            assertThat(body["error"].asText()).isEqualTo("object_prefix_violation")
            assertThat(body["tables"].map { it.asText() }).containsExactly("ingest.events")
            assertThat(body["retry"].asText()).isEqualTo("re-prepare")
            assertThat(body["detail"].asText()).contains(path).contains("without column_stats")
        }
    }

    @Test
    fun `the value is read at the identity field's key_index, beside a bucket of the same column`() {
        val cat = fixture()
        catalogs.createTable(cat, "ingest", "mixed", columns)
        alter.alterTable(
            cat,
            "ingest",
            "mixed",
            listOf(
                AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(1, Transform.BUCKET, 8), spec.single())),
                AlterOp.SetObjectPrefixField("team_id"),
            ),
        )
        // Key 0 is the bucket ("2"), key 1 the identity ("4"): the file is
        // team 4's, and it is the identity value that names its prefix.
        val four =
            FileRegistration(
                under(cat, "mixed", "4") + "a.parquet",
                10,
                1_000,
                20,
                stats(le8(4), le8(4)),
                listOf("2", "4"),
            )
        append(cat, "mixed", four)
        val (id) = ids(cat, listOf(four.path))
        delete(cat, "mixed", dv(id, under(cat, "mixed", "4") + "dv.puffin"))
        val atBucket =
            FileRegistration(
                under(cat, "mixed", "2") + "b.parquet",
                10,
                1_000,
                20,
                stats(le8(2), le8(2)),
                listOf("2", "4"),
            )
        assertThat(refusal(cat) { append(cat, "mixed", atBucket) }.message)
            .contains("expected under ${under(cat, "mixed", "4")}")
    }

    @Test
    fun `a layout whose column is gone is a broken invariant, never a silent off`() {
        val cat = fixture()
        // Only reachable by hand: the DDL refuses dropping the field.
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                """
                UPDATE hog_column c SET end_snapshot = k.last_snapshot_id
                FROM hog_catalog k
                WHERE k.catalog_id = c.catalog_id AND k.name = ? AND c.field_id = 1 AND c.end_snapshot IS NULL
                  AND c.table_id = (SELECT tv.table_id FROM hog_table_version tv
                                     WHERE tv.catalog_id = k.catalog_id AND tv.name = 'events' AND tv.end_snapshot IS NULL)
                """,
                cat,
            )
        }
        val thrown = catchThrowable { append(cat, "events", file(root(cat, "events") + "a.parquet", "4", null)) }
        assertThat(thrown).isInstanceOf(IllegalStateException::class.java).hasMessageContaining("not a live column")
    }

    // ---- deletion vectors ------------------------------------------------------

    private fun dv(
        target: Long,
        path: String,
        count: Long = 1,
    ) = DeleteFileRegistration(target, path, count, 64)

    private fun delete(
        cat: String,
        table: String,
        vararg dvs: DeleteFileRegistration,
        readSnapshot: Long = db.head(cat),
    ) = commits.commit(
        cat,
        CommitRequest(readSnapshot = readSnapshot, deletes = listOf(TableDeletes("ingest", table, dvs.toList()))),
    )

    @Test
    fun `a DV must sit under its target's prefix`() {
        val cat = fixture()
        val four = good(cat, value = 4)
        val seven = good(cat, value = 7)
        append(cat, "events", four, seven)
        val (f4, f7) = ids(cat, listOf(four.path, seven.path))

        delete(cat, "events", dv(f4, under(cat, "events", "4") + "dv-${UUID.randomUUID()}.puffin"))
        delete(cat, "events", dv(f7, under(cat, "events", "7") + "deep/dv-${UUID.randomUUID()}.puffin"))

        for (path in listOf(
            // DuckDB's DV shape today: the table root.
            root(cat, "events") + "hoglake-dv-${UUID.randomUUID()}.puffin",
            under(cat, "events", "7") + "dv-${UUID.randomUUID()}.puffin",
            under(cat, "events", "42") + "dv-${UUID.randomUUID()}.puffin",
        )) {
            val refused = refusal(cat) { delete(cat, "events", dv(f4, path, count = 2)) }
            assertThat(refused.table).isEqualTo("ingest.events")
            assertThat(refused.message)
                .contains(path)
                .contains("expected under ${under(cat, "events", "4")}")
                .contains("target data_file_id $f4")
        }
    }

    @Test
    fun `the same DVs on the option-off twin are accepted`() {
        val cat = fixture()
        val file = good(cat, "plain")
        append(cat, "plain", file)
        val (id) = ids(cat, listOf(file.path))
        delete(cat, "plain", dv(id, root(cat, "plain") + "hoglake-dv-${UUID.randomUUID()}.puffin"))
        delete(cat, "plain", dv(id, under(cat, "plain", "7") + "dv-${UUID.randomUUID()}.puffin", count = 2))
    }

    @Test
    fun `a stale vector still answers its retryable conflict before the layout does`() {
        val cat = fixture()
        val four = good(cat)
        append(cat, "events", four)
        val (f4) = ids(cat, listOf(four.path))
        val read = db.head(cat)
        delete(cat, "events", dv(f4, under(cat, "events", "4") + "dv-1.puffin"))
        // Built on `read`, before that vector: a conflict, whatever its path.
        assertThatThrownBy {
            delete(cat, "events", dv(f4, root(cat, "events") + "dv-2.puffin", count = 2), readSnapshot = read)
        }.isInstanceOf(HoglakeException.CommitConflict::class.java)
            .isNotInstanceOf(HoglakeException.ObjectPrefixViolation::class.java)
    }

    @Test
    fun `after a spec change that moves the identity key_index, appends and DVs read each file's own spec`() {
        val cat = fixture()
        // Spec 1: team_id at key 0.
        val four = good(cat, value = 4)
        append(cat, "events", four)
        // Spec 2: v at key 0, team_id at key 1. Spec 1's fields stay, ended.
        alter.alterTable(
            cat,
            "ingest",
            "events",
            listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(2, Transform.IDENTITY), spec.single()))),
        )
        val five =
            FileRegistration(
                under(cat, "events", "5") + "${UUID.randomUUID()}.parquet",
                10,
                1_000,
                20,
                stats(le8(5), le8(5)),
                listOf("9", "5"),
            )
        // The key_index is the LIVE spec's: read across both specs it is
        // [0, 1], no single one, and the append is a broken invariant.
        append(cat, "events", five)
        val (f4, f5) = ids(cat, listOf(four.path, five.path))
        // A target's value is at ITS spec's key_index: across both, f5's
        // is ["9", "5"] and its DV is refused as having no value.
        delete(cat, "events", dv(f4, under(cat, "events", "4") + "dv-${UUID.randomUUID()}.puffin"))
        delete(cat, "events", dv(f5, under(cat, "events", "5") + "dv-${UUID.randomUUID()}.puffin"))
        val atV = under(cat, "events", "9") + "dv-${UUID.randomUUID()}.puffin"
        assertThat(refusal(cat) { delete(cat, "events", dv(f5, atV, count = 2)) }.message)
            .contains(atV)
            .contains("expected under ${under(cat, "events", "5")}")
    }

    @Test
    fun `a DV whose target has no value for the field is refused, never waved through`() {
        val cat = fixture()
        val four = good(cat)
        append(cat, "events", four)
        val (f4) = ids(cat, listOf(four.path))
        // Only reachable by hand (or by a retired spec purged under a live file).
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "DELETE FROM hog_file_partition_value v USING hog_catalog c " +
                    "WHERE c.catalog_id = v.catalog_id AND c.name = ? AND v.data_file_id = ?",
                cat,
                f4,
            )
        }
        val path = under(cat, "events", "4") + "dv-${UUID.randomUUID()}.puffin"
        val refused = refusal(cat) { delete(cat, "events", dv(f4, path)) }
        assertThat(refused.message).contains(path).contains("has no canonical 'team_id' value, got none")
    }

    /**
     * A file names its spec by spec_id at every snapshot, so a DV on a
     * file written under a spec that has since been retired reads its
     * value through that spec's field rows. Expiry past the spec change
     * must keep them (`ExpiryService.VERSIONED_TABLES_NEVER_CLEARED`):
     * were they cleared, the target would have no value and every DV on
     * it would be refused, under its own prefix included.
     */
    @Test
    fun `a DV on a file written under a retired spec is still checked against its prefix after expiry`() {
        val cat = fixture()
        // F1, under spec 1: [identity(team_id)].
        val four = good(cat, value = 4)
        append(cat, "events", four)
        val (f1) = ids(cat, listOf(four.path))
        // Spec 2 keeps the identity and adds day(ts); spec 1 is retired, and F1 still names it.
        alter.alterTable(cat, "ingest", "events", listOf(AlterOp.AddColumn(ColumnDef("ts", ColType.TIMESTAMPTZ))))
        val ts = catalogs.getTable(cat, "ingest", "events", totals = false).columns.single { it.def.name == "ts" }
        alter.alterTable(
            cat,
            "ingest",
            "events",
            listOf(AlterOp.SetPartitionSpec(listOf(spec.single(), PartitionFieldDef(ts.fieldId, Transform.DAY)))),
        )
        val retiredAt = db.head(cat)

        // Every snapshot out of retention: the floor lands on the spec change.
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_snapshot s SET snapshot_time = now() - interval '1 hour' FROM hog_catalog c " +
                    "WHERE c.catalog_id = s.catalog_id AND c.name = ?",
                cat,
            )
            h.execute("UPDATE hog_catalog SET snapshot_retention_seconds = 60 WHERE name = ?", cat)
        }
        val swept = ExpiryService(db.jdbi).runOnce(cat, batchSize = 100)
        assertThat(swept.newEarliestSnapshotId).describedAs("the floor passes the spec change").isEqualTo(retiredAt)
        val retired =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT ps.end_snapshot FROM hog_partition_spec ps JOIN hog_catalog c USING (catalog_id)
                    WHERE c.name = :cat AND ps.end_snapshot IS NOT NULL
                    """,
                ).bind("cat", cat).mapTo(Long::class.java).list()
            }
        assertThat(retired).describedAs("the retired spec F1 names outlives the floor").containsExactly(retiredAt)

        delete(cat, "events", dv(f1, under(cat, "events", "4") + "dv-${UUID.randomUUID()}.puffin"))
        val wrong = under(cat, "events", "5") + "dv-${UUID.randomUUID()}.puffin"
        val refused = refusal(cat) { delete(cat, "events", dv(f1, wrong, count = 2)) }
        assertThat(refused.table).isEqualTo("ingest.events")
        assertThat(refused.message).contains(wrong).contains("expected under ${under(cat, "events", "4")}")
    }

    /** One transaction: append [staged] to [table] and a DV at [dvPath] on it, by path. */
    private fun stagedTransaction(
        cat: String,
        table: String,
        staged: FileRegistration,
        dvPath: String,
    ): CommitRequest {
        val uuid = catalogs.getTable(cat, "ingest", table, totals = false).tableUuid
        return CommitRequest(
            readSnapshot = db.head(cat),
            idempotencyKey = UUID.randomUUID(),
            requireUnchangedTables = true,
            allowPendingDeletes = true,
            appends = listOf(TableAppend("ingest", table, listOf(staged), uuid)),
            deletes =
                listOf(
                    TableDeletes(
                        "ingest",
                        table,
                        listOf(DeleteFileRegistration(0, dvPath, 1, 64, dataFilePath = staged.path)),
                        uuid,
                    ),
                ),
        )
    }

    @Test
    fun `a transaction's DV on its own staged file is checked against the staged file's value`() {
        val cat = fixture()
        commits.commit(
            cat,
            stagedTransaction(
                cat,
                "events",
                good(cat, value = 5),
                under(cat, "events", "5") + "dv-${UUID.randomUUID()}.puffin",
            ),
        )
        val wrong = under(cat, "events", "4") + "dv-${UUID.randomUUID()}.puffin"
        val refused =
            refusal(cat) { commits.commit(cat, stagedTransaction(cat, "events", good(cat, value = 5), wrong)) }
        assertThat(refused.message).contains(wrong).contains("expected under ${under(cat, "events", "5")}")
    }

    @Test
    fun `a transaction's staged DV reads the identity key_index, not the file's last value`() {
        val cat = fixture()
        catalogs.createTable(cat, "ingest", "multi", columns)
        alter.alterTable(
            cat,
            "ingest",
            "multi",
            listOf(
                AlterOp.SetPartitionSpec(listOf(spec.single(), PartitionFieldDef(2, Transform.IDENTITY))),
                AlterOp.SetObjectPrefixField("team_id"),
            ),
        )

        // Key 0 is team_id ("4"), key 1 is v ("7"): the staged file's value
        // is the one at the identity's key_index, not whichever came last.
        fun staged() =
            FileRegistration(
                under(cat, "multi", "4") + "${UUID.randomUUID()}.parquet",
                10,
                1_000,
                20,
                stats(le8(4), le8(4)),
                listOf("4", "7"),
            )
        commits.commit(cat, stagedTransaction(cat, "multi", staged(), under(cat, "multi", "4") + "dv.puffin"))
        val atV = under(cat, "multi", "7") + "dv-${UUID.randomUUID()}.puffin"
        val refused = refusal(cat) { commits.commit(cat, stagedTransaction(cat, "multi", staged(), atV)) }
        assertThat(refused.message).contains(atV).contains("expected under ${under(cat, "multi", "4")}")
    }

    // ---- uploads, receipts, creation --------------------------------------------

    private fun claimState(
        cat: String,
        path: String,
    ): String =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT u.state FROM hog_upload u JOIN hog_catalog c USING (catalog_id) " +
                    "WHERE c.name = :cat AND u.path = :path",
            ).bind("cat", cat).bind("path", path).mapTo(String::class.java).one()
        }

    @Test
    fun `an upload claimed at the table root is refused and stays claimed, one under the prefix registers`() {
        val cat = fixture()
        val atRoot = UUID.randomUUID()
        val rootClaim = uploads.claim(cat, UUID.randomUUID(), atRoot, root(cat, "events"), "data")
        refusal(cat) {
            commits.commit(
                cat,
                CommitRequest(
                    readSnapshot = db.head(cat),
                    idempotencyKey = atRoot,
                    appends =
                        listOf(
                            TableAppend("ingest", "events", listOf(file(rootClaim.path, "4", stats(le8(4), le8(4))))),
                        ),
                ),
            )
        }
        assertThat(claimState(cat, rootClaim.path)).describedAs("the refusal rolls back, so nothing settles").isEqualTo(
            "active",
        )

        val underPrefix = UUID.randomUUID()
        val prefixClaim = uploads.claim(cat, UUID.randomUUID(), underPrefix, under(cat, "events", "4"), "data")
        commits.commit(
            cat,
            CommitRequest(
                readSnapshot = db.head(cat),
                idempotencyKey = underPrefix,
                appends =
                    listOf(
                        TableAppend("ingest", "events", listOf(file(prefixClaim.path, "4", stats(le8(4), le8(4))))),
                    ),
            ),
        )
        assertThat(claimState(cat, prefixClaim.path)).isEqualTo("registered")
    }

    @Test
    fun `a receipt replays even when the layout would now refuse its files`() {
        val cat = fixture()
        // `late` takes the option AFTER a keyed commit of a root-level
        // file, which the truncate makes legal.
        catalogs.createTable(cat, "ingest", "late", columns)
        alter.alterTable(cat, "ingest", "late", listOf(AlterOp.SetPartitionSpec(spec)))
        val request =
            CommitRequest(
                readSnapshot = db.head(cat),
                idempotencyKey = UUID.randomUUID(),
                appends =
                    listOf(
                        TableAppend(
                            "ingest",
                            "late",
                            listOf(file(root(cat, "late") + "a.parquet", "4", stats(le8(4), le8(4)))),
                        ),
                    ),
            )
        val published = commits.commit(cat, request)
        catalogs.truncateTable(
            cat,
            "ingest",
            "late",
            catalogs.getTable(cat, "ingest", "late", totals = false).tableUuid,
            author = "test",
            message = "empty it",
        )
        alter.alterTable(cat, "ingest", "late", listOf(AlterOp.SetObjectPrefixField("team_id")))

        assertThat(commits.commit(cat, request)).describedAs("the replay").isEqualTo(published)
        refusal(
            cat,
        ) { commits.commit(cat, request.copy(idempotencyKey = UUID.randomUUID(), readSnapshot = db.head(cat))) }
    }

    @Test
    fun `an atomic creation registers its files with no layout to check`() {
        val cat = fixture()
        val prepared =
            creations.prepare(
                cat,
                UUID.randomUUID(),
                TableCreationDefinition("ingest", "created", columns, partitionFields = spec),
            )
        val published =
            creations.publish(
                cat,
                prepared.operationId,
                listOf(file(prepared.writePath.trimEnd('/') + "/a.parquet", "4", null)),
            )
        assertThat(published.state).describedAs("%s", published.reason).isEqualTo("committed")
        assertThat(catalogs.getTable(cat, "ingest", "created", totals = false).objectPrefixField).isNull()
    }
}
