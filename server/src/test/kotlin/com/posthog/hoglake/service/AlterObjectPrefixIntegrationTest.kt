package com.posthog.hoglake.service

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.HttpClient
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
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
 * `set_object_prefix_field` (V29) and the DDL rules that keep the layout
 * true once it is on: what the option may be set on, that it is set once,
 * and every alter that would strand the files written under it. The
 * commit-side rule is `CommitObjectPrefixIntegrationTest`.
 *
 * Each refusal is asserted with its reason AND with the state it leaves —
 * no layout row, nothing in the GET — because an alter that throws after
 * inserting the row would otherwise pass on the message alone.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AlterObjectPrefixIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val alter = AlterService(db.jdbi)
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val creations = TableCreationService(db.jdbi, catalogs, commits)
    private val app = App.build(Config(hydratorIntervalMs = 0), db.jdbi)
    private val json = ObjectMapper()
    private val counter = AtomicInteger(0)

    @AfterAll
    fun tearDown() = db.close()

    /**
     * `ns.events`: team_id int (field 1, promotable), id long (2), u64
     * uint64 (3), name string (4), addr struct{zip int} (5, 6). No spec.
     */
    private fun fixture(): String {
        val cat = "prefix-alter-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(
            cat,
            "ns",
            "events",
            listOf(
                ColumnDef("team_id", ColType.INT),
                ColumnDef("id", ColType.LONG),
                ColumnDef("u64", ColType.UINT64),
                ColumnDef("name", ColType.STRING),
                ColumnDef("addr", ColType.STRUCT, children = listOf(ColumnDef("zip", ColType.INT))),
            ),
        )
        return cat
    }

    private fun alter(
        cat: String,
        vararg ops: AlterOp,
    ) = alter.alterTable(cat, "ns", "events", ops.toList())

    private fun identity(fieldId: Long) = PartitionFieldDef(fieldId, Transform.IDENTITY)

    /** The fixture with the option on team_id, set together with its spec. */
    private fun withOption(): String =
        fixture().also {
            alter(
                it,
                AlterOp.SetPartitionSpec(listOf(identity(1))),
                AlterOp.SetObjectPrefixField("team_id"),
            )
        }

    private fun field(
        cat: String,
        snapshot: Long? = null,
    ): String? = catalogs.getTable(cat, "ns", "events", snapshot = snapshot, totals = false).objectPrefixField

    /** Every hog_table_layout row of the catalog: (begin, end, field id). */
    private fun layoutRows(cat: String): List<Triple<Long, Long?, Long>> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT l.begin_snapshot, l.end_snapshot, l.object_prefix_field_id
                FROM hog_table_layout l JOIN hog_catalog c USING (catalog_id)
                WHERE c.name = :cat ORDER BY l.begin_snapshot
                """,
            ).bind("cat", cat)
                .map { rs, _ ->
                    Triple(rs.getLong(1), rs.getObject(2) as Long?, rs.getLong(3))
                }.list()
        }

    private fun assertRefused(
        cat: String,
        vararg ops: AlterOp,
        reason: String,
    ) {
        val before = layoutRows(cat)
        val head = db.head(cat)
        val thrown = catchThrowable { alter(cat, *ops) }
        assertThat(thrown).describedAs("%s", ops.toList()).isInstanceOf(HoglakeException.Validation::class.java)
        assertThat(thrown.message).contains(reason)
        assertThat(layoutRows(cat)).describedAs("a refused alter leaves the layout as it was").isEqualTo(before)
        assertThat(db.head(cat)).describedAs("a refused alter publishes nothing").isEqualTo(head)
    }

    private fun le4(v: Int): ByteArray = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(v).array()

    /** One conforming team_id=4 file (the option on or off, its value is "4"). */
    private fun teamFile(cat: String): FileRegistration =
        FileRegistration(
            path = "s3://bucket/$cat/data/ns/events/team_id=4/${UUID.randomUUID()}.parquet",
            recordCount = 10,
            fileSizeBytes = 1_000,
            footerSize = 20,
            columnStats = listOf(ColumnStats(1, 10, 0, null, null, le4(4), le4(4))),
            partitionValues = listOf("4"),
        )

    private fun append(
        cat: String,
        readSnapshot: Long = db.head(cat),
    ) = commits.commit(
        cat,
        CommitRequest(
            readSnapshot = readSnapshot,
            appends = listOf(TableAppend("ns", "events", listOf(teamFile(cat)))),
        ),
    )

    // ---- setting it --------------------------------------------------------

    @Test
    fun `set with its spec in one request, and every read at every snapshot agrees`() {
        val cat = fixture()
        val before = db.head(cat)
        val receipt = alter(cat, AlterOp.SetPartitionSpec(listOf(identity(1))), AlterOp.SetObjectPrefixField("team_id"))
        val at = receipt.snapshotId!!

        assertThat(receipt.objectPrefixField).describedAs("the alter receipt").isEqualTo("team_id")
        assertThat(field(cat)).describedAs("GET at head").isEqualTo("team_id")
        assertThat(field(cat, at)).describedAs("GET at the alter's snapshot").isEqualTo("team_id")
        assertThat(field(cat, before)).describedAs("GET at the snapshot before the set").isNull()
        assertThat(layoutRows(cat)).containsExactly(Triple(at, null, 1L))
        // One DDL commit: the set is a `table_altered` change like any op,
        // which is what fences a writer whose read_snapshot predates it.
        val kinds =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT kind FROM hog_snapshot_change ch JOIN hog_catalog c USING (catalog_id)
                    WHERE c.name = :cat AND ch.snapshot_id = :s
                    """,
                ).bind("cat", cat).bind("s", at).mapTo(String::class.java).list()
            }
        assertThat(kinds).containsExactly("table_altered")
    }

    @Test
    fun `a writer whose read_snapshot predates the set gets ddl_since_read_snapshot`() {
        val cat = fixture()
        alter(cat, AlterOp.SetPartitionSpec(listOf(identity(1))))
        val read = db.head(cat)
        alter(cat, AlterOp.SetObjectPrefixField("team_id"))
        assertThatThrownBy { append(cat, readSnapshot = read) }
            .isInstanceOf(HoglakeException.DdlSinceReadSnapshot::class.java)
        // ...and the same file against a fresh read registers: the layout
        // admits it.
        append(cat)
    }

    @Test
    fun `the same field again is a no-op, and another field is refused`() {
        val cat = withOption()
        // Non-empty first: the no-op answers before the emptiness probe,
        // so a table that has data can be re-set to the same field.
        append(cat)
        val rows = layoutRows(cat)
        alter(cat, AlterOp.SetObjectPrefixField("team_id"))
        assertThat(layoutRows(cat)).describedAs("no second row for the same field").isEqualTo(rows)
        assertRefused(
            cat,
            AlterOp.SetPartitionSpec(listOf(identity(1), identity(2))),
            AlterOp.SetObjectPrefixField("id"),
            reason = "already 'team_id'",
        )
        assertThat(field(cat)).isEqualTo("team_id")
    }

    @Test
    fun `the field must be a top-level integer column with exactly one identity field on it`() {
        val cat = fixture()
        assertRefused(cat, AlterOp.SetObjectPrefixField("nope"), reason = "does not exist")
        assertRefused(
            cat,
            AlterOp.SetPartitionSpec(listOf(identity(6))),
            AlterOp.SetObjectPrefixField("addr.zip"),
            reason = "must be a top-level column",
        )
        // uint64 maps to Iceberg decimal(20,0), and is excluded however it
        // is partitioned; string is not an integer at all.
        assertRefused(
            cat,
            AlterOp.SetPartitionSpec(listOf(identity(3))),
            AlterOp.SetObjectPrefixField("u64"),
            reason = "'uint64'",
        )
        assertRefused(
            cat,
            AlterOp.SetPartitionSpec(listOf(identity(4))),
            AlterOp.SetObjectPrefixField("name"),
            reason = "'string'",
        )
        assertRefused(cat, AlterOp.SetObjectPrefixField("team_id"), reason = "exactly one identity field")
        for (spec in listOf(
            // bucket(team_id) only: a file's value is a bucket, not a team.
            listOf(PartitionFieldDef(1, Transform.BUCKET, 16)),
            // an identity on another column only.
            listOf(identity(2)),
            // two identity fields on it: validatePartitionFields has no
            // duplicate check, so this is the one rule that refuses it.
            listOf(identity(1), identity(1)),
        )) {
            assertRefused(
                cat,
                AlterOp.SetPartitionSpec(spec),
                AlterOp.SetObjectPrefixField("team_id"),
                reason = "exactly one identity field",
            )
        }
        assertThat(field(cat)).isNull()
    }

    @Test
    fun `an identity field at any key_index is accepted, beside other fields`() {
        val cat = fixture()
        alter(
            cat,
            AlterOp.SetPartitionSpec(listOf(identity(2), PartitionFieldDef(1, Transform.BUCKET, 4), identity(1))),
            AlterOp.SetObjectPrefixField("team_id"),
        )
        assertThat(field(cat)).isEqualTo("team_id")
    }

    @Test
    fun `only an empty table takes the option, and truncate empties it`() {
        val cat = fixture()
        alter(cat, AlterOp.SetPartitionSpec(listOf(identity(1))))
        append(cat)
        assertRefused(cat, AlterOp.SetObjectPrefixField("team_id"), reason = "only be set on an empty table")
        catalogs.truncateTable(
            cat,
            "ns",
            "events",
            catalogs.getTable(cat, "ns", "events", totals = false).tableUuid,
            author = "test",
            message = "empty the table",
        )
        alter(cat, AlterOp.SetObjectPrefixField("team_id"))
        assertThat(field(cat)).isEqualTo("team_id")
    }

    // ---- keeping it true ---------------------------------------------------

    @Test
    fun `while on, a spec change must keep exactly one identity field on it`() {
        val cat = withOption()
        assertRefused(cat, AlterOp.SetPartitionSpec(emptyList()), reason = "must keep exactly one identity")
        assertRefused(cat, AlterOp.SetPartitionSpec(listOf(identity(2))), reason = "must keep exactly one identity")
        assertRefused(
            cat,
            AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(1, Transform.BUCKET, 16))),
            reason = "must keep exactly one identity",
        )
        assertRefused(
            cat,
            AlterOp.SetPartitionSpec(listOf(identity(1), identity(1))),
            reason = "must keep exactly one identity",
        )
        // A reordered spec that keeps it is fine.
        val receipt = alter(cat, AlterOp.SetPartitionSpec(listOf(identity(2), identity(1))))
        assertThat(receipt.partitionSpec!!.fields.map { it.sourceFieldId }).containsExactly(2L, 1L)
        assertThat(receipt.objectPrefixField).isEqualTo("team_id")
    }

    @Test
    fun `while on, the field cannot be dropped or renamed, nor the table renamed`() {
        val cat = withOption()
        assertRefused(cat, AlterOp.DropColumn("team_id"), reason = "object-prefix field")
        assertRefused(cat, AlterOp.RenameColumn("team_id", "team"), reason = "object-prefix field")
        assertRefused(cat, AlterOp.RenameTable("events2"), reason = "object-prefix layout")
        // Other columns are unaffected, and so is promoting the field
        // within the allowed set (int -> long): the commit checks bounds
        // against the LIVE type.
        alter(cat, AlterOp.RenameColumn("name", "label"), AlterOp.DropColumn("u64"))
        val promoted = alter(cat, AlterOp.PromoteColumn("team_id", ColType.LONG))
        assertThat(promoted.objectPrefixField).isEqualTo("team_id")
        assertThat(promoted.columns.first { it.def.name == "team_id" }.def.type).isEqualTo(ColType.LONG)
    }

    @Test
    fun `an alter that sets the option and then breaks it rolls back whole`() {
        val cat = fixture()
        assertRefused(
            cat,
            AlterOp.SetPartitionSpec(listOf(identity(1))),
            AlterOp.SetObjectPrefixField("team_id"),
            AlterOp.RenameTable("events2"),
            reason = "object-prefix layout",
        )
        assertThat(catalogs.getTable(cat, "ns", "events", totals = false).partitionSpec).isNull()
        // Renaming BEFORE the set is fine: the layout begins with the new name.
        val receipt =
            alter.alterTable(
                cat,
                "ns",
                "events",
                listOf(
                    AlterOp.RenameTable("events2"),
                    AlterOp.SetPartitionSpec(listOf(identity(1))),
                    AlterOp.SetObjectPrefixField("team_id"),
                ),
            )
        assertThat(receipt.name).isEqualTo("events2")
        assertThat(receipt.objectPrefixField).isEqualTo("team_id")
    }

    @Test
    fun `an atomic replacement of a table with the option is refused at prepare, and the table keeps it`() {
        val cat = withOption()
        val live = catalogs.getTable(cat, "ns", "events", totals = false)
        val operation = UUID.randomUUID()
        val head = db.head(cat)
        assertThatThrownBy {
            creations.prepare(
                cat,
                operation,
                TableCreationDefinition(
                    "ns",
                    "events",
                    listOf(ColumnDef("team_id", ColType.INT)),
                    ReplacementTarget(live.tableUuid, head),
                    author = "test",
                    message = "replace",
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("cannot replace table 'ns.events'")
            .hasMessageContaining("object-prefix layout")
        // Nothing was prepared: no receipt to publish, abort or expire.
        assertThatThrownBy { creations.status(cat, operation) }.isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(db.head(cat)).isEqualTo(head)
        val after = catalogs.getTable(cat, "ns", "events", totals = false)
        assertThat(after.tableUuid).isEqualTo(live.tableUuid)
        assertThat(after.objectPrefixField).isEqualTo("team_id")
    }

    @Test
    fun `a replacement prepared without that check is rejected at publish by the backstop`() {
        val cat = fixture()
        alter(cat, AlterOp.SetPartitionSpec(listOf(identity(1))))
        val live = catalogs.getTable(cat, "ns", "events", totals = false)
        val prepared =
            creations.prepare(
                cat,
                UUID.randomUUID(),
                TableCreationDefinition(
                    "ns",
                    "events",
                    listOf(ColumnDef("team_id", ColType.INT)),
                    ReplacementTarget(live.tableUuid, db.head(cat)),
                    author = "test",
                    message = "replace",
                ),
            )
        // What a replica without the prepare check let through: the option
        // on at read_snapshot. By hand, so no change row after it makes the
        // publish `target_changed` first.
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_table_layout (catalog_id, table_id, begin_snapshot, object_prefix_field_id) " +
                    "SELECT catalog_id, ?, last_snapshot_id, 1 FROM hog_catalog WHERE name = ?",
                live.tableId,
                cat,
            )
        }
        val published = creations.publish(cat, prepared.operationId, emptyList())
        assertThat(published.state).isEqualTo("rejected")
        assertThat(published.reason).isEqualTo("definition_invalid")
        val after = catalogs.getTable(cat, "ns", "events", totals = false)
        assertThat(after.tableUuid).isEqualTo(live.tableUuid)
        assertThat(after.objectPrefixField).isEqualTo("team_id")
    }

    @Test
    fun `a dropped and recreated table starts without the option`() {
        val cat = withOption()
        catalogs.dropTable(cat, "ns", "events", author = "test", message = "drop")
        catalogs.createTable(cat, "ns", "events", listOf(ColumnDef("team_id", ColType.INT)))
        assertThat(field(cat)).isNull()
    }

    // ---- the wire ----------------------------------------------------------

    private fun api(block: suspend (HttpClient) -> Unit) =
        testApplication {
            application { app.module(this) }
            block(client)
        }

    private suspend fun HttpClient.postJson(
        url: String,
        body: String,
    ): HttpResponse =
        post(url) {
            contentType(ContentType.Application.Json)
            setBody(body)
        }

    private suspend fun body(response: HttpResponse): JsonNode = json.readTree(response.bodyAsText())

    @Test
    fun `over the wire - the op, the receipt, the GET at each snapshot, the capability`() {
        val cat = fixture()
        val table = "/v1/catalogs/$cat/namespaces/ns/tables/events"
        api { client ->
            val capabilities = body(client.get("/v1/catalogs/$cat"))["capabilities"].map { it.asText() }
            assertThat(capabilities).contains("object-prefix-layout-v1")

            val before = db.head(cat)
            assertThat(body(client.get(table)).has("object_prefix_field"))
                .describedAs("omitted while the option is off")
                .isFalse()
            val missing = client.postJson("$table/alter", """{"ops":[{"op":"set_object_prefix_field"}]}""")
            assertThat(missing.status).isEqualTo(HttpStatusCode.BadRequest)
            assertThat(body(missing)["detail"].asText()).contains("requires field 'name'")

            val altered =
                client.postJson(
                    "$table/alter",
                    """{"ops":[
                        {"op":"set_partition_spec","fields":[{"source_field_id":1,"transform":"identity"}]},
                        {"op":"set_object_prefix_field","name":"team_id"}]}""",
                )
            assertThat(altered.status).describedAs(altered.bodyAsText()).isEqualTo(HttpStatusCode.OK)
            assertThat(body(altered)["object_prefix_field"].asText()).isEqualTo("team_id")
            assertThat(body(client.get(table))["object_prefix_field"].asText()).isEqualTo("team_id")
            assertThat(body(client.get("$table?snapshot=$before")).has("object_prefix_field")).isFalse()

            val refused = client.postJson("$table/alter", """{"ops":[{"op":"rename_table","new_name":"x"}]}""")
            assertThat(refused.status).isEqualTo(HttpStatusCode.UnprocessableEntity)
            assertThat(body(refused)["error"].asText()).isEqualTo("validation")
        }
    }
}
