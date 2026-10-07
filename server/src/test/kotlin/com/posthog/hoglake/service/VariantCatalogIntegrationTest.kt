package com.posthog.hoglake.service

import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableInfo
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.stats.IcebergSingleValue
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.testing.tableWithExactTotals
import com.posthog.hoglake.wireObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.UUID

@Tag("integration")
class VariantCatalogIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val alter = AlterService(db.jdbi)

    @AfterEach
    fun close() = db.close()

    private fun table(): TableInfo {
        catalogs.createCatalog("variant", "s3://test-bucket/variant/")
        catalogs.createNamespace("variant", "ns")
        return catalogs.createTable(
            "variant",
            "ns",
            "events",
            listOf(ColumnDef("id", ColType.LONG), ColumnDef("properties", ColType.VARIANT)),
        )
    }

    @Test
    fun `variant DDL publication receipt retry and scan preserve metadata`() {
        val table = table()
        assertThat(catalogs.getTable("variant", "ns", "events").columns.last().def.type).isEqualTo(ColType.VARIANT)
        val request =
            CommitRequest(
                idempotencyKey = UUID.randomUUID(),
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "events",
                            listOf(
                                FileRegistration(
                                    path = "s3://test-bucket/variant/data/native.parquet",
                                    recordCount = 1,
                                    fileSizeBytes = 512,
                                    columnStats = emptyList(),
                                ),
                            ),
                            expectedTableUuid = table.tableUuid,
                        ),
                    ),
            )
        val result = commits.commit("variant", request)
        assertThat(commits.commit("variant", request)).isEqualTo(result)
        assertThat(ScanService(db.jdbi).planScan("variant", "ns", "events")).hasSize(1)
        assertThat(catalogs.tableWithExactTotals("variant", "ns", "events").recordCount).isEqualTo(1)
    }

    @Test
    fun `variant cannot receive scalar stats or become a routing or sort key`() {
        val field = table().columns.last().fieldId
        assertThatThrownBy {
            commits.commit(
                "variant",
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "events",
                                listOf(
                                    FileRegistration(
                                        path = "s3://test-bucket/variant/data/bad.parquet",
                                        recordCount = 1,
                                        fileSizeBytes = 512,
                                        columnStats = listOf(ColumnStats(field, 1, 0, null, null, null, null)),
                                    ),
                                ),
                            ),
                        ),
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java).hasMessageContaining("variant column statistics")
        for (transform in Transform.entries) {
            assertThatThrownBy {
                alter.alterTable(
                    "variant",
                    "ns",
                    "events",
                    listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(field, transform, 8)))),
                )
            }.isInstanceOf(HoglakeException.Validation::class.java).hasMessageContaining("variant")
        }
        assertThatThrownBy {
            alter.alterTable(
                "variant",
                "ns",
                "events",
                listOf(AlterOp.SetSortOrder(listOf(SortFieldDef(field, SortDirection.ASC, NullOrder.NULLS_FIRST)))),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java).hasMessageContaining("variant")
        assertThatThrownBy { IcebergSingleValue.encode(ColType.VARIANT, "{}") }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    // ---- type_params.shredding ---------------------------------------------

    private val declaration: Map<String, Any?> =
        wireObjectMapper().readValue(
            """
            {"type": "object", "fields": [
                {"name": "${'$'}browser", "type": "string"},
                {"name": "price", "type": "decimal8", "precision": 18, "scale": 2},
                {"name": "tags", "type": "array", "element": {"type": "string"}},
                {"name": "${'$'}set", "type": "object", "fields": [{"name": "plan", "type": "string"}]}]}
            """,
        )

    private fun columnsAt(snapshot: Long? = null) =
        catalogs.getTable("variant", "ns", "events", snapshot).columns.associate { it.def.name to it.def.typeParams }

    @Test
    fun `a top-level variant keeps its declaration through add_column, rename and time travel`() {
        catalogs.createCatalog("variant", "s3://test-bucket/variant/")
        catalogs.createNamespace("variant", "ns")
        val created =
            catalogs.createTable(
                "variant",
                "ns",
                "events",
                listOf(
                    ColumnDef("id", ColType.LONG),
                    ColumnDef("properties", ColType.VARIANT, mapOf("shredding" to declaration)),
                    ColumnDef("plain", ColType.VARIANT, mapOf("shredding" to null)),
                ),
            )
        assertThat(columnsAt()).containsEntry("properties", mapOf("shredding" to declaration))
        assertThat(columnsAt()).containsEntry("plain", mapOf("shredding" to null))

        val added =
            alter.alterTable(
                "variant",
                "ns",
                "events",
                listOf(
                    AlterOp.AddColumn(
                        ColumnDef("extra", ColType.VARIANT, mapOf("shredding" to mapOf("type" to "int64"))),
                    ),
                    AlterOp.RenameColumn("properties", "props"),
                ),
            )
        assertThat(columnsAt())
            .containsEntry("props", mapOf("shredding" to declaration))
            .containsEntry("extra", mapOf("shredding" to mapOf("type" to "int64")))
            .doesNotContainKey("properties")
        assertThat(added.snapshotId).isGreaterThan(created.snapshotId!!)
        assertThat(columnsAt(created.snapshotId))
            .containsEntry("properties", mapOf("shredding" to declaration))
            .doesNotContainKey("extra")
    }

    @Test
    fun `a declaration no writer can honour is refused by every DDL path`() {
        catalogs.createCatalog("variant", "s3://test-bucket/variant/")
        catalogs.createNamespace("variant", "ns")
        val invalid = mapOf("shredding" to mapOf("type" to "text"))
        val message = "variant column 'properties' has an invalid type_params.shredding: $ has an unknown type 'text'"
        assertThatThrownBy {
            catalogs.createTable("variant", "ns", "events", listOf(ColumnDef("properties", ColType.VARIANT, invalid)))
        }.isInstanceOf(HoglakeException.Validation::class.java).hasMessage(message)
        assertThat(catalogs.listTables("variant", "ns")).isEmpty()

        val creations = TableCreationService(db.jdbi, catalogs, commits)
        assertThatThrownBy {
            creations.prepare(
                "variant",
                UUID.randomUUID(),
                TableCreationDefinition("ns", "events", listOf(ColumnDef("properties", ColType.VARIANT, invalid))),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java).hasMessage(message)

        catalogs.createTable(
            "variant",
            "ns",
            "events",
            listOf(
                ColumnDef("id", ColType.LONG),
                ColumnDef("r", ColType.STRUCT, children = listOf(ColumnDef("a", ColType.INT))),
            ),
        )
        assertThatThrownBy {
            alter.alterTable(
                "variant",
                "ns",
                "events",
                listOf(AlterOp.AddColumn(ColumnDef("properties", ColType.VARIANT, invalid))),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java).hasMessage(message)
        // Grafted into a struct, the column is nested, whatever its own depth.
        val valid = mapOf("shredding" to declaration)
        assertThatThrownBy {
            alter.alterTable(
                "variant",
                "ns",
                "events",
                listOf(AlterOp.AddColumn(ColumnDef("x", ColType.VARIANT, valid), "r")),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessage(
                "variant column 'x' is nested, and only a top-level variant column can declare type_params.shredding",
            )
        assertThatThrownBy {
            alter.alterTable("variant", "ns", "events", listOf(AlterOp.AddColumn(ColumnDef("j", ColType.JSON, valid))))
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessage("column 'j' is 'json', and only a variant column can declare type_params.shredding")
        assertThat(columnsAt().keys).containsExactly("id", "r")
    }

    @Test
    fun `a receipt prepared before declarations were checked is rejected when it publishes`() {
        // An older replica stored the definition without checking its
        // declaration. Publishing re-validates it, and the receipt
        // becomes terminal instead of creating a table no writer can
        // write.
        catalogs.createCatalog("variant", "s3://test-bucket/variant/")
        catalogs.createNamespace("variant", "ns")
        val creations = TableCreationService(db.jdbi, catalogs, commits)
        val operation =
            creations.prepare(
                "variant",
                UUID.randomUUID(),
                TableCreationDefinition("ns", "events", listOf(ColumnDef("properties", ColType.VARIANT))),
            )
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                UPDATE hog_table_creation
                   SET definition = jsonb_set(definition, '{columns,0,type_params}', '{"shredding": {"type": "text"}}')
                 WHERE operation_id = :op
                """,
            ).bind("op", operation.operationId).execute()
        }
        val published = creations.publish("variant", operation.operationId, emptyList())
        assertThat(published.state).isEqualTo("rejected")
        assertThat(published.reason).isEqualTo("definition_invalid")
        assertThat(catalogs.listTables("variant", "ns")).isEmpty()
    }
}
