package com.posthog.hoglake.service

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
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
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
        assertThat(catalogs.getTable("variant", "ns", "events").recordCount).isEqualTo(1)
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
}
