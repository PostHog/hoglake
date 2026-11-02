package com.posthog.hoglake.testing

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.persistence.NamespaceRepo
import com.posthog.hoglake.persistence.SpecRepo
import com.posthog.hoglake.persistence.TableRepo
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import java.util.concurrent.atomic.AtomicInteger

internal class PartitionLookupFixture(val db: PgTestSupport.TestDb) {
    companion object {
        private val names = AtomicInteger()
    }

    val name = "lookup-${names.incrementAndGet()}"
    val catalogs = CatalogService(db.jdbi)
    val commits = CommitService(db.jdbi)
    val alter = AlterService(db.jdbi)
    val catalogId: Long
    val tableId: Long
    val specId: Long
    val url: String get() = "/v1/catalogs/$name/namespaces/ns/tables/t/partitions/lookup"

    init {
        catalogId = catalogs.createCatalog(name, "s3://bucket/$name").catalogId
        catalogs.createNamespace(name, "ns")
        catalogs.createTable(name, "ns", "t", listOf(ColumnDef("a", ColType.STRING), ColumnDef("b", ColType.STRING)))
        alter.alterTable(
            name,
            "ns",
            "t",
            listOf(
                AlterOp.SetPartitionSpec(
                    listOf(PartitionFieldDef(1, Transform.IDENTITY), PartitionFieldDef(2, Transform.IDENTITY)),
                ),
            ),
        )
        val ids =
            db.jdbi.withHandleUnchecked { h ->
                val cat = CatalogRepo.require(h, name)
                val ns = NamespaceRepo.findLiveByName(h, catalogId, "ns")!!
                val t = TableRepo.findLive(h, catalogId, ns.namespaceId, "t")!!
                t.tableId to SpecRepo.specAt(h, catalogId, t.tableId, cat.headSnapshotId)!!.specId
            }
        tableId = ids.first
        specId = ids.second
    }

    fun append(vararg values: List<String?>): Long =
        commits.commit(
            name,
            CommitRequest(
                readSnapshot = db.head(name),
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "t",
                            values.mapIndexed { i, tuple ->
                                FileRegistration(
                                    path = "s3://bucket/$name/${db.head(name)}-$i.parquet",
                                    recordCount = 10,
                                    fileSizeBytes = 100,
                                    partitionValues = tuple,
                                )
                            },
                        ),
                    ),
            ),
        ).snapshotId
}
