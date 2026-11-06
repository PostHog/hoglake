package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.compaction.CompactionGrouping
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.PartitionGroup
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.service.AlterBackend
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.MaintenanceSummarySampler
import com.posthog.hoglake.service.PartitionListingBackend
import com.posthog.hoglake.service.PartitionListingService
import com.posthog.hoglake.service.PartitionStatsBackend
import com.posthog.hoglake.service.PartitionStatsService
import com.posthog.hoglake.service.ScanService
import com.posthog.hoglake.store.CatalogStore
import com.posthog.hoglake.store.PostgresCatalogStore
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.sql.Types
import java.util.UUID

/** Independent PG arms need no optional binary. Compact raw cap/spec fixtures are NOT production capacity evidence. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HoglitePartitionParityIntegrationTest {
    enum class Backend { POSTGRES, HOGLITE }

    private val json = jacksonObjectMapper()
    private var pg: PgTestSupport.TestDb? = null
    private var live: HogliteConformanceEngine? = null
    private var fileId = 0L

    @AfterAll
    fun close() {
        pg?.close()
        live?.close()
    }

    private data class Fixture(
        val backend: Backend,
        val cat: String,
        val store: CatalogStore,
        val alter: AlterBackend,
        val stats: PartitionStatsBackend,
        val listing: PartitionListingBackend,
    )

    private fun fixture(
        backend: Backend,
        body: (Fixture) -> Unit,
    ) {
        val store: CatalogStore
        val alter: AlterBackend
        val stats: PartitionStatsBackend
        val listing: PartitionListingBackend
        if (backend == Backend.HOGLITE) {
            val engine = live ?: HogliteConformanceEngine().also { live = it }
            store = engine.store
            alter = HogliteAlters(engine.store)
            val partitions = HoglitePartitions(engine.store, TARGET, 2)
            stats = partitions
            listing = partitions
        } else {
            val db = pg ?: PgTestSupport.freshDatabase().also { pg = it }
            store = PostgresCatalogStore(CatalogService(db.jdbi), CommitService(db.jdbi), ScanService(db.jdbi))
            alter = AlterService(db.jdbi)
            stats = PartitionStatsService(db.jdbi, TARGET, 2)
            listing = PartitionListingService(db.jdbi, TARGET, 2)
        }
        run {
            val cat = "part" + UUID.randomUUID().toString().replace("-", "")
            store.createCatalog(cat, "s3://hoglake-it/$cat")
            store.createNamespace(cat, "ns")
            store.createTable(
                cat,
                "ns",
                "t",
                listOf(ColumnDef("team", ColType.STRING), ColumnDef("ts", ColType.TIMESTAMP)),
            )
            alter.alterTable(
                cat,
                "ns",
                "t",
                listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(1, Transform.IDENTITY)))),
            )
            val f = Fixture(backend, cat, store, alter, stats, listing)
            // Publish a genuine empty sample before adding explicitly raw PG tier fixtures.
            sample(f)
            body(f)
        }
    }

    private fun sample(f: Fixture) {
        if (f.backend != Backend.POSTGRES) return
        val db = pg!!
        db.jdbi.useHandle<Exception> { h ->
            h.execute(
                "UPDATE hog_maintenance_summary SET next_batch_at = now() WHERE catalog_id = ?",
                f.store.getCatalog(f.cat).catalogId,
            )
        }
        val sampler = MaintenanceSummarySampler(db.jdbi, TARGET, 2, CompactionGrouping.DEFAULT_MAX_INPUT_FILES, 3600)
        while (sampler.runOnce()) { /* actual publication, not synthetic sample JSON */ }
    }

    private fun clear(f: Fixture) {
        if (f.backend == Backend.HOGLITE) {
            live!!.store.catalogOperation(f.cat) {
                live!!.store.runTx(
                    f.cat,
                    HogliteCatalogStore.freshTxid("fixture"),
                    null,
                    HogliteTx().apply {
                        add("DELETE FROM files")
                    },
                )
            }
        } else {
            pg!!.jdbi.useHandle<Exception> {
                it.execute(
                    "DELETE FROM hog_maintenance_summary_tier WHERE catalog_id = ?",
                    f.store.getCatalog(f.cat).catalogId,
                )
            }
        }
    }

    private data class RawGroup(
        val spec: Long?,
        val values: List<String?>?,
        val bytes: Long,
        val files: Int = 1,
        val namespace: String = "ns",
        val table: String = "t",
    )

    /** Out-of-current-spec tuples deliberately model metadata vintages, NOT API-valid writes. */
    private fun raw(
        f: Fixture,
        rows: List<RawGroup>,
    ) {
        for (r in rows) {
            val table = f.store.getTable(f.cat, r.namespace, r.table, totals = false).tableId
            val id = ++fileId
            if (f.backend == Backend.HOGLITE) {
                live!!.store.catalogOperation(f.cat) {
                    val tx = HogliteTx()
                    repeat(r.files) { i ->
                        tx.add(
                            "INSERT INTO files(id,table_id,path,record_count,file_size_bytes,row_id_start," +
                                "stats_state,begin_snapshot,spec_id,partition_values_json) " +
                                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                            id * 100 + i, table, "s3://hoglake-it/${f.cat}/$id-$i",
                            1, r.bytes, id * 100 + i, "provided", 1, r.spec,
                            r.values?.let { json.writeValueAsString(it) },
                        )
                    }
                    live!!.store.runTx(f.cat, HogliteCatalogStore.freshTxid("fixture"), null, tx)
                }
            } else {
                val small = if (r.bytes < TARGET) r.files else 0
                pg!!.jdbi.useHandle<Exception> { h ->
                    val q =
                        h.createUpdate(
                            """INSERT INTO hog_maintenance_summary_tier
                           (catalog_id,generation,bucket_key,table_id,spec_id,partition_values,
 quota,remaining,pending,selected,
 pending_max_bytes,file_count,small_count,total_bytes,small_bytes,dv_count,
 record_count,newest_begin_snapshot)
                           SELECT catalog_id,published_generation,?, ?, ?, ?::text[],1000,1000,0,?,0,?,?,?, ?,0,?,1
                           FROM hog_maintenance_summary WHERE catalog_id = ?""",
                        ).bind(0, "raw-$id").bind(1, table).bindBySqlType(2, r.spec, Types.BIGINT)
                            .bind(
                                4,
                                if (small >= 2) small else 0,
                            ).bind(5, r.files).bind(6, small).bind(7, r.bytes * r.files)
                            .bind(
                                8,
                                if (small > 0) r.bytes * r.files else 0L,
                            ).bind(9, r.files).bind(10, f.store.getCatalog(f.cat).catalogId)
                    if (r.values == null) {
                        q.bindBySqlType(3, null, Types.ARRAY)
                    } else {
                        q.bindArray(3, String::class.java, *r.values.toTypedArray())
                    }
                    assertThat(q.execute()).isEqualTo(1)
                }
            }
        }
    }

    /** One tiny file/group, bounded write batches; no stats blobs/DVs or raised engine guards. */
    private fun bulk(
        f: Fixture,
        first: Int,
        last: Int,
    ) {
        val table = f.store.getTable(f.cat, "ns", "t", totals = false).tableId
        if (f.backend == Backend.HOGLITE) {
            for (start in first..last step 1000) {
                val end = minOf(start + 999, last)
                live!!.store.catalogOperation(f.cat) {
                    val tx =
                        HogliteTx().apply {
                            add(
                                """INSERT INTO files(id,table_id,path,record_count,file_size_bytes,row_id_start,
 stats_state,begin_snapshot,spec_id,partition_values_json)
                            WITH RECURSIVE g(i) AS (SELECT $start UNION ALL SELECT i+1 FROM g WHERE i < $end)
                            SELECT i,$table,'s3://hoglake-it/${f.cat}/'||i,1,2000,i,'provided',1,1,
 json_array(printf('p%06d',i)) FROM g""",
                            )
                        }
                    live!!.store.runTx(f.cat, HogliteCatalogStore.freshTxid("fixture"), null, tx)
                }
            }
        } else {
            pg!!.jdbi.useHandle<Exception> { h ->
                h.execute(
                    """INSERT INTO hog_maintenance_summary_tier
                    (catalog_id,generation,bucket_key,table_id,spec_id,partition_values,
 quota,remaining,pending,selected,
 pending_max_bytes,file_count,small_count,total_bytes,small_bytes,dv_count,
 record_count,newest_begin_snapshot)
                    SELECT s.catalog_id,s.published_generation,'bulk-'||i,$table,1,ARRAY['p'||lpad(i::text,6,'0')],
 1000,1000,0,0,0,1,0,2000,0,0,1,1
                    FROM hog_maintenance_summary s CROSS JOIN generate_series($first,$last) i WHERE s.catalog_id = ?""",
                    f.store.getCatalog(f.cat).catalogId,
                )
            }
        }
    }

    @ParameterizedTest(name = "{displayName} [{index}] {0}")
    @EnumSource(Backend::class)
    fun `partition backend caps distinguish stats500 from listing1000`(backend: Backend) =
        fixture(backend) { f ->
            bulk(f, 1, 1001)
            for (limit in listOf(499, 500, 501)) {
                val report = f.stats.partitionStats(f.cat, null, null, limit)
                assertThat(report.partitions).hasSize(minOf(limit, 500))
                assertThat(report.truncated).isTrue()
            }
            for (limit in listOf(999, 1000, 1001)) {
                val report = f.listing.listPartitions(f.cat, "ns", "t", limit = limit)
                assertThat(report.partitions).hasSize(minOf(limit, 1000))
                assertThat(report.total).isEqualTo(1001)
            }
            val page = f.listing.listPartitions(f.cat, "ns", "t", limit = 1001, offset = 1000)
            assertThat(page.partitions).hasSize(1)
            assertThat(page.partitions.single().values.single().raw).isEqualTo("p001001")
            for (limit in listOf(0, -1)) {
                assertThatThrownBy {
                    f.stats.partitionStats(f.cat, null, null, limit)
                }.isInstanceOf(HoglakeException.Validation::class.java)
                assertThatThrownBy {
                    f.listing.listPartitions(f.cat, "ns", "t", limit = limit)
                }.isInstanceOf(HoglakeException.Validation::class.java)
            }
            assertThatThrownBy {
                f.listing.listPartitions(f.cat, "ns", "t", offset = -1)
            }.isInstanceOf(HoglakeException.Validation::class.java)
        }

    @ParameterizedTest(name = "{displayName} [{index}] {0}")
    @EnumSource(Backend::class)
    fun `compact listing group boundary accepts50000 refuses50001 before filters`(backend: Backend) =
        fixture(backend) { f ->
            bulk(f, 1, 50_000)
            val attempt = runCatching { f.listing.listPartitions(f.cat, "ns", "t", limit = 1) }
            assertThat(attempt.exceptionOrNull()).describedAs("exactly50000 tiny groups must not be refused").isNull()
            val accepted = attempt.getOrThrow()
            assertThat(accepted.total).isEqualTo(50_000)
            assertThat(accepted.partitions).hasSize(1)
            bulk(f, 50_001, 50_001)
            for (filters in listOf(emptyList(), listOf(PartitionListingService.Filter(0, "p000001")))) {
                assertThatThrownBy { f.listing.listPartitions(f.cat, "ns", "t", limit = 1, filters = filters) }
                    .isInstanceOf(
                        HoglakeException.Validation::class.java,
                    ).hasMessageContaining("more than 50000").hasMessageContaining("partition groups")
            }
        }

    @ParameterizedTest(name = "{displayName} [{index}] {0}")
    @EnumSource(Backend::class)
    fun `stats typed tuples retain absent empty null elements prefixes and numeric specs`(backend: Backend) =
        fixture(backend) { f ->
            val tuples =
                listOf<List<String?>?>(
                    null, emptyList(),
                    listOf(
                        null,
                    ),
                    listOf(
                        "a",
                    ),
                    listOf(
                        "a",
                        null,
                    ),
                    listOf(
                        "a",
                        "b",
                    ),
                    listOf(
                        "aa",
                    ),
                    listOf("b"), listOf("null"), listOf("a", "b", null), listOf("a", "b", "c"), listOf("a", ""),
                )
            val rows =
                tuples.mapIndexed { i, tuple -> RawGroup(2, tuple, 2001L + i) } +
                    listOf(
                        RawGroup(null, null, 2020), RawGroup(null, emptyList(), 2021),
                        RawGroup(null, listOf("a"), 2022), RawGroup(10, listOf("a"), 2030),
                    )
            val expected =
                listOf(2021L, 2022, 2020, 2002, 2004, 2012, 2006, 2011, 2010, 2005, 2007, 2008, 2009, 2003, 2001, 2030)
            for (insertion in listOf(rows.reversed(), rows)) {
                clear(f)
                raw(f, insertion)
                val report = f.stats.partitionStats(f.cat, null, null, 100)
                assertThat(report.partitions.map { it.totalBytes }).isEqualTo(expected)
                assertThat(
                    report.partitions.map { it.specId },
                ).containsExactly(null, null, null, *Array(12) { 2L }, 10L)
                assertThat(
                    report.partitions.filter {
                        it.totalBytes in listOf(2001L, 2002L, 2020L, 2021L)
                    }.map { it.partitionValues },
                ).allMatch { it.isEmpty() }
                if (backend == Backend.POSTGRES) {
                    val oracle =
                        pg!!.jdbi.withHandle<List<Long>, Exception> { h ->
                            h.createQuery(
                                "SELECT total_bytes FROM hog_maintenance_summary_tier WHERE catalog_id = ? " +
                                    "ORDER BY spec_id NULLS FIRST, partition_values",
                            )
                                .bind(0, f.store.getCatalog(f.cat).catalogId).mapTo(Long::class.java).list()
                        }
                    assertThat(oracle).isEqualTo(expected)
                }
            }
        }

    @ParameterizedTest(name = "{displayName} [{index}] {0}")
    @EnumSource(Backend::class)
    fun `stats rank precedence keeps debt smallbytes namespace and table ahead of tuple`(backend: Backend) =
        fixture(backend) { f ->
            f.store.createTable(f.cat, "ns", "a", listOf(ColumnDef("id", ColType.LONG)))
            f.store.createNamespace(f.cat, "aa")
            f.store.createTable(f.cat, "aa", "z", listOf(ColumnDef("id", ColType.LONG)))
            raw(
                f,
                listOf(
                    RawGroup(1, listOf("a"), 100, 2),
                    RawGroup(10, listOf("z"), 200, 2),
                    RawGroup(null, null, 200, 2, table = "a"),
                    RawGroup(null, null, 200, 2, namespace = "aa", table = "z"),
                    RawGroup(1, listOf("last"), 10, 3),
                ),
            )
            val rows = f.stats.partitionStats(f.cat, null, null, 100).partitions
            assertThat(
                rows.map {
                    Triple(it.namespace, it.table, it.totalBytes)
                },
            ).containsExactly(
                Triple("ns", "t", 30L),
                Triple("aa", "z", 400L),
                Triple("ns", "a", 400L),
                Triple("ns", "t", 400L),
                Triple("ns", "t", 200L),
            )
            assertThat(rows.map { it.debtScore }).containsExactly(3, 2, 2, 2, 2)
        }

    @ParameterizedTest(name = "{displayName} [{index}] {0}")
    @EnumSource(Backend::class)
    fun `listing decoded raw null prefix filters and every measure direction stay unchanged`(backend: Backend) =
        fixture(backend) { f ->
            f.alter.alterTable(
                f.cat,
                "ns",
                "t",
                listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(2, Transform.DAY)))),
            )
            val inputs =
                listOf(listOf<String?>(null), listOf("-1"), listOf("01"), listOf("1"), listOf("2"), listOf("2", "tail"))
            raw(f, inputs.mapIndexed { i, v -> RawGroup(2, v, if (i % 2 == 0) 100 else 2000L, i + 1) }.reversed())
            val ordered = f.listing.listPartitions(f.cat, "ns", "t").partitions
            assertThat(ordered.map { it.values.map { v -> v.raw } }).isEqualTo(inputs)
            assertThat(ordered[2].values[0].decoded).isEqualTo(ordered[3].values[0].decoded)

            fun measure(
                g: PartitionGroup,
                sort: PartitionListingService.SortColumn,
            ): Long =
                when (sort) {
                    PartitionListingService.SortColumn.PARTITION -> 0
                    PartitionListingService.SortColumn.FILES -> g.fileCount
                    PartitionListingService.SortColumn.SMALL_FILES -> g.smallFileCount
                    PartitionListingService.SortColumn.DEBT -> g.debtScore
                    PartitionListingService.SortColumn.TOTAL_SIZE -> g.totalBytes
                    PartitionListingService.SortColumn.AVG_SIZE -> g.avgFileBytes
                    PartitionListingService.SortColumn.DVS -> g.dvCount
                    PartitionListingService.SortColumn.ROWS -> g.recordCount!!
                    PartitionListingService.SortColumn.LAST_WRITTEN -> g.lastWrittenSnapshot!!
                }
            for (sort in PartitionListingService.SortColumn.entries) {
                for (desc in listOf(null, false, true)) {
                    val descending = desc ?: sort.descendingByDefault
                    val expected =
                        if (sort == PartitionListingService.SortColumn.PARTITION) {
                            if (descending) ordered.reversed() else ordered
                        } else {
                            // Stable sorting of the independent, explicitly asserted tuple order models measure ties.
                            if (descending) {
                                ordered.sortedByDescending {
                                    measure(
                                        it,
                                        sort,
                                    )
                                }
                            } else {
                                ordered.sortedBy { measure(it, sort) }
                            }
                        }
                    assertThat(
                        f.listing.listPartitions(f.cat, "ns", "t", sort = sort, desc = desc).partitions,
                    ).isEqualTo(expected)
                }
            }
            assertThat(
                f.listing.listPartitions(
                    f.cat,
                    "ns",
                    "t",
                    filters = listOf(PartitionListingService.Filter(0, "")),
                ).partitions.map {
                    it.values[0].raw
                },
            ).containsExactly(null)
            assertThat(
                f.listing.listPartitions(
                    f.cat,
                    "ns",
                    "t",
                    filters = listOf(PartitionListingService.Filter(0, "1970-01-02")),
                ).partitions.map {
                    it.values[0].raw
                },
            ).containsExactly("01", "1")
            raw(f, listOf(RawGroup(2, emptyList(), 3000)))
            assertThat(
                f.listing.listPartitions(
                    f.cat,
                    "ns",
                    "t",
                    filters = listOf(PartitionListingService.Filter(0, "")),
                ).total,
            ).isEqualTo(1)
            assertThatThrownBy {
                f.listing.listPartitions(f.cat, "ns", "t", filters = listOf(PartitionListingService.Filter(1, "x")))
            }.isInstanceOf(HoglakeException.Validation::class.java)
            // Equal decoded/raw tuples across specs are NOT a new LIST spec-id tiebreak.
            raw(f, listOf(RawGroup(10, listOf("3"), 4000), RawGroup(2, listOf("3"), 4000)))
            val tied =
                f.listing.listPartitions(
                    f.cat,
                    "ns",
                    "t",
                ).partitions.filter { it.values.firstOrNull()?.raw == "3" }
            assertThat(tied.map { it.specId }).containsExactlyInAnyOrder(10L, 2L)
            // The underlying read may return either spec first; LIST deliberately leaves that tie unresolved.
            // Identity vintage exercises case-insensitive prefixes, separately from the day-decoding fixtures.
            raw(f, listOf(RawGroup(1, listOf("Alpha"), 4000)))
            assertThat(
                f.listing.listPartitions(
                    f.cat,
                    "ns",
                    "t",
                    filters = listOf(PartitionListingService.Filter(0, "al")),
                ).partitions.map {
                    it.values[0].raw
                },
            ).containsExactly("Alpha")
        }

    @ParameterizedTest(name = "{displayName} [{index}] {0}")
    @EnumSource(Backend::class)
    fun `store writes use actual PG sampler while hoglite stays exact and legacy escapes coalesce`(backend: Backend) =
        fixture(backend) { f ->
            run {
                fun append(
                    value: String,
                    suffix: String,
                ) {
                    f.store.commit(
                        f.cat,
                        CommitRequest(
                            readSnapshot = f.store.getCatalog(f.cat).headSnapshotId,
                            appends =
                                listOf(
                                    TableAppend(
                                        "ns",
                                        "t",
                                        listOf(
                                            FileRegistration(
                                                "s3://hoglake-it/${f.cat}/$suffix",
                                                1,
                                                100,
                                                partitionValues = listOf(value),
                                            ),
                                        ),
                                    ),
                                ),
                        ),
                    )
                }
                append("z", "z1")
                append("a", "a1")
                append("a", "a2")
                sample(f)
                val before = f.stats.partitionStats(f.cat, null, null, 50)
                assertThat(before.partitions.map { it.partitionValues[0].value }).containsExactly("a", "z")
                assertThat(before.partitions.first().debtScore).isEqualTo(2)
                assertThat(before.sampledAt != null).isEqualTo(backend == Backend.POSTGRES)
                append("b", "b1")
                val now = f.stats.partitionStats(f.cat, null, null, 50)
                assertThat(now.partitions).hasSize(if (backend == Backend.HOGLITE) 3 else 2)
                if (backend == Backend.HOGLITE) {
                    live!!.store.catalogOperation(f.cat) {
                        live!!.store.runTx(
                            f.cat,
                            HogliteCatalogStore.freshTxid("fixture"),
                            null,
                            HogliteTx().apply {
                                add(
                                    "UPDATE files SET partition_values_json = ? WHERE path = ?",
                                    "[ \"\\u0061\" ]",
                                    "s3://hoglake-it/${f.cat}/a1",
                                )
                            },
                        )
                    }
                    val escaped = f.stats.partitionStats(f.cat, null, null, 50)
                    assertThat(escaped.partitions.first().partitionValues[0].value).isEqualTo("a")
                    assertThat(escaped.partitions.first().fileCount).isEqualTo(2)
                }
            }
        }

    private companion object {
        const val TARGET = 1000L
    }
}
