package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.persistence.CatalogBinding
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.service.MaintenanceSummarySampler
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.jdbi.v3.core.statement.SqlLogger
import org.jdbi.v3.core.statement.StatementContext
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.UUID

@Tag("integration")
class PostgresCreationAndSamplerParityIntegrationTest {
    private data class Issued(
        val raw: String,
        val rendered: String,
        val parameters: String,
        val parsed: String,
        val executed: String,
        val arrayTypes: List<String>,
    )

    private fun capture(jdbi: Jdbi): MutableList<Issued> {
        val issued = mutableListOf<Issued>()
        jdbi.setSqlLogger(
            object : SqlLogger {
                override fun logAfterExecution(context: StatementContext) {
                    val item =
                        Issued(
                            context.rawSql,
                            context.renderedSql,
                            context.binding.toString(),
                            context.parsedSql.sql,
                            context.statement.toString(),
                            if (context.rawSql.contains(":catalogIds")) {
                                val metadata = context.statement.parameterMetaData
                                (1..metadata.parameterCount).map {
                                    "${metadata.getParameterType(
                                        it,
                                    )}:${metadata.getParameterTypeName(it)}"
                                }
                            } else {
                                emptyList()
                            },
                        )
                    issued += item
                    println("SQL_CAPTURE $item")
                }
            },
        )
        return issued
    }

    private fun <T> succeeds(body: () -> T): T {
        val result = runCatching(body)
        assertThat(result.exceptionOrNull()).isNull()
        return result.getOrThrow()
    }

    @Test
    fun `ordinary creation uses identity default under a real role without sequence rights before and after rename`() {
        PgTestSupport.freshDatabase().use { db ->
            val role = "catalog_writer_" + UUID.randomUUID().toString().replace("-", "")
            val seq =
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery(
                        "SELECT pg_get_serial_sequence('hog_catalog', 'catalog_id')",
                    ).mapTo(String::class.java).one()
                }
            db.jdbi.useHandle<Exception> { h ->
                h.execute(
                    "CREATE ROLE $role LOGIN PASSWORD 'fixture-only' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT",
                )
                h.execute("GRANT USAGE ON SCHEMA public TO $role")
                h.execute("GRANT INSERT, SELECT ON hog_catalog TO $role")
            }
            try {
                val restricted = Jdbi.create(db.jdbcUrl, role, "fixture-only")
                val issued = capture(restricted)
                restricted.useHandle<Exception> { h ->
                    assertThat(h.createQuery("SELECT current_user").mapTo(String::class.java).one()).isEqualTo(role)
                    assertThat(
                        h.createQuery("SELECT rolsuper OR rolinherit FROM pg_roles WHERE rolname = current_user")
                            .mapTo(Boolean::class.java).one(),
                    ).isFalse()
                    assertThat(
                        h.createQuery("SELECT has_sequence_privilege(current_user, :s, 'USAGE,UPDATE')")
                            .bind("s", seq).mapTo(Boolean::class.java).one(),
                    ).isFalse()
                    assertThatThrownBy {
                        h.createQuery(
                            "SELECT nextval(:s::regclass)",
                        ).bind("s", seq).mapTo(Long::class.java).one()
                    }
                        .hasRootCauseInstanceOf(java.sql.SQLException::class.java)
                        .rootCause().hasFieldOrPropertyWithValue("SQLState", "42501")
                    val first = succeeds { CatalogRepo.insert(h, "default-before", "s3://bucket/before") }
                    assertThat(first.catalogId).isPositive()
                    db.jdbi.useHandle<Exception> {
                            owner ->
                        owner.execute("ALTER SEQUENCE $seq RENAME TO renamed_catalog_identity")
                    }
                    val second = succeeds { CatalogRepo.insert(h, "default-after", "s3://bucket/after") }
                    assertThat(second.catalogId).isGreaterThan(first.catalogId)
                    for (sql in issued.filter { it.raw.contains("INSERT INTO hog_catalog") }) {
                        assertThat(
                            sql.raw.lowercase(),
                        ).doesNotContain("nextval", "boundid", "overriding", "hog_catalog (catalog_id")
                        assertThat(sql.parameters).doesNotContain("boundId")
                    }
                    assertThat(issued.count { it.raw.contains("INSERT INTO hog_catalog") }).isEqualTo(2)
                }
            } finally {
                db.jdbi.useHandle<Exception> { h ->
                    h.execute("DROP OWNED BY $role")
                    h.execute("DROP ROLE $role")
                }
            }
        }
    }

    @Test
    fun `bound creation preserves id sequence typed errors and fencing after rename`() {
        PgTestSupport.freshDatabase().use { db ->
            val issued = capture(db.jdbi)
            db.jdbi.useHandle<Exception> { h ->
                val first = CatalogRepo.insert(h, "ordinary", "s3://bucket/a")
                h.execute("ALTER SEQUENCE hog_catalog_catalog_id_seq RENAME TO bound_identity")
                val before = h.createQuery("SELECT last_value FROM bound_identity").mapTo(Long::class.java).one()
                CatalogBinding.withIdentity("bound", 50000L) {
                    assertThat(succeeds { CatalogRepo.insert(h, "bound", "s3://bucket/b") }.catalogId).isEqualTo(50000L)
                    assertThat(CatalogRepo.require(h, "bound").catalogId).isEqualTo(50000L)
                    assertThat(CatalogBinding.id("different")).isNull()
                    assertThatThrownBy {
                        CatalogBinding.withIdentity(
                            "different",
                            5L,
                        ) {}
                    }.isInstanceOf(IllegalStateException::class.java)
                }
                assertThat(
                    h.createQuery("SELECT last_value FROM bound_identity").mapTo(Long::class.java).one(),
                ).isEqualTo(before)
                val boundSql =
                    issued.single {
                        it.raw.contains("INSERT INTO hog_catalog") && it.parameters.contains("boundId")
                    }
                assertThat(
                    boundSql.raw,
                ).contains("OVERRIDING SYSTEM VALUE", ":boundId").doesNotContain("nextval", "COALESCE")
                assertThatThrownBy { CatalogBinding.withIdentity("bound", 50001L) { CatalogRepo.require(h, "bound") } }
                    .isInstanceOf(HoglakeException.NotFound::class.java)
                assertThat(CatalogBinding.currentId()).isNull()
                assertThat(CatalogRepo.insert(h, "after", "s3://bucket/c").catalogId).isEqualTo(first.catalogId + 1)
                CatalogBinding.withIdentity("bound", 50000L) {
                    CatalogBinding.withIdentity("bound", 50000L) {
                        assertThat(CatalogBinding.currentId()).isEqualTo(50000L)
                    }
                    assertThat(CatalogBinding.currentId()).isEqualTo(50000L)
                    val other = succeeds { CatalogRepo.insert(h, "different", "s3://bucket/other") }
                    assertThat(other.catalogId).isEqualTo(first.catalogId + 2)
                    assertThat(CatalogBinding.currentId()).isEqualTo(50000L)
                }
                assertThat(CatalogBinding.currentId()).isNull()
                assertThatThrownBy {
                    CatalogRepo.insert(h, "bound", "s3://bucket/d")
                }.isInstanceOf(HoglakeException.AlreadyExists::class.java)
                assertThatThrownBy {
                    CatalogRepo.insert(h, "INVALID", "s3://bucket/d")
                }.isInstanceOf(HoglakeException.Validation::class.java)
            }
        }
    }

    @Test
    fun `real mixed reservation uses exactly its allocated identity after sequence rename`() {
        PgTestSupport.freshDatabase().use { db ->
            HogliteConformanceEngine().use { engine ->
                db.jdbi.useHandle<Exception> { h ->
                    CatalogRepo.insert(h, "sequence-control", "s3://bucket/control")
                    h.execute("ALTER SEQUENCE hog_catalog_catalog_id_seq RENAME TO mixed_identity")
                }

                fun sequence() =
                    db.jdbi.withHandleUnchecked { h ->
                        h.createQuery("SELECT last_value FROM mixed_identity").mapTo(Long::class.java).one()
                    }
                val before = sequence()
                val cfg =
                    engine.config().copy(
                        database = com.posthog.hoglake.DatabaseBackend.MIXED,
                        hogliteUrl = engine.url(),
                    )
                com.posthog.hoglake.App.buildMixed(
                    cfg,
                    db.jdbi,
                    engine.store,
                    com.posthog.hoglake.HealthProbe.forProbe(
                        1000,
                    ) {
                        true
                    },
                ).use {
                        app ->
                    app.mixedRouter.directory.initialize()
                    var reserved: Long? = null
                    app.mixedRouter.beforePostgresInsert = { entry ->
                        reserved = entry.identity.toLong()
                        assertThat(reserved).isEqualTo(before + 1)
                        assertThat(sequence()).isEqualTo(reserved)
                    }
                    val result =
                        succeeds {
                            app.store.createCatalog(
                                "mixed-default",
                                "s3://bucket/mixed",
                                backend = "postgres",
                                connectionRef = "postgres",
                            )
                        }
                    assertThat(result.catalogId).isEqualTo(reserved)
                    assertThat(sequence()).isEqualTo(before + 1)
                    assertThat(
                        app.mixedRouter.directory.find("mixed-default")!!.identity,
                    ).isEqualTo(reserved.toString())
                    assertThat(app.store.getCatalog("mixed-default").catalogId).isEqualTo(reserved)
                    app.store.dropCatalog("mixed-default", "test", "drop fixture")
                    assertThat(app.mixedRouter.directory.find("mixed-default")).isNull()
                }
            }
        }
    }

    // Exact historical statements from the pre-backend PostgreSQL implementation.
    private val seed = """
        INSERT INTO hog_maintenance_summary (catalog_id)
        SELECT c.catalog_id FROM hog_catalog c
        WHERE NOT EXISTS (SELECT 1 FROM hog_maintenance_summary s WHERE s.catalog_id = c.catalog_id)
        ORDER BY c.catalog_id LIMIT 100 ON CONFLICT DO NOTHING
    """
    private val due = """
        SELECT catalog_id, generation, published_generation, scan_state::text AS scan
        FROM hog_maintenance_summary WHERE next_batch_at <= now()
        ORDER BY next_batch_at, catalog_id LIMIT 1 FOR UPDATE SKIP LOCKED
    """

    private fun normalize(sql: String) = sql.trim().replace(Regex("\\s+"), " ")

    @Test
    fun `null sampler filter executes the historical seed and due statements without filter bindings`() {
        PgTestSupport.freshDatabase().use { db ->
            val issued = capture(db.jdbi)
            assertThat(MaintenanceSummarySampler(db.jdbi, 1024, 2, 10).runOnce(1)).isFalse()
            val statements =
                issued.filter {
                    it.raw.contains("INSERT INTO hog_maintenance_summary") || it.raw.contains("AS scan")
                }
            assertThat(statements.map { normalize(it.raw) }).containsExactly(normalize(seed), normalize(due))
            assertThat(statements.map { normalize(it.rendered) }).containsExactly(normalize(seed), normalize(due))
            statements.forEach { assertThat(it.parameters).doesNotContain("catalogIds", "allCatalogs") }
        }
    }

    @Test
    fun `filtered sampler binds arrays for one several and empty ids and isolates both seed and due work`() =
        filtered(
            true,
        )

    @Test
    fun `sampler filters independently isolate seed due empty and tick work`() = filtered(false)

    private fun filtered(checkShape: Boolean) {
        PgTestSupport.freshDatabase().use { db ->
            val ids =
                db.jdbi.withHandleUnchecked {
                        h ->
                    (1..3).map { CatalogRepo.insert(h, "filter$it", "s3://bucket/$it").catalogId }
                }
            val sampler = MaintenanceSummarySampler(db.jdbi, 1024, 2, 10)
            val issued = capture(db.jdbi)

            fun state() =
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery(
                        "SELECT catalog_id, generation, published_generation, scan_state::text, next_batch_at " +
                            "FROM hog_maintenance_summary ORDER BY catalog_id",
                    ).mapToMap().list()
                }
            for (filter in listOf(listOf(ids[1]), listOf(ids[1], ids[2]), emptyList())) {
                db.jdbi.useHandle<Exception> { it.execute("DELETE FROM hog_maintenance_summary") }
                issued.clear()
                assertThat(sampler.runOnce(1, filter)).isEqualTo(filter.isNotEmpty())
                val statements =
                    issued.filter {
                        it.raw.contains("INSERT INTO hog_maintenance_summary") || it.raw.contains("AS scan")
                    }
                assertThat(statements).hasSize(2)
                if (checkShape) {
                    for ((index, statement) in statements.withIndex()) {
                        val expected =
                            if (index == 0) {
                                seed.replace(
                                    "        ORDER BY",
                                    "        AND c.catalog_id = ANY(:catalogIds)\n        ORDER BY",
                                )
                            } else {
                                due.replace(
                                    "        ORDER BY",
                                    "        AND catalog_id = ANY(:catalogIds)\n        ORDER BY",
                                )
                            }
                        assertThat(normalize(statement.raw)).isEqualTo(normalize(expected))
                        assertThat(statement.parsed).contains("ANY(?)").doesNotContain("allCatalogs", ":catalogIds")
                        assertThat(statement.parameters).contains("catalogIds")
                        assertThat(statement.arrayTypes).containsExactly("2003:_int8")
                        val array = "{" + filter.joinToString(",") { "\"$it\"" } + "}"
                        assertThat(statement.executed).contains(array)
                    }
                }
                assertThat(
                    state().map { (it["catalog_id"] as Number).toLong() },
                ).containsExactlyElementsOf(filter.sorted())
            }
            db.jdbi.useHandle<Exception> { h ->
                h.execute(
                    "INSERT INTO hog_maintenance_summary(catalog_id, next_batch_at) " +
                        "SELECT catalog_id, TIMESTAMPTZ '2000-01-01' + catalog_id * INTERVAL '1 second' " +
                        "FROM hog_catalog",
                )
            }
            val before = state()
            assertThat(sampler.runOnce(1, emptyList())).isFalse()
            assertThat(state()).isEqualTo(before)
            assertThat(sampler.runOnce(1, listOf(ids[2]))).isTrue()
            assertThat(state().take(2)).isEqualTo(before.take(2))
            val afterSelected = state()
            sampler.tick(rowBudget = 1, catalogIds = listOf(ids[2]))
            assertThat(state().take(2)).isEqualTo(afterSelected.take(2))
            assertThat(sampler.runOnce(1)).isTrue()
            assertThat(state()[0]).isNotEqualTo(before[0])
            assertThat(state()[1]).isEqualTo(before[1])
            assertThatThrownBy { sampler.runOnce(0) }.isInstanceOf(IllegalArgumentException::class.java)
            assertThatThrownBy { sampler.tick(0) }.isInstanceOf(IllegalArgumentException::class.java)
        }
    }
}
