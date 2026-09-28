package com.posthog.hoglake.testing

import com.posthog.hoglake.Database
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.flywaydb.core.api.MigrationVersion
import org.jdbi.v3.core.Jdbi
import org.testcontainers.containers.PostgreSQLContainer

/**
 * Shared Postgres fixture for integration tests. One container per JVM
 * (Testcontainers reuses it across classes); each test class gets a
 * fresh database via [freshDatabase] so tests never share state.
 *
 * Usage:
 *   @Tag("integration")
 *   class MyIntegrationTest {
 *       private val db = PgTestSupport.freshDatabase()
 *       // db.jdbi ...
 *   }
 */
object PgTestSupport {
    /**
     * The Postgres the suite runs against. Pinned to what production
     * runs, and overridable so a major-version move can be rehearsed
     * against the WHOLE suite before it happens:
     *
     *   ./gradlew :test -PpgImage=postgres:19
     *   HOGLAKE_TEST_PG_IMAGE=postgres:16 ./gradlew :test
     *
     * The override also runs the suite BACKWARDS against the version
     * being left behind, which is how a failure gets attributed to the
     * version rather than to the environment. Worth having because the
     * failures a version move produces are rarely in application code:
     * they are statistics views that moved columns, catalog shapes, and
     * planner changes — none of which a unit test can see.
     *
     * VISIBLE, not private, for the one class that must run its OWN
     * container because its subject is stopping it
     * (`HealthProbeIntegrationTest`): it has to start the same image the
     * rest of the suite pins, or a version override would silently skip
     * it.
     */
    val image: String =
        System.getProperty("pgImage")
            ?: System.getenv("HOGLAKE_TEST_PG_IMAGE")
            ?: "postgres:18"

    /**
     * The role every test database is created with — the same spelling
     * `Config`'s own defaults carry, so a fixture that points a
     * `Config` at [TestDb.jdbcUrl] needs no credential overrides.
     *
     * Named here rather than copied into each test: the health probe
     * (#218) builds its connection from `Config`, so a fixture that
     * restated these would silently stop matching the container the day
     * either moved.
     */
    const val USER: String = "hoglake"
    const val PASSWORD: String = "hoglake"

    private val container: PostgreSQLContainer<*> by lazy {
        PostgreSQLContainer(image)
            .withUsername(USER)
            .withPassword(PASSWORD)
            .also { it.start() }
    }

    private var dbCounter = 0

    class TestDb(val dataSource: HikariDataSource, val jdbi: Jdbi, val jdbcUrl: String) :
        AutoCloseable {
        override fun close() = dataSource.close()
    }

    /**
     * Create a new empty database and apply [sql] verbatim (no
     * migrations). Used by the schema-equivalence check to materialize
     * the canonical schema.sql.
     */
    @Synchronized
    fun freshDatabaseRaw(sql: String): TestDb {
        val db = freshEmpty()
        db.jdbi.useHandle<Exception> { h -> h.createScript(sql).execute() }
        return db
    }

    /**
     * Create a new empty database, run migrations, return a pooled Jdbi.
     *
     * [productionSession] wires the pool with [Database.SESSION_INIT_SQL]
     * — the `statement_timeout` / `idle_in_transaction_session_timeout`
     * a real pod's sessions carry. OPT-IN, and default OFF on purpose:
     * turning it on for the whole suite would put a 60 s statement bound
     * and a 30 s idle-in-transaction bound under every fixture in the
     * tree, including the six-figure bulk seeds and the drain tests that
     * hold a transaction open across MinIO round trips, and a suite that
     * starts failing on a slow machine teaches nothing about the code.
     * A test that needs session behaviour to be REACHABLE (a migration
     * whose build can block, say) asks for it.
     */
    @Synchronized
    fun freshDatabase(productionSession: Boolean = false): TestDb =
        freshEmpty(productionSession).also { Database.migrate(it.dataSource) }

    /**
     * A database migrated only as far as [version], so a migration test
     * can populate the state its migration will actually run against.
     * Driving [Database.flywayConfig] rather than a local Flyway builder
     * is deliberate (see that function): a copy asserts only that it
     * compiles. The production advisory lock is skipped — nothing else is
     * migrating this database — so `Database.migrate` is what the test
     * then calls to apply the rest.
     */
    @Synchronized
    fun freshDatabaseAt(
        version: String,
        productionSession: Boolean = false,
    ): TestDb =
        freshEmpty(productionSession).also {
            Database.flywayConfig(it.dataSource)
                .target(MigrationVersion.fromVersion(version))
                .load()
                .migrate()
        }

    @Synchronized
    private fun freshEmpty(productionSession: Boolean = false): TestDb {
        val name = "hoglake_test_${dbCounter++}"
        container.createConnection("").use { conn ->
            conn.createStatement().use { it.execute("CREATE DATABASE $name") }
        }
        val url = container.jdbcUrl.replace("/${container.databaseName}", "/$name")
        val ds =
            HikariDataSource(
                HikariConfig().apply {
                    jdbcUrl = url
                    username = container.username
                    password = container.password
                    maximumPoolSize = 8
                    poolName = name
                    // Database's own constant, never a copy of its text.
                    if (productionSession) connectionInitSql = Database.SESSION_INIT_SQL
                },
            )
        return TestDb(ds, Database.jdbi(ds), url)
    }
}
