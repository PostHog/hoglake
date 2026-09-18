package com.posthog.hoglake.testing

import com.posthog.hoglake.Database
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
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
     *   ./gradlew :test -PpgImage=postgres:18
     *   HOGLAKE_TEST_PG_IMAGE=postgres:18 ./gradlew :test
     *
     * The default is unchanged, so this costs nothing until someone asks
     * for it. Worth having because the failures a version move produces
     * are rarely in application code: they are statistics views that
     * moved columns, catalog shapes, and planner changes — none of which
     * a unit test can see.
     */
    private val image: String =
        System.getProperty("pgImage")
            ?: System.getenv("HOGLAKE_TEST_PG_IMAGE")
            ?: "postgres:16"

    private val container: PostgreSQLContainer<*> by lazy {
        PostgreSQLContainer(image)
            .withUsername("hoglake")
            .withPassword("hoglake")
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

    /** Create a new empty database, run migrations, return a pooled Jdbi. */
    @Synchronized
    fun freshDatabase(): TestDb = freshEmpty().also { Database.migrate(it.dataSource) }

    @Synchronized
    private fun freshEmpty(): TestDb {
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
                },
            )
        return TestDb(ds, Database.jdbi(ds), url)
    }
}
