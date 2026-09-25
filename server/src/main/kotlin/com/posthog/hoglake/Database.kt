package com.posthog.hoglake

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.configuration.FluentConfiguration
import org.flywaydb.database.postgresql.PostgreSQLConfigurationExtension
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.KotlinPlugin
import org.jdbi.v3.postgres.PostgresPlugin
import javax.sql.DataSource

/**
 * The service owns ALL catalog connections (README.md §7): one pool,
 * statement timeouts, no third-party sessions. Migrations run at startup
 * under an advisory lock so concurrent replicas don't race DDL.
 */
object Database {
    const val MIGRATION_LOCK_KEY: Long = 0x486F674C616B6531 // "HogLake1"

    /**
     * What every production session is configured with, as ONE
     * definition. `PgTestSupport` applies THIS constant rather than a
     * copy of it, so a migration test can reach the session behaviour a
     * real pod has — a build that blocks past `statement_timeout` is
     * killed, an open transaction that idles past
     * `idle_in_transaction_session_timeout` is killed — instead of
     * hanging in a test and passing in production, or the reverse. A
     * helper that restates these values asserts that it compiles.
     */
    const val SESSION_INIT_SQL: String =
        "SET idle_in_transaction_session_timeout = '30s'; SET statement_timeout = '60s'"

    /**
     * The idle-in-transaction half of [SESSION_INIT_SQL], as a Duration,
     * for the code that has to stay under it.
     *
     * `RemovalStore` is the caller: the cleanup drain makes object-store
     * calls inside a transaction that holds the per-catalog commit lock,
     * and a connection awaiting an S3 response IS idle in transaction.
     * Its SDK timeouts are derived from this rather than written down,
     * so changing the session bound moves them with it.
     *
     * [SESSION_INIT_SQL] stays the single source of truth — a test
     * parses the value out of it rather than trusting this to agree.
     */
    val SESSION_INIT_SQL_IDLE_TIMEOUT: java.time.Duration = java.time.Duration.ofSeconds(30)

    fun dataSource(cfg: Config): HikariDataSource {
        val hc =
            HikariConfig().apply {
                jdbcUrl = cfg.jdbcUrl
                username = cfg.dbUser
                password = cfg.dbPassword
                maximumPoolSize = cfg.dbPoolSize
                poolName = "hoglake"
                // Fail fast when the catalog is unreachable — a request must
                // 500/503 quickly, never hang on connection acquisition.
                connectionTimeout = 5_000
                validationTimeout = 2_500
                // No idle-in-transaction squatters, ever (README.md §7).
                connectionInitSql = SESSION_INIT_SQL
            }
        return HikariDataSource(hc)
    }

    /**
     * The migration configuration, as ONE definition. The migration
     * integration tests drive this function rather than restating it: a
     * helper that merely copies these lines asserts that it compiles, not
     * that it matches production, and the copies silently diverged the
     * moment this setting had to change.
     */
    fun flywayConfig(ds: DataSource): FluentConfiguration =
        Flyway.configure()
            .dataSource(ds)
            .locations("classpath:db/migration")
            // Concurrent index builds wait for old transactions; Flyway
            // must not hold its own transaction-level lock open while
            // executing a nontransactional migration.
            //
            // The TYPED extension, not the `.configuration(mapOf(...))`
            // string-map form: as of Flyway 13 that form throws on this
            // namespace unless jackson-databind is reachable to it, and a
            // misspelled key in that map was never an error — it was
            // dropped in silence, leaving the lock held.
            .also {
                it.getConfigurationExtension(PostgreSQLConfigurationExtension::class.java)
                    .isTransactionalLock = false
            }

    fun migrate(ds: DataSource) {
        ds.connection.use { conn ->
            conn.createStatement().use { st ->
                st.execute("SELECT pg_advisory_lock($MIGRATION_LOCK_KEY)")
            }
            try {
                flywayConfig(ds).load().migrate()
            } finally {
                conn.createStatement().use { st ->
                    st.execute("SELECT pg_advisory_unlock($MIGRATION_LOCK_KEY)")
                }
            }
        }
    }

    fun jdbi(ds: DataSource): Jdbi =
        Jdbi.create(ds)
            .installPlugin(KotlinPlugin())
            .installPlugin(PostgresPlugin())
}
