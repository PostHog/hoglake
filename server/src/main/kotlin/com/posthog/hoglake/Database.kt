package com.posthog.hoglake

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.github.oshai.kotlinlogging.KotlinLogging
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
    private val log = KotlinLogging.logger {}

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

    /**
     * The statement half of [SESSION_INIT_SQL], as a Duration, for the
     * code that has to derive a bound from it.
     *
     * `RetirementService` is the caller: a retirement batch runs
     * DELETEs under the per-catalog commit lock and sets a `SET LOCAL
     * statement_timeout` well under this, so a batch that turns out to
     * be too big for the table's cascade fan-out is CANCELLED and
     * halved rather than holding the lock for the whole session bound.
     * Derived from this rather than written down, so changing the
     * session bound moves it.
     *
     * [SESSION_INIT_SQL] stays the single source of truth — a test
     * parses the value out of it rather than trusting this to agree.
     */
    val SESSION_INIT_SQL_STATEMENT_TIMEOUT: java.time.Duration = java.time.Duration.ofSeconds(60)

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

    /**
     * How long a replica waits for another replica's migration before
     * giving up.
     *
     * UNDER THE DEPLOY'S OWN BUDGET, DELIBERATELY. `Main.kt` runs
     * [migrate] before Netty binds :8080, and the chart's TCP startup
     * probe allows 30 x 5 s = 150 s before the kubelet SIGKILLs the
     * container. A bound above that can never fire: the pod dies first,
     * with no message about what it was waiting for. 120 s leaves the
     * probe 30 s of headroom and turns the wait into a NAMED failure —
     * the exception below says which lock and for how long — which is
     * the whole reason the bound exists. A replica that gives up
     * crash-loops and tries again, so nothing is lost by failing early
     * and a wedged fleet says why.
     *
     * It also sizes the migrations this is compatible with: V17's
     * concurrent build is 26-35 s at production size, depending on
     * `maintenance_work_mem`, so a first replica finishes well inside
     * it. A migration that cannot — or one whose build can PARK behind a
     * foreign snapshot, which is any concurrent build — is one to
     * pre-apply out of band; V17's header says how.
     */
    val MIGRATION_LOCK_WAIT: java.time.Duration = java.time.Duration.ofSeconds(120)

    /** How often [awaitMigrationLock] retries. */
    val MIGRATION_LOCK_POLL: java.time.Duration = java.time.Duration.ofMillis(250)

    /** How often the wait says it is still waiting. */
    val MIGRATION_LOCK_LOG_EVERY: java.time.Duration = java.time.Duration.ofSeconds(5)

    fun migrate(ds: DataSource) {
        ds.connection.use { conn ->
            awaitMigrationLock(conn)
            try {
                flywayConfig(ds).load().migrate()
            } finally {
                conn.createStatement().use { st ->
                    st.execute("SELECT pg_advisory_unlock($MIGRATION_LOCK_KEY)")
                }
            }
        }
    }

    /**
     * Take the migration lock by POLLING `pg_try_advisory_lock`, never
     * by waiting inside `pg_advisory_lock`.
     *
     * The difference is a SNAPSHOT. A blocking `SELECT
     * pg_advisory_lock(...)` is an open statement, and an open statement
     * publishes an xmin for as long as it waits — so a replica queued
     * behind another replica's migration is a transaction older than
     * anything that migration starts. `CREATE INDEX CONCURRENTLY` waits
     * out exactly those transactions (WaitForOlderSnapshots), which
     * makes the build park behind a session that is itself waiting on
     * the migration the build belongs to: a chain, not a cycle, so the
     * deadlock detector never fires, and it unwinds only when the
     * waiting replica's 60 s `statement_timeout` ([SESSION_INIT_SQL])
     * kills it — a failed boot, a restarted pod, and a fresh snapshot
     * for the build's next phase to park behind.
     *
     * Measured on a 5M-row `hog_data_file` (V17's fixture), with writers
     * committing throughout, each duration on its own session's clock:
     * CIC alone 26 s; CIC with a second replica blocking on the old
     * code 58 s, of which 32 s was spent in `wait_event = virtualxid`
     * against that replica's virtual transaction (read out of
     * pg_stat_activity), while the replica's own boot failed when its
     * blocked `pg_advisory_lock` hit the 60 s `statement_timeout` its
     * session carries; CIC with a second replica polling 26 s, and the
     * replica takes the lock as soon as the first is done. What ended
     * the park is not claimed — the numbers are the three durations and
     * the wait event. A poll holds its snapshot for the microseconds one
     * `pg_try_advisory_lock` takes.
     *
     * The sleep is CLIENT-side for the same reason: `pg_sleep` is a
     * statement, and a statement has a snapshot.
     */
    private fun awaitMigrationLock(conn: java.sql.Connection) {
        val start = System.nanoTime()
        val deadline = start + MIGRATION_LOCK_WAIT.toNanos()
        var nextLog = start + MIGRATION_LOCK_LOG_EVERY.toNanos()
        while (true) {
            val acquired =
                conn.createStatement().use { st ->
                    st.executeQuery("SELECT pg_try_advisory_lock($MIGRATION_LOCK_KEY)").use { rs ->
                        rs.next() && rs.getBoolean(1)
                    }
                }
            val waited = java.time.Duration.ofNanos(System.nanoTime() - start)
            if (acquired) {
                if (waited >= MIGRATION_LOCK_LOG_EVERY) {
                    log.info { "migration lock $MIGRATION_LOCK_KEY acquired after $waited" }
                }
                return
            }
            check(System.nanoTime() < deadline) {
                "timed out after $MIGRATION_LOCK_WAIT waiting for the migration advisory lock " +
                    "($MIGRATION_LOCK_KEY); another replica has been migrating for longer than that"
            }
            // A silent wait is the failure mode this replaces: the pod
            // sits pre-bind, the startup probe counts down, and the
            // kubelet's SIGKILL is the only evidence anyone gets. Say it
            // every few seconds, with the elapsed time, so the log
            // distinguishes "waiting for the other replica's migration"
            // from "hung on the database".
            if (System.nanoTime() >= nextLog) {
                log.info {
                    "waiting for the migration advisory lock ($MIGRATION_LOCK_KEY): $waited " +
                        "elapsed of $MIGRATION_LOCK_WAIT — another replica is migrating"
                }
                nextLog = System.nanoTime() + MIGRATION_LOCK_LOG_EVERY.toNanos()
            }
            Thread.sleep(MIGRATION_LOCK_POLL.toMillis())
        }
    }

    fun jdbi(ds: DataSource): Jdbi =
        Jdbi.create(ds)
            .installPlugin(KotlinPlugin())
            .installPlugin(PostgresPlugin())
}
