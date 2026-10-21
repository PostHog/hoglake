package com.posthog.hoglake.persistence

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.jdbi.v3.core.argument.Argument
import org.jdbi.v3.core.statement.SqlStatement
import org.jdbi.v3.core.statement.StatementContext
import org.jdbi.v3.core.statement.StatementCustomizer
import org.jdbi.v3.core.statement.UnableToExecuteStatementException
import org.postgresql.PGStatement
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Types

/**
 * Small Postgres-facing helpers shared by the repositories: SQLSTATE
 * classification for constraint violations and jsonb (de)serialization
 * for `hog_column.type_params`.
 */
internal object Pg {
    const val UNIQUE_VIOLATION = "23505"
    const val CHECK_VIOLATION = "23514"
    const val LOCK_NOT_AVAILABLE = "55P03"

    /**
     * `query_canceled` — what BOTH `statement_timeout` expiry and an
     * explicit `pg_cancel_backend` raise. A caller that treats this
     * as "my own bound fired" must be one that SET that bound
     * itself, transaction-locally, for the statement it is running
     * (RetirementService does).
     */
    const val QUERY_CANCELED = "57014"

    private val json: ObjectMapper = jacksonObjectMapper()
    private val mapType = object : TypeReference<Map<String, Any?>>() {}

    /** SQLSTATE of the underlying SQLException, or null if not one. */
    fun sqlState(e: UnableToExecuteStatementException): String? = (e.cause as? SQLException)?.sqlState

    fun isUniqueViolation(e: UnableToExecuteStatementException): Boolean = sqlState(e) == UNIQUE_VIOLATION

    fun isCheckViolation(e: UnableToExecuteStatementException): Boolean = sqlState(e) == CHECK_VIOLATION

    const val FOREIGN_KEY_VIOLATION = "23503"

    /**
     * A foreign-key violation against `hog_catalog` itself: the INSERT of a
     * catalog-scoped row whose catalog was deleted after the writer
     * resolved it — a `DELETE /v1/catalogs/{c}` that won the race (#412).
     * Matched on the server's detail (`... is not present in table
     * "hog_catalog"`), so a violation against any OTHER parent stays the
     * bug it is and is not turned into a 404.
     */
    fun isCatalogForeignKeyViolation(e: Throwable): Boolean {
        val sql = generateSequence(e) { it.cause }.filterIsInstance<org.postgresql.util.PSQLException>().firstOrNull()
        return sql?.sqlState == FOREIGN_KEY_VIOLATION &&
            sql.serverErrorMessage?.detail?.contains("table \"hog_catalog\"") == true
    }

    /** lock_timeout expiry ("canceling statement due to lock timeout"). */
    fun isLockTimeout(e: UnableToExecuteStatementException): Boolean = sqlState(e) == LOCK_NOT_AVAILABLE

    /** statement_timeout expiry (or a cancel): "canceling statement due to ...". */
    fun isQueryCanceled(e: UnableToExecuteStatementException): Boolean = sqlState(e) == QUERY_CANCELED

    /** Serialize column type params for a jsonb column; null stays null. */
    fun toJson(params: Map<String, Any?>?): String? = params?.let { json.writeValueAsString(it) }

    /** Parse a jsonb column back into type params; null stays null. */
    fun fromJson(s: String?): Map<String, Any?>? = s?.let { json.readValue(it, mapType) }

    /**
     * pgjdbc `prepareThreshold = 0` for one statement: it is sent unnamed
     * on every execution, and Postgres plans an unnamed statement custom
     * each time, so a generic plan is never a candidate.
     *
     * For statements whose GENERIC plan is a different, worse shape than
     * their custom one — the generic plan cannot see the bound values'
     * statistics, so it cannot tell a selective value from a common one.
     * Consumers: `TableRepo.listLiveSummaries` and the compaction
     * planner's bucket probe (`CompactionService.candidateSql`), each of
     * which records the generic plan it is avoiding.
     *
     * SCOPE. `setPrepareThreshold(0)` is set on the one `PgStatement`
     * object: the statement goes out as the unnamed statement and is
     * planned custom at Bind. Nothing is set on the connection, and it is
     * not `plan_cache_mode`. pgjdbc's per-connection `CachedQuery`
     * execute count for the SQL text still increments on customized
     * runs, so an UNCUSTOMIZED execution of the same text on that
     * connection is server-prepared on its first run. Harmless — every
     * production call site of these statements is customized — and it is
     * what makes the polarity control in
     * `CompactionBucketProbePlanIntegrationTest` work.
     */
    val CUSTOM_PLANS_ONLY: StatementCustomizer =
        object : StatementCustomizer {
            override fun beforeExecution(
                stmt: PreparedStatement,
                ctx: StatementContext,
            ) {
                stmt.unwrap(PGStatement::class.java).prepareThreshold = 0
            }
        }
}

/**
 * Bind a nullable `bigint[]` (hog_data_file.split_offsets): the list as a
 * Postgres array, or SQL NULL.
 *
 * Called by `Hydrator.hydrate` (the offsets read off the footer) and
 * `CompactionService.commitGroup` (the output file's), each binding one
 * row per statement.
 *
 * ONE Argument type for both arms, and that is the point: a
 * PreparedBatch prepares its binder from the FIRST row's argument, so
 * `bindArray` on one row and `bindNull` on the next fails the whole batch
 * with "argument must be ... an array; was NullArgument", and rows with
 * and without offsets side by side are the ordinary case, not an edge.
 */
internal fun <T : SqlStatement<T>> T.bindBigintArrayOrNull(
    name: String,
    values: List<Long>?,
): T =
    bind(
        name,
        Argument { position, statement, _ ->
            if (values == null) {
                statement.setNull(position, Types.ARRAY)
            } else {
                statement.setArray(position, statement.connection.createArrayOf("bigint", values.toTypedArray()))
            }
        },
    )

/** Read a nullable `bigint[]` column back as a list; SQL NULL stays null. */
internal fun ResultSet.getBigintListOrNull(column: String): List<Long>? =
    getArray(column)?.let { arr -> (arr.array as Array<*>).map { (it as Number).toLong() } }
