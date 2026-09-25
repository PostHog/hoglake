package com.posthog.hoglake.persistence

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.jdbi.v3.core.argument.Argument
import org.jdbi.v3.core.statement.SqlStatement
import org.jdbi.v3.core.statement.UnableToExecuteStatementException
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

    /** lock_timeout expiry ("canceling statement due to lock timeout"). */
    fun isLockTimeout(e: UnableToExecuteStatementException): Boolean = sqlState(e) == LOCK_NOT_AVAILABLE

    /** statement_timeout expiry (or a cancel): "canceling statement due to ...". */
    fun isQueryCanceled(e: UnableToExecuteStatementException): Boolean = sqlState(e) == QUERY_CANCELED

    /** Serialize column type params for a jsonb column; null stays null. */
    fun toJson(params: Map<String, Any?>?): String? = params?.let { json.writeValueAsString(it) }

    /** Parse a jsonb column back into type params; null stays null. */
    fun fromJson(s: String?): Map<String, Any?>? = s?.let { json.readValue(it, mapType) }
}

/**
 * Bind a nullable `bigint[]` (hog_data_file.split_offsets): the list as a
 * Postgres array, or SQL NULL.
 *
 * ONE Argument type for both arms, and that is the point. A
 * PreparedBatch (CommitService.writeAppends inserts every file of a
 * commit in one) prepares its binder from the FIRST row's argument, so
 * `bindArray` on one row and `bindNull` on the next fails the whole
 * commit with "argument must be ... an array; was NullArgument" — a
 * commit mixing files with and without offsets is the ordinary case, not
 * an edge.
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
