package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.HoglakeException

/**
 * Builds one hoglite transaction for one hoglake catalog operation,
 * pairing each statement with the typed refusal it means when its
 * declarative guard fails.
 *
 * hoglite transactions carry the OCC contract IN the request: a
 * statement's `expect_affected` / `expect_scalar_int` is evaluated
 * inside the tenant's serial transaction, and a miss rolls the whole
 * transaction back and answers 409 with the failing statement's index.
 * This builder records, per guarded statement, which
 * [HoglakeException] that index decodes to — e.g. "table guard 0 rows"
 * is `unknown table`, while a read_snapshot guard miss is
 * `read_snapshot N is ahead of head M` — so the store can map hoglite's
 * flat 409 back onto hoglake's typed error model.
 *
 * Statement ORDER is therefore part of the contract, and the KDoc on
 * each builder call site says which index is which — the same style
 * hoglake's CommitTail CTE ladder uses for its guards.
 */
class HogliteTx {
    private val statements = mutableListOf<HogliteStatement>()

    /** The exception a guard on statement i decodes to, when i has one. */
    private val failures = mutableMapOf<Int, () -> HoglakeException>()

    val size: Int get() = statements.size

    /** Append a statement with no guard. Returns its index. */
    fun add(
        sql: String,
        vararg args: Any?,
    ): Int {
        statements += HogliteStatement(sql, args.toList())
        return statements.size - 1
    }

    /** A `SELECT COUNT(*) ...` that must return 0, else [failure]. For existence/emptiness preconditions. */
    fun expectZero(
        sql: String,
        vararg args: Any?,
        failure: () -> HoglakeException,
    ): Int = guarded(HogliteStatement(sql, args.toList(), expectScalarInt = 0), failure)

    /** A `SELECT COUNT(*) ...` that must return exactly 1, else [failure]. For "must exist" probes. */
    fun expectOne(
        sql: String,
        vararg args: Any?,
        failure: () -> HoglakeException,
    ): Int = guarded(HogliteStatement(sql, args.toList(), expectScalarInt = 1), failure)

    /** The write must affect exactly one row, else [failure]. For updates that must land. */
    fun affectOne(
        sql: String,
        vararg args: Any?,
        failure: () -> HoglakeException,
    ): Int = guarded(HogliteStatement(sql, args.toList(), expectAffected = 1), failure)

    /** The query's scalar must equal [expected], else [failure]. For counter/head guards. */
    fun expectScalar(
        expected: Long,
        sql: String,
        vararg args: Any?,
        failure: () -> HoglakeException,
    ): Int = guarded(HogliteStatement(sql, args.toList(), expectScalarInt = expected), failure)

    private fun guarded(
        statement: HogliteStatement,
        failure: () -> HoglakeException,
    ): Int {
        statements += statement
        failures[statements.size - 1] = failure
        return statements.size - 1
    }

    fun statements(): List<HogliteStatement> = statements

    /**
     * Decode a 409 from hoglite: the exception the failed statement's
     * guard was registered with, or a generic conflict when the index
     * names an unguarded statement (a bug — but a miss here must still
     * be a typed conflict, not a crash).
     */
    fun classify(e: HogliteGuardFailure): HoglakeException {
        val registered = failures[e.statementIndex]?.invoke()
        if (registered != null) return registered
        // Written as an if+return, not an Elvis into the constructor:
        // CommitConflictSpecParityTest scans for declaration-shaped text
        // and an Elvis's colon would read as one.
        return HoglakeException.CommitConflict(
            "hoglite rejected statement ${e.statementIndex} without a registered guard " +
                "(a store bug; the transaction rolled back cleanly): ${e.message}",
        )
    }
}
