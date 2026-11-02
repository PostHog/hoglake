package com.posthog.hoglake.hoglite

import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.UUID

/**
 * Which hoglake process runs a background loop's all-catalog sweep.
 *
 * Without it, every hoglake process ran every enabled loop (hydrator,
 * expiry, cleanup, compaction, retirement) over every catalog on its own
 * timer: P processes, P full sweeps per interval, each one a round trip or
 * more per catalog, most of them proxied to the tenant's owner. A sweep is
 * work for the deployment, not for each process, so one process does it.
 *
 * The lease is one row per loop in the registry tenant's `loop_leases`
 * table: the holder and an expiry in epoch milliseconds. [acquire] is ONE
 * guarded transaction. It creates the table if missing, inserts the row if
 * missing, then takes the row if this process already holds it or the
 * holder's lease has expired. The take is guarded `expect_affected = 1`,
 * so two processes racing serialize on the registry tenant and exactly one
 * wins. The holder renews the lease between catalogs ([renewIfDue]), so a
 * sweep that runs longer than [ttlMs] is not taken over halfway. It
 * releases the lease on shutdown ([release]), so the next process does not
 * have to wait out the TTL.
 *
 * The lease saves work; it is NOT a correctness fence. Every catalog
 * operation a sweep makes already carries its own guards (per-tenant
 * serial transactions, the drop fence, compaction claims), which is what
 * made P concurrent sweeps safe before this existed. So the failure modes
 * are mild by design: wall-clock skew between processes larger than the
 * remaining lease, or a holder paused past its TTL, gives two sweepers for
 * a while, which is the old behaviour. A registry that cannot be reached
 * gives no sweeper for that tick, and the next tick tries again. A missing
 * registry means no catalog exists, so there is nothing to sweep.
 *
 * During a rolling deploy, processes on the old build take no lease and
 * keep sweeping everything until they are replaced.
 */
internal class HogliteLoopLease(
    private val store: HogliteCatalogStore,
    /** The loop's name, the lease row's key. */
    val loop: String,
    /** How long a lease lasts without renewal. */
    private val ttlMs: Long,
    /** This process's identity: the pod name plus a per-process nonce, so a restarted container is a new holder. */
    private val holder: String = defaultHolder(),
    private val clock: () -> Long = System::currentTimeMillis,
) {
    private val log = KotlinLogging.logger {}

    /** When this process last took or renewed the lease (its clock), or null when it does not hold it. */
    @Volatile
    private var heldSince: Long? = null

    /** Whether this process holds the lease as of its last attempt. */
    val held: Boolean get() = heldSince != null

    /**
     * Take or renew the lease. One registry transaction. True when this
     * process holds it until `now + ttlMs`.
     */
    fun acquire(): Boolean {
        val now = clock()
        val statements =
            listOf(
                HogliteStatement(CREATE_TABLE),
                HogliteStatement(
                    "INSERT OR IGNORE INTO loop_leases(loop, holder, expires_at) VALUES (?, '', 0)",
                    listOf(loop),
                ),
                HogliteStatement(
                    "UPDATE loop_leases SET holder = ?, expires_at = ? " +
                        "WHERE loop = ? AND (holder = ? OR expires_at < ?)",
                    listOf(holder, now + ttlMs, loop, holder, now),
                    expectAffected = 1,
                ),
            )
        val won =
            try {
                store.client.tx(store.registryTenantId, HogliteCatalogStore.freshTxid("loop-lease"), null, statements)
                true
            } catch (_: HogliteGuardFailure) {
                false
            } catch (r: HogliteRejected) {
                // 404: no registry, so no catalog to sweep. Anything else
                // is this tick's problem; the next tick asks again.
                if (r.status != 404) log.warn { "loop lease '$loop': hoglite refused the lease: ${r.message}" }
                false
            } catch (e: com.posthog.hoglake.model.HoglakeException.BackendUnavailable) {
                log.warn { "loop lease '$loop': hoglite is unavailable: ${e.message}" }
                false
            }
        val wasHeld = held
        heldSince = if (won) now else null
        if (won != wasHeld) {
            log.info { "loop lease '$loop': ${if (won) "this process sweeps ($holder)" else "another process sweeps"}" }
        }
        return won
    }

    /**
     * Called between catalogs of a sweep: renew once a third of the TTL
     * has passed since the last take or renewal, and report whether the
     * sweep should go on. A lease lost to another process (it expired
     * under a stalled holder) stops this sweep at the next catalog.
     */
    fun renewIfDue(): Boolean {
        val since = heldSince ?: return false
        if (clock() - since < ttlMs / 3) return true
        return acquire()
    }

    /** Give the lease up (shutdown), so another process need not wait out the TTL. Best effort. */
    fun release() {
        if (!held) return
        heldSince = null
        runCatching {
            store.client.tx(
                store.registryTenantId,
                HogliteCatalogStore.freshTxid("loop-lease-release"),
                null,
                listOf(
                    HogliteStatement(
                        "UPDATE loop_leases SET expires_at = 0 WHERE loop = ? AND holder = ?",
                        listOf(loop, holder),
                    ),
                ),
            )
        }.onFailure {
            log.warn { "loop lease '$loop': release failed, it expires in at most ${ttlMs}ms: ${it.message}" }
        }
    }

    companion object {
        const val CREATE_TABLE =
            "CREATE TABLE IF NOT EXISTS loop_leases(loop TEXT PRIMARY KEY, holder TEXT NOT NULL, " +
                "expires_at INTEGER NOT NULL)"

        /** The shortest lease, whatever the interval: a renewal must never be due before it can land. */
        const val MIN_TTL_MS = 60_000L

        /**
         * The lease for a loop that ticks every [intervalMs]: two intervals.
         * The holder renews at every tick and between catalogs, so a live
         * holder never lets it lapse. A holder that dies is replaced
         * within about three intervals: the TTL plus the next tick of
         * another process.
         */
        fun ttlFor(intervalMs: Long): Long = maxOf(MIN_TTL_MS, 2 * intervalMs)

        fun defaultHolder(): String =
            com.posthog.hoglake.compaction.CompactionService.defaultPlannerIdentity() + "/" +
                UUID.randomUUID().toString().take(8)
    }
}
