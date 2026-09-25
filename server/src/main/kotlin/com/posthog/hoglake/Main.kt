package com.posthog.hoglake

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.server.engine.connector
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty

private val log = KotlinLogging.logger {}

/**
 * Grace for in-flight requests when a SIGTERM arrives, in milliseconds,
 * and then the hard cap. The kubelet's default
 * `terminationGracePeriodSeconds` is 30 s, and these have to fit inside
 * it with room for the loops and the pools that close afterwards.
 */
private const val SHUTDOWN_GRACE_MS = 3_000L
private const val SHUTDOWN_TIMEOUT_MS = 10_000L

fun main() {
    val cfg = Config.fromEnv()
    val ds = Database.dataSource(cfg)
    Database.migrate(ds)
    val jdbi = Database.jdbi(ds)

    // `ds` is handed over only so the hoglake_db_pool_* saturation
    // gauges have an MXBean to read (#218); everything else goes
    // through Jdbi as before.
    val app = App.build(cfg, jdbi, dataSource = ds)
    val background = app.startBackground()
    val server =
        embeddedServer(
            Netty,
            // DEFENCE IN DEPTH, not the fix (#218). The fix is that
            // handlers no longer block this group at all
            // (api/BlockingDispatch.kt). Ktor sizes the CALL group at
            // availableProcessors, which on the one-CPU production pod
            // is a single thread — `eventLoopGroupProxy-4-1`, the
            // thread every request in the incident log ran on — so a
            // floor under it keeps a probe's dispatch, and a /metrics
            // scrape, from queueing behind another call. The WORKER
            // group keeps Ktor's own default on purpose; see
            // Config.nettyCallGroupSize.
            configure = {
                connector { port = cfg.port }
                callGroupSize = cfg.nettyCallGroupSize
            },
        ) { app.module(this) }

    // ORDER IS THE POINT, and what it buys is narrower than "nothing is
    // interrupted". Stopping the ENGINE first stops new calls arriving
    // and gives the ones already running the grace window to finish;
    // only then do the loops and the pools close. A commit still parked
    // on the catalog lock when the grace window expires IS interrupted
    // — `RequestDispatcher.close()` falls back to `shutdownNow()` after
    // its own bounded wait, and a daemon thread dies with the JVM
    // regardless — but it is interrupted LATE, after the pod stopped
    // accepting, rather than while Netty was still handing it work.
    // Closing the dispatcher first would interrupt commits the pod was
    // simultaneously accepting more of, which is the behaviour #218
    // complains about, and would leave `/healthz` answering from a
    // cancelled probe scope while the Service still routed to it.
    Runtime.getRuntime().addShutdownHook(
        Thread({
            runCatching { server.stop(SHUTDOWN_GRACE_MS, SHUTDOWN_TIMEOUT_MS) }
                .onFailure { log.warn(it) { "engine shutdown failed; closing the rest anyway" } }
            background.close()
            app.close()
        }, "hoglake-shutdown"),
    )
    log.info { "hoglake listening on :${cfg.port}" }
    server.start(wait = true)
}
