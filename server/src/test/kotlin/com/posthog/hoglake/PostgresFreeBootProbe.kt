package com.posthog.hoglake

/** A real process with no PostgreSQL environment settings. */
object PostgresFreeBootProbe {
    @JvmStatic
    fun main(args: Array<String>) {
        val cfg = Config.fromEnv()
        check(cfg.jdbcUrl.isEmpty() && cfg.dbUser.isEmpty() && cfg.dbPassword.isEmpty())
        App.build(cfg).use { app ->
            app.startBackground().use {
                check(!app.meterRegistry.scrape().contains("hoglake_db_pool"))
                println("BOOTED_WITHOUT_POSTGRES")
            }
        }
    }
}
