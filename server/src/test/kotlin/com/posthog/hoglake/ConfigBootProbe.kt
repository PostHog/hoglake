package com.posthog.hoglake

/**
 * A child-JVM entry point that does nothing but construct [Config].
 *
 * It exists because `Config.init` reads the REAL process environment
 * (`checkRemovedEnv(System::getenv)`) and no test can set an environment
 * variable in its own JVM on a modern JDK. `Config` is a `data class`, so
 * the lookup cannot be a constructor parameter either — every primary
 * constructor parameter has to be a `val`, and a function-typed `val`
 * would land in `equals`/`hashCode`/`copy`/`componentN` and make two
 * identically-configured Configs unequal.
 *
 * So the wiring is tested the only way that tests the shipped path: run
 * it in a process that really has the variable set.
 * `RemovedEnvConfigTest` launches this class and reads the exit code.
 *
 * Exit codes are the contract:
 *  - `0` — `Config()` constructed, so nothing refused it. Each
 *    argument is a property name, printed as `name=value`.
 *  - `2` — construction threw `IllegalArgumentException`, i.e. a
 *    `require` in `init` fired. The message goes to stdout for the test
 *    to assert on.
 *  - `3` — construction threw something else, which is a different bug
 *    and must not be read as a refusal.
 *
 * TEST SOURCES ONLY: it is never on the production classpath.
 */
object ConfigBootProbe {
    @JvmStatic
    fun main(args: Array<String>) {
        try {
            val config = Config()
            println("CONSTRUCTED")
            // Each argument names a Config property to report as
            // `name=value`, so a test can see what the real environment
            // parsed to, not only whether it parsed.
            for (name in args) {
                val getter = Config::class.java.getMethod("get" + name.replaceFirstChar { it.uppercase() })
                println("$name=${getter.invoke(config)}")
            }
        } catch (e: IllegalArgumentException) {
            println("REFUSED: ${e.message}")
            // Not `exitProcess(2)`: the point is an exit code the parent
            // can distinguish, and a refusal is the expected outcome
            // rather than a crash, so nothing is printed to stderr.
            System.exit(2)
        } catch (e: Throwable) {
            println("THREW ${e::class.java.name}: ${e.message}")
            System.exit(3)
        }
    }
}
