package com.posthog.hoglake

import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.Properties

/**
 * The version of the server that is actually running, read once from the
 * resource Gradle generates from `project.version` (see
 * `generateVersionResource` in build.gradle.kts).
 *
 * `project.version` is the single source: build.gradle.kts -> this
 * resource -> GET /v1/info -> the webui badge. Nothing restates it, so
 * nothing can drift from it — the same reasoning as the
 * `checkOpenapiVersion` gate on the spec's info.version.
 *
 * [UNKNOWN] is the answer when the resource is missing, which means the
 * server is running from a classpath that no hoglake build produced. It
 * is deliberately a displayable string rather than an exception: not
 * knowing the version must never stop the server from serving.
 */
object BuildInfo {
    const val UNKNOWN = "unknown"

    private val log = KotlinLogging.logger {}

    val version: String = readVersion()

    private fun readVersion(): String {
        val stream =
            BuildInfo::class.java.getResourceAsStream("/com/posthog/hoglake/version.properties")
                ?: run {
                    log.warn { "version.properties is not on the classpath; reporting version '$UNKNOWN'" }
                    return UNKNOWN
                }
        return stream.use { input ->
            val read = Properties().apply { load(input) }.getProperty("version")
            if (read.isNullOrBlank()) {
                log.warn { "version.properties carries no version; reporting version '$UNKNOWN'" }
                UNKNOWN
            } else {
                read
            }
        }
    }
}
