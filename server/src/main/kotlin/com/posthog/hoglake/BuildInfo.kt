package com.posthog.hoglake

import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.Properties

/**
 * What the running server can say about itself: the semver it was built
 * as, and — when it was packaged by a pipeline rather than built locally
 * — the stamp identifying which build that was. Both are read once from
 * the resource Gradle generates (`generateVersionResource` in
 * build.gradle.kts).
 *
 * `project.version` is the single source for [version]: build.gradle.kts
 * -> this resource -> GET /v1/info -> the webui badge. Nothing restates
 * it, so nothing can drift from it — the same reasoning as the
 * `checkOpenapiVersion` gate on the spec's info.version.
 *
 * The two fields answer different questions and fail differently:
 *
 *  - [version] is the CONTRACT version and is always present. [UNKNOWN]
 *    when the resource is missing means the server is running from a
 *    classpath no hoglake build produced — a displayable string rather
 *    than an exception, because not knowing the version must never stop
 *    the server from serving.
 *  - [buildStamp] is null on any build nobody stamped, which is every
 *    local build and every PR image by design. Absent means "not off the
 *    pipeline", NOT "something went wrong", so it must never degrade to
 *    an `unknown` sentinel that would read as a broken deploy.
 */
object BuildInfo {
    const val UNKNOWN = "unknown"

    private val log = KotlinLogging.logger {}

    /**
     * A stamp is builder-supplied, travels a Docker build arg, and ends
     * up in every /v1/info response and the webui topbar. Bound its shape
     * here rather than trust the pipeline: a malformed build arg should
     * cost the badge, not put an unbounded blob on a hot endpoint. Same
     * position as `Identifiers`' decision note — everything unbounded
     * eventually gets a 10KB blob.
     */
    private val STAMP_SHAPE = Regex("^[A-Za-z0-9][A-Za-z0-9._+-]{0,63}$")

    private val properties: Properties? = readProperties()

    val version: String = readVersion()

    /** The packaging stamp (e.g. `20260915T2104Z`), or null when unstamped. */
    val buildStamp: String? = readBuildStamp()

    private fun readProperties(): Properties? {
        val stream =
            BuildInfo::class.java.getResourceAsStream("/com/posthog/hoglake/version.properties")
                ?: run {
                    log.warn { "version.properties is not on the classpath; reporting version '$UNKNOWN'" }
                    return null
                }
        return stream.use { input -> Properties().apply { load(input) } }
    }

    private fun readVersion(): String {
        val read = properties?.getProperty("version")
        return if (read.isNullOrBlank()) {
            if (properties != null) log.warn { "version.properties carries no version; reporting '$UNKNOWN'" }
            UNKNOWN
        } else {
            read
        }
    }

    private fun readBuildStamp(): String? = sanitizeStamp(properties?.getProperty("build"))

    /**
     * The stamp-acceptance rule, split out because it is the only part of
     * this object with a decision in it: blank (the ordinary unstamped
     * build) and malformed (a pipeline bug) both mean "no stamp", but
     * only the second is worth a log line.
     */
    internal fun sanitizeStamp(raw: String?): String? {
        val stamp = raw?.takeUnless { it.isBlank() } ?: return null
        if (!STAMP_SHAPE.matches(stamp)) {
            log.warn {
                val shown = if (stamp.length > 32) stamp.take(29) + "..." else stamp
                "build stamp '$shown' does not match $STAMP_SHAPE; reporting no build stamp"
            }
            return null
        }
        return stamp
    }
}
