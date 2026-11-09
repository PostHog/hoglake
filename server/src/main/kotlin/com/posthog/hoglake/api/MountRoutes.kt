package com.posthog.hoglake.api

import com.posthog.hoglake.auth.hoglakeAuth
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.store.MountRecord
import com.posthog.hoglake.store.MountStore
import com.posthog.hoglake.store.requireMountWriter
import io.ktor.server.application.Application
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.put
import io.ktor.server.routing.route
import io.ktor.server.routing.routing

/** One mount value; `retention_months` has no source yet (plan 5.6), so it is always null. */
data class MountValueDto(
    val value: Long?,
    val retentionMonths: Int? = null,
)

data class MountDto(
    val name: String,
    val revision: Long,
    val enabled: Boolean,
    val values: List<MountValueDto>,
)

/**
 * The whole mount at [revision] + 1; [revision] is the one the writer read
 * (0: never written). Nullable so a missing field is refused rather than read
 * as 0 or false; null [values] is empty, as a Go nil slice marshals.
 */
data class PutMountRequestDto(
    val revision: Long?,
    val enabled: Boolean?,
    val values: List<MountValueDto>? = null,
)

private fun MountRecord.toDto() = MountDto(name, revision, enabled, values.map { MountValueDto(it) })

/**
 * The mount API (operator only, `auth/AuthPolicy.kt`). Reads serve any
 * operator; a write serves only the token whose `sub` is [writer]
 * (HOGLAKE_MOUNT_WRITER_SUBJECT), checked before the body is read, in every
 * auth mode. Every write attempt is one `mount_write` audit line.
 */
fun Application.installMountRoutes(
    mounts: () -> MountStore,
    writer: String,
) {
    routing {
        route("/v1/mounts") {
            get { call.respond(mounts().list().map { it.toDto() }) }
            get("/{mount}") { call.respond(mounts().get(call.pathParam("mount")).toDto()) }
            put("/{mount}") {
                val name = call.pathParam("mount")
                val caller = call.hoglakeAuth.callerOrNull
                val write =
                    Audit.audited(
                        "mount_write",
                        name,
                        null,
                        detail = ::auditDetail,
                        actor = { caller?.actor },
                        // On refusals too: the jti is what ties an attempt to its token.
                        message = { caller?.let { "jti=${it.tokenId}" } },
                    ) {
                        requireMountWriter(caller, writer)
                        val req = call.receive<PutMountRequestDto>()
                        val values = req.values.orEmpty()
                        if (req.revision == null || req.enabled == null || values.any { it.value == null }) {
                            throw HoglakeException.Validation("revision, enabled and every value are required")
                        }
                        if (values.any { it.retentionMonths != null }) {
                            throw HoglakeException.Validation("retention_months has no source yet and must be null")
                        }
                        mounts().put(name, req.revision, req.enabled, values.map { it.value!! })
                    }
                call.respond(write.after.toDto())
            }
        }
    }
}

/** From and to, and the values that came and went (the first 100 of each): what an incident asks. */
private fun auditDetail(w: MountStore.Write): String {
    val before = w.before?.values.orEmpty()

    fun List<Long>.capped() = if (size <= 100) toString() else "${take(100)}+${size - 100}"
    return "revision=${w.before?.revision ?: 0}->${w.after.revision} enabled=${w.after.enabled} " +
        "values=${w.after.values.size} added=${(w.after.values - before.toSet()).capped()} " +
        "removed=${(before - w.after.values.toSet()).capped()}"
}
