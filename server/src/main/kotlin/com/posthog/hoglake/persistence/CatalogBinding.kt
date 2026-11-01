package com.posthog.hoglake.persistence

import com.posthog.hoglake.model.HoglakeException

/** A synchronous operation carries its durable PostgreSQL identity through nested service calls. */
internal object CatalogBinding {
    data class Identity(val name: String, val id: Long)

    private val current = ThreadLocal<Identity>()

    fun id(name: String): Long? = current.get()?.takeIf { it.name == name }?.id

    fun currentId(): Long? = current.get()?.id

    fun check(
        name: String,
        id: Long,
    ) {
        if (id(name)?.let { it != id } == true) throw HoglakeException.NotFound("catalog '$name' incarnation")
    }

    fun <T> withIdentity(
        name: String,
        id: Long,
        body: () -> T,
    ): T {
        val previous = current.get()
        check(previous == null || previous == Identity(name, id)) { "conflicting catalog operation binding" }
        current.set(Identity(name, id))
        try {
            return body()
        } finally {
            if (previous == null) current.remove() else current.set(previous)
        }
    }
}
