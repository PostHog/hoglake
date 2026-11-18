package com.posthog.hoglake.store

/** Backend kinds accepted in a verified routing claim. */
enum class BackendKind(val wire: String) {
    HOGLITE("hoglite"),
    POSTGRES("postgres"),
}
