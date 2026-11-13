package com.posthog.hoglake.persistence

import com.posthog.hoglake.model.HoglakeException

/** Ledger decoration must not hide a typed protocol or audit outcome. */
internal fun Throwable.partialHoglakeCause(): HoglakeException? =
    if (this is PartialResult) cause as? HoglakeException else null
