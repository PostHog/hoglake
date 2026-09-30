package com.posthog.hoglake.testing

/**
 * Whether wall-clock assertions may run.
 *
 * Some tests measure a cost — a commit-lock hold, a concurrent index
 * build against a plain one — and record the figure a design decision
 * was made on. The MEASUREMENT runs everywhere and is printed, because
 * a changed number is worth seeing. The ASSERTION on the measured
 * milliseconds runs only on a quiet host that an engineer is measuring
 * on deliberately: shared CI runners failed both the fan-in budget and
 * the concurrent-vs-plain ordering on their first run while every
 * relative property held, and a wall-clock threshold that flakes teaches
 * nothing. Set `HOGLAKE_MEASURE=1` to apply them.
 */
object Measuring {
    val enabled: Boolean = System.getenv("HOGLAKE_MEASURE") == "1"
}
