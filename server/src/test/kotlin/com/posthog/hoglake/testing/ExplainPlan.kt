package com.posthog.hoglake.testing

/**
 * The bit of EXPLAIN-output parsing that plan tests share, extracted so
 * its POLARITY can be asserted without a container.
 *
 * A plan assertion is only as good as the parse under it: a field that
 * always reads `false` makes its assertion vacuous, and an integration
 * test cannot tell the difference between "the plan is clean" and "the
 * parser never sees anything". `ExplainPlanTest` feeds this synthetic
 * plan text — with and without the shape — and pins both answers.
 */
object ExplainPlan {
    /**
     * A plan node: its line, how many times it ran, what it read, and
     * whether it threw rows away on `path` — which is what separates a
     * probe on `(catalog_id, path)` from a probe on a prefix of it.
     */
    data class Node(
        val line: String,
        val loops: Long,
        val buffers: Long,
        val filtersPath: Boolean,
    )

    private val LOOPS = Regex("""loops=(\d+)""")

    /**
     * Every node of [text] with its OWN detail.
     *
     * A node's detail lines follow it until the next `->` (a child) or a
     * line at its own indentation or shallower, so a leaf's `Buffers:`
     * is its own and a parent's is cumulative.
     *
     * `\bhit=` and `\bread=` rather than "shared hit=" / "shared read=":
     * one line carries BOTH as `shared hit=2608 read=15450`, so a
     * pattern anchored on "shared read=" silently reads zero and a cold
     * scan of a whole relation measures as its cache hits.
     */
    fun nodes(text: String): List<Node> {
        val lines = text.lines()

        fun indent(l: String) = l.length - l.trimStart().length
        return lines.mapIndexedNotNull { i, line ->
            val loops = LOOPS.find(line)?.groupValues?.get(1)?.toLong() ?: return@mapIndexedNotNull null
            val detail =
                lines.drop(i + 1)
                    .takeWhile { it.isNotBlank() && indent(it) > indent(line) && !it.contains("->") }
            val buffersLine = detail.firstOrNull { it.trim().startsWith("Buffers:") }
            val hit = buffersLine?.let { Regex("""\bhit=(\d+)""").find(it)?.groupValues?.get(1)?.toLong() } ?: 0L
            val read = buffersLine?.let { Regex("""\bread=(\d+)""").find(it)?.groupValues?.get(1)?.toLong() } ?: 0L
            val filtersPath = detail.any { it.trim().startsWith("Filter:") && it.contains("path") }
            Node(line.trim(), loops, hit + read, filtersPath)
        }
    }
}
