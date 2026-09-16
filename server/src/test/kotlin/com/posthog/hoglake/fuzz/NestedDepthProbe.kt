package com.posthog.hoglake.fuzz

import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.api.CreateTableRequestDto
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.columnDefDepth
import com.posthog.hoglake.service.ColumnTrees
import com.posthog.hoglake.wireObjectMapper

/**
 * Depth/breadth bomb probe for the phase-2 nested surfaces: where does
 * each recursive surface stop refusing and start dying?
 */
object NestedDepthProbe {
    private val mapper = wireObjectMapper()

    @JvmStatic
    fun main(args: Array<String>) {
        probeValidate()
        probeWireJson()
        probeBreadth()
    }

    private fun chain(depth: Int): ColumnDef {
        var d: ColumnDef = ColumnDef("element", ColType.LONG)
        repeat(depth) { i ->
            d = ColumnDef(if (i == depth - 1) "c0" else "element", ColType.LIST, null, true, listOf(d))
        }
        return d
    }

    private fun probeValidate() {
        for (depth in listOf(100, 1_000, 5_000, 10_000, 20_000, 50_000, 100_000, 200_000, 500_000)) {
            val t =
                runOnThread {
                    ColumnTrees.validate(listOf(chain(depth)))
                }
            val what =
                when (t) {
                    null -> "ACCEPTED (!!)"
                    is HoglakeException.Validation -> "typed 422"
                    else -> "${t.javaClass.name} @ ${t.stackTrace.firstOrNull()}"
                }
            println("ColumnTrees.validate(chain depth=$depth) -> $what")
            if (t != null && t !is HoglakeException.Validation) break
        }
        for (depth in listOf(10_000, 50_000, 100_000)) {
            val t = runOnThread { columnDefDepth(listOf(chain(depth))) }
            println(
                "  columnDefDepth(chain depth=$depth) -> ${t?.let { "${it.javaClass.name}" } ?: "ok"}",
            )
        }
    }

    private fun probeWireJson() {
        for (depth in listOf(100, 900, 1_000, 1_100, 5_000, 50_000)) {
            val body = StringBuilder("""{"name":"t","columns":""")
            repeat(depth) { body.append("""[{"name":"element","type":"list","children":""") }
            body.append("""[{"name":"element","type":"long"}]""")
            repeat(depth) { body.append("}]") }
            body.append("}")
            val t =
                runOnThread {
                    val dto = mapper.readValue<CreateTableRequestDto>(body.toString())
                    ColumnTrees.validate(dto.columns.map { it.toModel() })
                }
            val what =
                when (t) {
                    null -> "ACCEPTED (!!)"
                    is HoglakeException.Validation -> "typed 422"
                    is com.fasterxml.jackson.core.JacksonException ->
                        "JacksonException -> 400 (${t.javaClass.simpleName})"
                    else -> "${t.javaClass.name} @ ${t.stackTrace.firstOrNull()}"
                }
            println("wire body nesting=$depth -> $what")
        }
    }

    private fun probeBreadth() {
        for (n in listOf(1_000, 100_000, 1_000_000)) {
            val defs = (0 until n).map { ColumnDef("c$it", ColType.LONG) }
            val t0 = System.nanoTime()
            val t = runOnThread { ColumnTrees.validate(defs) }
            val ms = (System.nanoTime() - t0) / 1_000_000
            println("breadth=$n -> ${t?.javaClass?.simpleName ?: "ACCEPTED"} in ${ms}ms")
        }
    }

    /** Run on a plain thread (default stack) and return the escaping Throwable. */
    private fun runOnThread(body: () -> Unit): Throwable? {
        var out: Throwable? = null
        val th =
            Thread {
                try {
                    body()
                } catch (t: Throwable) {
                    out = t
                }
            }
        th.start()
        th.join()
        return out
    }
}
