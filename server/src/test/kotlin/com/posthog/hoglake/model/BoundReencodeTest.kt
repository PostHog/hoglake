package com.posthog.hoglake.model

import com.posthog.hoglake.stats.IcebergSingleValue
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * What a promotion owes the bounds already in `hog_file_column_stats`,
 * over EVERY legal promotion edge, without a database.
 *
 * The integration test that covers this walks a handful of ladders, so
 * it kept passing when the decision was narrowed back to a literal
 * `int -> long` pair — the edges it does not walk (`uint8 -> uint32`
 * above all) silently kept 4-byte bounds under an 8-byte type, which is
 * the stale-width poison that makes compaction's bound-merge drop the
 * column. This enumerates the edges instead of sampling them.
 */
class BoundReencodeTest {
    /** Every (from, to) the ALTER path will actually accept. */
    private val legalEdges: List<Pair<ColType, ColType>> =
        ColType.entries.flatMap { from ->
            ColType.entries.filter { from.canPromoteTo(it) }.map { from to it }
        }

    @Test
    fun `every legal promotion edge has the re-encode its mapped types demand`() {
        assertThat(legalEdges).describedAs("the matrix is not empty").isNotEmpty()
        for ((from, to) in legalEdges) {
            val expected =
                when {
                    from.icebergType == to.icebergType -> BoundReencode.NONE
                    from.icebergType == IcebergType.INT && to.icebergType == IcebergType.LONG ->
                        BoundReencode.INT_TO_LONG
                    from.icebergType == IcebergType.FLOAT && to.icebergType == IcebergType.DOUBLE ->
                        BoundReencode.FLOAT_TO_DOUBLE
                    else ->
                        error(
                            "${from.wire} -> ${to.wire} is promotable but induces the " +
                                "Iceberg evolution ${from.icebergType.wire} -> ${to.icebergType.wire}, " +
                                "which has no defined bound re-encode",
                        )
                }
            assertThat(boundReencodeFor(from, to))
                .describedAs("%s -> %s", from.wire, to.wire)
                .isEqualTo(expected)
        }
    }

    @Test
    fun `the whole unsigned ladder is covered, not just the types named long`() {
        // The mutation this exists to catch: a decision written as
        // `from == INT && to == LONG` gets every one of these wrong,
        // because not one of them mentions int or long by name.
        assertThat(boundReencodeFor(ColType.UINT8, ColType.UINT32)).isEqualTo(BoundReencode.INT_TO_LONG)
        assertThat(boundReencodeFor(ColType.UINT16, ColType.UINT32)).isEqualTo(BoundReencode.INT_TO_LONG)
        assertThat(boundReencodeFor(ColType.INT8, ColType.LONG)).isEqualTo(BoundReencode.INT_TO_LONG)
        assertThat(boundReencodeFor(ColType.INT16, ColType.LONG)).isEqualTo(BoundReencode.INT_TO_LONG)
        // And the ones that must stay untouched, for the same reason in
        // reverse: same mapped type, so the stored bytes are already right.
        assertThat(boundReencodeFor(ColType.INT8, ColType.INT16)).isEqualTo(BoundReencode.NONE)
        assertThat(boundReencodeFor(ColType.INT8, ColType.INT)).isEqualTo(BoundReencode.NONE)
        assertThat(boundReencodeFor(ColType.INT16, ColType.INT)).isEqualTo(BoundReencode.NONE)
        assertThat(boundReencodeFor(ColType.UINT8, ColType.UINT16)).isEqualTo(BoundReencode.NONE)
    }

    @Test
    fun `at least one edge of each kind exists, so neither arm is dead`() {
        val kinds = legalEdges.map { (f, t) -> boundReencodeFor(f, t) }.toSet()
        assertThat(kinds)
            .describedAs("the legal matrix exercises every re-encode kind")
            .containsExactlyInAnyOrder(
                BoundReencode.NONE,
                BoundReencode.INT_TO_LONG,
                BoundReencode.FLOAT_TO_DOUBLE,
            )
    }

    @Test
    fun `the widening the decision selects is value-preserving on real bounds`() {
        // The decision is only half the contract; the bytes are the other
        // half. Every INT_TO_LONG edge must turn a 4-byte bound into the
        // 8-byte encoding of the same number, sign included.
        for ((from, to) in legalEdges.filter { boundReencodeFor(it.first, it.second) == BoundReencode.INT_TO_LONG }) {
            for (v in listOf(0, 1, -1, 255, 65_535, Int.MAX_VALUE, Int.MIN_VALUE)) {
                val old = IcebergSingleValue.encode(from, v)
                assertThat(old).describedAs("%s bound width", from.wire).hasSize(4)
                val widened =
                    IcebergSingleValue.encode(
                        to,
                        (IcebergSingleValue.decode(from, old) as Int).toLong(),
                    )
                assertThat(widened).describedAs("%s -> %s bound width", from.wire, to.wire).hasSize(8)
                assertThat(IcebergSingleValue.decode(to, widened))
                    .describedAs("%s -> %s preserves %d", from.wire, to.wire, v)
                    .isEqualTo(v.toLong())
            }
        }
    }

    @Test
    fun `an illegal or absent promotion asks for nothing`() {
        // boundReencodeFor is total — AlterService calls it only after
        // canPromoteTo, but a NONE for anything it has no rule for is the
        // safe default: leave the bytes alone rather than mangle them.
        assertThat(boundReencodeFor(ColType.UINT32, ColType.UINT64)).isEqualTo(BoundReencode.NONE)
        assertThat(boundReencodeFor(ColType.STRING, ColType.JSON)).isEqualTo(BoundReencode.NONE)
        assertThat(boundReencodeFor(ColType.LONG, ColType.INT)).isEqualTo(BoundReencode.NONE)
    }
}
