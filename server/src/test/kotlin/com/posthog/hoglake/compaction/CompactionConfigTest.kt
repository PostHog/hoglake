package com.posthog.hoglake.compaction

import com.posthog.hoglake.Config
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path

/**
 * The compaction knobs as PRODUCTION sees them.
 *
 * Every other derate test constructs its config explicitly, or copies
 * one with an override — which proves the arithmetic and nothing about
 * the value a running server actually uses. Two things stay invisible
 * to that: [CompactionConfig.DEFAULT_NESTED_SORT_EXPANSION] quietly
 * becoming 1, and App's `nestedSortExpansion = cfg.compactionNestedSortExpansion`
 * wiring being dropped. Either restores the unbounded sorted-path heap
 * this branch set out to bound, with the whole suite still green.
 *
 * So the assertions here take the default path deliberately: a config
 * built WITHOUT the argument, and the env-backed [Config] the server
 * boots from.
 */
class CompactionConfigTest {
    /** `s struct{a int}` — the shape that triggers the derate. */
    private val nestedColumns =
        listOf(
            Column(
                1,
                0,
                ColumnDef("s", ColType.STRUCT),
                listOf(Column(2, 0, ColumnDef("a", ColType.INT))),
            ),
        )

    private val flatColumns = listOf(Column(1, 0, ColumnDef("id", ColType.LONG)))

    private val target = 512L * 1024 * 1024

    /** A config built the way production builds one: no derate argument. */
    private fun defaulted() = CompactionConfig(targetBytes = target, tierTarget = 8, maxGroupsPerRun = 1)

    @Test
    fun `a config with NO override derates a nested sorted table`() {
        // The default value itself, exercised through the default path.
        // With DEFAULT_NESTED_SORT_EXPANSION at 1 this is an equality and
        // the sorted path goes back to materializing a full target's
        // worth of nested object graph — tens of gigabytes at the
        // measured 30-70x expansion.
        val cfg = defaulted()
        assertThat(cfg.nestedSortExpansion)
            .describedAs("the default must actually derate; 1 is the no-op")
            .isGreaterThan(1)
        assertThat(cfg.effectiveTargetBytes(nestedColumns, sorted = true))
            .describedAs("a nested SORTED table is planned smaller than the raw target")
            .isLessThan(target)
            .isEqualTo(target / cfg.nestedSortExpansion)
    }

    @Test
    fun `the default is large enough for the measured expansion`() {
        // 30-70x measured (a list<long> table peaked at 343 MiB from 4.6
        // MiB compressed). Erring large costs smaller compaction groups;
        // erring small costs an OOM in a background loop, so the floor is
        // the top of the measured band, not the bottom.
        // EXACTLY 64, not "at least". The number is documented — in
        // CompactionConfig's KDoc, in Config's, and in
        // iceberg-federation.md §2.8 — and a `>=` assertion lets the code
        // drift upward while all three keep saying 64. If the value
        // should change, the docs change with it in the same commit.
        assertThat(CompactionConfig.DEFAULT_NESTED_SORT_EXPANSION)
            .describedAs("the documented default, exactly")
            .isEqualTo(64)
    }

    @Test
    fun `a config with NO override leaves flat and unsorted tables alone`() {
        // The derate is about nested object graphs, not about sorting or
        // about nesting alone. Applying it more widely would shrink
        // ordinary tables' groups 64-fold for nothing.
        val cfg = defaulted()
        assertThat(cfg.effectiveTargetBytes(flatColumns, sorted = true)).isEqualTo(target)
        assertThat(cfg.effectiveTargetBytes(nestedColumns, sorted = false)).isEqualTo(target)
        assertThat(cfg.effectiveTargetBytes(flatColumns, sorted = false)).isEqualTo(target)
    }

    @Test
    fun `the env-backed Config defaults to the same value the planner does`() {
        // The server boots from Config, not from CompactionConfig's own
        // default. If the two drift, the documented knob and the code's
        // fallback disagree, and which one you get depends on whether the
        // wiring below is present.
        assertThat(Config().compactionNestedSortExpansion)
            .isEqualTo(CompactionConfig.DEFAULT_NESTED_SORT_EXPANSION)
    }

    @Test
    fun `App wires the env knob into the planner's config`() {
        // Read off disk, like ScalarTypeParityTest reads the enum and the
        // migration: a Kotlin test cannot reach App's private
        // compactionService, and the failure mode — the wiring line being
        // dropped — is silent precisely because CompactionConfig's own
        // default equals Config's, so every default-valued deployment
        // behaves identically and only an operator who SET the env var
        // discovers the knob does nothing.
        val app = Files.readString(Path.of("src/main/kotlin/com/posthog/hoglake/App.kt"))
        assertThat(app)
            .describedAs("App must pass Config.compactionNestedSortExpansion into CompactionConfig")
            .containsPattern("""nestedSortExpansion\s*=\s*cfg\.compactionNestedSortExpansion""")
    }

    @Test
    fun `a derate of zero or less is refused at construction`() {
        // The knob is operator-settable; 0 would divide by zero and a
        // negative would invert the budget. Fail at boot, not on the
        // first sweep of a nested table.
        for (bad in listOf(0, -1)) {
            assertThatThrownBy {
                CompactionConfig(targetBytes = target, tierTarget = 8, maxGroupsPerRun = 1, nestedSortExpansion = bad)
            }
                .describedAs("nestedSortExpansion=%d", bad)
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("nested sort expansion")
        }
        // 1 is legal and documented: it disables the derate for an
        // operator who has sized the heap for it.
        assertThat(
            CompactionConfig(targetBytes = target, tierTarget = 8, maxGroupsPerRun = 1, nestedSortExpansion = 1)
                .effectiveTargetBytes(nestedColumns, sorted = true),
        ).isEqualTo(target)
    }

    @Test
    fun `the derated budget never falls below the tier ladder's floor`() {
        // A tiny target divided by 64 must still build a tier ladder —
        // CompactionTiers refuses a target below 2, and a table whose
        // budget derated to nothing would silently stop compacting.
        val cfg = CompactionConfig(targetBytes = 10, tierTarget = 2, maxGroupsPerRun = 1)
        val budget = cfg.effectiveTargetBytes(nestedColumns, sorted = true)
        assertThat(budget).isGreaterThanOrEqualTo(2)
        CompactionTiers.of(budget, cfg.tierTarget) // must not throw
    }
}
