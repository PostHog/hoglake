package com.posthog.hoglake.store

import com.posthog.hoglake.auth.PredicateGrant
import com.posthog.hoglake.auth.Scope
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.PartitionSpec
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.observability.Metrics
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * The compiler alone: values, arms and refusals. What the SQL selects is
 * ReadScopeIntegrationTest's; how it plans is ReadScopePlanIntegrationTest's.
 */
class ReadScopeTest {
    private val team = 1L
    private val ts = 2L

    private val teamDay = PartitionSpec(1, listOf(identity(team), field(ts, Transform.DAY)))

    private fun identity(id: Long) = PartitionFieldDef(id, Transform.IDENTITY)

    private fun field(
        id: Long,
        transform: Transform,
    ) = PartitionFieldDef(id, transform, if (transform == Transform.BUCKET) 16 else null)

    private fun values(vararg v: Pair<Long, Scope>) = PredicateGrant.Values(mapOf(*v))

    private fun compile(
        grant: PredicateGrant?,
        mount: Collection<Long> = listOf(42, 99),
        specs: List<PartitionSpec> = listOf(teamDay),
        layout: Long? = team,
    ) = ReadScope.compile(7, 11, team, layout, specs, mount, grant)

    @Test
    fun `listed values are intersected with the mount's`() {
        // #147's vector: 7 is outside the mount, so it is filtered out.
        val scope = compile(values(42L to Scope.WRITE, 7L to Scope.READ))
        assertThat(scope.values).containsExactly("42")
    }

    @Test
    fun `a star grant reads the mount's values, never more`() {
        assertThat(compile(PredicateGrant.All(Scope.READ)).values).containsExactly("42", "99")
        assertThat(compile(PredicateGrant.All(Scope.WRITE), mount = listOf(5)).values).containsExactly("5")
    }

    @Test
    fun `values are canonical decimals in numeric order`() {
        val scope = compile(PredicateGrant.All(Scope.READ), mount = listOf(100, 9, 42, 9, 0))
        assertThat(scope.values).containsExactly("0", "9", "42", "100")
    }

    @Test
    fun `an empty intersection compiles to FALSE`() {
        for (scope in listOf(
            compile(values(7L to Scope.READ)),
            compile(null),
            compile(PredicateGrant.All(Scope.READ), mount = emptyList()),
        )) {
            assertThat(scope.isEmpty).isTrue()
            assertThat(scope.sql("df")).isEqualTo("FALSE")
        }
    }

    @Test
    fun `each spec with one identity field on the mount's field is matched at that field's key`() {
        val reordered = PartitionSpec(2, listOf(field(ts, Transform.DAY), identity(team)))
        val bucketed = PartitionSpec(3, listOf(field(team, Transform.BUCKET)))
        val bucketThenIdentity = PartitionSpec(4, listOf(field(team, Transform.BUCKET), identity(team)))
        val twice = PartitionSpec(5, listOf(identity(team), identity(team)))
        val otherField = PartitionSpec(6, listOf(identity(ts)))
        val teamMonth = PartitionSpec(7, listOf(identity(team), field(ts, Transform.MONTH)))
        val scope =
            compile(
                PredicateGrant.All(Scope.READ),
                specs = listOf(reordered, teamMonth, bucketed, bucketThenIdentity, twice, otherField, teamDay),
            )
        assertThat(scope.arms).containsExactly(
            ReadScope.Arm(0, listOf(1, 7)),
            ReadScope.Arm(1, listOf(2, 4)),
        )
        val sql = scope.sql("df")
        assertThat(sql.split(" OR ")).hasSize(2)
        assertThat(sql)
            .startsWith("(df.catalog_id = :rs_catalog AND df.table_id = :rs_table ")
            .contains("df.stats_state = 'provided' AND NOT df.prefix_quarantined")
            .contains("df.spec_id = ANY(:rs_specs_1) AND df.data_file_id = ANY(ARRAY(")
            .contains("rs_pv.key_index = :rs_key_1 AND rs_pv.value = ANY(:rs_values)")
        // The events table's shape: every spec leads with the field, one arm, no OR.
        val events = compile(PredicateGrant.All(Scope.READ), specs = listOf(teamMonth, teamDay))
        assertThat(events.arms).containsExactly(ReadScope.Arm(0, listOf(1, 7)))
        assertThat(events.sql("df")).doesNotContain(" OR ")
    }

    @Test
    fun `a table whose object-prefix field is not the mount's is refused`() {
        assertThatThrownBy { compile(PredicateGrant.All(Scope.READ), layout = null) }
            .isInstanceOf(MountIntegrityError::class.java)
            .hasMessageContaining("object-prefix field is unset")
        assertThatThrownBy { compile(PredicateGrant.All(Scope.READ), layout = ts) }
            .isInstanceOf(MountIntegrityError::class.java)
    }

    @Test
    fun `a spec without fields is refused, whatever the caller holds`() {
        assertThatThrownBy { compile(null, specs = listOf(teamDay, PartitionSpec(2, emptyList()))) }
            .isInstanceOf(MountIntegrityError::class.java)
            .hasMessageContaining("spec 2 of table 11 has no readable fields")
    }

    @Test
    fun `no spec with the identity field is refused`() {
        for (specs in listOf(emptyList(), listOf(PartitionSpec(3, listOf(field(team, Transform.BUCKET)))))) {
            assertThatThrownBy { compile(PredicateGrant.All(Scope.READ), specs = specs) }
                .isInstanceOf(MountIntegrityError::class.java)
                .hasMessageContaining("no spec of table 11")
        }
    }

    @Test
    fun `a mount value that is not a canonical decimal is refused`() {
        assertThatThrownBy { compile(values(42L to Scope.READ), mount = listOf(42, -42)) }
            .isInstanceOf(MountIntegrityError::class.java)
            .hasMessageContaining("mount value -42")
    }

    @Test
    fun `each refusal is counted`() {
        val registry = SimpleMeterRegistry()
        Metrics.bind(registry)
        try {
            runCatching { compile(null, layout = null) }
            runCatching { compile(null, mount = listOf(-1)) }
            compile(null)
            assertThat(registry.counter("hoglake_mount_integrity_errors_total").count()).isEqualTo(2.0)
        } finally {
            Metrics.clear()
        }
    }
}
