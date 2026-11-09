package com.posthog.hoglake.store

import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.auth.JwksSigningKeys
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.PredicateGrant
import com.posthog.hoglake.auth.Scope
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.persistence.FileRepo
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * What the compiled scope selects from real catalog rows: every file kind
 * a mount must leave out sits beside the ones it must return.
 *
 * `events` (the subject, object-prefix layout on `team_id` from snapshot 1):
 *  - spec 1 = identity(team_id), day(ts); spec 2 = the REORDERED spec,
 *    day(ts), identity(team_id); spec 3 = bucket(team_id), another transform.
 *  - Day values equal to the team id 42 under both orders, a null value, an
 *    unpartitioned file, pending, failed and quarantined files, a file
 *    ended at snapshot 3, and files of teams 7 and 99.
 *
 * `other`, a second table in the catalog, with the same spec id, the same
 * key and the colliding value 42. `broken` has a spec row with no fields.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReadScopeIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val columns = listOf(ColumnDef("team_id", ColType.LONG), ColumnDef("ts", ColType.LONG))
    private val catalogId: Long
    private val events: Long
    private val other: Long
    private val broken: Long
    private val team: Long

    init {
        catalogs.createCatalog("rs", "s3://bucket/rs")
        catalogs.createNamespace("rs", "ingest")
        val info = catalogs.createTable("rs", "ingest", "events", columns)
        events = info.tableId
        team = info.columns.single { it.def.name == "team_id" }.fieldId
        val ts = info.columns.single { it.def.name == "ts" }.fieldId
        other = catalogs.createTable("rs", "ingest", "other", columns).tableId
        broken = catalogs.createTable("rs", "ingest", "broken", columns).tableId
        catalogId =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = 'rs'").mapTo(Long::class.java).one()
            }
        db.jdbi.useHandleUnchecked { h ->
            fun spec(
                table: Long,
                spec: Long,
                vararg fields: Pair<Long, String>,
            ) {
                h.execute(
                    "INSERT INTO hog_partition_spec (catalog_id, table_id, spec_id, begin_snapshot) " +
                        "VALUES (?, ?, ?, 0)",
                    catalogId,
                    table,
                    spec,
                )
                fields.forEachIndexed { key, (source, transform) ->
                    h.execute(
                        "INSERT INTO hog_partition_field (catalog_id, table_id, spec_id, key_index, source_field_id, " +
                            "transform, transform_param) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        catalogId,
                        table,
                        spec,
                        key,
                        source,
                        transform,
                        if (transform == "bucket") 16 else null,
                    )
                }
            }
            spec(events, 1, team to "identity", ts to "day")
            spec(events, 2, ts to "day", team to "identity")
            spec(events, 3, team to "bucket")
            spec(other, 1, team to "identity", ts to "day")
            spec(broken, 1, team to "identity")
            spec(broken, 2)
            for (table in listOf(events, other, broken)) {
                h.execute(
                    "INSERT INTO hog_table_layout (catalog_id, table_id, begin_snapshot, object_prefix_field_id) " +
                        "VALUES (?, ?, 1, ?)",
                    catalogId,
                    table,
                    team,
                )
            }

            fun file(
                id: Long,
                table: Long,
                spec: Long?,
                vararg values: String?,
                state: String = "provided",
                quarantined: Boolean = false,
                end: Long? = null,
            ) {
                h.execute(
                    "INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot, end_snapshot, " +
                        "path, record_count, file_size_bytes, row_id_start, spec_id, stats_state, " +
                        "prefix_quarantined) " +
                        "VALUES (?, ?, ?, 1, ?, ?, 1, 1, ?, ?, ?, ?)",
                    catalogId,
                    id,
                    table,
                    end,
                    "s3://bucket/rs/f$id.parquet",
                    id * 10,
                    spec,
                    state,
                    quarantined,
                )
                values.forEachIndexed { key, value ->
                    h.execute(
                        "INSERT INTO hog_file_partition_value (catalog_id, data_file_id, key_index, value) " +
                            "VALUES (?, ?, ?, ?)",
                        catalogId,
                        id,
                        key,
                        value,
                    )
                }
            }
            file(1, events, 1, "42", "19000")
            file(2, events, 1, "7", "42") // a day ordinal equal to team 42
            file(3, events, 2, "42", "7") // reordered: day 42, team 7
            file(4, events, 2, "19000", "42") // reordered: team 42
            file(5, events, 3, "42") // bucket(team_id) = 42
            file(6, events, 1, null, "19000")
            file(7, events, null) // unpartitioned
            file(8, events, 1, "42", "19000", state = "pending")
            file(9, events, 1, "42", "19000", state = "failed")
            file(10, events, 1, "42", "19000", quarantined = true)
            file(11, events, 1, "42", "19000", end = 3)
            file(12, events, 1, "99", "19000")
            file(13, events, 1, "7", "19000")
            file(21, other, 1, "42", "19000") // another table, same spec id, key and value
        }
    }

    @AfterAll
    fun tearDown() = db.close()

    /** The data file ids [scope] admits at [snapshot]; the statement tests visibility, nothing else. */
    private fun admitted(
        scope: ReadScope,
        snapshot: Long = 5,
    ): List<Long> =
        db.jdbi.withHandleUnchecked { h ->
            scope.bind(
                h.createQuery(
                    "SELECT df.data_file_id FROM hog_data_file df " +
                        "WHERE ${FileRepo.visibleAt("df")} AND ${scope.sql("df")} ORDER BY df.data_file_id",
                ),
            ).bind("snapshot", snapshot).mapTo(Long::class.java).list()
        }

    private fun load(
        mount: Collection<Long>,
        grant: PredicateGrant?,
        table: Long = events,
        snapshot: Long = 5,
    ): ReadScope =
        db.jdbi.withHandleUnchecked { h -> ReadScope.load(h, catalogId, table, snapshot, team, mount, grant) }

    @Test
    fun `the scope admits the granted team's proven live files of the table under every spec, and nothing else`() {
        val scope = load(listOf(42, 99), PredicateGrant.Values(mapOf(42L to Scope.READ, 7L to Scope.READ)))
        assertThat(scope.arms).containsExactly(ReadScope.Arm(0, listOf(1)), ReadScope.Arm(1, listOf(2)))
        assertThat(admitted(scope)).containsExactly(1, 4)
    }

    @Test
    fun `a star grant admits the mount's values`() {
        assertThat(admitted(load(listOf(42, 99), PredicateGrant.All(Scope.READ)))).containsExactly(1, 4, 12)
        assertThat(admitted(load(listOf(7), PredicateGrant.All(Scope.READ)))).containsExactly(2, 3, 13)
    }

    @Test
    fun `visibility is the statement's own snapshot`() {
        assertThat(admitted(load(listOf(42), PredicateGrant.All(Scope.READ), snapshot = 2), snapshot = 2))
            .containsExactly(1, 4, 11)
    }

    @Test
    fun `an empty intersection admits no file`() {
        val scope = load(listOf(42), PredicateGrant.Values(mapOf(7L to Scope.READ)))
        assertThat(scope.isEmpty).isTrue()
        assertThat(admitted(scope)).isEmpty()
        assertThat(admitted(load(listOf(42), null))).isEmpty()
    }

    /**
     * #147's vector (`tests/oauth/contract-vectors.json`, "a value outside
     * the mount ... filtered out"), on the token hogtower mints: the
     * `scoped_team` vector's caller holds teams 42 and 7, and the mount
     * holds 42 alone.
     */
    @Test
    fun `hogtower's scoped token reads only its values inside the mount`() {
        val root =
            ObjectMapper().readTree(
                javaClass.getResourceAsStream("/oauth/contract-vectors.json") ?: error("contract vectors missing"),
            )
        val (kid, key) = JwksSigningKeys.parseJwks(ObjectMapper().writeValueAsBytes(root["jwks"])).entries.single()
        val verifier =
            JwtVerifier(root["issuer"].asText(), "hoglake", StaticSigningKeys(kid, key), 60, setOf("team_id")) {
                root["verify_at"].asLong()
            }
        val vector = root["vectors"].single { it["name"].asText() == "scoped_team" }
        val grant = verifier.verify(vector["token"].asText()).predicates["team_id"]
        assertThat(grant).isEqualTo(PredicateGrant.Values(mapOf(42L to Scope.WRITE, 7L to Scope.READ)))

        val scope = load(listOf(42), grant)
        assertThat(scope.values).containsExactly("42")
        assertThat(admitted(scope)).containsExactly(1, 4)
    }

    @Test
    fun `a snapshot before the layout is refused`() {
        assertThatThrownBy { load(listOf(42), PredicateGrant.All(Scope.READ), snapshot = 0) }
            .isInstanceOf(MountIntegrityError::class.java)
    }

    @Test
    fun `a spec row without fields is refused`() {
        assertThatThrownBy { load(listOf(42), PredicateGrant.All(Scope.READ), table = broken) }
            .isInstanceOf(MountIntegrityError::class.java)
            .hasMessageContaining("spec 2 of table $broken has no fields")
    }

    @Test
    fun `the colliding table reads its own file through its own scope`() {
        assertThat(admitted(load(listOf(42), PredicateGrant.All(Scope.READ), table = other))).containsExactly(21)
    }
}
