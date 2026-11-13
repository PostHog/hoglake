package com.posthog.hoglake.persistence

import com.posthog.hoglake.Database
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
 * V36 relaxes V33's "one live namespace per (catalog, field, value)": a
 * database at V35 refuses a second contained namespace for a value, and
 * the same database after the migration takes it, with the lookup index
 * kept (non-unique) and the shape CHECK untouched.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class V36NamespaceContainmentManyMigrationIntegrationTest {
    private val db = PgTestSupport.freshDatabaseAt("35")
    private val index = "hog_namespace_live_containment"

    @AfterAll
    fun close() = db.close()

    private fun seedNamespace(
        id: Int,
        name: String,
        value: Int,
    ) = db.jdbi.useHandleUnchecked { h ->
        h.execute(
            "INSERT INTO hog_catalog (name, data_path) VALUES ('c', 's3://b/c') ON CONFLICT DO NOTHING",
        )
        h.execute(
            """
            INSERT INTO hog_namespace (catalog_id, namespace_id, name, contained_field, contained_value)
            SELECT catalog_id, $id, '$name', 'team_id', $value FROM hog_catalog WHERE name = 'c'
            """,
        )
    }

    private fun indexDef(): String =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT indexdef FROM pg_indexes WHERE indexname = :n")
                .bind("n", index).mapTo(String::class.java).one()
        }

    @Test
    fun `the unique index becomes a plain partial index and a second namespace takes the value`() {
        seedNamespace(1, "team_42", 42)
        assertThat(indexDef()).contains("CREATE UNIQUE INDEX")
        assertThatThrownBy { seedNamespace(2, "imports_42", 42) }
            .hasMessageContaining(index)

        Database.migrate(db.dataSource)

        assertThat(indexDef()).doesNotContain("UNIQUE").contains("catalog_id, contained_field, contained_value")
            .contains("NOT dropped")
        seedNamespace(2, "imports_42", 42)
        // The shape check still holds.
        assertThatThrownBy {
            db.jdbi.useHandleUnchecked { h ->
                h.execute("UPDATE hog_namespace SET contained_value = NULL WHERE name = 'imports_42'")
            }
        }.hasMessageContaining("hog_namespace_containment_shape")
        // Re-applying is free.
        PgTestSupport.applyMigrationFile(db, "V36__namespace_containment_many.sql")
        assertThat(indexDef()).doesNotContain("UNIQUE")
    }
}
