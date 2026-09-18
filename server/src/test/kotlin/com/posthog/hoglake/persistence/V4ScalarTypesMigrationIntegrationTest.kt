package com.posthog.hoglake.persistence

import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.flywaydb.core.Flyway
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import javax.sql.DataSource

/**
 * V4 against a POPULATED catalog, not an empty one.
 *
 * The schema-equivalence gate only proves fold(migrations) ==
 * schema.sql on a virgin database, which says nothing about what V4
 * does to a catalog that already has columns in it — and V4 is a
 * drop-and-recreate of a CHECK constraint, the one migration shape that
 * can fail on existing rows. This seeds V1-vocabulary columns under the
 * V3 schema, migrates, and asserts the rows survived, the vocabulary
 * actually widened, and the constraint still bites.
 */
@Tag("integration")
class V4ScalarTypesMigrationIntegrationTest {
    /** Every type V1 knew, i.e. what a pre-V4 catalog can contain. */
    private val v1Types =
        listOf(
            "boolean", "int", "long", "float", "double", "decimal",
            "date", "time", "timestamp", "timestamptz", "string", "uuid", "binary",
        )

    /** The ten V4 adds. */
    private val v4Types =
        listOf(
            "int8", "int16", "uint8", "uint16", "uint32", "uint64",
            "timestamp_s", "timestamp_ms", "timestamp_ns", "json",
        )

    /** Flyway configured exactly as Database.migrate does, optionally stopping at [target]. */
    private fun migrate(
        ds: DataSource,
        target: String? = null,
    ) {
        Flyway.configure()
            .dataSource(ds)
            .configuration(mapOf("flyway.postgresql.transactional.lock" to "false"))
            .locations("classpath:db/migration")
            .apply { if (target != null) target(org.flywaydb.core.api.MigrationVersion.fromVersion(target)) }
            .load()
            .migrate()
    }

    @Test
    fun `V4 widens the vocabulary on a catalog that already has columns`(): Unit =
        // Each test owns its database: this one must START at V3, and a
        // shared fixture would hand it whatever state ran first.
        PgTestSupport.freshDatabaseRaw("").use { db ->
            migrate(db.dataSource, target = "3")

            // A pre-V4 catalog: one table, one column per V1 type.
            db.jdbi.useHandleUnchecked { h ->
                // catalog_id is GENERATED ALWAYS; let it assign, then use it.
                h.execute("INSERT INTO hog_catalog (name, data_path) VALUES ('pre-v4', 's3://b/')")
                h.execute("INSERT INTO hog_namespace (catalog_id, namespace_id, name) VALUES (1, 1, 'ns')")
                h.execute("INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (1, 1, 1)")
                v1Types.forEachIndexed { i, type ->
                    h.execute(
                        """
                    INSERT INTO hog_column
                        (catalog_id, table_id, field_id, begin_snapshot, name, col_type, ordinal)
                    VALUES (1, 1, ?, 1, ?, ?, ?)
                    """,
                        i + 1,
                        "c_$type",
                        type,
                        i,
                    )
                }
            }

            // Under V3 the new names are still rejected — the premise.
            assertThatThrownBy { insertColumn(db, 100, "early", "uint64") }
                .describedAs("V3 must not already accept the V4 vocabulary")
                .hasMessageContaining("hog_column_col_type_check")

            migrate(db.dataSource)

            // Nothing was lost or rewritten.
            val surviving =
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery("SELECT col_type FROM hog_column WHERE catalog_id = 1 ORDER BY ordinal")
                        .mapTo(String::class.java)
                        .list()
                }
            assertThat(surviving).describedAs("pre-V4 rows survive the constraint swap").isEqualTo(v1Types)

            // The vocabulary actually widened.
            v4Types.forEachIndexed { i, type -> insertColumn(db, 200 + i, "new_$type", type) }
            assertThat(
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery("SELECT count(*) FROM hog_column WHERE catalog_id = 1")
                        .mapTo(Long::class.java)
                        .one()
                },
            ).isEqualTo((v1Types.size + v4Types.size).toLong())
        }

    @Test
    fun `the recreated constraint still refuses everything outside the vocabulary`(): Unit =
        PgTestSupport.freshDatabaseRaw("").use { db ->
            // A drop-and-recreate that quietly forgot to re-add the CHECK
            // would pass every other assertion in this class.
            migrate(db.dataSource)
            seedTable(db)
            for (bad in listOf("int128", "uint128", "timetz", "interval", "point", "biging", "UUID_T")) {
                assertThatThrownBy { insertColumn(db, 900, "bad_$bad", bad) }
                    .describedAs("col_type '%s'", bad)
                    .hasMessageContaining("hog_column_col_type_check")
            }
        }

    @Test
    fun `the constraint kept the name V1 gave it`(): Unit =
        PgTestSupport.freshDatabaseRaw("").use { db ->
            // schema.sql declares the CHECK inline, so Postgres names it
            // hog_column_col_type_check there. V4 must recreate it under the
            // same name or the schema-equivalence gate compares two
            // differently-named constraints and fails obscurely.
            migrate(db.dataSource)
            val def =
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery(
                        """
                    SELECT pg_get_constraintdef(oid) FROM pg_constraint
                    WHERE conrelid = 'hog_column'::regclass AND conname = 'hog_column_col_type_check'
                    """,
                    ).mapTo(String::class.java).findOne().orElse(null)
                }
            assertThat(def).describedAs("hog_column_col_type_check exists after V4").isNotNull()
            assertThat(def).contains("col_type")
            for (type in v4Types) {
                assertThat(def).describedAs("constraint lists %s", type).contains("'$type'")
            }
        }

    /** Catalog 1 + namespace + table, so hog_column inserts have parents. */
    private fun seedTable(db: PgTestSupport.TestDb) {
        db.jdbi.useHandleUnchecked { h ->
            h.execute("INSERT INTO hog_catalog (name, data_path) VALUES ('v4-test', 's3://b/')")
            h.execute("INSERT INTO hog_namespace (catalog_id, namespace_id, name) VALUES (1, 1, 'ns')")
            h.execute("INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (1, 1, 1)")
        }
    }

    private fun insertColumn(
        db: PgTestSupport.TestDb,
        fieldId: Int,
        name: String,
        type: String,
    ) {
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                """
                INSERT INTO hog_column
                    (catalog_id, table_id, field_id, begin_snapshot, name, col_type, ordinal)
                VALUES (1, 1, ?, 1, ?, ?, ?)
                """,
                fieldId,
                name,
                type,
                fieldId,
            )
        }
    }
}
