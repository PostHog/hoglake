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
 * V7 against a POPULATED catalog, not an empty one — V4's test, one
 * migration on.
 *
 * The schema-equivalence gate only proves fold(migrations) ==
 * schema.sql on a virgin database, which says nothing about what V7
 * does to a catalog that already has columns in it. V7 does three
 * things to a live table, and all three can go wrong on existing rows:
 * it ADDS a nullable column (must not rewrite or default anything), it
 * drops and recreates a CHECK (must not reject rows that were legal
 * before), and it REPLACES the live-ordinal unique index with a
 * per-parent one (must keep refusing what the old one refused).
 *
 * That last one is the sharp edge and has its own test: the new index
 * covers (catalog_id, table_id, parent_field_id, ordinal), and
 * Postgres treats NULLs as DISTINCT by default — so without NULLS NOT
 * DISTINCT the duplicate-ordinal guard would silently stop covering
 * top-level columns, which are exactly the rows it used to cover.
 */
@Tag("integration")
class V7NestedTypesMigrationIntegrationTest {
    /** Everything a pre-V7 catalog can contain (V1 + V4). */
    private val preV7Types =
        listOf(
            "boolean", "int8", "int16", "int", "long", "uint8", "uint16",
            "uint32", "uint64", "float", "double", "decimal", "date", "time",
            "timestamp_s", "timestamp_ms", "timestamp", "timestamp_ns",
            "timestamptz", "string", "json", "uuid", "binary",
        )

    /** The three V7 adds. */
    private val v7Types = listOf("list", "struct", "map")

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
    fun `V7 widens the vocabulary and adds the tree edge on a populated catalog`(): Unit =
        PgTestSupport.freshDatabaseRaw("").use { db ->
            migrate(db.dataSource, target = "4")

            // A pre-V7 catalog: one table, one column per pre-V7 type.
            db.jdbi.useHandleUnchecked { h ->
                h.execute("INSERT INTO hog_catalog (name, data_path) VALUES ('pre-v7', 's3://b/')")
                h.execute("INSERT INTO hog_namespace (catalog_id, namespace_id, name) VALUES (1, 1, 'ns')")
                h.execute("INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (1, 1, 1)")
                preV7Types.forEachIndexed { i, type ->
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

            // Under V4 the container names are still rejected — the
            // premise. Inserted WITHOUT parent_field_id, which does not
            // exist yet: that column's absence is the other half of the
            // premise, and naming it here would mask the CHECK.
            assertThatThrownBy {
                db.jdbi.useHandleUnchecked { h ->
                    h.execute(
                        """
                        INSERT INTO hog_column
                            (catalog_id, table_id, field_id, begin_snapshot, name, col_type, ordinal)
                        VALUES (1, 1, 100, 1, 'early', 'struct', 100)
                        """,
                    )
                }
            }
                .describedAs("V4 must not already accept the V7 vocabulary")
                .hasMessageContaining("hog_column_col_type_check")

            migrate(db.dataSource)

            // Nothing was lost, rewritten, or defaulted: every pre-V7 row
            // keeps its type AND comes out a top-level column, which is
            // what a NULL parent_field_id means.
            val surviving =
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery(
                        """
                        SELECT col_type, parent_field_id IS NULL AS top_level
                        FROM hog_column WHERE catalog_id = 1 ORDER BY ordinal
                        """,
                    ).map { rs, _ -> rs.getString("col_type") to rs.getBoolean("top_level") }.list()
                }
            assertThat(surviving.map { it.first })
                .describedAs("pre-V7 rows survive the constraint swap")
                .isEqualTo(preV7Types)
            assertThat(surviving.map { it.second })
                .describedAs("every pre-V7 row is top-level (parent_field_id IS NULL)")
                .allMatch { it }

            // The vocabulary actually widened, and a child row links back.
            insertColumn(db, 200, "s", "struct", ordinal = 100)
            insertColumn(db, 201, "a", "int", ordinal = 0, parentFieldId = 200)
            insertColumn(db, 202, "l", "list", ordinal = 101)
            insertColumn(db, 203, "element", "long", ordinal = 0, parentFieldId = 202)
            insertColumn(db, 204, "m", "map", ordinal = 102)
            assertThat(
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery(
                        "SELECT count(*) FROM hog_column WHERE catalog_id = 1 AND parent_field_id IS NOT NULL",
                    ).mapTo(Long::class.java).one()
                },
            ).isEqualTo(2L)
            assertThat(v7Types).hasSize(3) // the three names exercised above
        }

    @Test
    fun `the per-parent ordinal index still refuses duplicate TOP-LEVEL ordinals`(): Unit =
        PgTestSupport.freshDatabaseRaw("").use { db ->
            // THE regression this migration could have caused. Postgres
            // treats NULLs as distinct in a unique index by default, so
            // adding parent_field_id to the key without NULLS NOT DISTINCT
            // turns the guard off for every top-level column — silently,
            // because nothing else in the suite inserts a duplicate.
            migrate(db.dataSource)
            seedTable(db)
            insertColumn(db, 1, "a", "int", ordinal = 0)
            assertThatThrownBy { insertColumn(db, 2, "b", "int", ordinal = 0) }
                .describedAs("two live top-level columns at ordinal 0")
                .hasMessageContaining("hog_column_live_ordinal")
        }

    @Test
    fun `siblings of DIFFERENT parents may share an ordinal`(): Unit =
        PgTestSupport.freshDatabaseRaw("").use { db ->
            // The other half: ordinals order siblings now, so two struct
            // fields in two different structs are both legitimately
            // ordinal 0. The old table-wide index refused the second.
            migrate(db.dataSource)
            seedTable(db)
            insertColumn(db, 1, "s1", "struct", ordinal = 0)
            insertColumn(db, 2, "s2", "struct", ordinal = 1)
            insertColumn(db, 3, "a", "int", ordinal = 0, parentFieldId = 1)
            insertColumn(db, 4, "a", "int", ordinal = 0, parentFieldId = 2)
            // ...but two fields of the SAME struct still may not.
            assertThatThrownBy { insertColumn(db, 5, "b", "int", ordinal = 0, parentFieldId = 1) }
                .describedAs("two live fields of struct 1 at ordinal 0")
                .hasMessageContaining("hog_column_live_ordinal")
        }

    @Test
    fun `an end-snapshotted row frees its ordinal, at any level`(): Unit =
        PgTestSupport.freshDatabaseRaw("").use { db ->
            // The index is partial (WHERE end_snapshot IS NULL), and the
            // partiality has to survive the rebuild or every alter that
            // retires and re-inserts a column row would start failing.
            migrate(db.dataSource)
            seedTable(db)
            insertColumn(db, 1, "s", "struct", ordinal = 0)
            insertColumn(db, 2, "a", "int", ordinal = 0, parentFieldId = 1)
            db.jdbi.useHandleUnchecked { h ->
                h.execute("UPDATE hog_column SET end_snapshot = 5 WHERE field_id = 2")
            }
            insertColumn(db, 3, "a2", "int", ordinal = 0, parentFieldId = 1)
        }

    @Test
    fun `a column cannot be its own parent`(): Unit =
        PgTestSupport.freshDatabaseRaw("").use { db ->
            migrate(db.dataSource)
            seedTable(db)
            assertThatThrownBy { insertColumn(db, 7, "self", "struct", ordinal = 0, parentFieldId = 7) }
                .hasMessageContaining("hog_column_parent_not_self")
        }

    @Test
    fun `the recreated constraint still refuses everything outside the vocabulary`(): Unit =
        PgTestSupport.freshDatabaseRaw("").use { db ->
            // A drop-and-recreate that quietly forgot to re-add the CHECK
            // would pass every other assertion in this class.
            migrate(db.dataSource)
            seedTable(db)
            for (bad in listOf("int128", "uint128", "timetz", "interval", "point", "biging", "array", "row")) {
                assertThatThrownBy { insertColumn(db, 900, "bad_$bad", bad, ordinal = 900) }
                    .describedAs("col_type '%s'", bad)
                    .hasMessageContaining("hog_column_col_type_check")
            }
        }

    @Test
    fun `the constraint kept the name V1 gave it, and lists the containers`(): Unit =
        PgTestSupport.freshDatabaseRaw("").use { db ->
            // schema.sql declares the CHECK inline, so Postgres names it
            // hog_column_col_type_check there. V7 must recreate it under
            // the same name or the schema-equivalence gate compares two
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
            assertThat(def).describedAs("hog_column_col_type_check exists after V7").isNotNull()
            for (type in v7Types) {
                assertThat(def).describedAs("constraint lists %s", type).contains("'$type'")
            }
            // Order is load-bearing (the equivalence gate compares the
            // normalized text): the containers are APPENDED.
            assertThat(def!!.indexOf("'binary'")).isLessThan(def.indexOf("'list'"))
        }

    @Test
    fun `the migration refuses a catalog whose constraint it does not recognise`(): Unit =
        PgTestSupport.freshDatabaseRaw("").use { db ->
            // The loud guard. A hand-patched or doctored catalog gets a
            // message naming what V7 expected, not "constraint does not
            // exist" from a bare DROP.
            migrate(db.dataSource, target = "4")
            db.jdbi.useHandleUnchecked { h ->
                h.execute("ALTER TABLE hog_column DROP CONSTRAINT hog_column_col_type_check")
            }
            assertThatThrownBy { migrate(db.dataSource) }
                .hasMessageContaining("V7 expected the constraint hog_column_col_type_check")
        }

    @Test
    fun `the migration refuses a catalog whose ordinal index it does not recognise`(): Unit =
        PgTestSupport.freshDatabaseRaw("").use { db ->
            migrate(db.dataSource, target = "4")
            db.jdbi.useHandleUnchecked { h -> h.execute("DROP INDEX hog_column_live_ordinal") }
            assertThatThrownBy { migrate(db.dataSource) }
                .hasMessageContaining("V7 expected the index hog_column_live_ordinal")
        }

    @Test
    fun `the migration refuses an index that merely shares V1's NAME`(): Unit =
        PgTestSupport.freshDatabaseRaw("").use { db ->
            // EXISTENCE was never the check. V7 DROPs this index and
            // replaces it, so an index sharing V1's name while guarding
            // something else would be discarded silently — the exact
            // divergence the guard's own comment claims to catch. Here
            // it guards the wrong KEY.
            migrate(db.dataSource, target = "4")
            db.jdbi.useHandleUnchecked { h ->
                h.execute("DROP INDEX hog_column_live_ordinal")
                h.execute(
                    """
                    CREATE UNIQUE INDEX hog_column_live_ordinal
                        ON hog_column (catalog_id, table_id, name)
                        WHERE end_snapshot IS NULL
                    """,
                )
            }
            assertThatThrownBy { migrate(db.dataSource) }
                .hasMessageContaining("whose definition is not the one V1__init.sql created")
                .hasMessageContaining("(catalog_id, table_id, ordinal)")
        }

    @Test
    fun `the migration refuses an index that lost V1's partiality`(): Unit =
        PgTestSupport.freshDatabaseRaw("").use { db ->
            // Same name, same key, no WHERE: a total unique index is a
            // STRICTER guarantee, and replacing it silently would drop a
            // constraint the catalog had been relying on.
            migrate(db.dataSource, target = "4")
            db.jdbi.useHandleUnchecked { h ->
                h.execute("DROP INDEX hog_column_live_ordinal")
                h.execute(
                    "CREATE UNIQUE INDEX hog_column_live_ordinal ON hog_column (catalog_id, table_id, ordinal)",
                )
            }
            assertThatThrownBy { migrate(db.dataSource) }
                .hasMessageContaining("whose definition is not the one V1__init.sql created")
        }

    @Test
    fun `the migration refuses an index that is no longer UNIQUE`(): Unit =
        PgTestSupport.freshDatabaseRaw("").use { db ->
            migrate(db.dataSource, target = "4")
            db.jdbi.useHandleUnchecked { h ->
                h.execute("DROP INDEX hog_column_live_ordinal")
                h.execute(
                    """
                    CREATE INDEX hog_column_live_ordinal
                        ON hog_column (catalog_id, table_id, ordinal)
                        WHERE end_snapshot IS NULL
                    """,
                )
            }
            assertThatThrownBy { migrate(db.dataSource) }
                .hasMessageContaining("whose definition is not the one V1__init.sql created")
        }

    /** Catalog 1 + namespace + table, so hog_column inserts have parents. */
    private fun seedTable(db: PgTestSupport.TestDb) {
        db.jdbi.useHandleUnchecked { h ->
            h.execute("INSERT INTO hog_catalog (name, data_path) VALUES ('v7-test', 's3://b/')")
            h.execute("INSERT INTO hog_namespace (catalog_id, namespace_id, name) VALUES (1, 1, 'ns')")
            h.execute("INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (1, 1, 1)")
        }
    }

    private fun insertColumn(
        db: PgTestSupport.TestDb,
        fieldId: Int,
        name: String,
        type: String,
        ordinal: Int,
        parentFieldId: Int? = null,
    ) {
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                """
                INSERT INTO hog_column
                    (catalog_id, table_id, field_id, begin_snapshot, name, col_type, ordinal, parent_field_id)
                VALUES (1, 1, ?, 1, ?, ?, ?, ?)
                """,
                fieldId,
                name,
                type,
                ordinal,
                parentFieldId,
            )
        }
    }
}
