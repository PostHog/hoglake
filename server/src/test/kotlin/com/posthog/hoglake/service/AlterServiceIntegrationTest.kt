package com.posthog.hoglake.service

import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ChangeKind
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.atomic.AtomicInteger

/**
 * AlterService: op semantics, in-order application, time travel,
 * partition-spec lifecycle, and the one-snapshot/one-change contract.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AlterServiceIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val alter = AlterService(db.jdbi)
    private val catalogs = CatalogService(db.jdbi)

    private val counter = AtomicInteger(0)

    private val idCol = ColumnDef("id", ColType.LONG, nullable = false)
    private val nameCol = ColumnDef("name", ColType.STRING)
    private val scoreCol = ColumnDef("score", ColType.FLOAT)
    private val countCol = ColumnDef("count", ColType.INT)
    private val tsCol = ColumnDef("ts", ColType.TIMESTAMPTZ)
    private val leafCol = ColumnDef("leaf_scalar_field", ColType.LONG)

    @AfterAll
    fun tearDown() = db.close()

    /** Fresh catalog + namespace + one table with the five stock columns. */
    private fun fixture(): Pair<String, String> {
        val cat = "alter-cat-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(idCol, nameCol, scoreCol, countCol, tsCol))
        return cat to "ns"
    }

    private fun head(cat: String) = catalogs.getCatalog(cat).headSnapshotId

    private fun catId(cat: String): Long = catalogs.getCatalog(cat).catalogId

    @Test
    fun `an alter 422 never echoes an unbounded caller path or segment`() {
        // The failing SEGMENT is by definition the part that matched
        // nothing, so it is unvalidated caller input — the path around
        // it was already capped, which is what made this easy to miss.
        // Measured before the fix: 5,092 characters across four op
        // kinds.
        val (cat, ns) = fixture()
        val huge = "q".repeat(5_000)
        // A real STRUCT parent, so the failing segment reaches the
        // "struct ... has no field" branch. Pointed at a SCALAR the walk
        // stops earlier, at "is not a struct", and the segment is never
        // quoted — which is how a first attempt at this test let the
        // mutation survive.
        alter.alterTable(
            cat,
            ns,
            "t",
            listOf(
                AlterOp.AddColumn(
                    ColumnDef("addr", ColType.STRUCT, children = listOf(ColumnDef("zip", ColType.LONG))),
                ),
            ),
        )
        // The add_column already-exists message caps the NEW name and
        // not the parent: `where` is non-empty only when
        // requireStructParent already resolved every segment against
        // stored names, so the parent is bounded by construction.
        // `addr` already has a `zip` from the fixture above.
        val dup =
            catchThrowable {
                alter.alterTable(
                    cat,
                    ns,
                    "t",
                    listOf(AlterOp.AddColumn(ColumnDef("zip", ColType.LONG), parent = "addr")),
                )
            }
        assertThat(dup).isInstanceOf(HoglakeException.Validation::class.java)
        assertThat(dup.message!!).describedAs("the parent survives whole").contains("of struct 'addr'")

        val ops =
            listOf<AlterOp>(
                AlterOp.DropColumn("addr.$huge"),
                AlterOp.RenameColumn("addr.$huge", "other"),
                AlterOp.PromoteColumn("addr.$huge", ColType.STRING),
                AlterOp.AddColumn(ColumnDef("x", ColType.LONG), parent = "addr.$huge"),
                // And the path itself, unresolvable from its first
                // segment.
                AlterOp.DropColumn(huge),
            )
        for (op in ops) {
            val thrown = catchThrowable { alter.alterTable(cat, ns, "t", listOf(op)) }
            assertThat(thrown)
                .describedAs("%s", op::class.simpleName)
                .isInstanceOf(HoglakeException::class.java)
            assertThat(thrown.message!!.length)
                .describedAs("%s message length", op::class.simpleName)
                .isLessThan(300)
        }
    }

    @Test
    fun `an alter 422 keeps a legitimately deep path intact`() {
        // The other half of the rule: a path built from STORED names is
        // already bounded, and clipping it took away the half an
        // operator needs to find the column.
        //
        // LONGER THAN THE CAP, deliberately. A 46-character path against
        // a 64-character cap proves nothing — re-capping every site
        // leaves such a test green, so it discriminates the old 40-char
        // width from no cap at all rather than the un-cap decision. Four
        // struct levels of ~28 characters each is ~115, comfortably past
        // 64, and every segment is a stored catalog name.
        val (cat, ns) = fixture()
        val levels = listOf("outer_container_level_one", "second_container_level_two", "third_container_lvl")
        alter.alterTable(
            cat,
            ns,
            "t",
            listOf(
                AlterOp.AddColumn(
                    ColumnDef(
                        levels[0],
                        ColType.STRUCT,
                        children =
                            listOf(
                                ColumnDef(
                                    levels[1],
                                    ColType.STRUCT,
                                    children =
                                        listOf(
                                            ColumnDef(
                                                levels[2],
                                                ColType.STRUCT,
                                                children = listOf(ColumnDef("leaf_scalar_field", ColType.LONG)),
                                            ),
                                        ),
                                ),
                            ),
                    ),
                ),
            ),
        )
        val deep = levels.joinToString(".") + ".leaf_scalar_field"
        assertThat(deep.length).describedAs("the fixture must exceed the cap to prove anything")
            .isGreaterThan(64)

        // 1. A path that resolves to a SCALAR: the "not a struct"
        //    refusal quotes the container path, which is stored.
        val notAStruct =
            catchThrowable {
                alter.alterTable(
                    cat,
                    ns,
                    "t",
                    listOf(AlterOp.AddColumn(ColumnDef("x", ColType.LONG), parent = deep)),
                )
            }
        assertThat(notAStruct).isInstanceOf(HoglakeException.Validation::class.java)
        assertThat(notAStruct.message!!).describedAs("resolvable path survives whole").contains(deep)

        // 2. The add_column ALREADY-EXISTS refusal, whose `where` is
        //    non-empty only when requireStructParent already resolved
        //    every segment — so the parent is bounded by construction
        //    and capping it is pure loss. A short parent proves nothing
        //    here; this one is over 64 characters.
        val deepStruct = levels.joinToString(".")
        alter.alterTable(
            cat,
            ns,
            "t",
            listOf(AlterOp.AddColumn(ColumnDef("added_leaf", ColType.LONG), parent = deepStruct)),
        )
        val dup =
            catchThrowable {
                alter.alterTable(
                    cat,
                    ns,
                    "t",
                    listOf(AlterOp.AddColumn(ColumnDef("added_leaf", ColType.LONG), parent = deepStruct)),
                )
            }
        assertThat(deepStruct.length).isGreaterThan(64)
        assertThat(dup).isInstanceOf(HoglakeException.Validation::class.java)
        assertThat(dup.message!!)
            .describedAs("the resolved parent survives whole")
            .contains("of struct '$deepStruct'")

        // 3. The PARTITION-SOURCE refusal, which the un-capping was
        //    motivated by and which had no test at all. Its path is
        //    built by walking catalog names, so it is bounded by
        //    construction and must arrive whole.
        //
        //    Pointed at the INNERMOST struct, not the outer one: the
        //    outer container's path is its own 25-character name, which
        //    a 64-character cap leaves untouched, so the assertion would
        //    have been vacuous — verbatim the defect this test's own
        //    step 1 comment warns about.
        val innermost = leafOf(cat, ns, deepStruct)
        assertThat(deepStruct.length).describedAs("the quoted path must exceed the cap")
            .isGreaterThan(64)
        val nested =
            catchThrowable {
                alter.alterTable(
                    cat,
                    ns,
                    "t",
                    listOf(
                        AlterOp.SetPartitionSpec(
                            listOf(PartitionFieldDef(innermost.fieldId, Transform.IDENTITY)),
                        ),
                    ),
                )
            }
        assertThat(nested).isInstanceOf(HoglakeException.Validation::class.java)
        assertThat(nested.message!!)
            .describedAs("the partition-source refusal names the whole path")
            .contains(deepStruct)
    }

    /** The column [path] names, resolved by walking the live tree. */
    private fun leafOf(
        cat: String,
        ns: String,
        path: String,
    ): com.posthog.hoglake.model.Column {
        var cols = catalogs.getTable(cat, ns, "t").columns
        var found: com.posthog.hoglake.model.Column? = null
        for (segment in path.split('.')) {
            found = cols.single { it.def.name == segment }
            cols = found.children
        }
        return found!!
    }

    @Test
    fun `every refusal quoting a stored path quotes it whole`() {
        // The remaining un-capped sites, each with a path well over the
        // 64-character cap. Every one of them is built by walking
        // CATALOG names, so it is bounded by construction — and a silent
        // re-cap would clip exactly the half an operator needs. A
        // re-cap on any of these passed the whole suite before this
        // test existed.
        val (cat, ns) = fixture()
        val a = "outer_container_level_one"
        val b = "second_container_level_two"
        val c = "third_container_lvl"
        alter.alterTable(
            cat,
            ns,
            "t",
            listOf(
                AlterOp.AddColumn(
                    ColumnDef(
                        a,
                        ColType.STRUCT,
                        children =
                            listOf(
                                ColumnDef(
                                    b,
                                    ColType.STRUCT,
                                    children =
                                        listOf(
                                            ColumnDef(
                                                c,
                                                ColType.STRUCT,
                                                // TWO fields: dropping one must
                                                // not be refused as "the last
                                                // field of struct", which fires
                                                // before the source check and
                                                // would mask it.
                                                children = listOf(leafCol, ColumnDef("sibling", ColType.LONG)),
                                            ),
                                            ColumnDef(
                                                "list_container_under_struct",
                                                ColType.LIST,
                                                children =
                                                    listOf(
                                                        ColumnDef(
                                                            "element",
                                                            ColType.STRUCT,
                                                            children = listOf(leafCol),
                                                        ),
                                                    ),
                                            ),
                                        ),
                                ),
                            ),
                    ),
                ),
            ),
        )
        val structPath = "$a.$b.$c"
        val listPath = "$a.$b.list_container_under_struct"
        val underList = "$listPath.element.leaf_scalar_field"
        for (p in listOf(structPath, listPath, underList)) {
            assertThat(p.length).describedAs("%s must exceed the cap", p).isGreaterThan(64)
        }

        // assertStructInterior's LIST/MAP branch.
        assertThat(
            catchThrowable {
                alter.alterTable(
                    cat,
                    ns,
                    "t",
                    listOf(AlterOp.AddColumn(ColumnDef("x", ColType.LONG), parent = listPath)),
                )
            }.message,
        ).describedAs("list/map interior refusal").contains(listPath)

        // requireSourceField's "sits under" branch.
        val underListCol = leafOf(cat, ns, underList)
        assertThat(
            catchThrowable {
                alter.alterTable(
                    cat,
                    ns,
                    "t",
                    listOf(
                        AlterOp.SetPartitionSpec(
                            listOf(PartitionFieldDef(underListCol.fieldId, Transform.IDENTITY)),
                        ),
                    ),
                )
            }.message,
        ).describedAs("sits-under-a-list refusal").contains(underList)

        // Both dropBlockedMessage arms: the source ITSELF, and a
        // container holding one.
        val leaf = leafOf(cat, ns, "$structPath.leaf_scalar_field")
        alter.alterTable(
            cat,
            ns,
            "t",
            listOf(
                AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(leaf.fieldId, Transform.IDENTITY))),
            ),
        )
        assertThat(
            catchThrowable {
                alter.alterTable(cat, ns, "t", listOf(AlterOp.DropColumn("$structPath.leaf_scalar_field")))
            }.message,
        ).describedAs("drop the source itself")
            .contains("$structPath.leaf_scalar_field")
            .contains("it is a source")

        // And the LAST-FIELD refusal, found while fixing the above: it
        // quotes the same resolved path and fires before the source
        // check, so it needs the same guarantee. (Reached through a
        // STRUCT chain — a list interior is refused earlier, by the
        // branch tested above.)
        assertThat(
            catchThrowable {
                alter.alterTable(cat, ns, "t", listOf(AlterOp.DropColumn("$structPath.sibling")))
            },
        ).describedAs("dropping a non-source sibling is allowed").isNull()
        assertThat(
            catchThrowable {
                alter.alterTable(cat, ns, "t", listOf(AlterOp.DropColumn("$structPath.leaf_scalar_field")))
            }.message,
        ).describedAs("last field of a struct").contains("$structPath.leaf_scalar_field")
        assertThat(
            catchThrowable { alter.alterTable(cat, ns, "t", listOf(AlterOp.DropColumn(structPath))) }.message,
        ).describedAs("drop a container holding the source").contains(structPath)
    }

    // ---- happy paths, one op each ----------------------------------------

    @Test
    fun `add column appends with fresh field_id and next ordinal`() {
        val (cat, ns) = fixture()
        val info =
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.AddColumn(ColumnDef("tags", ColType.STRING)),
                ),
            )
        val added = info.columns.single { it.def.name == "tags" }
        assertThat(added.fieldId).isEqualTo(6) // fields 1..5 existed
        assertThat(added.ordinal).isEqualTo(5)
        assertThat(info.columns).hasSize(6)
        assertThat(catalogs.getTable(cat, ns, "t").columns.map { it.def.name })
            .containsExactly("id", "name", "score", "count", "ts", "tags")
    }

    @Test
    fun `drop column removes it live but keeps it at the old snapshot`() {
        val (cat, ns) = fixture()
        val before = head(cat)
        val info = alter.alterTable(cat, ns, "t", listOf(AlterOp.DropColumn("name")))
        assertThat(info.columns.map { it.def.name })
            .containsExactly("id", "score", "count", "ts")
        assertThat(catalogs.getTable(cat, ns, "t", snapshot = before).columns.map { it.def.name })
            .containsExactly("id", "name", "score", "count", "ts")
    }

    @Test
    fun `rename column keeps field_id and ordinal, time travel shows old name`() {
        val (cat, ns) = fixture()
        val before = head(cat)
        val oldCol = catalogs.getTable(cat, ns, "t").columns.single { it.def.name == "name" }

        val info = alter.alterTable(cat, ns, "t", listOf(AlterOp.RenameColumn("name", "label")))
        val renamed = info.columns.single { it.def.name == "label" }
        assertThat(renamed.fieldId).isEqualTo(oldCol.fieldId)
        assertThat(renamed.ordinal).isEqualTo(oldCol.ordinal)
        assertThat(renamed.def.type).isEqualTo(ColType.STRING)

        val old =
            catalogs.getTable(cat, ns, "t", snapshot = before)
                .columns.single { it.fieldId == oldCol.fieldId }
        assertThat(old.def.name).isEqualTo("name")
    }

    @Test
    fun `rename column is refused while a live id-less data file exists`() {
        val (cat, ns) = fixture()
        val tableId = catalogs.getTable(cat, ns, "t").tableId
        // A LIVE file flagged missing_field_ids: it binds columns by name,
        // so a rename would silently NULL its history in readers.
        db.jdbi.withHandleUnchecked { h ->
            h.execute(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                    path, record_count, file_size_bytes, row_id_start, missing_field_ids)
                VALUES (?, 1, ?, 2, 's3://bucket/idless.parquet', 10, 100, 0, true)
                """,
                catId(cat),
                tableId,
            )
        }
        assertThatThrownBy {
            alter.alterTable(cat, ns, "t", listOf(AlterOp.AddColumn(ColumnDef("new_column", ColType.LONG))))
        }.isInstanceOf(HoglakeException.IdlessFilesPresent::class.java)
        assertThatThrownBy {
            alter.alterTable(cat, ns, "t", listOf(AlterOp.RenameColumn("name", "label")))
        }
            .isInstanceOf(HoglakeException.IdlessFilesPresent::class.java)
            .hasMessageContaining("1 id-less")
            .hasMessageContaining("live data file(s)")
        // The refused request minted nothing: head unchanged, column intact.
        assertThat(catalogs.getTable(cat, ns, "t").columns.map { it.def.name }).contains("name")

        // RenameTable is unaffected — table binding rides table_uuid.
        alter.alterTable(cat, ns, "t", listOf(AlterOp.RenameTable("t_renamed")))
        assertThat(catalogs.getTable(cat, ns, "t_renamed").tableId).isEqualTo(tableId)

        // Once the flagged file is retired (end-snapshotted), rename works.
        db.jdbi.withHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_data_file SET end_snapshot = 4 WHERE catalog_id = ? AND data_file_id = 1",
                catId(cat),
            )
        }
        val info = alter.alterTable(cat, ns, "t_renamed", listOf(AlterOp.RenameColumn("name", "label")))
        assertThat(info.columns.map { it.def.name }).contains("label")
    }

    @Test
    fun `rename column ignores flagged files that are not live`() {
        val (cat, ns) = fixture()
        val tableId = catalogs.getTable(cat, ns, "t").tableId
        db.jdbi.withHandleUnchecked { h ->
            h.execute(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                    end_snapshot, path, record_count, file_size_bytes, row_id_start, missing_field_ids)
                VALUES (?, 1, ?, 2, 3, 's3://bucket/idless-historical.parquet', 10, 100, 0, true)
                """,
                catId(cat),
                tableId,
            )
        }
        val info = alter.alterTable(cat, ns, "t", listOf(AlterOp.RenameColumn("name", "label")))
        assertThat(info.columns.map { it.def.name }).contains("label")
    }

    @Test
    fun `promote int to long and float to double`() {
        val (cat, ns) = fixture()
        val before = head(cat)
        val info =
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.PromoteColumn("count", ColType.LONG),
                    AlterOp.PromoteColumn("score", ColType.DOUBLE),
                ),
            )
        assertThat(info.columns.single { it.def.name == "count" }.def.type)
            .isEqualTo(ColType.LONG)
        assertThat(info.columns.single { it.def.name == "score" }.def.type)
            .isEqualTo(ColType.DOUBLE)
        // Time travel: old types at the pre-alter snapshot, same field ids.
        val old = catalogs.getTable(cat, ns, "t", snapshot = before).columns
        assertThat(old.single { it.def.name == "count" }.def.type).isEqualTo(ColType.INT)
        assertThat(old.single { it.def.name == "score" }.def.type).isEqualTo(ColType.FLOAT)
    }

    @Test
    fun `rename column is refused while a live file is still pending hydration`() {
        // Pinned regression (bug hunt #6, the rename-guard TOCTOU): a
        // deferred-stats file has missing_field_ids=false (schema default)
        // until the hydrator's footer read — its id state is UNKNOWN, so
        // the guard must block on 'pending' too, not only on the flag.
        val (cat, ns) = fixture()
        val tableId = catalogs.getTable(cat, ns, "t").tableId
        db.jdbi.withHandleUnchecked { h ->
            h.execute(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                    path, record_count, file_size_bytes, row_id_start, stats_state)
                VALUES (?, 1, ?, 2, 's3://bucket/deferred.parquet', 10, 100, 0, 'pending')
                """,
                catId(cat),
                tableId,
            )
        }
        assertThatThrownBy {
            alter.alterTable(cat, ns, "t", listOf(AlterOp.AddColumn(ColumnDef("new_column", ColType.LONG))))
        }.isInstanceOf(HoglakeException.IdlessFilesPresent::class.java)
        assertThatThrownBy {
            alter.alterTable(cat, ns, "t", listOf(AlterOp.RenameColumn("name", "label")))
        }
            .isInstanceOf(HoglakeException.IdlessFilesPresent::class.java)
            .hasMessageContaining("1 not-yet-hydrated")
        assertThat(catalogs.getTable(cat, ns, "t").columns.map { it.def.name }).contains("name")

        // Failure before reading the footer (for example, the whole-object cap)
        // leaves missing_field_ids=false without ever verifying the IDs.
        db.jdbi.withHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_data_file SET stats_state = 'failed' WHERE catalog_id = ? AND data_file_id = 1",
                catId(cat),
            )
        }
        val beforeFailureChecks = head(cat)
        for (op in listOf(
            AlterOp.AddColumn(ColumnDef("new_column", ColType.LONG)),
            AlterOp.RenameColumn("name", "label"),
        )) {
            assertThatThrownBy { alter.alterTable(cat, ns, "t", listOf(op)) }
                .isInstanceOf(HoglakeException.IdlessFilesPresent::class.java)
        }
        assertThat(head(cat)).isEqualTo(beforeFailureChecks)

        // Hydration lands an id-bearing verdict: failed -> provided with
        // missing_field_ids=false. The rename is now allowed.
        db.jdbi.withHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_data_file SET stats_state = 'provided' WHERE catalog_id = ? AND data_file_id = 1",
                catId(cat),
            )
        }
        val info = alter.alterTable(cat, ns, "t", listOf(AlterOp.RenameColumn("name", "label")))
        assertThat(info.columns.map { it.def.name }).contains("label")
    }

    @Test
    fun `rename guard message distinguishes id-less from not-yet-hydrated blockers`() {
        val (cat, ns) = fixture()
        val tableId = catalogs.getTable(cat, ns, "t").tableId
        db.jdbi.withHandleUnchecked { h ->
            h.execute(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                    path, record_count, file_size_bytes, row_id_start, missing_field_ids)
                VALUES (?, 1, ?, 2, 's3://bucket/idless.parquet', 10, 100, 0, true)
                """,
                catId(cat),
                tableId,
            )
            h.execute(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                    path, record_count, file_size_bytes, row_id_start, stats_state)
                VALUES (?, 2, ?, 2, 's3://bucket/pending.parquet', 10, 100, 10, 'pending')
                """,
                catId(cat),
                tableId,
            )
        }
        assertThatThrownBy {
            alter.alterTable(cat, ns, "t", listOf(AlterOp.RenameColumn("name", "label")))
        }
            .isInstanceOf(HoglakeException.IdlessFilesPresent::class.java)
            .hasMessageContaining("1 id-less")
            .hasMessageContaining("1 not-yet-hydrated")
    }

    @Test
    fun `promote re-encodes existing 4-byte stats bounds to the new width, values preserved`() {
        // Pinned regression (bug hunt #5): int->long / float->double left
        // hog_file_column_stats bounds in the old 4-byte encoding, which
        // the live-typed decode (compaction bound-merge, readers) rejects.
        val (cat, ns) = fixture()
        val tableId = catalogs.getTable(cat, ns, "t").tableId
        val countField = catalogs.getTable(cat, ns, "t").columns.single { it.def.name == "count" }.fieldId
        val scoreField = catalogs.getTable(cat, ns, "t").columns.single { it.def.name == "score" }.fieldId
        db.jdbi.withHandleUnchecked { h ->
            h.execute(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                    path, record_count, file_size_bytes, row_id_start)
                VALUES (?, 1, ?, 2, 's3://bucket/promote.parquet', 10, 100, 0)
                """,
                catId(cat),
                tableId,
            )
            for ((field, lower, upper) in listOf(
                Triple(
                    countField,
                    com.posthog.hoglake.stats.IcebergSingleValue.encodeInt(-7),
                    com.posthog.hoglake.stats.IcebergSingleValue.encodeInt(123),
                ),
                Triple(
                    scoreField,
                    com.posthog.hoglake.stats.IcebergSingleValue.encodeFloat(1.5f),
                    com.posthog.hoglake.stats.IcebergSingleValue.encodeFloat(3.25f),
                ),
            )) {
                h.execute(
                    """
                    INSERT INTO hog_file_column_stats
                        (catalog_id, data_file_id, field_id, value_count, null_count, lower_bound, upper_bound)
                    VALUES (?, 1, ?, 10, 0, ?, ?)
                    """,
                    catId(cat),
                    field,
                    lower,
                    upper,
                )
            }
        }

        alter.alterTable(
            cat,
            ns,
            "t",
            listOf(
                AlterOp.PromoteColumn("count", ColType.LONG),
                AlterOp.PromoteColumn("score", ColType.DOUBLE),
            ),
        )

        val bounds =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT field_id, lower_bound, upper_bound FROM hog_file_column_stats
                    WHERE catalog_id = ? AND data_file_id = 1
                    """,
                ).bind(0, catId(cat))
                    .map { rs, _ ->
                        rs.getLong("field_id") to Pair(rs.getBytes("lower_bound"), rs.getBytes("upper_bound"))
                    }
                    .list().toMap()
            }
        // 8-byte encodings under the NEW types, values preserved exactly
        // (sign extension included — the negative int is the sharp edge).
        assertThat(bounds[countField]!!.first)
            .isEqualTo(com.posthog.hoglake.stats.IcebergSingleValue.encodeLong(-7L))
        assertThat(bounds[countField]!!.second)
            .isEqualTo(com.posthog.hoglake.stats.IcebergSingleValue.encodeLong(123L))
        assertThat(bounds[scoreField]!!.first)
            .isEqualTo(com.posthog.hoglake.stats.IcebergSingleValue.encodeDouble(1.5))
        assertThat(bounds[scoreField]!!.second)
            .isEqualTo(com.posthog.hoglake.stats.IcebergSingleValue.encodeDouble(3.25))
    }

    @Test
    fun `rename table resolves new name at head and old name at old snapshot`() {
        val (cat, ns) = fixture()
        val before = head(cat)
        val uuid = catalogs.getTable(cat, ns, "t").tableUuid

        val info = alter.alterTable(cat, ns, "t", listOf(AlterOp.RenameTable("t2")))
        assertThat(info.name).isEqualTo("t2")
        assertThat(info.tableUuid).isEqualTo(uuid)

        assertThat(catalogs.getTable(cat, ns, "t2").tableUuid).isEqualTo(uuid)
        assertThatThrownBy { catalogs.getTable(cat, ns, "t") }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(catalogs.getTable(cat, ns, "t", snapshot = before).tableUuid).isEqualTo(uuid)
    }

    @Test
    fun `set partition spec returns spec_id 1 with ordered fields`() {
        val (cat, ns) = fixture()
        val cols = catalogs.getTable(cat, ns, "t").columns.associateBy { it.def.name }
        val info =
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.SetPartitionSpec(
                        listOf(
                            PartitionFieldDef(cols["ts"]!!.fieldId, Transform.DAY),
                            PartitionFieldDef(cols["id"]!!.fieldId, Transform.BUCKET, 16),
                            PartitionFieldDef(cols["name"]!!.fieldId, Transform.IDENTITY),
                        ),
                    ),
                ),
            )
        val spec = info.partitionSpec!!
        assertThat(spec.specId).isEqualTo(1)
        assertThat(spec.fields).containsExactly(
            PartitionFieldDef(cols["ts"]!!.fieldId, Transform.DAY),
            PartitionFieldDef(cols["id"]!!.fieldId, Transform.BUCKET, 16),
            PartitionFieldDef(cols["name"]!!.fieldId, Transform.IDENTITY),
        )
        // key_index mirrors list position in the DB.
        val keyed =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                SELECT key_index, source_field_id FROM hog_partition_field
                WHERE catalog_id = :c AND table_id = :t ORDER BY key_index
                """,
                ).bind("c", catId(cat)).bind("t", info.tableId).mapToMap().list()
            }
        assertThat(keyed.map { it["key_index"] }).containsExactly(0, 1, 2)
    }

    // ---- spec lifecycle --------------------------------------------------

    @Test
    fun `replacing the spec increments spec_id and ends the old spec`() {
        val (cat, ns) = fixture()
        val cols = catalogs.getTable(cat, ns, "t").columns.associateBy { it.def.name }
        val tsField = PartitionFieldDef(cols["ts"]!!.fieldId, Transform.DAY)
        val idField = PartitionFieldDef(cols["id"]!!.fieldId, Transform.IDENTITY)

        val first = alter.alterTable(cat, ns, "t", listOf(AlterOp.SetPartitionSpec(listOf(tsField))))
        assertThat(first.partitionSpec!!.specId).isEqualTo(1)

        val second = alter.alterTable(cat, ns, "t", listOf(AlterOp.SetPartitionSpec(listOf(idField))))
        assertThat(second.partitionSpec!!.specId).isEqualTo(2)
        assertThat(second.partitionSpec!!.fields).containsExactly(idField)

        val specs =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT spec_id, end_snapshot FROM hog_partition_spec " +
                        "WHERE catalog_id = :c AND table_id = :t ORDER BY spec_id",
                ).bind("c", catId(cat)).bind("t", second.tableId).mapToMap().list()
            }
        assertThat(specs).hasSize(2)
        assertThat(specs[0]["end_snapshot"]).isNotNull()
        assertThat(specs[1]["end_snapshot"]).isNull()

        // Clear back to unpartitioned: no live spec row remains.
        val cleared = alter.alterTable(cat, ns, "t", listOf(AlterOp.SetPartitionSpec(emptyList())))
        assertThat(cleared.partitionSpec).isNull()
        val live =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT count(*) FROM hog_partition_spec " +
                        "WHERE catalog_id = :c AND table_id = :t AND end_snapshot IS NULL",
                ).bind("c", catId(cat)).bind("t", second.tableId).mapTo(Long::class.javaObjectType).one()
            }
        assertThat(live).isEqualTo(0)
    }

    @Test
    fun `dropping a live spec source column is rejected`() {
        val (cat, ns) = fixture()
        val ts = catalogs.getTable(cat, ns, "t").columns.single { it.def.name == "ts" }
        alter.alterTable(
            cat,
            ns,
            "t",
            listOf(
                AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(ts.fieldId, Transform.DAY))),
            ),
        )
        assertThatThrownBy { alter.alterTable(cat, ns, "t", listOf(AlterOp.DropColumn("ts"))) }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("partition spec")
        // Clearing the spec unblocks the drop.
        alter.alterTable(
            cat,
            ns,
            "t",
            listOf(
                AlterOp.SetPartitionSpec(emptyList()),
                AlterOp.DropColumn("ts"),
            ),
        )
        assertThat(catalogs.getTable(cat, ns, "t").columns.map { it.def.name })
            .doesNotContain("ts")
    }

    @Test
    fun `setting a spec on a column dropped earlier in the request is rejected`() {
        val (cat, ns) = fixture()
        val ts = catalogs.getTable(cat, ns, "t").columns.single { it.def.name == "ts" }
        assertThatThrownBy {
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.DropColumn("ts"),
                    AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(ts.fieldId, Transform.DAY))),
                ),
            )
        }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("not a live column")
        // Atomicity: the failed request left nothing behind, ts is intact.
        assertThat(catalogs.getTable(cat, ns, "t").columns.map { it.def.name }).contains("ts")
    }

    @Test
    fun `spec validation - transform types and params`() {
        val (cat, ns) = fixture()
        val cols = catalogs.getTable(cat, ns, "t").columns.associateBy { it.def.name }
        val nameId = cols["name"]!!.fieldId
        val tsId = cols["ts"]!!.fieldId

        // Temporal transform on a string column.
        assertThatThrownBy {
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(nameId, Transform.MONTH))),
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)

        // Bucket without a param, bucket with param 0.
        assertThatThrownBy {
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(nameId, Transform.BUCKET))),
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy {
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(nameId, Transform.BUCKET, 0))),
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)

        // Param on a non-bucket transform.
        assertThatThrownBy {
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(tsId, Transform.DAY, 4))),
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)

        // Unknown field id.
        assertThatThrownBy {
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(9999, Transform.IDENTITY))),
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)

        // Bucket is legal on any type; year/month/day/hour on date/ts/tstz.
        val ok =
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.SetPartitionSpec(
                        listOf(
                            PartitionFieldDef(nameId, Transform.BUCKET, 8),
                            PartitionFieldDef(tsId, Transform.HOUR),
                        ),
                    ),
                ),
            )
        assertThat(ok.partitionSpec!!.fields).hasSize(2)
    }

    // ---- in-order semantics ----------------------------------------------

    @Test
    fun `add then rename the new column in one request`() {
        val (cat, ns) = fixture()
        val info =
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.AddColumn(ColumnDef("tmp", ColType.STRING)),
                    AlterOp.RenameColumn("tmp", "final"),
                ),
            )
        assertThat(info.columns.map { it.def.name }).contains("final").doesNotContain("tmp")
        // Only one live row for the field; the tmp row was never visible.
        val fieldId = info.columns.single { it.def.name == "final" }.fieldId
        val rows =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT name FROM hog_column WHERE catalog_id = :c AND table_id = :t AND field_id = :f",
                ).bind("c", catId(cat)).bind("t", info.tableId).bind("f", fieldId)
                    .mapTo(String::class.java).list()
            }
        assertThat(rows).containsExactly("final")
    }

    @Test
    fun `drop then reference the dropped column fails`() {
        val (cat, ns) = fixture()
        assertThatThrownBy {
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.DropColumn("name"),
                    AlterOp.RenameColumn("name", "label"),
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
        // Rolled back: name is still there.
        assertThat(catalogs.getTable(cat, ns, "t").columns.map { it.def.name }).contains("name")
    }

    @Test
    fun `double rename in one request leaves one live row and clean history`() {
        val (cat, ns) = fixture()
        val before = head(cat)
        val fieldId = catalogs.getTable(cat, ns, "t").columns.single { it.def.name == "name" }.fieldId
        val info =
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.RenameColumn("name", "mid"),
                    AlterOp.RenameColumn("mid", "end"),
                ),
            )
        assertThat(info.columns.map { it.def.name }).contains("end")
        val rows =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT name, end_snapshot FROM hog_column " +
                        "WHERE catalog_id = :c AND table_id = :t AND field_id = :f ORDER BY begin_snapshot",
                ).bind("c", catId(cat)).bind("t", info.tableId).bind("f", fieldId).mapToMap().list()
            }
        // "mid" never persisted: only the original (ended) and "end" (live).
        assertThat(rows.map { it["name"] }).containsExactly("name", "end")
        assertThat(
            catalogs.getTable(cat, ns, "t", snapshot = before)
                .columns.single { it.fieldId == fieldId }.def.name,
        ).isEqualTo("name")
    }

    @Test
    fun `add then drop the same column in one request leaves no trace`() {
        val (cat, ns) = fixture()
        val info =
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.AddColumn(ColumnDef("ghost", ColType.STRING)),
                    AlterOp.DropColumn("ghost"),
                ),
            )
        assertThat(info.columns.map { it.def.name }).doesNotContain("ghost")
        val count =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT count(*) FROM hog_column " +
                        "WHERE catalog_id = :c AND table_id = :t AND name = 'ghost'",
                ).bind("c", catId(cat)).bind("t", info.tableId).mapTo(Long::class.javaObjectType).one()
            }
        assertThat(count).isEqualTo(0)
    }

    @Test
    fun `set spec twice in one request keeps only the final spec`() {
        val (cat, ns) = fixture()
        val cols = catalogs.getTable(cat, ns, "t").columns.associateBy { it.def.name }
        val info =
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(cols["ts"]!!.fieldId, Transform.DAY))),
                    AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(cols["id"]!!.fieldId, Transform.IDENTITY))),
                ),
            )
        assertThat(info.partitionSpec!!.fields)
            .containsExactly(PartitionFieldDef(cols["id"]!!.fieldId, Transform.IDENTITY))
        val specCount =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT count(*) FROM hog_partition_spec WHERE catalog_id = :c AND table_id = :t",
                ).bind("c", catId(cat)).bind("t", info.tableId).mapTo(Long::class.javaObjectType).one()
            }
        assertThat(specCount).isEqualTo(1)
    }

    // ---- rejections ------------------------------------------------------

    @Test
    fun `promotion lattice rejects everything but int-long and float-double`() {
        val (cat, ns) = fixture()
        val illegal =
            listOf(
                // int -> double
                "count" to ColType.DOUBLE,
                // no-op promotion
                "count" to ColType.INT,
                // narrowing long -> int
                "id" to ColType.INT,
                // float -> long
                "score" to ColType.LONG,
                // string -> anything
                "name" to ColType.LONG,
            )
        for ((col, target) in illegal) {
            assertThatThrownBy {
                alter.alterTable(cat, ns, "t", listOf(AlterOp.PromoteColumn(col, target)))
            }.isInstanceOf(HoglakeException.Validation::class.java)
        }
    }

    @Test
    fun `column op rejections - duplicate, missing, last column`() {
        val (cat, ns) = fixture()
        assertThatThrownBy {
            alter.alterTable(cat, ns, "t", listOf(AlterOp.AddColumn(ColumnDef("id", ColType.LONG))))
        }.isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy {
            alter.alterTable(cat, ns, "t", listOf(AlterOp.DropColumn("nope")))
        }.isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy {
            alter.alterTable(cat, ns, "t", listOf(AlterOp.RenameColumn("nope", "x")))
        }.isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy {
            alter.alterTable(cat, ns, "t", listOf(AlterOp.RenameColumn("name", "id")))
        }.isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy {
            alter.alterTable(cat, ns, "t", listOf(AlterOp.PromoteColumn("nope", ColType.LONG)))
        }.isInstanceOf(HoglakeException.Validation::class.java)

        // Last-column drop.
        catalogs.createTable(cat, ns, "solo", listOf(idCol))
        assertThatThrownBy {
            alter.alterTable(cat, ns, "solo", listOf(AlterOp.DropColumn("id")))
        }.isInstanceOf(HoglakeException.Validation::class.java)
    }

    @Test
    fun `rename table to an existing live name conflicts`() {
        val (cat, ns) = fixture()
        catalogs.createTable(cat, ns, "other", listOf(idCol))
        assertThatThrownBy {
            alter.alterTable(cat, ns, "t", listOf(AlterOp.RenameTable("other")))
        }.isInstanceOf(HoglakeException.AlreadyExists::class.java)
    }

    @Test
    fun `unknown catalog, namespace, table are NotFound and empty ops is Validation`() {
        val (cat, ns) = fixture()
        assertThatThrownBy {
            alter.alterTable("no-such", ns, "t", listOf(AlterOp.DropColumn("name")))
        }.isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy {
            alter.alterTable(cat, "no-such", "t", listOf(AlterOp.DropColumn("name")))
        }.isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy {
            alter.alterTable(cat, ns, "no-such", listOf(AlterOp.DropColumn("name")))
        }.isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy {
            alter.alterTable(cat, ns, "t", emptyList())
        }.isInstanceOf(HoglakeException.Validation::class.java)
    }

    // ---- commit bookkeeping ----------------------------------------------

    @Test
    fun `each alter bumps schema_version and records exactly one table_altered change`() {
        val (cat, ns) = fixture()
        val before = catalogs.getCatalog(cat)
        val info =
            alter.alterTable(
                cat,
                ns,
                "t",
                listOf(
                    AlterOp.AddColumn(ColumnDef("a", ColType.STRING)),
                    AlterOp.AddColumn(ColumnDef("b", ColType.STRING)),
                    AlterOp.RenameColumn("a", "c"),
                ),
            )
        val after = catalogs.getCatalog(cat)
        assertThat(after.headSnapshotId).isEqualTo(before.headSnapshotId + 1)
        assertThat(after.schemaVersion).isEqualTo(before.schemaVersion + 1)

        val (page, _) = catalogs.listSnapshots(cat, after = before.headSnapshotId, limit = 10)
        assertThat(page).hasSize(1)
        val snap = page.single()
        assertThat(snap.snapshotId).isEqualTo(after.headSnapshotId)
        assertThat(snap.schemaVersion).isEqualTo(after.schemaVersion)
        assertThat(snap.changes).hasSize(1)
        assertThat(snap.changes.single().kind).isEqualTo(ChangeKind.TABLE_ALTERED)
        assertThat(snap.changes.single().objectId).isEqualTo(info.tableId)
    }
}
