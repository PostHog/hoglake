package com.posthog.hoglake.testing

import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.withHandleUnchecked

/**
 * At-rest catalog invariants, as test assertions over a live database.
 *
 * WHY THIS EXISTS AND WHAT IT IS NOT. The verify subsystem (#261) ran
 * twelve of these checks as a production endpoint and a background loop,
 * and three compaction integration classes used it as an oracle —
 * `assertVerifyPasses(cat)` after every rewrite. Removing it took the
 * oracle with it. Most of that loss is correctly left to #261's paged
 * scrubber, because most of those arms were **unfalsifiable at every
 * call site**: each one built the service with production graces (24 h
 * retirement-orphan, 6 h staging-ticket, 1 h claim), so no fixture could
 * ever age a row past one, and others were vacuous against compaction
 * fixtures (the floor is 0, there are no offset or upload rows, and
 * `hog_delete_file_one_live_per_data_file` makes duplicate live DVs
 * unINSERTable).
 *
 * What is here is the set that was genuinely live, that compaction can
 * genuinely break, and that nothing else in the suite covers:
 *
 *  - [assertVisibilityBounds] — invariant 6. Uncovered anywhere else in
 *    the repo: the table CHECKs give `end > begin` and nothing else, so
 *    no test asserted any bound against the catalog's head.
 *  - [assertRemovalQueueUnreferenced] — invariant 4 at rest.
 *    `QeConcurrencyTortureTest` covers it for commit/expiry/cleanup but
 *    never runs compaction, and compaction is the subsystem that
 *    registers a path and settles a ledger row in ONE transaction (the
 *    #174 bug class).
 *  - [assertSnapshotsDense] — invariant 1, the one thing the per-catalog
 *    commit lock exists to protect.
 *  - [assertNoAbsentTicketOverLivePath] — `staging_tickets` arm (c), and
 *    the only reader the `'absent'` drain outcome has left (see
 *    `CleanupService.STAGING_REASON`).
 *
 * These are TEST SUPPORT, deliberately: unbounded `COUNT(*)`s over whole
 * manifests are exactly what #261 removed from production, and they are
 * legal here only because a fixture is thousands of rows. Nothing in
 * this file may be called from `src/main`, and the scrubber will not
 * reuse these shapes — it needs paged, resumable ones.
 */
object CatalogInvariants {
    /**
     * Invariant 6: a row is visible at S iff
     * `begin_snapshot <= S AND (end_snapshot IS NULL OR S < end_snapshot)`,
     * which is only meaningful if every row's bounds lie inside the
     * catalog's own snapshot range.
     *
     * One `COUNT(*)` per versioned table plus `hog_table`, which is the
     * identity row rather than a versioned row but carries the same shape
     * under other names (`created_snapshot` / `dropped_snapshot`).
     *
     * BOTH ENDS OF `[0, head]` ARE CHECKED, and the lower one is not
     * decoration: snapshot ids are non-negative by construction
     * (`last_snapshot_id` starts at 0 and the allocator only
     * increments), so a negative id is a row written by something that
     * bypassed the commit tail — direct SQL, a bad backfill, an overflow
     * — and `x <= head` is true of every negative value, so the upper
     * bound alone is blind to all of them. The KDoc promised `[0, head]`
     * while the predicate enforced only `<= head`; Copilot caught that
     * on #279.
     *
     * ONE LOWER BOUND PER TABLE SHAPE, not one per column, and the
     * omissions are derivations rather than oversights:
     *  - `end_snapshot < 0` is IMPLIED. Every versioned table carries
     *    `CHECK (end_snapshot IS NULL OR end_snapshot > begin_snapshot)`,
     *    so a negative end forces a negative begin under it, which the
     *    `begin_snapshot < 0` arm catches. With `begin >= 0` a negative
     *    end is not even INSERTable.
     *  - `dropped_snapshot < 0` is IMPLIED too, by a different route:
     *    `hog_table` has no such CHECK, but a negative dropped is either
     *    below a non-negative created (the `dropped < created` arm) or
     *    sits above a created that is itself negative (the
     *    `created_snapshot < 0` arm).
     * Both were in the first draft of this fix and were REMOVED after
     * `CatalogInvariantsIntegrationTest` proved them unfalsifiable: a
     * disjunct that cannot fire on its own is a claim of coverage that
     * does not exist, and it would mislead the next person reading for
     * what is actually enforced.
     *
     * `end_snapshot <= begin_snapshot` stays despite being unfalsifiable
     * for a different reason: it is backed by a CHECK rather than by
     * another arm here, so a migration that relaxed the CHECK would take
     * the invariant with it and this is the surface meant to notice.
     */
    fun assertVisibilityBounds(
        jdbi: Jdbi,
        catalog: String,
    ) {
        val offenders =
            jdbi.withHandleUnchecked { h ->
                VERSIONED_TABLES.flatMap { table ->
                    h.createQuery(
                        """
                        SELECT '$table' AS tbl, x.begin_snapshot, x.end_snapshot, c.last_snapshot_id
                        FROM $table x
                        JOIN hog_catalog c ON c.catalog_id = x.catalog_id
                        WHERE c.name = :cat
                          AND (x.begin_snapshot < 0
                               OR x.begin_snapshot > c.last_snapshot_id
                               OR (x.end_snapshot IS NOT NULL
                                   AND (x.end_snapshot <= x.begin_snapshot
                                        OR x.end_snapshot > c.last_snapshot_id)))
                        """,
                    ).bind("cat", catalog)
                        .map { rs, _ ->
                            "$table begin=${rs.getLong("begin_snapshot")} " +
                                "end=${rs.getObject("end_snapshot")} head=${rs.getLong("last_snapshot_id")}"
                        }.list()
                } +
                    h.createQuery(
                        """
                        SELECT t.table_id, t.created_snapshot, t.dropped_snapshot, c.last_snapshot_id
                        FROM hog_table t
                        JOIN hog_catalog c ON c.catalog_id = t.catalog_id
                        WHERE c.name = :cat
                          AND (t.created_snapshot < 0
                               OR t.created_snapshot > c.last_snapshot_id
                               OR (t.dropped_snapshot IS NOT NULL
                                   AND (t.dropped_snapshot < t.created_snapshot
                                        OR t.dropped_snapshot > c.last_snapshot_id)))
                        """,
                    ).bind("cat", catalog)
                        .map { rs, _ ->
                            "hog_table table_id=${rs.getLong("table_id")} " +
                                "created=${rs.getLong("created_snapshot")} " +
                                "dropped=${rs.getObject("dropped_snapshot")} " +
                                "head=${rs.getLong("last_snapshot_id")}"
                        }.list()
            }
        assertThat(offenders)
            .describedAs("invariant 6: versioned-row bounds outside [0, head] in catalog '%s'", catalog)
            .isEmpty()
    }

    /**
     * Invariant 4: physical deletion is never authorized by the queue.
     * An undrained `hog_file_removal` row whose path ANY file row still
     * claims is cleanup's `still_referenced` alert condition, observed at
     * rest instead of at drain time.
     *
     * NO LIVENESS FILTER, deliberately, and this is the one place the
     * rule reads backwards: a row that is end-snapshotted still FENCES
     * its object against physical deletion until retirement deletes the
     * row (AGENT.md invariant 11, the "path-keyed liveness" carve-out —
     * `CleanupService.referencedPaths` asks the same question the same
     * way). Adding `end_snapshot IS NULL` here would silently weaken the
     * check to a third of its reach and was the first version of this
     * helper; the production query it ports had no such filter either.
     *
     * A correlated `EXISTS`, which is the opposite of what the production
     * check used (a UNION of two hash semi-joins, because a correlated
     * form cannot be pulled up and went quadratic on a 14M-row manifest).
     * Here the correlation is the clearer statement of the rule and the
     * fixture is small — the production shape is the one that needed a
     * plan test, and it is gone with it.
     */
    fun assertRemovalQueueUnreferenced(
        jdbi: Jdbi,
        catalog: String,
    ) {
        val offenders =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT q.removal_id, q.path, q.reason
                    FROM hog_file_removal q
                    JOIN hog_catalog c ON c.catalog_id = q.catalog_id
                    WHERE c.name = :cat AND q.drained_at IS NULL
                      AND (EXISTS (SELECT 1 FROM hog_data_file f
                                    WHERE f.catalog_id = q.catalog_id AND f.path = q.path)
                           OR EXISTS (SELECT 1 FROM hog_delete_file d
                                       WHERE d.catalog_id = q.catalog_id AND d.path = q.path))
                    ORDER BY q.removal_id
                    """,
                ).bind("cat", catalog)
                    .map { rs, _ ->
                        "removal_id=${rs.getLong("removal_id")} reason=${rs.getString("reason")} " +
                            "path='${rs.getString("path")}' is queued for deletion but a file row still names it"
                    }.list()
            }
        assertThat(offenders)
            .describedAs("invariant 4: queued-but-live paths in catalog '%s'", catalog)
            .isEmpty()
    }

    /**
     * `staging_tickets` arm (c): a `compaction_staging` ticket drained
     * `'absent'` — "this object never existed" — over a path the catalog
     * holds a file row for. That is the staged-output race resolved the
     * wrong way, and the removal ledger is the ONLY place it is visible:
     * the file row looks perfectly ordinary.
     *
     * This assertion is also what keeps the `'absent'` outcome, and the
     * HEAD-before-DELETE carve-out that produces it, from being a value
     * nothing reads at all.
     */
    fun assertNoAbsentTicketOverLivePath(
        jdbi: Jdbi,
        catalog: String,
    ) {
        val offenders =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT q.removal_id, q.path
                    FROM hog_file_removal q
                    JOIN hog_catalog c ON c.catalog_id = q.catalog_id
                    WHERE c.name = :cat AND q.reason = 'compaction_staging'
                      AND q.drained_outcome = 'absent'
                      AND (EXISTS (SELECT 1 FROM hog_data_file f
                                    WHERE f.catalog_id = q.catalog_id AND f.path = q.path)
                           OR EXISTS (SELECT 1 FROM hog_delete_file d
                                       WHERE d.catalog_id = q.catalog_id AND d.path = q.path))
                    ORDER BY q.removal_id
                    """,
                ).bind("cat", catalog)
                    .map { rs, _ ->
                        "removal_id=${rs.getLong("removal_id")} path='${rs.getString("path")}' " +
                            "was drained 'absent' but the catalog holds a file row for that path"
                    }.list()
            }
        assertThat(offenders)
            .describedAs("staging ticket settled 'absent' over a registered path in '%s'", catalog)
            .isEmpty()
    }

    /**
     * Invariant 1: snapshot ids are dense per catalog, so the retained
     * set is exactly `[earliest_snapshot_id, last_snapshot_id]` —
     * `count(*)` equals `head - earliest + 1`, and the two ends are the
     * min and the max.
     *
     * Un-joined and `findOne`-free on purpose: a catalog with no snapshot
     * rows at all must produce a readable failure rather than an
     * `IllegalStateException` out of the mapper, which is what an INNER
     * JOIN with `GROUP BY` and `.one()` does.
     */
    fun assertSnapshotsDense(
        jdbi: Jdbi,
        catalog: String,
    ) {
        val (bounds, density) =
            jdbi.withHandleUnchecked { h ->
                val bounds =
                    h.createQuery(
                        "SELECT catalog_id, earliest_snapshot_id, last_snapshot_id " +
                            "FROM hog_catalog WHERE name = :cat",
                    ).bind("cat", catalog)
                        .map { rs, _ ->
                            Triple(
                                rs.getLong("catalog_id"),
                                rs.getLong("earliest_snapshot_id"),
                                rs.getLong("last_snapshot_id"),
                            )
                        }.findOne().orElseThrow { AssertionError("no catalog named '$catalog'") }
                val density =
                    h.createQuery(
                        "SELECT count(*) AS n, min(snapshot_id) AS lo, max(snapshot_id) AS hi " +
                            "FROM hog_snapshot WHERE catalog_id = :id",
                    ).bind("id", bounds.first)
                        .map { rs, _ -> Triple(rs.getLong("n"), rs.getLong("lo"), rs.getLong("hi")) }
                        .one()
                bounds to density
            }
        val (_, earliest, head) = bounds
        val (count, min, max) = density
        assertThat(count)
            .describedAs(
                "invariant 1: '%s' holds %d snapshot row(s) but head(%d) - earliest(%d) + 1 = %d",
                catalog,
                count,
                head,
                earliest,
                head - earliest + 1,
            ).isEqualTo(head - earliest + 1)
        // Guarded like the original: on an empty hog_snapshot the
        // aggregates are SQL NULL, which getLong reads as 0, and
        // asserting 0 == earliest would be an accident rather than a check.
        if (count > 0) {
            assertThat(min).describedAs("min(snapshot_id) != earliest_snapshot_id in '%s'", catalog)
                .isEqualTo(earliest)
            assertThat(max).describedAs("max(snapshot_id) != head in '%s'", catalog).isEqualTo(head)
        }
    }

    /**
     * The versioned tables, mirroring
     * `ExpiryService.VERSIONED_RETENTION_TABLES` plus the two the sweep
     * clears by their own object-queueing path. `ExpiryServiceIntegration
     * Test.VERSIONED_RETENTION_TABLES is every versioned table the schema
     * has` is what keeps this list honest against a migration.
     */
    private val VERSIONED_TABLES =
        listOf(
            "hog_data_file",
            "hog_delete_file",
            "hog_table_version",
            "hog_column",
            "hog_partition_spec",
            "hog_sort_spec",
            "hog_view",
        )
}
