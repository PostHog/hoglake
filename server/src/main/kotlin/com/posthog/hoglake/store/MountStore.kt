package com.posthog.hoglake.store

import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.persistence.getBigintListOrNull
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.jdbi.v3.core.mapper.RowMapper

/**
 * `hog_mount` (V35), the mounts of this instance's PostgreSQL. Only
 * `PUT /v1/mounts/{name}` writes it (hogtower's events mount loop, plan 3.5),
 * with CAS on the revision the writer read. A row is never deleted, so a
 * revision never resets.
 *
 * As the [MountRegistry], it serves a mount only while it is enabled and
 * [admitted] (HOGLAKE_MOUNT_ALLOWLIST). A name that is not admitted never
 * reads the database.
 */
class MountStore(
    private val jdbi: () -> Jdbi,
    private val admitted: (String) -> Boolean,
) : MountRegistry {
    override fun find(name: String): MountRecord? =
        if (admitted(name)) jdbi().withHandleUnchecked { read(it, name) }?.takeIf { it.enabled } else null

    /** [name] as written, or the defaults of a mount never written. */
    fun get(name: String): MountRecord {
        requireName(name)
        return jdbi().withHandleUnchecked { read(it, name) } ?: MountRecord(name)
    }

    /** Every mount, enabled or not, by name. */
    fun list(): List<MountRecord> =
        jdbi().withHandleUnchecked { it.createQuery("SELECT $COLUMNS FROM hog_mount ORDER BY name").map(MAPPER).list() }

    /** A write: the mount before it (null when never written) and after it. */
    data class Write(
        val before: MountRecord?,
        val after: MountRecord,
    )

    /**
     * Write [name] at [revision] + 1. It must be at [revision] now (0: never
     * written), or [HoglakeException.MountRevisionConflict]. A name that any
     * `hog_catalog` row holds is refused: catalogs have no dropped state, so
     * every row counts. No lock spans the two, and none is needed: a catalog
     * can no longer be created under a reserved name (`MountNames`).
     */
    fun put(
        name: String,
        revision: Long,
        enabled: Boolean,
        values: Collection<Long>,
    ): Write {
        requireName(name)
        if (revision < 0) throw HoglakeException.Validation("revision must be 0 or more, got $revision")
        if (values.size > MAX_VALUES) {
            throw HoglakeException.Validation("a mount holds at most $MAX_VALUES values, got ${values.size}")
        }
        values.firstOrNull { it < 0 }?.let { throw HoglakeException.Validation("mount value $it is negative") }
        val sorted = values.sorted()
        sorted.zipWithNext().firstOrNull { (a, b) -> a == b }?.let {
            throw HoglakeException.Validation("mount value ${it.first} is listed twice")
        }
        return jdbi().inTransactionUnchecked { h ->
            if (CatalogRepo.findByName(h, name) != null) {
                throw HoglakeException.Validation("'$name' is a catalog's name, so it cannot be a mount")
            }
            // Unlocked: a revision is never repeated, so if the guarded
            // write below lands, the row was still [before] when it did.
            val before = read(h, name)
            if ((before?.revision ?: 0) != revision) throw conflict(name, before?.revision ?: 0, revision)
            // The guard is the CAS: a writer racing from the same revision
            // waits on the winner's row (or its unique entry, for a first
            // write), then finds it moved and writes nothing.
            val written =
                h.createUpdate(
                    """
                    INSERT INTO hog_mount (name, revision, enabled, mount_values)
                    VALUES (:name, :next, :enabled, :values)
                    ON CONFLICT (name) DO UPDATE SET revision = EXCLUDED.revision,
                        enabled = EXCLUDED.enabled, mount_values = EXCLUDED.mount_values
                    WHERE hog_mount.revision = :revision
                    """,
                ).bind("name", name).bind("next", revision + 1).bind("revision", revision)
                    .bind("enabled", enabled).bindArray("values", Long::class.javaObjectType, sorted)
                    .execute()
            if (written != 1) throw conflict(name, null, revision)
            Write(before, MountRecord(name, revision + 1, enabled, sorted))
        }
    }

    companion object {
        /** Four times the largest org's teams (2,353); a token lists at most 200. */
        const val MAX_VALUES = 10_000

        private const val COLUMNS = "name, revision, enabled, mount_values"

        private val MAPPER =
            RowMapper { rs, _ ->
                MountRecord(
                    rs.getString("name"),
                    rs.getLong("revision"),
                    rs.getBoolean("enabled"),
                    rs.getBigintListOrNull("mount_values")!!,
                )
            }

        private fun read(
            h: Handle,
            name: String,
        ): MountRecord? =
            h.createQuery("SELECT $COLUMNS FROM hog_mount WHERE name = :name")
                .bind("name", name)
                .map(MAPPER)
                .findOne()
                .orElse(null)

        private fun requireName(name: String) {
            if (!MountNames.isReserved(name)) {
                throw HoglakeException.Validation("'$name' is not a mount name (<lowercase uuid>-events)")
            }
        }

        private fun conflict(
            name: String,
            at: Long?,
            revision: Long,
        ) = HoglakeException.MountRevisionConflict(
            "mount '$name' is not at revision $revision" + (at?.let { " but at $it" } ?: "") + "; read it again",
        )
    }
}
