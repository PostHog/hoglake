package com.posthog.hoglake.service

import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.TableInfo
import java.util.UUID

/**
 * The ALTER seam. The Postgres implementation is [AlterService]; the
 * hoglite backend's is [com.posthog.hoglake.hoglite.HogliteAlters]. The
 * op semantics — including the guarded variant's
 * `DdlSinceReadSnapshot` — are documented on [AlterService].
 */
interface AlterBackend {
    fun alterTable(
        catalog: String,
        namespace: String,
        table: String,
        ops: List<AlterOp>,
        expectedTableUuid: UUID? = null,
        readSnapshot: Long? = null,
        author: String? = null,
        message: String? = null,
    ): TableInfo
}
