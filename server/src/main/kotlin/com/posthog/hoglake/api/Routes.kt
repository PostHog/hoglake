package com.posthog.hoglake.api

import com.posthog.hoglake.BuildInfo
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.observability.CatalogTotals
import com.posthog.hoglake.observability.InstanceTotals
import com.posthog.hoglake.persistence.FileRepo
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.ColumnTrees
import com.posthog.hoglake.service.Identifiers
import com.posthog.hoglake.service.ScanService
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.plugins.BadRequestException
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.delete
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.put
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import java.time.Instant
import java.time.format.DateTimeParseException
import java.util.UUID

/**
 * The /v1 routes, one-to-one with openapi/hoglake.yaml. Handlers only
 * translate wire <-> model and pick status codes; behavior (locking,
 * validation, conflict detection) lives in CatalogService/CommitService,
 * whose HoglakeExceptions are mapped by ErrorMapping.
 */
fun Application.installApiRoutes(
    catalogs: CatalogService,
    commits: CommitService,
    instanceName: String = "",
    instanceTotals: () -> InstanceTotals? = { null },
    catalogTotals: () -> Map<String, CatalogTotals> = { emptyMap() },
) {
    routing {
        get("/v1/info") {
            // Totals come from the metrics sampler's last pass — never
            // computed here (a live-manifest sum per request would tax
            // the commit tail's RDS at fleet scale). Null (fields
            // omitted) only in the boot window before the first sample.
            val totals = instanceTotals()
            call.respond(
                InstanceInfoDto(
                    name = instanceName.ifBlank { null },
                    version = BuildInfo.version,
                    build = BuildInfo.buildStamp,
                    totalRows = totals?.totalRows,
                    totalSizeBytes = totals?.totalSizeBytes,
                ),
            )
        }
        route("/v1/catalogs") {
            get {
                // Totals ride the sampler's last pass (catalogTotals), so
                // listing catalogs never sums the manifest per request.
                val totals = catalogTotals()
                call.respond(catalogs.listCatalogs().map { it.toDto(totals[it.name]) })
            }
            post {
                val req = call.receive<CreateCatalogRequestDto>()
                call.respond(
                    HttpStatusCode.Created,
                    catalogs.createCatalog(req.name, req.dataPath).toDto(),
                )
            }

            route("/{catalog}") {
                get {
                    call.respond(catalogs.getCatalog(call.catalog()).toDto())
                }

                route("/namespaces") {
                    get {
                        call.respond(catalogs.listNamespaces(call.catalog()).map { it.toDto() })
                    }
                    post {
                        val req = call.receive<CreateNamespaceRequestDto>()
                        call.respond(
                            HttpStatusCode.Created,
                            catalogs.createNamespace(call.catalog(), req.name).toDto(),
                        )
                    }
                    get("/{namespace}") {
                        call.respond(
                            catalogs.getNamespace(call.catalog(), call.namespace()).toDto(),
                        )
                    }
                    delete("/{namespace}") {
                        call.respond(
                            catalogs.dropNamespace(
                                call.catalog(),
                                call.namespace(),
                                call.longQuery("expected_namespace_id"),
                            ).toDto(),
                        )
                    }

                    route("/{namespace}/tables") {
                        get {
                            call.respond(
                                catalogs.listTables(call.catalog(), call.namespace())
                                    .map { it.toDto() },
                            )
                        }
                        post {
                            val req = call.receive<CreateTableRequestDto>()
                            call.respond(
                                HttpStatusCode.Created,
                                catalogs.createTable(
                                    call.catalog(),
                                    call.namespace(),
                                    req.name,
                                    req.columns.map { it.toModel() },
                                ).toDto(),
                            )
                        }

                        route("/{table}") {
                            get {
                                call.respond(
                                    catalogs.getTable(
                                        call.catalog(),
                                        call.namespace(),
                                        call.table(),
                                        call.longQuery("snapshot"),
                                        call.instantQuery("at_timestamp"),
                                    ).toDto(),
                                )
                            }
                            delete {
                                call.respond(
                                    catalogs.dropTable(
                                        call.catalog(),
                                        call.namespace(),
                                        call.table(),
                                        call.expectedTableUuid(),
                                    ).toDto(),
                                )
                            }
                            post("/truncate") {
                                val expected =
                                    call.expectedTableUuid()
                                        ?: throw BadRequestException("expected_table_uuid is required")
                                call.respond(
                                    catalogs.truncateTable(
                                        call.catalog(),
                                        call.namespace(),
                                        call.table(),
                                        expected,
                                    ).toDto(),
                                )
                            }
                            get("/files") {
                                val sort =
                                    call.request.queryParameters["sort"]?.let {
                                        FileRepo.FileSortColumn.fromWire(it)
                                            ?: throw BadRequestException(
                                                "query parameter 'sort' must be one of " +
                                                    FileRepo.FileSortColumn.entries.joinToString(", ") { c -> c.wire } +
                                                    ", got '$it'",
                                            )
                                    }
                                val desc =
                                    when (val o = call.request.queryParameters["order"]) {
                                        null, "asc" -> false
                                        "desc" -> true
                                        else ->
                                            throw BadRequestException(
                                                "query parameter 'order' must be 'asc' or 'desc', got '$o'",
                                            )
                                    }
                                // No 'limit' -> unbounded, the historical
                                // behavior every non-webui caller and the
                                // existing tests rely on. The webui always
                                // sends one.
                                val limit =
                                    call.intQuery("limit")?.also {
                                        if (it < 1) {
                                            throw BadRequestException(
                                                "query parameter 'limit' must be positive, got '$it'",
                                            )
                                        }
                                    }?.coerceAtMost(FILES_MAX_LIMIT)
                                val offset =
                                    call.intQuery("offset")?.also {
                                        if (it < 0) {
                                            throw BadRequestException(
                                                "query parameter 'offset' must be >= 0, got '$it'",
                                            )
                                        }
                                    } ?: 0
                                // partition=key_index:value, repeatable. The
                                // value is the stored, transformed string the
                                // caller got from /partitions/values, so the
                                // match is plain string equality — the server
                                // never encodes. Multiple params AND together.
                                val partitionFilter =
                                    call.request.queryParameters.getAll("partition")
                                        ?.associate { param ->
                                            val (k, v) =
                                                param.split(":", limit = 2).also {
                                                    if (it.size != 2) {
                                                        throw BadRequestException(
                                                            "query parameter 'partition' must be " +
                                                                "key_index:value, got '$param'",
                                                        )
                                                    }
                                                }
                                            val keyIndex =
                                                k.toIntOrNull()
                                                    ?: throw BadRequestException(
                                                        "partition key_index must be a non-negative integer, got '$k'",
                                                    )
                                            if (keyIndex < 0) {
                                                throw BadRequestException(
                                                    "partition key_index must be a non-negative integer, got '$k'",
                                                )
                                            }
                                            keyIndex to v
                                        } ?: emptyMap()
                                call.respond(
                                    catalogs.listFiles(
                                        call.catalog(),
                                        call.namespace(),
                                        call.table(),
                                        call.longQuery("snapshot"),
                                        call.instantQuery("at_timestamp"),
                                        sort = sort,
                                        desc = desc,
                                        limit = limit,
                                        offset = offset,
                                        partitionFilter = partitionFilter,
                                    ).map { it.toDto() },
                                )
                            }
                            get("/partitions/values") {
                                call.respond(
                                    catalogs.partitionValues(
                                        call.catalog(),
                                        call.namespace(),
                                        call.table(),
                                        call.longQuery("snapshot"),
                                        call.instantQuery("at_timestamp"),
                                    ).toDto(),
                                )
                            }
                            get("/files/{fileId}/stats") {
                                call.respond(
                                    catalogs.fileStats(
                                        call.catalog(),
                                        call.namespace(),
                                        call.table(),
                                        call.longPath("fileId"),
                                        call.longQuery("snapshot"),
                                        call.instantQuery("at_timestamp"),
                                    ).toDto(),
                                )
                            }
                            get("/changes") {
                                val from =
                                    call.longQuery("from_snapshot")
                                        ?: throw BadRequestException(
                                            "missing required query parameter 'from_snapshot'",
                                        )
                                call.respond(
                                    catalogs.changes(
                                        call.catalog(),
                                        call.namespace(),
                                        call.table(),
                                        from,
                                        call.longQuery("to_snapshot"),
                                    ).toDto(),
                                )
                            }
                        }
                    }
                }

                get("/snapshots") {
                    val after = call.longQuery("after") ?: 0L
                    val before = call.longQuery("before")
                    val limit = (call.intQuery("limit") ?: 1000).coerceAtMost(10_000)
                    val (page, hasMore) =
                        catalogs.listSnapshots(call.catalog(), after, limit, before)
                    call.respond(SnapshotPageDto(page.map { it.toDto() }, hasMore))
                }

                // Dedicated endpoint prevents old servers silently ignoring the
                // new request key and degrading retries to at-least-once.
                post("/commit/prepared") {
                    val req = call.receive<CommitRequestDto>()
                    if (req.idempotencyKey == null) {
                        throw com.posthog.hoglake.model.HoglakeException.Validation("idempotency_key is required")
                    }
                    call.respond(commits.commit(call.catalog(), req.toModel()).toDto())
                }

                // Distinct endpoint fences replicas that predate the DELETE contract.
                post("/commit/deletes/prepared") {
                    val req = call.receive<CommitRequestDto>()
                    if (req.idempotencyKey == null || req.readSnapshot == null || req.appends.isNotEmpty() ||
                        req.deletes.isEmpty() || req.deletes.any { it.expectedTableUuid == null }
                    ) {
                        throw com.posthog.hoglake.model.HoglakeException.Validation(
                            "prepared DELETE requires idempotency_key, read_snapshot, guarded deletes and no appends",
                        )
                    }
                    call.respond(commits.commit(call.catalog(), req.toModel(allowEmptyDeletes = true)).toDto())
                }

                // A separate endpoint fences old replicas and includes the stronger
                // conflict contract in the durable full-payload receipt.
                post("/commit/mutations/prepared") {
                    val req = call.receive<CommitRequestDto>()
                    if (req.idempotencyKey == null || req.readSnapshot == null || req.deletes.isEmpty() ||
                        req.deletes.any { it.expectedTableUuid == null } ||
                        req.appends.any { it.expectedTableUuid == null }
                    ) {
                        throw com.posthog.hoglake.model.HoglakeException.Validation(
                            "prepared mutation requires idempotency_key, read_snapshot and guarded target tables",
                        )
                    }
                    call.respond(
                        commits.commit(
                            call.catalog(),
                            req.toModel(allowEmptyDeletes = true).copy(requireUnchangedTables = true),
                        ).toDto(),
                    )
                }

                post("/commit/uploads") {
                    val req = call.receive<CommitRequestDto>()
                    if (req.idempotencyKey == null || req.readSnapshot == null ||
                        req.appends.any { it.expectedTableUuid == null } ||
                        req.deletes.any { it.expectedTableUuid == null }
                    ) {
                        throw com.posthog.hoglake.model.HoglakeException.Validation(
                            "claimed uploads require a guarded idempotent commit",
                        )
                    }
                    call.respond(
                        commits.commit(
                            call.catalog(),
                            req.toModel(allowEmptyDeletes = true)
                                .copy(requireUnchangedTables = req.deletes.isNotEmpty()),
                        ).toDto(),
                    )
                }

                // requireUnchangedTables is unconditional here because the
                // transaction contract demands it, but it binds only the
                // DELETE targets (CommitService.checkConflicts): an
                // append-only transaction keeps the ordinary DDL-only
                // conflict rule and does not 409 on a concurrent INSERT.
                post("/commit/transaction") {
                    val req = call.receive<CommitRequestDto>()
                    if (req.idempotencyKey == null || req.readSnapshot == null ||
                        req.appends.any { it.expectedTableUuid == null } ||
                        req.deletes.any { it.expectedTableUuid == null }
                    ) {
                        throw com.posthog.hoglake.model.HoglakeException.Validation(
                            "transaction requires idempotency_key, read_snapshot and guarded targets",
                        )
                    }
                    call.respond(
                        commits.commit(
                            call.catalog(),
                            req.toModel(allowEmptyDeletes = true)
                                .copy(requireUnchangedTables = true, allowPendingDeletes = true),
                        ).toDto(),
                    )
                }

                get("/commit/receipts/{operation}") {
                    val operation =
                        try {
                            UUID.fromString(call.parameters["operation"])
                        } catch (_: IllegalArgumentException) {
                            throw BadRequestException("invalid operation UUID")
                        }
                    val receipt = commits.receipt(call.catalog(), operation)
                    call.respond(CommitReceiptDto(operation, receipt.snapshotId, receipt.schemaVersion))
                }

                post("/commit") {
                    val req = call.receive<CommitRequestDto>()
                    call.respond(commits.commit(call.catalog(), req.toModel()).toDto())
                }

                get("/consumers") {
                    call.respond(
                        catalogs.listConsumers(call.catalog()).toConsumerListDto(),
                    )
                }

                route("/consumers/{consumer}/offsets") {
                    get {
                        call.respond(
                            catalogs.listOffsets(call.catalog(), call.consumer())
                                .map { it.toDto() },
                        )
                    }
                    get("/{tableUuid}") {
                        call.respond(
                            catalogs.getOffset(
                                call.catalog(),
                                call.consumer(),
                                call.uuidPath("tableUuid"),
                            ).toDto(),
                        )
                    }
                    put("/{tableUuid}") {
                        val tableUuid = call.uuidPath("tableUuid")
                        val req = call.receive<CommitOffsetRequestDto>()
                        call.respond(
                            catalogs.commitOffset(
                                call.catalog(),
                                call.consumer(),
                                tableUuid,
                                req.snapshotId,
                            ).toDto(),
                        )
                    }
                }
            }
        }
    }
}

/**
 * GET .../tables/{table}/scan?snapshot= — read planning (openapi
 * planScan): data files paired with their visible deletion vectors, and
 * with `include=column_stats` each provided file's column statistics
 * (narrowed by `stats_fields`). Installed separately so App.kt wires it
 * with its own ScanService.
 */
fun Application.installScanRoutes(scan: ScanService) {
    routing {
        get("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/scan") {
            // getAll, joined: Parameters[...] is only the FIRST occurrence,
            // so a repeated `stats_fields=3&stats_fields=7` would silently
            // drop field 7's bounds and a second, misspelt `include` would
            // be ignored rather than refused.
            fun joined(name: String) = call.request.queryParameters.getAll(name)?.joinToString(",")
            call.respond(
                scan.planScan(
                    call.catalog(),
                    call.namespace(),
                    call.table(),
                    call.longQuery("snapshot"),
                    call.instantQuery("at_timestamp"),
                    parseScanStatsRequest(joined("include"), joined("stats_fields")),
                ).map { it.toDto() },
            )
        }
    }
}

/** The `include` values GET .../scan understands. */
internal val SCAN_INCLUDES = setOf("column_stats")

/**
 * Most DISTINCT values `include` may name.
 *
 * The parameter names optional PARTS of the plan and there is one of
 * them, so sixteen is already an order of magnitude of headroom. The
 * cap exists because the unknown-value refusal ECHOES what it did not
 * recognise: without a bound, sixteen hundred misspellings produce
 * sixteen hundred quoted tokens in a 422 body. [Identifiers.cap]
 * bounds each token's LENGTH; this bounds their COUNT, and a message
 * needs both.
 *
 * DISTINCT is the whole of it. `explode: true` makes repetition the
 * CANONICAL encoding of this parameter — `include=column_stats&include=column_stats`
 * is what a generated client emits for a two-element list, and the
 * route joins occurrences before the parse — so a cap counting raw
 * tokens refuses a request that names one legal value seventeen times.
 * The count that can hurt a reader is how many DIFFERENT values come
 * back in the message, which is what this bounds. [MAX_RAW_SCAN_TOKENS]
 * is the separate, generous guard on the work done BEFORE the dedupe.
 */
internal const val MAX_SCAN_INCLUDES = 16

/**
 * Most field ids `stats_fields` may name.
 *
 * [ColumnTrees.MAX_COLUMN_NODES] itself, not a copy of its value: a
 * request may legitimately name every column node a table is allowed to
 * have, and nothing beyond that can resolve to anything. Restating the
 * number would let the two drift apart silently, with the parser
 * refusing ids the DDL had just allowed.
 */
internal const val MAX_STATS_FIELDS = ColumnTrees.MAX_COLUMN_NODES

/**
 * The raw-token guard both parameters share, applied BEFORE the
 * dedupe: ten times the larger of the two distinct-value caps.
 *
 * The caps above are on DISTINCT values, which is the number that
 * reaches a message or a query, and a distinct count cannot be taken
 * without building the set first. This bounds that work. Ten times, and
 * not the cap itself, because repetition is the canonical encoding
 * under `explode: true`: a client sending each of ten thousand field
 * ids as its own occurrence is well behaved, and even a client that
 * sends each of them twice is only careless. A hundred thousand tokens
 * is nobody's list.
 */
internal const val MAX_RAW_SCAN_TOKENS = 10 * ColumnTrees.MAX_COLUMN_NODES

/**
 * `include` and `stats_fields` as a scan's stats request, or null when
 * none was asked for.
 *
 * THE LINE BETWEEN 400 AND 422 is whether the VALUE is well formed.
 * Malformed values are 400s (BadRequestException) on both parameters
 * and for the same reasons: an empty token, a non-integer field id, too
 * many of either. Well-formed but unusable ones are 422s (Validation):
 * an `include` value the server does not know — refused rather than
 * ignored, so a caller misspelling it cannot mistake "no stats" for
 * "nothing to prune" — and `stats_fields` without `include=column_stats`,
 * which would otherwise silently return no statistics at all.
 *
 * `include=` and `stats_fields=` therefore answer the SAME status. They
 * did not: an empty `include` used to reach the unknown-value arm and
 * answer 422 while an empty `stats_fields` answered 400, so one caller
 * sending both empty got two different verdicts on one mistake.
 *
 * Both parameters UNION across repeated occurrences (the route joins
 * them with commas before calling this), so `stats_fields=3&stats_fields=7`
 * asks for both columns rather than silently dropping the second.
 */
internal fun parseScanStatsRequest(
    include: String?,
    statsFields: String?,
): ScanService.ColumnStatsRequest? {
    val includes = include?.let { parseScanIncludes(it) } ?: emptySet()
    val fieldIds = statsFields?.let { parseStatsFields(it) }
    if ("column_stats" !in includes) {
        if (fieldIds != null) {
            throw HoglakeException.Validation("stats_fields requires include=column_stats")
        }
        return null
    }
    return ScanService.ColumnStatsRequest(fieldIds)
}

/**
 * Split one of the two parameters into raw tokens, bounded by
 * [MAX_RAW_SCAN_TOKENS].
 *
 * The guard is on the SPLIT, before anything is trimmed or deduped,
 * because that is the only work whose size the caller controls
 * directly. Everything after it counts distinct values.
 */
private fun scanTokens(
    name: String,
    raw: String,
): List<String> {
    val parts = raw.split(',')
    if (parts.size > MAX_RAW_SCAN_TOKENS) {
        throw BadRequestException(
            "query parameter '$name' carries more than $MAX_RAW_SCAN_TOKENS values",
        )
    }
    return parts
}

/** `include`: a comma-separated list of non-empty known part names. */
internal fun parseScanIncludes(raw: String): Set<String> {
    val values = scanTokens("include", raw).map { it.trim() }.toSet()
    if (values.any { it.isEmpty() }) {
        throw BadRequestException(
            "query parameter 'include' must be a comma-separated list of non-empty values; " +
                "supported: ${SCAN_INCLUDES.sorted().joinToString()}",
        )
    }
    // DISTINCT values, after the dedupe: under `explode: true` a client
    // repeating one legal value is sending a list of one, and refusing
    // it would refuse the parameter's own canonical encoding.
    if (values.size > MAX_SCAN_INCLUDES) {
        throw BadRequestException("query parameter 'include' names more than $MAX_SCAN_INCLUDES distinct values")
    }
    val unknown = values - SCAN_INCLUDES
    if (unknown.isNotEmpty()) {
        throw HoglakeException.Validation(
            "include: unknown value(s) ${unknown.sorted().joinToString { "'${Identifiers.cap(it)}'" }}; " +
                "supported: ${SCAN_INCLUDES.sorted().joinToString()}",
        )
    }
    return values
}

/** `stats_fields`: a non-empty comma-separated list of int64 field ids. */
internal fun parseStatsFields(raw: String): Set<Long> {
    val ids =
        scanTokens("stats_fields", raw).map { part ->
            part.trim().toLongOrNull()
                ?: throw BadRequestException(
                    "query parameter 'stats_fields' must be a comma-separated list of integer field ids",
                )
        }.toSet()
    // Again DISTINCT: the cap is [ColumnTrees.MAX_COLUMN_NODES] because
    // that is how many field ids a legal table can HAVE, and a caller
    // naming one id ten thousand times has named one column.
    if (ids.size > MAX_STATS_FIELDS) {
        throw BadRequestException(
            "query parameter 'stats_fields' names more than $MAX_STATS_FIELDS distinct field ids",
        )
    }
    return ids
}

// ---- parameter helpers ---------------------------------------------------
// internal (not private): ViewRoutes.kt shares them.

internal fun ApplicationCall.pathParam(name: String): String =
    parameters[name] ?: throw BadRequestException("missing path parameter '$name'")

internal fun ApplicationCall.catalog() = pathParam("catalog")

internal fun ApplicationCall.namespace() = pathParam("namespace")

private fun ApplicationCall.table() = pathParam("table")

private fun ApplicationCall.consumer() = pathParam("consumer")

private fun ApplicationCall.longPath(name: String): Long {
    val raw = pathParam(name)
    return raw.toLongOrNull()
        ?: throw BadRequestException("path parameter '$name' must be an integer, got '$raw'")
}

private fun ApplicationCall.uuidPath(name: String): UUID {
    val raw = pathParam(name)
    return try {
        UUID.fromString(raw)
    } catch (_: IllegalArgumentException) {
        throw BadRequestException("path parameter '$name' must be a UUID, got '$raw'")
    }
}

/** Hard cap on a single file page, matching the snapshots endpoint's cap. */
private const val FILES_MAX_LIMIT = 10_000

internal fun ApplicationCall.longQuery(name: String): Long? =
    request.queryParameters[name]?.let { parseLongQuery(name, it) }

internal fun parseLongQuery(
    name: String,
    raw: String,
): Long = raw.toLongOrNull() ?: throw BadRequestException("query parameter '$name' must be an integer")

private fun ApplicationCall.intQuery(name: String): Int? =
    request.queryParameters[name]?.let {
        it.toIntOrNull()
            ?: throw BadRequestException("query parameter '$name' must be an integer, got '$it'")
    }

/** ISO-8601 instant query parameter (e.g. 2026-09-04T12:00:00Z); unparseable -> 400. */
private fun ApplicationCall.instantQuery(name: String): Instant? =
    request.queryParameters[name]?.let {
        try {
            Instant.parse(it)
        } catch (_: DateTimeParseException) {
            throw BadRequestException(
                "query parameter '$name' must be an ISO-8601 instant, got '$it'",
            )
        }
    }

/** Optional on legacy DDL endpoints; mandatory for truncate. */
internal fun ApplicationCall.expectedTableUuid(): UUID? {
    val raw = request.queryParameters["expected_table_uuid"] ?: return null
    return parseExpectedTableUuid(raw)
}

internal fun parseExpectedTableUuid(raw: String): UUID =
    try {
        UUID.fromString(raw)
    } catch (_: IllegalArgumentException) {
        throw BadRequestException("expected_table_uuid must be a UUID")
    }
