package com.posthog.hoglake.auth

import com.posthog.hoglake.store.MountNames
import io.ktor.http.HttpMethod
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * The route table: that it covers every route the server publishes, and that the
 * decision it makes for each of the three caller shapes is the documented one.
 *
 * The totality assertion is the reason this class exists. A table that is merely
 * *correct today* rots the first time someone adds a route, and the rot is
 * silent — the route is simply denied in `enforce`, which looks like a client
 * bug. Pinning the table against `openapi/hoglake.yaml` makes the omission a red
 * build instead.
 */
class AuthPolicyTest {
    @Test
    fun `no rule can shadow another rule on the same path`() {
        // `match` returns the FIRST rule that fits, so an earlier, weaker rule
        // that can also fit a path silently becomes that path's policy. The table
        // is ordered by length and then by literal count, which makes this
        // impossible for any pair that differs in specificity — but ordering is a
        // property of the sort, and the sort is a line somebody can change. This
        // asserts the property itself.
        val rules = AuthPolicy.rules
        val ambiguous = mutableListOf<String>()

        for (i in rules.indices) {
            for (j in i + 1 until rules.size) {
                val first = rules[i]
                val second = rules[j]
                if (first.method != second.method) continue

                val a = first.pattern.split('/').filter { it.isNotEmpty() }
                val b = second.pattern.split('/').filter { it.isNotEmpty() }
                if (a.size != b.size) continue

                // Both can match one path only if every position agrees on the
                // literal ones: identical, or one side is a `{param}`.
                fun isParam(seg: String) = seg.startsWith('{') && seg.endsWith('}')
                val satisfiable = a.indices.all { k -> a[k] == b[k] || isParam(a[k]) || isParam(b[k]) }

                if (satisfiable && first.requirement.ordinal < second.requirement.ordinal) {
                    ambiguous.add(
                        "${first.method.value} ${first.pattern} (${first.requirement}) shadows " +
                            "${second.pattern} (${second.requirement})",
                    )
                }
            }
        }

        assertThat(ambiguous)
            .describedAs("a weaker rule is tried before a stricter one that can match the same path")
            .isEmpty()
    }

    @Test
    fun `the match order is longest first, then most literal`() {
        // The sort is the mechanism the previous test depends on, so pin it
        // directly: `rules` must be monotonic under (segments, literals), both
        // descending. A wildcard rule of the same length therefore never precedes
        // the literal rule it could shadow, whatever order they were inserted in.
        val rules = AuthPolicy.rules
        val outOfOrder =
            rules.zipWithNext().filter { (a, b) ->
                a.segments.size < b.segments.size ||
                    (a.segments.size == b.segments.size && a.literalSegments < b.literalSegments)
            }
        assertThat(outOfOrder)
            .describedAs("these consecutive rules are in the wrong match order")
            .isEmpty()
    }

    @Test
    fun `literalSegments counts the literal positions of a pattern`() {
        // The key the sort uses, checked against a hand-counted pattern: a
        // miscount here would reorder the table without failing anything else.
        val mixed = AuthPolicy.Entry(HttpMethod.Get, "/v1/catalogs/{catalog}/x/{y}", Requirement.READ)
        val literals = AuthPolicy.Entry(HttpMethod.Get, "/v1/catalogs/{catalog}/x/compact", Requirement.MAINTENANCE)

        assertThat(mixed.segments).hasSize(5)
        assertThat(mixed.literalSegments).isEqualTo(3)
        assertThat(literals.literalSegments).isEqualTo(4)
    }

    @Test
    fun `every published route has a policy`() {
        val published = publishedRoutes()
        // The spec is the contract; if this is empty the parse broke, and a
        // vacuous pass would be worse than a failure.
        assertThat(published).hasSizeGreaterThan(60)

        // The local-only routes have no policy entry. The plugin decides them before the policy table
        // (AuthPlugin.kt, selectLocalOnly), with the dedicated read verifier only.
        val (localOnly, ordinary) = published.partition { (_, path) -> ReadSessionProfile.isLocalOnlyPath(path) }
        val missing =
            ordinary.filter { (method, path) -> AuthPolicy.match(HttpMethod.parse(method.uppercase()), path) == null }
        assertThat(missing).describedAs("routes published in the OpenAPI spec with no AuthPolicy entry").isEmpty()

        // Each local-only route matches an allowed (method, canonicalPattern) pair in LOCAL_ONLY_ALLOWED_METHODS.
        val publishedLocalOnly =
            localOnly.map { (method, path) ->
                val canonical = ReadSessionProfile.canonicalLocalOnlyPath(path)
                val matched = canonical?.let { ReadSessionProfile.matchLocalOnlyRoute(it) }
                assertThat(matched).describedAs("local-only route $method $path").isNotNull()
                HttpMethod.parse(method.uppercase()) to matched!!.pattern
            }.toSet()
        val expectedLocalOnly =
            ReadSessionProfile.LOCAL_ONLY_ALLOWED_METHODS.flatMap { (pattern, methods) ->
                methods.map { it to pattern }
            }.toSet()
        assertThat(publishedLocalOnly).isEqualTo(expectedLocalOnly)
    }

    @Test
    fun `every policy names a route that is actually published`() {
        // The other direction: a rule for a path nobody serves is dead policy
        // that a reviewer would read as protection. `/openapi.yaml` is the one
        // intentional exception — the server serves it and the spec does not
        // document itself.
        val published = publishedRoutes().map { (m, p) -> m.uppercase() to p }.toSet()
        val orphans =
            AuthPolicy.rules
                .filter { (it.method.value.uppercase() to it.pattern) !in published }
                .filterNot { it.method == HttpMethod.Get && it.pattern == "/openapi.yaml" }
        assertThat(orphans).describedAs("AuthPolicy entries for routes the spec does not publish").isEmpty()
    }

    @Test
    fun `no two policies describe the same route`() {
        val duplicates =
            AuthPolicy.rules.groupBy { it.method to it.pattern }.filterValues { it.size > 1 }.keys
        assertThat(duplicates).isEmpty()
    }

    @Test
    fun `an unlisted path is refused rather than defaulted`() {
        assertThat(AuthPolicy.match(HttpMethod.Get, "/v1/not-a-route")).isNull()
        assertThat(AuthPolicy.authorize(null, null)).isInstanceOf(AuthDecision.Unauthenticated::class.java)
        assertThat(AuthPolicy.authorize(operator(), null)).isInstanceOf(AuthDecision.Unauthenticated::class.java)
    }

    @Test
    fun `segment counting keeps a catalog route from shadowing the listing`() {
        assertThat(AuthPolicy.match(HttpMethod.Get, "/v1/catalogs")?.requirement).isEqualTo(Requirement.AUTHENTICATED)
        val one = AuthPolicy.match(HttpMethod.Get, "/v1/catalogs/lake")
        assertThat(one?.requirement).isEqualTo(Requirement.READ)
        assertThat(one?.catalog).isEqualTo("lake")
    }

    @Test
    fun `the deepest route wins on a path that also matches a shorter one`() {
        val match = AuthPolicy.match(HttpMethod.Post, "/v1/catalogs/lake/namespaces/ns/tables/t/alter")
        assertThat(match?.requirement).isEqualTo(Requirement.WRITE)
        assertThat(match?.catalog).isEqualTo("lake")
    }

    @Test
    fun `a trailing slash matches no rule, as it matches no route`() {
        // The router keeps a trailing slash as an empty segment (IgnoreTrailingSlash
        // is not installed), so this path serves nothing; the table must not
        // authorize it as though it were the catalog route.
        assertThat(AuthPolicy.match(HttpMethod.Get, "/v1/catalogs/lake/")).isNull()
    }

    @Test
    fun `a percent-encoded catalog is authorized as the catalog it decodes to`() {
        assertThat(AuthPolicy.match(HttpMethod.Get, "/v1/catalogs/la%6Be")?.catalog).isEqualTo("lake")
        assertThat(AuthPolicy.match(HttpMethod.Post, "/v1/catalogs/lake/%63ommit")?.requirement)
            .isEqualTo(Requirement.WRITE)
        // A segment the router cannot decode routes nowhere; it authorizes nothing.
        assertThat(AuthPolicy.match(HttpMethod.Get, "/v1/catalogs/%")).isNull()
    }

    @Test
    fun `head demands what get demands`() {
        assertThat(AuthPolicy.match(HttpMethod.Head, "/livez")?.requirement).isEqualTo(Requirement.PUBLIC)
        assertThat(AuthPolicy.match(HttpMethod.Head, "/v1/catalogs/lake")?.requirement).isEqualTo(Requirement.READ)
    }

    @Test
    fun `allowed methods lists the table's methods for a path, and none for an unknown path`() {
        assertThat(AuthPolicy.allowedMethods("/livez")).containsExactly(HttpMethod.Get)
        assertThat(AuthPolicy.allowedMethods("/v1/catalogs/lake"))
            .containsExactlyInAnyOrder(HttpMethod.Get, HttpMethod.Delete)
        assertThat(AuthPolicy.allowedMethods("/v1/nothing-here")).isEmpty()
    }

    // ---- the decision matrix --------------------------------------------

    @Test
    fun `public routes allow an anonymous caller`() {
        val match = AuthPolicy.match(HttpMethod.Get, "/livez")!!
        assertThat(AuthPolicy.authorize(null, match)).isEqualTo(AuthDecision.Allow)
        assertThat(AuthPolicy.authorize(tenant(read = listOf("lake")), match)).isEqualTo(AuthDecision.Allow)
    }

    @Test
    fun `a route naming no catalog requires a token but authorizes nothing`() {
        val match = AuthPolicy.match(HttpMethod.Get, "/v1/catalogs")!!
        assertThat(AuthPolicy.authorize(null, match)).isInstanceOf(AuthDecision.Unauthenticated::class.java)
        assertThat(AuthPolicy.authorize(tenant(read = listOf("lake")), match)).isEqualTo(AuthDecision.Allow)
    }

    @Test
    fun `a tenant asking about someone else's catalog gets the unknown-catalog answer`() {
        val match = AuthPolicy.match(HttpMethod.Get, "/v1/catalogs/their-lake")!!
        val decision = AuthPolicy.authorize(tenant(read = listOf("lake")), match)
        // NOT InsufficientScope: this maps to 404, so the catalog cannot be
        // probed for existence by a caller that was never given its name.
        assertThat(decision).isEqualTo(AuthDecision.UnknownCatalog("their-lake"))
    }

    @Test
    fun `a tenant short of the scope gets a truthful 403`() {
        val match = AuthPolicy.match(HttpMethod.Post, "/v1/catalogs/lake/maintenance/compact")!!
        val decision = AuthPolicy.authorize(tenant(read = listOf("lake")), match)
        assertThat(decision).isEqualTo(AuthDecision.InsufficientScope("lake", Scope.MAINTENANCE))
    }

    @Test
    fun `a writer may commit but may not compact`() {
        val caller = tenant(write = listOf("lake"))
        assertThat(AuthPolicy.authorize(caller, AuthPolicy.match(HttpMethod.Post, "/v1/catalogs/lake/commit")))
            .isEqualTo(AuthDecision.Allow)
        assertThat(
            AuthPolicy.authorize(caller, AuthPolicy.match(HttpMethod.Post, "/v1/catalogs/lake/maintenance/compact")),
        )
            .isInstanceOf(AuthDecision.InsufficientScope::class.java)
    }

    @Test
    fun `dropping a catalog needs admin, not maintenance`() {
        val caller = tenant(maintenance = listOf("lake"))
        assertThat(AuthPolicy.authorize(caller, AuthPolicy.match(HttpMethod.Delete, "/v1/catalogs/lake")))
            .isEqualTo(AuthDecision.InsufficientScope("lake", Scope.ADMIN))
    }

    @Test
    fun `an operator reaches every route, including the operator-only ones`() {
        val caller = operator()
        assertThat(
            AuthPolicy.authorize(caller, AuthPolicy.match(HttpMethod.Get, "/metrics")),
        ).isEqualTo(AuthDecision.Allow)
        assertThat(AuthPolicy.authorize(caller, AuthPolicy.match(HttpMethod.Get, "/v1/database/health")))
            .isEqualTo(AuthDecision.Allow)
        assertThat(AuthPolicy.authorize(caller, AuthPolicy.match(HttpMethod.Get, "/v1/catalogs/anything")))
            .isEqualTo(AuthDecision.Allow)
    }

    @Test
    fun `a catalog-bound operator reaches its catalog and provisioning, and nothing fleet-wide`() {
        val caller = boundOperator("lake")
        // Its own catalog, the operator-only containment route included.
        assertThat(AuthPolicy.authorize(caller, AuthPolicy.match(HttpMethod.Get, "/v1/catalogs/lake")))
            .isEqualTo(AuthDecision.Allow)
        assertThat(AuthPolicy.authorize(caller, AuthPolicy.match(HttpMethod.Delete, "/v1/catalogs/lake")))
            .isEqualTo(AuthDecision.Allow)
        assertThat(
            AuthPolicy.authorize(
                caller,
                AuthPolicy.match(HttpMethod.Put, "/v1/catalogs/lake/namespaces/ns/containment"),
            ),
        ).isEqualTo(AuthDecision.Allow)
        // Another catalog: the 404 a tenant gets, on both route shapes.
        assertThat(AuthPolicy.authorize(caller, AuthPolicy.match(HttpMethod.Get, "/v1/catalogs/other")))
            .isEqualTo(AuthDecision.UnknownCatalog("other"))
        assertThat(
            AuthPolicy.authorize(
                caller,
                AuthPolicy.match(HttpMethod.Put, "/v1/catalogs/other/namespaces/ns/containment"),
            ),
        ).isEqualTo(AuthDecision.UnknownCatalog("other"))
        // Provisioning names its catalog in the body: let through to the handler, which holds it.
        assertThat(AuthPolicy.authorize(caller, AuthPolicy.match(HttpMethod.Post, "/v1/catalogs")))
            .isEqualTo(AuthDecision.Allow)
        // MUTATION: `caller.kind.isOperator()` in place of `fleetWide` and these are Allow.
        for ((method, path) in listOf(
            HttpMethod.Get to "/metrics",
            HttpMethod.Get to "/v1/database/health",
            HttpMethod.Post to "/v1/debug/decode-bound",
            HttpMethod.Get to "/v1/maintenance/status",
            HttpMethod.Get to "/v1/maintenance/runs",
        )) {
            assertThat(AuthPolicy.authorize(caller, AuthPolicy.match(method, path)))
                .describedAs(path)
                .isEqualTo(AuthDecision.InsufficientScope(path, Scope.ADMIN))
        }
        // The listing is filtered by the handler to what the caller sees.
        assertThat(AuthPolicy.authorize(caller, AuthPolicy.match(HttpMethod.Get, "/v1/catalogs")))
            .isEqualTo(AuthDecision.Allow)
        assertThat(caller.visibleCatalogs()).containsExactly("lake")
    }

    @Test
    fun `a tenant cannot reach the operator-only surfaces`() {
        val caller = tenant(admin = listOf("lake"))
        assertThat(AuthPolicy.authorize(caller, AuthPolicy.match(HttpMethod.Get, "/v1/database/health")))
            .isInstanceOf(AuthDecision.InsufficientScope::class.java)
        assertThat(AuthPolicy.authorize(caller, AuthPolicy.match(HttpMethod.Post, "/v1/catalogs")))
            .isInstanceOf(AuthDecision.InsufficientScope::class.java)
        assertThat(AuthPolicy.authorize(caller, AuthPolicy.match(HttpMethod.Delete, "/v1/catalogs/lake")))
            .isEqualTo(AuthDecision.Allow)
    }

    @Test
    fun `partition lookup POST is a catalog metadata read`() {
        val match =
            AuthPolicy.match(
                HttpMethod.Post,
                "/v1/catalogs/lake/namespaces/ns/tables/t/partitions/lookup",
            )!!
        assertThat(match.requirement).isEqualTo(Requirement.READ)
        assertThat(AuthPolicy.authorize(tenant(read = listOf("lake")), match)).isEqualTo(AuthDecision.Allow)
        assertThat(AuthPolicy.authorize(tenant(read = listOf("other")), match))
            .isEqualTo(AuthDecision.UnknownCatalog("lake"))
    }

    @Test
    fun `a scoped caller is refused on every catalog route but the listed ones, decided per namespace`() {
        // A route added to the table is refused to scoped callers until it is
        // listed in SCOPED_ROUTES or BODY_SCOPED_ROUTES.
        val scoped =
            Caller(
                subject = "hlc_team",
                orgId = "org-1",
                kind = Caller.Kind.SCOPED,
                catalogScopes = mapOf("lake" to setOf(Scope.WRITE)),
                tokenId = "jti",
                clientName = "laptop",
                predicates = mapOf("team_id" to PredicateGrant.All(Scope.WRITE)),
            )
        for (rule in AuthPolicy.rules) {
            val path = rule.pattern.replace("{catalog}", "lake").replace(Regex("\\{[a-zA-Z]+}"), "x")
            val match = AuthPolicy.match(rule.method, path)!!
            val decision = AuthPolicy.authorize(scoped, match)
            when (rule.requirement) {
                Requirement.PUBLIC, Requirement.AUTHENTICATED ->
                    assertThat(decision).describedAs(rule.pattern).isEqualTo(AuthDecision.Allow)
                Requirement.OPERATOR ->
                    assertThat(
                        decision,
                    ).describedAs(rule.pattern).isInstanceOf(AuthDecision.InsufficientScope::class.java)
                else -> {
                    val route = rule.method to rule.pattern
                    val scope = rule.requirement.scopeOrNull()!!
                    if (route in AuthPolicy.BODY_SCOPED_ROUTES) {
                        assertThat(decision).describedAs(rule.pattern).isEqualTo(AuthDecision.ByBody("lake", scope))
                    } else if (route !in AuthPolicy.SCOPED_ROUTES) {
                        assertThat(decision).describedAs(rule.pattern)
                            .isEqualTo(AuthDecision.NotPredicated("lake", match.pattern))
                    } else if ("{namespace}" in rule.pattern) {
                        assertThat(decision).describedAs(rule.pattern)
                            .isEqualTo(AuthDecision.ByNamespace("lake", "x", scope))
                    } else {
                        assertThat(decision).describedAs(rule.pattern).isEqualTo(AuthDecision.Allow)
                    }
                }
            }
        }
        // HEAD is GET for this too.
        assertThat(
            AuthPolicy.authorize(
                scoped,
                AuthPolicy.match(io.ktor.http.HttpMethod.Head, "/v1/catalogs/lake/namespaces/team_42")!!,
            ),
        ).isEqualTo(AuthDecision.ByNamespace("lake", "team_42", Scope.READ))
        // The namespace is the one the router will serve: percent-decoded.
        assertThat(
            AuthPolicy.authorize(
                scoped,
                AuthPolicy.match(io.ktor.http.HttpMethod.Get, "/v1/catalogs/lake/namespaces/team%5F42/tables")!!,
            ),
        ).isEqualTo(AuthDecision.ByNamespace("lake", "team_42", Scope.READ))
        // Another catalog is still unknown, not forbidden.
        val other = AuthPolicy.match(io.ktor.http.HttpMethod.Get, "/v1/catalogs/other")!!
        assertThat(AuthPolicy.authorize(scoped, other)).isEqualTo(AuthDecision.UnknownCatalog("other"))
    }

    @Test
    fun `every scoped route is a read or a write the table lists, in one list`() {
        assertThat(AuthPolicy.SCOPED_ROUTES intersect AuthPolicy.BODY_SCOPED_ROUTES).isEmpty()
        for ((method, pattern) in AuthPolicy.SCOPED_ROUTES + AuthPolicy.BODY_SCOPED_ROUTES) {
            val rule = AuthPolicy.rules.singleOrNull { it.method == method && it.pattern == pattern }
            assertThat(rule).describedAs("%s %s is in the table", method.value, pattern).isNotNull()
            assertThat(rule!!.requirement).describedAs("%s %s", method.value, pattern)
                .isIn(Requirement.READ, Requirement.WRITE)
        }
        // A body-scoped route either names no namespace in its path, or is
        // listed there because its path cannot name all it touches.
        for ((_, pattern) in AuthPolicy.BODY_SCOPED_ROUTES) {
            assertThat(pattern).describedAs(pattern).doesNotContain("{namespace}")
        }
    }

    @Test
    fun `the reserved shape is a canonical lowercase org uuid followed by -events`() {
        assertThat(MountNames.isReserved("0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f0-events")).isTrue()
        for (name in listOf(
            "0190F3C4-1B2A-4C3D-8E4F-A5B6C7D8E9F0-events",
            "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f0-events2",
            "x0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f0-events",
            "0190f3c41b2a4c3d8e4fa5b6c7d8e9f0-events",
            "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f-events",
            "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f0",
            "millrace-dev-events",
        )) {
            assertThat(MountNames.isReserved(name)).describedAs(name).isFalse()
        }
    }

    @Test
    fun `the mount API has no rule, and a reserved-shape name is predicated like any catalog`() {
        for (path in listOf("/v1/mounts", "/v1/mounts/x")) {
            for (method in listOf(HttpMethod.Get, HttpMethod.Put, HttpMethod.Head)) {
                assertThat(AuthPolicy.match(method, path)).describedAs("%s %s", method.value, path).isNull()
            }
            // No method of the path is served either, so enforce answers 401, not 405.
            assertThat(AuthPolicy.allowedMethods(path)).describedAs(path).isEmpty()
        }
        val mount = "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f0-events"
        val reader = Caller("hlc_team", "org-1", Caller.Kind.SCOPED, mapOf(mount to setOf(Scope.READ)), "jti", null)
        for (method in listOf(HttpMethod.Get, HttpMethod.Head)) {
            assertThat(AuthPolicy.authorize(reader, AuthPolicy.match(method, "/v1/catalogs/$mount")!!))
                .isEqualTo(AuthDecision.NotPredicated(mount, "/v1/catalogs/{catalog}"))
        }
    }

    @Test
    fun `a scoped caller's permission on a value is the lower of its predicate and its catalog`() {
        fun caller(
            cat: Set<Scope>,
            grant: PredicateGrant,
        ) = Caller(
            subject = "hlc_team",
            orgId = "org-1",
            kind = Caller.Kind.SCOPED,
            catalogScopes = mapOf("lake" to cat),
            tokenId = "jti",
            clientName = null,
            predicates = mapOf("team_id" to grant),
        )
        val values = PredicateGrant.Values(mapOf(42L to Scope.WRITE, 7L to Scope.READ))
        val rw = caller(setOf(Scope.WRITE), values)
        assertThat(rw.mayUseContained("lake", "team_id", 42, null, Scope.WRITE)).isTrue()
        assertThat(rw.mayUseContained("lake", "team_id", 7, null, Scope.READ)).isTrue()
        assertThat(rw.mayUseContained("lake", "team_id", 7, null, Scope.WRITE)).isFalse()
        assertThat(rw.mayUseContained("lake", "team_id", 9, null, Scope.READ)).isFalse()
        assertThat(rw.mayUseContained("lake", "org_id", 42, null, Scope.READ)).isFalse()
        assertThat(rw.mayUseContained("other", "team_id", 42, null, Scope.READ)).isFalse()
        val readOnlyCatalog = caller(setOf(Scope.READ), values)
        assertThat(readOnlyCatalog.mayUseContained("lake", "team_id", 42, null, Scope.WRITE)).isFalse()
        assertThat(readOnlyCatalog.mayUseContained("lake", "team_id", 42, null, Scope.READ)).isTrue()
        val all = caller(setOf(Scope.WRITE), PredicateGrant.All(Scope.READ))
        assertThat(all.mayUseContained("lake", "team_id", 123, null, Scope.READ)).isTrue()
        assertThat(all.mayUseContained("lake", "team_id", 123, null, Scope.WRITE)).isFalse()
        // An org mark must equal the token's org; an unmarked namespace is not compared.
        assertThat(rw.mayUseContained("lake", "team_id", 42, "org-1", Scope.WRITE)).isTrue()
        assertThat(rw.mayUseContained("lake", "team_id", 42, "org-2", Scope.WRITE)).isFalse()
        assertThat(rw.mayUseContained("lake", "team_id", 7, "org-2", Scope.READ)).isFalse()
        assertThat(rw.copy(orgId = null).mayUseContained("lake", "team_id", 42, "org-1", Scope.READ)).isFalse()
        // Never for another kind: a tenant's authority is `holds`.
        assertThat(
            rw.copy(kind = Caller.Kind.TENANT).mayUseContained("lake", "team_id", 42, null, Scope.READ),
        ).isFalse()
        assertThat(
            rw.copy(kind = Caller.Kind.TENANT).mayUseContained("lake", "team_id", 42, "org-1", Scope.READ),
        ).isFalse()
    }

    // ---- fixtures -------------------------------------------------------

    private fun tenant(
        read: List<String> = emptyList(),
        write: List<String> = emptyList(),
        maintenance: List<String> = emptyList(),
        admin: List<String> = emptyList(),
    ): Caller {
        val map =
            buildMap {
                read.forEach { put(it, getOrDefault(it, emptySet()) + Scope.READ) }
                write.forEach { put(it, getOrDefault(it, emptySet()) + Scope.WRITE) }
                maintenance.forEach { put(it, getOrDefault(it, emptySet()) + Scope.MAINTENANCE) }
                admin.forEach { put(it, getOrDefault(it, emptySet()) + Scope.ADMIN) }
            }
        return Caller(
            subject = "client:test",
            orgId = "org-1",
            kind = Caller.Kind.TENANT,
            catalogScopes = map,
            tokenId = "jti",
            clientName = "test",
        )
    }

    private fun operator() = Caller("sa:test", null, Caller.Kind.OPERATOR, emptyMap(), "jti", null)

    private fun boundOperator(catalog: String) =
        Caller("sa:test", "org-1", Caller.Kind.OPERATOR, mapOf(catalog to setOf(Scope.ADMIN)), "jti", null)

    /**
     * Read `paths:` out of the OpenAPI document.
     *
     * A tolerant line scan, not a YAML parse: there is no YAML parser on the
     * classpath (the same constraint ScalarTypeParityTest works under), and the
     * two-level shape this needs — a two-space path key, a four-space method —
     * is stable in this file. A malformed document makes the size assertion in
     * the first test fail loudly rather than passing vacuously.
     */
    private fun publishedRoutes(): List<Pair<String, String>> {
        val text =
            checkNotNull(javaClass.getResourceAsStream("/openapi/hoglake.yaml")) {
                "openapi/hoglake.yaml is not on the test classpath"
            }.bufferedReader().readText()

        // The document's own `servers: - url: /v1` is the prefix for every path,
        // and the three operational endpoints override it with `servers: [{ url:
        // / }]` because they are served at the root. Ignoring that override is
        // how a totality check silently compares two different namespaces and
        // then has to be weakened to pass.
        val path = Regex("""^  (/\S*):\s*$""")
        val rootOverride = Regex("""^    servers:\s*\[\s*\{\s*url:\s*/\s*}""")
        val method = Regex("""^    (get|post|put|patch|delete|head|options|trace):\s*$""")
        val out = mutableListOf<Pair<String, String>>()
        var current: String? = null
        var prefix = "/v1"
        for (line in text.lines()) {
            path.find(line)?.let {
                current = it.groupValues[1]
                prefix = "/v1"
                return@let
            }
            if (current != null && rootOverride.containsMatchIn(line)) prefix = ""
            method.find(line)?.let { m ->
                current?.let { out += m.groupValues[1] to (prefix + it) }
            }
        }
        return out
    }
}
