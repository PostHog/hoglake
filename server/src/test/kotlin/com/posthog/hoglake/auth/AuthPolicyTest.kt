package com.posthog.hoglake.auth

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
    fun `every published route has a policy`() {
        val published = publishedRoutes()
        // The spec is the contract; if this is empty the parse broke, and a
        // vacuous pass would be worse than a failure.
        assertThat(published).hasSizeGreaterThan(60)

        val missing =
            published.filter { (method, path) -> AuthPolicy.match(HttpMethod.parse(method.uppercase()), path) == null }
        assertThat(missing).describedAs("routes published in the OpenAPI spec with no AuthPolicy entry").isEmpty()
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
    fun `a trailing slash does not slip past the table`() {
        assertThat(AuthPolicy.match(HttpMethod.Get, "/v1/catalogs/lake/")?.requirement).isEqualTo(Requirement.READ)
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
    fun `a tenant cannot reach the operator-only surfaces`() {
        val caller = tenant(admin = listOf("lake"))
        assertThat(AuthPolicy.authorize(caller, AuthPolicy.match(HttpMethod.Get, "/v1/database/health")))
            .isInstanceOf(AuthDecision.InsufficientScope::class.java)
        assertThat(AuthPolicy.authorize(caller, AuthPolicy.match(HttpMethod.Post, "/v1/catalogs")))
            .isInstanceOf(AuthDecision.InsufficientScope::class.java)
        assertThat(AuthPolicy.authorize(caller, AuthPolicy.match(HttpMethod.Delete, "/v1/catalogs/lake")))
            .isEqualTo(AuthDecision.Allow)
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
