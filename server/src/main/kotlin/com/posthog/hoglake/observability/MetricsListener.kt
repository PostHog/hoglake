package com.posthog.hoglake.observability

import io.ktor.http.ContentType
import io.ktor.server.application.Application
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry

/**
 * The infra listener (docs/auth-tenancy.md §6.7): the Prometheus scrape,
 * served on its own port with no auth plugin.
 *
 * WHY A SECOND LISTENER rather than a token for the scraper: on the API
 * port `/metrics` is operator-only, because the exposition names every
 * catalog. A scraper cannot refresh a 15-minute token, so the token would
 * have to be long-lived, which is an operator credential sitting in the
 * vmagent config. And under `warn` every untokened scrape counts as a
 * refusal, so the zero-refusals gate for `enforce` could never pass. The
 * boundary for this port is the network instead: it is on the pod, not on
 * the Service.
 *
 * Only `/metrics` is mounted. Everything else 404s, so nothing reachable
 * through the API port's auth can be reached around it here.
 */
fun Application.metricsListener(registry: PrometheusMeterRegistry) {
    routing {
        get("/metrics") {
            call.respondText(
                registry.scrape(),
                ContentType.parse("text/plain; version=0.0.4; charset=utf-8"),
            )
        }
    }
}
