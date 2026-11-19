---
hoglake: minor (Server)
---

The server writes one JSON access line per request on the `hoglake.access` logger. Each line names the caller (org, team, token `jti`, `usr` and client), the route pattern, the status, the duration, the wait for a request thread and the error code. Refusals are logged; the health and metrics probes are not. The line never carries a token, a header value, the query string or a body. The plain-text Ktor request line is gone.
