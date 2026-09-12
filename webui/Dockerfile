# Console image. Static Vite build served by nginx, which also
# reverse-proxies the four API paths the client calls same-origin
# (/v1, /healthz, /metrics, /openapi.yaml) — the production analogue of
# vite.config.ts's dev proxy. The upstream is HOGLAKE_API (env,
# default http://hoglake-server:8080), substituted at container start
# by the nginx image's envsubst entrypoint.

# --platform=$BUILDPLATFORM: the vite output is static; only nginx is
# per-target-arch.
FROM --platform=$BUILDPLATFORM node:22-alpine AS build
WORKDIR /src
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

# Unprivileged variant: runs as uid 101, listens on 8080 — the golden
# chart's default securityContext drops ALL capabilities, so a
# root+port-80 nginx cannot bind. NGINX_ENVSUBST_FILTER keeps the
# entrypoint's envsubst away from nginx runtime vars ($uri, $host) if a
# colliding lowercase env var ever appears.
FROM nginxinc/nginx-unprivileged:1.29-alpine
# HOGLAKE_API must have NO trailing slash: with one, proxy_pass enters
# URI-replacing mode and /v1/foo silently becomes /foo upstream. The
# hostname must be fully qualified or an IP: runtime resolution
# (resolver directive) ignores resolv.conf search domains.
ENV HOGLAKE_API=http://hoglake-server:8080
ENV NGINX_ENVSUBST_FILTER=^HOGLAKE_
# Sourced by the entrypoint before envsubst: derives
# HOGLAKE_DNS_RESOLVER from resolv.conf for runtime upstream
# resolution (the console boots with or without the server existing).
COPY --chmod=755 10-resolver.envsh /docker-entrypoint.d/10-resolver.envsh
COPY nginx.conf.template /etc/nginx/templates/default.conf.template
COPY --from=build /src/dist/ /usr/share/nginx/html/
EXPOSE 8080
