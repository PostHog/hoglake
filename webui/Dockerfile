# Console image. Static Vite build served by nginx, which also
# reverse-proxies the four API paths the client calls same-origin
# (/v1, /healthz, /metrics, /openapi.yaml) — the production analogue of
# vite.config.ts's dev proxy. The upstream is HOGLAKE_API (env,
# default http://hoglake-server:8080), substituted at container start
# by the nginx image's envsubst entrypoint.

FROM node:22-alpine AS build
WORKDIR /src
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM nginx:1.27-alpine
ENV HOGLAKE_API=http://hoglake-server:8080
COPY nginx.conf.template /etc/nginx/templates/default.conf.template
COPY --from=build /src/dist/ /usr/share/nginx/html/
EXPOSE 80
