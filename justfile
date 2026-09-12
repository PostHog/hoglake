# hoglake — top-level recipes composing the per-component justfiles.
# `just server test`, `just pyhoglake unit`, `just test-all`, ...

mod server
mod pyhoglake
mod webui
mod hedgerow
mod bench

default:
    @just --list

# Full verification: server then client suites (client integration skips unless a server is up; `just dev` first for end-to-end)
test-all:
    just server test
    just pyhoglake test

# Style gates: ktlint (server) + ruff check/format (pyhoglake, hedgerow),
# every tool pinned. bench is not wired in yet — it passes `ruff check`
# but carries a formatting backlog; see AGENT.md open items.
lint-all:
    just server lint
    just pyhoglake lint
    just hedgerow lint

# Bring up the dev stack and the server (foreground).
dev:
    just server compose-up
    just server run

# The whole thing in containers — Postgres + MinIO + hoglake-server +
# hoglake-webui, building the images from the working tree as needed.
# Webui on http://localhost:${HOGLAKE_WEBUI_PORT:-5173} (proxies /v1 to
# the server on :8080, HOGLAKE_SERVER_PORT to override). The compaction
# loop is enabled (60s; HOGLAKE_COMPACTION_INTERVAL_MS=0 just up to
# disable). State persists in named volumes across up/down.
up:
    docker compose -f server/docker-compose.yml --profile full up -d --build

# Tear the whole stack down (Postgres/MinIO data volumes survive).
down:
    docker compose -f server/docker-compose.yml --profile full down

# Tear down AND wipe all state (drops the named volumes).
down-volumes:
    docker compose -f server/docker-compose.yml --profile full down -v
