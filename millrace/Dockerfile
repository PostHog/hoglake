# Millrace image: the Kafka → hoglake ingestion daemon and its
# maintenance (SlateDB GC) service, one image, two entrypoints. The
# chart runs the pipeline StatefulSet with the default entrypoint
# (`millrace`) and the maintenance Deployment with
# `python -m millrace.maintenance`; both read only environment.
#
# Build context is the REPO ROOT, not millrace/, for the same reason
# bench/Dockerfile gives: millrace is a member of the repo's uv
# workspace and installs pyhoglake as a workspace member
# (`[tool.uv.sources] pyhoglake = { workspace = true }`), so the root
# pyproject.toml and uv.lock, the pyhoglake tree and every member's
# pyproject.toml must be in the context, and the image ships the client
# from the SAME commit as the daemon — the pair the live suite tests.
#
#   docker build -f millrace/Dockerfile -t hoglake-millrace .
#
# No credentials and no environment-specific values are baked in.
# Everything (MILLRACE_*, HOGLAKE_URL, HOGLAKE_S3_*, AWS_*) is supplied
# at run time; in-cluster the object-store credentials for both the
# staging bucket and the lake come from the pod's projected
# web-identity token via the ambient AWS chain.

# No --platform=$BUILDPLATFORM: the installed tree is a virtualenv of
# native manylinux wheels (pyarrow, slatedb, confluent-kafka), per
# target arch by construction. Every wheel millrace installs from the
# root uv.lock is prebuilt for linux amd64 and arm64, so the
# foreign-arch pass under qemu only unpacks and never compiles.
FROM python:3.13-slim-trixie AS build

# uv, pinned: an unpinned installer resolves differently per build.
COPY --from=ghcr.io/astral-sh/uv:0.13.4 /uv /usr/local/bin/uv

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/hoglake-millrace/venv

WORKDIR /src

# The workspace root: its manifest and the one lock.
COPY pyproject.toml uv.lock ./
# pyhoglake first and whole: a workspace member uv builds from source.
COPY pyhoglake/ pyhoglake/
# The members millrace does not install: manifests only, which is all
# the --locked check reads.
COPY hedgerow/pyproject.toml hedgerow/
COPY bench/pyproject.toml bench/
# Manifests only, so the dependency layer survives a millrace
# source-only change. (pyproject's `readme` resolves at build time.)
COPY millrace/pyproject.toml millrace/README.md millrace/
RUN uv sync --locked --package millrace --no-dev --no-editable --no-install-project

COPY millrace/millrace/ millrace/millrace/
RUN uv sync --locked --package millrace --no-dev --no-editable

# --no-editable makes the venv self-contained (workspace members are
# editable for development; here they are installed as wheels), so the
# runtime stage copies one directory and no source tree.

FROM python:3.13-slim-trixie
# Non-root, the uid the server and bench images use. uid == gid so a
# pod spec names one number for runAsUser, runAsGroup and fsGroup.
RUN groupadd --system --gid 10001 hoglake \
 && useradd --system --uid 10001 --gid 10001 hoglake
COPY --from=build /opt/hoglake-millrace/venv/ /opt/hoglake-millrace/venv/
ENV PATH="/opt/hoglake-millrace/venv/bin:$PATH"
# Nothing is written outside /tmp: staging lives on the object store
# (SlateDB), parquet is built in memory and uploaded. /work is a
# writable cwd. Read-only-rootfs friendly otherwise.
RUN install -d -o 10001 -g 10001 /work
WORKDIR /work
USER 10001
# The operational HTTP surface: /metrics, /healthz, /readyz. The
# pipeline listens on MILLRACE_METRICS_PORT (8000), the maintenance
# service on MILLRACE_MAINTENANCE_METRICS_PORT (8001).
EXPOSE 8000 8001
# A bare run with no environment refuses at config (exit 2) and names
# every missing knob — the smoke test, and the behavior an operator
# sees from a pod with an empty env.
ENTRYPOINT ["millrace"]
