# Bench image: the stress/benchmark harness with its dependencies
# already installed, so running it next to a deployed server is a pod
# and an exec — no copying a tree in, no `uv sync` in the pod, no drift
# between the harness in the pod and the commit it came from.
#
# Deliberately SEPARATE from the server image. Bench brings hundreds of
# megabytes of Python runtime that every server and maintenance pod
# would otherwise carry, it would couple its release cadence to the
# control plane's, and it is a data generator — it creates catalogs,
# writes hundreds of thousands of rows, and drops and recreates tables
# in the analytics-lifecycle scenario. None of that belongs inside pods
# that already hold S3 write and catalog authority. This image is
# pulled BY HAND; no CD deploys it.
#
# Build context is the REPO ROOT, not bench/ (unlike server/ and
# webui/, which build from their own directories). bench installs
# pyhoglake from the sibling tree — `[tool.uv.sources] pyhoglake =
# { path = "../pyhoglake" }` — so both trees must be in the context or
# the relative path does not resolve ("Distribution not found at:
# file:///work/pyhoglake"). That is the point: the image ships the
# client from the SAME commit as the harness, which is the pair the
# repo tests, rather than whatever PyPI's newest release happens to be.
#
#   docker build -f bench/Dockerfile -t hoglake-bench .
#
# No credentials and no environment-specific values are baked in.
# Everything (HOGLAKE_URL, HOGLAKE_S3_*, --bucket) is supplied at run
# time; in-cluster the S3 credentials come from the pod's projected
# web-identity token via the ambient AWS chain.

# NOTE the absence of --platform=$BUILDPLATFORM here, which server/ and
# webui/ both use. Their build stages emit arch-neutral output (JVM
# bytecode, static Vite assets), so building them once natively is
# free. This one does not: the installed tree is a virtualenv holding
# native manylinux wheels (pyarrow, numpy, psycopg-binary), which are
# per-target-arch by construction. Pinning this stage to $BUILDPLATFORM
# would install amd64 wheels into the arm64 image and fail at import on
# the Graviton fleet. Every wheel in bench/uv.lock is prebuilt for both
# arches, so the foreign-arch pass under qemu only unpacks — it never
# compiles.
FROM python:3.13-slim-trixie AS build

# uv, the same installer the trees are developed and CI-tested with
# (ci-python.yml uses astral-sh/setup-uv). Pinned: an unpinned
# installer resolves differently per build.
COPY --from=ghcr.io/astral-sh/uv:0.12.17 /uv /usr/local/bin/uv

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/hoglake-bench/venv

WORKDIR /src/bench

# pyhoglake first and whole: it is a path dependency, so uv builds it
# from source, and a stub would make the dependency layer lie.
COPY pyhoglake/ /src/pyhoglake/
# Manifests only, so the dependency layer survives a bench source-only
# change. (pyproject's `readme` is resolved at build time, hence
# README.md.)
COPY bench/pyproject.toml bench/uv.lock bench/README.md ./
RUN uv sync --locked --no-dev --no-editable --no-install-project

COPY bench/src/ src/
RUN uv sync --locked --no-dev --no-editable

# --no-editable above matters twice. It keeps the venv self-contained,
# so the runtime stage copies one directory and no source tree; and it
# leaves site-packages overridable by PYTHONPATH, which is what the
# unmerged-branch escape hatch in bench/README.md relies on (an
# editable install's import hook wins over PYTHONPATH and would
# silently keep running the baked-in harness).

FROM python:3.13-slim-trixie
# Non-root, same uid as the server image. Bench needs no filesystem
# beyond its working directory and /tmp (parquet is built in memory and
# uploaded; the results journal is one append-only file in the cwd).
# uid == gid on purpose, so the pod spec can name one number for
# runAsUser, runAsGroup and fsGroup — fsGroup is what makes the
# emptyDir at /work writable by a non-root container.
RUN groupadd --system --gid 10001 hoglake \
 && useradd --system --uid 10001 --gid 10001 hoglake
COPY --from=build /opt/hoglake-bench/venv/ /opt/hoglake-bench/venv/
ENV PATH="/opt/hoglake-bench/venv/bin:$PATH"
# The results journal is written to the cwd, so /work belongs to the
# run user. Everything else stays read-only-rootfs friendly: nothing
# outside /work and /tmp is written.
RUN install -d -o 10001 -g 10001 /work
WORKDIR /work
USER 10001
# Nothing runs on its own and nothing listens. The deploy pod overrides
# this with `sleep infinity` so the operator drives it; a bare
# `docker run` gets the CLI, and `--help` needs no server.
ENTRYPOINT ["hoglake-bench"]
CMD ["--help"]
