PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=hoglake
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Core extensions needed for testing. parquet is in-tree; httpfs is the
# s3 filesystem for integration tests against the dev MinIO.
DEFAULT_TEST_EXTENSION_DEPS=
FULL_TEST_EXTENSION_DEPS=httpfs

# Stabilize all tests in CI
ifdef CI
TEST_FLAGS:=--stabilize-tests
endif
# ~[.] excludes hidden/slow (.test_slow) tests
T ?= $(TEST_FLAGS) "~[.]test/*"

ifeq (${BUILD_EXTENSION_TEST_DEPS}, full)
	USE_MERGED_VCPKG_MANIFEST:=1
endif

deps:
	${PROJ_DIR}scripts/fetch-deps.sh

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile
