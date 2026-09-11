# This file is included by DuckDB's build system. It specifies which
# extensions to load.

duckdb_extension_load(hoglake
        SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)

if(NOT DEFINED ENV{DISABLE_EXTENSIONS_FOR_TEST})
    duckdb_extension_load(json)
endif()
