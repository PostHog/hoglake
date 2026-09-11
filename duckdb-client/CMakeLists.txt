cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME hoglake)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(src/include)
# duckdb-vendored header-only deps: cpp-httplib (REST transport) and
# yyjson (wire codec; compiled into duckdb core).
include_directories(${CMAKE_CURRENT_LIST_DIR}/duckdb/third_party/httplib)
include_directories(${CMAKE_CURRENT_LIST_DIR}/duckdb/third_party/yyjson/include)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wreturn-std-move)
endif()

set(EXTENSION_SOURCES
    src/hoglake_extension.cpp
    src/common/hoglake_types.cpp
    src/rest/hoglake_api_client.cpp
    src/storage/hoglake_catalog.cpp
    src/storage/hoglake_schema_entry.cpp
    src/storage/hoglake_storage.cpp
    src/storage/hoglake_table_entry.cpp
    src/storage/hoglake_transaction.cpp
    src/storage/hoglake_transaction_manager.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
