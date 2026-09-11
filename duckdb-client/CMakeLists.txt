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

# roaring (vcpkg) is used for deletion vectors, same as ducklake
find_package(roaring CONFIG REQUIRED)
if(TARGET roaring::roaring-headers)
  get_target_property(ROARING_INCLUDE_DIRS roaring::roaring-headers INTERFACE_INCLUDE_DIRECTORIES)
else()
  get_target_property(ROARING_INCLUDE_DIRS roaring::roaring INTERFACE_INCLUDE_DIRECTORIES)
endif()
if(NOT ROARING_INCLUDE_DIRS OR ROARING_INCLUDE_DIRS STREQUAL "ROARING_INCLUDE_DIRS-NOTFOUND")
  set(ROARING_INCLUDE_DIRS "${roaring_DIR}/../../include")
endif()
include_directories(${ROARING_INCLUDE_DIRS})

set(EXTENSION_SOURCES
    src/hoglake_extension.cpp
    src/common/hoglake_types.cpp
    src/rest/hoglake_api_client.cpp
    src/storage/hoglake_catalog.cpp
    src/storage/hoglake_delete_filter.cpp
    src/storage/hoglake_multi_file_list.cpp
    src/storage/hoglake_multi_file_reader.cpp
    src/storage/hoglake_puffin.cpp
    src/storage/hoglake_scan.cpp
    src/storage/hoglake_schema_entry.cpp
    src/storage/hoglake_storage.cpp
    src/storage/hoglake_table_entry.cpp
    src/storage/hoglake_transaction.cpp
    src/storage/hoglake_transaction_manager.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

if(TARGET roaring::roaring-headers)
  target_link_libraries(${EXTENSION_NAME} roaring::roaring roaring::roaring-headers roaring::roaring-headers-cpp)
  target_link_libraries(${TARGET_NAME}_loadable_extension roaring::roaring roaring::roaring-headers roaring::roaring-headers-cpp)
else()
  target_link_libraries(${EXTENSION_NAME} roaring::roaring)
  target_link_libraries(${TARGET_NAME}_loadable_extension roaring::roaring)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
