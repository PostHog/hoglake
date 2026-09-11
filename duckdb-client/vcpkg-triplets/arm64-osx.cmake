# Overlay triplet (same name as the builtin arm64-osx, so it shadows
# it when VCPKG_OVERLAY_TRIPLETS points here). Identical to the
# builtin plus HAVE_PIPE2=0: recent macOS SDKs declare pipe2() as
# "introduced in macOS 27", so curl's check_symbol_exists finds it and
# the build then fails under -Werror=partial-availability with a macOS
# 11 deployment target. Pre-seeding the cache variable skips the broken
# detection; it is harmless for every other port.
set(VCPKG_TARGET_ARCHITECTURE arm64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)

set(VCPKG_CMAKE_SYSTEM_NAME Darwin)
set(VCPKG_OSX_ARCHITECTURES arm64)

set(VCPKG_CMAKE_CONFIGURE_OPTIONS -DHAVE_PIPE2=0)
