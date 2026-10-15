# Standalone probe of the librdkafka share consumer API (not part of the component).
# Configure with the same vcpkg toolchain and triplet as the component, e.g.:
#   cmake -S tests/share-probe -B out/build/share-probe -G Ninja -DCMAKE_BUILD_TYPE=Release
#     -DCMAKE_TOOLCHAIN_FILE=C:/Sources/vcpkg/scripts/buildsystems/vcpkg.cmake
#     -DVCPKG_TARGET_TRIPLET=x64-windows-static
cmake_minimum_required(VERSION 3.20)
project(share_probe C)

if(MSVC)
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
endif()

find_package(RdKafka CONFIG REQUIRED)

add_executable(share_probe share_probe.c)
target_link_libraries(share_probe PRIVATE RdKafka::rdkafka)
