# Unit test of src/connect_failure.* (specs/ssl-fail-fast.md): error classification, hints,
# error text and sliced waiting. No broker needed. Configure with the component's toolchain:
#   cmake -S tests/connect-failure -B out/build/connect-failure -G Ninja -DCMAKE_BUILD_TYPE=Release
#     -DCMAKE_TOOLCHAIN_FILE=C:/Sources/vcpkg/scripts/buildsystems/vcpkg.cmake
#     -DVCPKG_TARGET_TRIPLET=x64-windows-static
#   cmake --build out/build/connect-failure && out/build/connect-failure/connect_failure_test
cmake_minimum_required(VERSION 3.20)
project(connect_failure_test CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(MSVC)
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
endif()

find_package(RdKafka CONFIG REQUIRED)

set(SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../src)
add_executable(connect_failure_test main.cpp ${SRC_DIR}/connect_failure.cpp)
target_include_directories(connect_failure_test PRIVATE ${SRC_DIR})
target_link_libraries(connect_failure_test PRIVATE RdKafka::rdkafka)

if(MSVC)
    target_compile_options(connect_failure_test PRIVATE /utf-8 /W4)
endif()
