# SSL fail-fast acceptance test (specs/ssl-fail-fast.md): loads SimpleKafka1C.dll like the 1C
# platform and checks fail-fast on a broker with a certificate not matching its host name.
# Needs such a broker and client certificates; no dependencies beyond include/, e.g.:
#   cmake -S tests/ssl-fail-fast -B out/build/ssl-fail-fast -G Ninja -DCMAKE_BUILD_TYPE=Release
#   cmake --build out/build/ssl-fail-fast
#   out/build/ssl-fail-fast/ssl_fail_fast out/build/x64-Release/SimpleKafka1C.dll kafka.internal:9093 C:/Temp/denvic
cmake_minimum_required(VERSION 3.20)
project(ssl_fail_fast CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(MSVC)
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
endif()

# The platform-side host is shared with the native harness
set(HOST_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../native-harness)

add_executable(ssl_fail_fast main.cpp ${HOST_DIR}/addin_host.cpp)
target_include_directories(ssl_fail_fast PRIVATE ${HOST_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/../../include)

if(MSVC)
    target_compile_options(ssl_fail_fast PRIVATE /utf-8 /W4 /wd4201) # C4201: nameless unions in the 1C header types.h
    target_compile_definitions(ssl_fail_fast PRIVATE _CRT_SECURE_NO_WARNINGS NOMINMAX WIN32_LEAN_AND_MEAN)
else()
    target_link_libraries(ssl_fail_fast PRIVATE ${CMAKE_DL_LIBS})
endif()
