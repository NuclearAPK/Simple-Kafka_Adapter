# Native harness: loads SimpleKafka1C.dll like the 1C platform and runs the acceptance
# scenarios of specs/share-consumer.md against the Docker broker (docker/kafka-share).
# Configure with the same vcpkg toolchain and triplet as the component, e.g.:
#   cmake -S tests/native-harness -B out/build/native-harness -G Ninja -DCMAKE_BUILD_TYPE=Release
#     -DCMAKE_TOOLCHAIN_FILE=C:/Sources/vcpkg/scripts/buildsystems/vcpkg.cmake
#     -DVCPKG_TARGET_TRIPLET=x64-windows-static
cmake_minimum_required(VERSION 3.20)
project(native_harness CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(MSVC)
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
endif()

set(Boost_USE_STATIC_LIBS ON)
find_package(Boost REQUIRED COMPONENTS json)
# Broker-side preparation (share group configs) that the component has no API for
find_package(RdKafka CONFIG REQUIRED)

add_executable(native_harness main.cpp addin_host.cpp)
target_include_directories(native_harness PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../include)
target_link_libraries(native_harness PRIVATE Boost::json RdKafka::rdkafka)

if(MSVC)
    target_compile_options(native_harness PRIVATE /utf-8 /W4 /wd4201) # C4201: nameless unions in the 1C header types.h
    target_compile_definitions(native_harness PRIVATE _CRT_SECURE_NO_WARNINGS NOMINMAX WIN32_LEAN_AND_MEAN)
else()
    target_link_libraries(native_harness PRIVATE ${CMAKE_DL_LIBS})
endif()
